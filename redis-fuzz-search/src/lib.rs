use libafl::{
    Error,
    corpus::{InMemoryCorpus, InMemoryOnDiskCorpus, OnDiskCorpus, CorpusId, Corpus},
    mutators::{havoc_mutations, HavocScheduledMutator, MutationResult, Mutator}, 
    state::StdState,
    schedulers::QueueScheduler,
    monitors::{TuiMonitor, SimpleMonitor, MultiMonitor},
    events::{SimpleEventManager, SimpleRestartingEventManager, setup_restarting_mgr_std, EventConfig},
    fuzzer::{Fuzzer, StdFuzzer},
    inputs::{Input, BytesInput},
    executors::{ExitKind, InProcessExecutor, InProcessForkExecutor},
    stages::mutational::StdMutationalStage,
    generators::RandBytesGenerator,
    state::{HasRand, HasCorpus},
};
use libafl_bolts::{
    rands::{Rand, StdRand}, shmem::{ShMemProvider, StdShMemProvider}, tuples::tuple_list, AsSliceMut,
    os::{dup2, dup_and_mute_outputs},
};
use std::ffi::CString;
use std::os::raw::c_char;
mod smith;
use smith::{
    CommandSpec, ArgSpec, ArgKind, COMMAND_SPECS,
    CommandGenerator, FLUSHALL_OP
};
use std::os::unix::io::{AsRawFd, FromRawFd};
use serde::{Serialize, Deserialize};
use rand::{thread_rng, seq::IteratorRandom, Rng};
use std::fs::{OpenOptions, File};
use std::io::{self, Write};
use std::env;
use std::time::Duration;
use libafl::events::SendExiting;

use libafl::observers::{HitcountsMapObserver, CanTrack};
use libafl::feedbacks::{CrashFeedback, MaxMapFeedback};
use libafl_targets::coverage::{EDGES_MAP_PTR, std_edges_map_observer};
use libafl_targets::{forkserver, EDGES_MAP_DEFAULT_SIZE};

use mimalloc::MiMalloc;
#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;


mod mutator;
use mutator::CommandSeqMutator;
//mod replay;

#[derive(Clone, Debug, Hash, Serialize, Deserialize)]
pub struct CommandInput {
    pub op: i32,
    pub argv: Vec<String>,
}

#[derive(Clone, Debug, Hash, Serialize, Deserialize)]
pub struct CommandInputSeq(pub Vec<CommandInput>);

impl Input for CommandInputSeq {}


unsafe extern "C" {
    fn harness_init();
    fn harness_add_argv(idx: i32, s: *const c_char, len: usize);
    fn harness_call_cmd(op: i32);
    fn harness_set_argc(argc: usize);
    fn harness_seed_search();
    fn lua_state_init();
    fn lua_state_destroy();
}

fn fuzz() -> Result<(), Error> {

    /*#[cfg(unix)]
    let mut stdout_cpy = {
        let (new_stdout, new_stderr) = unsafe { dup_and_mute_outputs()? };
        if std::env::var("LIBAFL_FUZZ_DEBUG").is_ok() {
            unsafe {
                dup2(new_stderr, io::stderr().as_raw_fd())?;
            }
        }
        #[cfg(unix)]
        unsafe {
            File::from_raw_fd(new_stdout)
        }
    };*/


    let mut shmem_provider = StdShMemProvider::new().unwrap();

    //#[cfg(not(feature = "tui"))]
    //#[cfg(feature = "tui")]


    /*let mon = TuiMonitor::builder()
        .title("redis-fuzz")
        .enhanced_graphics(true)
        .build();*/

    let mon = SimpleMonitor::new(|s| println!("{s}"));

    //let mut mgr = SimpleEventManager::new(mon);
    
    let (state, mut mgr) = match setup_restarting_mgr_std(
        mon,
        1337,
        EventConfig::from_name("redis-fuzz"),
    ) {
        Ok(t) => t,
        Err(Error::ShuttingDown) => return Ok(()),
        Err(e) => return Err(e),
    };
    /*let (state, mut mgr) = match SimpleRestartingEventManager::launch(mon, &mut shmem_provider) {
        Ok(t) => t,
        Err(Error::ShuttingDown) => return Ok(()),
        Err(e) => return Err(e),
    };*/

    let mut shmem = shmem_provider.new_shmem(EDGES_MAP_DEFAULT_SIZE).unwrap();
    unsafe { EDGES_MAP_PTR = shmem.as_slice_mut().as_mut_ptr() }
    let edges_observer = HitcountsMapObserver::new(
        unsafe { std_edges_map_observer("edges") }
    ).track_indices();
    let mut feedback = MaxMapFeedback::new(&edges_observer);
    let mut objective = CrashFeedback::new();

    let mut state = state.unwrap_or_else(|| {
        StdState::new(
            StdRand::new(),
            InMemoryOnDiskCorpus::<CommandInputSeq>::new("./corpus").unwrap(),
            OnDiskCorpus::new("./crashes").unwrap(),
            &mut feedback,
            &mut objective,
        ).unwrap()
    });
    let mut fuzzer = StdFuzzer::new(QueueScheduler::new(), feedback, objective);
    let mut stages = tuple_list!(StdMutationalStage::new(
        CommandSeqMutator::new(StdRand::new())
    ));

    let mut cmdgen = smith::CommandGenerator::new(StdRand::new());
    let specs: Vec<&CommandSpec> =
        COMMAND_SPECS.values().map(|b| b.as_ref()).collect();
    let mut rng = rand::thread_rng();
    for _ in 0..128 {
        let seq = cmdgen.generate_command_sequence(&specs, rng.gen_range(1..100));
        state.corpus_mut().add(seq.into()).unwrap();
    }

    let flushall_cmd = CString::new("FLUSHALL").unwrap();
    let flushall_op = *FLUSHALL_OP.get().unwrap();
    let mut harness = |input: &CommandInputSeq| unsafe {
        static INIT: std::sync::Once = std::sync::Once::new();
        INIT.call_once(|| {
            harness_init();
        });
        lua_state_init();
        // Seed a fresh `idx` index + sample docs so FT.* commands exercise
        // real index paths instead of early-exiting on "no such index".
        harness_seed_search();
        for cmd in &input.0 {
            harness_set_argc(cmd.argv.len());
            for (i, arg) in cmd.argv.iter().enumerate() {
                harness_add_argv(
                    i as i32,
                    CString::new(arg.clone()).unwrap().as_ptr(),
                    arg.len()
                );
            }
            harness_call_cmd(cmd.op);
        }
        // reset state with flushall to avoid coverage interference, not perfect
        harness_set_argc(1);
        harness_add_argv(0, flushall_cmd.as_ptr(), 8);
        harness_call_cmd(flushall_op);

        lua_state_destroy();
        ExitKind::Ok
    };

    let mut executor = InProcessExecutor::new(
        &mut harness,
        tuple_list!(edges_observer),
        &mut fuzzer,
        &mut state,
        &mut mgr,
        /*Duration::from_millis(5000),
        shmem_provider,*/
    ).unwrap();
    fuzzer.fuzz_loop(&mut stages, &mut executor, &mut state, &mut mgr).unwrap();
    Ok(())
}

#[unsafe(no_mangle)]
pub extern "C" fn libafl_main() {
    let _ = fuzz();
}
