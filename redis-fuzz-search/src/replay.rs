use std::{ffi::CString, os::raw::c_char, path::Path};
use libafl::testcase::Testcase;

fn load_testcase(path: &Path) -> CommandInputSeq {
    let blob = std::fs::read(path).expect("read testcase file");
    postcard::from_bytes::<Testcase<CommandInputSeq>>(&blob)
        .or_else(|_| bincode::deserialize::<Testcase<CommandInputSeq>>(&blob))
        .expect("deserialize testcase")
        .input
        .expect("empty testcase")
}

unsafe fn execute(seq: &CommandInputSeq) {
    for cmd in &seq.0 {
        harness_set_argc(cmd.argv.len());
        for (i, arg) in cmd.argv.iter().enumerate() {
            let cstr = CString::new(arg.as_str()).unwrap();
            harness_add_argv(i as i32, cstr.as_ptr(), arg.len());
        }
        harness_call_cmd(cmd.op);
    }
}

pub fn replay_file(path: &Path) {
    unsafe {
        harness_init()
    };
    let seq = load_testcase(path);
    unsafe {
        execute(&seq)
    };
}
