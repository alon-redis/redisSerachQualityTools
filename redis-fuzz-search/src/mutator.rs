use crate::smith::{CommandGenerator, CONFIG, OP_TO_SPEC, COMMAND_SPECS};
use crate::CommandInputSeq;
use libafl::{
    corpus::{Corpus, CorpusId},
    mutators::{MutationResult, Mutator},
    state::{HasCorpus, HasRand},
    Error,
};
use libafl_bolts::{nonzero, rands::Rand, Named};
use std::borrow::Cow;
use std::cmp::{max, min};
use std::num::NonZero;
use std::time::Duration;

pub struct CommandSeqMutator<R: Rand> {
    generator: CommandGenerator<R>,
    max_seq_len: usize,
}

impl<R: Rand> CommandSeqMutator<R> {
    pub fn new(rand: R) -> Self {
        Self {
            generator: CommandGenerator::new(rand),
            max_seq_len: CONFIG.max_seq_len,
        }
    }

    #[inline]
    fn rand_len<S: HasRand>(&self, state: &mut S, upper: usize) -> usize {
        if upper <= 1 {
            return 1;
        }
        state.rand_mut().below(NonZero::new(upper).unwrap()) + 1
    }

    fn delete_chunk<S: HasRand>(&self, state: &mut S, seq: &mut CommandInputSeq) {
        if seq.0.len() < 2 {
            return;
        }
        let start = state
            .rand_mut()
            .below(NonZero::new(seq.0.len() - 1).unwrap());
        let len = state
            .rand_mut()
            .below(NonZero::new(seq.0.len() - start).unwrap())
            + 1;
        seq.0.drain(start..start + len);
    }

    fn duplicate_chunk<S: HasRand>(&self, state: &mut S, seq: &mut CommandInputSeq) {
        if seq.0.len() < 2 || seq.0.len() >= self.max_seq_len {
            return;
        }
        let start = state
            .rand_mut()
            .below(NonZero::new(seq.0.len() - 1).unwrap());
        let len = state
            .rand_mut()
            .below(NonZero::new(seq.0.len() - start).unwrap())
            + 1;
        let end = min(start + len, seq.0.len());
        let dup = seq.0[start..end].to_vec();
        seq.0.extend_from_slice(&dup);
        seq.0.truncate(self.max_seq_len);
    }

    fn swap_commands<S: HasRand>(&self, state: &mut S, seq: &mut CommandInputSeq) {
        if seq.0.len() < 2 {
            return;
        }
        let i = state.rand_mut().below(NonZero::new(seq.0.len()).unwrap());
        let mut j = state.rand_mut().below(NonZero::new(seq.0.len()).unwrap());
        if i == j {
            j = (j + 1) % seq.0.len();
        }
        seq.0.swap(i, j);
    }

    fn shuffle_window<S: HasRand>(&self, state: &mut S, seq: &mut CommandInputSeq) {
        let n = seq.0.len();
        if n < 3 {
            return;
        }
        let win = 2 + state.rand_mut().below(NonZero::new(min(8, n)).unwrap());
        if win >= n {
            seq.0.reverse();
            return;
        }
        let start = state
            .rand_mut()
            .below(NonZero::new(n - win + 1).unwrap());
        let mut slice = seq.0[start..start + win].to_vec();
        for k in (1..slice.len()).rev() {
            let r = state.rand_mut().below(NonZero::new(k + 1).unwrap());
            slice.swap(k, r);
        }
        seq.0.splice(start..start + win, slice);
    }

    fn insert_generated<S: HasRand>(&mut self, state: &mut S, seq: &mut CommandInputSeq) {
        let room = self.max_seq_len.saturating_sub(seq.0.len());
        if room == 0 {
            return;
        }
        let pos = if seq.0.is_empty() {
            0
        } else {
            state
                .rand_mut()
                .below(NonZero::new(seq.0.len()).unwrap())
        };
        let gen_len = self.rand_len(state, room.min(CONFIG.insert_max_len.max(1)));
        let mut new_cmds = self
            .generator
            .generate_command_sequence(
                &COMMAND_SPECS.values().map(|b| b.as_ref()).collect::<Vec<_>>(),
                gen_len,
            )
            .0;
        seq.0.splice(pos..pos, new_cmds.drain(..));
    }

    fn replace_generated<S: HasRand>(&mut self, state: &mut S, seq: &mut CommandInputSeq) {
        if seq.0.is_empty() {
            self.insert_generated(state, seq);
            return;
        }
        let max_len = seq.0.len().min(CONFIG.insert_max_len.max(1));
        let len = self.rand_len(state, max_len);
        let start = state
            .rand_mut()
            .below(NonZero::new(seq.0.len() - len + 1).unwrap());
        let mut new_cmds = self
            .generator
            .generate_command_sequence(
                &COMMAND_SPECS.values().map(|b| b.as_ref()).collect::<Vec<_>>(),
                len,
            )
            .0;
        seq.0.splice(start..start + len, new_cmds.drain(..));
    }

    fn arg_mutate_typed<S: HasRand>(&self, state: &mut S, seq: &mut CommandInputSeq) {
        if seq.0.is_empty() {
            return;
        }
        let idx = state
            .rand_mut()
            .below(NonZero::new(seq.0.len()).unwrap());
        let cmd = &mut seq.0[idx];

        let tokens = OP_TO_SPEC
            .get(&cmd.op)
            .map(|s| s.tokens_set())
            .unwrap_or_default();

        if cmd.argv.len() <= 1 {
            return;
        }
        let mut tries = 0;
        let pos = loop {
            tries += 1;
            if tries > 16 {
                return;
            }
            let p = 1 + state
                .rand_mut()
                .below(NonZero::new(cmd.argv.len() - 1).unwrap());
            if !tokens.contains(&cmd.argv[p]) {
                break p;
            }
        };

        let val = cmd.argv[pos].clone();
        let newv = self.mutate_value(state, val);
        cmd.argv[pos] = newv;
    }

    fn mutate_value<S: HasRand>(&self, state: &mut S, v: String) -> String {
        if v.eq_ignore_ascii_case("inf") || v.eq_ignore_ascii_case("nan") {
            return if state.rand_mut().below(nonzero!(2)) == 0 {
                "0".to_string()
            } else {
                "-0".to_string()
            };
        }
        let is_int = v.as_bytes().iter().all(|c| {
            (*c >= b'0' && *c <= b'9') || *c == b'-' || *c == b'+'
        }) && v.chars().any(|c| c.is_ascii_digit());
        let is_float = !is_int
            && (v.contains('.') || v.contains('e') || v.contains('E') || v.eq_ignore_ascii_case("nan") || v.eq_ignore_ascii_case("inf"));
        let looks_pattern = v.contains('*') || v.contains('?') || v.contains('[');
        if is_int {
            self.mutate_int(state, &v)
        } else if is_float {
            self.mutate_float(state, &v)
        } else if looks_pattern {
            self.mutate_pattern(state, &v)
        } else {
            self.mutate_string(state, &v)
        }
    }

    fn mutate_int<S: HasRand>(&self, state: &mut S, v: &str) -> String {
        let mut n = v.parse::<i128>().unwrap_or(0);
        match state.rand_mut().below(nonzero!(6)) {
            0 => n = n.wrapping_add(1),
            1 => n = n.wrapping_sub(1),
            2 => n = n.wrapping_mul(2),
            3 => n = n.wrapping_neg(),
            4 => {
                const INTERESTING: &[i128] = &[
                    -1, 0, 1, 2, 3, 4, 8, 15, 16, 31, 32, 63, 64, 127, 128, 255, 256, 511, 512,
                    1023, 1024, 32767, 32768, 65535, 65536, 2147483647, -2147483648,
                    9223372036854775807, -9223372036854775808,
                ];
                let idx = state
                    .rand_mut()
                    .below(NonZero::new(INTERESTING.len()).unwrap()) as usize;
                n = INTERESTING[idx];
            }
            _ => {
                let r = state.rand_mut().below(nonzero!(20000)) as i128 - 10000;
                n = r;
            }
        }
        n.to_string()
    }

    fn mutate_float<S: HasRand>(&self, state: &mut S, v: &str) -> String {
        if state.rand_mut().below(nonzero!(10)) < 2 {
            return if state.rand_mut().below(nonzero!(2)) == 0 {
                "inf".to_string()
            } else {
                "nan".to_string()
            };
        }
        let mut x = v.parse::<f64>().unwrap_or(0.0);
        match state.rand_mut().below(nonzero!(6)) {
            0 => x = x * 2.0,
            1 => x = x / 2.0,
            2 => x = x + 1.0,
            3 => x = x - 1.0,
            4 => x = -x,
            _ => {
                let e = (state.rand_mut().below(nonzero!(20)) as i32) - 10;
                return format!("{:.6}e{:+}", x, e);
            }
        }
        format!("{:.6}", x)
    }

    fn mutate_pattern<S: HasRand>(&self, state: &mut S, v: &str) -> String {
        let mut s = v.to_string();
        match state.rand_mut().below(nonzero!(5)) {
            0 => s.push('*'),
            1 => s.push('?'),
            2 => s = format!("*{}*", s),
            3 => {
                let pos =
                    state.rand_mut().below(NonZero::new(s.len().max(1)).unwrap()) as usize;
                s.insert(pos.min(s.len()), '*');
            }
            _ => {
                if s.len() > 1 {
                    let cut = state
                        .rand_mut()
                        .below(NonZero::new(s.len() - 1).unwrap()) as usize
                        + 1;
                    s.truncate(cut);
                }
            }
        }
        s
    }

    fn mutate_string<S: HasRand>(&self, state: &mut S, v: &str) -> String {
        let mut s = v.as_bytes().to_vec();
        if s.is_empty() {
            s.push(b'a');
        }
        match state.rand_mut().below(nonzero!(8)) {
            0 => {
                // flip a byte
                let i = state.rand_mut().below(NonZero::new(s.len()).unwrap()) as usize;
                let mut b = s[i] ^ (1u8 << (state.rand_mut().below(nonzero!(8)) as u8));
                if b == 0 {
                    b = 1;
                }
                s[i] = b;
            }
            1 => {
                // duplicate slice
                let start = state.rand_mut().below(NonZero::new(s.len()).unwrap()) as usize;
                let end = min(s.len(), start + state.rand_mut().below(nonzero!(8)) as usize + 1);
                let mut dup = s[start..end].to_vec();
                s.extend_from_slice(&dup);
                s.truncate(CONFIG.mutation.string.max_len);
            }
            2 => {
                // delete slice
                if s.len() > 1 {
                    let start = state.rand_mut().below(NonZero::new(s.len() - 1).unwrap()) as usize;
                    let len = state.rand_mut().below(NonZero::new(s.len() - start).unwrap()) as usize + 1;
                    s.drain(start..min(start + len, s.len()));
                }
            }
            3 => {
                // insert bytes
                let pos = state.rand_mut().below(NonZero::new(s.len()).unwrap()) as usize;
                let ins_len = (state.rand_mut().below(nonzero!(8)) as usize) + 1;
                let mut ins = Vec::with_capacity(ins_len);
                for _ in 0..ins_len {
                    let mut b = 32 + state.rand_mut().below(nonzero!(95)) as u8; // printable
                    if b == 0 {
                        b = 1;
                    }
                    ins.push(b);
                }
                s.splice(pos..pos, ins);
                s.truncate(CONFIG.mutation.string.max_len);
            }
            4 => s.make_ascii_uppercase(),
            5 => s.make_ascii_lowercase(),
            6 => s = format!("{}{}", v, v).into_bytes(),
            _ => {
                // truncate/extend mildly
                if self.max_seq_len % 2 == 0 {
                    s.truncate(s.len().saturating_sub(1));
                } else {
                    s.extend_from_slice(b"X");
                }
            }
        }
        for b in &mut s {
            if *b == 0 {
                *b = 1;
            }
        }
        String::from_utf8_lossy(&s).into_owned()
    }

    fn splice_other_input<S: HasRand + HasCorpus<CommandInputSeq>>(
        &self,
        state: &mut S,
        seq: &mut CommandInputSeq,
    ) {
        use std::num::NonZero;

        let count = state.corpus().count();
        if count < 2 || seq.0.len() >= self.max_seq_len {
            return;
        }

        let mut tries = 0;
        let donor = loop {
            tries += 1;
            if tries > 8 {
                return;
            }

            let idx = state.rand_mut().below(NonZero::new(count).unwrap());
            let maybe_inp = {
                let corpus = state.corpus();
                let id = corpus.nth(idx);
                if let Ok(cell) = corpus.get(id) {
                    let mut tc = cell.borrow_mut();
                    if tc.load_input(corpus).is_ok() {
                        tc.input().as_ref().cloned()
                    } else {
                        None
                    }
                } else {
                    None
                }
            };

            if let Some(inp) = maybe_inp {
                break inp;
            }
        };

        if donor.0.is_empty() {
            return;
        }

        let room = self.max_seq_len.saturating_sub(seq.0.len());
        if room == 0 {
            return;
        }

        let max_take = std::cmp::min(room, donor.0.len());
        let take = 1 + state.rand_mut().below(NonZero::new(max_take).unwrap());
        let start = state
            .rand_mut()
            .below(NonZero::new(donor.0.len() - take + 1).unwrap());

        let slice = donor.0[start..start + take].to_vec();
        let pos = if seq.0.is_empty() {
            0
        } else {
            state.rand_mut().below(NonZero::new(seq.0.len()).unwrap())
        };
        seq.0.splice(pos..pos, slice);
        seq.0.truncate(self.max_seq_len);
    }
}

impl<R: Rand> Named for CommandSeqMutator<R> {
    fn name(&self) -> &Cow<'static, str> {
        &Cow::Borrowed("CommandSeqMutator")
    }
}

impl<S, R> Mutator<CommandInputSeq, S> for CommandSeqMutator<R>
where
    S: HasRand + HasCorpus<CommandInputSeq>,
    R: Rand,
{
    fn mutate(&mut self, state: &mut S, input: &mut CommandInputSeq) -> Result<MutationResult, Error> {
        let probs = &CONFIG.mutation.probabilities;
        let ops: &[fn(&mut Self, &mut S, &mut CommandInputSeq)] = &[
            |s, st, i| s.delete_chunk(st, i),
            |s, st, i| s.duplicate_chunk(st, i),
            |s, st, i| s.insert_generated(st, i),
            |s, st, i| s.replace_generated(st, i),
            |s, st, i| s.swap_commands(st, i),
            |s, st, i| s.shuffle_window(st, i),
            |s, st, i| s.arg_mutate_typed(st, i),
        ];
        let weights = [
            probs.delete_chunk,
            probs.duplicate_chunk,
            probs.insert_generated,
            probs.replace_generated,
            probs.swap_commands,
            probs.shuffle_window,
            probs.arg_mutate,
        ];

        let mut r = {
            let sum: f64 = weights.iter().sum();
            let rv = {
                let a = state.rand_mut().next() >> 11;
                (a as f64) * (1.0 / ((1u64 << 53) as f64))
            };
            rv * sum
        };

        let mut chosen = 0usize;
        for (i, w) in weights.iter().enumerate() {
            if r <= *w {
                chosen = i;
                break;
            }
            r -= *w;
        }
        (ops[chosen])(self, state, input);

        if CONFIG.mutation.splice.enable {
            let a = {
                let a = state.rand_mut().next() >> 11;
                (a as f64) * (1.0 / ((1u64 << 53) as f64))
            };
            if a < CONFIG.mutation.splice.probability {
                self.splice_other_input(state, input);
            }
        }

        Ok(MutationResult::Mutated)
    }

    fn post_exec(&mut self, _input: &mut S, _corpus_idx: Option<CorpusId>) -> Result<(), Error> {
        Ok(())
    }
}
