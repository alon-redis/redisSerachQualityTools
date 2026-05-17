use once_cell::sync::{Lazy, OnceCell};
use serde::Deserialize;
use serde_json::{from_str, Value};
use std::{
    borrow::Cow,
    collections::{HashMap, HashSet},
    ffi::CStr,
    fs,
    fs::read_to_string,
    io::{self, Read},
    os::raw::c_char,
    path::Path,
};
use libafl_bolts::rands::Rand;
use core::num::NonZeroUsize;
use std::ffi::CString;

use crate::{CommandInput, CommandInputSeq};

#[derive(Debug, Deserialize)]
pub struct CommandSpec {
    pub op: i32,
    pub name: String,
    pub module: String,
    pub arguments: Vec<ArgSpec>,
    #[serde(default)]
    pub group: String,
    #[serde(default)]
    pub command_flags: Vec<String>,
    #[serde(default)]
    pub arity: i64,
}

impl CommandSpec {
    #[inline]
    pub fn is_blocking(&self) -> bool {
        self.command_flags.iter().any(|f| f == "BLOCKING")
    }

    #[inline]
    pub fn tokens_set(&self) -> HashSet<String> {
        fn collect(args: &[ArgSpec], out: &mut HashSet<String>) {
            for a in args {
                if let Some(t) = &a.token {
                    out.insert(t.clone());
                }
                if !a.arguments.is_empty() {
                    collect(&a.arguments, out);
                }
            }
        }
        let mut s = HashSet::new();
        collect(&self.arguments, &mut s);
        s
    }
}

#[derive(Debug, Deserialize, Clone)]
pub struct ArgSpec {
    #[serde(default)]
    pub name: String,
    pub token: Option<String>,
    #[serde(default, rename = "type")]
    pub kind: ArgKind,
    #[serde(default)]
    pub optional: bool,
    #[serde(default)]
    pub multiple: bool,
    #[serde(default)]
    pub arguments: Vec<ArgSpec>,
    #[serde(default)]
    pub key_spec_index: Option<u8>,
}

#[derive(Debug, Deserialize, Clone)]
#[serde(rename_all = "kebab-case")]
pub enum ArgKind {
    Array,
    Block,
    Double,
    Integer,
    Key,
    Null,
    Number,
    Object,
    Oneof,
    Pattern,
    PureToken,
    Function,
    String,
    UnixTime,
}

impl Default for ArgKind {
    fn default() -> Self {
        ArgKind::String
    }
}

pub static COMMAND_SPECS: Lazy<HashMap<String, Box<CommandSpec>>> = Lazy::new(|| {
    /*let core_dir = concat!(env!("CARGO_MANIFEST_DIR"), "/src/redis/src/commands");
    let default_modules: &[(&str, &str)] = &[
        ("bloom", concat!(env!("CARGO_MANIFEST_DIR"), "/src/RedisBloom/commands.json")),
        //("timeseries", concat!(env!("CARGO_MANIFEST_DIR"), "/src/redistimeseries/commands.json")),
        ("search", concat!(env!("CARGO_MANIFEST_DIR"), "/src/redisearch/commands.json")),
        //("json", concat!(env!("CARGO_MANIFEST_DIR"), "/src/redisjson/commands.json")),
    ];*/
    let core_dir = "./src/redis/src/commands";
    let default_modules: &[(&str, &str)] = &[
        // RediSearch-focused fuzzer: only the search module is loaded.
        ("search", "./src/redisearch/commands.json"),
        //("bloom", "./src/RedisBloom/commands.json"),
        //("timeseries", "./src/redistimeseries/commands.json"),
        //("json", "./src/redisjson/commands.json"),
    ];
    load_commands(core_dir, default_modules)
        .expect("failed to load commands")
});

pub static OP_TO_SPEC: Lazy<HashMap<i32, &'static CommandSpec>> = Lazy::new(|| {
    let mut m = HashMap::new();
    for v in COMMAND_SPECS.values() {
        let ptr: &'static CommandSpec = unsafe { &*(v.as_ref() as *const CommandSpec) };
        m.insert(v.op, ptr);
    }
    m
});

pub static FLUSHALL_OP: OnceCell<i32> = OnceCell::new();

#[derive(Debug, Deserialize)]
pub struct FuzzConfig {
    #[serde(default)]
    pub blacklist: Vec<String>,
    #[serde(default)]
    pub whitelist: Vec<String>,
    #[serde(default)]
    pub modules: Vec<String>,

    #[serde(default = "d_max_seq_len")]
    pub max_seq_len: usize,
    #[serde(default = "d_insert_max_len")]
    pub insert_max_len: usize,

    #[serde(default)]
    pub weights: Weights,

    #[serde(default)]
    pub preconditions: Preconditions,

    #[serde(default)]
    pub blocking: BlockingCfg,

    #[serde(default)]
    pub arg_caps: ArgCaps,

    #[serde(default)]
    pub mutation: MutationCfg,
}

#[derive(Debug, Deserialize, Default)]
pub struct Weights {
    #[serde(default)]
    pub group: HashMap<String, f64>,
    #[serde(default)]
    pub command: HashMap<String, f64>,
}

#[derive(Debug, Deserialize)]
pub struct Preconditions {
    #[serde(default = "d_true")]
    pub enable: bool,
    #[serde(default = "d_pre_prob")]
    pub probability: f64,
    #[serde(default)]
    pub members: MembersRange,
}
impl Default for Preconditions {
    fn default() -> Self {
        Self {
            enable: true,
            probability: d_pre_prob(),
            members: Default::default(),
        }
    }
}

#[derive(Debug, Deserialize, Clone, Copy)]
pub struct MembersRange {
    #[serde(default = "d_range_small")]
    pub list: (u32, u32),
    #[serde(default = "d_range_small")]
    pub set: (u32, u32),
    #[serde(default = "d_range_small")]
    pub zset: (u32, u32),
    #[serde(default = "d_range_small")]
    pub hash: (u32, u32),
    #[serde(default = "d_range_small")]
    pub stream: (u32, u32),
}
impl Default for MembersRange {
    fn default() -> Self {
        Self {
            list: d_range_small(),
            set: d_range_small(),
            zset: d_range_small(),
            hash: (1, 6),
            stream: (1, 5),
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct BlockingCfg {
    #[serde(default = "d_true")]
    pub allow: bool,
    #[serde(default = "d_min_timeout")]
    pub min_timeout_sec: f64,
    #[serde(default = "d_max_timeout")]
    pub max_timeout_sec: f64,
}
impl Default for BlockingCfg {
    fn default() -> Self {
        Self {
            allow: true,
            min_timeout_sec: d_min_timeout(),
            max_timeout_sec: d_max_timeout(),
        }
    }
}

#[derive(Debug, Deserialize, Default)]
pub struct ArgCaps {
    #[serde(default = "d_count_cap")]
    pub COUNT: u64,
    #[serde(default = "d_limit_cap")]
    pub LIMIT: u64,
    #[serde(default = "d_range_count_cap")]
    pub RANGE_COUNT: u64,
}

#[derive(Debug, Deserialize)]
pub struct MutationCfg {
    #[serde(default)]
    pub probabilities: MutationProbs,
    #[serde(default)]
    pub string: StringCfg,
    #[serde(default)]
    pub splice: SpliceCfg,
}
impl Default for MutationCfg {
    fn default() -> Self {
        Self {
            probabilities: Default::default(),
            string: Default::default(),
            splice: Default::default(),
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct MutationProbs {
    #[serde(default = "d_p_12")]
    pub delete_chunk: f64,
    #[serde(default = "d_p_10")]
    pub duplicate_chunk: f64,
    #[serde(default = "d_p_22")]
    pub insert_generated: f64,
    #[serde(default = "d_p_20")]
    pub replace_generated: f64,
    #[serde(default = "d_p_10")]
    pub swap_commands: f64,
    #[serde(default = "d_p_08")]
    pub shuffle_window: f64,
    #[serde(default = "d_p_18")]
    pub arg_mutate: f64,
}
impl Default for MutationProbs {
    fn default() -> Self {
        Self {
            delete_chunk: d_p_12(),
            duplicate_chunk: d_p_10(),
            insert_generated: d_p_22(),
            replace_generated: d_p_20(),
            swap_commands: d_p_10(),
            shuffle_window: d_p_08(),
            arg_mutate: d_p_18(),
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct StringCfg {
    #[serde(default = "d_str_max")]
    pub max_len: usize,
    #[serde(default = "d_str_special")]
    pub special_prob: f64,
}
impl Default for StringCfg {
    fn default() -> Self {
        Self {
            max_len: d_str_max(),
            special_prob: d_str_special(),
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct SpliceCfg {
    #[serde(default = "d_true")]
    pub enable: bool,
    #[serde(default = "d_p_07")]
    pub probability: f64,
    #[serde(default = "d_splice_max")]
    pub max_len: usize,
}
impl Default for SpliceCfg {
    fn default() -> Self {
        Self {
            enable: true,
            probability: d_p_07(),
            max_len: d_splice_max(),
        }
    }
}

#[inline] fn d_true() -> bool { true }
#[inline] fn d_max_seq_len() -> usize { 2048 }
#[inline] fn d_insert_max_len() -> usize { 16 }
#[inline] fn d_pre_prob() -> f64 { 0.35 }
#[inline] fn d_min_timeout() -> f64 { 0.0005 }
#[inline] fn d_max_timeout() -> f64 { 0.010 }
#[inline] fn d_count_cap() -> u64 { 64 }
#[inline] fn d_limit_cap() -> u64 { 64 }
#[inline] fn d_range_count_cap() -> u64 { 1024 }
#[inline] fn d_range_small() -> (u32, u32) { (1, 8) }
#[inline] fn d_p_12() -> f64 { 0.12 }
#[inline] fn d_p_10() -> f64 { 0.10 }
#[inline] fn d_p_22() -> f64 { 0.22 }
#[inline] fn d_p_20() -> f64 { 0.20 }
#[inline] fn d_p_08() -> f64 { 0.08 }
#[inline] fn d_p_18() -> f64 { 0.18 }
#[inline] fn d_p_07() -> f64 { 0.07 }
#[inline] fn d_str_max() -> usize { 128 }
#[inline] fn d_str_special() -> f64 { 0.25 }
#[inline] fn d_splice_max() -> usize { 64 }

pub static CONFIG: Lazy<FuzzConfig> = Lazy::new(|| {
    /*let path = std::env::var("FUZZ_CONFIG")
        .unwrap_or(concat!(env!("CARGO_MANIFEST_DIR"), "/defconfig.json").to_string());*/
    let path = std::env::var("FUZZ_CONFIG")
        .unwrap_or("./defconfig.json".to_string());
    let data = read_to_string(&path).expect("Failed to load config");
    from_str(&data).expect("Invalid config JSON")
});

pub struct CommandGenerator<R: Rand> {
    rand: R,
    p_option: NonZeroUsize,
    max_repeat: NonZeroUsize,
    tokens_cache: HashMap<i32, HashSet<String>>,
}

impl<R: Rand> CommandGenerator<R> {
    pub fn new(rand: R) -> Self {
        Self {
            rand,
            p_option: NonZeroUsize::new(2).unwrap(),
            max_repeat: NonZeroUsize::new(10).unwrap(),
            tokens_cache: HashMap::new(),
        }
    }

    pub fn generate_command_sequence(
        &mut self,
        specs: &[&CommandSpec],
        len: usize,
    ) -> CommandInputSeq {
        let mut seq = Vec::with_capacity(len);
        let mut pool = KeyPool::default();

        for _ in 0..len {
            let spec = self.weighted_pick(specs);
            if CONFIG.preconditions.enable
                && self.flip(CONFIG.preconditions.probability)
            {
                self.emit_preconditions(spec, &mut seq, &mut pool);
            }
            seq.push(self.generate_command(spec, &mut pool));
        }
        CommandInputSeq(seq)
    }

    fn weighted_pick<'a>(&mut self, specs: &'a [&CommandSpec]) -> &'a CommandSpec {
        let mut total = 0f64;
        for s in specs {
            total += self.spec_weight(s);
        }
        if total <= 0.0 {
            let idx = self.rand.below(NonZeroUsize::new(specs.len()).unwrap());
            return specs[idx];
        }
        let mut r = self.rand_biased_f64() * total;
        for s in specs {
            let w = self.spec_weight(s);
            if r <= w {
                return s;
            }
            r -= w;
        }
        specs[specs.len() - 1]
    }

    fn spec_weight(&self, s: &CommandSpec) -> f64 {
        let gw = CONFIG
            .weights
            .group
            .get(&s.group)
            .copied()
            .unwrap_or(1.0);
        let cw = CONFIG
            .weights
            .command
            .get(&s.name)
            .copied()
            .unwrap_or(1.0);
        gw * cw
    }

    pub fn generate_command(&mut self, spec: &CommandSpec, pool: &mut KeyPool) -> CommandInput {
        let mut cmd = vec![spec.name.clone()];
        for arg in &spec.arguments {
            self.generate_argument(spec, arg, &mut cmd, pool);
        }
        CommandInput {
            op: spec.op,
            argv: cmd,
        }
    }

    fn generate_argument(
        &mut self,
        spec: &CommandSpec,
        arg: &ArgSpec,
        cmd: &mut Vec<String>,
        pool: &mut KeyPool,
    ) {
        if arg.optional && self.rand.below(self.p_option) == 0 {
            return;
        }
        let repeat_count = if arg.multiple {
            self.rand.below(self.max_repeat) + 1
        } else {
            1
        };

        for _ in 0..repeat_count {
            if let Some(token) = &arg.token {
                cmd.push(token.clone());
            }
            // Search-module arg overrides: route well-known arg names to
            // the seed-created index/dict/alias/suggestion-list/doc-key so
            // FT.* commands hit real code paths most of the time.
            if spec.module == "search" {
                if let Some(v) = self.search_arg_override(arg) {
                    cmd.push(v);
                    continue;
                }
            }
            match arg.kind {
                ArgKind::String => cmd.push(self.gen_string_for(spec, arg)),
                ArgKind::Integer => cmd.push(self.gen_integer_for(arg)),
                ArgKind::Double => cmd.push(self.gen_double_for(spec, arg)),
                ArgKind::Key => cmd.push(self.gen_key_for(spec, pool)),
                ArgKind::Pattern => cmd.push(self.gen_pattern()),
                ArgKind::UnixTime => cmd.push(self.gen_unix_time()),
                ArgKind::PureToken => {}
                ArgKind::Oneof => {
                    if !arg.arguments.is_empty() {
                        let choice =
                            self.rand.below(NonZeroUsize::new(arg.arguments.len()).unwrap());
                        self.generate_argument(spec, &arg.arguments[choice], cmd, pool);
                    }
                }
                ArgKind::Block => {
                    for nested_arg in &arg.arguments {
                        self.generate_argument(spec, nested_arg, cmd, pool);
                    }
                }
                ArgKind::Function | ArgKind::Array | ArgKind::Object | ArgKind::Null | ArgKind::Number => {
                    cmd.push(self.gen_string());
                }
            }
        }
    }

    // For search commands, map well-known argument names to seed-created
    // resources so the fuzzer hits real code paths most of the time
    // (without making it impossible to ever target an unknown name).
    fn search_arg_override(&mut self, arg: &ArgSpec) -> Option<String> {
        let name = arg.name.to_ascii_lowercase();
        let hit = self.flip(0.85);
        if !hit {
            return None;
        }
        match name.as_str() {
            "index" | "index_name" => Some("idx".to_string()),
            "dict" | "dictionary" => Some("dict".to_string()),
            "key" if matches!(arg.kind, ArgKind::Key | ArgKind::String) => {
                let n = 1 + (self.rand.below(NonZeroUsize::new(2).unwrap()) as u32);
                Some(format!("doc:{}", n))
            }
            "alias" => Some("idx".to_string()),
            "synonym_group_id" | "group_id" => Some("g1".to_string()),
            "field" | "field_name" | "load_field" | "groupby_field" | "sortby_field" => {
                const FIELDS: &[&str] = &["title", "body", "n", "t", "loc", "v"];
                let i = self.rand.below(NonZeroUsize::new(FIELDS.len()).unwrap()) as usize;
                Some(FIELDS[i].to_string())
            }
            "query" => {
                const Q: &[&str] = &[
                    "*",
                    "hello",
                    "@title:hello",
                    "@body:demo",
                    "@n:[0 100]",
                    "@t:{tag1}",
                    "hello | world",
                    "(@title:foo) (@n:[0 50])",
                    "@v:[VECTOR_RANGE 0.5 $vec]",
                ];
                let i = self.rand.below(NonZeroUsize::new(Q.len()).unwrap()) as usize;
                Some(Q[i].to_string())
            }
            "prefix" => {
                if self.flip(0.5) {
                    Some("doc:".to_string())
                } else {
                    Some(self.gen_string())
                }
            }
            _ => None,
        }
    }

    fn gen_string_for(&mut self, spec: &CommandSpec, _arg: &ArgSpec) -> String {
        // Search commands benefit from occasionally seeing field names and
        // tokens drawn from the seeded vocabulary; otherwise stay random.
        if spec.module == "search" && self.flip(0.20) {
            const VOCAB: &[&str] = &[
                "hello", "world", "demo", "document", "foo", "bar", "baz",
                "title", "body", "n", "t", "loc", "v",
                "tag1", "tag2", "blue", "red",
                "TEXT", "TAG", "NUMERIC", "GEO", "VECTOR", "SORTABLE", "NOSTEM",
                "ON", "HASH", "JSON", "PREFIX", "SCHEMA", "LANGUAGE", "SCORE",
                "english", "french", "german",
            ];
            let i = self.rand.below(NonZeroUsize::new(VOCAB.len()).unwrap()) as usize;
            return VOCAB[i].to_string();
        }
        self.gen_string()
    }

    fn gen_key_for(&mut self, spec: &CommandSpec, pool: &mut KeyPool) -> String {
        if spec.module == "search" && self.flip(0.75) {
            // Bias toward the seeded doc keys / index name.
            const KEYS: &[&str] = &["doc:1", "doc:2", "doc:3", "idx", "sug", "dict"];
            let i = self.rand.below(NonZeroUsize::new(KEYS.len()).unwrap()) as usize;
            return KEYS[i].to_string();
        }
        self.gen_key(pool)
    }

    fn emit_preconditions(
        &mut self,
        spec: &CommandSpec,
        seq: &mut Vec<CommandInput>,
        pool: &mut KeyPool,
    ) {
        let r = CONFIG.preconditions.members;

        match spec.name.as_str() {
            // lists
            "LSET" | "LINDEX" | "LREM" | "LPOP" | "RPOP" | "LINSERT" | "BLPOP" | "BRPOP" => {
                let key = pool.ensure_key("list");
                let n = self.range_u32(r.list);
                let mut argv = vec!["LPUSH".to_string(), key.clone()];
                for _ in 0..n {
                    argv.push(self.gen_string());
                }
                seq.push(CommandInput { op: op_for("LPUSH"), argv });
            }
            // sets
            "SPOP" | "SRANDMEMBER" | "SREM" | "SISMEMBER" | "SMOVE" | "SMISMEMBER" => {
                let key = pool.ensure_key("set");
                let n = self.range_u32(r.set);
                let mut argv = vec!["SADD".to_string(), key.clone()];
                for _ in 0..n {
                    argv.push(self.gen_string());
                }
                seq.push(CommandInput { op: op_for("SADD"), argv });
            }
            // zsets
            "ZPOPMIN" | "ZPOPMAX" | "BZPOPMIN" | "BZPOPMAX" | "ZCARD" | "ZINCRBY" | "ZREM"
            | "ZRANGE" | "ZREVRANGE" | "ZMSCORE" | "ZRANK" | "ZREVRANK" => {
                let key = pool.ensure_key("zset");
                let n = self.range_u32(r.zset);
                let mut argv = vec!["ZADD".to_string(), key.clone()];
                for _ in 0..n {
                    argv.push(self.gen_double()); // score
                    argv.push(self.gen_string()); // member
                }
                seq.push(CommandInput { op: op_for("ZADD"), argv });
            }
            // hashes
            "HGET" | "HSET" | "HDEL" | "HINCRBY" | "HGETALL" | "HVALS" | "HKEYS" => {
                let key = pool.ensure_key("hash");
                let n = self.range_u32(r.hash);
                let mut argv = vec!["HSET".to_string(), key.clone()];
                for _ in 0..n {
                    argv.push(self.gen_string()); // field
                    argv.push(self.gen_string()); // value
                }
                seq.push(CommandInput { op: op_for("HSET"), argv });
            }
            // streams
            "XREAD" | "XREVRANGE" | "XRANGE" | "XDEL" | "XTRIM" => {
                let key = pool.ensure_key("stream");
                let n = self.range_u32(r.stream);
                for _ in 0..n {
                    let mut argv =
                        vec!["XADD".to_string(), key.clone(), "*".to_string(), "f".to_string()];
                    argv.push(self.gen_string());
                    seq.push(CommandInput { op: op_for("XADD"), argv });
                }
            }
            _ => {}
        }
    }

    fn gen_string(&mut self) -> String {
        let max = CONFIG.mutation.string.max_len.max(1);
        let len = 1 + self.rand.below(NonZeroUsize::new(max).unwrap());
        let special = self.flip(CONFIG.mutation.string.special_prob);
        let mut s = String::with_capacity(len);
        for _ in 0..len {
            let ch = if special && self.flip(0.25) {
                const PUNCT: &[u8] = b"*?[]{}()^$|.+-_,;:/@!%~#=";
                let i = self.rand.below(NonZeroUsize::new(PUNCT.len()).unwrap()) as usize;
                PUNCT[i] as char
            } else {
                let choice = self.rand.below(NonZeroUsize::new(4).unwrap());
                match choice {
                    0 => (b'a' + self.rand.below(NonZeroUsize::new(26).unwrap()) as u8) as char,
                    1 => (b'A' + self.rand.below(NonZeroUsize::new(26).unwrap()) as u8) as char,
                    2 => (b'0' + self.rand.below(NonZeroUsize::new(10).unwrap()) as u8) as char,
                    _ => b'_' as char,
                }
            };
            s.push(ch);
        }
        s
    }

    fn gen_integer_for(&mut self, arg: &ArgSpec) -> String {
        const INTERESTING: &[i64] = &[
            -9223372036854775808, -2147483648, -65536, -32768, -1024, -512, -256, -128, -64, -32,
            -16, -8, -7, -6, -5, -4, -3, -2, -1, 0, 1, 2, 3, 4, 5, 6, 7, 8, 15, 16, 31, 32, 63, 64,
            127, 128, 255, 256, 511, 512, 1023, 1024, 32767, 32768, 65535, 65536, 2147483647,
            9223372036854775807,
        ];
        if self.flip(0.6) {
            let idx = self.rand.below(NonZeroUsize::new(INTERESTING.len()).unwrap()) as usize;
            return INTERESTING[idx].to_string();
        }
        let v = self.rand.below(NonZeroUsize::new(20000).unwrap()) as i64 - 10000;
        let name = arg.name.to_ascii_uppercase();
        if name == "COUNT" || name == "LIMIT" {
            let cap = if name == "COUNT" {
                CONFIG.arg_caps.COUNT
            } else {
                CONFIG.arg_caps.LIMIT
            };
            let vv = ((v.abs() as u64) % (cap.max(1))) as u64 + 1;
            return vv.to_string();
        }
        v.to_string()
    }

    fn gen_double_for(&mut self, spec: &CommandSpec, arg: &ArgSpec) -> String {
        let upper_name = arg.name.to_ascii_uppercase();
        /*if spec.is_blocking() && (upper_name == "TIMEOUT" || upper_name.ends_with("TIMEOUT")) {
            let min = CONFIG.blocking.min_timeout_sec;
            let max = CONFIG.blocking.max_timeout_sec.max(min);
            let r = min + self.rand_biased_f64() * (max - min);
            return format!("{:.6}", r);
        }*/
        if self.flip(0.55) {
            return self.gen_integer_for(arg);
        }
        if self.flip(0.05) {
            return if self.flip(0.5) { "inf" } else { "nan" }.to_string();
        }
        let sign = if self.flip(0.5) { "-" } else { "" };
        let int = self.rand.below(NonZeroUsize::new(1_000_000).unwrap());
        let frac = self.rand.below(NonZeroUsize::new(1_000_000).unwrap());
        format!("{}{}.{}", sign, int, frac)
    }

    fn gen_double(&mut self) -> String {
        self.gen_double_for(
            &CommandSpec {
                op: -1,
                module: "core".to_string(),
                name: String::new(),
                arguments: vec![],
                group: String::new(),
                command_flags: vec![],
                arity: 0,
            },
            &ArgSpec {
                name: String::new(),
                token: None,
                kind: ArgKind::Double,
                optional: false,
                multiple: false,
                arguments: vec![],
                key_spec_index: None,
            },
        )
    }

    fn gen_key(&mut self, pool: &mut KeyPool) -> String {
        if self.flip(0.6) {
            if let Some(k) = pool.any_key() {
                return k;
            }
        }
        pool.new_key()
    }

    fn gen_pattern(&mut self) -> String {
        const PRESETS: &[&str] = &["*", "key:*", "*:value", "user:*:data", "cache:*", "a*b?c*"];
        if self.flip(0.5) {
            let idx = self.rand.below(NonZeroUsize::new(PRESETS.len()).unwrap()) as usize;
            PRESETS[idx].to_string()
        } else {
            let parts = 1 + self.rand.below(NonZeroUsize::new(6).unwrap());
            let mut s = String::new();
            for i in 0..parts {
                if i != 0 {
                    s.push(':');
                }
                if self.flip(0.5) {
                    s.push('*');
                } else if self.flip(0.3) {
                    s.push('?');
                } else {
                    s.push_str(&self.gen_string());
                }
            }
            s
        }
    }

    fn gen_unix_time(&mut self) -> String {
        (self.rand.below(NonZeroUsize::new(1_000_000_000).unwrap()) as u64).to_string()
    }

    #[inline]
    fn range_u32(&mut self, r: (u32, u32)) -> u32 {
        let (lo, hi) = r;
        if hi <= lo {
            return lo;
        }
        lo + (self.rand.below(NonZeroUsize::new((hi - lo + 1) as usize).unwrap()) as u32)
    }

    #[inline]
    fn flip(&mut self, p: f64) -> bool {
        self.rand_biased_f64() < p
    }

    #[inline]
    fn rand_biased_f64(&mut self) -> f64 {
        let a = self.rand.next() >> 11;
        (a as f64) * (1.0 / ((1u64 << 53) as f64))
    }
}

#[derive(Default)]
pub struct KeyPool {
    counter: u64,
    all: Vec<String>,
}

impl KeyPool {
    fn new_key(&mut self) -> String {
        self.counter += 1;
        let k = format!("k{}", self.counter);
        self.all.push(k.clone());
        k
    }
    fn any_key(&mut self) -> Option<String> {
        if self.all.is_empty() {
            None
        } else {
            Some(self.all[self.all.len() - 1].clone())
        }
    }
    fn ensure_key(&mut self, _typ: &str) -> String {
        if let Some(k) = self.any_key() {
            k
        } else {
            self.new_key()
        }
    }
}

unsafe extern "C" {
    fn query_command_table(idx: i32) -> *const c_char;
    fn core_command_register(op: i32);
    fn module_command_register(op: i32, name: *const c_char);
}

fn load_commands(core_dir: &str, default_modules: &[(&str, &str)]) -> io::Result<HashMap<String, Box<CommandSpec>>> {
    let mut op_map: HashMap<String, i32> = HashMap::new();
    let mut max_op = 0;
    loop {
        unsafe {
            let s = query_command_table(max_op);
            if s.is_null() {
                break;
            }
            op_map.insert(
                CStr::from_ptr(s).to_str().unwrap().to_string().to_uppercase(),
                max_op,
            );
            core_command_register(max_op);
            max_op += 1;
        }
    }
    FLUSHALL_OP.set(*op_map.get("FLUSHALL").unwrap()).ok();
    let mut map = HashMap::new();
    let mut process_raw = |raw: HashMap<String, Value>, module_name: &str| -> io::Result<()> {
        if !CONFIG.modules.contains(&module_name.to_string()) { return Ok(()); }
        for (name, val) in raw {
            // modules have container command names that have space in them like "FT.CONFIG HELP",
            // just ignore these for now
            if name.contains(' ') { continue; }
            if val.get("container").is_some() { continue; }
            if !CONFIG.whitelist.is_empty() && !CONFIG.whitelist.contains(&name) { continue; }
            if CONFIG.blacklist.contains(&name) { continue; }

            let arity = val.get("arity").and_then(|v| v.as_i64()).unwrap_or(0);
            let group = val.get("group").and_then(|v| v.as_str()).unwrap_or("").to_string();
            let arguments = match val.get("arguments") {
                Some(v) => serde_json::from_value::<Vec<ArgSpec>>(v.clone())?,
                _ => Vec::new(),
            };
            if arity.abs() > 1 && arguments.is_empty() {
                continue;
            }
            let flags_vec = val.get("command_flags").and_then(|v| v.as_array()).map(|arr| {
                arr.iter().filter_map(|x| x.as_str().map(|s| s.to_string())).collect::<Vec<String>>()
            }).unwrap_or_default();

            let op;
            if let Some(&core_op) = op_map.get(&name) {
                op = core_op;
            } else {
                op = max_op;
                unsafe {
                    module_command_register(op, CString::new(name.clone()).unwrap().as_ptr());
                }
                max_op += 1;
            }
            map.insert(name.clone(), Box::new(CommandSpec {
                name, op, arguments, group, command_flags: flags_vec, arity,
                module: module_name.to_string(),
            }));
        }
        Ok(())
    };
    for entry in fs::read_dir(core_dir)? {
        let entry = entry?;
        if !entry.file_type()?.is_file() {
            continue;
        }
        if entry.path().extension().and_then(|s| s.to_str()) != Some("json") {
            continue;
        }
        let mut buf = String::new();
        fs::File::open(entry.path())?.read_to_string(&mut buf)?;
        let raw: HashMap<String, Value> =
            serde_json::from_str(&buf).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        process_raw(raw, "core")?;
    }
    let mut modules: Vec<(String, String)> = Vec::new();
    if let Ok(envv) = std::env::var("REDIS_MODULES") {
        for item in envv.split(',').map(|s| s.trim()).filter(|s| !s.is_empty()) {
            if let Some((n, p)) = item.split_once('=') {
                modules.push((n.trim().to_string(), p.trim().to_string()));
            }
        }
    } else {
        modules.extend(default_modules.iter().map(|(n, p)| ((*n).to_string(), (*p).to_string())));
    }
    for (mname, mpath) in modules {
        let p = Path::new(&mpath);
        if !p.exists() { continue; }
        let buf = fs::read_to_string(p)?;
        let raw: HashMap<String, Value> =
            serde_json::from_str(&buf).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        process_raw(raw, &mname)?;
    }
    if map.is_empty() {
        panic!("config denies all commands");
    }
    Ok(map)
}

fn op_for(name: &str) -> i32 {
    COMMAND_SPECS
        .get(name)
        .map(|b| b.op)
        .unwrap_or_else(|| panic!("missing op for {}", name))
}
