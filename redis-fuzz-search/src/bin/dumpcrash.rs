use std::{env, fs};
use postcard;
use serde::{Serialize, Deserialize};

#[derive(Serialize, Deserialize)]
pub struct CommandInput {
    pub op: i32,
    pub argv: Vec<String>,
}

#[derive(Serialize, Deserialize)]
pub struct CommandInputSeq(pub Vec<CommandInput>);

fn main() -> anyhow::Result<()> {
    let path = env::args().nth(1).expect("usage: dumpcrash <file>");
    let buf = fs::read(&path)?;
    let seq: CommandInputSeq = postcard::from_bytes(&buf)?;
    for cmd in seq.0 {
        println!("{}", cmd.argv.join(" "));
    }
    Ok(())
}
