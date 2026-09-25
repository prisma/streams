//! Prints the configuration `streams-slate` would run with for the argv
//! given here and this process's environment, then whether validation
//! accepts it. Nothing is opened, spawned or logged. This is the HEAD half
//! of the old-vs-new effective-configuration comparison
//! (scripts/effective-config/), which runs it under `env -i` with one
//! deployment family's placeholder variables at a time.
//!
//! The verdict is `accepted`, `refused` (with every problem) or
//! `argv-refused` (clap's own message). Validation notices are not
//! reachable from the public facade; the comparison reads them from the
//! real binary's boot log instead.
use clap::Parser;

fn main() {
    let argv = std::iter::once(String::from("streams-slate")).chain(std::env::args().skip(1));
    let cli = match streams_slate::CliArgs::try_parse_from(argv) {
        Ok(cli) => cli,
        Err(refusal) => {
            println!("@@ verdict\nargv-refused\n{refusal}");
            return;
        }
    };
    let config = streams_slate::ServerConfig::load(cli, &streams_slate::ProcessEnvironment);
    println!("@@ root\n{config:#?}");
    match config.validate() {
        Ok(_) => println!("@@ verdict\naccepted"),
        Err(refusal) => println!("@@ verdict\nrefused\n{refusal}"),
    }
}
