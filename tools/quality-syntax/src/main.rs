//! Rust syntax facts for the Python quality gates; no service dependencies.
mod facts;
mod imports;
mod scan;

use serde::Deserialize;
use std::error::Error;

#[derive(Deserialize)]
struct Input {
    path: String,
    source: String,
}

fn main() -> Result<(), Box<dyn Error>> {
    let inputs: Vec<Input> = serde_json::from_reader(std::io::stdin().lock())?;
    let mut output = Vec::with_capacity(inputs.len());
    for input in inputs {
        output.push(scan::source(&input.path, &input.source)?);
    }
    serde_json::to_writer(std::io::stdout().lock(), &output)?;
    Ok(())
}
