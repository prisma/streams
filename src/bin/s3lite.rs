//! s3lite: a minimal in-memory S3-compatible server with injected latency.
//!
//! Implements enough of the S3 REST API for both SlateDB (via the
//! `object_store` AWS client: conditional PUTs, range GETs, ListObjectsV2,
//! multipart uploads, batch delete) and the existing Prisma Streams R2 client
//! (plain PUT/GET/HEAD/DELETE/ListV2). Authorization headers are ignored.
//!
//! Every S3 operation sleeps `--latency-ms` (default 25) before executing to
//! emulate object-store round-trip latency. `GET /_s3lite/stats` (no latency)
//! reports op counts for PUT-amplification comparisons.
//!
//! This root owns only the command line; the emulator is `s3lite/emulator.rs`.

use std::time::Duration;

use clap::Parser;

// The emulator is test code in every test build: this binary's own tests
// and the provider contract suite, which compiles the same file.
#[cfg(not(test))]
#[path = "s3lite/emulator.rs"]
mod emulator;
#[cfg(test)]
#[path = "s3lite/emulator.rs"]
mod emulator;

#[derive(Parser, Debug)]
#[command(name = "s3lite")]
struct Args {
    #[arg(long, default_value = "127.0.0.1:9500")]
    listen: String,
    /// Injected latency per S3 operation, in milliseconds.
    #[arg(long, default_value_t = 25)]
    latency_ms: u64,
    /// Bench mode: PUTs whose key contains this substring AND ends in
    /// ".sst" are acknowledged but their bodies dropped (metadata kept,
    /// GET returns 500). Lets an in-memory emulator absorb an unbounded
    /// history tier during sustained runs. Never used in correctness tests.
    #[arg(long)]
    discard_substr: Option<String>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let app = emulator::router(emulator::AppState::new(
        Duration::from_millis(args.latency_ms),
        args.discard_substr,
    ));
    let listener = tokio::net::TcpListener::bind(&args.listen).await?;
    eprintln!(
        "s3lite listening on {} (latency {}ms per op)",
        args.listen, args.latency_ms
    );
    axum::serve(listener, app).await?;
    Ok(())
}
