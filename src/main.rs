//! `streams-slate` binary — the composition root and nothing more:
//! allocator, tracing init, ONE configuration load, tokio runtime
//! construction, then `streams_slate::run(config)`. All server logic
//! lives in the library crate (src/lib.rs).

// musl's allocator fragments badly under this workload (docker phase 1:
// RSS 2x the accounted budgets); mimalloc keeps RSS near actual live set.
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info,slatedb=warn".into()),
        )
        .init();

    // WP-01 PR 3.1: the process environment is parsed ONCE, here at the
    // composition root, into an owned ServerConfig. Nothing reads the
    // environment at runtime.
    use clap::Parser;
    let cli = streams_slate::CliArgs::parse();
    let config = streams_slate::ServerConfig::load(cli, &streams_slate::ProcessEnvironment);

    // R28: SWEEP_MAINT_RESIDENT=0 would silently starve every cold
    // debt class (the rotation would open and immediately close each
    // indebted engine, so no absorber lives long enough to drain). The
    // config stores the raw value; the billing adapter floors at use.
    if config.billing.sweep_maint_resident == 0 {
        eprintln!(
            "Error: SWEEP_MAINT_RESIDENT=0 starves all cold-debt drain; \
             set >= 1 or unset (default 2)"
        );
        std::process::exit(1);
    }
    // Run 13: tokio timer drift of ~230 ms p50 (vs 4 ms for a raw thread)
    // proved the event loop is starved by inline blocking work. On a 1-vCPU
    // box #[tokio::main] means ONE worker — a single blocking poll freezes
    // every future, including durable-watermark acks (O14a). A worker floor
    // of 2+ lets the OS timeslice around a blocked worker.
    let workers: usize = std::env::var("TOKIO_WORKERS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or_else(|| {
            std::thread::available_parallelism()
                .map(|n| n.get())
                .unwrap_or(1)
        })
        .max(2);
    tracing::info!("tokio runtime: {workers} worker threads");
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(workers)
        .enable_all()
        .build()?
        .block_on(streams_slate::run(config))
}
