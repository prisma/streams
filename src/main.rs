//! `streams-slate` binary — the composition root and nothing more:
//! allocator, tracing init, pre-runtime fail-closed environment checks,
//! tokio runtime construction, then `streams_slate::bootstrap::async_main`.
//! All server logic lives in the library crate (src/lib.rs).

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
    // R28: a certified survival deploy must fail at boot, not OOM at
    // +28 min, if any memory knob was dropped or overridden.
    streams_slate::bootstrap::assert_certified_memprofile();
    // R28: SWEEP_MAINT_RESIDENT=0 would silently starve every cold
    // debt class (the rotation would open and immediately close each
    // indebted engine, so no absorber lives long enough to drain).
    if std::env::var("SWEEP_MAINT_RESIDENT")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        == Some(0)
    {
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
    tracing::info!(
        model = %streams_slate::quota::pressure_model_json(),
        "project memory-pressure model (round-13; weights are code-versioned)"
    );
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(workers)
        .enable_all()
        .build()?
        .block_on(streams_slate::bootstrap::async_main())
}
