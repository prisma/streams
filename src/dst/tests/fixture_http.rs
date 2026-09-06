//! Fixture http.

use super::fixture_failpoints::FailpointGuard;
use super::fixture_requests::{PRISMA_KEY, hreq, preq};
use super::fixture_runtime::RigRuntime;
use super::fixture_storage::mem;
use object_store::ObjectStore;
use std::sync::Arc;

/// The FOCUSED rig knobs a test may turn. The runtime (process
/// incarnation + clock) is deliberately NOT one of them: it is a
/// separate required capability of [`http_rig_build`], so every
/// restart / multi-instance call site names its incarnation.
pub(super) struct HttpRigOptions {
    /// PR 6.1-D: this rig's OWN fleet coordination store, if any.
    pub(super) fleet_store: Option<Arc<dyn ObjectStore>>,
    pub(super) prefixes: Vec<String>,
    pub(super) shard: crate::shard::ShardConfig,
    pub(super) per_segment_slots: i64,
    pub(super) max_request_body_bytes: Option<usize>,
    /// Static account bearer (the negative authorization matrix).
    pub(super) auth: Option<String>,
    /// A NAMED instance makes ring ownership real: setting
    /// `ownership.set_ring_active` afterward makes rendezvous routing live, and
    /// shards the ring assigns elsewhere answer 409 + Streams-Replay-To.
    pub(super) instance: Option<String>,
    /// The shard OPENER parks on this lock right before maintenance
    /// restoration (R26-5): the test holds the lock, fires a request,
    /// proves nothing is answered from unrestored state, then releases.
    pub(super) open_park: Option<Arc<tokio::sync::Mutex<()>>>,
    pub(super) absorber: Option<crate::history::AbsorberConfig>,
    /// An explicit auth service (MT Stage 5 shadow tests).
    pub(super) auth_service: Option<Arc<crate::auth::AuthService>>,
    /// (static fleet token, workload token source); None = the default
    /// static-bridge rig posture.
    pub(super) fleet_auth: Option<(Option<String>, Option<crate::peer::FleetTokenSource>)>,
}

impl Default for HttpRigOptions {
    fn default() -> Self {
        Self {
            fleet_store: None,
            prefixes: vec!["00".to_string()],
            shard: crate::shard::ShardConfig::default(),
            per_segment_slots: 0,
            max_request_body_bytes: None,
            auth: None,
            instance: None,
            open_park: None,
            absorber: None,
            auth_service: None,
            fleet_auth: None,
        }
    }
}

/// Absorbers that are NEVER DUE (huge byte + age thresholds): durable
/// maintenance backlog stays put, which is what the R27-2 sweep-policy
/// tests need — the subject is the sweep's retention decision, not
/// absorber timing.
pub(super) fn cold_absorber() -> crate::history::AbsorberConfig {
    crate::history::AbsorberConfig {
        threshold_bytes: u64::MAX,
        threshold_age: std::time::Duration::from_secs(1_000_000),
        tick: std::time::Duration::from_millis(50),
        sweep_every: u32::MAX,
        ..Default::default()
    }
}

/// A running rig: the composition root, its loopback address, and the
/// manual clock the test drives (the SAME clock the runtime reads —
/// proven by `rig_clock_is_the_runtime_clock`).
pub(super) struct HttpRig {
    pub(super) state: Arc<crate::http::AppState>,
    pub(super) addr: std::net::SocketAddr,
    pub(super) clock: crate::runtime::ManualClock,
    /// PR 6-F: the simulated process's supervisor — a restart test
    /// terminates the old SERVER SURFACE and its supervised runtime
    /// loops through it before the replacement starts (engine-internal
    /// and open-gate helper tasks join the supervisor with WP-15).
    pub(super) tasks: crate::tasks::TaskSupervisor,
}

impl HttpRig {
    pub(super) fn parts(self) -> (Arc<crate::http::AppState>, std::net::SocketAddr) {
        (self.state, self.addr)
    }
}

/// Full-fidelity HTTP rig: real AppState + axum server on a loopback
/// port, one shard prefix, fast absorber. The gap tests need the exact
/// header behavior clients see, not engine-level approximations.
pub(super) async fn http_rig(
    store: Arc<dyn ObjectStore>,
) -> (Arc<crate::http::AppState>, std::net::SocketAddr) {
    http_rig_at(store, RigRuntime::first()).await
}

/// A rig under an EXPLICIT process incarnation: the second cold server
/// over the same store, a restart phase, a peer instance.
pub(super) async fn http_rig_at(
    store: Arc<dyn ObjectStore>,
    runtime: RigRuntime,
) -> (Arc<crate::http::AppState>, std::net::SocketAddr) {
    http_rig_build(store, runtime, HttpRigOptions::default())
        .await
        .parts()
}

/// http_rig with explicit shard prefixes (multi-engine capacity tests)
/// and a shard config (e.g. serial WAL for deterministic throughput).
pub(super) async fn http_rig_opts(
    store: Arc<dyn ObjectStore>,
    prefixes: Vec<String>,
    shard_cfg: crate::shard::ShardConfig,
) -> (Arc<crate::http::AppState>, std::net::SocketAddr) {
    http_rig_full(store, prefixes, shard_cfg, 0).await
}

pub(super) async fn http_rig_full(
    store: Arc<dyn ObjectStore>,
    prefixes: Vec<String>,
    shard_cfg: crate::shard::ShardConfig,
    per_segment_slots: i64,
) -> (Arc<crate::http::AppState>, std::net::SocketAddr) {
    http_rig_build(
        store,
        RigRuntime::first(),
        HttpRigOptions {
            prefixes,
            shard: shard_cfg,
            per_segment_slots,
            ..Default::default()
        },
    )
    .await
    .parts()
}

/// A rig with a NAMED instance so ring ownership is real (the first
/// incarnation; a peer instance names its own via `http_rig_named_at`).
pub(super) async fn http_rig_named(
    store: Arc<dyn ObjectStore>,
    instance: &str,
) -> (Arc<crate::http::AppState>, std::net::SocketAddr) {
    http_rig_named_at(store, instance, RigRuntime::first()).await
}

pub(super) async fn http_rig_named_at(
    store: Arc<dyn ObjectStore>,
    instance: &str,
    runtime: RigRuntime,
) -> (Arc<crate::http::AppState>, std::net::SocketAddr) {
    http_rig_build(
        store,
        runtime,
        HttpRigOptions {
            instance: Some(instance.to_string()),
            ..Default::default()
        },
    )
    .await
    .parts()
}

/// A rig whose absorbers are never due (see [`cold_absorber`]).
pub(super) async fn http_rig_cold_absorb(
    store: Arc<dyn ObjectStore>,
    prefixes: Vec<String>,
) -> (Arc<crate::http::AppState>, std::net::SocketAddr) {
    let rig = http_rig_build(
        store,
        RigRuntime::first(),
        HttpRigOptions {
            prefixes,
            absorber: Some(cold_absorber()),
            ..Default::default()
        },
    )
    .await;
    // Recovery deliberately backdates discovered tails, so a large age
    // threshold alone cannot promise "never due" during startup races.
    // Pause this rig's owned absorber resource before callers create debt.
    rig.state
        .runtime
        .history
        .paused
        .store(true, std::sync::atomic::Ordering::Relaxed);
    rig.parts()
}

/// A rig with an explicit auth service (MT Stage 5 shadow tests).
pub(super) async fn http_rig_with_auth_service(
    store: Arc<dyn ObjectStore>,
    svc: Arc<crate::auth::AuthService>,
) -> (Arc<crate::http::AppState>, std::net::SocketAddr) {
    http_rig_build(
        store,
        RigRuntime::first(),
        HttpRigOptions {
            auth_service: Some(svc),
            ..Default::default()
        },
    )
    .await
    .parts()
}

/// The rig's shard opener: the REAL production open order — db build →
/// load_or_rebuild_maintenance → engine start → absorber — with the
/// R26-5 park point before restoration and the fast test absorber
/// defaults (a caller's absorber config, e.g. `cold_absorber`, wins).
/// PR 6.1.1-B: the principal rig opens engines the way production does
/// — including the close notifier, so the whole suite exercises the real
/// directory lifecycle (a fenced or failed engine evicts itself and arms
/// the anti-flap holdoff) instead of a rig-only shape with no close
/// callback at all.
pub(super) fn rig_opener(
    store: Arc<dyn ObjectStore>,
    keys: Arc<crate::history::KeyCache>,
    shard_cfg: crate::shard::ShardConfig,
    open_park: Option<Arc<tokio::sync::Mutex<()>>>,
    absorber_cfg: Option<crate::history::AbsorberConfig>,
    notifier: crate::shard_directory::ShardCloseNotifier,
) -> crate::sharddir::OpenFn {
    Box::new(
        move |prefix: String, incarnation: crate::sharddir::EngineIncarnation| {
            let notifier = notifier.clone();
            let store = store.clone();
            let keys = keys.clone();
            let shard_cfg = shard_cfg.clone();
            let open_park = open_park.clone();
            let absorber_cfg = absorber_cfg.clone();
            let fut: futures_util::future::BoxFuture<
                'static,
                anyhow::Result<Arc<crate::shard::ShardEngine>>,
            > = Box::pin(async move {
                let db = slatedb::Db::builder(format!("{prefix}/shard"), store.clone())
                    .with_settings(slatedb::config::Settings {
                        flush_interval: Some(std::time::Duration::from_millis(5)),
                        manifest_poll_interval: std::time::Duration::from_millis(50),
                        ..Default::default()
                    })
                    .build()
                    .await?;
                let (absorb_tx, absorb_rx) = crate::history::absorber_channel();
                // R26-5 park point: BEFORE restoration, exactly where a
                // slow durable-state load sits in production.
                if let Some(park) = &open_park {
                    let _p = park.lock().await;
                }
                // R25-A: tests use the REAL load path — a fresh DB rebuilds to
                // zero; a reopened DB restores its durable backlog, exactly as
                // the production opener does.
                let __maint = crate::shard::load_or_rebuild_maintenance(&db)
                    .await
                    .expect("load maintenance");
                let on_close = {
                    let notifier = notifier.clone();
                    let prefix = prefix.clone();
                    Arc::new(move || {
                        notifier.closed(&prefix, incarnation);
                    }) as Arc<dyn Fn() + Send + Sync>
                };
                let engine = crate::shard::ShardEngine::start(
                    prefix,
                    Arc::new(db),
                    store.clone(),
                    shard_cfg.clone(),
                    absorb_tx,
                    Some(on_close),
                    __maint,
                );
                crate::history::Absorber::start(
                    store,
                    engine.clone(),
                    keys,
                    absorber_cfg
                        .clone()
                        .unwrap_or(crate::history::AbsorberConfig {
                            threshold_bytes: 1,
                            threshold_age: std::time::Duration::from_millis(1),
                            tick: std::time::Duration::from_millis(20),
                            sweep_every: u32::MAX,
                            ..Default::default()
                        }),
                    absorb_rx,
                );
                Ok(engine)
            });
            fut
        },
    )
}

/// Build a rig from ONE process runtime and the focused options.
pub(super) async fn http_rig_build(
    store: Arc<dyn ObjectStore>,
    runtime: RigRuntime,
    opts: HttpRigOptions,
) -> HttpRig {
    let HttpRigOptions {
        fleet_store,
        prefixes,
        shard: shard_cfg,
        per_segment_slots,
        max_request_body_bytes,
        auth,
        instance: instance_name,
        open_park,
        absorber: absorber_cfg,
        auth_service,
        fleet_auth,
    } = opts;
    let registry = crate::registry::Registry::new(
        store.clone(),
        &crate::tenant::CellId::new("test-cell").unwrap(),
    );
    let keys = Arc::new(crate::history::KeyCache::default());
    // PR 4.1: the principal rig NEVER selects OS entropy or the OS
    // clock — every migrated ambient input (boot id, stream/consumer
    // epochs, touch-journal epochs, trusted time) is under rig control,
    // with domain-separated seeded streams so concurrent draw order
    // cannot couple unrelated ids. PR 4.1.1: the process incarnation
    // is the caller's, never fixed here.
    let RigRuntime {
        caps: rig_runtime,
        clock,
        touch_entropy,
    } = runtime;
    let touch = Arc::new(crate::touch::TouchRegistry::with_entropy(touch_entropy));
    let opener_store = store.clone();
    let opener_keys = keys.clone();
    let opener_absorber = absorber_cfg.clone();
    // The rig's owned configuration (WP-01 PR 3.1): the no-environment
    // knob posture — every knob default, no env overlay. PR 4.1.1.1:
    // the CLI half comes from the HERMETIC fixture, never from a parse
    // — clap's `env = ...` bindings would otherwise read the ambient
    // process environment into the principal rig.
    // PR 6.1.2-B: the instance name reaches the CONFIG as well as the
    // ownership service. Production derives both from one
    // `config.cli.instance_name`; the rig used to name only ownership,
    // so a rig's config claimed to be "streams" whatever the test called
    // it — and any assembly reading the config disagreed with the
    // runtime it was assembling.
    let rig_config = Arc::new(crate::config::ServerConfig::load(
        {
            let mut cli = crate::config::CliArgs::deterministic();
            if let Some(limit) = max_request_body_bytes {
                cli.max_request_body_bytes = limit;
            }
            if let Some(name) = instance_name.clone() {
                cli.instance_name = name;
            }
            cli
        },
        &crate::config::MapEnvironment::empty(),
    ));
    let mut rig_runtime = rig_runtime.with_config(&rig_config);
    let protocol_clock: Arc<dyn crate::runtime::Clock> =
        Arc::new(crate::runtime::SystemClock::default());
    // Wire deadlines and rate refill follow real transport time in HTTP rigs.
    // Tests that drive time explicitly may supply the shared usage capability.
    rig_runtime.usage = shard_cfg.shared_usage.clone().unwrap_or_else(|| {
        Arc::new(crate::usage::UsageService::new(
            &rig_config.admission,
            protocol_clock.clone(),
        ))
    });
    let mut opener_shard_cfg = shard_cfg.clone();
    if let Some(shared) = &opener_shard_cfg.shared_history {
        rig_runtime.history = shared.clone();
    }
    if let Some(shared) = &opener_shard_cfg.shared_postings_cache {
        rig_runtime.postings = shared.clone();
    }
    opener_shard_cfg.shared_usage = Some(rig_runtime.usage.clone());
    opener_shard_cfg.shared_ops = Some(rig_runtime.ops.clone());
    opener_shard_cfg.shared_history = Some(rig_runtime.history.clone());
    opener_shard_cfg.shared_postings_cache = Some(rig_runtime.postings.clone());
    let ownership = crate::ownership::OwnershipService::new(instance_name.unwrap_or_default());
    let (fleet_static_token, fleet_token_source) = match fleet_auth {
        Some((t, s)) => (t, s),
        None => (Some("dst-internal-token".to_string()), None),
    };
    let peer = crate::peer::PeerClient::new(fleet_static_token, fleet_token_source);
    // Per-rig budget: isolated from every other rig in the process.
    let livefeed = crate::sse::service::LiveFeedService::from_config(&rig_config.sse);
    livefeed.set_heartbeat_ms(15_000);
    let bearer = crate::deployment_bearer::DeploymentBearer::new(auth, None);
    let tasks = crate::tasks::TaskSupervisor::new();
    let billing = crate::billing_service::BillingService::new(
        Some(PRISMA_KEY.to_string()),
        Arc::new(crate::billing::ReadUsageAccumulator::new(
            crate::billing::MeterSource {
                cell: "cell_test".to_string(),
                instance: "dst-instance".to_string(),
                boot: rig_runtime.identity.boot_id.clone(),
            },
        )),
    );
    let state = Arc::new(crate::http::AppState {
        runtime: rig_runtime.clone(),
        // These fixtures mint external JWTs, policies and watch capabilities
        // with real wall time; their seeded manual clock controls local tests.
        protocol_clock,
        config: rig_config.clone(),
        registry: Arc::new(registry),
        watches: std::sync::OnceLock::new(),
        reads: std::sync::OnceLock::new(),
        creations: std::sync::OnceLock::new(),
        peer,
        livefeed,
        bearer,
        billing,
        tasks: tasks.monitor(),
        rollup: crate::rollup::RollupSlot::default(),
        fleet: crate::fleet::FleetRepository::new(fleet_store.clone()),
        deployment: crate::deployment::DeploymentIdentity::new(
            crate::tenant::ProjectId::new("proj-test").unwrap(),
            "acct_test".to_string(),
            crate::tenant::CellId::new("cell_test").unwrap(),
            "test".to_string(),
        ),
        shards: crate::shard_directory::ShardDirectory::new(
            prefixes,
            ownership.clone(),
            crate::shard_directory::OpenTiming {
                open_deadline: rig_config.shard.open_deadline,
                open_wait: std::time::Duration::from_millis(rig_config.shard.open_wait_ms),
            },
            |notifier| {
                rig_opener(
                    opener_store,
                    opener_keys,
                    opener_shard_cfg,
                    open_park,
                    opener_absorber,
                    notifier,
                )
            },
        ),
        admission: crate::admission::AdmissionController::new(crate::admission::AdmissionKnobs {
            max_inflight: 0,
            per_stream_cap: per_segment_slots,
            rss_shed_mb: 0,
            project_memory_pressure_bytes: 0,
            project_memory_release_pct: 75,
            subscriptions: crate::admission::SubscriptionCapacity {
                effective: 0,
                configured: 0,
            },
            record_ceiling_bytes: 0,
        }),
        cert_sealed_publish_delay_ms: Arc::new(std::sync::atomic::AtomicU64::new(0)),
        ownership,
        data_store: store,
        keys,
        touch,
        origin_marker: "dst-instance".to_string(),
        auth: auth_service.unwrap_or_else(|| {
            std::sync::Arc::new(
                crate::auth::AuthService::new(
                    crate::auth::AuthMode::Off,
                    "https://auth.prisma.io".into(),
                    "test-cell",
                )
                .unwrap(),
            )
        }),
        quotas: crate::quota::QuotaRegistry::new(rig_runtime.ops.clone()),
        catalog_cursor_key: None,
    });
    let app = crate::http::router(state.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let serve_tasks = tasks.clone();
    let _ = tasks.spawn(
        "http",
        crate::tasks::Policy::Critical,
        move |_cancel| async move {
            // #269: rigs serve through the PRODUCTION h1 loop so the whole
            // suite exercises it (axum::serve here would leave the real
            // connection path tested only by out-of-tree probes). It observes
            // the supervisor's cancellation itself and owns its connections.
            crate::http::serve_h1(listener, app, 64 * 1024, serve_tasks)
                .await
                .ok();
            crate::tasks::TaskResult::Done
        },
    );
    HttpRig {
        state,
        addr,
        clock,
        tasks,
    }
}

/// Boot a rig, create + fill a stream, then drive a split INTO the
/// parked seal-gap: Phase A CAS'd, parent sealed, successors withheld.
/// Returns everything the gap assertions need.
pub(super) async fn rig_in_seal_gap(
    stream: &str,
    per_key: usize,
) -> (
    Arc<crate::http::AppState>,
    std::net::SocketAddr,
    FailpointGuard,
    tokio::task::JoinHandle<bool>,
) {
    let store = mem();
    let (state, addr) = http_rig(store).await;
    let (st, _, _) = hreq(
        addr,
        "PUT",
        &format!("/v1/stream/{stream}"),
        &[("content-type", "application/json")],
        b"",
    )
    .await;
    assert!(st == 200 || st == 201, "create {st}");
    for i in 0..per_key {
        for k in ["ga", "gb"] {
            let body = serde_json::json!({ "k": k, "n": i }).to_string();
            let (st, _, _) = preq(
                addr,
                "POST",
                &format!("/v1/streams/{stream}/records"),
                &[
                    ("prisma-encryption-key", PRISMA_KEY),
                    ("prisma-routing-key", k),
                ],
                body.as_bytes(),
            )
            .await;
            assert!(st == 200 || st == 204, "append {st}");
        }
    }
    crate::failpoints::arm_scaler_before_publish(stream);
    let guard = FailpointGuard(stream.to_string());
    let split = {
        let state = state.clone();
        let name = stream.to_string();
        tokio::spawn(async move {
            crate::scaler3::execute_split(
                &state,
                &state.deployment.raw_adapter_sref(&name),
                0,
                0x8000_0000_0000_0000,
            )
            .await
        })
    };
    // The gap is entered once the parent identity's engine handle is
    // CLOSED while the descriptor still shows one segment + pending.
    let desc = state
        .registry
        .get(&state.deployment.raw_adapter_sref(stream))
        .await
        .unwrap()
        .unwrap();
    let identity = desc.resolve_segment("").identity;
    // 30s, not 10: CI's loaded runners take multiples of a laptop's
    // wall time through this path; the deadline exists to fail fast on
    // a REAL wedge (which parks forever), not to race the scheduler
    // (seal_gap_get raced it at 10s on 2/3 CI runs, 2026-08-19).
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
    loop {
        let closed = match state
            .engine_for_scaler(
                &crate::crypto::RouteHash::for_stream(&state.deployment.raw_adapter_sref(stream)).0,
            )
            .await
        {
            Some(e) => match e.stream_handle(identity).await {
                Ok(h) => h.state.lock().unwrap().durable.closed,
                Err(_) => false,
            },
            None => false,
        };
        let d = state
            .registry
            .get(&state.deployment.raw_adapter_sref(stream))
            .await
            .unwrap()
            .unwrap();
        let pending = d
            .segments
            .as_ref()
            .map(|m| m.pending.is_some() && m.segments.len() == 1)
            .unwrap_or(false);
        if closed && pending {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "seal gap never entered (closed={closed} pending={pending})"
        );
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    (state, addr, guard, split)
}

/// Wait until the withheld publication lands after release.
pub(super) async fn await_published(state: &Arc<crate::http::AppState>, stream: &str) {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
    loop {
        state
            .registry
            .invalidate(&state.deployment.raw_adapter_sref(stream));
        let d = state
            .registry
            .get(&state.deployment.raw_adapter_sref(stream))
            .await
            .unwrap()
            .unwrap();
        let done = d
            .segments
            .as_ref()
            .map(|m| m.pending.is_none() && m.segments.len() > 1)
            .unwrap_or(false);
        if done {
            return;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "publication never completed after release"
        );
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
}

/// Close every engine this runtime is serving, through the REAL
/// retirement protocol — the only thing that removes a resident,
/// initiates close and stops the engine-owned loops.
///
/// PR 6.1.2-A: this used to take cloned `Arc`s and drop them, with a
/// comment claiming retirement had already closed them. That was untrue
/// at nearly every one of its ~170 call sites: dropping one clone of a
/// handle the directory still owns removes no resident, calls no
/// `begin_close` and terminates no loop, so every restart, snapshot and
/// quiescence boundary a test believed it had was imaginary.
///
/// The anti-flap holdoff the protocol arms is cleared afterwards, on
/// purpose: a test shutdown is a QUIESCENCE boundary for one instance,
/// not the possession yield the holdoff exists to damp. Production waits
/// it out; a restart test reopens the same storage deliberately.
pub(super) async fn engine_shutdown(state: &Arc<crate::http::AppState>) {
    for prefix in state.shards.held_prefixes() {
        match state.shards.retire(
            &prefix,
            crate::shard_directory::RetirementReason::Shutdown,
            |_, _| true,
        ) {
            crate::shard_directory::RetireOutcome::Retired(engine) => {
                assert!(
                    engine.is_closed(),
                    "retirement must initiate close of {prefix}"
                );
            }
            // A close callback evicted it between the listing and here.
            crate::shard_directory::RetireOutcome::Absent => {}
            crate::shard_directory::RetireOutcome::Kept => {
                panic!("an unconditional shutdown retirement was declined for {prefix}")
            }
        }
        state.shards.clear_holdoff(&prefix);
    }
    assert_eq!(
        state.shards.open_count(),
        0,
        "engine shutdown left resident shards"
    );
}

/// Two-instance rig: a full instance over the shared store with the
/// complete 2-bit shard prefix code, so segment routes split across
/// shards that can be owned by different instances.
pub(super) async fn http_rig_owner(
    store: Arc<dyn ObjectStore>,
    instance: &str,
) -> (Arc<crate::http::AppState>, std::net::SocketAddr) {
    http_rig_owner_at(store, instance, RigRuntime::first()).await
}

pub(super) async fn http_rig_owner_at(
    store: Arc<dyn ObjectStore>,
    instance: &str,
    runtime: RigRuntime,
) -> (Arc<crate::http::AppState>, std::net::SocketAddr) {
    http_rig_build(
        store,
        runtime,
        HttpRigOptions {
            prefixes: ["00", "01", "10", "11"]
                .iter()
                .map(|s| s.to_string())
                .collect(),
            instance: Some(instance.to_string()),
            ..Default::default()
        },
    )
    .await
    .parts()
}
