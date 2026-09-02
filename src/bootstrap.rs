//! Binary bootstrap: store opening, validation, service construction and
//! task startup (WP-01/PR 2: moved out of src/main.rs; PR 3.1: takes the
//! owned [`crate::config::ServerConfig`]). The binary calls exactly one
//! entry point: [`run`].

use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use object_store::aws::{AmazonS3, AmazonS3Builder, S3ConditionalPut};
use object_store::{ObjectStore, ObjectStoreExt};
use slatedb::Db;

use crate::config::validation::ValidatedServerConfig;
use crate::config::validation::{resolve_effective_capacity, shard_settings};
use crate::history::{Absorber, AbsorberConfig, KeyCache, absorber_channel};
use crate::http::AppState;
use crate::registry::{Registry, load_or_init_topology};
use crate::shard::{ShardConfig, ShardEngine};

impl crate::config::ServerConfig {
    fn raw_store(&self, bucket: &Option<String>) -> anyhow::Result<AmazonS3> {
        let bucket = bucket.as_deref().unwrap_or(&self.cli.bucket);
        AmazonS3Builder::new()
            .with_endpoint(&self.cli.s3_endpoint)
            .with_bucket_name(bucket)
            .with_region(&self.cli.region)
            .with_access_key_id(&self.cli.access_key_id)
            .with_secret_access_key(&self.cli.secret_access_key)
            .with_allow_http(true)
            .with_conditional_put(S3ConditionalPut::ETagMatch)
            // Idle pooled connections die silently across scale-to-zero
            // snapshot/restore; expiring them just under the platform's 5 s
            // idle threshold means a restored image wakes with an empty
            // pool instead of dead sockets (EXPERIMENT-PILOT.md). The pool
            // is shared by every shard/stream on the instance, and manifest
            // polling keeps it warm whenever any shard is open — the cold
            // path only bites fully-idle instances. POOL_IDLE_SECS exists
            // so production fleets can lift this once the platform stops
            // killing idle flows (2026-07 plan); until then keep <5.
            .with_client_options(
                object_store::ClientOptions::new()
                    .with_allow_http(true) // ClientOptions REPLACES the builder's allow_http
                    .with_pool_idle_timeout(Duration::from_secs(self.storage.pool_idle_secs)),
            )
            // Records Tigris's Server-Timing (their internal ms) and
            // x-tigris-served-from per response → sp50/sp99 + served_from
            // in /v1/debug/store. wall − server = network path.
            .with_http_connector(crate::store_timing::SniffConnector)
            .build()
            .context("build s3 object store")
    }

    // TimingStore sits beneath PrefixStore so it times final, fully-prefixed
    // paths (O14a split: our pipeline vs egress path vs Tigris). All stores
    // share one global gauge — the egress budget is per instance.
    fn store_for(&self, bucket: &Option<String>) -> anyhow::Result<Arc<dyn ObjectStore>> {
        let s3 = crate::store_timing::TimingStore::new(self.raw_store(bucket)?);
        Ok(match &self.cli.path_prefix {
            Some(p) => Arc::new(object_store::prefix::PrefixStore::new(s3, p.as_str())),
            None => Arc::new(s3),
        })
    }

    /// Fleet-coordination store (heartbeats, desired.json): shared across
    /// instances, so prefixed by --fleet-prefix, not --path-prefix.
    fn fleet_store(&self) -> anyhow::Result<Option<Arc<dyn ObjectStore>>> {
        let Some(p) = &self.cli.fleet_prefix else {
            return Ok(None);
        };
        let s3 = crate::store_timing::TimingStore::new(self.raw_store(&None)?);
        Ok(Some(Arc::new(object_store::prefix::PrefixStore::new(
            s3,
            p.as_str(),
        ))))
    }
}

static SLATEDB_RT_THREADS: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(2);

/// Composition-root call, before the first SlateDB opens: size the
/// dedicated runtime from the process configuration. Tests never call
/// this and get the default (2), matching the old env-unset default.
pub fn init_slatedb_runtime_threads(threads: usize) {
    SLATEDB_RT_THREADS.store(threads, std::sync::atomic::Ordering::Relaxed);
}

pub fn slatedb_runtime() -> &'static tokio::runtime::Runtime {
    static RT: std::sync::OnceLock<tokio::runtime::Runtime> = std::sync::OnceLock::new();
    RT.get_or_init(|| {
        let threads = SLATEDB_RT_THREADS.load(std::sync::atomic::Ordering::Relaxed);
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(threads)
            .thread_name("slatedb-rt")
            .enable_all()
            .build()
            .expect("build slatedb runtime")
    })
}

/// Run `fut` to completion on the SlateDB runtime. Used for every
/// `Db::builder(...).build()` / `DbReader` open so all slatedb-internal
/// tasks land on `slatedb_runtime()`'s threads.
pub async fn on_slatedb_rt<F>(fut: F) -> F::Output
where
    F: std::future::Future + Send + 'static,
    F::Output: Send + 'static,
{
    let (tx, rx) = tokio::sync::oneshot::channel();
    slatedb_runtime().spawn(async move {
        let _ = tx.send(fut.await);
    });
    rx.await.expect("slatedb-rt task dropped")
}

/// The server bootstrap: the composition root hands in ONE owned,
/// PROVEN [`ValidatedServerConfig`] (PR 3.2: validation is complete
/// before this function runs — the type is the evidence); this function
/// runs the OS preflight, then constructs stores, builds the runtime
/// owners and serves. Called from the binary's `run` facade; tests
/// drive owners directly, not this.
pub async fn run(validated: ValidatedServerConfig) -> anyhow::Result<()> {
    // Transitional posture (WP-01 PR 3.2, pending WP-02): several
    // subsystems still read process-global init-once holders (absorb
    // budget/pause, history/telemetry/postings caches, usage limits,
    // scaler policy, store gates), seeded once below. Two ServerConfig
    // VALUES coexist fine, but a second running server in this process
    // would silently observe the first one's seeds — so run() enforces
    // its process-singleton contract loudly until WP-02 moves those
    // policies into per-runtime owners.
    // PR 3.2.1 naming review: this is a once-EVER process latch, not
    // "a server is currently running" — even a failed first invocation
    // consumes the right to call run() again, because the process-global
    // holders it may have partially seeded cannot be un-seeded. Do not
    // add reset-on-error logic here; WP-02 removes the holders instead.
    static RUN_WAS_INVOKED: std::sync::atomic::AtomicBool =
        std::sync::atomic::AtomicBool::new(false);
    if RUN_WAS_INVOKED.swap(true, std::sync::atomic::Ordering::SeqCst) {
        anyhow::bail!(
            "run() is process-singleton in the current transitional posture: \
             process-global policy holders (caches, budgets, limits, scaler) are \
             seeded once per process; a second runtime would observe the first \
             one's seeds (WP-02 replaces these with per-runtime owners)"
        );
    }
    let crate::config::validation::BootstrapParts {
        config,
        tenant,
        cell_id,
        auth_mode,
        catalog_cursor_key,
        cert_sealed_publish_delay_ms,
        initial_shards,
        configured_capacity,
        mut notices,
    } = validated.into_bootstrap_parts();

    // ---- preflight: the one OS-resource probe, BEFORE any process-
    // global initialization or remote I/O (PR 3.2). Round-4 review: the
    // capacity posture is validated against the real descriptor
    // ceiling; the SSE cap may be clamped to what nofile_hard can
    // actually carry.
    // WP-15/PR 4: this runtime's capabilities — clock, entropy, boot
    // identity — minted HERE, owned by this runtime, handed to owners
    // at construction. No process-global once-cell is involved.
    let runtime_caps = crate::runtime::RuntimeCaps::production(&config.cli.instance_name);
    tracing::info!(
        boot_id = %runtime_caps.identity.boot_id,
        instance = %runtime_caps.identity.instance,
        "runtime identity minted"
    );
    let limits = crate::http::raise_nofile();
    // PR 6-B: the posture travels inside the capacity value; the
    // effective cap is installed in the admission controller below —
    // the configuration graph is never mutated after validation.
    let effective = resolve_effective_capacity(configured_capacity, limits, &mut notices)
        .map_err(|e| anyhow::anyhow!("{e}"))?;
    let (nofile_soft, nofile_hard) = (
        limits.soft.map_or(0, |n| n.get()),
        limits.hard.map_or(0, |n| n.get()),
    );
    // PR 4.1: validation is silent; its advisories (and the preflight's)
    // are emitted HERE, after the whole configuration was accepted.
    for n in &notices {
        if n.is_warning() {
            tracing::warn!("{n}");
        } else {
            tracing::info!("{n}");
        }
    }
    tracing::info!(
        "nofile soft={nofile_soft} hard={nofile_hard} (raised to hard at boot); \
         feed retention budget={}B",
        crate::sse::budget::feed_total_cap(&config.sse)
    );
    if cert_sealed_publish_delay_ms > 0 {
        tracing::warn!(
            ms = cert_sealed_publish_delay_ms,
            "CERTIFICATION MODE: sealed publication delayed"
        );
    }

    tracing::info!(config = %config.redacted_summary(), "effective configuration (redacted)");
    tracing::info!(
        model = %crate::quota::pressure_model_json(),
        "project memory-pressure model (round-13; weights are code-versioned)"
    );
    init_slatedb_runtime_threads(config.engine.slatedb_rt_threads);
    // Process-global infrastructure sized once from the owned config
    // (WP-01 PR 3.1): the absorber budget, the shared caches (history,
    // telemetry, postings), the usage limits, the scaler policy, the
    // store egress gates, and the debug pause flag's INITIAL value.
    // Each holder documents why it is process-global; un-seeded tests
    // get the old defaults.
    crate::history::init_absorb_pause(config.history.absorb_pause_initial);
    crate::history::init_absorb_budget(&config.history);
    crate::history::init_history_cache(config.history.cache_bytes);
    crate::billing::init_telemetry_cache(config.billing.telemetry_cache_bytes);
    crate::postings_cache::init_postings_cache(config.postings.cache_bytes);
    crate::usage::init_limits(&config.admission);
    crate::scaler3::init_policy(&config.scaler);
    crate::store_timing::configure(&config.storage);

    // FIRST: the body ceiling sizes the absorber's worst-frame
    // reservation, which floors the process-wide budget. It must be
    // fixed before anything reads either (CHAOS-3). Engine-settings
    // validity (CHAOS-2) was proven by `validate()` before run().
    crate::http::install_max_body_bytes(config.cli.max_request_body_bytes);

    let ops_store = config.store_for(&config.cli.ops_bucket)?;
    let shard_store = config.store_for(&config.cli.shard_bucket)?;
    let data_store = config.store_for(&config.cli.data_bucket)?;

    // R23-5: a synchronous storage canary, BEFORE we bind.
    //
    // The /health readiness signal only fires for failures that reach a
    // shard open. A registry or control-plane storage failure refuses
    // requests earlier, so `shard_opens.started` stays 0 and readiness
    // stays silent — verified in the field by killing the object store
    // after boot. This closes that gap at the only moment it is cheap:
    // prove each bucket is usable, and refuse to start if it is not.
    //
    // Deliberately a write AND a read-back on every bucket we depend on.
    // Credentials that can read but not write are a real and silent
    // failure mode that would otherwise surface as a 500 per append
    // forever — which is the whole CHAOS-2 disease.
    let canary_prefix = config.cli.path_prefix.clone().unwrap_or_default();
    for (label, store) in [
        ("ops", ops_store.clone()),
        ("shard", shard_store.clone()),
        ("data", data_store.clone()),
    ] {
        let store: Arc<dyn ObjectStore> = store;
        // R24-E: the canary key must be unique per INSTANCE-INCARNATION,
        // not per PID. Firecracker VMs commonly start at the same pid, so
        // two instances sharing a namespace would collide on one object:
        // A puts, B puts, A deletes, B reads -> missing, and B refuses to
        // start for a store that is perfectly healthy. PR 4.1: the
        // incarnation IS the runtime identity — its boot id, minted from
        // the runtime's entropy — so no pid and no wall-clock nonce is
        // needed; the create-only put below remains the collision
        // detector.
        let probe = object_store::path::Path::from(format!(
            "{}_canary/{}-{}",
            canary_prefix.trim_end_matches('/'),
            config.cli.instance_name.replace('/', "_"),
            runtime_caps.identity.boot_id,
        ));
        let payload = b"streams-startup-canary".to_vec();
        store
            .put_opts(
                &probe,
                object_store::PutPayload::from(payload.clone()),
                object_store::PutOptions::from(object_store::PutMode::Create),
            )
            .await
            .with_context(|| {
                format!(
                    "startup canary: cannot WRITE to the {label} bucket — this process \
                     would have booted, answered /health with ok, and failed every append"
                )
            })?;
        let got = store
            .get(&probe)
            .await
            .with_context(|| format!("startup canary: cannot READ BACK from the {label} bucket"))?
            .bytes()
            .await
            .with_context(|| format!("startup canary: {label} read-back body failed"))?;
        if got.as_ref() != payload.as_slice() {
            anyhow::bail!(
                "startup canary: {label} bucket returned {} bytes, expected {} — \
                 this store is not durable for this process",
                got.len(),
                payload.len()
            );
        }
        let _ = store.delete(&probe).await; // best effort
    }
    tracing::info!("startup canary: ops/shard/data buckets readable and writable");
    // R23-5: and if we ever DO end up unready with no shard ever opened,
    // exit rather than sit in rotation-limbo (see spawn_unready_watchdog).
    // WP-02 / PR 6-F: the task supervisor owns every long-lived loop this
    // runtime spawns — cancellation, join results, failure policy.
    // PR 6.1-A: NO loop starts before every fallible startup step has
    // passed (stores, topology, required billing opens, the listener
    // bind), so an early `?` never strands a running loop; the watchdog
    // that used to start here now starts with the others below.
    let tasks = crate::tasks::TaskSupervisor::new();

    let registry = Registry::new(ops_store.clone(), &cell_id);
    // WP-02 / PR 6-D: the deployment identity, from the PROVEN parts.
    let deployment = crate::deployment::DeploymentIdentity::new(
        tenant,
        config.cli.account_id.clone(),
        cell_id.clone(),
        config.cli.telemetry_region.clone(),
    );
    // PR 3.2: tenant, auth mode, cursor key and the certification delay
    // were proven (and derived) by `validate()` — bootstrap only
    // consumes them. The auth service itself is constructed here
    // because it is a runtime owner, not a configuration fact.
    // §10.4: the denial journal drains through the system ledger key.
    // Without one, enforce still refuses correctly but the journal is
    // VOID — denials are only counted, never durably recorded. Loud at
    // boot so a preview cell cannot mistake itself for an audited one.
    if auth_mode != crate::auth::AuthMode::Off && config.cli.usage_stream_key.is_none() {
        tracing::warn!(
            "STREAMS_AUTH_MODE={} without USAGE_STREAM_KEY: the _audit_events \
             denial journal is DISABLED (denials appear only in \
             audit_events_dropped_total)",
            config.cli.streams_auth_mode
        );
    }
    let auth_service = std::sync::Arc::new(crate::auth::AuthService::new(
        auth_mode,
        config.cli.streams_auth_issuer.clone(),
        &config.cli.cell_id,
    )?);
    // Only relevant when no topology exists yet; an existing topology
    // wins. The effective count was resolved and PROVEN by validate().
    let topology = load_or_init_topology(
        &ops_store,
        initial_shards,
        config.cli.max_request_body_bytes,
    )
    .await
    .context("load topology")?;
    // R23-2: the body ceiling is a property of the NAMESPACE, not of the
    // process. The absorber sizes its worst-frame reservation from the
    // running setting, so starting against a namespace created with a
    // different ceiling would either under-reserve for records already
    // written — the exact under-reservation the process-wide budget
    // exists to prevent — or silently move the product limit customers
    // were told about. Refuse either way.
    //
    // A topology written before this field existed carries None; those
    // namespaces were created at the 32 MiB protocol pin and are held
    // to it.
    let stored_ceiling = topology
        .max_request_body_bytes
        .unwrap_or(crate::http::MAX_BODY_BYTES);
    if stored_ceiling != config.cli.max_request_body_bytes {
        anyhow::bail!(
            "MAX_REQUEST_BODY_BYTES is {} but this namespace was created with {} — \
             the ceiling sizes the absorber's worst-frame reservation, so changing it \
             on an existing namespace would under-reserve for records already written \
             (or silently move the published product limit). Set \
             MAX_REQUEST_BODY_BYTES={} to start against this namespace, or point \
             PATH_PREFIX at a fresh one.",
            config.cli.max_request_body_bytes,
            stored_ceiling,
            stored_ceiling,
        );
    }
    tracing::info!(
        "topology v{}: {} shard(s), body ceiling {} bytes (namespace-pinned)",
        topology.version,
        topology.shards.len(),
        stored_ceiling,
    );

    let keys = Arc::new(KeyCache::default());
    let touch = Arc::new(crate::touch::TouchRegistry::with_entropy(
        runtime_caps.entropy.clone(),
    ));

    // Shards open lazily on first routed request (COMPUTE-SPEC §5.1):
    // opening fences the previous owner, so ownership follows routing.
    // PR 6.1-B: the opener is a FACTORY over the directory's close
    // notifier — the one capability an engine's close needs — and it
    // captures nothing else of the runtime.
    let opener = |notifier: crate::shard_directory::ShardCloseNotifier| -> crate::sharddir::OpenFn {
        let shard_store = shard_store.clone();
        let data_store = data_store.clone();
        let keys = keys.clone();
        let touch = touch.clone();
        let settings = shard_settings(&config.cli, &config.engine);
        // §1.1: one block cache for the whole process, not one per DB
        // (SlateDB default: 512 MB PER DB — a 16-shard 1 GB instance dies
        // by cache fill; the run 6/8 zombie generator).
        let shared_cache: Arc<slatedb::db_cache::foyer::FoyerCache> =
            Arc::new(slatedb::db_cache::foyer::FoyerCache::new_with_opts(
                slatedb::db_cache::foyer::FoyerCacheOptions {
                    max_capacity: config.cli.shared_cache_bytes,
                    ..Default::default()
                },
            ));
        let absorb_bytes = config.cli.absorb_bytes;
        let absorb_age = config.cli.absorb_age_secs;
        let absorb_pass_bytes = config.cli.absorb_pass_bytes;
        let absorb_concurrency = config.cli.absorb_concurrency;
        let absorb_pace_window_ms = config.cli.absorb_pace_window_ms;
        let absorb_pace_ms = config.cli.absorb_pace_ms;
        let absorb_read_par = config.cli.absorb_read_par;
        let absorb_small_bytes = config.cli.absorb_small_bytes;
        // Startup invariant (OOM disposition 2): the per-gather packing
        // cap must fit the process budget after the build multiplier,
        // or the envelope claim quietly breaks via reservation
        // clamping. Clamp the PACKING LIMIT (not the reservation) and
        // say so loudly.
        let absorb_gather_max_bytes = {
            let budget = crate::history::absorb_budget().capacity();
            let max_allowed = budget / crate::history::ABSORB_BUILD_MULTIPLIER;
            crate::history::RESOLVED_GATHER_PACKING_BYTES.store(
                crate::history::resolved_gather_packing_bytes(config.cli.absorb_gather_max_bytes),
                std::sync::atomic::Ordering::Relaxed,
            );
            if config.cli.absorb_gather_max_bytes > max_allowed {
                tracing::warn!(
                    "ABSORB_GATHER_MAX_BYTES {} x{} exceeds the process budget {} — \
                     clamping the gather packing limit to {}",
                    config.cli.absorb_gather_max_bytes,
                    crate::history::ABSORB_BUILD_MULTIPLIER,
                    budget,
                    max_allowed,
                );
                max_allowed
            } else {
                config.cli.absorb_gather_max_bytes
            }
        };
        let handle_idle_evict_secs = config.cli.handle_idle_evict_secs;
        let handle_max_resident = config.cli.handle_max_resident;
        let trim_per_op = config.cli.trim_per_op;
        let trim_global_budget = config.cli.trim_global_budget;
        let wal_group_commit = config.cli.wal_group_commit != 0;
        let wal_flush_gap = Duration::from_millis(if config.cli.wal_flush_gap_ms == 0 {
            config.cli.flush_interval_ms
        } else {
            config.cli.wal_flush_gap_ms
        });
        let wal_post_ack_gather = Duration::from_millis(config.cli.wal_post_ack_gather_ms);
        let wal_gather_skip_reqs = if config.cli.wal_gather_skip_reqs == 0 {
            u32::MAX
        } else {
            config.cli.wal_gather_skip_reqs
        };
        let wal_gather_skip_bytes = if config.cli.wal_gather_skip_bytes == 0 {
            u64::MAX
        } else {
            config.cli.wal_gather_skip_bytes
        };
        let tail_ring_bytes = config.cli.tail_ring_bytes;
        // Per-open inputs cloned out of the owned config: the Fn opener
        // runs once per shard open and cannot move fields out of its
        // captured variables, so it clones from these locals per call.
        let opener_history = config.history.clone();
        let opener_compactor = config.engine.compactor_options();
        let opener_frame_compress = config.crypto.frame_compress;
        Box::new(
            move |prefix: String, incarnation: crate::sharddir::EngineIncarnation| {
                let shard_store = shard_store.clone();
                let shared_cache = shared_cache.clone();
                let data_store = data_store.clone();
                let keys = keys.clone();
                let touch = touch.clone();
                let mut settings = settings.clone();
                // O14a: desynchronize WAL flush ticks across shards. 16
                // shards flushing on the same phase PUT in synchronized
                // bursts every interval; staggering by a per-shard offset
                // (base..1.5x base) spreads the PUTs across the window.
                if let Some(base) = settings.flush_interval {
                    let mut h: u32 = 2166136261;
                    for b in prefix.bytes() {
                        h ^= b as u32;
                        h = h.wrapping_mul(16777619);
                    }
                    let spread = (base.as_millis() as u64 / 2).max(1);
                    settings.flush_interval = Some(base + Duration::from_millis(h as u64 % spread));
                }
                let opener_history = opener_history.clone();
                let opener_compactor = opener_compactor.clone();
                let notifier = notifier.clone();
                Box::pin(async move {
                    let path = crate::sharddir::shard_db_path(&prefix);
                    tracing::info!("opening shard log {path} (lazy; fences prior owner)");
                    let db = {
                        let p2 = path.clone();
                        crate::bootstrap::on_slatedb_rt(async move {
                            Db::builder(p2.as_str(), shard_store)
                                .with_settings(settings)
                                .with_db_cache(shared_cache)
                                .build()
                                .await
                        })
                        .await
                        .with_context(|| format!("open shard log {path}"))?
                    };
                    let db = Arc::new(db);
                    // R25-A: load (or rebuild) the durable maintenance
                    // state SYNCHRONOUSLY, before the engine exists.
                    // Failure here is an engine-open failure — a shard
                    // whose backlog cannot be established must not
                    // serve, because "unknown" would be treated as
                    // "zero" by every admission decision after it.
                    let maintenance = crate::shard::load_or_rebuild_maintenance(&db)
                        .await
                        .with_context(|| format!("load maintenance state for shard {prefix}"))?;
                    let (absorb_tx, absorb_rx) = absorber_channel();
                    let on_close = {
                        let touch = touch.clone();
                        let prefix = prefix.clone();
                        let notifier = notifier.clone();
                        Arc::new(move || {
                            touch.close_shard(&prefix);
                            notifier.closed(&prefix, incarnation);
                        }) as Arc<dyn Fn() + Send + Sync>
                    };
                    let engine = ShardEngine::start(
                        prefix.clone(),
                        db,
                        data_store.clone(),
                        ShardConfig {
                            max_trim_per_op: trim_per_op,
                            trim_global_budget,
                            wal_group_commit,
                            wal_flush_gap,
                            wal_post_ack_gather,
                            wal_gather_skip_reqs,
                            wal_gather_skip_bytes,
                            tail_ring_bytes,
                            handle_idle_evict: Duration::from_secs(handle_idle_evict_secs),
                            handle_max_resident,
                            shared_postings_cache: Some(crate::postings_cache::process_cache()),
                            frame_compression: crate::crypto::FrameCompression::from_enabled(
                                opener_frame_compress,
                            ),
                            history: opener_history,
                            compactor_options: opener_compactor,
                            ..Default::default()
                        },
                        absorb_tx,
                        Some(on_close),
                        maintenance,
                    );
                    Absorber::start(
                        data_store,
                        engine.clone(),
                        keys,
                        AbsorberConfig {
                            threshold_bytes: absorb_bytes,
                            threshold_age: Duration::from_secs(absorb_age),
                            pass_bytes: absorb_pass_bytes,
                            concurrency: absorb_concurrency,
                            small_pass_bytes: absorb_small_bytes,
                            gather_max_bytes: absorb_gather_max_bytes,
                            gather_pace_window: Duration::from_millis(absorb_pace_window_ms),
                            gather_pace: Duration::from_millis(absorb_pace_ms),
                            gather_read_par: absorb_read_par,
                            ..Default::default()
                        },
                        absorb_rx,
                    );
                    Ok(engine)
                })
            },
        )
    };

    let fleet_store_opt = config.fleet_store()?;
    // WP-02 / PR 6-A: the ownership and shard-directory OWNERS take their
    // own configuration here, before the composition root. PR 6.1-B: the
    // directory builds its serving map and gate itself, from the opener
    // factory and its timings.
    let ownership = crate::ownership::OwnershipService::new(config.cli.instance_name.clone());
    let shard_directory = crate::shard_directory::ShardDirectory::new(
        topology.shards.clone(),
        ownership.clone(),
        crate::shard_directory::OpenTiming {
            open_deadline: config.shard.open_deadline,
            open_wait: Duration::from_millis(config.shard.open_wait_ms),
        },
        opener,
    );
    let admission = crate::admission::AdmissionController::new(crate::admission::AdmissionKnobs {
        max_inflight: config.cli.admit_max_inflight,
        per_stream_cap: config.cli.admit_max_inflight_per_stream,
        rss_shed_mb: config.cli.admit_rss_shed_mb,
        project_memory_pressure_bytes: config.cli.project_memory_pressure_bytes,
        project_memory_release_pct: config.cli.project_memory_release_pct,
        subscriptions: crate::admission::SubscriptionCapacity {
            effective: effective.sse_max_connections,
            configured: effective.configured,
        },
        record_ceiling_bytes: config.cli.max_record_payload_bytes.unwrap_or(0),
    });
    // SR3-1: the MODE determines the runtime credential state — in
    // workload mode the static token does not exist at runtime,
    // whatever the environment carried; in static mode no source
    // exists and relays use the bridge token.
    let fleet_static_token = if config.cli.fleet_auth_mode == "workload" {
        None
    } else {
        config.cli.fleet_internal_token.clone()
    };
    let fleet_token_source: Option<crate::peer::FleetTokenSource> = (config.cli.fleet_auth_mode
        == "workload")
        .then_some(config.cli.workload_token_file.as_ref())
        .flatten()
        .map(|path| {
            // Expiry-aware file cache: the platform rotates the file;
            // this re-reads when forced (peer 401) or within 30s of
            // the cached token's exp. The exp is read WITHOUT
            // verification — freshness scheduling only; peers verify.
            let path = path.clone();
            let cache: std::sync::Mutex<Option<(String, i64)>> = std::sync::Mutex::new(None);
            std::sync::Arc::new(move |force: bool| {
                let now = chrono::Utc::now().timestamp();
                let mut c = cache.lock().unwrap();
                if !force
                    && let Some((tok, exp)) = c.as_ref()
                    && now < exp - 30
                {
                    return Some(tok.clone());
                }
                let tok = std::fs::read_to_string(&path).ok()?.trim().to_string();
                let exp = crate::auth::unverified_exp(&tok).unwrap_or(now);
                *c = Some((tok.clone(), exp));
                Some(tok)
            }) as crate::peer::FleetTokenSource
        });
    let peer = crate::peer::PeerClient::new(fleet_static_token, fleet_token_source);
    let livefeed = crate::sse::service::LiveFeedService::from_config(&config.sse);
    let bearer = crate::deployment_bearer::DeploymentBearer::new(
        config.cli.auth_token.clone(),
        config.cli.conformance_default_key.clone(),
    );
    // WP-02 / PR 6-E: the billing owner takes its ledger key and the
    // read accumulator here; the spool and rollup are installed later by
    // the telemetry loops, exactly once.
    let billing = crate::billing_service::BillingService::new(
        config.cli.usage_stream_key.clone(),
        Arc::new(crate::billing::ReadUsageAccumulator::new(
            crate::billing::MeterSource {
                cell: config.cli.cell_id.clone(),
                instance: config.cli.instance_name.clone(),
                boot: runtime_caps.identity.boot_id.clone(),
            },
        )),
    );
    let config = Arc::new(config);
    let state = Arc::new(AppState {
        runtime: runtime_caps.clone(),
        config: config.clone(),
        registry,
        shards: shard_directory,
        admission,
        peer,
        fleet: crate::fleet::FleetRepository::new(fleet_store_opt.clone()),
        livefeed,
        bearer,
        deployment,
        billing,
        rollup: crate::rollup::RollupSlot::default(),
        tasks: tasks.monitor(),
        cert_sealed_publish_delay_ms: std::sync::atomic::AtomicU64::new(
            // PR 3.2: proven by validate(); no panic path in bootstrap.
            cert_sealed_publish_delay_ms,
        ),
        ownership,
        data_store,
        keys,
        touch,
        origin_marker: if config.cli.instance_name.is_empty() {
            format!("streams/{}", env!("CARGO_PKG_VERSION"))
        } else {
            config.cli.instance_name.clone()
        },
        auth: auth_service.clone(),
        quotas: crate::quota::QuotaRegistry::default(),
        catalog_cursor_key,
    });
    // PR 6.1-A: the LAST fallible startup steps come before the first
    // long-lived loop starts.
    if config.cli.billing_mode == "required" {
        // PR 3.2.1: the PURE required-mode prerequisites (usage key
        // present, no placeholder identities) were proven by
        // validate(); only the store I/O below remains here.
        // Round-22 items 2b/10: the read spool must be OPEN and
        // READABLE before this instance serves a single request —
        // required mode has no memory-only fallback window, so a spool
        // that cannot open (or whose rows cannot be scanned) is fatal.
        crate::billing::open_read_spool(&state).await.map_err(|e| {
            anyhow::anyhow!("BILLING_MODE=required: read spool must open before serving: {e}")
        })?;
        // ...and the rollup instance's database likewise: a rollup
        // owner that cannot open its DB must not serve (item 10).
        if config.cli.rollup == "1" {
            crate::billing::open_rollup(
                &state,
                &config.cli.path_prefix.clone().unwrap_or_default(),
            )
            .await
            .map_err(|e| {
                anyhow::anyhow!("BILLING_MODE=required: rollup DB must open before serving: {e}")
            })?;
        }
    }
    let listener = tokio::net::TcpListener::bind(&config.cli.listen)
        .await
        .with_context(|| format!("bind {}", config.cli.listen))?;
    tracing::info!("streams-slate listening on {}", config.cli.listen);
    // Every fallible step has passed: the long-lived loops start here.
    // An instance that never becomes ready must exit rather than sit in
    // rotation-limbo (see spawn_unready_watchdog).
    crate::sharddir::spawn_unready_watchdog(&config.shard, state.runtime.clock.clone(), &tasks);
    // PR 6.1-A: SIGTERM / Ctrl-C request the ordered shutdown — the
    // accept loop returns once cancelled, then every loop is joined.
    {
        let request = tasks.shutdown_request();
        let _ = tasks.spawn(
            "signal",
            crate::tasks::Policy::Noncritical,
            move |cancel| async move {
                tokio::select! {
                    _ = cancel.cancelled() => {}
                    _ = shutdown_signal() => {
                        tracing::info!("termination signal: shutting down");
                        request.request();
                    }
                }
                crate::tasks::TaskResult::Done
            },
        );
    }
    // MULTITENANCY Stage 5: feed refresher — an immediate first fetch,
    // then a cadence well inside the staleness window (checked above).
    if auth_mode != crate::auth::AuthMode::Off {
        crate::auth_feed::spawn_refresher(
            auth_service.clone(),
            Box::new(crate::auth_feed::FileKeySource(
                config.cli.streams_auth_keys_file.clone().unwrap(),
            )),
            Box::new(crate::auth_feed::FilePolicySource(
                config.cli.streams_auth_policy_file.clone().unwrap(),
            )),
            Box::new(crate::auth_feed::FileGrantSource(
                config.cli.streams_auth_grants_file.clone().unwrap(),
            )),
            std::time::Duration::from_secs(config.cli.streams_auth_refresh_secs.max(1)),
            &tasks,
        );
    }
    // Unified scaler (ROUTING-V3 §5): sketch-driven splits/merges.
    crate::scaler3::start(Arc::downgrade(&state), &tasks);
    {
        // RSS sampler for the shed check (500 ms; /proc read per request
        // would be silly). Unconditional: this used to live inside the
        // fleet-mode block, which left ADMIT_RSS_SHED_MB comparing against
        // a frozen 0 in standalone mode — the shed was dead exactly where
        // the 2026-07-21 single-instance gate needed it (OOM at ~725 MB
        // with admit_shed=0).
        //
        // Purge-on-pressure: mimalloc only purges freed OS pages on
        // allocation-path ticks, so a process that goes IDLE after an
        // overload spike never purges — RSS stays frozen at the high
        // water and the shed 429s forever (the wedge liveness gate's
        // FAIL signature: byte-identical RSS for minutes, zero store
        // writes, zero backlog). When the sampler sees RSS above the
        // shed line it forces a collection (segments decommit;
        // purge_decommits defaults on) and re-measures, so retained-idle
        // memory can't masquerade as live pressure. Rate-limited; the
        // instance is already shedding writes when this runs.
        let st = state.clone();
        let shed_line_mb = config.cli.admit_rss_shed_mb;
        let bp_limits = crate::backpressure::Limits::from_config(&config.admission);
        tracing::info!(
            unabsorbed_instance = bp_limits.unabsorbed_bytes_instance,
            unabsorbed_shard = bp_limits.unabsorbed_bytes_shard,
            lag_secs = bp_limits.absorb_lag_secs,
            release_pct = bp_limits.release_pct,
            "maintenance backpressure bounds",
        );
        let _ = tasks.spawn(
            "rss-sampler",
            crate::tasks::Policy::Critical,
            move |cancel| async move {
                let mut last_purge: Option<std::time::Instant> = None;
                let mut ticks: u64 = 0;
                loop {
                    let mut mb = crate::fleet::rss_bytes() / 1048576;
                    let purge_due = shed_line_mb > 0
                        && mb > shed_line_mb
                        && last_purge.is_none_or(|t| t.elapsed() >= Duration::from_secs(10));
                    if purge_due {
                        let _ = tokio::task::spawn_blocking(|| unsafe {
                            libmimalloc_sys::mi_collect(true);
                        })
                        .await;
                        last_purge = Some(std::time::Instant::now());
                        mb = crate::fleet::rss_bytes() / 1048576;
                    }
                    st.admission.record_rss_mb(mb);
                    // Peak-since-scrape for the ops snapshot (OOM review I4):
                    // 250 ms sampling, max-held until the scrape drains it.
                    crate::ops::RSS_PEAK_MB.fetch_max(mb, std::sync::atomic::Ordering::Relaxed);
                    // Maintenance backpressure re-evaluates on the same tick
                    // (R23-1). Doing it here keeps the request path to a
                    // single atomic read — walking the lag map per append
                    // would put the overload on the hot path.
                    if ticks.is_multiple_of(8) {
                        let snap = crate::backpressure::snapshot(&st.shards);
                        st.admission.apply_maintenance(&snap, &bp_limits);
                    }
                    ticks = ticks.wrapping_add(1);
                    tokio::select! {
                        _ = cancel.cancelled() => return crate::tasks::TaskResult::Done,
                        _ = tokio::time::sleep(Duration::from_millis(250)) => {}
                    }
                }
            },
        );
    }
    if let Some(fleet_store) = fleet_store_opt {
        crate::fleet::start(
            state.clone(),
            fleet_store,
            crate::fleet::FleetCfg {
                instance: config.cli.instance_name.clone(),
                capacity_rps: config.cli.scale_rps_capacity,
                edge_slots: config.cli.scale_edge_slots,
                target_util: (config.cli.scale_out_cpu_pct as f64 / 100.0).clamp(0.05, 0.95),
                scale_in_util: (config.cli.scale_in_cpu_pct as f64 / 100.0).clamp(0.05, 0.90),
                hot_cpu_pct: config.cli.scale_out_cpu_pct as f64,
                cpu_sustain: Duration::from_secs(config.cli.scale_cpu_sustain_secs),
                scale_in: Duration::from_secs(config.cli.scale_in_secs),
                latency_ms: config.cli.scale_latency_ms,
                edge_latency_ms: config.cli.scale_edge_latency_ms,
                latency_sustain: Duration::from_secs(config.cli.scale_lat_sustain_secs),
                max: config.cli.fleet_max,
            },
            &tasks,
        );
        tracing::info!(
            "fleet coordination on (prefix={}, cap={} rps)",
            config.cli.fleet_prefix.as_deref().unwrap_or(""),
            config.cli.scale_rps_capacity
        );
    }
    // Telemetry pipeline (docs/OBSERVABILITY-BILLING.md): the drainer on
    // every instance; the rollup consumer where ROLLUP=1.
    // ONE startup budget summary (OOM review): every fixed memory
    // bound in a single log line, plus a headroom warning when their
    // sum leaves less than 100 MiB below the shed line — posture
    // mistakes surface at boot, not at the kill line. WP-01: values come
    // from the installed AppConfig (identical parsing, once).
    {
        let cfg = &config;
        let shared = config.cli.shared_cache_bytes as usize;
        let history = cfg.history.cache_bytes;
        let postings = cfg.postings.cache_bytes;
        let telemetry = cfg.billing.telemetry_cache_bytes;
        let budget = crate::history::absorb_budget();
        let absorb_budget = budget.capacity();
        let gathers = budget.gather_slots();
        // Every gather reserves at least the worst-frame transient, so
        // the EFFECTIVE concurrency is the byte budget divided by that
        // floor — 1 under the 1-GiB profile regardless of configured
        // slots. Print both so nobody reads two slots as two-way.
        // R23-3: use the SHARED accounting so the log, the debug
        // surface, and the campaign verification cannot disagree. A
        // gather reserves max(packing x multiplier, worst_frame), not
        // the worst frame alone.
        crate::history::RESOLVED_GATHER_PACKING_BYTES.store(
            crate::history::resolved_gather_packing_bytes(config.cli.absorb_gather_max_bytes),
            std::sync::atomic::Ordering::Relaxed,
        );
        let per_gather = crate::history::per_gather_reservation_bytes();
        let effective_gathers = crate::history::effective_gather_concurrency();
        let rt_threads = cfg.engine.slatedb_rt_threads;
        let mib = |b: usize| b / (1024 * 1024);
        tracing::info!(
            "memory budget: caches shared={}MiB history={}MiB postings={}MiB telemetry={}MiB; unflushed/db={}MiB; absorb budget={}MiB (worst-frame build={}MiB, per-gather reservation={}MiB, configured gather slots={}, EFFECTIVE gather concurrency={}); slatedb rt threads={}; shed line={}MB (RSS + reserved absorber bytes)",
            mib(shared),
            mib(history),
            mib(postings),
            mib(telemetry),
            mib(config.cli.max_unflushed_bytes),
            mib(absorb_budget),
            mib(crate::history::absorb_worst_frame_transient()),
            mib(per_gather),
            gathers,
            effective_gathers,
            rt_threads,
            config.cli.admit_rss_shed_mb,
        );
        let _ = crate::history::RESOLVED_MEMORY_CONFIG.set(serde_json::json!({
            "gatherPackingLimitBytes": config.cli
                .absorb_gather_max_bytes
                .min(absorb_budget / crate::history::ABSORB_BUILD_MULTIPLIER),
            "absorbBudgetBytes": absorb_budget,
            "gatherSlots": gathers,
            "effectiveGatherConcurrency": effective_gathers,
            "slatedbRuntimeThreads": rt_threads,
            "sharedCacheBytes": shared,
            "historyCacheBytes": history,
            "postingsCacheBytes": postings,
            "telemetryCacheBytes": telemetry,
            "maxUnflushedBytes": config.cli.max_unflushed_bytes,
            "l0SstSizeBytes": config.cli.l0_sst_size_bytes,
            "l0MaxSsts": config.cli.l0_max_ssts,
            "shedLineMb": config.cli.admit_rss_shed_mb,
        }));
        let fixed_mb = mib(shared + history + postings + telemetry + absorb_budget) as u64;
        if config.cli.admit_rss_shed_mb > 0 && fixed_mb + 100 > config.cli.admit_rss_shed_mb {
            tracing::warn!(
                "fixed memory budgets ({fixed_mb} MiB) leave <100 MiB below the shed line                  ({} MB) — this posture does not fit the instance class",
                config.cli.admit_rss_shed_mb,
            );
        }
    }
    crate::billing::spawn_telemetry(state.clone(), &tasks);
    if config.cli.rollup == "1" {
        crate::billing::spawn_rollup(
            state.clone(),
            config.cli.path_prefix.clone().unwrap_or_default(),
            &tasks,
        );
    }
    let app = crate::http::router(state);

    crate::store_timing::spawn_sentinels();

    // #269: bounded h1 buffers — see http::serve_h1.
    let max_buf = config.http.h1_max_buf;
    crate::http::serve_h1(listener, app, max_buf, tasks.clone()).await?;
    // PR 6-F / 6.1-A: the accept loop returned because shutdown was
    // requested — its connections are already gone; now every supervised
    // loop is cancelled, joined and reported (WP-15 §9 sequences
    // admission, engines and stores ahead of this in its remaining slice).
    let report = tasks.shutdown(std::time::Duration::from_secs(10)).await;
    tracing::info!(
        finished = ?report.finished(),
        aborted = ?report.aborted,
        panicked = ?report.panicked(),
        "supervised loops stopped"
    );
    Ok(())
}

/// PR 6.1-A: the process's termination request (SIGTERM or Ctrl-C).
async fn shutdown_signal() {
    let ctrl_c = tokio::signal::ctrl_c();
    #[cfg(unix)]
    {
        let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("SIGTERM handler");
        tokio::select! {
            _ = ctrl_c => {}
            _ = term.recv() => {}
        }
    }
    #[cfg(not(unix))]
    {
        let _ = ctrl_c.await;
    }
}
