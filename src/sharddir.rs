//! Single-flight shard opening (the eu-central-1 wedge fix).
//!
//! Opening a shard log replays its untrimmed WAL from the object store,
//! and on a bad day that replay is *minutes* long — hundreds of files at
//! hundreds of milliseconds each. The old `engine_for` awaited the open
//! inside the request handler while holding the open lock, which composed
//! into a storm (docs/SOAK-REGIONS.md, eu-central-1, 2026-07-26):
//!
//! 1. The client gives up before the replay finishes and disconnects;
//!    axum drops the handler future, releasing the open lock.
//! 2. The inner `Db` open was spawned onto the SlateDB runtime
//!    (`on_slatedb_rt`), so dropping the await does NOT cancel it — the
//!    replay keeps running, detached, its result destined for a oneshot
//!    nobody holds.
//! 3. The next request starts a second full replay. Then a third.
//!    Detached replays pile up until they consume the entire outbound
//!    connection budget (`get:wal` 12,666/min, 41–88 in flight).
//! 4. Each detached open that *completes* bumps the writer epoch, fencing
//!    the previous zombie — a writer-epoch war of one process against
//!    itself. No writer survives long enough to flush L0, so
//!    `replay_after_wal_id` never advances and every new replay does the
//!    full range again.
//! 5. The serving map never gets populated, because insertion happened in
//!    the request task that died. Appends starve; the region is wedged.
//!
//! `OpenGate` breaks every link in that chain:
//!
//! - **Single-flight**: at most one open per prefix, ever. Concurrent
//!   callers subscribe to the same outcome.
//! - **Cancellation-proof**: the open runs in its own spawned task that
//!   OWNS the result — it inserts the engine into the serving map itself.
//!   Callers wait with a bounded timeout and get a retryable 503 if the
//!   open is slow; their disconnection changes nothing.
//! - **Escalating holdoff**: an open that fails, or an engine that dies
//!   young, pushes the next attempt out exponentially (3 s → 60 s cap).
//!   A sick store gets a trickle of opens, not a storm.

use std::collections::HashMap;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};

use crate::shard::ShardEngine;

/// Base holdoff after a fence-close or failed open (matches the old 3 s
/// anti-flap), doubling per strike up to [`HOLDOFF_CAP`].
const HOLDOFF_BASE: Duration = Duration::from_secs(3);
const HOLDOFF_CAP: Duration = Duration::from_secs(60);
/// An engine that dies younger than this counts as a strike; surviving
/// longer resets the escalation.
const SHORT_LIVED: Duration = Duration::from_secs(30);

// Default ceiling on one open attempt: SHARD_OPEN_DEADLINE_MS, default
// 180 s (crate::config ShardRuntimeConfig::open_deadline). A WAL replay
// is legitimately minutes long on a bad day, but *unbounded* is not a
// budget: the soak2 campaign's final run left eu-central-1 with an open
// that looped in slatedb's compactions-log recovery for 20+ minutes —
// the gate contained it (one open, 648 coalesced waiters, zero storm),
// but the shard was unavailable the whole time.

// Process-global counters for /v1/debug/store: the cloud-run detector for
// this failure mode is "opens_started climbing while the serving map stays
// empty", and it must be visible without logs.
static OPENS_STARTED: AtomicU64 = AtomicU64::new(0);
static OPENS_COMPLETED: AtomicU64 = AtomicU64::new(0);
static OPENS_FAILED: AtomicU64 = AtomicU64::new(0);
static OPENS_COALESCED: AtomicU64 = AtomicU64::new(0);
static OPENS_IN_FLIGHT: AtomicI64 = AtomicI64::new(0);
static OPENS_DEADLINED: AtomicU64 = AtomicU64::new(0);
/// Abandoned opens that eventually completed and were closed by the
/// reaper instead of installed.
static OPENS_REAPED: AtomicU64 = AtomicU64::new(0);

/// Message from the most recent open failure, for the readiness surface.
static LAST_OPEN_ERROR: Mutex<Option<String>> = Mutex::new(None);

/// DISTINCT shard prefixes that have failed to open, and never opened.
///
/// R23-5: readiness previously counted failed ATTEMPTS, so one poison
/// shard retried three times could evict a whole never-used instance
/// from rotation — contradicting the "one poison stream must not evict a
/// healthy instance" property the code claimed. Counting distinct
/// prefixes makes the claim true: three DIFFERENT shards failing with
/// zero lifetime successes is a broken data plane; one shard failing
/// three times is one broken shard.
// mt-lint: allow(name-keyed-map): shard prefixes, not stream names
static FAILED_PREFIXES: Mutex<Option<std::collections::BTreeSet<String>>> = Mutex::new(None);

fn note_failed_prefix(prefix: &str) {
    if let Ok(mut g) = FAILED_PREFIXES.lock() {
        g.get_or_insert_with(Default::default)
            .insert(prefix.to_string());
    }
}

fn distinct_failed_prefixes() -> u64 {
    FAILED_PREFIXES
        .lock()
        .ok()
        .and_then(|g| g.as_ref().map(|s| s.len() as u64))
        .unwrap_or(0)
}

/// DISTINCT shards that must fail before an instance which has NEVER
/// served a shard declares itself unready. Three different shards
/// failing with zero successes is not a flaky store — it is a broken
/// instance.
const NEVER_OPENED_STRIKES: u64 = 3;

/// Readiness verdict: `Some(reason)` when this process has never
/// successfully opened a single shard while THREE DISTINCT shards have
/// failed to open.
///
/// CHAOS-2 (2026-08-09): a broken-from-boot data plane presents
/// identically at the edge whatever the cause — the process boots,
/// binds, answers `/health` with `ok`, and returns 500 to every append
/// forever, with one WARN per attempt as the only evidence. A load
/// balancer keeps such an instance in rotation indefinitely.
///
/// **R23-5 scope correction.** An earlier version of this comment (and
/// of the campaign report) claimed the check covers "every" such cause
/// including wrong bucket and bad credentials. It does not. This signal
/// only fires for failures that REACH A SHARD OPEN. A registry or
/// control-plane storage failure refuses the request earlier, leaving
/// `started == 0` and this check silent — verified in the field by
/// killing the object store after boot. The startup storage canary is
/// what covers that class; this is the runtime half of the pair.
///
/// Two conditions, both required, and both deliberately narrow:
///   * ZERO lifetime successful opens — an instance that has served
///     shards and later starts failing stays ready, so a mid-life store
///     blip cannot cascade a whole fleet out of rotation.
///   * THREE DISTINCT shard prefixes failed — counting attempts instead
///     would let one poison shard, retried three times, evict a
///     never-used instance. That was the actual behaviour before R23-5,
///     and it contradicted the property this comment claimed.
pub fn unready_reason() -> Option<String> {
    let distinct = distinct_failed_prefixes();
    if OPENS_COMPLETED.load(Ordering::Relaxed) > 0 || distinct < NEVER_OPENED_STRIKES {
        return None;
    }
    let last = LAST_OPEN_ERROR
        .lock()
        .ok()
        .and_then(|g| g.clone())
        .unwrap_or_else(|| "unknown".into());
    Some(format!(
        "no shard has ever opened ({distinct} distinct shards failed); last error: {last}"
    ))
}

/// How long a never-ready instance may stay unready before it exits.
///
/// R23-5: once readiness reports 503 the load balancer stops sending the
/// requests that would trigger another open attempt, so the instance can
/// sit unready forever even after the store heals — a zombie holding a
/// slot. Exiting is strictly better: the platform restarts it, the
/// startup canary re-runs against the healed store, and a genuinely
/// broken deployment crash-loops visibly instead of idling in an
/// ambiguous state. 0 disables.
pub fn unready_exit_after(cfg: &crate::config::ShardRuntimeConfig) -> Duration {
    Duration::from_secs(cfg.unready_exit_after_secs)
}

/// Watchdog: exit if this instance has been unready for too long without
/// ever having opened a shard.
/// WP-15/PR 4 (retry-timing exemplar): the watchdog's cadence and
/// elapsed measurement go through the injected [`crate::runtime::Clock`]
/// — a deterministic test can drive the unready window without wall
/// time. The survival `process::exit` stays until WP-15's task
/// supervision gives critical tasks a result policy.
pub fn spawn_unready_watchdog(
    cfg: &crate::config::ShardRuntimeConfig,
    clock: std::sync::Arc<dyn crate::runtime::Clock>,
) {
    let limit = unready_exit_after(cfg);
    if limit.is_zero() {
        return;
    }
    tokio::spawn(async move {
        let mut since_ms: Option<i64> = None;
        loop {
            clock.sleep(Duration::from_secs(10)).await;
            match unready_reason() {
                None => since_ms = None,
                Some(reason) => {
                    let t0 = *since_ms.get_or_insert_with(|| clock.now().ms());
                    let elapsed = Duration::from_millis((clock.now().ms() - t0).max(0) as u64);
                    if elapsed >= limit {
                        tracing::error!(
                            "unready for {:?} and no shard has ever opened ({reason}); \
                             exiting so the platform restarts this instance rather than \
                             leaving it in rotation-limbo",
                            elapsed,
                        );
                        std::process::exit(1);
                    }
                }
            }
        }
    });
}

pub fn stats_json() -> serde_json::Value {
    serde_json::json!({
        "unready": unready_reason(),
        "distinct_failed_prefixes": distinct_failed_prefixes(),
        "started": OPENS_STARTED.load(Ordering::Relaxed),
        "completed": OPENS_COMPLETED.load(Ordering::Relaxed),
        "failed": OPENS_FAILED.load(Ordering::Relaxed),
        "coalesced": OPENS_COALESCED.load(Ordering::Relaxed),
        "in_flight": OPENS_IN_FLIGHT.load(Ordering::Relaxed),
        "deadlined": OPENS_DEADLINED.load(Ordering::Relaxed),
        "reaped": OPENS_REAPED.load(Ordering::Relaxed),
    })
}

type OpenResult = Result<Arc<ShardEngine>, String>;
type OpenFn = Box<
    dyn Fn(String) -> futures_util::future::BoxFuture<'static, anyhow::Result<Arc<ShardEngine>>>
        + Send
        + Sync,
>;

#[derive(Default)]
struct PrefixGate {
    /// Present while an open task is running: subscribe, don't start.
    inflight: Option<tokio::sync::watch::Receiver<Option<OpenResult>>>,
    /// No new open may start before this instant.
    holdoff_until: Option<Instant>,
    /// Consecutive failures or short-lived engines; drives the holdoff.
    strikes: u32,
    /// When the currently-serving engine was opened (for lifetime-based
    /// strike reset on close).
    opened_at: Option<Instant>,
}

fn holdoff_for(strikes: u32) -> Duration {
    let mult = 1u32 << strikes.min(5); // 3s,6s,12s,24s,48s,96s→cap
    (HOLDOFF_BASE * mult).min(HOLDOFF_CAP)
}

struct GateInner {
    // mt-lint: allow(name-keyed-map): shard prefix -> engine
    shards: Arc<RwLock<HashMap<String, Arc<ShardEngine>>>>,
    opener: OpenFn,
    // mt-lint: allow(name-keyed-map): shard prefix -> open/park gate
    st: Mutex<HashMap<String, PrefixGate>>,
    /// Ceiling on one open attempt (SHARD_OPEN_DEADLINE_MS via config).
    open_deadline: Duration,
    /// Per-INSTANCE mirrors of the global counters, for tests. The
    /// statics feed process metrics and are shared with every other
    /// OpenGate in the binary — a paused-clock gate test asserting on
    /// them raced ordinary http_rig tests opening engines concurrently
    /// (completed bled to 1 in one full-suite run per ~8). Tests
    /// assert on THEIR gate's counters instead.
    #[cfg(test)]
    c_started: AtomicU64,
    #[cfg(test)]
    c_completed: AtomicU64,
    #[cfg(test)]
    c_failed: AtomicU64,
    #[cfg(test)]
    c_coalesced: AtomicU64,
}

/// What a caller gets back. `Wait` is always retryable and never means the
/// open was abandoned — the open (if any) continues in its own task.
pub enum OpenOutcome {
    Ready(Arc<ShardEngine>),
    /// Try again in `retry_after_secs`; `code` distinguishes "recently
    /// fenced away" from "open in progress, slower than your patience".
    Wait {
        code: &'static str,
        retry_after_secs: u64,
    },
    Failed(String),
}

#[derive(Clone)]
pub struct OpenGate {
    inner: Arc<GateInner>,
}

impl OpenGate {
    pub fn new(
        shards: Arc<RwLock<HashMap<String, Arc<ShardEngine>>>>,
        opener: OpenFn,
        open_deadline: Duration,
    ) -> Self {
        Self::with_deadline(shards, opener, open_deadline)
    }

    pub fn with_deadline(
        shards: Arc<RwLock<HashMap<String, Arc<ShardEngine>>>>,
        opener: OpenFn,
        open_deadline: Duration,
    ) -> Self {
        OpenGate {
            inner: Arc::new(GateInner {
                shards,
                opener,
                st: Mutex::new(HashMap::new()),
                open_deadline,
                #[cfg(test)]
                c_started: AtomicU64::new(0),
                #[cfg(test)]
                c_completed: AtomicU64::new(0),
                #[cfg(test)]
                c_failed: AtomicU64::new(0),
                #[cfg(test)]
                c_coalesced: AtomicU64::new(0),
            }),
        }
    }

    /// The serving map (shared with the fleet loop and debug surfaces).
    pub fn shards(&self) -> &Arc<RwLock<HashMap<String, Arc<ShardEngine>>>> {
        &self.inner.shards
    }

    /// Get the engine for `prefix`, starting (or joining) a single-flight
    /// open if needed, waiting at most `wait` for it.
    pub async fn get_or_open(&self, prefix: &str, wait: Duration) -> OpenOutcome {
        if let Some(e) = self.inner.shards.read().unwrap().get(prefix) {
            return OpenOutcome::Ready(e.clone());
        }

        // Decide under the state lock: subscribe, holdoff, or start.
        let mut rx = {
            let mut st = self.inner.st.lock().unwrap();
            let g = st.entry(prefix.to_string()).or_default();

            if let Some(rx) = &g.inflight {
                OPENS_COALESCED.fetch_add(1, Ordering::Relaxed);
                #[cfg(test)]
                self.inner.c_coalesced.fetch_add(1, Ordering::Relaxed);
                rx.clone()
            } else {
                if let Some(until) = g.holdoff_until {
                    let now = Instant::now();
                    if now < until {
                        return OpenOutcome::Wait {
                            code: "shard_moving",
                            retry_after_secs: (until - now).as_secs().max(1),
                        };
                    }
                }
                // Raced with a completed open? (map insert happens before
                // the inflight entry clears)
                if let Some(e) = self.inner.shards.read().unwrap().get(prefix) {
                    return OpenOutcome::Ready(e.clone());
                }
                let (tx, rx) = tokio::sync::watch::channel(None);
                g.inflight = Some(rx.clone());
                OPENS_STARTED.fetch_add(1, Ordering::Relaxed);
                OPENS_IN_FLIGHT.fetch_add(1, Ordering::Relaxed);
                #[cfg(test)]
                self.inner.c_started.fetch_add(1, Ordering::Relaxed);

                // The open task OWNS the outcome: it inserts into the
                // serving map and updates gate state no matter what happens
                // to the caller. This is the cancellation-proofing — the
                // old code did all of this in the request task, and a
                // 30 s client timeout turned every slow replay into a
                // leaked, doomed zombie writer.
                let inner = self.inner.clone();
                let p = prefix.to_string();
                tokio::spawn(async move {
                    // The opener races a deadline. Without one, a single
                    // open that loops inside slatedb recovery makes the
                    // shard unavailable forever — observed live on
                    // eu-central-1 at the end of the soak2 campaign.
                    let mut fut = Box::pin((inner.opener)(p.clone()));
                    let res: Result<anyhow::Result<Arc<ShardEngine>>, tokio::time::error::Elapsed> =
                        tokio::time::timeout(inner.open_deadline, &mut fut).await;
                    OPENS_IN_FLIGHT.fetch_sub(1, Ordering::Relaxed);
                    let out: OpenResult = match res {
                        Ok(Ok(engine)) => {
                            OPENS_COMPLETED.fetch_add(1, Ordering::Relaxed);
                            #[cfg(test)]
                            inner.c_completed.fetch_add(1, Ordering::Relaxed);
                            inner
                                .shards
                                .write()
                                .unwrap()
                                .insert(p.clone(), engine.clone());
                            let mut st = inner.st.lock().unwrap();
                            let g = st.entry(p.clone()).or_default();
                            g.inflight = None;
                            g.opened_at = Some(Instant::now());
                            g.holdoff_until = None;
                            Ok(engine)
                        }
                        Ok(Err(e)) => {
                            OPENS_FAILED.fetch_add(1, Ordering::Relaxed);
                            #[cfg(test)]
                            inner.c_failed.fetch_add(1, Ordering::Relaxed);
                            let msg = format!("{e:#}");
                            tracing::warn!(prefix = %p, "shard open failed: {msg}");
                            if let Ok(mut slot) = LAST_OPEN_ERROR.lock() {
                                *slot = Some(format!("{p}: {msg}"));
                            }
                            note_failed_prefix(&p);
                            let mut st = inner.st.lock().unwrap();
                            let g = st.entry(p.clone()).or_default();
                            g.inflight = None;
                            g.strikes = g.strikes.saturating_add(1);
                            g.holdoff_until = Some(Instant::now() + holdoff_for(g.strikes));
                            Err(msg)
                        }
                        Err(_deadline) => {
                            OPENS_DEADLINED.fetch_add(1, Ordering::Relaxed);
                            OPENS_FAILED.fetch_add(1, Ordering::Relaxed);
                            #[cfg(test)]
                            inner.c_failed.fetch_add(1, Ordering::Relaxed);
                            tracing::warn!(
                                prefix = %p,
                                "shard open exceeded its deadline ({:?}); \
                                 abandoning under supervision",
                                inner.open_deadline
                            );
                            if let Ok(mut slot) = LAST_OPEN_ERROR.lock() {
                                *slot = Some(format!(
                                    "{p}: open exceeded deadline {:?}",
                                    inner.open_deadline
                                ));
                            }
                            note_failed_prefix(&p);
                            {
                                let mut st = inner.st.lock().unwrap();
                                let g = st.entry(p.clone()).or_default();
                                g.inflight = None;
                                g.strikes = g.strikes.saturating_add(1);
                                g.holdoff_until = Some(Instant::now() + holdoff_for(g.strikes));
                            }
                            // SUPERVISED abandonment, not detachment: the
                            // old engine_for dropped abandoned opens on the
                            // floor, and their late completions became the
                            // zombie writers of the reopen storm. The
                            // reaper drives the open to its end and closes
                            // whatever it produces — the slot was forfeited
                            // at the deadline.
                            let p2 = p.clone();
                            tokio::spawn(async move {
                                if let Ok(engine) = fut.await {
                                    OPENS_REAPED.fetch_add(1, Ordering::Relaxed);
                                    tracing::info!(
                                        prefix = %p2,
                                        "deadlined open completed late; closing it"
                                    );
                                    engine.begin_close();
                                }
                            });
                            Err(format!("shard open exceeded {:?}", inner.open_deadline))
                        }
                    };
                    let _ = tx.send(Some(out));
                });
                rx
            }
        };

        // Wait (bounded) for the shared outcome. Callers that give up do
        // not affect the open; they just stop watching.
        let waited = tokio::time::timeout(wait, async {
            loop {
                if let Some(out) = rx.borrow().clone() {
                    return out;
                }
                if rx.changed().await.is_err() {
                    // Sender dropped without a value: treat as failure.
                    return Err("shard open task vanished".to_string());
                }
            }
        })
        .await;

        match waited {
            Ok(Ok(engine)) => OpenOutcome::Ready(engine),
            Ok(Err(msg)) => OpenOutcome::Failed(msg),
            Err(_elapsed) => OpenOutcome::Wait {
                code: "shard_opening",
                retry_after_secs: 3,
            },
        }
    }

    /// Called when a shard engine closes (fenced by a new owner, or a
    /// fatal store error). Evicts it and arms the holdoff — escalating if
    /// the engine died young, because rapid open→die cycles are exactly
    /// the storm this module exists to prevent.
    pub fn notify_closed(&self, prefix: &str) {
        self.inner.shards.write().unwrap().remove(prefix);
        let mut st = self.inner.st.lock().unwrap();
        let g = st.entry(prefix.to_string()).or_default();
        let lifetime = g.opened_at.map(|t| t.elapsed());
        g.opened_at = None;
        match lifetime {
            Some(l) if l >= SHORT_LIVED => g.strikes = 0,
            _ => g.strikes = g.strikes.saturating_add(1),
        }
        g.holdoff_until = Some(Instant::now() + holdoff_for(g.strikes));
    }

    /// Test/ops visibility: is an open currently in flight for `prefix`?
    pub fn opening(&self, prefix: &str) -> bool {
        self.inner
            .st
            .lock()
            .unwrap()
            .get(prefix)
            .map(|g| g.inflight.is_some())
            .unwrap_or(false)
    }

    #[cfg(test)]
    pub fn reset_counters_for_tests() {
        OPENS_STARTED.store(0, Ordering::Relaxed);
        OPENS_COMPLETED.store(0, Ordering::Relaxed);
        OPENS_FAILED.store(0, Ordering::Relaxed);
        OPENS_COALESCED.store(0, Ordering::Relaxed);
        OPENS_IN_FLIGHT.store(0, Ordering::Relaxed);
        if let Ok(mut slot) = LAST_OPEN_ERROR.lock() {
            *slot = None;
        }
        if let Ok(mut g) = FAILED_PREFIXES.lock() {
            *g = None;
        }
    }

    /// This gate's OWN counters — immune to other tests' engine opens.
    #[cfg(test)]
    pub fn instance_counters(&self) -> (u64, u64, u64, u64) {
        (
            self.inner.c_started.load(Ordering::Relaxed),
            self.inner.c_completed.load(Ordering::Relaxed),
            self.inner.c_failed.load(Ordering::Relaxed),
            self.inner.c_coalesced.load(Ordering::Relaxed),
        )
    }

    #[cfg(test)]
    pub fn counters_for_tests() -> (u64, u64, u64, u64) {
        (
            OPENS_STARTED.load(Ordering::Relaxed),
            OPENS_COMPLETED.load(Ordering::Relaxed),
            OPENS_FAILED.load(Ordering::Relaxed),
            OPENS_COALESCED.load(Ordering::Relaxed),
        )
    }
}

/// Canonical shard-DB path for a topology prefix. ONE definition — the
/// static audit found history2 derived its path independently and
/// landed BESIDE the shards/ tree ("01/history2", or "/history2" for a
/// one-shard root) instead of inside the shard it belongs to.
pub fn shard_db_path(prefix: &str) -> String {
    if prefix.is_empty() {
        "shards/root".to_string()
    } else if prefix.contains('/') {
        // Already a full path (tests pass explicit roots).
        prefix.to_string()
    } else {
        format!("shards/{prefix}")
    }
}

/// The shard's shared history-v2 partition, ALWAYS under the shard DB's
/// own path so ownership (clone/split/move) travels with the shard.
pub fn history2_path(prefix: &str) -> String {
    format!("{}/history2", shard_db_path(prefix))
}

#[cfg(test)]
mod path_tests {
    use super::*;

    #[test]
    fn history2_lives_under_the_shard_db_path() {
        assert_eq!(shard_db_path(""), "shards/root");
        assert_eq!(history2_path(""), "shards/root/history2");
        assert_eq!(shard_db_path("01"), "shards/01");
        assert_eq!(history2_path("01"), "shards/01/history2");
        assert_eq!(history2_path("a/b"), "a/b/history2");
    }
}
