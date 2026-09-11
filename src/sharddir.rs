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
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};
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

mod health;
pub(crate) use health::ShardHealth;

/// How long a never-ready instance may stay unready before it exits.
///
/// R23-5: once readiness reports 503 the load balancer stops sending the
/// requests that would trigger another open attempt, so the instance can
/// sit unready forever even after the store heals — a zombie holding a
/// slot. Exiting is strictly better: the platform restarts it, the
/// startup canary re-runs against the healed store, and a genuinely
/// broken deployment crash-loops visibly instead of idling in an
/// ambiguous state. 0 disables.
pub(crate) fn unready_exit_after(cfg: &crate::config::ShardRuntimeConfig) -> Duration {
    Duration::from_secs(cfg.unready_exit_after_secs)
}

/// Watchdog: exit if this instance has been unready for too long without
/// ever having opened a shard.
/// The unready watchdog's POLICY (PR 4.1): a pure state machine over
/// MONOTONIC readings. It knows nothing about sleeping or about how the
/// process terminates — the task adapter below owns those. Elapsed time
/// is measured in the monotonic domain by construction, so a wall-clock
/// step (NTP correction, VM restore) can neither expire the window
/// early (forward jump) nor postpone or suppress expiry (backward jump)
/// — the regression the first PR-4 migration introduced by subtracting
/// `TrustedNow` values.
#[derive(Debug, Default)]
pub(crate) struct UnreadyWindow {
    since: Option<crate::runtime::MonotonicNow>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum WatchdogDecision {
    /// Ready (or never unready): no window is open.
    Healthy,
    /// Unready for `elapsed`, still inside the limit.
    Waiting { elapsed: Duration },
    /// Unready for at least the limit.
    Expired { elapsed: Duration },
}

impl UnreadyWindow {
    /// Observe one sample. A ready observation closes the window; the
    /// first unready observation after that opens a FRESH one.
    pub(crate) fn observe(
        &mut self,
        unready: bool,
        now: crate::runtime::MonotonicNow,
        limit: Duration,
    ) -> WatchdogDecision {
        if !unready {
            self.since = None;
            return WatchdogDecision::Healthy;
        }
        let since = *self.since.get_or_insert(now);
        let elapsed = now.since(since);
        if elapsed >= limit {
            WatchdogDecision::Expired { elapsed }
        } else {
            WatchdogDecision::Waiting { elapsed }
        }
    }
}

/// The watchdog TASK ADAPTER: samples readiness on the injected clock's
/// cadence, feeds the pure policy monotonic readings, and — until WP-15
/// task supervision gives critical tasks a result policy — keeps the
/// survival `process::exit` when the policy says Expired.
#[expect(
    clippy::let_underscore_must_use,
    reason = "spawn_unready_watchdog; the supervisor rejects a spawn only while it is stopping, when no exit deadline is owed; a rejected watchdog has nothing left to police"
)]
pub(crate) fn spawn_unready_watchdog(
    cfg: &crate::config::ShardRuntimeConfig,
    clock: std::sync::Arc<dyn crate::runtime::Clock>,
    tasks: &crate::tasks::TaskSupervisor,
    directory: crate::shard_directory::ShardDirectory,
) {
    let limit = unready_exit_after(cfg);
    if limit.is_zero() {
        return;
    }
    let _ = tasks.spawn(
        "unready-watchdog",
        crate::tasks::Policy::Critical,
        move |cancel| async move {
            let mut window = UnreadyWindow::default();
            loop {
                tokio::select! {
                    _ = cancel.cancelled() => return crate::tasks::TaskResult::Done,
                    _ = clock.sleep(Duration::from_secs(10)) => {}
                }
                let reason = directory.unready_reason();
                match window.observe(reason.is_some(), clock.monotonic(), limit) {
                    WatchdogDecision::Expired { elapsed } => {
                        tracing::error!(
                            "shard readiness failed for {:?} ({}); \
                         exiting so the platform restarts this instance rather than \
                         leaving it in rotation-limbo",
                            elapsed,
                            reason.unwrap_or_default(),
                        );
                        std::process::exit(1);
                    }
                    WatchdogDecision::Waiting { .. } | WatchdogDecision::Healthy => {}
                }
            }
        },
    );
}

#[cfg(test)]
mod watchdog_policy_tests;

pub(crate) fn stats_json() -> serde_json::Value {
    serde_json::json!({
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
/// PR 6.1-B: the identity of ONE open attempt's engine within its
/// directory. A close notification carries the incarnation it belongs
/// to, and the serving map evicts only the resident with that identity:
/// an old engine's late close (the db reports its close reason after
/// `begin_close` already notified once) cannot remove the replacement
/// that opened after the holdoff.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) struct EngineIncarnation(u64);

/// A resident of the serving map: the engine and the incarnation the
/// gate minted for the open that produced it.
#[derive(Clone)]
pub(crate) struct Resident {
    pub engine: Arc<ShardEngine>,
    pub incarnation: EngineIncarnation,
}

// mt-lint: allow(name-keyed-map): shard prefix -> resident engine (layout-4 prefixes, not stream names)
///
/// LOCK ORDER (PR 6.1.2-A) — the ONE permitted order in this module is
/// **gate state (`GateInner::st`) first, serving map second**. Every
/// operation that needs both takes them in that order and no other.
///
/// The inverse order is a real deadlock, not a theoretical one: 6.1.1-B
/// made retirement hold this map's write guard while arming the holdoff,
/// which locks `st`, and `get_or_open` re-checks this map while holding
/// `st`. Because `st` is ONE mutex shared across every prefix, the two
/// operations deadlocked even when they named DIFFERENT shards.
pub(crate) type ServingMap = Arc<RwLock<HashMap<String, Resident>>>;

/// Opens the shard log for `prefix`; the incarnation is the identity the
/// engine's close notification must carry.
pub(crate) type OpenFn = Box<
    dyn Fn(
            String,
            EngineIncarnation,
        ) -> futures_util::future::BoxFuture<'static, anyhow::Result<Arc<ShardEngine>>>
        + Send
        + Sync,
>;

#[derive(Default)]
struct PrefixGate {
    /// A replacement cannot open until the retired owner's workers AND stores
    /// terminate. A caller timeout never discards this authority.
    closing: Option<crate::shard::EngineShutdown>,
    /// A deadlined open is still owned until its reaper settles it.
    reaping: bool,
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

/// Arm the anti-flap holdoff for `prefix` under an ALREADY-HELD gate
/// state. Escalates when the engine died young, because rapid
/// open->die cycles are the storm this module exists to prevent.
///
/// PR 6.1.2-A: the one arming body. It takes the guard rather than the
/// lock so that a caller holding the gate state can arm WITHOUT ever
/// reaching for `st` in the forbidden order (see `ServingMap`).
fn arm_holdoff_locked(st: &mut HashMap<String, PrefixGate>, prefix: &str) {
    let g = st.entry(prefix.to_string()).or_default();
    let lifetime = g.opened_at.map(|t| t.elapsed());
    g.opened_at = None;
    match lifetime {
        Some(l) if l >= SHORT_LIVED => g.strikes = 0,
        _ => g.strikes = g.strikes.saturating_add(1),
    }
    g.holdoff_until = Some(Instant::now() + holdoff_for(g.strikes));
}

fn holdoff_for(strikes: u32) -> Duration {
    let mult = 1u32 << strikes.min(5); // 3s,6s,12s,24s,48s,96s→cap
    (HOLDOFF_BASE * mult).min(HOLDOFF_CAP)
}

struct GateInner {
    health: ShardHealth,
    stopping: AtomicBool,
    shards: ServingMap,
    opener: OpenFn,
    /// Incarnations are minted per open attempt, per gate.
    next_incarnation: AtomicU64,
    // mt-lint: allow(name-keyed-map): shard prefix -> open/park gate
    ///
    /// LOCK ORDER (PR 6.1.2-A): this mutex is taken **before** the
    /// serving map, never after — see `ServingMap`. It is a single
    /// mutex for ALL prefixes, so an inverted acquisition deadlocks
    /// shards that never meet.
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
    /// PR 6.1.2-A: the forced-interleaving park. Per GATE INSTANCE, not
    /// process-global and not keyed by prefix name: two directories in
    /// one test binary must never park each other's opens.
    #[cfg(test)]
    park: GatePark,
}

/// PR 6.1.2-A: parks an open INSIDE the gate state lock so a test can
/// force the interleaving that the old retirement order deadlocked on.
///
/// This cannot be one of the `failpoints` registry's pause sites: those
/// await a `tokio::sync::Notify`, and the site holds a std `MutexGuard`,
/// which must never be held across an await. So it is a condvar, and the
/// parked thread blocks. It exists ONLY under `cfg(test)`; it is not a
/// third lock in the retirement path.
#[cfg(test)]
#[derive(Default)]
struct ParkState {
    /// The prefix whose next open parks, if any.
    prefix: Option<String>,
    arrived: bool,
}

#[cfg(test)]
#[derive(Default)]
pub(crate) struct GatePark {
    st: Mutex<ParkState>,
    cv: std::sync::Condvar,
}

#[cfg(test)]
impl GatePark {
    /// Park the next open of `prefix` while it holds the gate state.
    pub(crate) fn arm(&self, prefix: &str) {
        let mut s = self.st.lock().unwrap();
        s.prefix = Some(prefix.to_string());
        s.arrived = false;
    }

    /// Let the parked open continue.
    pub(crate) fn release(&self) {
        self.st.lock().unwrap().prefix = None;
        self.cv.notify_all();
    }

    /// Whether an open reached the park within `deadline`.
    pub(crate) fn wait_arrived(&self, deadline: Duration) -> bool {
        let start = Instant::now();
        loop {
            if self.st.lock().unwrap().arrived {
                return true;
            }
            if start.elapsed() > deadline {
                return false;
            }
            std::thread::sleep(Duration::from_millis(2));
        }
    }

    /// The site: blocks while armed for `prefix`.
    fn hit(&self, prefix: &str) {
        let mut s = self.st.lock().unwrap();
        if s.prefix.as_deref() != Some(prefix) {
            return;
        }
        s.arrived = true;
        while s.prefix.as_deref() == Some(prefix) {
            s = self.cv.wait(s).unwrap();
        }
    }
}

/// What `retire_resident` did with the slot. `Kept` means the decision
/// declined and the SAME resident is still serving; `Absent` means there
/// was nothing to retire and the decision was never consulted.
pub(crate) enum Retirement {
    Retired(Arc<ShardEngine>),
    Kept,
    Absent,
}

/// What a caller gets back. `Wait` is always retryable and never means the
/// open was abandoned — the open (if any) continues in its own task.
pub(crate) enum OpenOutcome {
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
pub(crate) struct OpenGate {
    inner: Arc<GateInner>,
}

impl OpenGate {
    #[expect(
        clippy::unwrap_used,
        reason = "OpenGate::unready_reason; a poisoned gate state may hold a half-recorded open, close or holdoff for a prefix; recovering it could serve, reopen or reap the wrong incarnation"
    )]
    pub(crate) fn unready_reason(&self) -> Option<String> {
        self.inner.health.unready_reason().or_else(|| {
            self.inner
                .st
                .lock()
                .unwrap()
                .iter()
                .find_map(|(prefix, gate)| {
                    gate.closing
                        .as_ref()?
                        .failure()
                        .map(|failure| format!("{prefix}: {failure}"))
                })
        })
    }

    pub(crate) fn new(shards: ServingMap, opener: OpenFn, open_deadline: Duration) -> Self {
        OpenGate {
            inner: Arc::new(GateInner {
                health: ShardHealth::default(),
                stopping: AtomicBool::new(false),
                shards,
                opener,
                next_incarnation: AtomicU64::new(0),
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
                #[cfg(test)]
                park: GatePark::default(),
            }),
        }
    }

    /// Get the engine for `prefix`, starting (or joining) a single-flight
    /// open if needed, waiting at most `wait` for it.
    #[expect(
        clippy::too_many_lines,
        reason = "OpenGate::get_or_open; the single-flight decision, the owned open task with its deadline and reaper, and the bounded wait are one cancellation-proofing sequence; splitting it would hide which step owns the outcome a caller that gives up leaves behind"
    )]
    #[expect(
        clippy::disallowed_methods,
        reason = "OpenGate::get_or_open; the open task must outlive any caller that gives up and the reaper drives an abandoned open to its end; supervising them under a request task would recreate the leaked zombie writers the owned task replaced"
    )]
    #[expect(
        clippy::let_underscore_must_use,
        reason = "OpenGate::get_or_open; every subscriber may have given up before the outcome lands; a send with no receivers has nothing to notify and the serving map already holds the result"
    )]
    #[expect(
        clippy::unwrap_used,
        reason = "OpenGate::get_or_open; a poisoned gate state or serving map may hold a half-recorded open, retirement or holdoff; recovering either could serve, reopen or reap the wrong incarnation"
    )]
    pub(crate) async fn get_or_open(&self, prefix: &str, wait: Duration) -> OpenOutcome {
        if self.inner.stopping.load(Ordering::SeqCst) {
            return closing_outcome();
        }
        if let Some(r) = self.inner.shards.read().unwrap().get(prefix) {
            return if r.engine.is_closed() {
                closing_outcome()
            } else {
                OpenOutcome::Ready(r.engine.clone())
            };
        }

        let deadline = tokio::time::Instant::now() + wait;
        if let Some(outcome) = self.wait_retired(prefix, wait).await {
            return outcome;
        }
        let wait = deadline.saturating_duration_since(tokio::time::Instant::now());

        // Decide under the state lock: subscribe, holdoff, or start.
        let mut rx = {
            let mut st = self.inner.st.lock().unwrap();
            let g = st.entry(prefix.to_string()).or_default();
            if self.inner.stopping.load(Ordering::SeqCst)
                || g.reaping
                || g.closing
                    .as_ref()
                    .is_some_and(|engine| !engine.terminated())
            {
                return closing_outcome();
            }
            g.closing = None;

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
                // PR 6.1.2-A: the forced-interleaving site. This is the
                // exact window the old retirement order deadlocked
                // against — the gate state is HELD here and the serving
                // map is about to be taken. Tests park here to prove the
                // order is safe; in release builds it does not exist.
                #[cfg(test)]
                self.inner.park.hit(prefix);
                // Raced with a completed open? (map insert happens before
                // the inflight entry clears)
                if let Some(r) = self.inner.shards.read().unwrap().get(prefix) {
                    return if r.engine.is_closed() {
                        closing_outcome()
                    } else {
                        OpenOutcome::Ready(r.engine.clone())
                    };
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
                    let incarnation =
                        EngineIncarnation(inner.next_incarnation.fetch_add(1, Ordering::Relaxed));
                    let mut fut = Box::pin((inner.opener)(p.clone(), incarnation));
                    let res: Result<anyhow::Result<Arc<ShardEngine>>, tokio::time::error::Elapsed> =
                        tokio::time::timeout(inner.open_deadline, &mut fut).await;
                    OPENS_IN_FLIGHT.fetch_sub(1, Ordering::Relaxed);
                    let out: OpenResult = match res {
                        Ok(Ok(engine)) => publish_open(&inner, &p, incarnation, engine),

                        Ok(Err(e)) => {
                            OPENS_FAILED.fetch_add(1, Ordering::Relaxed);
                            #[cfg(test)]
                            inner.c_failed.fetch_add(1, Ordering::Relaxed);
                            let msg = format!("{e:#}");
                            tracing::warn!(prefix = %p, "shard open failed: {msg}");
                            inner.health.failed(&p, format!("{p}: {msg}"));
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
                            inner.health.failed(
                                &p,
                                format!("{p}: open exceeded deadline {:?}", inner.open_deadline),
                            );
                            {
                                let mut st = inner.st.lock().unwrap();
                                let g = st.entry(p.clone()).or_default();
                                g.inflight = None;
                                g.reaping = true;
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
                            let reaper = inner.clone();
                            tokio::spawn(async move {
                                use futures_util::FutureExt;
                                let result = std::panic::AssertUnwindSafe(fut).catch_unwind().await;
                                let engine = match result {
                                    Ok(Ok(engine)) => Some(engine),
                                    Ok(Err(_)) | Err(_) => None,
                                };
                                {
                                    let mut state = reaper.st.lock().unwrap();
                                    let gate = state.entry(p2.clone()).or_default();
                                    gate.reaping = false;
                                    gate.closing =
                                        engine.as_ref().map(|engine| engine.shutdown_handle());
                                }
                                if let Some(engine) = engine {
                                    OPENS_REAPED.fetch_add(1, Ordering::Relaxed);
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

    #[expect(
        clippy::let_underscore_must_use,
        reason = "OpenGate::wait_retired; the retiring engine's wait is a bounded courtesy; whether it finished or timed out, the gate state is re-read before any decision"
    )]
    #[expect(
        clippy::unwrap_used,
        reason = "OpenGate::wait_retired; a poisoned gate state may hold a half-recorded open, close or holdoff for a prefix; recovering it could serve, reopen or reap the wrong incarnation"
    )]
    async fn wait_retired(&self, prefix: &str, wait: Duration) -> Option<OpenOutcome> {
        let retiring = {
            let state = self.inner.st.lock().unwrap();
            if self.inner.stopping.load(Ordering::SeqCst) {
                return Some(closing_outcome());
            }
            let gate = state.get(prefix)?;
            if let Some(until) = gate.holdoff_until
                && until > Instant::now()
            {
                return Some(OpenOutcome::Wait {
                    code: "shard_moving",
                    retry_after_secs: (until - Instant::now()).as_secs().max(1),
                });
            }
            gate.closing.clone()
        };
        if let Some(engine) = retiring
            && !engine.terminated()
        {
            let _ = engine.wait(wait).await;
            if !engine.terminated() {
                return Some(closing_outcome());
            }
        }
        None
    }

    /// Called when a shard engine closes (fenced by a new owner, or a
    /// fatal store error). Evicts it — ONLY if the resident is that very
    /// incarnation; a stale notification for an engine that was already
    /// replaced changes nothing — and arms the holdoff, escalating if the
    /// engine died young, because rapid open→die cycles are exactly the
    /// storm this module exists to prevent. Returns whether it evicted.
    #[expect(
        clippy::unwrap_used,
        reason = "OpenGate::notify_closed; a poisoned gate state or serving map may hold a half-recorded retirement, and the resident matched under the same write guard is still present; recovering the former could retire the wrong incarnation and a fallible remove would deny a resident the guard just proved"
    )]
    pub(crate) fn notify_closed(&self, prefix: &str, incarnation: EngineIncarnation) -> bool {
        // PR 6.1.2-A: gate state FIRST, serving map second — the one
        // permitted order (see `ServingMap`). Holding both also makes
        // the eviction and its holdoff one step, exactly as `retire`.
        let mut st = self.inner.st.lock().unwrap();
        let engine = {
            let mut map = self.inner.shards.write().unwrap();
            match map.get(prefix) {
                Some(r) if r.incarnation == incarnation => map.remove(prefix).unwrap().engine,
                _ => return false,
            }
        };
        if let Some(role) = engine.required_task_failure() {
            self.inner.health.engine_failed(prefix, role);
        }
        st.entry(prefix.to_string()).or_default().closing = Some(engine.shutdown_handle());
        arm_holdoff_locked(&mut st, prefix);
        drop(st);
        // Direct notifications and callbacks use the same owner. Reentrant
        // callbacks find no resident; no gate/map guard is held while closing.
        engine.begin_close();
        true
    }

    /// Retire the resident of `prefix`: remove it, arm the anti-flap
    /// holdoff, and hand the engine back for closing — as ONE decision
    /// that no request observer can see half-applied. `decide` sees the
    /// engine AND its incarnation and may decline, which reinstates the
    /// very same resident under the same guards.
    ///
    /// PR 6.1.2-A: retirement lives HERE, in the component that owns
    /// both pieces of state, so it can take them in the one permitted
    /// order — gate state, then serving map (see `ServingMap`). The
    /// directory used to hold the serving map and then reach for the
    /// gate state, which deadlocked against `get_or_open` across
    /// unrelated prefixes. Closing the engine is deliberately NOT done
    /// here: the caller does it after both guards are released.
    #[expect(
        clippy::unwrap_used,
        reason = "OpenGate::retire_resident; a poisoned gate state or serving map may hold a half-recorded open, retirement or holdoff; recovering either could serve, reopen or reap the wrong incarnation"
    )]
    pub(crate) fn retire_resident(
        &self,
        prefix: &str,
        decide: impl FnOnce(&Arc<ShardEngine>, EngineIncarnation) -> bool,
    ) -> Retirement {
        let mut st = self.inner.st.lock().unwrap();
        let mut map = self.inner.shards.write().unwrap();
        let Some(resident) = map.remove(prefix) else {
            return Retirement::Absent;
        };
        if !decide(&resident.engine, resident.incarnation) {
            map.insert(prefix.to_string(), resident);
            return Retirement::Kept;
        }
        // Armed while the slot is still held: the removal and the
        // holdoff are one decision, never half-applied.
        arm_holdoff_locked(&mut st, prefix);
        st.entry(prefix.to_string()).or_default().closing = Some(resident.engine.shutdown_handle());
        Retirement::Retired(resident.engine)
    }

    #[expect(
        clippy::unwrap_used,
        reason = "OpenGate::stop; a poisoned gate state may hold a half-recorded open, close or holdoff for a prefix; recovering it could serve, reopen or reap the wrong incarnation"
    )]
    pub(crate) fn stop(&self) {
        let _state = self.inner.st.lock().unwrap();
        self.inner.stopping.store(true, Ordering::SeqCst);
    }
    #[expect(
        clippy::unwrap_used,
        reason = "OpenGate::shutdown_pending; a poisoned gate state may hold a half-recorded open, close or holdoff for a prefix; recovering it could serve, reopen or reap the wrong incarnation"
    )]
    pub(crate) fn shutdown_pending(&self) -> (Vec<crate::shard::EngineShutdown>, usize) {
        let state = self.inner.st.lock().unwrap();
        let engines = state
            .values()
            .filter_map(|gate| gate.closing.clone())
            .collect();
        let opens = state
            .values()
            .filter(|gate| gate.inflight.is_some() || gate.reaping)
            .count();
        (engines, opens)
    }

    /// The forced-interleaving park for THIS gate (tests only).
    #[cfg(test)]
    pub(crate) fn test_park(&self) -> &GatePark {
        &self.inner.park
    }

    /// The incarnation of the engine currently serving `prefix`.
    #[cfg(test)]
    pub(crate) fn resident_incarnation(&self, prefix: &str) -> Option<EngineIncarnation> {
        self.inner
            .shards
            .read()
            .unwrap()
            .get(prefix)
            .map(|r| r.incarnation)
    }

    /// Tests only: forget the anti-flap holdoff so a replacement can open
    /// at once (the holdoff itself is proven by the flap test).
    #[cfg(test)]
    pub(crate) fn clear_holdoff(&self, prefix: &str) {
        if let Some(g) = self.inner.st.lock().unwrap().get_mut(prefix) {
            g.holdoff_until = None;
            g.strikes = 0;
        }
    }

    #[cfg(test)]
    pub(crate) fn reset_counters_for_tests() {
        OPENS_STARTED.store(0, Ordering::Relaxed);
        OPENS_COMPLETED.store(0, Ordering::Relaxed);
        OPENS_FAILED.store(0, Ordering::Relaxed);
        OPENS_COALESCED.store(0, Ordering::Relaxed);
        OPENS_IN_FLIGHT.store(0, Ordering::Relaxed);
    }

    /// This gate's OWN counters — immune to other tests' engine opens.
    #[cfg(test)]
    pub(crate) fn instance_counters(&self) -> (u64, u64, u64, u64) {
        (
            self.inner.c_started.load(Ordering::Relaxed),
            self.inner.c_completed.load(Ordering::Relaxed),
            self.inner.c_failed.load(Ordering::Relaxed),
            self.inner.c_coalesced.load(Ordering::Relaxed),
        )
    }
}

/// Canonical shard-DB path for a topology prefix. ONE definition — the
/// static audit found history2 derived its path independently and
/// landed BESIDE the shards/ tree ("01/history2", or "/history2" for a
/// one-shard root) instead of inside the shard it belongs to.
pub(crate) fn shard_db_path(prefix: &str) -> String {
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
pub(crate) fn history2_path(prefix: &str) -> String {
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

#[cfg(test)]
mod health_ownership_tests {
    use super::*;
    #[test]
    fn runtimes_cannot_inherit_or_heal_each_others_health() {
        let a = ShardHealth::default();
        let b = ShardHealth::default();
        for n in 0..100 {
            a.failed(&format!("shard-{n}"), "unavailable".into());
        }
        assert_eq!(a.0.lock().unwrap().failed.len(), 3);
        assert!(a.unready_reason().is_some());
        assert!(b.unready_reason().is_none());
        b.succeeded();
        assert!(a.unready_reason().is_some());
        a.succeeded();
        assert!(a.unready_reason().is_none());
        assert!(ShardHealth::default().unready_reason().is_none());
    }
}

#[expect(
    clippy::unwrap_used,
    reason = "publish_open; a poisoned gate state or serving map may hold a half-recorded open, retirement or holdoff; recovering either could serve, reopen or reap the wrong incarnation"
)]
fn publish_open(
    inner: &GateInner,
    prefix: &str,
    incarnation: EngineIncarnation,
    engine: Arc<ShardEngine>,
) -> OpenResult {
    let refused = {
        let mut state = inner.st.lock().unwrap();
        let gate = state.entry(prefix.to_string()).or_default();
        gate.inflight = None;
        if inner.stopping.load(Ordering::SeqCst) || engine.is_closed() {
            if let Some(role) = engine.required_task_failure() {
                inner.health.engine_failed(prefix, role);
            }
            gate.closing = Some(engine.shutdown_handle());
            true
        } else {
            inner.shards.write().unwrap().insert(
                prefix.to_string(),
                Resident {
                    engine: engine.clone(),
                    incarnation,
                },
            );
            gate.opened_at = Some(Instant::now());
            gate.holdoff_until = None;
            false
        }
    };
    if refused {
        engine.begin_close();
        Err("engine closed or directory stopped during open".into())
    } else {
        OPENS_COMPLETED.fetch_add(1, Ordering::Relaxed);
        inner.health.succeeded();
        #[cfg(test)]
        inner.c_completed.fetch_add(1, Ordering::Relaxed);
        Ok(engine)
    }
}

fn closing_outcome() -> OpenOutcome {
    OpenOutcome::Wait {
        code: "shard_closing",
        retry_after_secs: 1,
    }
}
