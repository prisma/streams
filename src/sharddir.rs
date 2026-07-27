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

// Process-global counters for /v1/debug/store: the cloud-run detector for
// this failure mode is "opens_started climbing while the serving map stays
// empty", and it must be visible without logs.
static OPENS_STARTED: AtomicU64 = AtomicU64::new(0);
static OPENS_COMPLETED: AtomicU64 = AtomicU64::new(0);
static OPENS_FAILED: AtomicU64 = AtomicU64::new(0);
static OPENS_COALESCED: AtomicU64 = AtomicU64::new(0);
static OPENS_IN_FLIGHT: AtomicI64 = AtomicI64::new(0);

pub fn stats_json() -> serde_json::Value {
    serde_json::json!({
        "started": OPENS_STARTED.load(Ordering::Relaxed),
        "completed": OPENS_COMPLETED.load(Ordering::Relaxed),
        "failed": OPENS_FAILED.load(Ordering::Relaxed),
        "coalesced": OPENS_COALESCED.load(Ordering::Relaxed),
        "in_flight": OPENS_IN_FLIGHT.load(Ordering::Relaxed),
    })
}

type OpenResult = Result<Arc<ShardEngine>, String>;
type OpenFn = Box<
    dyn Fn(String) -> futures_util::future::BoxFuture<'static, anyhow::Result<Arc<ShardEngine>>>
        + Send
        + Sync,
>;

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

impl Default for PrefixGate {
    fn default() -> Self {
        PrefixGate {
            inflight: None,
            holdoff_until: None,
            strikes: 0,
            opened_at: None,
        }
    }
}

fn holdoff_for(strikes: u32) -> Duration {
    let mult = 1u32 << strikes.min(5); // 3s,6s,12s,24s,48s,96s→cap
    (HOLDOFF_BASE * mult).min(HOLDOFF_CAP)
}

struct GateInner {
    shards: Arc<RwLock<HashMap<String, Arc<ShardEngine>>>>,
    opener: OpenFn,
    st: Mutex<HashMap<String, PrefixGate>>,
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
    ) -> Self {
        OpenGate {
            inner: Arc::new(GateInner {
                shards,
                opener,
                st: Mutex::new(HashMap::new()),
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

                // The open task OWNS the outcome: it inserts into the
                // serving map and updates gate state no matter what happens
                // to the caller. This is the cancellation-proofing — the
                // old code did all of this in the request task, and a
                // 30 s client timeout turned every slow replay into a
                // leaked, doomed zombie writer.
                let inner = self.inner.clone();
                let p = prefix.to_string();
                tokio::spawn(async move {
                    let res = (inner.opener)(p.clone()).await;
                    OPENS_IN_FLIGHT.fetch_sub(1, Ordering::Relaxed);
                    let out: OpenResult = match res {
                        Ok(engine) => {
                            OPENS_COMPLETED.fetch_add(1, Ordering::Relaxed);
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
                        Err(e) => {
                            OPENS_FAILED.fetch_add(1, Ordering::Relaxed);
                            let msg = format!("{e:#}");
                            tracing::warn!(prefix = %p, "shard open failed: {msg}");
                            let mut st = inner.st.lock().unwrap();
                            let g = st.entry(p.clone()).or_default();
                            g.inflight = None;
                            g.strikes = g.strikes.saturating_add(1);
                            g.holdoff_until = Some(Instant::now() + holdoff_for(g.strikes));
                            Err(msg)
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
