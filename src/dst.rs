//! Simulation testing for the shard data plane (docs/DST.md).
//!
//! **Scope, stated honestly.** This is a *seeded fault-injection suite*
//! over the real single-node data plane, not yet whole-system deterministic
//! simulation in the TigerBeetle sense. What the seed controls is the
//! **fault schedule**: which object-store operation is delayed, which
//! fails, and which succeeds but loses its response. Task scheduling is
//! Tokio's. See docs/DST.md for exactly which guarantees hold today and
//! what closing the gap costs.
//!
//! Two design choices are load-bearing:
//!
//! *Faults are keyed, not drawn in sequence.* The decision for an
//! operation is a pure function of `(seed, path, op, occurrence)`. With one
//! shared RNG stream — the obvious implementation — the *identity* of the
//! operation consuming each random number depends on which task reaches
//! the mutex first, so a seed does not in fact reproduce a fault
//! placement under concurrency. Keying removes that dependency.
//!
//! *Records are identified by attempt, not by payload.* A client retrying
//! an ambiguous append resends the same bytes, so payload equality cannot
//! tell "the system duplicated my write" from "I deliberately wrote it
//! twice". Every attempt carries `(op, attempt)`, which makes that
//! distinction exactly.
//!
//! Invariants:
//!
//!   I1  every acknowledged append is readable
//!   I2  per routing key, acknowledged order is preserved
//!   I3  no attempt is stored twice
//!   I4  a fenced owner acknowledges nothing
//!   I5  a definitively rejected append never appears
//!   I6  an idempotent producer's retry commits at most once

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use object_store::path::Path as ObjPath;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, Result as OsResult,
};

// ---- semantic classification ----------------------------------------

/// Object-store verb.
///
/// `head` is absent deliberately: `ObjectStoreExt::head` is implemented on
/// top of `get_opts`, so a HEAD arrives here as a `Get`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum StoreOp {
    Put,
    Get,
    Delete,
    List,
    Copy,
}

/// Semantic class of the object being touched, from the SAME classifier
/// production telemetry uses (`store_timing::classify`) — so a scenario
/// that targets "the WAL" targets what `/v1/debug/store` calls the WAL.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ObjClass {
    Wal,
    Manifest,
    Sst,
    Fleet,
    Other,
}

impl ObjClass {
    pub fn of(path: &str) -> Self {
        match crate::store_timing::classify(path) {
            0 => ObjClass::Wal,
            1 => ObjClass::Manifest,
            2 => ObjClass::Sst,
            3 => ObjClass::Fleet,
            _ => ObjClass::Other,
        }
    }
}

/// What may happen to one operation.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Toxic {
    None,
    /// Delay, then perform. Milliseconds are *simulated*: scenarios run
    /// with paused time, so a realistic 185 ms costs no wall clock.
    Latency(u64),
    /// Fail before the inner store sees it — the operation definitely did
    /// not take effect.
    ErrorBeforeDispatch,
    /// Perform the operation, then lose the response: the caller sees an
    /// error for work that DID take effect.
    ///
    /// The most valuable fault we inject, because it is the only one that
    /// manufactures **append ambiguity** — the state the producer
    /// idempotence contract exists to resolve, and the state the
    /// eu-central-1 wedge put every client into for twenty minutes
    /// (docs/SOAK-REGIONS.md).
    LostResponse,
}

/// Fault probabilities for one (op, class). Checked against a single roll
/// in the order error → lost-response → latency.
#[derive(Debug, Clone, Copy)]
pub struct FaultPlan {
    pub error_pct: u8,
    pub lost_response_pct: u8,
    pub latency_pct: u8,
    /// Inclusive simulated-millisecond bounds for `Toxic::Latency`.
    pub latency_ms: (u64, u64),
}

impl FaultPlan {
    pub const CLEAN: FaultPlan = FaultPlan {
        error_pct: 0,
        lost_response_pct: 0,
        latency_pct: 0,
        latency_ms: (0, 0),
    };

    /// What we measured against Tigris: 8–185 ms per operation across
    /// regions, iad1 at the top of that range (docs/SOAK-REGIONS.md).
    /// Affordable only because scenarios run with time paused.
    pub const TIGRIS_LATENCY: (u64, u64) = (8, 185);

    pub const fn new(error_pct: u8, lost_response_pct: u8, latency_pct: u8) -> Self {
        FaultPlan {
            error_pct,
            lost_response_pct,
            latency_pct,
            latency_ms: Self::TIGRIS_LATENCY,
        }
    }
}

/// Which operations get which plan, so a scenario can hammer the WAL
/// without disturbing manifest CAS.
#[derive(Debug, Clone)]
pub struct FaultProfile {
    default: FaultPlan,
    by_class: HashMap<ObjClass, FaultPlan>,
    by_op_class: HashMap<(StoreOp, ObjClass), FaultPlan>,
}

impl FaultProfile {
    pub fn uniform(plan: FaultPlan) -> Self {
        FaultProfile {
            default: plan,
            by_class: HashMap::new(),
            by_op_class: HashMap::new(),
        }
    }

    pub fn clean() -> Self {
        Self::uniform(FaultPlan::CLEAN)
    }

    pub fn with_class(mut self, class: ObjClass, plan: FaultPlan) -> Self {
        self.by_class.insert(class, plan);
        self
    }

    pub fn with_op_class(mut self, op: StoreOp, class: ObjClass, plan: FaultPlan) -> Self {
        self.by_op_class.insert((op, class), plan);
        self
    }

    fn plan_for(&self, op: StoreOp, class: ObjClass) -> FaultPlan {
        self.by_op_class
            .get(&(op, class))
            .or_else(|| self.by_class.get(&class))
            .copied()
            .unwrap_or(self.default)
    }
}

// ---- mechanism coverage ---------------------------------------------

/// Named counters for the mechanisms a scenario claims to exercise.
///
/// A fencing scenario in which nothing was ever fenced is not a passing
/// run, it is an invalid one. The docker ladder taught this expensively:
/// D3 and D4 passed their order checks for several passes while never once
/// triggering the mechanism under test (`bench/docker/harness/README.md`).
#[derive(Debug, Default)]
pub struct Coverage {
    counters: Mutex<HashMap<&'static str, u64>>,
}

impl Coverage {
    pub fn hit(&self, name: &'static str) {
        *self.counters.lock().unwrap().entry(name).or_insert(0) += 1;
    }

    pub fn get(&self, name: &str) -> u64 {
        self.counters
            .lock()
            .unwrap()
            .get(name)
            .copied()
            .unwrap_or(0)
    }

    pub fn snapshot(&self) -> Vec<(String, u64)> {
        let mut v: Vec<(String, u64)> = self
            .counters
            .lock()
            .unwrap()
            .iter()
            .map(|(k, v)| (k.to_string(), *v))
            .collect();
        v.sort();
        v
    }

    /// Fail the scenario if a mechanism it claims to test never fired.
    pub fn require(&self, names: &[&str]) -> Result<(), String> {
        let missing: Vec<&str> = names.iter().copied().filter(|n| self.get(n) == 0).collect();
        if missing.is_empty() {
            Ok(())
        } else {
            Err(format!(
                "scenario never exercised {missing:?}; coverage={:?}",
                self.snapshot()
            ))
        }
    }
}

/// Mechanism names. Scenarios `require` the ones they claim to test.
pub mod mech {
    pub const STORE_ERROR: &str = "store_error_before_dispatch";
    pub const STORE_LOST_RESPONSE: &str = "store_success_response_lost";
    pub const STORE_LATENCY: &str = "store_latency_injected";
    pub const APPEND_ACKED: &str = "append_acked";
    pub const APPEND_REJECTED: &str = "append_rejected";
    pub const APPEND_UNKNOWN: &str = "append_unknown_outcome";
    pub const APPEND_RETRIED: &str = "append_retried";
    pub const PRODUCER_DUPLICATE: &str = "producer_duplicate_suppressed";
    pub const OLD_OWNER_FENCED: &str = "old_owner_fenced";
    pub const AFTER_DURABLE_BEFORE_ACK: &str = "after_durable_before_ack";
    pub const CLIENT_DEADLINE_EXPIRED: &str = "client_deadline_expired";
    pub const IN_FLIGHT_AT_FENCE: &str = "append_in_flight_at_fence";
    pub const READ_FROM_HISTORY: &str = "read_served_from_history";
}

// ---- the fault-injecting store --------------------------------------

/// Stable mixing function for the fault key.
///
/// Explicitly not `DefaultHasher`: its output is not guaranteed stable
/// across Rust releases, and a replay that changes when the toolchain
/// changes is not a replay.
fn mix(seed: u64, path: &str, op: u8, n: u64) -> u64 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for b in path.as_bytes() {
        h ^= *b as u64;
        h = h.wrapping_mul(0x1000_0000_1b3);
    }
    let mut z = seed ^ h ^ ((op as u64) << 56) ^ n.wrapping_mul(0x9e37_79b9_7f4a_7c15);
    z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    z ^ (z >> 31)
}

#[derive(Debug)]
struct Hold {
    op: StoreOp,
    class: ObjClass,
    /// Closed when the scenario releases the gate.
    gate: Arc<tokio::sync::Semaphore>,
    /// Bumped when an operation actually parks, so a scenario can wait for
    /// the hold to *engage* instead of sleeping and hoping.
    engaged: Arc<AtomicU64>,
    /// Park at most this many operations; later ones pass straight
    /// through. Without a bound, holding a class deadlocks the scenario
    /// itself: opening the new owner also writes to the held class, so the
    /// handoff can never happen and the gate is never released.
    max_parked: u64,
}

/// Mutable fault state, behind an `Arc` so `delete_stream` — which must
/// return a `'static` stream — can carry it.
#[derive(Debug)]
struct FaultState {
    seed: u64,
    profile: FaultProfile,
    /// Occurrence index per (path, op): the third component of the fault
    /// key, so a retry of the same PUT is a different draw and can
    /// therefore succeed.
    occurrences: Mutex<HashMap<(String, u8), u64>>,
    /// Per-(op, class) operation counts — the protocol-cost ledger.
    /// Budgets like "a reopen may cost at most one WAL replay" are
    /// assertions over these.
    op_counts: Mutex<HashMap<(StoreOp, ObjClass), u64>>,
    hold: Mutex<Option<Hold>>,
    coverage: Arc<Coverage>,
    injected_latency: AtomicU64,
    injected_errors: AtomicU64,
    injected_lost: AtomicU64,
    ops: AtomicU64,
}

impl FaultState {
    fn op_code(op: StoreOp) -> u8 {
        match op {
            StoreOp::Put => 0,
            StoreOp::Get => 2,
            StoreOp::Delete => 4,
            StoreOp::List => 5,
            StoreOp::Copy => 6,
        }
    }

    fn roll(&self, op: StoreOp, class: ObjClass, path: &str) -> Toxic {
        let plan = self.profile.plan_for(op, class);
        if plan.error_pct == 0 && plan.lost_response_pct == 0 && plan.latency_pct == 0 {
            return Toxic::None;
        }
        let code = Self::op_code(op);
        let n = {
            let mut occ = self.occurrences.lock().unwrap();
            let e = occ.entry((path.to_string(), code)).or_insert(0);
            *e += 1;
            *e
        };
        let z = mix(self.seed, path, code, n);
        let x = (z % 100) as u8;
        let err_hi = plan.error_pct;
        let lost_hi = err_hi.saturating_add(plan.lost_response_pct);
        let lat_hi = lost_hi.saturating_add(plan.latency_pct);
        if x < err_hi {
            Toxic::ErrorBeforeDispatch
        } else if x < lost_hi {
            Toxic::LostResponse
        } else if x < lat_hi {
            let (lo, hi) = plan.latency_ms;
            let span = hi.saturating_sub(lo).saturating_add(1).max(1);
            Toxic::Latency(lo + (z >> 8) % span)
        } else {
            Toxic::None
        }
    }

    /// Latency / hold / error decision, shared by every verb.
    /// `Ok(true)` means "perform it, then discard the response".
    async fn gate(&self, op: StoreOp, path: &str) -> OsResult<bool> {
        self.ops.fetch_add(1, Ordering::Relaxed);
        let class = ObjClass::of(path);
        *self
            .op_counts
            .lock()
            .unwrap()
            .entry((op, class))
            .or_insert(0) += 1;

        let held = {
            let h = self.hold.lock().unwrap();
            match h.as_ref() {
                Some(hold) if hold.op == op && hold.class == class => {
                    Some((hold.gate.clone(), hold.engaged.clone(), hold.max_parked))
                }
                _ => None,
            }
        };
        if let Some((gate, engaged, max_parked)) = held {
            // fetch_add returns the PREVIOUS value, so this parks exactly
            // the first `max_parked` matching operations.
            if engaged.fetch_add(1, Ordering::SeqCst) < max_parked {
                // resolves only once the scenario closes the gate
                let _ = gate.acquire().await;
            }
        }

        match self.roll(op, class, path) {
            Toxic::None => Ok(false),
            Toxic::Latency(ms) => {
                self.injected_latency.fetch_add(1, Ordering::Relaxed);
                self.coverage.hit(mech::STORE_LATENCY);
                tokio::time::sleep(std::time::Duration::from_millis(ms)).await;
                Ok(false)
            }
            Toxic::ErrorBeforeDispatch => {
                self.injected_errors.fetch_add(1, Ordering::Relaxed);
                self.coverage.hit(mech::STORE_ERROR);
                Err(object_store::Error::Generic {
                    store: "FaultStore",
                    source: "injected fault (before dispatch)".into(),
                })
            }
            Toxic::LostResponse => {
                // Counted by the CALLER, and only when the response is
                // actually discarded. Counting here would let a scenario
                // satisfy `require(STORE_LOST_RESPONSE)` on a verb that
                // silently ignores the decision — anti-vacuity that lies.
                Ok(true)
            }
        }
    }
}

impl FaultState {
    /// Record an actually-applied lost response.
    fn note_lost(&self) {
        self.injected_lost.fetch_add(1, Ordering::Relaxed);
        self.coverage.hit(mech::STORE_LOST_RESPONSE);
    }
}

fn lost_response_error() -> object_store::Error {
    object_store::Error::Generic {
        store: "FaultStore",
        source: "injected fault (response lost after the operation applied)".into(),
    }
}

/// Deterministic fault-injecting `ObjectStore` decorator.
#[derive(Debug)]
pub struct FaultStore {
    inner: Arc<dyn ObjectStore>,
    st: Arc<FaultState>,
}

impl std::fmt::Display for FaultStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "FaultStore({})", self.inner)
    }
}

impl FaultStore {
    pub fn new(inner: Arc<dyn ObjectStore>, seed: u64, profile: FaultProfile) -> Arc<Self> {
        Arc::new(Self {
            inner,
            st: Arc::new(FaultState {
                seed,
                profile,
                occurrences: Mutex::new(HashMap::new()),
                op_counts: Mutex::new(HashMap::new()),
                hold: Mutex::new(None),
                coverage: Arc::new(Coverage::default()),
                injected_latency: AtomicU64::new(0),
                injected_errors: AtomicU64::new(0),
                injected_lost: AtomicU64::new(0),
                ops: AtomicU64::new(0),
            }),
        })
    }

    pub fn uniform(inner: Arc<dyn ObjectStore>, seed: u64, plan: FaultPlan) -> Arc<Self> {
        Self::new(inner, seed, FaultProfile::uniform(plan))
    }

    pub fn coverage(&self) -> Arc<Coverage> {
        self.st.coverage.clone()
    }

    pub fn injected_latency(&self) -> u64 {
        self.st.injected_latency.load(Ordering::Relaxed)
    }
    pub fn injected_errors(&self) -> u64 {
        self.st.injected_errors.load(Ordering::Relaxed)
    }
    pub fn injected_lost(&self) -> u64 {
        self.st.injected_lost.load(Ordering::Relaxed)
    }
    pub fn ops(&self) -> u64 {
        self.st.ops.load(Ordering::Relaxed)
    }

    /// Operations of one (verb, class) so far — the protocol-cost ledger.
    pub fn count(&self, op: StoreOp, class: ObjClass) -> u64 {
        self.st
            .op_counts
            .lock()
            .unwrap()
            .get(&(op, class))
            .copied()
            .unwrap_or(0)
    }

    /// Park the first `max_parked` matching operations until
    /// `release_hold`. The returned counter is bumped when an operation
    /// reaches the gate, so a scenario can wait for the hold to engage
    /// rather than sleeping and hoping.
    ///
    /// `max_parked` matters: the scenario that uses this holds a WAL write
    /// and then opens a second owner on the same shard, and that open
    /// writes to the WAL too. An unbounded hold parks the handoff itself
    /// and the gate is never released.
    pub fn hold_class(&self, op: StoreOp, class: ObjClass, max_parked: u64) -> Arc<AtomicU64> {
        let engaged = Arc::new(AtomicU64::new(0));
        *self.st.hold.lock().unwrap() = Some(Hold {
            op,
            class,
            gate: Arc::new(tokio::sync::Semaphore::new(0)),
            engaged: engaged.clone(),
            max_parked,
        });
        engaged
    }

    pub fn release_hold(&self) {
        if let Some(h) = self.st.hold.lock().unwrap().take() {
            h.gate.close();
        }
    }
}

#[async_trait::async_trait]
impl ObjectStore for FaultStore {
    async fn put_opts(
        &self,
        location: &ObjPath,
        payload: PutPayload,
        opts: PutOptions,
    ) -> OsResult<PutResult> {
        let lose = self.st.gate(StoreOp::Put, location.as_ref()).await?;
        let res = self.inner.put_opts(location, payload, opts).await;
        if lose && res.is_ok() {
            self.st.note_lost();
            return Err(lost_response_error());
        }
        res
    }

    async fn put_multipart_opts(
        &self,
        location: &ObjPath,
        opts: PutMultipartOptions,
    ) -> OsResult<Box<dyn MultipartUpload>> {
        // Latency and pre-dispatch errors only. A "lost" response here
        // would leak an upload the caller can no longer drive, which
        // models nothing real — and per-part/complete faulting needs a
        // wrapped MultipartUpload we have not built (docs/DST.md §8).
        let _ = self.st.gate(StoreOp::Put, location.as_ref()).await?;
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(&self, location: &ObjPath, options: GetOptions) -> OsResult<GetResult> {
        // Reads are faulted for *availability* (the 503 class), never for
        // content. A store that returns wrong bytes is outside the
        // object-store contract, and simulating one would test a system we
        // do not have and cannot ship against.
        let lose = self.st.gate(StoreOp::Get, location.as_ref()).await?;
        let res = self.inner.get_opts(location, options).await;
        if lose && res.is_ok() {
            // The object was read; the caller never sees it. For a GET
            // this is indistinguishable from a transport failure, which
            // is exactly what it models.
            self.st.note_lost();
            return Err(lost_response_error());
        }
        res
    }

    /// Streaming list. Previously delegated straight through, so a
    /// scenario could believe it was faulting listings while GC and
    /// recovery walked an untouched store. Faults apply at two points a
    /// real listing can fail: before the first item, and mid-stream after
    /// partial results (truncation with a terminal error).
    fn list(
        &self,
        prefix: Option<&ObjPath>,
    ) -> futures_util::stream::BoxStream<'static, OsResult<ObjectMeta>> {
        use futures_util::StreamExt;
        let p = prefix.map(|p| p.as_ref().to_string()).unwrap_or_default();
        let class = ObjClass::of(&p);
        let plan = self.st.profile.plan_for(StoreOp::List, class);
        if plan.error_pct == 0 && plan.lost_response_pct == 0 && plan.latency_pct == 0 {
            return self.inner.list(prefix);
        }
        let inner = self.inner.list(prefix);
        let st = self.st.clone();
        // Decide once before the first item (fail / delay / proceed), then
        // stream. `unfold` keeps the inner stream owned by the state
        // machine rather than captured by an FnMut closure.
        futures_util::stream::unfold(
            (Some(inner), st, p, 0u64, None::<bool>, false),
            |(mut inner, st, p, mut n, mut lose_mid, mut done)| async move {
                if done {
                    return None;
                }
                if lose_mid.is_none() {
                    match st.gate(StoreOp::List, &p).await {
                        Err(e) => {
                            return Some((Err(e), (None, st, p, n, Some(false), true)));
                        }
                        Ok(l) => lose_mid = Some(l),
                    }
                }
                if lose_mid == Some(true) && n >= 3 {
                    // Truncate after partial results with a terminal error:
                    // a real S3 listing failure mode, and the point at
                    // which the lost response is actually applied.
                    st.note_lost();
                    return Some((Err(lost_response_error()), (None, st, p, n, lose_mid, true)));
                }
                let item = match inner.as_mut() {
                    Some(stream) => {
                        use futures_util::StreamExt;
                        stream.next().await
                    }
                    None => None,
                };
                match item {
                    Some(v) => {
                        n += 1;
                        Some((v, (inner, st, p, n, lose_mid, false)))
                    }
                    None => {
                        done = true;
                        let _ = done;
                        None
                    }
                }
            },
        )
        .boxed()
    }

    async fn list_with_delimiter(&self, prefix: Option<&ObjPath>) -> OsResult<ListResult> {
        let p = prefix.map(|p| p.as_ref().to_string()).unwrap_or_default();
        let lose = self.st.gate(StoreOp::List, &p).await?;
        let res = self.inner.list_with_delimiter(prefix).await;
        if lose && res.is_ok() {
            self.st.note_lost();
            return Err(lost_response_error());
        }
        res
    }

    async fn copy_opts(&self, from: &ObjPath, to: &ObjPath, opts: CopyOptions) -> OsResult<()> {
        let lose = self.st.gate(StoreOp::Copy, from.as_ref()).await?;
        let res = self.inner.copy_opts(from, to, opts).await;
        if lose && res.is_ok() {
            self.st.note_lost();
            return Err(lost_response_error());
        }
        res
    }

    /// Deletes are the GC path — the one that deleted live SSTs under a
    /// zombie DB in ladder pass 3. Leaving the most dangerous verb
    /// unfaulted was a real gap. A faulted delete leaves the object in
    /// place, which is how garbage accumulates when GC cannot keep up.
    fn delete_stream(
        &self,
        locations: futures_util::stream::BoxStream<'static, OsResult<ObjPath>>,
    ) -> futures_util::stream::BoxStream<'static, OsResult<ObjPath>> {
        use futures_util::StreamExt;
        let inner = self.inner.clone();
        let st = self.st.clone();
        locations
            .then(move |loc| {
                let inner = inner.clone();
                let st = st.clone();
                async move {
                    let p = loc?;
                    let lose = st.gate(StoreOp::Delete, p.as_ref()).await?;
                    let one = {
                        let p = p.clone();
                        futures_util::stream::once(async move { Ok(p) }).boxed()
                    };
                    let mut s = inner.delete_stream(one);
                    let res = match s.next().await {
                        Some(r) => r,
                        None => Ok(p),
                    };
                    if lose && res.is_ok() {
                        st.note_lost();
                        return Err(lost_response_error());
                    }
                    res
                }
            })
            .boxed()
    }
}

// ---- the tracing store -----------------------------------------------

/// Outcome of one traced operation, coarse enough that traces stay
/// comparable across runs and stores.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TraceOutcome {
    /// Dispatched but not yet resolved — only ever visible in a snapshot
    /// taken while the operation is in flight.
    Pending,
    Ok,
    NotFound,
    AlreadyExists,
    Precondition,
    NotModified,
    NotSupported,
    NotImplemented,
    InvalidPath,
    /// The consumer abandoned a streaming operation before it finished
    /// (dropped the list stream mid-page). Recorded, not hidden.
    Cancelled,
    /// Everything else (Generic, JoinError, …): the trace records THAT it
    /// failed, not the store's prose.
    Error,
}

impl TraceOutcome {
    fn of(e: &object_store::Error) -> Self {
        use object_store::Error as E;
        match e {
            E::NotFound { .. } => Self::NotFound,
            E::AlreadyExists { .. } => Self::AlreadyExists,
            E::Precondition { .. } => Self::Precondition,
            E::NotModified { .. } => Self::NotModified,
            E::NotSupported { .. } => Self::NotSupported,
            E::NotImplemented { .. } => Self::NotImplemented,
            E::InvalidPath { .. } => Self::InvalidPath,
            _ => Self::Error,
        }
    }
}

/// What one trace entry MEANS — the discriminator `operation_counts()`
/// keys on (PR 3.2: phases were previously hidden in `detail` strings,
/// which let a diagnostic observation masquerade as an attempted store
/// operation and double-count deletes).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TraceEventKind {
    /// One attempted store operation (put/get/list/copy/multipart leg).
    Operation,
    /// Diagnostic observation: one input item a traced delete stream
    /// consumed. An `Ok` input IS an attempted delete (the inner store
    /// received it); an `Err` input is an observation only — the store
    /// never saw a path.
    DeleteInput,
    /// Diagnostic observation: one item the inner delete stream
    /// returned. Never counted as an operation — the trait does not
    /// promise results correspond to inputs (batching stores coalesce).
    DeleteResult,
}

/// One trace entry, in trace-lock acquisition order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoreTraceEvent {
    /// Monotonic event id, unique for the life of the `TraceStore` and
    /// allocated under the same lock that inserts the event — so the
    /// vector order IS id order. NOT dense: `reset()` keeps the id
    /// space moving, and stream lifetime tokens consume ids without
    /// producing events.
    pub seq: u64,
    pub kind: TraceEventKind,
    pub op: StoreOp,
    pub class: ObjClass,
    /// The path as recorded: verbatim only when the store was built with
    /// `verbatim`, segment-redacted otherwise. Credentials and headers
    /// are never recorded — the `ObjectStore` interface never exposes
    /// them to a decorator.
    pub path: String,
    /// Payload length for puts, requested span for bounded/suffix ranged
    /// gets. `None` when not applicable or unknowable at dispatch
    /// (offset ranges, full gets, deletes, lists, copies).
    pub bytes: Option<u64>,
    pub outcome: TraceOutcome,
    /// First 16 hex chars of the payload sha256 — recorded ONLY when the
    /// store was built with `content_hashes: true`. Payload bytes
    /// themselves are never retained either way.
    pub content_hash: Option<String>,
    /// Free-form qualifier: copy destination, `head` marker, multipart
    /// markers (`multipart-open` / `part` / `complete parts=N` / `abort`).
    pub detail: Option<String>,
}

/// First 16 hex chars of sha256 — enough to distinguish payloads or path
/// segments in a trace without retaining them.
fn sha16(bytes: &[u8]) -> String {
    use sha2::Digest;
    let d = sha2::Sha256::digest(bytes);
    crate::crypto::hex(&d[..8])
}

/// Segment-preserving redaction. Tenant- and stream-derived segments are
/// replaced by a 16-hex-char sha256 prefix; the structural tokens the
/// `ObjClass` classifier keys on (`wal`, `compacted`, `fleet`, `routers`,
/// `shards`) survive verbatim, as does the `manifest` marker and the file
/// extension (`.sst` is itself a classification signal). The redacted
/// path therefore classifies identically to the original while leaking no
/// tenant material.
fn redact_path(path: &str) -> String {
    path.split('/')
        .map(|seg| {
            if matches!(seg, "wal" | "compacted" | "fleet" | "routers" | "shards") {
                return seg.to_string();
            }
            let h = sha16(seg.as_bytes());
            let stem = if seg.contains("manifest") {
                format!("manifest-{h}")
            } else {
                h
            };
            match seg.rsplit_once('.') {
                Some((_, ext)) if !ext.is_empty() => format!("{stem}.{ext}"),
                _ => stem,
            }
        })
        .collect::<Vec<_>>()
        .join("/")
}

/// Everything the trace correlates, behind ONE mutex (PR 3.2). The
/// previous shape allocated ids with relaxed atomics BEFORE taking the
/// vector lock, so two concurrent starts could insert out of id order
/// and completion — which derived a vector index from the id — would
/// silently resolve neither event: both stayed `Pending`, `in_flight`
/// never drained, and `reset()` panicked forever. Ids are now allocated
/// and inserted in the same critical section; completion looks events
/// up through an id→index map, never by arithmetic on insertion order.
#[derive(Debug, Default)]
struct TraceLog {
    /// Monotonic id source. Never rewinds — `reset()` clears the window
    /// but keeps the id space moving, so a stale completion can never
    /// land on a younger event.
    next_seq: u64,
    /// The current trace window, in id (= lock-acquisition) order.
    events: Vec<StoreTraceEvent>,
    /// id → index into `events`, for the CURRENT window only.
    // mt-lint: allow(name-keyed-map): keyed by trace event id
    index: HashMap<u64, usize>,
    /// Ids of operations whose LIFETIME is still open: point operations
    /// between begin and finish, streams between creation and
    /// exhaustion/drop. Lifetime is deliberately distinct from outcome —
    /// a list stream that has already yielded an error is still alive
    /// (the trait lets it keep serving items), and `reset()` refuses
    /// while ANY lifetime is open.
    active: std::collections::HashSet<u64>,
}

/// Shared trace state: the correlated log plus the two immutable
/// recording options.
#[derive(Debug)]
struct TraceState {
    log: Mutex<TraceLog>,
    redact: bool,
    content_hashes: bool,
}

impl TraceState {
    #[allow(clippy::too_many_arguments)]
    fn event(
        &self,
        seq: u64,
        kind: TraceEventKind,
        op: StoreOp,
        path: &str,
        bytes: Option<u64>,
        outcome: TraceOutcome,
        content_hash: Option<String>,
        detail: Option<String>,
    ) -> StoreTraceEvent {
        StoreTraceEvent {
            seq,
            kind,
            op,
            class: ObjClass::of(path),
            path: if self.redact {
                redact_path(path)
            } else {
                path.to_string()
            },
            bytes,
            outcome,
            content_hash,
            detail,
        }
    }

    /// Open one operation: allocate the id AND insert the Pending event
    /// in the same critical section, and mark the lifetime active.
    fn begin(
        &self,
        op: StoreOp,
        path: &str,
        bytes: Option<u64>,
        content_hash: Option<String>,
        detail: Option<String>,
    ) -> u64 {
        let ev = |seq| {
            self.event(
                seq,
                TraceEventKind::Operation,
                op,
                path,
                bytes,
                TraceOutcome::Pending,
                content_hash.clone(),
                detail.clone(),
            )
        };
        let mut log = self.log.lock().unwrap();
        let seq = log.next_seq;
        log.next_seq += 1;
        let e = ev(seq);
        let at = log.events.len();
        log.events.push(e);
        log.index.insert(seq, at);
        log.active.insert(seq);
        seq
    }

    /// Open a lifetime WITHOUT an event of its own: a traced delete
    /// stream is observed through its per-item `DeleteInput` /
    /// `DeleteResult` entries, but its lifetime must still hold
    /// `reset()` off until the stream is exhausted or dropped.
    fn begin_lifetime(&self) -> u64 {
        let mut log = self.log.lock().unwrap();
        let seq = log.next_seq;
        log.next_seq += 1;
        log.active.insert(seq);
        seq
    }

    /// Push an already-resolved observation (delete-stream items are
    /// complete facts at the moment they pass through).
    fn observe(
        &self,
        kind: TraceEventKind,
        op: StoreOp,
        path: &str,
        detail: Option<String>,
        outcome: TraceOutcome,
    ) {
        let mut log = self.log.lock().unwrap();
        let seq = log.next_seq;
        log.next_seq += 1;
        let e = self.event(seq, kind, op, path, None, outcome, None, detail);
        let at = log.events.len();
        log.events.push(e);
        log.index.insert(seq, at);
    }

    /// Record an observed outcome WITHOUT retiring the lifetime — first
    /// fact wins. Streaming operations use this for item errors: the
    /// stream stays alive (and keeps blocking `reset()`) after it.
    fn note_outcome(&self, seq: u64, outcome: TraceOutcome) {
        let mut log = self.log.lock().unwrap();
        Self::note_locked(&mut log, seq, outcome);
    }

    fn note_locked(log: &mut TraceLog, seq: u64, outcome: TraceOutcome) {
        if let Some(&at) = log.index.get(&seq) {
            let e = &mut log.events[at];
            if e.outcome == TraceOutcome::Pending {
                e.outcome = outcome;
            }
        }
    }

    /// Close a lifetime exactly once. `final_outcome` applies only when
    /// no earlier fact was recorded (a stream that errored and was then
    /// dropped keeps the error; one that was abandoned untouched records
    /// `Cancelled`; clean exhaustion records `Ok`).
    fn retire(&self, seq: u64, final_outcome: TraceOutcome) {
        let mut log = self.log.lock().unwrap();
        if log.active.remove(&seq) {
            Self::note_locked(&mut log, seq, final_outcome);
        }
    }

    /// Point operations resolve outcome and lifetime together.
    fn finish(&self, seq: u64, outcome: TraceOutcome) {
        let mut log = self.log.lock().unwrap();
        log.active.remove(&seq);
        Self::note_locked(&mut log, seq, outcome);
    }

    fn finish_with<T>(&self, seq: u64, res: &OsResult<T>) {
        self.finish(
            seq,
            match res {
                Ok(_) => TraceOutcome::Ok,
                Err(e) => TraceOutcome::of(e),
            },
        );
    }

    fn hash_payload(&self, payload: &PutPayload) -> Option<String> {
        if !self.content_hashes {
            return None;
        }
        use sha2::Digest;
        let mut h = sha2::Sha256::new();
        for b in payload.iter() {
            h.update(b);
        }
        Some(crate::crypto::hex(&h.finalize()[..8]))
    }
}

/// Owns one streaming operation's lifetime. Dropping it retires the
/// lifetime exactly once (the `active` set makes retirement idempotent);
/// the drop-path outcome is `Cancelled`, which only sticks if no real
/// fact (error, clean exhaustion) was recorded first.
struct StreamTraceGuard {
    st: Arc<TraceState>,
    seq: u64,
}

impl Drop for StreamTraceGuard {
    fn drop(&mut self) {
        self.st.retire(self.seq, TraceOutcome::Cancelled);
    }
}

/// Tracing `ObjectStore` decorator for refactor comparisons.
///
/// Records every operation the client dispatches, so a refactor PR can
/// assert the exact object-store operation trace is unchanged by code
/// movement. Behavior is pass-through: nothing is delayed, altered, or
/// dropped.
///
/// *Ordering contract (PR 3.2).* The trace order is the order in which
/// operations acquired the trace lock at dispatch: id allocation and
/// event insertion happen in one critical section, so the event vector
/// is always in id order. Under a single client that is exactly the
/// call order; under concurrent clients it is a legal serialization of
/// their dispatches (which is the strongest order that exists for
/// concurrent starts). Completion order is not recorded — outcomes are
/// filled in afterwards BY ID through the log's id→index map, and a
/// snapshot taken mid-flight shows `TraceOutcome::Pending`.
#[derive(Debug)]
pub struct TraceStore {
    inner: Arc<dyn ObjectStore>,
    st: Arc<TraceState>,
}

impl std::fmt::Display for TraceStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "TraceStore({})", self.inner)
    }
}

impl TraceStore {
    /// Safe default: paths redacted, payload hashes off.
    pub fn new(inner: Arc<dyn ObjectStore>) -> Arc<Self> {
        Self::with_options(inner, true, false)
    }

    /// Verbatim paths. Only for fixtures whose paths carry no tenant or
    /// stream material — redaction is the default precisely because real
    /// paths embed both.
    pub fn verbatim(inner: Arc<dyn ObjectStore>) -> Arc<Self> {
        Self::with_options(inner, false, false)
    }

    /// Explicit knobs. `content_hashes` opts into retaining a 16-hex-char
    /// sha256 prefix of each put payload; payload bytes themselves are
    /// never retained either way.
    pub fn with_options(
        inner: Arc<dyn ObjectStore>,
        redact: bool,
        content_hashes: bool,
    ) -> Arc<Self> {
        Arc::new(Self {
            inner,
            st: Arc::new(TraceState {
                log: Mutex::new(TraceLog::default()),
                redact,
                content_hashes,
            }),
        })
    }

    /// Snapshot of every trace entry so far — operations AND the
    /// diagnostic delete observations — in id order. This is the
    /// observation report; `operation_counts()` is the operation ledger.
    pub fn events(&self) -> Vec<StoreTraceEvent> {
        self.st.log.lock().unwrap().events.clone()
    }

    /// Drop everything recorded so far. Refuses (panics) while any
    /// operation's LIFETIME is still open — including a list stream
    /// that already yielded an error but has not been exhausted or
    /// dropped, and a delete stream that is still alive. Silently
    /// clearing under an open lifetime would let a late fact outlive
    /// the wipe and corrupt the comparison this type exists to make.
    pub fn reset(&self) {
        let mut log = self.st.log.lock().unwrap();
        if !log.active.is_empty() {
            let n = log.active.len();
            // Release the lock BEFORE panicking: a poisoned trace mutex
            // would turn one refused reset into a wedged harness.
            drop(log);
            panic!("cannot reset TraceStore while operations are active ({n} open)");
        }
        log.events.clear();
        log.index.clear();
    }

    /// (op, class) → attempted-operation count, sorted — the same shape
    /// FaultStore's ledger answers, for whole-trace budget assertions.
    ///
    /// The delete accounting is pinned (PR 3.2): an attempted delete is
    /// each `Ok` INPUT the inner store consumed. Returned results are
    /// never counted (the trait lets stores batch, reorder, coalesce or
    /// drop results), and an input ERROR is never counted (the store
    /// was not handed a path). A cost baseline therefore answers "how
    /// many store operations were attempted", not "how many diagnostic
    /// observations happened" — use `events()` for the latter.
    pub fn operation_counts(&self) -> Vec<(StoreOp, ObjClass, u64)> {
        let mut m: HashMap<(StoreOp, ObjClass), u64> = HashMap::new();
        for e in self.st.log.lock().unwrap().events.iter() {
            let attempted = match e.kind {
                TraceEventKind::Operation => true,
                TraceEventKind::DeleteInput => e.outcome == TraceOutcome::Ok,
                TraceEventKind::DeleteResult => false,
            };
            if attempted {
                *m.entry((e.op, e.class)).or_insert(0) += 1;
            }
        }
        let mut v: Vec<_> = m
            .into_iter()
            .map(|((op, class), n)| (op, class, n))
            .collect();
        v.sort_by_key(|(op, class, _)| (*op as u8, *class as u8));
        v
    }
}

#[async_trait::async_trait]
impl ObjectStore for TraceStore {
    async fn put_opts(
        &self,
        location: &ObjPath,
        payload: PutPayload,
        opts: PutOptions,
    ) -> OsResult<PutResult> {
        let seq = self.st.begin(
            StoreOp::Put,
            location.as_ref(),
            Some(payload.content_length() as u64),
            self.st.hash_payload(&payload),
            None,
        );
        let res = self.inner.put_opts(location, payload, opts).await;
        self.st.finish_with(seq, &res);
        res
    }

    async fn put_multipart_opts(
        &self,
        location: &ObjPath,
        opts: PutMultipartOptions,
    ) -> OsResult<Box<dyn MultipartUpload>> {
        let seq = self.st.begin(
            StoreOp::Put,
            location.as_ref(),
            None,
            None,
            Some("multipart-open".to_string()),
        );
        let res = self.inner.put_multipart_opts(location, opts).await;
        self.st.finish_with(seq, &res);
        res.map(|up| {
            Box::new(TracedMultipart {
                inner: up,
                st: self.st.clone(),
                path: location.as_ref().to_string(),
                parts: 0,
            }) as Box<dyn MultipartUpload>
        })
    }

    async fn get_opts(&self, location: &ObjPath, options: GetOptions) -> OsResult<GetResult> {
        // Span of a ranged read, when the request itself fixes one. An
        // `Offset` span depends on the object size, unknowable at
        // dispatch. HEAD arrives as a Get (`ObjectStoreExt::head` is built
        // on get_opts), marked in `detail` so traces stay comparable.
        let bytes = options.range.as_ref().and_then(|r| match r {
            object_store::GetRange::Bounded(b) => Some(b.end.saturating_sub(b.start)),
            object_store::GetRange::Suffix(n) => Some(*n),
            object_store::GetRange::Offset(_) => None,
        });
        let detail = options.head.then(|| "head".to_string());
        let seq = self
            .st
            .begin(StoreOp::Get, location.as_ref(), bytes, None, detail);
        let res = self.inner.get_opts(location, options).await;
        self.st.finish_with(seq, &res);
        res
    }

    /// Streaming list. The event is recorded at dispatch; the stream
    /// itself is pass-through. Outcome and LIFETIME are deliberately
    /// separate (PR 3.2): an item error records the outcome (first fact
    /// wins) but the stream stays ALIVE — the trait lets it serve more
    /// items — so `reset()` keeps refusing until clean exhaustion or
    /// drop retires the lifetime exactly once. Clean exhaustion upgrades
    /// a never-failed stream to Ok; a stream abandoned before any fact
    /// records Cancelled.
    fn list(
        &self,
        prefix: Option<&ObjPath>,
    ) -> futures_util::stream::BoxStream<'static, OsResult<ObjectMeta>> {
        use futures_util::StreamExt;
        let p = prefix.map(|p| p.as_ref().to_string()).unwrap_or_default();
        let seq = self.st.begin(StoreOp::List, &p, None, None, None);
        let inner = self.inner.list(prefix);
        let guard = StreamTraceGuard {
            st: self.st.clone(),
            seq,
        };
        futures_util::stream::unfold((inner, guard), |(mut inner, guard)| async move {
            match inner.next().await {
                Some(Ok(m)) => Some((Ok(m), (inner, guard))),
                Some(Err(e)) => {
                    // Outcome only: the stream can legally continue.
                    guard.st.note_outcome(guard.seq, TraceOutcome::of(&e));
                    Some((Err(e), (inner, guard)))
                }
                None => {
                    guard.st.retire(guard.seq, TraceOutcome::Ok);
                    None
                }
            }
        })
        .boxed()
    }

    async fn list_with_delimiter(&self, prefix: Option<&ObjPath>) -> OsResult<ListResult> {
        let p = prefix.map(|p| p.as_ref().to_string()).unwrap_or_default();
        let seq = self.st.begin(
            StoreOp::List,
            &p,
            None,
            None,
            Some("with-delimiter".to_string()),
        );
        let res = self.inner.list_with_delimiter(prefix).await;
        self.st.finish_with(seq, &res);
        res
    }

    async fn copy_opts(&self, from: &ObjPath, to: &ObjPath, opts: CopyOptions) -> OsResult<()> {
        let dst = if self.st.redact {
            redact_path(to.as_ref())
        } else {
            to.as_ref().to_string()
        };
        let seq = self.st.begin(
            StoreOp::Copy,
            from.as_ref(),
            None,
            None,
            Some(format!("to={dst}")),
        );
        let res = self.inner.copy_opts(from, to, opts).await;
        self.st.finish_with(seq, &res);
        res
    }

    /// Deletes are traced WITHOUT changing the call shape: exactly one
    /// delegated `delete_stream`, with input items and output items
    /// recorded as typed observations (`DeleteInput` / `DeleteResult`)
    /// as they pass through. The trait does not promise that results
    /// correspond to inputs (an implementation may batch, reorder,
    /// coalesce, or drop results), so NO outcome is ever fabricated: an
    /// empty output stays empty, input errors and inner failures pass
    /// through untouched. The stream also carries a LIFETIME token
    /// (PR 3.2): while it is alive — consuming inputs, producing
    /// outputs — `reset()` refuses; exhaustion or drop retires the
    /// lifetime exactly once.
    fn delete_stream(
        &self,
        locations: futures_util::stream::BoxStream<'static, OsResult<ObjPath>>,
    ) -> futures_util::stream::BoxStream<'static, OsResult<ObjPath>> {
        use futures_util::StreamExt;
        let lifetime = self.st.begin_lifetime();
        let st_in = self.st.clone();
        let traced_in = locations.map(move |loc| {
            match &loc {
                Ok(p) => st_in.observe(
                    TraceEventKind::DeleteInput,
                    StoreOp::Delete,
                    p.as_ref(),
                    None,
                    TraceOutcome::Ok,
                ),
                Err(e) => st_in.observe(
                    TraceEventKind::DeleteInput,
                    StoreOp::Delete,
                    "",
                    None,
                    TraceOutcome::of(e),
                ),
            }
            loc
        });
        let out = self.inner.delete_stream(Box::pin(traced_in));
        let st_out = self.st.clone();
        let guard = StreamTraceGuard {
            st: self.st.clone(),
            seq: lifetime,
        };
        futures_util::stream::unfold((out, guard), move |(mut out, guard)| {
            let st_out = st_out.clone();
            async move {
                match out.next().await {
                    Some(res) => {
                        match &res {
                            Ok(p) => st_out.observe(
                                TraceEventKind::DeleteResult,
                                StoreOp::Delete,
                                p.as_ref(),
                                None,
                                TraceOutcome::Ok,
                            ),
                            Err(e) => st_out.observe(
                                TraceEventKind::DeleteResult,
                                StoreOp::Delete,
                                "",
                                None,
                                TraceOutcome::of(e),
                            ),
                        }
                        Some((res, (out, guard)))
                    }
                    None => {
                        // Clean exhaustion retires the lifetime; the
                        // guard's later drop is then a no-op.
                        guard.st.retire(guard.seq, TraceOutcome::Ok);
                        None
                    }
                }
            }
        })
        .boxed()
    }
}

/// Multipart session wrapper. `StoreOp` has no part variant, so the whole
/// session traces as `Put` events distinguished by `detail`:
/// `multipart-open` at creation (bytes=None), `part` per part (with the
/// part's byte count), `complete parts=N`, `abort`.
#[derive(Debug)]
struct TracedMultipart {
    inner: Box<dyn MultipartUpload>,
    st: Arc<TraceState>,
    /// Raw path, for classification and redaction on each event.
    path: String,
    parts: u64,
}

#[async_trait::async_trait]
impl MultipartUpload for TracedMultipart {
    fn put_part(&mut self, data: PutPayload) -> object_store::UploadPart {
        let seq = self.st.begin(
            StoreOp::Put,
            &self.path,
            Some(data.content_length() as u64),
            self.st.hash_payload(&data),
            Some("part".to_string()),
        );
        self.parts += 1;
        let st = self.st.clone();
        let fut = self.inner.put_part(data);
        Box::pin(async move {
            let res = fut.await;
            st.finish_with(seq, &res);
            res
        })
    }

    async fn complete(&mut self) -> OsResult<PutResult> {
        let seq = self.st.begin(
            StoreOp::Put,
            &self.path,
            None,
            None,
            Some(format!("complete parts={}", self.parts)),
        );
        let res = self.inner.complete().await;
        self.st.finish_with(seq, &res);
        res
    }

    async fn abort(&mut self) -> OsResult<()> {
        let seq = self.st.begin(
            StoreOp::Put,
            &self.path,
            None,
            None,
            Some("abort".to_string()),
        );
        let res = self.inner.abort().await;
        self.st.finish_with(seq, &res);
        res
    }
}

// ---- the reference model --------------------------------------------

/// Identity of one *attempt* at one logical client operation.
pub type AttemptId = (u64, u32);

/// Terminal state of one attempt, as the client would classify it.
#[derive(Debug, Clone, PartialEq)]
pub enum Outcome {
    /// Durably acknowledged, with the offset the server reported.
    Acked { last_offset: u64, duplicate: bool },
    /// The server decided against it before committing anything.
    Rejected,
    /// The request may or may not have committed: no response, or an
    /// ambiguous fencing error.
    Unknown,
}

/// What the workload believes it did.
#[derive(Default, Debug)]
pub struct OpLog {
    /// Per routing key, attempts that were acknowledged, in ack order.
    pub acked: HashMap<String, Vec<AttemptId>>,
    /// Attempts the server definitively rejected: they must never appear.
    pub rejected: HashSet<AttemptId>,
    /// Attempts with an unresolved outcome: absent or present are both
    /// legal, twice is not.
    pub unknown: HashSet<AttemptId>,
    /// Logical operations driven with producer idempotence: across all of
    /// an operation's attempts, at most one may be stored.
    pub idempotent: HashSet<u64>,
    /// Every attempt the workload ISSUED (whatever its outcome). An
    /// observed record that belongs to no issued attempt is a fabrication
    /// — a class the old audit tolerated because it only checked that
    /// acked attempts were present, never that present attempts were
    /// issued.
    pub issued: HashSet<AttemptId>,
    /// Offset reported by the server for each acked attempt, so a read can
    /// be checked against what the client was told, not just for presence.
    pub acked_offsets: HashMap<AttemptId, u64>,
}

impl OpLog {
    pub fn total_acked(&self) -> usize {
        self.acked.values().map(|v| v.len()).sum()
    }

    fn all_acked(&self) -> HashSet<AttemptId> {
        self.acked.values().flatten().copied().collect()
    }

    /// Audit what a reader actually drained. `observed` is per routing key,
    /// in read order.
    pub fn audit(&self, observed: &HashMap<String, Vec<AttemptId>>) -> Result<(), String> {
        // The ledger must be self-consistent, or a harness bug could
        // silently weaken every check below.
        let acked = self.all_acked();
        if let Some(a) = self.rejected.intersection(&acked).next() {
            return Err(format!(
                "harness bug: op{}#{} recorded as both acked and rejected",
                a.0, a.1
            ));
        }

        let mut seen_count: HashMap<AttemptId, usize> = HashMap::new();
        for attempts in observed.values() {
            for a in attempts {
                *seen_count.entry(*a).or_insert(0) += 1;
            }
        }

        // I7: every observed record belongs to an issued attempt. Only
        // enforced when the workload actually tracked issuance, so hand-
        // built oracle unit tests stay valid.
        if !self.issued.is_empty()
            && let Some(a) = seen_count.keys().find(|a| !self.issued.contains(a))
        {
            return Err(format!(
                "I7 violated: op{}#{} is readable but was never issued",
                a.0, a.1
            ));
        }

        // I3: nothing is stored twice.
        if let Some((a, n)) = seen_count.iter().find(|(_, n)| **n > 1) {
            return Err(format!(
                "I3 violated: attempt op{}#{} stored {n} times",
                a.0, a.1
            ));
        }

        // I5: a definitively rejected attempt never appears.
        if let Some(a) = self.rejected.iter().find(|a| seen_count.contains_key(a)) {
            return Err(format!(
                "I5 violated: op{}#{} was rejected but is readable",
                a.0, a.1
            ));
        }

        // I1 + I2, per key.
        for (key, acked) in &self.acked {
            let seen = observed.get(key).cloned().unwrap_or_default();
            for a in acked {
                if !seen.contains(a) {
                    return Err(format!(
                        "I1 violated: key {key} acked op{}#{} but it is not readable",
                        a.0, a.1
                    ));
                }
            }
            let mut it = seen.iter();
            for want in acked {
                if !it.any(|got| got == want) {
                    return Err(format!(
                        "I2 violated: key {key} op{}#{} out of acknowledged order",
                        want.0, want.1
                    ));
                }
            }
        }

        // I6: an idempotent operation commits at most once, however many
        // times its client retried.
        for op in &self.idempotent {
            let stored: Vec<AttemptId> = seen_count
                .keys()
                .copied()
                .filter(|(o, _)| o == op)
                .collect();
            if stored.len() > 1 {
                return Err(format!(
                    "I6 violated: idempotent op{op} stored {} times ({stored:?})",
                    stored.len()
                ));
            }
        }
        Ok(())
    }
}

// ---- workload --------------------------------------------------------

/// Drives logical client operations, with retries, against a real engine.
pub struct Workload {
    next_op: u64,
    /// Next producer sequence per routing key. Producer sequences are
    /// per-(producer id) and must start at 0 and be contiguous — an epoch
    /// bump with a non-zero sequence is rejected outright
    /// (`AppendErr::ProducerEpochSeq`). A retry reuses the SAME sequence:
    /// that reuse is what makes it idempotent.
    producer_seq: HashMap<String, u64>,
    /// Attempts per logical operation before the client gives up.
    pub max_attempts: u32,
    pub coverage: Arc<Coverage>,
}

impl Workload {
    pub fn new(coverage: Arc<Coverage>) -> Self {
        Workload {
            next_op: 1,
            producer_seq: HashMap::new(),
            max_attempts: 3,
            coverage,
        }
    }

    /// One attempt, classified as the client would classify it.
    #[allow(clippy::too_many_arguments)]
    async fn attempt(
        &self,
        engine: &Arc<crate::shard::ShardEngine>,
        hash: [u8; 16],
        key: &crate::crypto::StreamKey,
        rk: &str,
        op: u64,
        attempt: u32,
        producer: Option<crate::shard::ProducerReq>,
    ) -> Outcome {
        use crate::shard::{AppendErr, AppendReq};
        let payload = serde_json::json!({ "op": op, "att": attempt, "k": rk }).to_string();
        let subkey = crate::crypto::derive_subkey(key, &hash, rk, 0);
        let (tx, rx) = tokio::sync::oneshot::channel();
        let req = AppendReq {
            enqueued_at: std::time::Instant::now(),
            hash,
            route: hash,
            entries: vec![bytes::Bytes::from(payload.into_bytes())],
            usage: crate::usage::counters(&hash),
            routing_key: rk.to_string(),
            key_hash: crate::crypto::stream_hash(rk),
            producer_lineage: Vec::new(),
            key_version: 0,
            subkey,
            ts_hint_ms: None,
            seq: None,
            bytes: 0,
            close: false,
            producer,
            deferred_error: None,
            sealed_reject_new: None,
            touch: None,
            seal_gen: None,
            seal_fence_to: None,
            billing: None,
            resp: tx,
        };
        if engine.try_enqueue(req).is_err() {
            // Never entered the engine: definitely not committed.
            return Outcome::Rejected;
        }
        match rx.await {
            Ok(Ok(ack)) => Outcome::Acked {
                last_offset: ack.last_offset,
                duplicate: ack.duplicate,
            },
            // The engine reached a decision before committing anything.
            Ok(Err(
                AppendErr::SeqConflict { .. }
                | AppendErr::ProducerSeqReused
                | AppendErr::ProducerGap { .. }
                | AppendErr::ProducerStale { .. }
                | AppendErr::ProducerEpochSeq
                | AppendErr::SealSuperseded
                | AppendErr::CtMismatch
                | AppendErr::BadBody(_),
            )) => Outcome::Rejected,
            // Fenced, closed, or failed mid-flight: the write may or may
            // not have landed. Exactly the state the soak wedge produced.
            Ok(Err(AppendErr::Moved | AppendErr::Closed { .. } | AppendErr::Internal(_))) => {
                Outcome::Unknown
            }
            // Responder dropped: the request's fate is unobservable.
            Err(_) => Outcome::Unknown,
        }
    }

    /// One attempt with an explicit producer identity and an optional
    /// **client deadline** — the public boundary. A deadline that expires
    /// leaves the server's append running and yields `Unknown`, which is
    /// exactly the operational shape storage faults produce (slow, not
    /// failed). Returns the raw outcome; the caller owns the ledger.
    #[allow(clippy::too_many_arguments)]
    pub async fn attempt_with_deadline(
        &self,
        engine: &Arc<crate::shard::ShardEngine>,
        hash: [u8; 16],
        key: &crate::crypto::StreamKey,
        rk: &str,
        body: &str,
        producer: Option<crate::shard::ProducerReq>,
        deadline: Option<std::time::Duration>,
    ) -> Outcome {
        use crate::shard::{AppendErr, AppendReq};
        let subkey = crate::crypto::derive_subkey(key, &hash, rk, 0);
        let (tx, rx) = tokio::sync::oneshot::channel();
        let req = AppendReq {
            enqueued_at: std::time::Instant::now(),
            hash,
            route: hash,
            entries: vec![bytes::Bytes::from(body.as_bytes().to_vec())],
            usage: crate::usage::counters(&hash),
            routing_key: rk.to_string(),
            key_hash: crate::crypto::stream_hash(rk),
            producer_lineage: Vec::new(),
            key_version: 0,
            subkey,
            ts_hint_ms: None,
            seq: None,
            bytes: 0,
            close: false,
            producer,
            deferred_error: None,
            sealed_reject_new: None,
            touch: None,
            seal_gen: None,
            seal_fence_to: None,
            billing: None,
            resp: tx,
        };
        if engine.try_enqueue(req).is_err() {
            return Outcome::Rejected;
        }
        let got = match deadline {
            Some(d) => match tokio::time::timeout(d, rx).await {
                Ok(r) => r,
                Err(_) => {
                    // The client stopped waiting. The append is still
                    // running server-side: this is the ambiguity.
                    self.coverage.hit(mech::CLIENT_DEADLINE_EXPIRED);
                    self.coverage.hit(mech::APPEND_UNKNOWN);
                    return Outcome::Unknown;
                }
            },
            None => rx.await,
        };
        match got {
            Ok(Ok(ack)) => {
                if ack.duplicate {
                    self.coverage.hit(mech::PRODUCER_DUPLICATE);
                }
                self.coverage.hit(mech::APPEND_ACKED);
                Outcome::Acked {
                    last_offset: ack.last_offset,
                    duplicate: ack.duplicate,
                }
            }
            Ok(Err(
                AppendErr::SeqConflict { .. }
                | AppendErr::ProducerSeqReused
                | AppendErr::ProducerGap { .. }
                | AppendErr::ProducerStale { .. }
                | AppendErr::ProducerEpochSeq
                | AppendErr::SealSuperseded
                | AppendErr::CtMismatch
                | AppendErr::BadBody(_),
            )) => {
                self.coverage.hit(mech::APPEND_REJECTED);
                Outcome::Rejected
            }
            Ok(Err(AppendErr::Moved | AppendErr::Closed { .. } | AppendErr::Internal(_))) => {
                self.coverage.hit(mech::APPEND_UNKNOWN);
                Outcome::Unknown
            }
            Err(_) => {
                self.coverage.hit(mech::APPEND_UNKNOWN);
                Outcome::Unknown
            }
        }
    }

    /// One logical operation, retried like a production client: a retry
    /// after an unknown outcome is a NEW attempt of the SAME operation.
    ///
    /// With `idempotent`, every attempt carries the same producer sequence,
    /// so the engine must suppress the duplicate (I6). Without it, a retry
    /// may legitimately commit twice — which is exactly why the oracle
    /// tracks operations rather than payloads.
    pub async fn append(
        &mut self,
        engine: &Arc<crate::shard::ShardEngine>,
        hash: [u8; 16],
        key: &crate::crypto::StreamKey,
        rk: &str,
        idempotent: bool,
        log: &mut OpLog,
    ) -> Outcome {
        self.append_to(&[engine], hash, key, rk, idempotent, log)
            .await
    }

    /// One logical operation, failing over across owners.
    ///
    /// Attempt `i` goes to `engines[min(i, len-1)]`, which is what a client
    /// following `Streams-Replay-To` does after a shard moves: same logical
    /// operation, same producer sequence, new owner. The retry is only
    /// idempotent if producer state survived the handoff — which is the
    /// property this exists to test.
    #[allow(clippy::too_many_arguments)]
    pub async fn append_to(
        &mut self,
        engines: &[&Arc<crate::shard::ShardEngine>],
        hash: [u8; 16],
        key: &crate::crypto::StreamKey,
        rk: &str,
        idempotent: bool,
        log: &mut OpLog,
    ) -> Outcome {
        let op = self.next_op;
        self.next_op += 1;
        let pseq = if idempotent {
            log.idempotent.insert(op);
            let e = self.producer_seq.entry(rk.to_string()).or_insert(0);
            let s = *e;
            *e += 1;
            s
        } else {
            0
        };
        let mut last = Outcome::Unknown;
        for attempt in 0..self.max_attempts {
            if attempt > 0 {
                self.coverage.hit(mech::APPEND_RETRIED);
            }
            let producer = idempotent.then(|| crate::shard::ProducerReq {
                id: format!("dst-producer-{rk}"),
                epoch: 1,
                seq: pseq,
                request_hash: None,
            });
            let engine = engines[(attempt as usize).min(engines.len() - 1)];
            log.issued.insert((op, attempt));
            last = self
                .attempt(engine, hash, key, rk, op, attempt, producer)
                .await;
            match &last {
                Outcome::Acked {
                    duplicate,
                    last_offset,
                } => {
                    log.acked_offsets.insert((op, attempt), *last_offset);
                    if *duplicate {
                        self.coverage.hit(mech::PRODUCER_DUPLICATE);
                    }
                    self.coverage.hit(mech::APPEND_ACKED);
                    log.acked
                        .entry(rk.to_string())
                        .or_default()
                        .push((op, attempt));
                    return last;
                }
                Outcome::Rejected => {
                    self.coverage.hit(mech::APPEND_REJECTED);
                    log.rejected.insert((op, attempt));
                    return last;
                }
                Outcome::Unknown => {
                    self.coverage.hit(mech::APPEND_UNKNOWN);
                    log.unknown.insert((op, attempt));
                    // retry
                }
            }
        }
        last
    }

    /// `per_key` operations for each routing key.
    #[allow(clippy::too_many_arguments)]
    pub async fn run(
        &mut self,
        engine: &Arc<crate::shard::ShardEngine>,
        hash: [u8; 16],
        key: &crate::crypto::StreamKey,
        routing_keys: &[&str],
        per_key: u64,
        idempotent: bool,
        log: &mut OpLog,
    ) {
        for _ in 0..per_key {
            for rk in routing_keys {
                self.append(engine, hash, key, rk, idempotent, log).await;
            }
        }
    }
}

// ---- reader ----------------------------------------------------------

/// Read everything back **through the production merged reader**
/// (`http::read_merged`): history tier for `[0, absorbed)`, shard log for
/// `[absorbed, next)`.
///
/// Reimplementing that boundary here would mean the oracle tests a copy of
/// the read path rather than the read path, and a copy is free to drift.
/// One history-reader service per store, defaults suitable for
/// correctness scenarios. Budget scenarios construct their own (pinned
/// poll, chosen cap) and hold it across reads.
pub async fn drain_observed(
    engine: &Arc<crate::shard::ShardEngine>,
    hash: [u8; 16],
    key: &crate::crypto::StreamKey,
    coverage: &Coverage,
) -> HashMap<String, Vec<AttemptId>> {
    let mut out: HashMap<String, Vec<AttemptId>> = HashMap::new();
    let Ok(handle) = engine.stream_handle(hash).await else {
        return out;
    };
    if handle.state.lock().unwrap().durable.absorbed > 0 {
        coverage.hit(mech::READ_FROM_HISTORY);
    }
    let mut from = 0u64;
    // Bounded: each pass must advance `from`, and the loop stops the first
    // time it cannot.
    for _ in 0..1024 {
        let res = match crate::http::read_merged(
            key,
            &hash,
            &handle,
            engine,
            from,
            None,
            8 * 1024 * 1024,
            crate::shard::Deliver::Durable,
        )
        .await
        {
            Ok(r) => r,
            Err(_) => return out,
        };
        if std::env::var("DST_DRAIN_TRACE").is_ok() {
            let offs: Vec<u64> = res.recs.iter().map(|r| r.off).collect();
            eprintln!(
                "DRAIN from={from} n={} last={:?} completed={} end={} offs={offs:?}",
                res.recs.len(),
                res.last,
                res.completed,
                res.end
            );
        }
        for rec in &res.recs {
            let Ok(v) = serde_json::from_slice::<serde_json::Value>(&rec.payload) else {
                continue;
            };
            let (Some(op), Some(att), Some(k)) = (
                v.get("op").and_then(|x| x.as_u64()),
                v.get("att").and_then(|x| x.as_u64()),
                v.get("k").and_then(|x| x.as_str()),
            ) else {
                continue;
            };
            out.entry(k.to_string()).or_default().push((op, att as u32));
        }
        if res.completed {
            return out;
        }
        // An incomplete page that made no progress is a transient: the
        // reader raced the absorbed boundary (an honest empty page asks
        // the caller to re-poll, and read_merged only says `completed`
        // when the page really reached `end`). Retry — the pass bound
        // above keeps a genuinely wedged engine from hanging the oracle,
        // and the audit then reports the missing records honestly.
        match res.last {
            Some(last) if last + 1 > from => from = last + 1,
            _ => {}
        }
    }
    out
}

#[cfg(test)]
mod dst_tests;

#[cfg(test)]
mod trace_tests {
    use super::*;

    fn mem() -> Arc<dyn ObjectStore> {
        Arc::new(object_store::memory::InMemory::new())
    }

    /// The trace is in id (= trace-lock acquisition) order, so a
    /// before/after refactor diff compares equal iff the client issued the
    /// same operations in the same order. For a single sequential client
    /// with no streams, ids happen to be dense from 0 — but the CONTRACT
    /// is unique + strictly increasing, not dense (see `StoreTraceEvent`).
    #[tokio::test]
    async fn events_are_ordered_by_dispatch_seq() {
        let s = TraceStore::verbatim(mem());
        let pa = ObjPath::from("shards/x/wal/1.sst");
        let pb = ObjPath::from("shards/x/wal/2.sst");
        s.put_opts(&pa, PutPayload::from(vec![1u8; 4]), PutOptions::default())
            .await
            .unwrap();
        s.put_opts(&pb, PutPayload::from(vec![2u8; 4]), PutOptions::default())
            .await
            .unwrap();
        s.get_opts(&pa, GetOptions::default()).await.unwrap();

        let evs = s.events();
        assert_eq!(evs.len(), 3);
        for w in evs.windows(2) {
            assert!(w[0].seq < w[1].seq, "ids strictly increase in trace order");
        }
        assert_eq!(evs[0].op, StoreOp::Put);
        assert_eq!(evs[0].path, pa.as_ref());
        assert_eq!(evs[1].op, StoreOp::Put);
        assert_eq!(evs[1].path, pb.as_ref());
        assert_eq!(evs[2].op, StoreOp::Get);
        assert!(evs.iter().all(|e| e.outcome == TraceOutcome::Ok));

        // The operation ledger and reset.
        let c = s.operation_counts();
        assert!(c.contains(&(StoreOp::Put, ObjClass::Wal, 2)), "{c:?}");
        assert!(c.contains(&(StoreOp::Get, ObjClass::Wal, 1)), "{c:?}");
        s.reset();
        assert!(s.events().is_empty());
    }

    #[tokio::test]
    async fn put_records_byte_count() {
        let s = TraceStore::new(mem());
        let p = ObjPath::from("shards/x/wal/1.sst");
        s.put_opts(&p, PutPayload::from(vec![7u8; 1234]), PutOptions::default())
            .await
            .unwrap();
        let evs = s.events();
        assert_eq!(evs.len(), 1);
        assert_eq!(evs[0].op, StoreOp::Put);
        assert_eq!(evs[0].class, ObjClass::Wal);
        assert_eq!(evs[0].bytes, Some(1234));
    }

    #[tokio::test]
    async fn ranged_get_records_span() {
        let inner = mem();
        let p = ObjPath::from("a/b/1.sst");
        inner
            .put_opts(&p, PutPayload::from(vec![0u8; 16]), PutOptions::default())
            .await
            .unwrap();
        let s = TraceStore::new(inner);

        let bounded = GetOptions {
            range: Some(object_store::GetRange::Bounded(2..7)),
            ..Default::default()
        };
        s.get_opts(&p, bounded).await.unwrap();
        let suffix = GetOptions {
            range: Some(object_store::GetRange::Suffix(4)),
            ..Default::default()
        };
        s.get_opts(&p, suffix).await.unwrap();
        s.get_opts(&p, GetOptions::default()).await.unwrap();

        let evs = s.events();
        assert_eq!(evs.len(), 3);
        assert_eq!(evs[0].bytes, Some(5), "bounded range records its span");
        assert_eq!(evs[1].bytes, Some(4), "suffix range records its span");
        assert_eq!(evs[2].bytes, None, "a full get has no span");
    }

    /// Redaction is the default: no tenant-derived segment survives, the
    /// path still classifies identically, and payload bytes are never
    /// retained.
    #[tokio::test]
    async fn redaction_hashes_paths_and_never_stores_payload_bytes_by_default() {
        let s = TraceStore::new(mem());
        let p = ObjPath::from("acme-corp/shards/secret-stream-name/wal/00000042.sst");
        s.put_opts(&p, PutPayload::from(vec![0xabu8; 8]), PutOptions::default())
            .await
            .unwrap();
        let evs = s.events();
        let e = &evs[0];
        assert_ne!(e.path, p.as_ref());
        for leaked in ["acme-corp", "secret-stream-name", "00000042"] {
            assert!(
                !e.path.contains(leaked),
                "redacted path must not contain {leaked}: {}",
                e.path
            );
        }
        // Classification is preserved, both in the event and on re-classify.
        assert_eq!(e.class, ObjClass::Wal);
        assert_eq!(ObjClass::of(&e.path), ObjClass::Wal, "{}", e.path);
        assert!(e.path.ends_with(".sst"), "extension survives: {}", e.path);
        assert!(
            e.path.contains("/wal/"),
            "structural segments survive: {}",
            e.path
        );
        // Payload bytes are never retained by default.
        assert_eq!(e.content_hash, None);

        // The manifest marker survives redaction too.
        let s2 = TraceStore::new(mem());
        let mp = ObjPath::from("tenant9/shards/root-3/manifest-00001.json");
        s2.put_opts(&mp, PutPayload::from(vec![0u8; 1]), PutOptions::default())
            .await
            .unwrap();
        let e2 = &s2.events()[0];
        assert_eq!(e2.class, ObjClass::Manifest);
        assert_eq!(ObjClass::of(&e2.path), ObjClass::Manifest, "{}", e2.path);
        assert!(!e2.path.contains("tenant9"));
        assert!(!e2.path.contains("root-3"));
    }

    #[tokio::test]
    async fn hash_on_mode_records_content_hashes() {
        let s = TraceStore::with_options(mem(), true, true);
        let payload = b"stream-payload-bytes".to_vec();
        let p = ObjPath::from("shards/x/wal/9.sst");
        s.put_opts(&p, PutPayload::from(payload.clone()), PutOptions::default())
            .await
            .unwrap();
        let want = {
            use sha2::Digest;
            let d = sha2::Sha256::digest(&payload);
            crate::crypto::hex(&d[..8])
        };
        assert_eq!(want.len(), 16, "16 hex chars, not payload bytes");
        let e = &s.events()[0];
        assert_eq!(e.content_hash.as_deref(), Some(want.as_str()));
    }

    #[tokio::test]
    async fn outcome_is_recorded_on_error() {
        let s = TraceStore::new(mem());
        let p = ObjPath::from("shards/x/wal/nope.sst");
        let res = s.get_opts(&p, GetOptions::default()).await;
        assert!(res.is_err(), "get of a nonexistent object must fail");
        let evs = s.events();
        assert_eq!(evs.len(), 1);
        assert_eq!(evs[0].op, StoreOp::Get);
        assert_eq!(evs[0].outcome, TraceOutcome::NotFound);
    }

    /// The session is Put events all the way down: open, one per part
    /// with byte counts, and a complete that notes how many parts landed.
    #[tokio::test]
    async fn multipart_session_is_traced() {
        let s = TraceStore::verbatim(mem());
        let p = ObjPath::from("shards/x/wal/big.sst");
        let mut up = s
            .put_multipart_opts(&p, PutMultipartOptions::default())
            .await
            .unwrap();
        up.put_part(PutPayload::from(vec![1u8; 10])).await.unwrap();
        up.put_part(PutPayload::from(vec![2u8; 20])).await.unwrap();
        up.complete().await.unwrap();

        let evs = s.events();
        assert_eq!(evs.len(), 4);
        assert_eq!(evs[0].detail.as_deref(), Some("multipart-open"));
        assert_eq!(evs[0].bytes, None);
        assert_eq!(evs[1].detail.as_deref(), Some("part"));
        assert_eq!(evs[1].bytes, Some(10));
        assert_eq!(evs[2].detail.as_deref(), Some("part"));
        assert_eq!(evs[2].bytes, Some(20));
        assert_eq!(evs[3].detail.as_deref(), Some("complete parts=2"));
        assert!(
            evs.iter()
                .all(|e| e.op == StoreOp::Put && e.outcome == TraceOutcome::Ok)
        );
    }

    // ---- delete_stream: pass-through with EXACTLY ONE delegated call ----

    /// A scripted delete_stream store: counts invocations and consumed
    /// inputs, returns a canned output stream. Used to prove the trace
    /// layer neither fans the call out nor manufactures results.
    #[derive(Debug)]
    struct DeleteSpy {
        inner: object_store::memory::InMemory,
        calls: std::sync::Arc<std::sync::atomic::AtomicU64>,
        consumed_inputs: std::sync::Arc<std::sync::atomic::AtomicU64>,
        // object_store::Error is not Clone, so scripted failures are
        // stored as their message and rebuilt at stream time.
        scripted_output: Vec<Result<ObjPath, String>>,
    }

    impl std::fmt::Display for DeleteSpy {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "DeleteSpy({})", self.inner)
        }
    }

    #[async_trait::async_trait]
    impl ObjectStore for DeleteSpy {
        async fn put_opts(
            &self,
            location: &ObjPath,
            payload: PutPayload,
            opts: PutOptions,
        ) -> OsResult<PutResult> {
            self.inner.put_opts(location, payload, opts).await
        }
        async fn put_multipart_opts(
            &self,
            location: &ObjPath,
            opts: PutMultipartOptions,
        ) -> OsResult<Box<dyn MultipartUpload>> {
            self.inner.put_multipart_opts(location, opts).await
        }
        async fn get_opts(&self, location: &ObjPath, options: GetOptions) -> OsResult<GetResult> {
            self.inner.get_opts(location, options).await
        }
        fn list(
            &self,
            prefix: Option<&ObjPath>,
        ) -> futures_util::stream::BoxStream<'static, OsResult<ObjectMeta>> {
            self.inner.list(prefix)
        }
        async fn list_with_delimiter(&self, prefix: Option<&ObjPath>) -> OsResult<ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }
        async fn copy_opts(&self, from: &ObjPath, to: &ObjPath, opts: CopyOptions) -> OsResult<()> {
            self.inner.copy_opts(from, to, opts).await
        }
        fn delete_stream(
            &self,
            locations: futures_util::stream::BoxStream<'static, OsResult<ObjPath>>,
        ) -> futures_util::stream::BoxStream<'static, OsResult<ObjPath>> {
            use futures_util::StreamExt;
            self.calls
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            let consumed = self.consumed_inputs.clone();
            // Lazy pass-through, one pull of the input per output item:
            // each consumed Ok input counts and releases the next
            // scripted result (fewer scripted results than inputs = the
            // coalescing-store shape); an input error surfaces in place,
            // untouched; a dropped consumer stops driving the input.
            let scripted = self.scripted_output.clone().into_iter();
            futures_util::stream::unfold(
                (locations, scripted, consumed),
                |(mut locations, mut scripted, consumed)| async move {
                    loop {
                        match locations.next().await {
                            Some(Ok(_)) => {
                                consumed.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                                if let Some(r) = scripted.next() {
                                    let r = r.map_err(|msg| object_store::Error::Generic {
                                        store: "spy",
                                        source: msg.into(),
                                    });
                                    return Some((r, (locations, scripted, consumed)));
                                }
                                // Scripted output exhausted: drain on.
                            }
                            Some(Err(e)) => return Some((Err(e), (locations, scripted, consumed))),
                            None => return None,
                        }
                    }
                },
            )
            .boxed()
        }
    }

    fn spy(
        scripted: Vec<OsResult<ObjPath>>,
    ) -> (
        Arc<DeleteSpy>,
        std::sync::Arc<std::sync::atomic::AtomicU64>,
        std::sync::Arc<std::sync::atomic::AtomicU64>,
    ) {
        let calls = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
        let consumed = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
        (
            Arc::new(DeleteSpy {
                inner: object_store::memory::InMemory::new(),
                calls: calls.clone(),
                consumed_inputs: consumed.clone(),
                scripted_output: scripted
                    .into_iter()
                    .map(|r| r.map_err(|e| e.to_string()))
                    .collect(),
            }),
            calls,
            consumed,
        )
    }

    fn delete_err() -> object_store::Error {
        object_store::Error::Generic {
            store: "spy",
            source: "scripted delete failure".into(),
        }
    }

    /// Three inputs, three scripted results: one inner call, everything
    /// passes through, input and result both traced.
    #[tokio::test]
    async fn delete_stream_delegates_exactly_once_and_traces_both_sides() {
        let (sp, calls, _consumed) = spy(vec![
            Ok(ObjPath::from("a/1.sst")),
            Ok(ObjPath::from("a/2.sst")),
            Ok(ObjPath::from("a/3.sst")),
        ]);
        let s = TraceStore::verbatim(sp);
        use futures_util::StreamExt;
        let out: Vec<_> = s
            .delete_stream(
                futures_util::stream::iter(vec![
                    Ok(ObjPath::from("a/1.sst")),
                    Ok(ObjPath::from("a/2.sst")),
                    Ok(ObjPath::from("a/3.sst")),
                ])
                .boxed(),
            )
            .collect()
            .await;
        assert_eq!(
            calls.load(std::sync::atomic::Ordering::Relaxed),
            1,
            "exactly one inner delete_stream"
        );
        assert_eq!(out.len(), 3, "every scripted result passes through");
        assert!(out.iter().all(|r| r.is_ok()));
        let evs = s.events();
        let inputs = evs
            .iter()
            .filter(|e| e.kind == TraceEventKind::DeleteInput)
            .count();
        let results = evs
            .iter()
            .filter(|e| e.kind == TraceEventKind::DeleteResult)
            .count();
        assert_eq!((inputs, results), (3, 3), "both sides traced: {evs:?}");
        // PR 3.2: the operation ledger counts ATTEMPTED deletes — the
        // Ok inputs the inner store consumed — exactly once each, never
        // the diagnostic result observations on top.
        let c = s.operation_counts();
        assert!(
            c.contains(&(StoreOp::Delete, ObjClass::Sst, 3)),
            "3 attempted deletes, not 6 observations: {c:?}"
        );
    }

    /// The inner store returns FEWER results than inputs (batching stores
    /// coalesce): the client sees exactly what the store returned — the
    /// trace layer must not invent completions.
    #[tokio::test]
    async fn delete_stream_never_fabricates_results() {
        let (sp, calls, _) = spy(vec![Ok(ObjPath::from("a/1.sst"))]);
        let s = TraceStore::verbatim(sp);
        use futures_util::StreamExt;
        let out: Vec<_> = s
            .delete_stream(
                futures_util::stream::iter(vec![
                    Ok(ObjPath::from("a/1.sst")),
                    Ok(ObjPath::from("a/2.sst")),
                    Ok(ObjPath::from("a/3.sst")),
                ])
                .boxed(),
            )
            .collect()
            .await;
        assert_eq!(calls.load(std::sync::atomic::Ordering::Relaxed), 1);
        assert_eq!(out.len(), 1, "no manufactured Ok for unreturned inputs");

        // And the extreme: empty output stays empty.
        let (sp2, _, _) = spy(vec![]);
        let s2 = TraceStore::verbatim(sp2);
        let out2: Vec<_> = s2
            .delete_stream(futures_util::stream::iter(vec![Ok(ObjPath::from("a/1.sst"))]).boxed())
            .collect()
            .await;
        assert!(out2.is_empty(), "empty output must remain empty");
    }

    /// Input errors pass through untouched (and are traced as such).
    #[tokio::test]
    async fn delete_stream_preserves_input_errors() {
        let (sp, calls, _) = spy(vec![Ok(ObjPath::from("a/2.sst"))]);
        let s = TraceStore::verbatim(sp);
        use futures_util::StreamExt;
        let out: Vec<_> = s
            .delete_stream(
                futures_util::stream::iter(vec![
                    Ok(ObjPath::from("a/1.sst")),
                    Err(delete_err()),
                    Ok(ObjPath::from("a/3.sst")),
                ])
                .boxed(),
            )
            .collect()
            .await;
        assert_eq!(calls.load(std::sync::atomic::Ordering::Relaxed), 1);
        assert_eq!(out.len(), 2);
        assert!(out[0].is_ok());
        assert!(out[1].is_err(), "the input error must surface unchanged");
        let evs = s.events();
        let err_inputs = evs
            .iter()
            .filter(|e| e.kind == TraceEventKind::DeleteInput && e.outcome != TraceOutcome::Ok)
            .count();
        assert_eq!(err_inputs, 1, "the input error is traced: {evs:?}");
        // PR 3.2: the error input is an observation, NOT an attempted
        // delete — only the two Ok inputs count in the ledger.
        let c = s.operation_counts();
        assert!(
            c.contains(&(StoreOp::Delete, ObjClass::Sst, 2)),
            "input errors never count as operations: {c:?}"
        );
    }

    /// Inner failures pass through unchanged (and are traced as such).
    #[tokio::test]
    async fn delete_stream_preserves_inner_failures() {
        let (sp, calls, _) = spy(vec![Ok(ObjPath::from("a/1.sst")), Err(delete_err())]);
        let s = TraceStore::verbatim(sp);
        use futures_util::StreamExt;
        let out: Vec<_> = s
            .delete_stream(
                futures_util::stream::iter(vec![
                    Ok(ObjPath::from("a/1.sst")),
                    Ok(ObjPath::from("a/2.sst")),
                ])
                .boxed(),
            )
            .collect()
            .await;
        assert_eq!(calls.load(std::sync::atomic::Ordering::Relaxed), 1);
        assert_eq!(out.len(), 2);
        assert!(out[0].is_ok());
        assert!(out[1].is_err(), "the inner failure must surface unchanged");
    }

    /// Dropping the consumer mid-stream does not trigger extra delegated
    /// calls — the one invocation happened at dispatch, and nothing
    /// retries or replays behind the client's back.
    #[tokio::test]
    async fn delete_stream_dropped_consumer_triggers_no_extra_calls() {
        let (sp, calls, consumed) = spy(vec![
            Ok(ObjPath::from("a/1.sst")),
            Ok(ObjPath::from("a/2.sst")),
            Ok(ObjPath::from("a/3.sst")),
        ]);
        let s = TraceStore::verbatim(sp);
        use futures_util::StreamExt;
        let mut stream = s.delete_stream(
            futures_util::stream::iter(vec![
                Ok(ObjPath::from("a/1.sst")),
                Ok(ObjPath::from("a/2.sst")),
                Ok(ObjPath::from("a/3.sst")),
            ])
            .boxed(),
        );
        let first = stream.next().await;
        assert!(matches!(first, Some(Ok(_))));
        drop(stream);
        assert_eq!(
            calls.load(std::sync::atomic::Ordering::Relaxed),
            1,
            "no replay on drop"
        );
        assert!(
            consumed.load(std::sync::atomic::Ordering::Relaxed) < 3,
            "the abandoned input stream stops being consumed"
        );
    }

    /// reset() refuses to run while an operation is in flight — the
    /// alternative is silent misattribution of the late completion.
    #[tokio::test]
    #[should_panic(expected = "while operations are active")]
    async fn reset_refuses_while_in_flight() {
        use futures_util::StreamExt;
        let s = TraceStore::new(mem());
        let mut stream = s.list(Some(&ObjPath::from("anything")));
        drop(stream.next());
        // The list event is still Pending (stream abandoned but not dropped),
        // so in_flight > 0 and reset must refuse.
        let _ = stream;
        // Force the guard NOT to run: leak the stream.
        std::mem::forget(stream);
        s.reset();
    }

    /// After completions land, reset is clean and the id space keeps
    /// moving: a later event cannot be mistaken for an earlier one.
    #[tokio::test]
    async fn reset_after_completion_is_clean_and_ids_stay_monotonic() {
        let s = TraceStore::new(mem());
        let p = ObjPath::from("shards/x/wal/1.sst");
        s.put_opts(&p, PutPayload::from(vec![1u8; 4]), PutOptions::default())
            .await
            .unwrap();
        s.reset();
        assert!(s.events().is_empty());
        s.put_opts(&p, PutPayload::from(vec![2u8; 4]), PutOptions::default())
            .await
            .unwrap();
        let evs = s.events();
        assert_eq!(evs.len(), 1);
        assert!(
            evs[0].seq > 0,
            "ids are monotonic across reset, not re-based: {:?}",
            evs[0].seq
        );
    }

    /// A list stream the consumer abandons is recorded Cancelled (and
    /// its lifetime retired, so reset still works) — not left Pending
    /// forever and not silently treated as successful.
    #[tokio::test]
    async fn abandoned_list_stream_is_marked_cancelled() {
        let s = TraceStore::new(mem());
        {
            let _stream = s.list(Some(&ObjPath::from("nothing-here")));
        }
        let evs = s.events();
        assert_eq!(evs.len(), 1);
        assert_eq!(evs[0].op, StoreOp::List);
        assert_eq!(evs[0].outcome, TraceOutcome::Cancelled, "{evs:?}");
        // The lifetime was retired: reset works.
        s.reset();
        assert!(s.events().is_empty());
    }

    // ---- PR 3.2: one lock owns id/event/lifetime association ----------

    /// Acceptance 1-3: N tasks race `begin` through one barrier, then
    /// completions land in REVERSE id order. Every event must resolve
    /// exactly once — completion locates events by id through the log's
    /// map, never by arithmetic on insertion position. Under the
    /// pre-3.2 shape (id allocated with an atomic BEFORE the vector
    /// lock) this interleaving could push events out of id order,
    /// completions then found the wrong slot, both events stayed
    /// Pending, and `reset()` panicked forever.
    #[test]
    fn concurrent_begins_with_reverse_finishes_resolve_every_event_exactly_once() {
        let s = TraceStore::verbatim(Arc::new(object_store::memory::InMemory::new()));
        const N: usize = 8;
        for round in 0..50 {
            let barrier = std::sync::Barrier::new(N);
            let seqs: Vec<u64> = std::thread::scope(|scope| {
                let handles: Vec<_> = (0..N)
                    .map(|_| {
                        let st = s.st.clone();
                        let b = &barrier;
                        scope.spawn(move || {
                            b.wait(); // all N contend for the trace lock at once
                            st.begin(StoreOp::Put, "shards/x/wal/c.sst", None, None, None)
                        })
                    })
                    .collect();
                handles.into_iter().map(|h| h.join().unwrap()).collect()
            });
            let mut sorted = seqs.clone();
            sorted.sort_unstable();
            sorted.dedup();
            assert_eq!(sorted.len(), N, "round {round}: ids must be unique");
            for seq in sorted.iter().rev() {
                s.st.finish(*seq, TraceOutcome::Ok);
            }
            let evs = s.events();
            assert_eq!(evs.len(), N, "round {round}");
            for w in evs.windows(2) {
                assert!(
                    w[0].seq < w[1].seq,
                    "round {round}: vector must be in id order: {evs:?}"
                );
            }
            assert!(
                evs.iter().all(|e| e.outcome == TraceOutcome::Ok),
                "round {round}: every event resolves exactly once, none stay Pending: {evs:?}"
            );
            s.reset(); // acceptance 9: nothing leaked a lifetime
            assert!(s.events().is_empty());
        }
    }

    /// Acceptance 4: reset racing `begin` can neither clear an active
    /// operation out from under its completion nor orphan one. All
    /// interleavings serialize on the one trace lock: reset either wins
    /// (clears a quiet window; the op then lands in the fresh window) or
    /// observes the active lifetime and refuses. After joining, nothing
    /// is active and nothing is Pending — deterministically, in every
    /// round.
    #[test]
    fn reset_racing_with_begin_cannot_orphan_an_operation() {
        let s = TraceStore::verbatim(Arc::new(object_store::memory::InMemory::new()));
        for round in 0..200 {
            let barrier = std::sync::Barrier::new(2);
            std::thread::scope(|scope| {
                let st = s.st.clone();
                let b = &barrier;
                scope.spawn(move || {
                    b.wait();
                    let seq = st.begin(StoreOp::Put, "shards/x/wal/r.sst", None, None, None);
                    std::thread::yield_now();
                    st.finish(seq, TraceOutcome::Ok);
                });
                barrier.wait();
                // A refused reset is a legal outcome of the race; a
                // poisoned lock or a lost event is not.
                let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| s.reset()));
            });
            let log = s.st.log.lock().unwrap();
            assert!(log.active.is_empty(), "round {round}: orphaned lifetime");
            assert!(
                log.events
                    .iter()
                    .all(|e| e.outcome != TraceOutcome::Pending),
                "round {round}: a completed operation may never stay Pending"
            );
            drop(log);
            s.reset(); // at quiescence reset must always work
        }
    }

    // ---- PR 3.2: stream lifetime is distinct from observed outcome ----

    /// A store whose list yields a scripted error FIRST, then delegates
    /// to the real inner listing — the "stream keeps serving after an
    /// error" case the ObjectStore trait explicitly allows.
    #[derive(Debug)]
    struct ListSpy {
        inner: Arc<object_store::memory::InMemory>,
    }

    impl std::fmt::Display for ListSpy {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "ListSpy({})", self.inner)
        }
    }

    #[async_trait::async_trait]
    impl ObjectStore for ListSpy {
        async fn put_opts(
            &self,
            location: &ObjPath,
            payload: PutPayload,
            opts: PutOptions,
        ) -> OsResult<PutResult> {
            self.inner.put_opts(location, payload, opts).await
        }
        async fn put_multipart_opts(
            &self,
            location: &ObjPath,
            opts: PutMultipartOptions,
        ) -> OsResult<Box<dyn MultipartUpload>> {
            self.inner.put_multipart_opts(location, opts).await
        }
        async fn get_opts(&self, location: &ObjPath, options: GetOptions) -> OsResult<GetResult> {
            self.inner.get_opts(location, options).await
        }
        fn list(
            &self,
            prefix: Option<&ObjPath>,
        ) -> futures_util::stream::BoxStream<'static, OsResult<ObjectMeta>> {
            use futures_util::StreamExt;
            futures_util::stream::iter(vec![Err(object_store::Error::Generic {
                store: "list-spy",
                source: "scripted list failure".into(),
            })])
            .chain(self.inner.list(prefix))
            .boxed()
        }
        async fn list_with_delimiter(&self, prefix: Option<&ObjPath>) -> OsResult<ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }
        async fn copy_opts(&self, from: &ObjPath, to: &ObjPath, opts: CopyOptions) -> OsResult<()> {
            self.inner.copy_opts(from, to, opts).await
        }
        fn delete_stream(
            &self,
            locations: futures_util::stream::BoxStream<'static, OsResult<ObjPath>>,
        ) -> futures_util::stream::BoxStream<'static, OsResult<ObjPath>> {
            self.inner.delete_stream(locations)
        }
    }

    async fn list_spy_with_one_object() -> Arc<ListSpy> {
        let inner = Arc::new(object_store::memory::InMemory::new());
        inner
            .put_opts(
                &ObjPath::from("a/1.sst"),
                PutPayload::from(vec![1u8; 4]),
                PutOptions::default(),
            )
            .await
            .unwrap();
        Arc::new(ListSpy { inner })
    }

    /// Acceptance 5-6 / required test 1: a list stream that yielded an
    /// error is still ALIVE, so reset must refuse. Pre-3.2 the item
    /// error retired the operation and the documented "refuses while
    /// anything is in flight" claim was false for exactly this case.
    #[tokio::test]
    #[should_panic(expected = "while operations are active")]
    async fn reset_refuses_while_an_errored_list_stream_is_still_open() {
        use futures_util::StreamExt;
        let spy = list_spy_with_one_object().await;
        let s = TraceStore::verbatim(spy);
        let mut stream = s.list(Some(&ObjPath::from("a")));
        let first = stream.next().await;
        assert!(matches!(first, Some(Err(_))), "scripted error first");
        // The stream can keep serving items; its lifetime is open.
        s.reset();
    }

    /// Required test 2: a list that errors, then serves another item,
    /// then completes — the first fact (the error) is the recorded
    /// outcome, exhaustion retires the lifetime exactly once, and reset
    /// then works.
    #[tokio::test]
    async fn errored_list_stream_serves_on_and_retires_at_exhaustion() {
        use futures_util::StreamExt;
        let spy = list_spy_with_one_object().await;
        let s = TraceStore::verbatim(spy);
        let out: Vec<_> = s.list(Some(&ObjPath::from("a"))).collect().await;
        assert_eq!(out.len(), 2);
        assert!(out[0].is_err() && out[1].is_ok(), "error then live item");
        let evs = s.events();
        assert_eq!(evs.len(), 1);
        assert_eq!(
            evs[0].outcome,
            TraceOutcome::Error,
            "first fact wins; exhaustion does not overwrite it: {evs:?}"
        );
        s.reset(); // exhaustion retired the lifetime
        assert!(s.events().is_empty());
    }

    /// Required test 3: an unconsumed delete stream is an ACTIVE
    /// operation — reset must refuse while it is alive. Pre-3.2 delete
    /// streams had no lifetime at all and reset would clear the trace
    /// under a stream still appending observations.
    #[tokio::test]
    #[should_panic(expected = "while operations are active")]
    async fn reset_refuses_while_a_delete_stream_is_active() {
        let (sp, _, _) = spy(vec![Ok(ObjPath::from("a/1.sst"))]);
        let s = TraceStore::verbatim(sp);
        use futures_util::StreamExt;
        let _stream =
            s.delete_stream(futures_util::stream::iter(vec![Ok(ObjPath::from("a/1.sst"))]).boxed());
        s.reset();
    }

    /// Required test 4: dropping a delete stream midway retires its
    /// lifetime exactly once — reset works afterwards, repeatedly.
    #[tokio::test]
    async fn dropped_delete_stream_retires_exactly_once() {
        let (sp, _, _) = spy(vec![
            Ok(ObjPath::from("a/1.sst")),
            Ok(ObjPath::from("a/2.sst")),
        ]);
        let s = TraceStore::verbatim(sp);
        use futures_util::StreamExt;
        let mut stream = s.delete_stream(
            futures_util::stream::iter(vec![
                Ok(ObjPath::from("a/1.sst")),
                Ok(ObjPath::from("a/2.sst")),
            ])
            .boxed(),
        );
        let first = stream.next().await;
        assert!(matches!(first, Some(Ok(_))));
        drop(stream);
        s.reset();
        assert!(s.events().is_empty());
        s.reset(); // idempotent at quiescence: nothing double-retired
    }

    /// Acceptance 8 (the double-count finding): one ordinary successful
    /// delete = ONE attempted operation in the ledger, even though the
    /// diagnostic trace holds both the input and the result observation.
    #[tokio::test]
    async fn ordinary_delete_counts_once_in_the_operation_ledger() {
        let (sp, _, _) = spy(vec![Ok(ObjPath::from("a/1.sst"))]);
        let s = TraceStore::verbatim(sp);
        use futures_util::StreamExt;
        let out: Vec<_> = s
            .delete_stream(futures_util::stream::iter(vec![Ok(ObjPath::from("a/1.sst"))]).boxed())
            .collect()
            .await;
        assert_eq!(out.len(), 1);
        let evs = s.events();
        assert_eq!(evs.len(), 2, "input + result observations: {evs:?}");
        assert_eq!(evs[0].kind, TraceEventKind::DeleteInput);
        assert_eq!(evs[1].kind, TraceEventKind::DeleteResult);
        let c = s.operation_counts();
        assert_eq!(
            c,
            vec![(StoreOp::Delete, ObjClass::Sst, 1)],
            "one delete attempted, not two: {c:?}"
        );
    }
}
