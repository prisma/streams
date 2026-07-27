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
        self.counters.lock().unwrap().get(name).copied().unwrap_or(0)
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
    let mut z = seed
        ^ h
        ^ ((op as u64) << 56)
        ^ n.wrapping_mul(0x9e37_79b9_7f4a_7c15);
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
    pub fn hold_class(
        &self,
        op: StoreOp,
        class: ObjClass,
        max_parked: u64,
    ) -> Arc<AtomicU64> {
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
        if !self.issued.is_empty() {
            if let Some(a) = seen_count.keys().find(|a| !self.issued.contains(a)) {
                return Err(format!(
                    "I7 violated: op{}#{} is readable but was never issued",
                    a.0, a.1
                ));
            }
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
            let stored: Vec<AttemptId> =
                seen_count.keys().copied().filter(|(o, _)| o == op).collect();
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
            entries: vec![bytes::Bytes::from(payload.into_bytes())],
            usage: crate::usage::counters(&hash),
            routing_key: rk.to_string(),
            key_version: 0,
            subkey,
            ts_hint_ms: None,
            seq: None,
            bytes: 0,
            close: false,
            producer,
            deferred_error: None,
            touch: None,
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
                | AppendErr::ProducerGap { .. }
                | AppendErr::ProducerStale { .. }
                | AppendErr::ProducerEpochSeq
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
            entries: vec![bytes::Bytes::from(body.as_bytes().to_vec())],
            usage: crate::usage::counters(&hash),
            routing_key: rk.to_string(),
            key_version: 0,
            subkey,
            ts_hint_ms: None,
            seq: None,
            bytes: 0,
            close: false,
            producer,
            deferred_error: None,
            touch: None,
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
                | AppendErr::ProducerGap { .. }
                | AppendErr::ProducerStale { .. }
                | AppendErr::ProducerEpochSeq
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
        self.append_to(&[engine], hash, key, rk, idempotent, log).await
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
pub fn fresh_hist(store: &Arc<dyn ObjectStore>) -> Arc<crate::history::HistReaders> {
    crate::history::HistReaders::new(
        store.clone(),
        8,
        std::time::Duration::from_secs(120),
        5_000,
    )
}

pub async fn drain_observed(
    hist: &Arc<crate::history::HistReaders>,
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
            hist,
            key,
            &hash,
            &handle,
            engine,
            from,
            None,
            8 * 1024 * 1024,
        )
        .await
        {
            Ok(r) => r,
            Err(_) => return out,
        };
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
        let next = match res.last {
            Some(last) => last + 1,
            None => return out,
        };
        if res.completed || next <= from {
            return out;
        }
        from = next;
    }
    out
}

#[cfg(test)]
mod dst_tests;
