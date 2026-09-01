//! The seeded fault-injecting `ObjectStore` decorator (docs/DST.md):
//! keyed fault decisions — a pure function of `(seed, path, op,
//! occurrence)` — so a seed reproduces a fault placement under
//! concurrency. Split out of the dst catch-all (PR 3.2.1).

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use object_store::path::Path as ObjPath;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, Result as OsResult,
};

use super::{Coverage, ObjClass, StoreOp, mech};

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
