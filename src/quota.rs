//! Server-side project admission backstops (docs/MULTITENANCY.md
//! §17.3, Stage 6).
//!
//! The GATEWAY owns primary quota enforcement (§17.2, platform-side);
//! every instance still maintains its own bounded per-project
//! admission so a project that slips past — or a gateway bug — cannot
//! take the cell down or starve its neighbors. Enforcement is scoped
//! to enforce-mode requests (the only ones with a verified project),
//! and each refusal names ONLY the offending project:
//!
//!   429 project_rate_limit        the project's request-rate bucket
//!   429 project_concurrency_limit the project's inflight ceiling
//!   503 project_tracker_capacity  the tracker itself is full (new
//!                                 projects only; tracked ones are
//!                                 never affected)
//!
//! Projects NEVER share a bucket (§17.3: no overflow coupling) — the
//! tracker map is keyed by project, and a full tracker refuses to
//! track new projects rather than lumping them together.
//!
//! Stage 6a enforces request rate + concurrency. Byte-rate quotas
//! (append/read volume) and live-subscription counting need metering
//! points with streaming lifetimes and land with 6b.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use crate::project_policy::ProjectQuotas;
use crate::tenant::ProjectId;

/// Bounded tracker: beyond this many distinct projects the tracker
/// refuses NEW ones (503, typed `TrackerCapacity`) instead of growing
/// without bound. The cap must hold the certified TENANT POPULATION,
/// not just its active window: the workload-cert rotation (10,000
/// tenants, 100 active per 5s window ⇒ 20 first-seen projects/s)
/// demands 20/s x IDLE_EVICT_MS = 6,000 un-evictable entries at
/// steady state — the previous cap of 4,096 shed 19% of that
/// rotation with typed TrackerCapacity (2026-08-19 churn rung; the
/// cert_rotation test below pins the shape). 16,384 holds the 10k
/// population outright with headroom, at ~8 MiB worst-case tracker
/// memory (~500 B/entry). Still a deliberate HARD ceiling: refusing
/// to track (never merging strangers into shared buckets) remains
/// the fail-closed choice; the churn test pins evict-idle-first,
/// never-evict-active, and the typed refusal at true saturation.
pub(crate) const MAX_TRACKED_PROJECTS: usize = 16_384;

/// A tracked project with no admission attempts for this long (and no
/// inflight work) may be evicted under tracker pressure. Its buckets
/// restart full — an idle project lost no accumulated debt worth
/// keeping at this horizon.
pub(crate) const IDLE_EVICT_MS: i64 = 300_000;

mod bucket;
use bucket::Bucket;

pub(crate) struct ProjectAdmission {
    ops: Arc<crate::ops::OpsService>,
    /// Last admission attempt (ms) — the idle-eviction clock.
    last_seen_ms: std::sync::atomic::AtomicI64,
    bucket: Mutex<Bucket>,
    /// §17.2 volume backstops: append payload bytes and records.
    append_bytes: Mutex<Bucket>,
    append_records: Mutex<Bucket>,
    /// Read volume is debited POST-HOC (the response size is known
    /// only after serving), so this bucket runs negative and reads are
    /// refused while it is in debt.
    read_bytes: Mutex<Bucket>,
    inflight: AtomicU64,
    live_subs: AtomicU64,
    /// SR2-4 max_streams accounting. Seeded LAZILY from the durable
    /// catalog on the first limited create after boot (the count is
    /// process-local; the catalog is the truth it re-derives from).
    /// Soft-deleted fork-retained sources still count — they hold
    /// storage and the name — and release only on their terminal
    /// hard delete.
    streams: Mutex<StreamCount>,
    /// SR2-4 queued_append_bytes: bytes admitted but not yet decided.
    queued_bytes: AtomicU64,

    // ---- round-13: per-project memory-pressure admission ----------
    // ONE canonical project state (review: never a second project map).
    // Static subscription footprint:
    /// Live LiveFeed feeds this project holds on this instance —
    /// charged exactly once per feed by the feed's own pressure guard,
    /// never per subscriber.
    live_feeds: AtomicU64,
    /// EXACT retained LiveFeed bytes — the same reservation the feed
    /// budget accounts (budget.rs updates this counter; it is never
    /// estimated twice).
    retained_sse_bytes: AtomicU64,
    // Request/transient footprint:
    /// Request-body bytes buffered (or reserved from Content-Length)
    /// after auth, before the queued-append phase takes over.
    buffered_body_bytes: AtomicU64,
    // Durable-write pressure:
    /// Encoded frame bytes committed but not yet absorbed —
    /// stream-incarnation-safe attribution via StreamPressureBinding,
    /// seeded from durable debt on open (never from zero).
    unabsorbed_frame_bytes: AtomicU64,
    /// Streams currently holding nonzero unabsorbed bytes (0->pos and
    /// pos->0 transitions only).
    dirty_streams: AtomicU64,
    // Admission state:
    /// 0 = clear, 1 = engaged. Hysteresis latch — engage at the high
    /// watermark, release below high x release_pct/100. Without it the
    /// noisy project oscillates accept/refuse on every completion.
    memory_latch: std::sync::atomic::AtomicU8,
    memory_shed_count: AtomicU64,
    memory_engage_count: AtomicU64,
}

/// Round-13 pressure model v1. The weights are SAFETY ESTIMATES
/// rounded UP from the round-12 certified memory model (26.28 KiB per
/// connection, 7.95 KiB per feed, R^2 0.9987) plus the L1 resident-
/// stream measurement (~45 KiB); they are never billing quantities.
/// Versioned IN CODE: a calibration campaign bumps the version, not a
/// profile knob. Exact counters (retained/queued/body/frame bytes)
/// enter unweighted.
pub(crate) const PROJECT_PRESSURE_MODEL_VERSION: u32 = 1;
pub(crate) const PRESSURE_SUB_WEIGHT_BYTES: u64 = 32 * 1024;
pub(crate) const PRESSURE_FEED_WEIGHT_BYTES: u64 = 16 * 1024;
pub(crate) const PRESSURE_DIRTY_STREAM_WEIGHT_BYTES: u64 = 64 * 1024;

/// Startup + manifest visibility for the model coefficients.
pub(crate) fn pressure_model_json() -> serde_json::Value {
    serde_json::json!({
        "version": PROJECT_PRESSURE_MODEL_VERSION,
        "sub_weight_bytes": PRESSURE_SUB_WEIGHT_BYTES,
        "feed_weight_bytes": PRESSURE_FEED_WEIGHT_BYTES,
        "dirty_stream_weight_bytes": PRESSURE_DIRTY_STREAM_WEIGHT_BYTES,
    })
}

impl ProjectAdmission {
    /// `estimated_project_pressure_bytes` — named for what it is: a
    /// conservative model, not RSS attribution.
    pub(crate) fn estimated_pressure_bytes(&self) -> u64 {
        self.live_subs.load(Ordering::Relaxed) * PRESSURE_SUB_WEIGHT_BYTES
            + self.live_feeds.load(Ordering::Relaxed) * PRESSURE_FEED_WEIGHT_BYTES
            + self.retained_sse_bytes.load(Ordering::Relaxed)
            + self.buffered_body_bytes.load(Ordering::Relaxed)
            + self.queued_bytes.load(Ordering::Relaxed)
            + self.unabsorbed_frame_bytes.load(Ordering::Relaxed)
            + self.dirty_streams.load(Ordering::Relaxed) * PRESSURE_DIRTY_STREAM_WEIGHT_BYTES
    }

    /// Any nonzero pressure dimension pins the entry against tracker
    /// eviction (review: eviction must not orphan outstanding
    /// pressure).
    fn has_pressure(&self) -> bool {
        self.live_feeds.load(Ordering::Relaxed) > 0
            || self.retained_sse_bytes.load(Ordering::Relaxed) > 0
            || self.buffered_body_bytes.load(Ordering::Relaxed) > 0
            || self.queued_bytes.load(Ordering::Relaxed) > 0
            || self.unabsorbed_frame_bytes.load(Ordering::Relaxed) > 0
            || self.dirty_streams.load(Ordering::Relaxed) > 0
    }

    /// Evaluate the memory latch for a WRITE from this project.
    /// Returns true when the append must receive the typed
    /// project_memory_pressure refusal. Emits ONE ops event per
    /// engage and one per release — never per rejected request.
    pub(crate) fn memory_gate(&self, project: &ProjectId, high: u64, release_pct: u64) -> bool {
        if high == 0 {
            return false;
        }
        let p = self.estimated_pressure_bytes();
        let engaged = self.memory_latch.load(Ordering::Relaxed) == 1;
        if !engaged {
            if p > high
                && self
                    .memory_latch
                    .compare_exchange(0, 1, Ordering::Relaxed, Ordering::Relaxed)
                    .is_ok()
            {
                self.memory_engage_count.fetch_add(1, Ordering::Relaxed);
                self.ops.emit(
                    crate::ops::OpsEvent::new(
                        "project_memory_pressure_engaged",
                        format!("pmp-e/{}/{}", project.as_str(), p),
                    )
                    .warn()
                    .fields(serde_json::json!({
                        "project": project.as_str(),
                        "estimated_pressure_bytes": p,
                        "high_water_bytes": high,
                        "model_version": PROJECT_PRESSURE_MODEL_VERSION,
                    })),
                );
                self.memory_shed_count.fetch_add(1, Ordering::Relaxed);
                return true;
            }
            return false;
        }
        let release_below = high.saturating_mul(release_pct.clamp(1, 100)) / 100;
        if p < release_below {
            if self
                .memory_latch
                .compare_exchange(1, 0, Ordering::Relaxed, Ordering::Relaxed)
                .is_ok()
            {
                self.ops.emit(
                    crate::ops::OpsEvent::new(
                        "project_memory_pressure_released",
                        format!("pmp-r/{}/{}", project.as_str(), p),
                    )
                    .fields(serde_json::json!({
                        "project": project.as_str(),
                        "estimated_pressure_bytes": p,
                        "release_below_bytes": release_below,
                    })),
                );
            }
            return false;
        }
        self.memory_shed_count.fetch_add(1, Ordering::Relaxed);
        true
    }

    /// Read-only pressure dimensions (observability + tests).
    pub(crate) fn unabsorbed_frame_bytes_now(&self) -> u64 {
        self.unabsorbed_frame_bytes.load(Ordering::Relaxed)
    }
    pub(crate) fn dirty_streams_now(&self) -> u64 {
        self.dirty_streams.load(Ordering::Relaxed)
    }
    pub(crate) fn buffered_body_bytes_now(&self) -> u64 {
        self.buffered_body_bytes.load(Ordering::Relaxed)
    }

    /// EXACT retained-byte mirror for the LiveFeed budget (budget.rs
    /// calls these on reserve/release — one counter, one truth).
    pub(crate) fn retained_sse_add(&self, bytes: u64) {
        self.retained_sse_bytes.fetch_add(bytes, Ordering::Relaxed);
    }
    pub(crate) fn retained_sse_sub(&self, bytes: u64) {
        let mut cur = self.retained_sse_bytes.load(Ordering::Relaxed);
        loop {
            let next = cur.saturating_sub(bytes);
            match self.retained_sse_bytes.compare_exchange_weak(
                cur,
                next,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => return,
                Err(v) => cur = v,
            }
        }
    }
}

/// One live LiveFeed feed's static charge — held BY the feed object,
/// so concurrent first subscribers charge the feed exactly once and
/// the last teardown releases it exactly once.
pub(crate) struct FeedPressureGuard {
    admission: Arc<ProjectAdmission>,
}

impl FeedPressureGuard {
    pub(crate) fn acquire(admission: Arc<ProjectAdmission>) -> Self {
        admission.live_feeds.fetch_add(1, Ordering::Relaxed);
        FeedPressureGuard { admission }
    }
}

impl Drop for FeedPressureGuard {
    fn drop(&mut self) {
        self.admission.live_feeds.fetch_sub(1, Ordering::Relaxed);
    }
}

/// Request-body memory charge: reserved from Content-Length (or grown
/// incrementally for chunked bodies) after auth, released or handed
/// to the queued-append charge when the body is decided. Never
/// pessimistically the protocol ceiling.
pub(crate) struct BufferedBodyGuard {
    admission: Arc<ProjectAdmission>,
    bytes: u64,
}

impl BufferedBodyGuard {
    pub(crate) fn reserve(admission: Arc<ProjectAdmission>, bytes: u64) -> Self {
        admission
            .buffered_body_bytes
            .fetch_add(bytes, Ordering::Relaxed);
        BufferedBodyGuard { admission, bytes }
    }
    /// Chunked bodies charge as chunks arrive.
    pub(crate) fn grow(&mut self, more: u64) {
        self.bytes += more;
        self.admission
            .buffered_body_bytes
            .fetch_add(more, Ordering::Relaxed);
    }
}

impl Drop for BufferedBodyGuard {
    fn drop(&mut self) {
        self.admission
            .buffered_body_bytes
            .fetch_sub(self.bytes, Ordering::Relaxed);
    }
}

/// Stream-incarnation-safe durable-write attribution: held by the
/// stream handle, bound once from the tenant-qualified descriptor.
/// Frames the committer adds raise the project's unabsorbed debt;
/// absorption retires it; the 0->pos / pos->0 edges move the
/// dirty-stream count. On open of a stream with existing durable debt
/// the binding is SEEDED from the tail — never from zero (a decaying
/// approximation could clear itself during a real absorber stall).
/// Drop (close, eviction, owner movement) releases this instance's
/// attribution exactly.
pub(crate) struct StreamPressureBinding {
    admission: Arc<ProjectAdmission>,
    current_unabsorbed: AtomicU64,
}

impl StreamPressureBinding {
    pub(crate) fn bind(admission: Arc<ProjectAdmission>, seed_unabsorbed: u64) -> Self {
        if seed_unabsorbed > 0 {
            admission
                .unabsorbed_frame_bytes
                .fetch_add(seed_unabsorbed, Ordering::Relaxed);
            admission.dirty_streams.fetch_add(1, Ordering::Relaxed);
        }
        StreamPressureBinding {
            admission,
            current_unabsorbed: AtomicU64::new(seed_unabsorbed),
        }
    }

    /// The committer added `bytes` of ACTUAL encoded frame.
    pub(crate) fn frames_added(&self, bytes: u64) {
        if bytes == 0 {
            return;
        }
        let prev = self.current_unabsorbed.fetch_add(bytes, Ordering::Relaxed);
        self.admission
            .unabsorbed_frame_bytes
            .fetch_add(bytes, Ordering::Relaxed);
        if prev == 0 {
            self.admission.dirty_streams.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Absorption retired `bytes` of frame. The pos->0 edge decision
    /// rides the CAS itself — a later re-read could race a concurrent
    /// add's 0->pos edge and leak a dirty-stream count.
    pub(crate) fn frames_retired(&self, bytes: u64) {
        if bytes == 0 {
            return;
        }
        let mut cur = self.current_unabsorbed.load(Ordering::Relaxed);
        let (taken, went_zero) = loop {
            let take = bytes.min(cur);
            match self.current_unabsorbed.compare_exchange_weak(
                cur,
                cur - take,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break (take, take > 0 && cur == take),
                Err(v) => cur = v,
            }
        };
        if taken > 0 {
            self.admission.retained_sub_frames(taken);
            if went_zero {
                self.admission.dirty_streams.fetch_sub(1, Ordering::Relaxed);
            }
        }
    }
}

impl ProjectAdmission {
    fn retained_sub_frames(&self, bytes: u64) {
        let mut cur = self.unabsorbed_frame_bytes.load(Ordering::Relaxed);
        loop {
            let next = cur.saturating_sub(bytes);
            match self.unabsorbed_frame_bytes.compare_exchange_weak(
                cur,
                next,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => return,
                Err(v) => cur = v,
            }
        }
    }
}

impl Drop for StreamPressureBinding {
    fn drop(&mut self) {
        let left = self.current_unabsorbed.load(Ordering::Relaxed);
        if left > 0 {
            self.admission.retained_sub_frames(left);
            self.admission.dirty_streams.fetch_sub(1, Ordering::Relaxed);
        }
    }
}

#[derive(Default)]
struct StreamCount {
    seeded: bool,
    count: u64,
}

/// Holds one reserved stream slot until the create DECIDES: `commit`
/// keeps the +1 (the caller truly created a new stream), drop rolls
/// it back (replay, refusal, error).
pub(crate) struct StreamReservation {
    admission: Arc<ProjectAdmission>,
    committed: bool,
}

impl StreamReservation {
    pub(crate) fn commit(mut self) {
        self.committed = true;
    }
}

impl Drop for StreamReservation {
    #[expect(
        clippy::unwrap_used,
        reason = "StreamReservation::drop; a poisoned count may have been partially updated before cancellation; decrementing recovered state could free a slot still occupied by a live stream"
    )]
    fn drop(&mut self) {
        if !self.committed {
            let mut st = self.admission.streams.lock().unwrap();
            st.count = st.count.saturating_sub(1);
        }
    }
}

/// Releases the queued-byte charge when the append is DECIDED (the
/// handler's await returns, success or failure).
pub(crate) struct QueuedBytesGuard {
    admission: Arc<ProjectAdmission>,
    bytes: u64,
}

impl Drop for QueuedBytesGuard {
    fn drop(&mut self) {
        self.admission
            .queued_bytes
            .fetch_sub(self.bytes, Ordering::Relaxed);
    }
}

#[derive(Debug)]
pub(crate) enum QuotaRefusal {
    /// Seconds until a token is expected (for Retry-After).
    Rate {
        retry_after_secs: u64,
    },
    Concurrency,
    TrackerCapacity,
    /// SR2-4: the project holds `max_streams` live streams.
    StreamLimit,
    /// SR2-4: the project's committer-queued append bytes are at the
    /// ceiling; retry after in-flight appends decide.
    QueuedBytes,
    /// Round-13: the project's estimated memory pressure crossed the
    /// per-project backstop — typed, project-audited, retryable.
    MemoryPressure,
}

/// Releases the inflight slot on drop — hold it for the handler's
/// lifetime. (Streaming response bodies outlive the handler; their
/// long-lived cost is the live-subscription dimension, not this
/// counter.)
pub(crate) struct QuotaGuard {
    admission: Arc<ProjectAdmission>,
}

impl Drop for QuotaGuard {
    fn drop(&mut self) {
        self.admission.inflight.fetch_sub(1, Ordering::Relaxed);
    }
}

/// One live subscription (§17.2). Attached to the STREAMING response
/// body, so it releases when the stream ends or the client goes away —
/// not when the handler returns.
pub(crate) struct SubscriptionGuard {
    admission: Arc<ProjectAdmission>,
}

impl Drop for SubscriptionGuard {
    fn drop(&mut self) {
        self.admission.live_subs.fetch_sub(1, Ordering::Relaxed);
    }
}

#[derive(Default, Clone)]
pub(crate) struct QuotaRegistry {
    ops: Arc<crate::ops::OpsService>,
    projects: Arc<Mutex<HashMap<ProjectId, Arc<ProjectAdmission>>>>,
}

impl QuotaRegistry {
    pub(crate) fn new(ops: Arc<crate::ops::OpsService>) -> Self {
        Self {
            ops,
            projects: Default::default(),
        }
    }

    /// Acquire admission for one request of `project` under `quotas`
    /// (from the CURRENT policy snapshot — never token claims, §17.2).
    /// Quota value 0 = not configured at this level (cell safety
    /// limits still apply elsewhere).
    #[expect(
        clippy::unwrap_used,
        reason = "QuotaRegistry::admit; a poisoned entry or project map may be partially updated; recovery could mint fresh request credit or orphan charged state"
    )]
    pub(crate) fn admit(
        &self,
        project: &ProjectId,
        quotas: &ProjectQuotas,
        now_ms: i64,
    ) -> Result<QuotaGuard, QuotaRefusal> {
        let admission = {
            let mut m = self.projects.lock().unwrap();
            match m.get(project) {
                Some(a) => a.clone(),
                None => {
                    if m.len() >= MAX_TRACKED_PROJECTS {
                        // Review item 5: EVICT idle entries before
                        // refusing — a tracker that filled once must
                        // not refuse project 1,025 until restart.
                        // Never evict a project with inflight requests
                        // or live subscriptions; their guards point at
                        // the Arc we would orphan.
                        m.retain(|_, a| {
                            a.inflight.load(Ordering::Relaxed) > 0
                                || a.live_subs.load(Ordering::Relaxed) > 0
                                // Round-13: outstanding memory pressure
                                // pins the entry — eviction would
                                // orphan feed/body/frame attribution.
                                || a.has_pressure()
                                || now_ms - a.last_seen_ms.load(Ordering::Relaxed) < IDLE_EVICT_MS
                        });
                        if m.len() >= MAX_TRACKED_PROJECTS {
                            // Refuse to TRACK, never to merge: an
                            // untracked project sharing a bucket with
                            // strangers would couple their fates.
                            return Err(QuotaRefusal::TrackerCapacity);
                        }
                    }
                    let a = Arc::new(ProjectAdmission {
                        ops: self.ops.clone(),
                        last_seen_ms: std::sync::atomic::AtomicI64::new(now_ms),
                        bucket: Mutex::new(Bucket::full(quotas.requests_per_sec, now_ms)),
                        append_bytes: Mutex::new(Bucket::full(quotas.append_bytes_per_sec, now_ms)),
                        append_records: Mutex::new(Bucket::full(
                            quotas.append_records_per_sec,
                            now_ms,
                        )),
                        read_bytes: Mutex::new(Bucket::full(quotas.read_bytes_per_sec, now_ms)),
                        inflight: AtomicU64::new(0),
                        live_subs: AtomicU64::new(0),
                        streams: Mutex::new(StreamCount::default()),
                        queued_bytes: AtomicU64::new(0),
                        live_feeds: AtomicU64::new(0),
                        retained_sse_bytes: AtomicU64::new(0),
                        buffered_body_bytes: AtomicU64::new(0),
                        unabsorbed_frame_bytes: AtomicU64::new(0),
                        dirty_streams: AtomicU64::new(0),
                        memory_latch: std::sync::atomic::AtomicU8::new(0),
                        memory_shed_count: AtomicU64::new(0),
                        memory_engage_count: AtomicU64::new(0),
                    });
                    m.insert(project.clone(), a.clone());
                    a
                }
            }
        };

        admission.last_seen_ms.store(now_ms, Ordering::Relaxed);
        // Request-rate token bucket: refill rps/sec continuously,
        // capped at one second's worth (burst == rate). The rate is
        // re-read from the CURRENT quotas every admit, so a policy
        // update takes effect on the next request without any
        // republish handshake.
        if quotas.requests_per_sec > 0
            && let Err(retry_after_secs) =
                admission
                    .bucket
                    .lock()
                    .unwrap()
                    .take(quotas.requests_per_sec as f64, 1.0, now_ms)
        {
            return Err(QuotaRefusal::Rate { retry_after_secs });
        }

        if quotas.max_inflight_requests > 0 {
            // Optimistic acquire; back out on overshoot. Relaxed is
            // fine: this is a backstop counter, not a synchronization
            // edge.
            let prev = admission.inflight.fetch_add(1, Ordering::Relaxed);
            if prev >= quotas.max_inflight_requests {
                admission.inflight.fetch_sub(1, Ordering::Relaxed);
                return Err(QuotaRefusal::Concurrency);
            }
        } else {
            admission.inflight.fetch_add(1, Ordering::Relaxed);
        }
        Ok(QuotaGuard { admission })
    }

    /// §17.2 append-volume backstop, checked at the APPEND site with
    /// the exact buffered payload size — after the request-rate admit,
    /// before the write is dispatched. A single append larger than one
    /// second's budget is still admitted when the bucket is full
    /// (otherwise it could never succeed); it drives the bucket
    /// negative and later appends wait it out.
    #[expect(
        clippy::unwrap_used,
        reason = "QuotaRegistry::admit_append; either poisoned bucket or its project map may contain an incomplete charge; recovery could admit beyond a limit or charge only half a batch"
    )]
    pub fn admit_append(
        &self,
        project: &ProjectId,
        quotas: &ProjectQuotas,
        payload_bytes: u64,
        records: u64,
        now_ms: i64,
    ) -> Result<(), QuotaRefusal> {
        if quotas.append_bytes_per_sec == 0 && quotas.append_records_per_sec == 0 {
            return Ok(());
        }
        let admission = {
            let m = self.projects.lock().unwrap();
            match m.get(project) {
                Some(a) => a.clone(),
                // admit() ran first on this request; absence means the
                // tracker refused it there.
                None => return Err(QuotaRefusal::TrackerCapacity),
            }
        };
        // Review item 5: the two debits are ATOMIC — both buckets are
        // held, both refilled, both CHECKED, and only then both
        // charged. The first cut charged bytes before records could
        // refuse, so a refused batch still burned byte budget.
        let mut bytes_b = admission.append_bytes.lock().unwrap();
        let mut recs_b = admission.append_records.lock().unwrap();
        let mut plan: [Option<(f64, f64)>; 2] = [None, None]; // (rate, cost)
        let check = |b: &mut Bucket,
                     rate_u: u64,
                     cost: f64,
                     slot: &mut Option<(f64, f64)>|
         -> Result<(), QuotaRefusal> {
            if rate_u == 0 {
                return Ok(());
            }
            let rate = rate_u as f64;
            b.refill(rate, now_ms);
            let full = b.level >= rate - f64::EPSILON;
            if full && cost > rate {
                // Oversized single op from a full bucket: admit once,
                // go negative; later ops wait the debt out.
                *slot = Some((rate, cost));
                return Ok(());
            }
            if b.level < cost {
                return Err(QuotaRefusal::Rate {
                    retry_after_secs: b.retry_after(rate, cost),
                });
            }
            *slot = Some((rate, cost));
            Ok(())
        };
        check(
            &mut bytes_b,
            quotas.append_bytes_per_sec,
            payload_bytes as f64,
            &mut plan[0],
        )?;
        check(
            &mut recs_b,
            quotas.append_records_per_sec,
            records as f64,
            &mut plan[1],
        )?;
        if let Some((_, cost)) = plan[0] {
            bytes_b.level -= cost;
        }
        if let Some((_, cost)) = plan[1] {
            recs_b.level -= cost;
        }
        Ok(())
    }

    /// Read admission (§17.2): reads are refused while the project's
    /// read-byte bucket is IN DEBT from earlier responses. The check is
    /// cheap and runs before serving; the debit lands after.
    #[expect(
        clippy::unwrap_used,
        reason = "QuotaRegistry::check_read; a poisoned balance cannot prove whether existing debt has cleared; recovery could admit more work against an untrusted balance"
    )]
    pub(crate) fn check_read(
        &self,
        project: &ProjectId,
        quotas: &ProjectQuotas,
        now_ms: i64,
    ) -> Result<(), QuotaRefusal> {
        if quotas.read_bytes_per_sec == 0 {
            return Ok(());
        }
        let Some(admission) = self.tracked(project) else {
            return Ok(()); // request-rate admit tracks first
        };
        let rate = quotas.read_bytes_per_sec as f64;
        let mut b = admission.read_bytes.lock().unwrap();
        b.refill(rate, now_ms);
        if b.level < 0.0 {
            return Err(QuotaRefusal::Rate {
                retry_after_secs: b.retry_after(rate, 0.0),
            });
        }
        Ok(())
    }

    /// Post-hoc read debit with the SERVED byte count. Deliberately
    /// unconditional and negative-capable: the response was already
    /// sent, so the debt is real either way.
    #[expect(
        clippy::unwrap_used,
        reason = "QuotaRegistry::debit_read; a poisoned balance may contain a partial debit; recovery could lose the cost of bytes already delivered"
    )]
    pub(crate) fn debit_read(
        &self,
        project: &ProjectId,
        quotas: &ProjectQuotas,
        bytes: u64,
        now_ms: i64,
    ) {
        if quotas.read_bytes_per_sec == 0 || bytes == 0 {
            return;
        }
        if let Some(admission) = self.tracked(project) {
            let rate = quotas.read_bytes_per_sec as f64;
            let mut b = admission.read_bytes.lock().unwrap();
            b.refill(rate, now_ms);
            b.level -= bytes as f64;
        }
    }

    /// §17.2 subscriptions: acquire one live-subscription slot. The
    /// guard rides the streaming response body.
    pub(crate) fn admit_subscription(
        &self,
        project: &ProjectId,
        quotas: &ProjectQuotas,
    ) -> Result<Option<SubscriptionGuard>, QuotaRefusal> {
        let Some(admission) = self.tracked(project) else {
            return Ok(None);
        };
        // Round-13.3 (field A1): the count is UNCONDITIONAL — live
        // subscriptions are memory pressure whether or not a refusal
        // quota is configured (a default-quota noisy project held 200
        // connections the pressure model could not see). The quota,
        // when configured, stays the refusal line.
        let prev = admission.live_subs.fetch_add(1, Ordering::Relaxed);
        if quotas.max_live_subscriptions > 0 && prev >= quotas.max_live_subscriptions {
            admission.live_subs.fetch_sub(1, Ordering::Relaxed);
            return Err(QuotaRefusal::Concurrency);
        }
        Ok(Some(SubscriptionGuard { admission }))
    }

    /// SR2-4: does the project's stream count still need its catalog
    /// seed? The caller counts (async, catalog pages) only when this
    /// says so, then passes the count to `reserve_stream`.
    #[expect(
        clippy::unwrap_used,
        reason = "QuotaRegistry::needs_stream_seed; a poisoned count may have an incomplete seeded flag or count; treating it as valid could bypass the catalog seed"
    )]
    pub(crate) fn needs_stream_seed(&self, project: &ProjectId) -> bool {
        self.tracked(project)
            .map(|a| !a.streams.lock().unwrap().seeded)
            .unwrap_or(false)
    }

    /// SR2-4: reserve one stream slot under `max_streams`, race-safely
    /// WITHIN THIS PROCESS (count checked and bumped under one lock;
    /// concurrent racers on this instance serialize here, losers
    /// refuse typed). SR3-2 posture: this is a PER-INSTANCE SAFETY
    /// BACKSTOP, not an exact project-wide quota — two cell instances
    /// owning different shards can each admit against their own view
    /// and briefly exceed the cap by the instance count. The exact
    /// project-wide owner (durable registry counter vs gateway quota
    /// affinity) is an open platform decision recorded in
    /// docs/CONTROL-PLANE-INTEGRATION.md §9. `seed` supplies the
    /// catalog count when this project has not been seeded since boot;
    /// the first reservation wins the seed, later ones ignore theirs.
    #[expect(
        clippy::unwrap_used,
        reason = "QuotaRegistry::reserve_stream; a poisoned count cannot prove current occupancy; recovering zero or partial counts could exceed the stream limit"
    )]
    pub(crate) fn reserve_stream(
        &self,
        project: &ProjectId,
        quotas: &ProjectQuotas,
        seed: Option<u64>,
    ) -> Result<Option<StreamReservation>, QuotaRefusal> {
        if quotas.max_streams == 0 {
            return Ok(None);
        }
        let Some(admission) = self.tracked(project) else {
            return Ok(None);
        };
        let mut st = admission.streams.lock().unwrap();
        if !st.seeded {
            let Some(n) = seed else {
                // Caller must seed first; refuse closed rather than
                // guess (a wrong zero would admit past the cap).
                return Err(QuotaRefusal::StreamLimit);
            };
            st.seeded = true;
            st.count = n;
        }
        if st.count >= quotas.max_streams {
            return Err(QuotaRefusal::StreamLimit);
        }
        st.count += 1;
        drop(st);
        Ok(Some(StreamReservation {
            admission,
            committed: false,
        }))
    }

    /// SR2-4: a terminal hard delete frees the slot. Unseeded (or
    /// untracked) projects no-op — their next seed recounts the
    /// catalog, which already reflects the deletion.
    #[expect(
        clippy::unwrap_used,
        reason = "QuotaRegistry::release_stream; a poisoned count may contain an incomplete reservation or release; decrementing recovered state could free occupied capacity"
    )]
    pub(crate) fn release_stream(&self, project: &ProjectId) {
        if let Some(a) = self.tracked(project) {
            let mut st = a.streams.lock().unwrap();
            if st.seeded {
                st.count = st.count.saturating_sub(1);
            }
        }
    }

    /// SR2-4: charge `bytes` to the project's committer-queue budget
    /// BEFORE the append is enqueued; the guard releases when the
    /// append DECIDES. 0 = not configured.
    pub(crate) fn charge_queued(
        &self,
        project: &ProjectId,
        quotas: &ProjectQuotas,
        bytes: u64,
    ) -> Result<Option<QueuedBytesGuard>, QuotaRefusal> {
        let Some(admission) = self.tracked(project) else {
            return Ok(None);
        };
        // Round-13.3 (field A1): queued bytes are the standing
        // committer-queue memory — charged UNCONDITIONALLY (the noisy
        // project held ~12 MB of ten-second queue the model could not
        // see); the configured ceiling stays the refusal line.
        let new = admission.queued_bytes.fetch_add(bytes, Ordering::Relaxed) + bytes;
        if quotas.queued_append_bytes > 0 && new > quotas.queued_append_bytes {
            admission.queued_bytes.fetch_sub(bytes, Ordering::Relaxed);
            return Err(QuotaRefusal::QueuedBytes);
        }
        Ok(Some(QueuedBytesGuard { admission, bytes }))
    }

    #[expect(
        clippy::unwrap_used,
        reason = "QuotaRegistry::tracked; a poisoned map may have an incomplete entry publication; recovery could orphan an existing project charge"
    )]
    fn tracked(&self, project: &ProjectId) -> Option<Arc<ProjectAdmission>> {
        self.projects.lock().unwrap().get(project).cloned()
    }

    /// Round-13: the pressure layers (LiveFeed budget, stream
    /// bindings, body guards) attach to the ONE canonical project
    /// entry. admit() runs first on every authenticated request, so
    /// the entry exists whenever pressure can.
    pub(crate) fn pressure_handle(&self, project: &ProjectId) -> Option<Arc<ProjectAdmission>> {
        self.tracked(project)
    }

    /// Bounded per-project pressure rows + process aggregates for
    /// /v1/debug/load.
    #[expect(
        clippy::unwrap_used,
        reason = "QuotaRegistry::memory_pressure_json; a poisoned project map may omit still-charged entries; recovering it would report an incomplete memory-pressure view"
    )]
    pub(crate) fn memory_pressure_json(&self, high: u64, limit: usize) -> serde_json::Value {
        let m = self.projects.lock().unwrap();
        let mut engaged = 0u64;
        let mut shed_total = 0u64;
        let mut highest = 0u64;
        let mut rows: Vec<(u64, serde_json::Value)> = Vec::new();
        for (id, a) in m.iter() {
            let p = a.estimated_pressure_bytes();
            let is_engaged = a.memory_latch.load(Ordering::Relaxed) == 1;
            engaged += u64::from(is_engaged);
            shed_total += a.memory_shed_count.load(Ordering::Relaxed);
            highest = highest.max(p);
            if p > 0 || is_engaged {
                rows.push((
                    p,
                    serde_json::json!({
                        "project": id.as_str(),
                        "pressure_model_version": PROJECT_PRESSURE_MODEL_VERSION,
                        "estimated_pressure_bytes": p,
                        "high_water_bytes": high,
                        "engaged": is_engaged,
                        "live_subscriptions": a.live_subs.load(Ordering::Relaxed),
                        "live_feeds": a.live_feeds.load(Ordering::Relaxed),
                        "retained_sse_bytes": a.retained_sse_bytes.load(Ordering::Relaxed),
                        "buffered_body_bytes": a.buffered_body_bytes_now(),
                        "queued_append_bytes": a.queued_bytes.load(Ordering::Relaxed),
                        "unabsorbed_frame_bytes": a.unabsorbed_frame_bytes_now(),
                        "dirty_streams": a.dirty_streams_now(),
                        "engage_count": a.memory_engage_count.load(Ordering::Relaxed),
                        "shed_count": a.memory_shed_count.load(Ordering::Relaxed),
                    }),
                ));
            }
        }
        rows.sort_by_key(|x| std::cmp::Reverse(x.0));
        rows.truncate(limit);
        serde_json::json!({
            "projects_memory_engaged": engaged,
            "project_memory_shed_total": shed_total,
            "highest_project_pressure_bytes": highest,
            "rows": rows.into_iter().map(|(_, v)| v).collect::<Vec<_>>(),
        })
    }

    /// Operator visibility: (projects tracked, total inflight).
    #[expect(
        clippy::unwrap_used,
        reason = "QuotaRegistry::stats; a poisoned project map may omit active guards; recovering it would misreport live occupancy"
    )]
    pub(crate) fn stats(&self) -> (usize, u64) {
        let m = self.projects.lock().unwrap();
        let inflight = m.values().map(|a| a.inflight.load(Ordering::Relaxed)).sum();
        (m.len(), inflight)
    }
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod pressure_tests;

#[cfg(test)]
mod pressure_counting_tests;

#[cfg(test)]
mod poison_tests;
