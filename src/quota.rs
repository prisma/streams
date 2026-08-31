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
pub const MAX_TRACKED_PROJECTS: usize = 16_384;

/// A tracked project with no admission attempts for this long (and no
/// inflight work) may be evicted under tracker pressure. Its buckets
/// restart full — an idle project lost no accumulated debt worth
/// keeping at this horizon.
pub const IDLE_EVICT_MS: i64 = 300_000;

struct Bucket {
    /// Fractional tokens currently available.
    level: f64,
    last_ms: i64,
}

impl Bucket {
    /// Refill at `rate`/sec (capped at one second's burst) and try to
    /// take `cost` tokens. On refusal returns seconds until enough
    /// tokens exist (Retry-After).
    fn take(&mut self, rate: f64, cost: f64, now_ms: i64) -> Result<(), u64> {
        let dt_s = ((now_ms - self.last_ms).max(0) as f64) / 1000.0;
        self.level = (self.level + dt_s * rate).min(rate);
        self.last_ms = now_ms;
        if self.level < cost {
            return Err((((cost - self.level) / rate).ceil().max(1.0)) as u64);
        }
        self.level -= cost;
        Ok(())
    }
}

pub struct ProjectAdmission {
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
pub const PROJECT_PRESSURE_MODEL_VERSION: u32 = 1;
pub const PRESSURE_SUB_WEIGHT_BYTES: u64 = 32 * 1024;
pub const PRESSURE_FEED_WEIGHT_BYTES: u64 = 16 * 1024;
pub const PRESSURE_DIRTY_STREAM_WEIGHT_BYTES: u64 = 64 * 1024;

/// Startup + manifest visibility for the model coefficients.
pub fn pressure_model_json() -> serde_json::Value {
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
    pub fn estimated_pressure_bytes(&self) -> u64 {
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
    pub fn memory_gate(&self, project: &ProjectId, high: u64, release_pct: u64) -> bool {
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
                crate::ops::emit(
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
                crate::ops::emit(
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
    pub fn unabsorbed_frame_bytes_now(&self) -> u64 {
        self.unabsorbed_frame_bytes.load(Ordering::Relaxed)
    }
    pub fn dirty_streams_now(&self) -> u64 {
        self.dirty_streams.load(Ordering::Relaxed)
    }
    pub fn buffered_body_bytes_now(&self) -> u64 {
        self.buffered_body_bytes.load(Ordering::Relaxed)
    }

    /// EXACT retained-byte mirror for the LiveFeed budget (budget.rs
    /// calls these on reserve/release — one counter, one truth).
    pub fn retained_sse_add(&self, bytes: u64) {
        self.retained_sse_bytes.fetch_add(bytes, Ordering::Relaxed);
    }
    pub fn retained_sse_sub(&self, bytes: u64) {
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
pub struct FeedPressureGuard {
    admission: Arc<ProjectAdmission>,
}

impl FeedPressureGuard {
    pub fn acquire(admission: Arc<ProjectAdmission>) -> Self {
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
pub struct BufferedBodyGuard {
    admission: Arc<ProjectAdmission>,
    bytes: u64,
}

impl BufferedBodyGuard {
    pub fn reserve(admission: Arc<ProjectAdmission>, bytes: u64) -> Self {
        admission
            .buffered_body_bytes
            .fetch_add(bytes, Ordering::Relaxed);
        BufferedBodyGuard { admission, bytes }
    }
    /// Chunked bodies charge as chunks arrive.
    pub fn grow(&mut self, more: u64) {
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
pub struct StreamPressureBinding {
    admission: Arc<ProjectAdmission>,
    current_unabsorbed: AtomicU64,
}

impl StreamPressureBinding {
    pub fn bind(admission: Arc<ProjectAdmission>, seed_unabsorbed: u64) -> Self {
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
    pub fn frames_added(&self, bytes: u64) {
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
    pub fn frames_retired(&self, bytes: u64) {
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
pub struct StreamReservation {
    admission: Arc<ProjectAdmission>,
    committed: bool,
}

impl StreamReservation {
    pub fn commit(mut self) {
        self.committed = true;
    }
}

impl Drop for StreamReservation {
    fn drop(&mut self) {
        if !self.committed {
            let mut st = self.admission.streams.lock().unwrap();
            st.count = st.count.saturating_sub(1);
        }
    }
}

/// Releases the queued-byte charge when the append is DECIDED (the
/// handler's await returns, success or failure).
pub struct QueuedBytesGuard {
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
pub enum QuotaRefusal {
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
pub struct QuotaGuard {
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
pub struct SubscriptionGuard {
    admission: Arc<ProjectAdmission>,
}

impl Drop for SubscriptionGuard {
    fn drop(&mut self) {
        self.admission.live_subs.fetch_sub(1, Ordering::Relaxed);
    }
}

#[derive(Default)]
pub struct QuotaRegistry {
    projects: Mutex<HashMap<ProjectId, Arc<ProjectAdmission>>>,
}

impl QuotaRegistry {
    /// Acquire admission for one request of `project` under `quotas`
    /// (from the CURRENT policy snapshot — never token claims, §17.2).
    /// Quota value 0 = not configured at this level (cell safety
    /// limits still apply elsewhere).
    pub fn admit(
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
                        last_seen_ms: std::sync::atomic::AtomicI64::new(now_ms),
                        bucket: Mutex::new(Bucket {
                            // A fresh project starts with a full
                            // second's burst.
                            level: quotas.requests_per_sec as f64,
                            last_ms: now_ms,
                        }),
                        append_bytes: Mutex::new(Bucket {
                            level: quotas.append_bytes_per_sec as f64,
                            last_ms: now_ms,
                        }),
                        append_records: Mutex::new(Bucket {
                            level: quotas.append_records_per_sec as f64,
                            last_ms: now_ms,
                        }),
                        read_bytes: Mutex::new(Bucket {
                            level: quotas.read_bytes_per_sec as f64,
                            last_ms: now_ms,
                        }),
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
            let _ = b.take(rate, 0.0, now_ms); // refill only
            let full = b.level >= rate - f64::EPSILON;
            if full && cost > rate {
                // Oversized single op from a full bucket: admit once,
                // go negative; later ops wait the debt out.
                *slot = Some((rate, cost));
                return Ok(());
            }
            if b.level < cost {
                return Err(QuotaRefusal::Rate {
                    retry_after_secs: (((cost - b.level) / rate).ceil().max(1.0)) as u64,
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
    pub fn check_read(
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
        let _ = b.take(rate, 0.0, now_ms); // refill only
        if b.level < 0.0 {
            return Err(QuotaRefusal::Rate {
                retry_after_secs: ((-b.level / rate).ceil().max(1.0)) as u64,
            });
        }
        Ok(())
    }

    /// Post-hoc read debit with the SERVED byte count. Deliberately
    /// unconditional and negative-capable: the response was already
    /// sent, so the debt is real either way.
    pub fn debit_read(&self, project: &ProjectId, quotas: &ProjectQuotas, bytes: u64, now_ms: i64) {
        if quotas.read_bytes_per_sec == 0 || bytes == 0 {
            return;
        }
        if let Some(admission) = self.tracked(project) {
            let rate = quotas.read_bytes_per_sec as f64;
            let mut b = admission.read_bytes.lock().unwrap();
            let _ = b.take(rate, 0.0, now_ms); // refill first
            b.level -= bytes as f64;
        }
    }

    /// §17.2 subscriptions: acquire one live-subscription slot. The
    /// guard rides the streaming response body.
    pub fn admit_subscription(
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
    pub fn needs_stream_seed(&self, project: &ProjectId) -> bool {
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
    pub fn reserve_stream(
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
    pub fn release_stream(&self, project: &ProjectId) {
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
    pub fn charge_queued(
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

    fn tracked(&self, project: &ProjectId) -> Option<Arc<ProjectAdmission>> {
        self.projects.lock().unwrap().get(project).cloned()
    }

    /// Round-13: the pressure layers (LiveFeed budget, stream
    /// bindings, body guards) attach to the ONE canonical project
    /// entry. admit() runs first on every authenticated request, so
    /// the entry exists whenever pressure can.
    pub fn pressure_handle(&self, project: &ProjectId) -> Option<Arc<ProjectAdmission>> {
        self.tracked(project)
    }

    /// Bounded per-project pressure rows + process aggregates for
    /// /v1/debug/load.
    pub fn memory_pressure_json(&self, high: u64, limit: usize) -> serde_json::Value {
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
        rows.sort_by(|x, y| y.0.cmp(&x.0));
        rows.truncate(limit);
        serde_json::json!({
            "projects_memory_engaged": engaged,
            "project_memory_shed_total": shed_total,
            "highest_project_pressure_bytes": highest,
            "rows": rows.into_iter().map(|(_, v)| v).collect::<Vec<_>>(),
        })
    }

    /// Operator visibility: (projects tracked, total inflight).
    pub fn stats(&self) -> (usize, u64) {
        let m = self.projects.lock().unwrap();
        let inflight = m.values().map(|a| a.inflight.load(Ordering::Relaxed)).sum();
        (m.len(), inflight)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn q(rps: u64, inflight: u64) -> ProjectQuotas {
        ProjectQuotas {
            requests_per_sec: rps,
            max_inflight_requests: inflight,
            ..Default::default()
        }
    }

    fn pid(s: &str) -> ProjectId {
        ProjectId::new(s).unwrap()
    }

    /// Workload-cert W1xW2 shape (bench/WORKLOAD-CERT-PLAN.md): 10,000
    /// resident tenants, 100 active per 5s window rotating over the
    /// whole population = 20 first-seen projects/s sustained. With
    /// IDLE_EVICT_MS of un-evictable recency, steady-state demand is
    /// 20/s x 300s = 6,000 tracked entries — the cap must hold the
    /// certified tenant population, not just its active window.
    #[test]
    fn cert_rotation_over_ten_thousand_tenants_never_hits_tracker_capacity() {
        let r = QuotaRegistry::default();
        let quotas = ProjectQuotas::default();
        let t0: i64 = 1_000_000;
        let name = |i: usize| pid(&format!("cert_{i}"));
        let mut refused = 0usize;
        // 10,000 distinct projects, 20 new per second (cert pacing),
        // each holding its guard only for the request instant.
        for i in 0..10_000usize {
            let now = t0 + (i as i64) * 50; // 20/s
            match r.admit(&name(i), &quotas, now) {
                Ok(_g) => {}
                Err(QuotaRefusal::TrackerCapacity) => refused += 1,
                Err(e) => panic!("unexpected refusal {e:?}"),
            }
        }
        assert_eq!(
            refused, 0,
            "the certification rotation must never see TrackerCapacity"
        );
    }

    /// SR-6 tracker-capacity churn: at the cap the tracker refuses
    /// NEW projects with the typed refusal while every entry is
    /// recent, evicts the idle mass (never a project with live
    /// guards) once the horizon passes, and re-tracks returning
    /// projects — a tracker that filled once must not refuse until
    /// restart, and must never orphan an entry whose guards are held.
    #[test]
    fn tracker_capacity_churn_evicts_idle_never_active() {
        let r = QuotaRegistry::default();
        let quotas = q(0, 2);
        let t0: i64 = 1_000_000;
        let name = |i: usize| pid(&format!("churn_{i}"));
        // Pin churn_0 with BOTH its concurrency slots held live.
        let _g0 = r.admit(&name(0), &quotas, t0).expect("pin 1");
        let _g1 = r.admit(&name(0), &quotas, t0).expect("pin 2");
        for i in 1..MAX_TRACKED_PROJECTS {
            r.admit(&name(i), &quotas, t0).expect("fill");
        }
        // At capacity with every entry recent: typed refusal —
        // track-or-refuse, never merge into a stranger's buckets.
        match r.admit(&name(MAX_TRACKED_PROJECTS), &quotas, t0 + 1_000) {
            Err(QuotaRefusal::TrackerCapacity) => {}
            Ok(_) => panic!("expected TrackerCapacity, got admission"),
            Err(e) => panic!("expected TrackerCapacity, got {e:?}"),
        }
        // Past the idle horizon the idle mass is evictable: thousands
        // of NEW projects admit (the first triggers the sweep).
        let t1 = t0 + IDLE_EVICT_MS + 1_000;
        for i in 0..2_000 {
            r.admit(&name(MAX_TRACKED_PROJECTS + 1 + i), &quotas, t1)
                .expect("churn admit");
        }
        // churn_0 sat idle far past the horizon through the sweep, but
        // its guards are live: retention is observable as its inflight
        // count — the third admit refuses on concurrency. (Had the
        // sweep orphaned it, a FRESH entry with inflight=0 would have
        // admitted here.)
        match r.admit(&name(0), &quotas, t1) {
            Err(QuotaRefusal::Concurrency) => {}
            Ok(_) => panic!("pinned project was evicted (fresh entry admitted)"),
            Err(e) => panic!("pinned project was evicted: {e:?}"),
        }
        // An idle-evicted project simply re-tracks on return (full
        // burst — documented, an idle project lost no debt worth
        // keeping at this horizon).
        r.admit(&name(1), &quotas, t1)
            .expect("evicted project returns");
    }

    #[test]
    fn rate_bucket_admits_burst_then_refuses_then_refills() {
        let r = QuotaRegistry::default();
        let p = pid("proj_a");
        let quotas = q(2, 0);
        assert!(r.admit(&p, &quotas, 1_000).is_ok());
        assert!(r.admit(&p, &quotas, 1_000).is_ok());
        match r.admit(&p, &quotas, 1_000) {
            Err(QuotaRefusal::Rate { retry_after_secs }) => assert!(retry_after_secs >= 1),
            _ => panic!("third request in the same second must be rate-refused"),
        }
        // 500ms later: one token refilled (2/sec).
        assert!(r.admit(&p, &quotas, 1_500).is_ok());
        assert!(matches!(
            r.admit(&p, &quotas, 1_500),
            Err(QuotaRefusal::Rate { .. })
        ));
    }

    #[test]
    fn zero_rate_is_unlimited_and_projects_never_share_buckets() {
        let r = QuotaRegistry::default();
        let a = pid("proj_a");
        let b = pid("proj_b");
        for _ in 0..100 {
            assert!(r.admit(&a, &q(0, 0), 1_000).is_ok());
        }
        // Project A being hot must not consume B's tokens.
        let limited = q(1, 0);
        assert!(r.admit(&b, &limited, 1_000).is_ok());
        assert!(matches!(
            r.admit(&b, &limited, 1_000),
            Err(QuotaRefusal::Rate { .. })
        ));
        assert!(r.admit(&a, &q(0, 0), 1_000).is_ok(), "A unaffected by B");
    }

    #[test]
    fn concurrency_releases_with_the_guard() {
        let r = QuotaRegistry::default();
        let p = pid("proj_c");
        let quotas = q(0, 2);
        let g1 = r.admit(&p, &quotas, 1_000).unwrap();
        let _g2 = r.admit(&p, &quotas, 1_000).unwrap();
        assert!(matches!(
            r.admit(&p, &quotas, 1_000),
            Err(QuotaRefusal::Concurrency)
        ));
        drop(g1);
        assert!(r.admit(&p, &quotas, 1_000).is_ok());
    }

    #[test]
    fn append_volume_buckets_meter_bytes_and_records() {
        let r = QuotaRegistry::default();
        let p = pid("proj_v");
        let quotas = ProjectQuotas {
            append_bytes_per_sec: 1_000,
            append_records_per_sec: 10,
            ..Default::default()
        };
        // Track the project first (as the request-rate admit does).
        let _g = r.admit(&p, &quotas, 1_000).unwrap();
        assert!(r.admit_append(&p, &quotas, 600, 5, 1_000).is_ok());
        assert!(r.admit_append(&p, &quotas, 400, 5, 1_000).is_ok());
        // Bytes bucket dry (and records bucket dry).
        assert!(matches!(
            r.admit_append(&p, &quotas, 1, 1, 1_000),
            Err(QuotaRefusal::Rate { .. })
        ));
        // Half a second later: 500 bytes / 5 records refilled.
        assert!(r.admit_append(&p, &quotas, 500, 5, 1_500).is_ok());
        assert!(matches!(
            r.admit_append(&p, &quotas, 1, 0, 1_500),
            Err(QuotaRefusal::Rate { .. })
        ));
    }

    #[test]
    fn oversized_single_append_admits_once_then_waits() {
        let r = QuotaRegistry::default();
        let p = pid("proj_o");
        let quotas = ProjectQuotas {
            append_bytes_per_sec: 100,
            ..Default::default()
        };
        let _g = r.admit(&p, &quotas, 1_000).unwrap();
        // 5x one second's budget: admitted from a full bucket (it
        // could otherwise never succeed), driving the bucket negative.
        assert!(r.admit_append(&p, &quotas, 500, 1, 1_000).is_ok());
        // The debt is real: even a tiny append waits it out...
        match r.admit_append(&p, &quotas, 1, 0, 1_000) {
            Err(QuotaRefusal::Rate { retry_after_secs }) => {
                assert!(retry_after_secs >= 4, "debt horizon: {retry_after_secs}")
            }
            _ => panic!("bucket must be in debt"),
        }
        // ...and clears after the debt window.
        assert!(r.admit_append(&p, &quotas, 50, 0, 7_000).is_ok());
    }

    #[test]
    fn read_debit_runs_negative_and_blocks_until_refilled() {
        let r = QuotaRegistry::default();
        let p = pid("proj_r");
        let quotas = ProjectQuotas {
            read_bytes_per_sec: 100,
            ..Default::default()
        };
        let _g = r.admit(&p, &quotas, 1_000).unwrap();
        // First read passes (no debt), serves 350 bytes -> level -250.
        assert!(r.check_read(&p, &quotas, 1_000).is_ok());
        r.debit_read(&p, &quotas, 350, 1_000);
        match r.check_read(&p, &quotas, 1_000) {
            Err(QuotaRefusal::Rate { retry_after_secs }) => {
                assert!(retry_after_secs >= 2, "debt horizon: {retry_after_secs}")
            }
            _ => panic!("in-debt bucket must refuse reads"),
        }
        // 2.6s later the 250-byte debt has refilled past zero.
        assert!(r.check_read(&p, &quotas, 3_600).is_ok());
    }

    #[test]
    fn subscription_slots_release_with_the_guard() {
        let r = QuotaRegistry::default();
        let p = pid("proj_s");
        let quotas = ProjectQuotas {
            max_live_subscriptions: 1,
            ..Default::default()
        };
        let _g = r.admit(&p, &quotas, 1_000).unwrap();
        let s1 = r.admit_subscription(&p, &quotas).unwrap();
        assert!(s1.is_some());
        assert!(matches!(
            r.admit_subscription(&p, &quotas),
            Err(QuotaRefusal::Concurrency)
        ));
        drop(s1);
        assert!(r.admit_subscription(&p, &quotas).unwrap().is_some());
        // Round-13.3: unlimited (0) still COUNTS — the guard exists so
        // the subscription is visible as memory pressure; only the
        // refusal line is gone.
        let unlimited = ProjectQuotas::default();
        let g = r.admit_subscription(&p, &unlimited).unwrap();
        assert!(g.is_some(), "counting guard under an unconfigured quota");
    }

    #[test]
    fn refused_batch_charges_nothing_atomic_debit() {
        // Review item 5: bytes budget generous, records budget tiny.
        // A batch that the RECORDS bucket refuses must not burn BYTES.
        let r = QuotaRegistry::default();
        let p = pid("proj_at");
        let quotas = ProjectQuotas {
            append_bytes_per_sec: 1_000,
            append_records_per_sec: 2,
            ..Default::default()
        };
        let _g = r.admit(&p, &quotas, 1_000).unwrap();
        // Spend one record so the records bucket is NOT full (the
        // oversized-from-full rule must not apply).
        assert!(r.admit_append(&p, &quotas, 100, 1, 1_000).is_ok());
        // Refused on records (needs 2, has 1) — bytes must be
        // untouched by the refused attempt.
        assert!(matches!(
            r.admit_append(&p, &quotas, 800, 2, 1_000),
            Err(QuotaRefusal::Rate { .. })
        ));
        // Exactly the remaining byte budget still fits: had the
        // refused batch charged bytes, this would fail.
        assert!(r.admit_append(&p, &quotas, 900, 1, 1_000).is_ok());
    }

    #[test]
    fn tracker_evicts_idle_projects_never_active_ones() {
        let r = QuotaRegistry::default();
        // Fill the tracker at t=0; keep p0 ACTIVE via a held guard.
        let g0 = r.admit(&pid("p0"), &q(0, 0), 0).unwrap();
        for i in 1..MAX_TRACKED_PROJECTS {
            let _ = r.admit(&pid(&format!("p{i}")), &q(0, 0), 0).unwrap();
        }
        // Before the idle horizon: full tracker refuses the newcomer.
        assert!(matches!(
            r.admit(&pid("p_new"), &q(0, 0), IDLE_EVICT_MS - 1),
            Err(QuotaRefusal::TrackerCapacity)
        ));
        // Past the horizon: idle entries evict, the newcomer fits...
        assert!(r.admit(&pid("p_new"), &q(0, 0), IDLE_EVICT_MS + 1).is_ok());
        // ...and ONLY the project with INFLIGHT work survived the
        // sweep beside the newcomer (p_new's own guard dropped at the
        // assert, so p0's held guard is the one live slot).
        let (tracked, inflight) = r.stats();
        assert_eq!(tracked, 2, "p0 (active) + p_new: {tracked}");
        assert_eq!(inflight, 1, "p0's held guard: {inflight}");
        drop(g0);
    }

    #[test]
    fn tracker_bound_refuses_new_projects_only() {
        let r = QuotaRegistry::default();
        for i in 0..MAX_TRACKED_PROJECTS {
            assert!(r.admit(&pid(&format!("p{i}")), &q(0, 0), 1_000).is_ok());
        }
        assert!(matches!(
            r.admit(&pid("p_new"), &q(0, 0), 1_000),
            Err(QuotaRefusal::TrackerCapacity)
        ));
        // Already-tracked projects are untouched by tracker pressure.
        assert!(r.admit(&pid("p0"), &q(0, 0), 1_000).is_ok());
    }
}

#[cfg(test)]
mod pressure_tests {
    use super::*;

    fn pid(s: &str) -> ProjectId {
        ProjectId::new(s).unwrap()
    }
    fn adm(r: &QuotaRegistry, name: &str) -> Arc<ProjectAdmission> {
        let p = pid(name);
        let _ = r.admit(&p, &ProjectQuotas::default(), 1_000).unwrap();
        r.pressure_handle(&p).unwrap()
    }

    /// Battery 1: static subscription pressure alone reaches the high
    /// watermark and engages the latch (reducing append headroom).
    #[test]
    fn static_subscription_pressure_engages_the_latch() {
        let r = QuotaRegistry::default();
        let a = adm(&r, "p1");
        for _ in 0..4 {
            a.live_subs.fetch_add(1, Ordering::Relaxed);
        }
        let high = 3 * PRESSURE_SUB_WEIGHT_BYTES; // 4 subs > high
        assert!(a.memory_gate(&pid("p1"), high, 75), "engages over high");
        assert_eq!(a.memory_engage_count.load(Ordering::Relaxed), 1);
    }

    /// Battery 2: a feed is charged once per feed via its guard —
    /// never per subscriber — and releases exactly once on drop.
    #[test]
    fn feed_weight_charges_once_per_feed() {
        let r = QuotaRegistry::default();
        let a = adm(&r, "p2");
        let g1 = FeedPressureGuard::acquire(a.clone());
        assert_eq!(a.estimated_pressure_bytes(), PRESSURE_FEED_WEIGHT_BYTES);
        let g2 = FeedPressureGuard::acquire(a.clone());
        assert_eq!(a.estimated_pressure_bytes(), 2 * PRESSURE_FEED_WEIGHT_BYTES);
        drop(g1);
        drop(g2);
        assert_eq!(a.estimated_pressure_bytes(), 0);
    }

    /// Battery 3: retained SSE bytes enter the model EXACTLY once,
    /// unweighted (the budget mirrors its own reservation; the model
    /// never re-estimates it).
    #[test]
    fn retained_bytes_are_not_double_counted() {
        let r = QuotaRegistry::default();
        let a = adm(&r, "p3");
        a.retained_sse_add(100_000);
        assert_eq!(a.estimated_pressure_bytes(), 100_000);
        a.retained_sse_sub(40_000);
        assert_eq!(a.estimated_pressure_bytes(), 60_000);
        a.retained_sse_sub(1_000_000); // over-release clamps, never wraps
        assert_eq!(a.estimated_pressure_bytes(), 0);
    }

    /// Battery 4: the buffered-body guard releases on EVERY exit path
    /// (parse failure, cancellation, refusal are all drops).
    #[test]
    fn body_guard_releases_on_drop() {
        let r = QuotaRegistry::default();
        let a = adm(&r, "p4");
        let mut g = BufferedBodyGuard::reserve(a.clone(), 1_000);
        g.grow(2_000);
        assert_eq!(a.estimated_pressure_bytes(), 3_000);
        drop(g);
        assert_eq!(a.estimated_pressure_bytes(), 0);
    }

    /// Battery 5: body -> queued transfer has no transient double
    /// charge (the body guard ends before the queued charge begins).
    #[test]
    fn body_to_queued_transfer_never_double_charges() {
        let r = QuotaRegistry::default();
        let p = pid("p5");
        let a = adm(&r, "p5");
        let g = BufferedBodyGuard::reserve(a.clone(), 5_000);
        assert_eq!(a.estimated_pressure_bytes(), 5_000);
        drop(g); // the transfer point
        let quotas = ProjectQuotas {
            queued_append_bytes: 1 << 20,
            ..Default::default()
        };
        let _q = r.charge_queued(&p, &quotas, 5_000).unwrap();
        assert_eq!(
            a.estimated_pressure_bytes(),
            5_000,
            "queued only — never body+queued at once"
        );
    }

    /// Battery 6+7+8: exact frame-debt attribution — adds exact,
    /// retires exact, dirty-stream count moves ONLY on 0->pos and
    /// pos->0 edges.
    #[test]
    fn frame_debt_attribution_is_exact_with_edge_only_dirty_count() {
        let r = QuotaRegistry::default();
        let a = adm(&r, "p6");
        let b = StreamPressureBinding::bind(a.clone(), 0);
        b.frames_added(1_000);
        assert_eq!(a.unabsorbed_frame_bytes.load(Ordering::Relaxed), 1_000);
        assert_eq!(a.dirty_streams.load(Ordering::Relaxed), 1);
        b.frames_added(500); // still ONE dirty stream
        assert_eq!(a.dirty_streams.load(Ordering::Relaxed), 1);
        b.frames_retired(600); // partial: stays dirty
        assert_eq!(a.unabsorbed_frame_bytes.load(Ordering::Relaxed), 900);
        assert_eq!(a.dirty_streams.load(Ordering::Relaxed), 1);
        b.frames_retired(900); // pos -> 0
        assert_eq!(a.unabsorbed_frame_bytes.load(Ordering::Relaxed), 0);
        assert_eq!(a.dirty_streams.load(Ordering::Relaxed), 0);
        b.frames_added(10); // 0 -> pos again
        assert_eq!(a.dirty_streams.load(Ordering::Relaxed), 1);
        drop(b); // release outstanding attribution
        assert_eq!(a.unabsorbed_frame_bytes.load(Ordering::Relaxed), 0);
        assert_eq!(a.dirty_streams.load(Ordering::Relaxed), 0);
    }

    /// Battery 9 (unit leg): binding to a stream with existing durable
    /// debt seeds from the tail — never from zero — and drop releases
    /// exactly the seed plus subsequent net.
    #[test]
    fn binding_seeds_existing_durable_debt() {
        let r = QuotaRegistry::default();
        let a = adm(&r, "p9");
        let b = StreamPressureBinding::bind(a.clone(), 5_000_000);
        assert_eq!(a.unabsorbed_frame_bytes.load(Ordering::Relaxed), 5_000_000);
        assert_eq!(a.dirty_streams.load(Ordering::Relaxed), 1);
        b.frames_retired(5_000_000);
        assert_eq!(a.dirty_streams.load(Ordering::Relaxed), 0);
        drop(b);
        assert_eq!(a.unabsorbed_frame_bytes.load(Ordering::Relaxed), 0);
    }

    /// Battery 11: the latch engages at high, HOLDS between the
    /// release point and high (no flap), and releases only below
    /// high x release_pct.
    #[test]
    fn hysteresis_latch_does_not_flap() {
        let r = QuotaRegistry::default();
        let p = pid("p11");
        let a = adm(&r, "p11");
        let high = 100 * 1024;
        a.retained_sse_add(101 * 1024);
        assert!(a.memory_gate(&p, high, 75), "engage over high");
        a.retained_sse_sub(21 * 1024); // 80 KiB: between 75 KiB and high
        assert!(a.memory_gate(&p, high, 75), "held engaged in the band");
        assert!(a.memory_gate(&p, high, 75), "still engaged (no flap)");
        a.retained_sse_sub(10 * 1024); // 70 KiB < 75 KiB release point
        assert!(!a.memory_gate(&p, high, 75), "releases below the point");
        assert!(!a.memory_gate(&p, high, 75), "stays released");
        assert_eq!(a.memory_engage_count.load(Ordering::Relaxed), 1);
    }

    /// Battery 12: tracker eviction can NEVER remove a project holding
    /// pressure — orphaned attribution would leak forever.
    #[test]
    fn eviction_cannot_remove_a_project_with_pressure() {
        let r = QuotaRegistry::default();
        let old_ms = 1_000;
        // Fill the tracker with idle projects at an ancient timestamp.
        for i in 0..MAX_TRACKED_PROJECTS {
            let _ = r.admit(&pid(&format!("f{i}")), &ProjectQuotas::default(), old_ms);
        }
        // One of them holds pressure (a live feed's static charge).
        let pinned = r.pressure_handle(&pid("f7")).unwrap();
        let _feed = FeedPressureGuard::acquire(pinned);
        // A NEW project far past the idle horizon forces the eviction
        // sweep; the pressured entry must survive it.
        let now = old_ms + IDLE_EVICT_MS + 1;
        let _ = r
            .admit(&pid("fresh"), &ProjectQuotas::default(), now)
            .unwrap();
        assert!(
            r.pressure_handle(&pid("f7")).is_some(),
            "pressure pins the entry through eviction"
        );
        assert!(
            r.pressure_handle(&pid("f8")).is_none(),
            "idle peers evicted"
        );
    }

    /// Battery 13: project A engaging its latch never rejects
    /// project B (isolation is the whole point).
    #[test]
    fn engaged_project_does_not_reject_neighbors() {
        let r = QuotaRegistry::default();
        let pa = pid("pa");
        let pb = pid("pb");
        let a = adm(&r, "pa");
        let b = adm(&r, "pb");
        let high = 64 * 1024;
        a.retained_sse_add(65 * 1024);
        assert!(a.memory_gate(&pa, high, 75), "A engaged");
        assert!(!b.memory_gate(&pb, high, 75), "B unaffected");
    }

    /// Battery 14 (backstop ordering): with the per-project gate OFF
    /// (high = 0) nothing is refused here — several compliant projects
    /// reaching the cell ceiling remains the GLOBAL RSS gate's job.
    #[test]
    fn per_project_gate_off_defers_to_the_global_gate() {
        let r = QuotaRegistry::default();
        let p = pid("p14");
        let a = adm(&r, "p14");
        a.retained_sse_add(1 << 30);
        assert!(
            !a.memory_gate(&p, 0, 75),
            "0 = off; the global gate owns it"
        );
    }
}

#[cfg(test)]
mod pressure_counting_tests {
    use super::*;

    fn pid(s: &str) -> ProjectId {
        ProjectId::new(s).unwrap()
    }

    /// Round-13.3 red (field A1 finding): the pressure model's EXACT
    /// dimensions must count UNCONDITIONALLY — live subscriptions were
    /// only counted when max_live_subscriptions was configured as a
    /// refusal quota, so a default-quota noisy project showed subs=0
    /// pressure while holding 200 connections.
    #[test]
    fn live_subs_count_without_a_configured_quota() {
        let r = QuotaRegistry::default();
        let p = pid("c1");
        let _ = r.admit(&p, &ProjectQuotas::default(), 1_000).unwrap();
        let g = r.admit_subscription(&p, &ProjectQuotas::default()).unwrap();
        assert!(
            g.is_some(),
            "an unconfigured quota still returns a counting guard"
        );
        let a = r.pressure_handle(&p).unwrap();
        assert_eq!(
            a.estimated_pressure_bytes(),
            PRESSURE_SUB_WEIGHT_BYTES,
            "the subscription is pressure even with no refusal quota"
        );
        drop(g);
        assert_eq!(a.estimated_pressure_bytes(), 0);
    }

    /// Round-13.3 red (field A1 finding): queued append bytes are the
    /// standing committer-queue memory — they must charge pressure
    /// even when queued_append_bytes is not configured as a ceiling
    /// (the noisy project held ~12 MB of 10-second queue that the
    /// model could not see).
    #[test]
    fn queued_bytes_charge_without_a_configured_ceiling() {
        let r = QuotaRegistry::default();
        let p = pid("c2");
        let _ = r.admit(&p, &ProjectQuotas::default(), 1_000).unwrap();
        let g = r
            .charge_queued(&p, &ProjectQuotas::default(), 500_000)
            .unwrap();
        assert!(g.is_some(), "an unconfigured ceiling still charges");
        let a = r.pressure_handle(&p).unwrap();
        assert_eq!(a.estimated_pressure_bytes(), 500_000);
        drop(g);
        assert_eq!(a.estimated_pressure_bytes(), 0);
        // And the configured ceiling still refuses at its line.
        let q = ProjectQuotas {
            queued_append_bytes: 100,
            ..Default::default()
        };
        assert!(matches!(
            r.charge_queued(&p, &q, 500),
            Err(QuotaRefusal::QueuedBytes)
        ));
        assert_eq!(a.estimated_pressure_bytes(), 0, "refusal rolls back");
    }
}
