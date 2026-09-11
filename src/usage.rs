//! Per-stream service limits, usage telemetry, and billing emission.
//!
//! Limits (per stream shard, token buckets with LIMIT_BURST_SECS of
//! capacity): LIMIT_BYTES_PER_SEC (default 5 MB/s), LIMIT_REQS_PER_SEC
//! (default 1000), LIMIT_RECS_PER_SEC (default 5000). 0 disables a bucket.
//! Rejections are 429s whose error code names the limit that fired.
//!
//! Telemetry: cumulative per-stream counters (requests, records, bytes in,
//! bytes out, plaintext bytes, frame bytes) — the last two make stored
//! pre-compression volume and the achieved compression rate derivable at
//! any time. /v1/debug/usage exposes them; the billing task emits deltas
//! as records on one internal stream (BILLING_STREAM / BILLING_STREAM_KEY).

use crate::crypto::{RouteHash, SegmentHash};
use crate::runtime::{Clock, MonotonicNow};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
#[cfg(test)]
use std::time::Instant;

/// Bound on distinct streams tracked (same discipline as the per-stream
/// admission map). Beyond the cap the module must NEVER fail open
/// (static audit P0: limits silently stopped applying and counters
/// vanished into unregistered Arcs past 65,536 streams). Overflow
/// traffic instead shares ONE conservative bucket and ONE aggregate
/// counter set, and idle tracked entries are opportunistically evicted
/// to make room for new streams.
const MAX_TRACKED: usize = 65_536;

/// A tracked entry idle at least this long may be evicted at cap. The
/// billing emitter runs every minute, so anything idle this long has
/// had its last nonzero delta emitted many intervals ago; the emitter
/// additionally treats a shrinking cumulative counter as a reset.
const EVICT_IDLE: std::time::Duration = std::time::Duration::from_secs(600);

/// Scan for evictable entries only every Nth overflow admission — a
/// full-map scan on every hot-path admit at cap would be O(65k).
const EVICT_SCAN_EVERY: u64 = 64;

/// Evict one entry idle >= `idle` from a full map. Returns whether a
/// slot was freed. Cumulative counters for an evicted stream restart at
/// zero if it returns; the billing emitter handles that as a counter
/// reset.
fn evict_one_idle_at(
    m: &mut HashMap<[u8; 16], StreamUsage>,
    idle: std::time::Duration,
    now: MonotonicNow,
) -> bool {
    let victim = m
        .iter()
        .find(|(_, u)| now.since(u.bucket.last) >= idle)
        .map(|(h, _)| *h);
    match victim {
        Some(h) => {
            m.remove(&h);
            true
        }
        None => false,
    }
}

#[derive(Debug, Clone)]
pub(crate) struct Limits {
    pub bytes_per_sec: f64,
    pub reqs_per_sec: f64,
    pub recs_per_sec: f64,
    pub burst_secs: f64,
}

struct Bucket {
    bytes: f64,
    reqs: f64,
    recs: f64,
    last: MonotonicNow,
}

#[derive(Default)]
pub(crate) struct Counters {
    /// Process-unique incarnation id (0 for ad-hoc test constructions):
    /// the billing emitter uses this to tell two incarnations of the
    /// same stream apart. Plain counter-regression detection misses the
    /// evict → return → regrow-past-checkpoint case and silently
    /// under-bills the difference.
    pub generation: u64,
    pub requests: AtomicU64,
    pub records: AtomicU64,
    pub bytes_in: AtomicU64,
    pub bytes_out: AtomicU64,
    /// Pre-compression (plaintext) record bytes committed — cumulative
    /// stored volume before compression.
    pub plaintext_bytes: AtomicU64,
    /// Frame bytes committed (post compress+encrypt+header) — what the
    /// store actually holds; plaintext/frame = achieved compression rate.
    pub frame_bytes: AtomicU64,
}

impl Counters {
    /// A counters object with a fresh generation id.
    fn fresh() -> std::sync::Arc<Counters> {
        static NEXT_GEN: AtomicU64 = AtomicU64::new(1);
        std::sync::Arc::new(Counters {
            generation: NEXT_GEN.fetch_add(1, Ordering::Relaxed),
            ..Default::default()
        })
    }
}

struct StreamUsage {
    bucket: Bucket,
    counters: std::sync::Arc<Counters>,
}

/// Cumulative per-code rate-limit refusals (R26-7). A campaign must be
/// able to separate the ordinary per-stream limiter from maintenance
/// shedding — the 2026-08-11 soak's ~4,900 rec/s plateau was exactly
/// explained by LIMIT_RECS_PER_SEC=5,000 while the report credited the
/// maintenance gate, because nothing recorded WHICH refusal fired.
pub(crate) static LIMIT_REFUSALS_BYTES: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);
pub(crate) static LIMIT_REFUSALS_REQUESTS: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);
pub(crate) static LIMIT_REFUSALS_RECORDS: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

pub(crate) fn note_limit_refusal(hit: &LimitHit) {
    let c = match hit {
        LimitHit::Bytes { .. } => &LIMIT_REFUSALS_BYTES,
        LimitHit::Requests { .. } => &LIMIT_REFUSALS_REQUESTS,
        LimitHit::Records { .. } => &LIMIT_REFUSALS_RECORDS,
    };
    c.fetch_add(1, Ordering::Relaxed);
}

pub(crate) fn limit_refusals_json() -> serde_json::Value {
    serde_json::json!({
        "limit_bytes_per_sec": LIMIT_REFUSALS_BYTES.load(Ordering::Relaxed),
        "limit_requests_per_sec": LIMIT_REFUSALS_REQUESTS.load(Ordering::Relaxed),
        "limit_records_per_sec": LIMIT_REFUSALS_RECORDS.load(Ordering::Relaxed),
    })
}

/// Which limit an append violated, with a suggested retry delay.
pub(crate) enum LimitHit {
    Bytes { retry_ms: u64 },
    Requests { retry_ms: u64 },
    Records { retry_ms: u64 },
}

impl LimitHit {
    pub(crate) fn code(&self) -> &'static str {
        match self {
            LimitHit::Bytes { .. } => "limit_bytes_per_sec",
            LimitHit::Requests { .. } => "limit_requests_per_sec",
            LimitHit::Records { .. } => "limit_records_per_sec",
        }
    }
    pub(crate) fn message(&self, l: &Limits) -> String {
        match self {
            LimitHit::Bytes { .. } => format!(
                "stream ingest limit exceeded: {:.1} MB/s per stream shard",
                l.bytes_per_sec / 1e6
            ),
            LimitHit::Requests { .. } => format!(
                "stream request limit exceeded: {:.0} append requests/s per stream shard",
                l.reqs_per_sec
            ),
            LimitHit::Records { .. } => format!(
                "stream record limit exceeded: {:.0} records/s per stream shard",
                l.recs_per_sec
            ),
        }
    }
    pub(crate) fn retry_ms(&self) -> u64 {
        match self {
            LimitHit::Bytes { retry_ms }
            | LimitHit::Requests { retry_ms }
            | LimitHit::Records { retry_ms } => (*retry_ms).max(50),
        }
    }
}

/// Refill-and-consume against one bucket. Returns Err(the first limit
/// hit) without consuming anything when any bucket is short — the
/// request is rejected whole.
#[expect(
    clippy::cast_possible_truncation,
    reason = "admit_on; the retry delay is a non-negative millisecond count bounded by the bucket's deficit over its rate; a checked conversion would only restate the bucket arithmetic"
)]
#[expect(
    clippy::cast_sign_loss,
    reason = "admit_on; the retry delay is the ceiling of a non-negative deficit over a positive rate; a checked conversion would only restate the bucket arithmetic"
)]
fn admit_on(
    bucket: &mut Bucket,
    l: &Limits,
    bytes: u64,
    records: u64,
    now: MonotonicNow,
) -> Result<(), LimitHit> {
    let dt = now.since(bucket.last).as_secs_f64();
    bucket.last = now;
    bucket.bytes = (bucket.bytes + dt * l.bytes_per_sec).min(l.bytes_per_sec * l.burst_secs);
    bucket.reqs = (bucket.reqs + dt * l.reqs_per_sec).min(l.reqs_per_sec * l.burst_secs);
    bucket.recs = (bucket.recs + dt * l.recs_per_sec).min(l.recs_per_sec * l.burst_secs);

    let need_ms = |deficit: f64, rate: f64| -> u64 {
        if rate <= 0.0 {
            0
        } else {
            ((deficit / rate) * 1000.0).ceil() as u64
        }
    };
    if l.bytes_per_sec > 0.0 && bucket.bytes < bytes as f64 {
        return Err(LimitHit::Bytes {
            retry_ms: need_ms(bytes as f64 - bucket.bytes, l.bytes_per_sec),
        });
    }
    if l.reqs_per_sec > 0.0 && bucket.reqs < 1.0 {
        return Err(LimitHit::Requests {
            retry_ms: need_ms(1.0 - bucket.reqs, l.reqs_per_sec),
        });
    }
    if l.recs_per_sec > 0.0 && bucket.recs < records as f64 {
        return Err(LimitHit::Records {
            retry_ms: need_ms(records as f64 - bucket.recs, l.recs_per_sec),
        });
    }
    bucket.bytes -= bytes as f64;
    bucket.reqs -= 1.0;
    bucket.recs -= records as f64;
    Ok(())
}

/// One runtime's usage policy, admission state and maintenance signals.
/// Every engine and transport in that runtime shares this handle; another
/// runtime cannot consume its tokens, counters or backlog state.
pub(crate) struct UsageService {
    limits: Limits,
    clock: Arc<dyn Clock>,
    map: Mutex<HashMap<[u8; 16], StreamUsage>>,
    overflow_bucket: Mutex<Bucket>,
    overflow_counters: Arc<Counters>,
    overflow_admits: AtomicU64,
    overflow_scan_tick: AtomicU64,
    lag_map: Mutex<HashMap<SegmentHash, u64>>,
    storage_links: Mutex<HashMap<RouteHash, std::collections::HashSet<SegmentHash>>>,
    // mt-lint: allow(name-keyed-map): owned shard engine prefix -> pending work
    pending_summary: Mutex<HashMap<String, (u64, u64)>>,
    // mt-lint: allow(name-keyed-map): owned shard engine prefix -> lag
    shard_lag_map: Mutex<HashMap<String, u64>>,
}
impl std::fmt::Debug for UsageService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UsageService")
            .field("limits", &self.limits)
            .finish_non_exhaustive()
    }
}
/// One tracked stream's usage row: its hash and the seven counters the
/// operator snapshot reports for it.
type UsageRow = ([u8; 16], u64, u64, u64, u64, u64, u64, u64);

impl UsageService {
    pub(crate) fn new(cfg: &crate::config::AdmissionConfig, clock: Arc<dyn Clock>) -> Self {
        let limits = Limits {
            bytes_per_sec: cfg.limit_bytes_per_sec,
            reqs_per_sec: cfg.limit_reqs_per_sec,
            recs_per_sec: cfg.limit_recs_per_sec,
            burst_secs: cfg.limit_burst_secs,
        };
        let overflow_bucket = Mutex::new(Bucket {
            bytes: limits.bytes_per_sec * limits.burst_secs,
            reqs: limits.reqs_per_sec * limits.burst_secs,
            recs: limits.recs_per_sec * limits.burst_secs,
            last: clock.monotonic(),
        });
        Self {
            limits,
            clock,
            overflow_bucket,
            map: Mutex::new(HashMap::new()),
            overflow_counters: Counters::fresh(),
            overflow_admits: AtomicU64::new(0),
            overflow_scan_tick: AtomicU64::new(0),
            lag_map: Mutex::new(HashMap::new()),
            storage_links: Mutex::new(HashMap::new()),
            pending_summary: Mutex::new(HashMap::new()),
            shard_lag_map: Mutex::new(HashMap::new()),
        }
    }
    pub(crate) fn limits(&self) -> &Limits {
        &self.limits
    }
    pub(crate) fn overflow_stats(&self) -> (u64, u64, u64, u64) {
        let c = &self.overflow_counters;
        (
            self.overflow_admits.load(Ordering::Relaxed),
            c.requests.load(Ordering::Relaxed),
            c.records.load(Ordering::Relaxed),
            c.bytes_in.load(Ordering::Relaxed),
        )
    }

    #[expect(
        clippy::unwrap_used,
        reason = "UsageService::tracked_streams; poisoned accounting state may be inconsistent; recovery or a lock wrapper would hide the failed state transition"
    )]
    pub(crate) fn tracked_streams(&self) -> usize {
        self.map.lock().unwrap().len()
    }

    #[cfg(test)]
    pub(crate) fn evict_idle_for_test(&self, idle: std::time::Duration) -> bool {
        evict_one_idle_at(&mut self.map.lock().unwrap(), idle, self.clock.monotonic())
    }

    /// Permanent capacity check used before publishing lifecycle intent.
    pub(crate) fn permanently_unadmittable(
        &self,
        bytes: u64,
        records: u64,
    ) -> Option<&'static str> {
        let l = self.limits();
        if l.bytes_per_sec > 0.0 && bytes as f64 > l.bytes_per_sec * l.burst_secs {
            return Some("bytes");
        }
        if l.recs_per_sec > 0.0 && records as f64 > l.recs_per_sec * l.burst_secs {
            return Some("records");
        }
        // The REQUEST bucket too: rates are floats, so a configuration like
        // 0.1 req/s over a 2 s burst holds 0.2 tokens and can never admit
        // the one token every request costs. Publishing an intent against
        // that leaves the collection sealing behind a permanent 429.
        if l.reqs_per_sec > 0.0 && l.reqs_per_sec * l.burst_secs < 1.0 {
            return Some("requests");
        }
        None
    }

    /// Choose the accounting handle atomically with whole-request admission.
    /// Overflow shares one conservative bucket and one accounted counter set.
    pub(crate) fn admit_append(
        &self,
        hash: &[u8; 16],
        bytes: u64,
        records: u64,
    ) -> Result<std::sync::Arc<Counters>, LimitHit> {
        self.admit_append_in(&self.map, &self.overflow_bucket, hash, bytes, records)
    }

    #[expect(
        clippy::unwrap_used,
        reason = "UsageService::admit_append_in; poisoned accounting state may be inconsistent; recovery or a lock wrapper would hide the failed state transition"
    )]
    #[expect(
        clippy::too_many_arguments,
        reason = "UsageService::admit_append_in; admission takes the tenant, stream, size and clock parts separately as the request resolved them; a request struct would exist for this single call site"
    )]
    fn admit_append_in(
        &self,
        map: &Mutex<HashMap<[u8; 16], StreamUsage>>,
        overflow: &Mutex<Bucket>,
        hash: &[u8; 16],
        bytes: u64,
        records: u64,
    ) -> Result<std::sync::Arc<Counters>, LimitHit> {
        let l = self.limits();
        let mut m = map.lock().unwrap();
        let n = m.len();
        if !m.contains_key(hash) && n >= MAX_TRACKED {
            let tick = self.overflow_scan_tick.fetch_add(1, Ordering::Relaxed);
            let freed = tick.is_multiple_of(EVICT_SCAN_EVERY)
                && evict_one_idle_at(&mut m, EVICT_IDLE, self.clock.monotonic());
            if !freed {
                drop(m);
                admit_on(
                    &mut overflow.lock().unwrap(),
                    l,
                    bytes,
                    records,
                    self.clock.monotonic(),
                )?;
                self.overflow_admits.fetch_add(1, Ordering::Relaxed);
                return Ok(self.overflow_counters.clone());
            }
        }
        let u = m.entry(*hash).or_insert_with(|| StreamUsage {
            bucket: Bucket {
                bytes: l.bytes_per_sec * l.burst_secs,
                reqs: l.reqs_per_sec * l.burst_secs,
                recs: l.recs_per_sec * l.burst_secs,
                last: self.clock.monotonic(),
            },
            counters: Counters::fresh(),
        });
        admit_on(&mut u.bucket, l, bytes, records, self.clock.monotonic())?;
        Ok(u.counters.clone())
    }

    /// Resolve a counter without charging tokens, for read/deferred/close paths.
    #[expect(
        clippy::unwrap_used,
        reason = "UsageService::counters; poisoned accounting state may be inconsistent; recovery or a lock wrapper would hide the failed state transition"
    )]
    pub(crate) fn counters(&self, hash: &[u8; 16]) -> std::sync::Arc<Counters> {
        let l = self.limits();
        let mut m = self.map.lock().unwrap();
        let n = m.len();
        match m.get(hash) {
            Some(u) => u.counters.clone(),
            // Past the cap, all untracked streams account into ONE shared
            // aggregate — both hot-path counters() calls resolve to the same
            // Arc, so nothing vanishes into unrelated temporaries.
            None if n >= MAX_TRACKED => self.overflow_counters.clone(),
            None => m
                .entry(*hash)
                .or_insert_with(|| StreamUsage {
                    bucket: Bucket {
                        bytes: l.bytes_per_sec * l.burst_secs,
                        reqs: l.reqs_per_sec * l.burst_secs,
                        recs: l.recs_per_sec * l.burst_secs,
                        last: self.clock.monotonic(),
                    },
                    counters: Counters::fresh(),
                })
                .counters
                .clone(),
        }
    }

    #[expect(
        clippy::unwrap_used,
        reason = "UsageService::set_absorb_lag; poisoned accounting state may be inconsistent; recovery or a lock wrapper would hide the failed state transition"
    )]
    pub(crate) fn set_absorb_lag(&self, hash: SegmentHash, secs: u64) {
        self.lag_map.lock().unwrap().insert(hash, secs);
    }

    #[expect(
        clippy::unwrap_used,
        reason = "UsageService::clear_absorb_lag; poisoned accounting state may be inconsistent; recovery or a lock wrapper would hide the failed state transition"
    )]
    pub(crate) fn clear_absorb_lag(&self, hash: SegmentHash) {
        self.lag_map.lock().unwrap().remove(&hash);
    }

    /// Join tenant route identity to segment identities within this runtime.
    #[expect(
        clippy::unwrap_used,
        reason = "UsageService::link_storage; poisoned accounting state may be inconsistent; recovery or a lock wrapper would hide the failed state transition"
    )]
    pub(crate) fn link_storage(&self, usage_hash: RouteHash, storage_hash: SegmentHash) {
        let mut m = self.storage_links.lock().unwrap();
        if m.len() >= MAX_TRACKED && !m.contains_key(&usage_hash) {
            return;
        }
        m.entry(usage_hash).or_default().insert(storage_hash);
    }

    #[expect(
        clippy::unwrap_used,
        reason = "UsageService::absorb_lag_for_usage; poisoned accounting state may be inconsistent; recovery or a lock wrapper would hide the failed state transition"
    )]
    pub(crate) fn absorb_lag_for_usage(&self, usage_hash: RouteHash) -> u64 {
        let links = self.storage_links.lock().unwrap();
        let Some(set) = links.get(&usage_hash) else {
            return 0;
        };
        let lags = self.lag_map.lock().unwrap();
        set.iter()
            .filter_map(|h| lags.get(h).copied())
            .max()
            .unwrap_or(0)
    }

    #[expect(
        clippy::unwrap_used,
        reason = "UsageService::absorb_backlog_summary; poisoned accounting state may be inconsistent; recovery or a lock wrapper would hide the failed state transition"
    )]
    pub(crate) fn absorb_backlog_summary(&self) -> (usize, u64) {
        let m = self.lag_map.lock().unwrap();
        let lagging = m.values().filter(|v| **v > 0).count();
        let max = m.values().copied().max().unwrap_or(0);
        (lagging, max)
    }

    /// One row per engine; closing that engine removes its contribution.
    #[expect(
        clippy::unwrap_used,
        reason = "UsageService::set_absorb_pending_summary; poisoned accounting state may be inconsistent; recovery or a lock wrapper would hide the failed state transition"
    )]
    pub(crate) fn set_absorb_pending_summary(
        &self,
        shard_prefix: &str,
        eligible: u64,
        oldest_eligible_secs: u64,
    ) {
        self.pending_summary
            .lock()
            .unwrap()
            .insert(shard_prefix.to_string(), (eligible, oldest_eligible_secs));
    }

    #[expect(
        clippy::unwrap_used,
        reason = "UsageService::clear_absorb_pending_summary; poisoned accounting state may be inconsistent; recovery or a lock wrapper would hide the failed state transition"
    )]
    pub(crate) fn clear_absorb_pending_summary(&self, shard_prefix: &str) {
        self.pending_summary.lock().unwrap().remove(shard_prefix);
    }

    #[cfg(test)]
    pub(crate) fn absorb_pending_summary_for(&self, shard_prefix: &str) -> Option<(u64, u64)> {
        self.pending_summary
            .lock()
            .unwrap()
            .get(shard_prefix)
            .copied()
    }

    #[expect(
        clippy::unwrap_used,
        reason = "UsageService::absorb_pending_summary; poisoned accounting state may be inconsistent; recovery or a lock wrapper would hide the failed state transition"
    )]
    pub(crate) fn absorb_pending_summary(&self) -> (u64, u64) {
        self.pending_summary
            .lock()
            .unwrap()
            .values()
            .fold((0, 0), |acc, v| (acc.0 + v.0, acc.1.max(v.1)))
    }

    #[cfg(test)]
    pub(crate) fn absorb_lag(&self, hash: SegmentHash) -> u64 {
        self.lag_map
            .lock()
            .unwrap()
            .get(&hash)
            .copied()
            .unwrap_or(0)
    }

    #[expect(
        clippy::unwrap_used,
        reason = "UsageService::absorb_lag_max; poisoned accounting state may be inconsistent; recovery or a lock wrapper would hide the failed state transition"
    )]
    pub(crate) fn absorb_lag_max(&self) -> u64 {
        self.lag_map
            .lock()
            .unwrap()
            .values()
            .copied()
            .max()
            .unwrap_or(0)
    }

    #[expect(
        clippy::unwrap_used,
        reason = "UsageService::set_shard_lag; poisoned accounting state may be inconsistent; recovery or a lock wrapper would hide the failed state transition"
    )]
    pub(crate) fn set_shard_lag(&self, prefix: &str, secs: u64) {
        self.shard_lag_map
            .lock()
            .unwrap()
            .insert(prefix.to_string(), secs);
    }

    #[expect(
        clippy::unwrap_used,
        reason = "UsageService::clear_shard_lag; poisoned accounting state may be inconsistent; recovery or a lock wrapper would hide the failed state transition"
    )]
    pub(crate) fn clear_shard_lag(&self, prefix: &str) {
        self.shard_lag_map.lock().unwrap().remove(prefix);
    }

    #[expect(
        clippy::unwrap_used,
        reason = "UsageService::shard_lag_all; poisoned accounting state may be inconsistent; recovery or a lock wrapper would hide the failed state transition"
    )]
    pub(crate) fn shard_lag_all(&self) -> Vec<(String, u64)> {
        self.shard_lag_map
            .lock()
            .unwrap()
            .iter()
            .map(|(p, s)| (p.clone(), *s))
            .collect()
    }

    /// Per-stream counters from this owner; overflow is explicitly aggregated.
    #[expect(
        clippy::unwrap_used,
        reason = "UsageService::snapshot; poisoned accounting state may be inconsistent; recovery or a lock wrapper would hide the failed state transition"
    )]
    pub(crate) fn snapshot(&self) -> Vec<UsageRow> {
        self.map
            .lock()
            .unwrap()
            .iter()
            .map(|(h, u)| {
                (
                    *h,
                    u.counters.generation,
                    u.counters.requests.load(Ordering::Relaxed),
                    u.counters.records.load(Ordering::Relaxed),
                    u.counters.bytes_in.load(Ordering::Relaxed),
                    u.counters.bytes_out.load(Ordering::Relaxed),
                    u.counters.plaintext_bytes.load(Ordering::Relaxed),
                    u.counters.frame_bytes.load(Ordering::Relaxed),
                )
            })
            .collect()
    }
}

// Compatibility for direct storage fixtures only; production has no global
// usage getter and every admission/read/absorber uses the runtime handle.
#[cfg(test)]
fn test_usage() -> &'static UsageService {
    static OWNER: std::sync::OnceLock<UsageService> = std::sync::OnceLock::new();
    OWNER.get_or_init(|| {
        UsageService::new(
            &crate::config::AdmissionConfig::default(),
            Arc::new(crate::runtime::SystemClock::default()),
        )
    })
}
#[cfg(test)]
pub(crate) fn limits() -> &'static Limits {
    test_usage().limits()
}
#[cfg(test)]
fn overflow_counters() -> &'static Arc<Counters> {
    &test_usage().overflow_counters
}
#[cfg(test)]
fn evict_one_idle(m: &mut HashMap<[u8; 16], StreamUsage>, idle: std::time::Duration) -> bool {
    evict_one_idle_at(m, idle, test_usage().clock.monotonic())
}
#[cfg(test)]
pub(crate) fn overflow_stats() -> (u64, u64, u64, u64) {
    test_usage().overflow_stats()
}

#[cfg(test)]
pub(crate) fn admit_append(
    hash: &[u8; 16],
    bytes: u64,
    records: u64,
) -> Result<std::sync::Arc<Counters>, LimitHit> {
    test_usage().admit_append(hash, bytes, records)
}

#[cfg(test)]
fn admit_append_in(
    map: &Mutex<HashMap<[u8; 16], StreamUsage>>,
    overflow: &Mutex<Bucket>,
    hash: &[u8; 16],
    bytes: u64,
    records: u64,
) -> Result<std::sync::Arc<Counters>, LimitHit> {
    test_usage().admit_append_in(map, overflow, hash, bytes, records)
}

#[cfg(test)]
pub(crate) fn counters(hash: &[u8; 16]) -> std::sync::Arc<Counters> {
    test_usage().counters(hash)
}

#[cfg(test)]
pub(crate) fn set_absorb_lag(hash: SegmentHash, secs: u64) {
    test_usage().set_absorb_lag(hash, secs)
}

#[cfg(test)]
pub(crate) fn clear_absorb_lag(hash: SegmentHash) {
    test_usage().clear_absorb_lag(hash)
}

#[cfg(test)]
pub(crate) fn link_storage(usage_hash: RouteHash, storage_hash: SegmentHash) {
    test_usage().link_storage(usage_hash, storage_hash)
}

#[cfg(test)]
pub(crate) fn absorb_lag_for_usage(usage_hash: RouteHash) -> u64 {
    test_usage().absorb_lag_for_usage(usage_hash)
}

#[cfg(test)]
pub(crate) fn absorb_backlog_summary() -> (usize, u64) {
    test_usage().absorb_backlog_summary()
}

#[cfg(test)]
pub(crate) fn absorb_lag(hash: SegmentHash) -> u64 {
    test_usage().absorb_lag(hash)
}

#[cfg(test)]
pub(crate) fn absorb_lag_max() -> u64 {
    test_usage().absorb_lag_max()
}

#[cfg(test)]
pub(crate) fn set_shard_lag(prefix: &str, secs: u64) {
    test_usage().set_shard_lag(prefix, secs)
}

#[cfg(test)]
pub(crate) fn clear_shard_lag(prefix: &str) {
    test_usage().clear_shard_lag(prefix)
}

#[cfg(test)]
pub(crate) fn shard_lag_all() -> Vec<(String, u64)> {
    test_usage().shard_lag_all()
}

#[cfg(test)]
mod shard_lag_tests {
    use super::*;

    // Regression: the rebalancer used to derive a shard prefix from a
    // stream hash. Lag must be published BY the shard that owns it.
    #[test]
    fn shard_lag_roundtrips_and_clears() {
        set_shard_lag("101", 42);
        set_shard_lag("110", 7);
        let all: std::collections::HashMap<String, u64> = shard_lag_all().into_iter().collect();
        assert_eq!(all.get("101"), Some(&42));
        assert_eq!(all.get("110"), Some(&7));

        // a fenced-away shard must stop reporting, or it shows as
        // phantom lag on an instance serving nothing (ladder pass 1 D3)
        clear_shard_lag("101");
        let all: std::collections::HashMap<String, u64> = shard_lag_all().into_iter().collect();
        assert!(!all.contains_key("101"));
        assert_eq!(all.get("110"), Some(&7));
        clear_shard_lag("110");
    }

    #[test]
    fn absorb_lag_max_is_the_worst_stream() {
        // The lag map is process-global and other tests' absorbers
        // publish into it concurrently: assert with a sentinel that IS
        // the global worst (no real absorber publishes a year of lag)
        // and exact per-stream reads, not an exact global max.
        let a = SegmentHash([1u8; 16]);
        let b = SegmentHash([2u8; 16]);
        set_absorb_lag(a, 5);
        set_absorb_lag(b, 40_000_000);
        assert_eq!(absorb_lag(a), 5);
        assert_eq!(absorb_lag(b), 40_000_000);
        assert!(
            absorb_lag_max() >= 40_000_000,
            "the max must reflect the worst stream"
        );
        clear_absorb_lag(b);
        assert!(absorb_lag_max() < 40_000_000, "clearing removes the worst");
        clear_absorb_lag(a);
    }

    /// The wide tests' invisible-backlog finding: usage counters key by
    /// the NAME hash, the absorber keys lag by the ENGINE hash, and the
    /// per-stream join silently read 0 forever. The linked join must
    /// bridge the keyspaces — including per-key streams, where one name
    /// maps to several segment hashes (report the worst).
    #[test]
    fn lag_join_bridges_usage_and_engine_hashes() {
        let usage_h = RouteHash([10u8; 16]);
        let seg_a = SegmentHash([11u8; 16]);
        let seg_b = SegmentHash([12u8; 16]);
        // Unlinked: the join has nothing, even with lag present.
        set_absorb_lag(seg_a, 30);
        assert_eq!(absorb_lag_for_usage(usage_h), 0);
        link_storage(usage_h, seg_a);
        link_storage(usage_h, seg_b);
        set_absorb_lag(seg_b, 90);
        assert_eq!(
            absorb_lag_for_usage(usage_h),
            90,
            "join must report the worst linked segment"
        );
        let (lagging, max) = absorb_backlog_summary();
        assert!(lagging >= 2, "summary missed lagging streams");
        assert!(max >= 90);
        clear_absorb_lag(seg_a);
        clear_absorb_lag(seg_b);
        assert_eq!(absorb_lag_for_usage(usage_h), 0, "cleared lag must read 0");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn poisoned_usage_state_cannot_publish_new_counters() {
        use std::panic::{AssertUnwindSafe, catch_unwind};
        let usage = UsageService::new(
            &crate::config::AdmissionConfig::default(),
            Arc::new(crate::runtime::ManualClock::at(0)),
        );
        assert!(
            catch_unwind(AssertUnwindSafe(|| {
                let _guard = usage.map.lock().unwrap();
                panic!("interrupt an accounting state transition");
            }))
            .is_err()
        );
        assert!(usage.map.is_poisoned());
        assert!(catch_unwind(AssertUnwindSafe(|| usage.counters(&[1; 16]))).is_err());
    }

    #[expect(
        clippy::cast_possible_truncation,
        reason = "buckets_enforce_and_reject_whole; the fixture's cap is a small positive whole number computed from its own limits; a checked conversion would only restate the limit it sets"
    )]
    #[expect(
        clippy::cast_sign_loss,
        reason = "buckets_enforce_and_reject_whole; the fixture's cap is a small positive whole number computed from its own limits; a checked conversion would only restate the limit it sets"
    )]
    #[test]
    fn buckets_enforce_and_reject_whole() {
        let h = [1u8; 16];
        // Deterministic: a single request over the records bucket CAPACITY
        // (rate x burst) must fail regardless of refill timing...
        let cap = (limits().recs_per_sec * limits().burst_secs) as u64;
        let e = admit_append(&h, 10, cap + 1);
        assert!(matches!(e, Err(LimitHit::Records { .. })));
        if let Err(hit) = e {
            assert!(hit.retry_ms() >= 1);
            assert!(!hit.code().is_empty());
            assert!(!hit.message(limits()).is_empty());
        }
        // ...and reject-whole means nothing was consumed: a normal request
        // still passes immediately.
        assert!(admit_append(&h, 10, 1).is_ok());
    }

    #[test]
    fn byte_limit_names_itself() {
        let h = [2u8; 16];
        // one oversized request beyond 2s of byte budget
        let e = admit_append(&h, 11_000_000, 1);
        assert!(matches!(e, Err(LimitHit::Bytes { .. })));
    }

    /// Every counters incarnation carries a distinct, increasing
    /// generation — the billing emitter's evict-and-return discriminator
    /// (review round 4).
    #[test]
    fn counter_generations_are_unique_per_incarnation() {
        let a = Counters::fresh();
        let b = Counters::fresh();
        assert!(
            a.generation > 0,
            "fresh counters must have a nonzero generation"
        );
        assert!(
            b.generation > a.generation,
            "each incarnation must get a new generation"
        );
    }

    /// Static-audit P0: past MAX_TRACKED distinct streams, admission
    /// previously returned Ok(()) unconditionally (no limits) and
    /// counters() minted unrelated temporaries (lost accounting). The
    /// overflow population must stay rate-limited — through the shared
    /// conservative bucket — and its traffic must aggregate somewhere
    /// visible.
    #[expect(
        clippy::cast_possible_truncation,
        reason = "past_the_cap_limits_still_apply_and_counters_aggregate; the fixture's caps and refill are small positive whole numbers computed from its own limits; checked conversions would only restate the limits it sets"
    )]
    #[expect(
        clippy::cast_sign_loss,
        reason = "past_the_cap_limits_still_apply_and_counters_aggregate; the fixture's caps and refill are small positive whole numbers computed from its own limits; checked conversions would only restate the limits it sets"
    )]
    #[test]
    fn past_the_cap_limits_still_apply_and_counters_aggregate() {
        // PRIVATE map + overflow bucket: filling the process-global map
        // to MAX_TRACKED starved every concurrent test's new stream
        // through the drained shared bucket (2026-07-31 parallel-suite
        // 429 flake). The logic under test is identical — the
        // production entrypoint is a thin binding of the statics onto
        // admit_append_in.
        let l0 = limits();
        let tmap: Mutex<HashMap<[u8; 16], StreamUsage>> = Mutex::new(HashMap::new());
        let tover: Mutex<Bucket> = Mutex::new(Bucket {
            bytes: l0.bytes_per_sec * l0.burst_secs,
            reqs: l0.reqs_per_sec * l0.burst_secs,
            recs: l0.recs_per_sec * l0.burst_secs,
            last: test_usage().clock.monotonic(),
        });
        // Fill the map to the cap with distinct hashes.
        {
            let l = limits();
            let mut m = tmap.lock().unwrap();
            let mut h = [0u8; 16];
            while m.len() < MAX_TRACKED {
                let i = m.len() as u64;
                h[..8].copy_from_slice(&i.to_le_bytes());
                h[8] = 0xCA;
                m.entry(h).or_insert_with(|| StreamUsage {
                    bucket: Bucket {
                        bytes: l.bytes_per_sec * l.burst_secs,
                        reqs: l.reqs_per_sec * l.burst_secs,
                        recs: l.recs_per_sec * l.burst_secs,
                        last: test_usage().clock.monotonic(),
                    },
                    counters: Default::default(),
                });
            }
        }
        assert!(tmap.lock().unwrap().len() >= MAX_TRACKED);

        // 5,000 distinct NEW streams past the cap: every admit routes
        // through ONE shared bucket, so the aggregate population cannot
        // exceed one stream's allowance — limits fire long before 5,000
        // request tokens exist (burst is reqs_per_sec x burst_secs).
        let (admits_before, _, _, _) = overflow_stats();
        let mut admitted = 0u64;
        let mut limited = 0u64;
        let loop_started = Instant::now();
        for i in 0..5_000u64 {
            let mut h = [0u8; 16];
            h[..8].copy_from_slice(&i.to_le_bytes());
            h[8] = 0xFE;
            match admit_append_in(&tmap, &tover, &h, 100, 1) {
                Ok(c) => {
                    admitted += 1;
                    // Account into the Arc ADMISSION returned — the same
                    // object the append path carries end-to-end (review
                    // round 4: choosing it atomically with admission is
                    // what stops a concurrent promotion from splitting
                    // one request's accounting across two objects).
                    assert!(
                        std::sync::Arc::ptr_eq(&c, overflow_counters()),
                        "past-cap admission must hand back the shared overflow aggregate"
                    );
                    c.requests.fetch_add(1, Ordering::Relaxed);
                    c.records.fetch_add(1, Ordering::Relaxed);
                    c.bytes_in.fetch_add(100, Ordering::Relaxed);
                }
                Err(_) => limited += 1,
            }
        }
        assert!(
            limited > 0,
            "overflow admission must be rate-limited, got {admitted} unlimited admits"
        );
        let l = limits();
        // One stream's allowance = its burst plus whatever refilled while
        // the loop itself ran.
        let refill = (loop_started.elapsed().as_secs_f64() * l.reqs_per_sec).ceil() as u64;
        let burst = (l.reqs_per_sec * l.burst_secs).ceil() as u64 + refill + 8;
        assert!(
            admitted <= burst,
            "overflow population exceeded one stream's allowance: {admitted} > {burst}"
        );

        // Aggregate accounting: everything admitted is visible in the
        // shared overflow counters, nothing vanished.
        let (admits_after, req_total, rec_total, bytes_total) = overflow_stats();
        assert_eq!(admits_after - admits_before, admitted);
        assert!(req_total >= admitted);
        assert!(rec_total >= admitted);
        assert!(bytes_total >= admitted * 100);

        // Both hot-path counters() calls for an overflow stream resolve
        // to the same Arc.
        let h = [0xEE; 16];
        let a = counters(&h);
        let b = counters(&h);
        assert!(
            std::sync::Arc::ptr_eq(&a, &b),
            "overflow counters must be shared"
        );

        // Idle entries are evictable to make room again.
        assert!(
            evict_one_idle(&mut tmap.lock().unwrap(), std::time::Duration::ZERO),
            "an idle tracked entry must be evictable at cap"
        );
    }
}

#[cfg(test)]
#[path = "usage/runtime_tests.rs"]
mod runtime_tests;
