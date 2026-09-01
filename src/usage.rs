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
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, OnceLock};
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

/// Shared token bucket for ALL streams past the cap: conservative by
/// construction (the whole overflow population shares one stream's
/// allowance) and visible, never silently unlimited.
fn overflow_bucket() -> &'static Mutex<Bucket> {
    static B: OnceLock<Mutex<Bucket>> = OnceLock::new();
    B.get_or_init(|| {
        let l = limits();
        Mutex::new(Bucket {
            bytes: l.bytes_per_sec * l.burst_secs,
            reqs: l.reqs_per_sec * l.burst_secs,
            recs: l.recs_per_sec * l.burst_secs,
            last: Instant::now(),
        })
    })
}

/// Aggregate counters for every stream past the cap. Both counters()
/// call sites on the append path resolve to this same Arc, so overflow
/// traffic is accounted (in aggregate) instead of written into
/// unrelated temporaries and dropped.
fn overflow_counters() -> &'static std::sync::Arc<Counters> {
    static C: OnceLock<std::sync::Arc<Counters>> = OnceLock::new();
    C.get_or_init(Counters::fresh)
}

static OVERFLOW_ADMITS: AtomicU64 = AtomicU64::new(0);
static OVERFLOW_SCAN_TICK: AtomicU64 = AtomicU64::new(0);

/// (admissions routed through the shared overflow bucket, aggregate
/// overflow counter totals) — the visibility half of never-fail-open.
pub fn overflow_stats() -> (u64, u64, u64, u64) {
    let c = overflow_counters();
    (
        OVERFLOW_ADMITS.load(Ordering::Relaxed),
        c.requests.load(Ordering::Relaxed),
        c.records.load(Ordering::Relaxed),
        c.bytes_in.load(Ordering::Relaxed),
    )
}

pub fn tracked_streams() -> usize {
    map().lock().unwrap().len()
}

/// Evict one entry idle >= `idle` from a full map. Returns whether a
/// slot was freed. Cumulative counters for an evicted stream restart at
/// zero if it returns; the billing emitter handles that as a counter
/// reset.
fn evict_one_idle(m: &mut HashMap<[u8; 16], StreamUsage>, idle: std::time::Duration) -> bool {
    let now = Instant::now();
    let victim = m
        .iter()
        .find(|(_, u)| now.duration_since(u.bucket.last) >= idle)
        .map(|(h, _)| *h);
    match victim {
        Some(h) => {
            m.remove(&h);
            true
        }
        None => false,
    }
}

#[cfg(test)]
pub(crate) fn evict_idle_for_test(idle: std::time::Duration) -> bool {
    evict_one_idle(&mut map().lock().unwrap(), idle)
}

/// Whether a request this size can EVER be admitted — capacity, not
/// current tokens. Waiting cannot fix a body larger than the bucket
/// itself, so this is a permanent 413 rather than a 429, and callers
/// that publish durable intent must consult it BEFORE promising
/// anything.
///
/// A rate of zero means the limit is disabled, exactly as the admission
/// path reads it; treating zero as "capacity zero" would reject every
/// non-empty record on a deployment that turned the limiter off.
pub fn permanently_unadmittable(bytes: u64, records: u64) -> Option<&'static str> {
    let l = limits();
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

pub struct Limits {
    pub bytes_per_sec: f64,
    pub reqs_per_sec: f64,
    pub recs_per_sec: f64,
    pub burst_secs: f64,
}

pub fn limits() -> &'static Limits {
    static L: OnceLock<Limits> = OnceLock::new();
    L.get_or_init(|| {
        let cfg = crate::config::current();
        Limits {
            bytes_per_sec: cfg.admission.limit_bytes_per_sec,
            reqs_per_sec: cfg.admission.limit_reqs_per_sec,
            recs_per_sec: cfg.admission.limit_recs_per_sec,
            burst_secs: cfg.admission.limit_burst_secs,
        }
    })
}

struct Bucket {
    bytes: f64,
    reqs: f64,
    recs: f64,
    last: Instant,
}

#[derive(Default)]
pub struct Counters {
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

fn map() -> &'static Mutex<HashMap<[u8; 16], StreamUsage>> {
    static M: OnceLock<Mutex<HashMap<[u8; 16], StreamUsage>>> = OnceLock::new();
    M.get_or_init(|| Mutex::new(HashMap::new()))
}

/// Cumulative per-code rate-limit refusals (R26-7). A campaign must be
/// able to separate the ordinary per-stream limiter from maintenance
/// shedding — the 2026-08-11 soak's ~4,900 rec/s plateau was exactly
/// explained by LIMIT_RECS_PER_SEC=5,000 while the report credited the
/// maintenance gate, because nothing recorded WHICH refusal fired.
pub static LIMIT_REFUSALS_BYTES: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);
pub static LIMIT_REFUSALS_REQUESTS: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);
pub static LIMIT_REFUSALS_RECORDS: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

pub fn note_limit_refusal(hit: &LimitHit) {
    let c = match hit {
        LimitHit::Bytes { .. } => &LIMIT_REFUSALS_BYTES,
        LimitHit::Requests { .. } => &LIMIT_REFUSALS_REQUESTS,
        LimitHit::Records { .. } => &LIMIT_REFUSALS_RECORDS,
    };
    c.fetch_add(1, Ordering::Relaxed);
}

pub fn limit_refusals_json() -> serde_json::Value {
    serde_json::json!({
        "limit_bytes_per_sec": LIMIT_REFUSALS_BYTES.load(Ordering::Relaxed),
        "limit_requests_per_sec": LIMIT_REFUSALS_REQUESTS.load(Ordering::Relaxed),
        "limit_records_per_sec": LIMIT_REFUSALS_RECORDS.load(Ordering::Relaxed),
    })
}

/// Which limit an append violated, with a suggested retry delay.
pub enum LimitHit {
    Bytes { retry_ms: u64 },
    Requests { retry_ms: u64 },
    Records { retry_ms: u64 },
}

impl LimitHit {
    pub fn code(&self) -> &'static str {
        match self {
            LimitHit::Bytes { .. } => "limit_bytes_per_sec",
            LimitHit::Requests { .. } => "limit_requests_per_sec",
            LimitHit::Records { .. } => "limit_records_per_sec",
        }
    }
    pub fn message(&self) -> String {
        let l = limits();
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
    pub fn retry_ms(&self) -> u64 {
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
fn admit_on(bucket: &mut Bucket, l: &Limits, bytes: u64, records: u64) -> Result<(), LimitHit> {
    let now = Instant::now();
    let dt = now.duration_since(bucket.last).as_secs_f64();
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

/// Admission for one append request: `bytes` of body carrying `records`
/// records. Past MAX_TRACKED distinct streams this NEVER fails open:
/// an idle tracked entry is evicted (amortized scan) to make room, and
/// otherwise the request is admitted through the shared conservative
/// overflow bucket.
///
/// On success returns the counters object the request must account
/// into — chosen ATOMICALLY with the admission decision. The caller
/// carries this one Arc through both count sites (request-side and
/// committed-side); re-resolving by hash mid-request used to race a
/// concurrent eviction/promotion and split one request's accounting
/// across the overflow aggregate and a fresh tracked entry (review
/// round 4).
pub fn admit_append(
    hash: &[u8; 16],
    bytes: u64,
    records: u64,
) -> Result<std::sync::Arc<Counters>, LimitHit> {
    admit_append_in(map(), overflow_bucket(), hash, bytes, records)
}

/// The admission core, parametric over the tracked map and the shared
/// overflow bucket. Production calls bind the process statics; the
/// past-the-cap test binds PRIVATE instances so filling a map to
/// MAX_TRACKED cannot starve concurrent tests' streams through the
/// real shared bucket (2026-07-31 parallel-suite 429 flake).
pub(crate) fn admit_append_in(
    map: &Mutex<HashMap<[u8; 16], StreamUsage>>,
    overflow: &Mutex<Bucket>,
    hash: &[u8; 16],
    bytes: u64,
    records: u64,
) -> Result<std::sync::Arc<Counters>, LimitHit> {
    let l = limits();
    let mut m = map.lock().unwrap();
    let n = m.len();
    if !m.contains_key(hash) && n >= MAX_TRACKED {
        let tick = OVERFLOW_SCAN_TICK.fetch_add(1, Ordering::Relaxed);
        let freed = tick.is_multiple_of(EVICT_SCAN_EVERY) && evict_one_idle(&mut m, EVICT_IDLE);
        if !freed {
            drop(m);
            admit_on(&mut overflow.lock().unwrap(), l, bytes, records)?;
            OVERFLOW_ADMITS.fetch_add(1, Ordering::Relaxed);
            return Ok(overflow_counters().clone());
        }
    }
    let u = m.entry(*hash).or_insert_with(|| StreamUsage {
        bucket: Bucket {
            bytes: l.bytes_per_sec * l.burst_secs,
            reqs: l.reqs_per_sec * l.burst_secs,
            recs: l.recs_per_sec * l.burst_secs,
            last: Instant::now(),
        },
        counters: Counters::fresh(),
    });
    admit_on(&mut u.bucket, l, bytes, records)?;
    Ok(u.counters.clone())
}

/// Counters handle for a stream (shared Arc; cheap to hold on hot paths).
pub fn counters(hash: &[u8; 16]) -> std::sync::Arc<Counters> {
    let l = limits();
    let mut m = map().lock().unwrap();
    let n = m.len();
    match m.get(hash) {
        Some(u) => u.counters.clone(),
        // Past the cap, all untracked streams account into ONE shared
        // aggregate — both hot-path counters() calls resolve to the same
        // Arc, so nothing vanishes into unrelated temporaries.
        None if n >= MAX_TRACKED => overflow_counters().clone(),
        None => m
            .entry(*hash)
            .or_insert_with(|| StreamUsage {
                bucket: Bucket {
                    bytes: l.bytes_per_sec * l.burst_secs,
                    reqs: l.reqs_per_sec * l.burst_secs,
                    recs: l.recs_per_sec * l.burst_secs,
                    last: Instant::now(),
                },
                counters: Counters::fresh(),
            })
            .counters
            .clone(),
    }
}

/// Absorption lag gauge (seconds behind), fed by the absorber's tick:
/// per-stream age of the oldest unabsorbed bytes. THE scale-out signal
/// (rebalance shards off a host when this exceeds ~60 s).
fn lag_map() -> &'static Mutex<HashMap<SegmentHash, u64>> {
    static M: OnceLock<Mutex<HashMap<SegmentHash, u64>>> = OnceLock::new();
    M.get_or_init(|| Mutex::new(HashMap::new()))
}

pub fn set_absorb_lag(hash: SegmentHash, secs: u64) {
    lag_map().lock().unwrap().insert(hash, secs);
}

pub fn clear_absorb_lag(hash: SegmentHash) {
    lag_map().lock().unwrap().remove(&hash);
}

/// Usage counters are keyed by the NAME hash (`stream_hash(&desc.name)`,
/// the shard-routing key), while the absorber publishes lag under the
/// ENGINE hash (storage/segment hash). The /v1/debug/usage join used to
/// look lag up by the name hash and therefore always read 0 — the wide
/// tests' "absorb lag is invisible" finding (docs/COST-WIDE2.md §4).
/// This alias map, fed by the append path where both hashes are in hand,
/// closes the join; RouteHash/SegmentHash make the two key spaces
/// uncrossable at compile time. Per-key streams link one usage entry to
/// many segment hashes; the join takes the max.
fn storage_links() -> &'static Mutex<HashMap<RouteHash, std::collections::HashSet<SegmentHash>>> {
    static M: OnceLock<Mutex<HashMap<RouteHash, std::collections::HashSet<SegmentHash>>>> =
        OnceLock::new();
    M.get_or_init(|| Mutex::new(HashMap::new()))
}

pub fn link_storage(usage_hash: RouteHash, storage_hash: SegmentHash) {
    let mut m = storage_links().lock().unwrap();
    if m.len() >= MAX_TRACKED && !m.contains_key(&usage_hash) {
        return;
    }
    m.entry(usage_hash).or_default().insert(storage_hash);
}

/// Absorb lag for a usage entry: the max across its linked engine
/// hashes (a per-key stream has one per touched segment).
pub fn absorb_lag_for_usage(usage_hash: RouteHash) -> u64 {
    let links = storage_links().lock().unwrap();
    let Some(set) = links.get(&usage_hash) else {
        return 0;
    };
    let lags = lag_map().lock().unwrap();
    set.iter()
        .filter_map(|h| lags.get(h).copied())
        .max()
        .unwrap_or(0)
}

/// Aggregate backlog view, independent of per-stream listing caps:
/// (streams with nonzero lag, max lag secs). Complements the
/// per-instance `absorb_lag_max` the heartbeat already carries.
pub fn absorb_backlog_summary() -> (usize, u64) {
    let m = lag_map().lock().unwrap();
    let lagging = m.values().filter(|v| **v > 0).count();
    let max = m.values().copied().max().unwrap_or(0);
    (lagging, max)
}

/// Absorber pending-set summary, published each absorber tick PER
/// SHARD (one absorber per shard engine — a single global gauge would
/// be last-writer-wins across shards and report one shard's quarter of
/// the truth, which is exactly how the first version shipped):
/// (eligible streams, oldest eligible age secs). Every pending stream
/// is eligible — the interim sparse-deferral policy was deleted in
/// R26-1 because an ineligible residual stalls the durable no-progress
/// clock into the instance-wide maintenance latch.
// mt-lint: allow(name-keyed-map): shard prefix -> pending-absorb summary
static PENDING_SUMMARY: OnceLock<Mutex<HashMap<String, (u64, u64)>>> = OnceLock::new();

pub fn set_absorb_pending_summary(shard_prefix: &str, eligible: u64, oldest_eligible_secs: u64) {
    PENDING_SUMMARY
        .get_or_init(|| Mutex::new(HashMap::new()))
        .lock()
        .unwrap()
        .insert(shard_prefix.to_string(), (eligible, oldest_eligible_secs));
}

/// Remove one shard's pending-summary row. MUST run when an absorber
/// exits (shard fenced/moved/evicted): the frozen row would otherwise
/// double-count against the new owner's live row and the instance
/// rollup reports phantom backlog forever (review round 4 — the fleet
/// campaign reads that rollup as its drain proof).
pub fn clear_absorb_pending_summary(shard_prefix: &str) {
    PENDING_SUMMARY
        .get_or_init(|| Mutex::new(HashMap::new()))
        .lock()
        .unwrap()
        .remove(shard_prefix);
}

/// One shard's pending-summary row (None once cleared/never published).
pub fn absorb_pending_summary_for(shard_prefix: &str) -> Option<(u64, u64)> {
    PENDING_SUMMARY
        .get_or_init(|| Mutex::new(HashMap::new()))
        .lock()
        .unwrap()
        .get(shard_prefix)
        .copied()
}

/// Instance-wide rollup: sums across shards, max for the oldest age.
pub fn absorb_pending_summary() -> (u64, u64) {
    PENDING_SUMMARY
        .get_or_init(|| Mutex::new(HashMap::new()))
        .lock()
        .unwrap()
        .values()
        .fold((0, 0), |acc, v| (acc.0 + v.0, acc.1.max(v.1)))
}

pub fn absorb_lag(hash: SegmentHash) -> u64 {
    lag_map().lock().unwrap().get(&hash).copied().unwrap_or(0)
}

pub fn absorb_lag_max() -> u64 {
    lag_map()
        .lock()
        .unwrap()
        .values()
        .copied()
        .max()
        .unwrap_or(0)
}

/// Per-SHARD absorb lag, published by each shard's absorber (which knows
/// its own prefix). The rebalancer must not re-derive a shard from a
/// stream hash: records are keyed by storage_hash while the shard is
/// chosen by stream_hash(name), so that mapping is simply wrong (ladder
/// p6b D3: victim selection never matched, no move ever fired).
fn shard_lag_map() -> &'static Mutex<HashMap<String, u64>> {
    // mt-lint: allow(name-keyed-map): shard prefix -> absorb lag
    static M: OnceLock<Mutex<HashMap<String, u64>>> = OnceLock::new();
    M.get_or_init(|| Mutex::new(HashMap::new()))
}

pub fn set_shard_lag(prefix: &str, secs: u64) {
    shard_lag_map()
        .lock()
        .unwrap()
        .insert(prefix.to_string(), secs);
}

pub fn clear_shard_lag(prefix: &str) {
    shard_lag_map().lock().unwrap().remove(prefix);
}

/// (shard_prefix, lag_secs) for every shard with unabsorbed bytes.
pub fn shard_lag_all() -> Vec<(String, u64)> {
    shard_lag_map()
        .lock()
        .unwrap()
        .iter()
        .map(|(p, s)| (p.clone(), *s))
        .collect()
}

/// Every stream with unabsorbed bytes and its lag — the rebalancer maps
/// these to shard prefixes to choose which shard to move off a laggard.
pub fn absorb_lag_all() -> Vec<(SegmentHash, u64)> {
    lag_map()
        .lock()
        .unwrap()
        .iter()
        .map(|(h, s)| (*h, *s))
        .collect()
}

/// Snapshot every stream's cumulative counters (for /v1/debug/usage and
/// the billing emitter). The second field is the counters object's
/// generation id — the emitter uses it to detect evict-and-return
/// incarnations. NOTE (accepted best-effort posture): the shared
/// overflow aggregate is deliberately NOT a row here — it has no
/// per-stream attribution to bill against; it is visible via
/// overflow_stats() / /v1/debug/usage instead.
pub fn snapshot() -> Vec<([u8; 16], u64, u64, u64, u64, u64, u64, u64)> {
    map()
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
            assert!(!hit.message().is_empty());
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
            last: Instant::now(),
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
                        last: Instant::now(),
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
