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

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, OnceLock};
use std::time::Instant;

/// Bound on distinct streams tracked (same discipline as the per-stream
/// admission map): beyond this, new streams are unlimited/untracked
/// rather than evicting hot entries.
const MAX_TRACKED: usize = 65_536;

pub struct Limits {
    pub bytes_per_sec: f64,
    pub reqs_per_sec: f64,
    pub recs_per_sec: f64,
    pub burst_secs: f64,
}

fn envf(k: &str, d: f64) -> f64 {
    std::env::var(k)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(d)
}

pub fn limits() -> &'static Limits {
    static L: OnceLock<Limits> = OnceLock::new();
    L.get_or_init(|| Limits {
        bytes_per_sec: envf("LIMIT_BYTES_PER_SEC", 5_000_000.0),
        reqs_per_sec: envf("LIMIT_REQS_PER_SEC", 1_000.0),
        recs_per_sec: envf("LIMIT_RECS_PER_SEC", 5_000.0),
        burst_secs: envf("LIMIT_BURST_SECS", 2.0),
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

struct StreamUsage {
    bucket: Bucket,
    counters: std::sync::Arc<Counters>,
}

fn map() -> &'static Mutex<HashMap<[u8; 16], StreamUsage>> {
    static M: OnceLock<Mutex<HashMap<[u8; 16], StreamUsage>>> = OnceLock::new();
    M.get_or_init(|| Mutex::new(HashMap::new()))
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

/// Refill-and-consume for one append request: `bytes` of body carrying
/// `records` records. Returns Err(the first limit hit) without consuming
/// anything when any bucket is short — the request is rejected whole.
pub fn admit_append(hash: &[u8; 16], bytes: u64, records: u64) -> Result<(), LimitHit> {
    let l = limits();
    let mut m = map().lock().unwrap();
    let n = m.len();
    let u = match m.get_mut(hash) {
        Some(u) => u,
        None => {
            if n >= MAX_TRACKED {
                return Ok(());
            }
            m.entry(*hash).or_insert_with(|| StreamUsage {
                bucket: Bucket {
                    bytes: l.bytes_per_sec * l.burst_secs,
                    reqs: l.reqs_per_sec * l.burst_secs,
                    recs: l.recs_per_sec * l.burst_secs,
                    last: Instant::now(),
                },
                counters: Default::default(),
            })
        }
    };
    let now = Instant::now();
    let dt = now.duration_since(u.bucket.last).as_secs_f64();
    u.bucket.last = now;
    u.bucket.bytes = (u.bucket.bytes + dt * l.bytes_per_sec).min(l.bytes_per_sec * l.burst_secs);
    u.bucket.reqs = (u.bucket.reqs + dt * l.reqs_per_sec).min(l.reqs_per_sec * l.burst_secs);
    u.bucket.recs = (u.bucket.recs + dt * l.recs_per_sec).min(l.recs_per_sec * l.burst_secs);

    let need_ms = |deficit: f64, rate: f64| -> u64 {
        if rate <= 0.0 {
            0
        } else {
            ((deficit / rate) * 1000.0).ceil() as u64
        }
    };
    if l.bytes_per_sec > 0.0 && u.bucket.bytes < bytes as f64 {
        return Err(LimitHit::Bytes {
            retry_ms: need_ms(bytes as f64 - u.bucket.bytes, l.bytes_per_sec),
        });
    }
    if l.reqs_per_sec > 0.0 && u.bucket.reqs < 1.0 {
        return Err(LimitHit::Requests {
            retry_ms: need_ms(1.0 - u.bucket.reqs, l.reqs_per_sec),
        });
    }
    if l.recs_per_sec > 0.0 && u.bucket.recs < records as f64 {
        return Err(LimitHit::Records {
            retry_ms: need_ms(records as f64 - u.bucket.recs, l.recs_per_sec),
        });
    }
    u.bucket.bytes -= bytes as f64;
    u.bucket.reqs -= 1.0;
    u.bucket.recs -= records as f64;
    Ok(())
}

/// Counters handle for a stream (shared Arc; cheap to hold on hot paths).
pub fn counters(hash: &[u8; 16]) -> std::sync::Arc<Counters> {
    let l = limits();
    let mut m = map().lock().unwrap();
    let n = m.len();
    match m.get(hash) {
        Some(u) => u.counters.clone(),
        None if n >= MAX_TRACKED => Default::default(),
        None => m
            .entry(*hash)
            .or_insert_with(|| StreamUsage {
                bucket: Bucket {
                    bytes: l.bytes_per_sec * l.burst_secs,
                    reqs: l.reqs_per_sec * l.burst_secs,
                    recs: l.recs_per_sec * l.burst_secs,
                    last: Instant::now(),
                },
                counters: Default::default(),
            })
            .counters
            .clone(),
    }
}

/// Absorption lag gauge (seconds behind), fed by the absorber's tick:
/// per-stream age of the oldest unabsorbed bytes. THE scale-out signal
/// (rebalance shards off a host when this exceeds ~60 s).
fn lag_map() -> &'static Mutex<HashMap<[u8; 16], u64>> {
    static M: OnceLock<Mutex<HashMap<[u8; 16], u64>>> = OnceLock::new();
    M.get_or_init(|| Mutex::new(HashMap::new()))
}

pub fn set_absorb_lag(hash: &[u8; 16], secs: u64) {
    lag_map().lock().unwrap().insert(*hash, secs);
}

pub fn clear_absorb_lag(hash: &[u8; 16]) {
    lag_map().lock().unwrap().remove(hash);
}

/// Usage counters are keyed by the NAME hash (`stream_hash(&desc.name)`,
/// the shard-routing key), while the absorber publishes lag under the
/// ENGINE hash (storage/segment hash). The /v1/debug/usage join used to
/// look lag up by the name hash and therefore always read 0 — the wide
/// tests' "absorb lag is invisible" finding (docs/COST-WIDE2.md §4).
/// This alias map, fed by the append path where both hashes are in hand,
/// closes the join. Per-key streams link one usage entry to many
/// segment hashes; the join takes the max.
fn storage_links() -> &'static Mutex<HashMap<[u8; 16], std::collections::HashSet<[u8; 16]>>> {
    static M: OnceLock<Mutex<HashMap<[u8; 16], std::collections::HashSet<[u8; 16]>>>> =
        OnceLock::new();
    M.get_or_init(|| Mutex::new(HashMap::new()))
}

pub fn link_storage(usage_hash: &[u8; 16], storage_hash: &[u8; 16]) {
    let mut m = storage_links().lock().unwrap();
    if m.len() >= MAX_TRACKED && !m.contains_key(usage_hash) {
        return;
    }
    m.entry(*usage_hash).or_default().insert(*storage_hash);
}

/// Absorb lag for a usage entry: the max across its linked engine
/// hashes (a per-key stream has one per touched segment).
pub fn absorb_lag_for_usage(usage_hash: &[u8; 16]) -> u64 {
    let links = storage_links().lock().unwrap();
    let Some(set) = links.get(usage_hash) else {
        return 0;
    };
    let lags = lag_map().lock().unwrap();
    set.iter().filter_map(|h| lags.get(h).copied()).max().unwrap_or(0)
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
/// (eligible streams, oldest eligible age secs, policy-deferred sparse
/// streams, their pending bytes). "Deferred" is the interim sparse
/// policy (age absorption requires min_age_bytes) — an intentional
/// cost decision that must never read as absorption lag.
static PENDING_SUMMARY: OnceLock<Mutex<HashMap<String, (u64, u64, u64, u64)>>> = OnceLock::new();

pub fn set_absorb_pending_summary(
    shard_prefix: &str,
    eligible: u64,
    oldest_eligible_secs: u64,
    deferred: u64,
    deferred_bytes: u64,
) {
    PENDING_SUMMARY
        .get_or_init(|| Mutex::new(HashMap::new()))
        .lock()
        .unwrap()
        .insert(
            shard_prefix.to_string(),
            (eligible, oldest_eligible_secs, deferred, deferred_bytes),
        );
}

/// Instance-wide rollup: sums across shards, max for the oldest age.
pub fn absorb_pending_summary() -> (u64, u64, u64, u64) {
    PENDING_SUMMARY
        .get_or_init(|| Mutex::new(HashMap::new()))
        .lock()
        .unwrap()
        .values()
        .fold((0, 0, 0, 0), |acc, v| {
            (acc.0 + v.0, acc.1.max(v.1), acc.2 + v.2, acc.3 + v.3)
        })
}

pub fn absorb_lag(hash: &[u8; 16]) -> u64 {
    lag_map().lock().unwrap().get(hash).copied().unwrap_or(0)
}

pub fn absorb_lag_max() -> u64 {
    lag_map().lock().unwrap().values().copied().max().unwrap_or(0)
}

/// Per-SHARD absorb lag, published by each shard's absorber (which knows
/// its own prefix). The rebalancer must not re-derive a shard from a
/// stream hash: records are keyed by storage_hash while the shard is
/// chosen by stream_hash(name), so that mapping is simply wrong (ladder
/// p6b D3: victim selection never matched, no move ever fired).
fn shard_lag_map() -> &'static Mutex<HashMap<String, u64>> {
    static M: OnceLock<Mutex<HashMap<String, u64>>> = OnceLock::new();
    M.get_or_init(|| Mutex::new(HashMap::new()))
}

pub fn set_shard_lag(prefix: &str, secs: u64) {
    shard_lag_map().lock().unwrap().insert(prefix.to_string(), secs);
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
pub fn absorb_lag_all() -> Vec<([u8; 16], u64)> {
    lag_map()
        .lock()
        .unwrap()
        .iter()
        .map(|(h, s)| (*h, *s))
        .collect()
}

/// Snapshot every stream's cumulative counters (for /v1/debug/usage and
/// the billing emitter).
pub fn snapshot() -> Vec<([u8; 16], u64, u64, u64, u64, u64, u64)> {
    map()
        .lock()
        .unwrap()
        .iter()
        .map(|(h, u)| {
            (
                *h,
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
        let all: std::collections::HashMap<String, u64> =
            shard_lag_all().into_iter().collect();
        assert_eq!(all.get("101"), Some(&42));
        assert_eq!(all.get("110"), Some(&7));

        // a fenced-away shard must stop reporting, or it shows as
        // phantom lag on an instance serving nothing (ladder pass 1 D3)
        clear_shard_lag("101");
        let all: std::collections::HashMap<String, u64> =
            shard_lag_all().into_iter().collect();
        assert!(!all.contains_key("101"));
        assert_eq!(all.get("110"), Some(&7));
        clear_shard_lag("110");
    }

    #[test]
    fn absorb_lag_max_is_the_worst_stream() {
        let a = [1u8; 16];
        let b = [2u8; 16];
        set_absorb_lag(&a, 5);
        set_absorb_lag(&b, 61);
        assert_eq!(absorb_lag_max(), 61);
        clear_absorb_lag(&a);
        clear_absorb_lag(&b);
    }

    /// The wide tests' invisible-backlog finding: usage counters key by
    /// the NAME hash, the absorber keys lag by the ENGINE hash, and the
    /// per-stream join silently read 0 forever. The linked join must
    /// bridge the keyspaces — including per-key streams, where one name
    /// maps to several segment hashes (report the worst).
    #[test]
    fn lag_join_bridges_usage_and_engine_hashes() {
        let usage_h = [10u8; 16];
        let seg_a = [11u8; 16];
        let seg_b = [12u8; 16];
        // Unlinked: the join has nothing, even with lag present.
        set_absorb_lag(&seg_a, 30);
        assert_eq!(absorb_lag_for_usage(&usage_h), 0);
        link_storage(&usage_h, &seg_a);
        link_storage(&usage_h, &seg_b);
        set_absorb_lag(&seg_b, 90);
        assert_eq!(
            absorb_lag_for_usage(&usage_h),
            90,
            "join must report the worst linked segment"
        );
        let (lagging, max) = absorb_backlog_summary();
        assert!(lagging >= 2, "summary missed lagging streams");
        assert!(max >= 90);
        clear_absorb_lag(&seg_a);
        clear_absorb_lag(&seg_b);
        assert_eq!(absorb_lag_for_usage(&usage_h), 0, "cleared lag must read 0");
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
}
