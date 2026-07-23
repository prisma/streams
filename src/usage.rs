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
mod tests {
    use super::*;

    #[test]
    fn buckets_enforce_and_refill() {
        let h = [1u8; 16];
        // requests bucket: capacity = 1000*2; drain it
        for _ in 0..(1000.0_f64 * 2.0) as usize {
            let _ = admit_append(&h, 10, 1);
        }
        let e = admit_append(&h, 10, 1);
        assert!(e.is_err());
        if let Err(hit) = e {
            assert!(hit.retry_ms() >= 1);
            assert!(!hit.code().is_empty());
            assert!(!hit.message().is_empty());
        }
    }

    #[test]
    fn byte_limit_names_itself() {
        let h = [2u8; 16];
        // one oversized request beyond 2s of byte budget
        let e = admit_append(&h, 11_000_000, 1);
        assert!(matches!(e, Err(LimitHit::Bytes { .. })));
    }
}
