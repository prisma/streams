//! Pravega-style auto-scaling controller (SCALING.md).
//!
//! Segments are INTERNAL CHILD STREAMS named "<parent>#<seg_id>", each a
//! plain totally-ordered stream (total order per segment ⊇ per-key order,
//! and a segment is exactly the Kinesis-shard commit-chain model). The
//! parent name is what clients use; append routing happens server-side:
//! stream-key → key_point → segment map → child stream. Sealing a segment
//! IS closing its child stream (existing machinery: the committer freezes
//! next_offset and further appends get 409 Stream-Closed, which the
//! routing wrapper converts into a map refresh + retry).
//!
//! The scaler loop watches per-segment usage (the same counters that feed
//! billing) as 2-minute EWMAs against the per-segment service limits and
//! splits hot segments / merges cold adjacent ones, CAS-writing the
//! segment map. CAS failures mean another instance decided first — the
//! loser reloads and re-evaluates. No leases needed: every transition is
//! serialized by the map's etag.

use std::collections::HashMap;
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, Instant};

use crate::segmap::{self, SegmentMap};

pub fn seg_stream_name(parent: &str, seg_id: u32) -> String {
    format!("{parent}#{seg_id}")
}

fn envf(k: &str, d: f64) -> f64 {
    std::env::var(k)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(d)
}

pub struct ScalePolicy {
    pub eval_secs: u64,
    /// EWMA window (Pravega-style two-minute rate by default).
    pub rate_window_secs: f64,
    pub hot_pct: f64,
    pub cold_pct: f64,
    pub hot_evals: u32,
    pub cold_evals: u32,
    pub cooldown_secs: i64,
    pub max_segments: usize,
}

pub fn policy() -> &'static ScalePolicy {
    static P: OnceLock<ScalePolicy> = OnceLock::new();
    P.get_or_init(|| ScalePolicy {
        eval_secs: envf("SCALE_EVAL_SECS", 10.0) as u64,
        rate_window_secs: envf("SCALE_RATE_WINDOW_SECS", 120.0),
        hot_pct: envf("SCALE_HOT_PCT", 75.0) / 100.0,
        cold_pct: envf("SCALE_COLD_PCT", 15.0) / 100.0,
        hot_evals: envf("SCALE_HOT_EVALS", 2.0) as u32,
        cold_evals: envf("SCALE_COLD_EVALS", 180.0) as u32,
        cooldown_secs: envf("SCALE_COOLDOWN_SECS", 600.0) as i64,
        max_segments: envf("MAX_SEGMENTS_PER_STREAM", 64.0) as usize,
    })
}

// ---- segment map cache (append-path hot lookup) -------------------------

struct CachedMap {
    map: SegmentMap,
    etag: Option<String>,
    at: Instant,
}

fn cache() -> &'static Mutex<HashMap<[u8; 16], CachedMap>> {
    static C: OnceLock<Mutex<HashMap<[u8; 16], CachedMap>>> = OnceLock::new();
    C.get_or_init(|| Mutex::new(HashMap::new()))
}

const CACHE_TTL: Duration = Duration::from_secs(2);

pub fn invalidate(hash: &[u8; 16]) {
    cache().lock().unwrap().remove(hash);
}

/// The scaled streams this instance has routed for — the scaler's work
/// list (cheap local discovery instead of bucket scans; every instance
/// serving a scaled stream runs the evaluation, CAS arbitrates).
fn known_scaled() -> &'static Mutex<HashMap<[u8; 16], String>> {
    static K: OnceLock<Mutex<HashMap<[u8; 16], String>>> = OnceLock::new();
    K.get_or_init(|| Mutex::new(HashMap::new()))
}

/// Load (with small TTL cache) the segment map for a scaled parent.
/// Missing map = implicit single-segment initial (seg 0 on "").
pub async fn load_map(
    store: &std::sync::Arc<dyn object_store::ObjectStore>,
    hash: &[u8; 16],
) -> SegmentMap {
    if let Some(c) = cache().lock().unwrap().get(hash) {
        if c.at.elapsed() < CACHE_TTL {
            return c.map.clone();
        }
    }
    let (map, etag) = match segmap::load(store, hash).await {
        Ok(Some((m, e))) => (m, e),
        _ => (SegmentMap::initial("", crate::shard::now_ms()), None),
    };
    cache().lock().unwrap().insert(
        *hash,
        CachedMap {
            map: map.clone(),
            etag,
            at: Instant::now(),
        },
    );
    map
}

/// Route a (parent, routing_key) append to its live segment stream name.
/// Registers the parent in the scaler work list.
pub async fn route(
    store: &std::sync::Arc<dyn object_store::ObjectStore>,
    parent: &str,
    routing_key: &str,
) -> String {
    let hash = crate::crypto::stream_hash(parent);
    known_scaled()
        .lock()
        .unwrap()
        .entry(hash)
        .or_insert_with(|| parent.to_string());
    let map = load_map(store, &hash).await;
    let k = segmap::key_point(routing_key);
    let seg = map.route(k).map(|s| s.seg_id).unwrap_or(0);
    seg_stream_name(parent, seg)
}

// ---- scaler loop ---------------------------------------------------------

pub struct SegEwma {
    pub bytes_rate: f64,
    pub reqs_rate: f64,
    pub recs_rate: f64,
    pub prev: (u64, u64, u64), // cumulative (bytes_in, requests, records)
    pub hot_streak: u32,
    pub cold_streak: u32,
}

/// Debug snapshot of the scaler's view (for /v1/debug/scaler).
pub fn debug_snapshot() -> serde_json::Value {
    let parents: Vec<String> = known_scaled().lock().unwrap().values().cloned().collect();
    let ew = ewma_state().lock().unwrap();
    let per: Vec<serde_json::Value> = ew
        .iter()
        .map(|(parent, segs)| {
            let segs: Vec<serde_json::Value> = segs
                .iter()
                .map(|(id, e)| {
                    serde_json::json!({
                        "seg": id,
                        "bytes_rate": e.bytes_rate.round(),
                        "reqs_rate": e.reqs_rate.round(),
                        "recs_rate": e.recs_rate.round(),
                        "hot_streak": e.hot_streak,
                        "cold_streak": e.cold_streak,
                    })
                })
                .collect();
            serde_json::json!({"parent": parent, "segments": segs})
        })
        .collect();
    serde_json::json!({"registered": parents, "ewmas": per})
}

/// Complete a split whose scaler died between seal and map-save: the
/// segment's stream is closed on its shard but the map still shows it
/// live, so routed appends bounce off `stream_closed` forever. Any valid
/// completion is correct (the crashed transition was never published) —
/// we split at the range midpoint. Returns true if this call published.
pub async fn resume_split(
    store: &std::sync::Arc<dyn object_store::ObjectStore>,
    parent: &str,
    seg_id: u32,
    sealed_next_offset: u64,
) -> bool {
    let hash = crate::crypto::stream_hash(parent);
    let Ok(Some((mut map, etag))) = segmap::load(store, &hash).await else {
        return false;
    };
    let Some(seg) = map.get(seg_id) else {
        invalidate(&hash);
        return false;
    };
    if seg.sealed_ms.is_some() {
        // Transition already recorded (we merely had a stale cache).
        invalidate(&hash);
        return false;
    }
    let (lo, hi) = (seg.lo, seg.hi);
    let mid = lo + (hi - lo) / 2;
    let now = crate::shard::now_ms();
    let ok = map.split(seg_id, mid, sealed_next_offset, "", "", now).is_ok()
        && segmap::save(store, &hash, &map, etag).await.is_ok();
    invalidate(&hash);
    if ok {
        tracing::info!("scaler: resumed crashed split of {parent} seg{seg_id} at {mid:#x}");
    }
    ok
}

/// One scaler evaluation for one parent stream. Returns Some(description)
/// when a transition was committed (for logs/tests). `owns` gates which
/// segments this instance may act on (its usage counters are only
/// authoritative for shards it serves).
pub async fn evaluate_stream<F, Fut, O>(
    store: &std::sync::Arc<dyn object_store::ObjectStore>,
    parent: &str,
    ewmas: &mut HashMap<u32, SegEwma>,
    seal: F,
    owns: O,
) -> Option<String>
where
    F: Fn(String) -> Fut,
    Fut: std::future::Future<Output = Option<u64>>,
    O: Fn(&str) -> bool,
{
    let p = policy();
    let hash = crate::crypto::stream_hash(parent);
    let (mut map, etag) = match segmap::load(store, &hash).await {
        Ok(Some(v)) => v,
        Ok(None) => {
            // Older scaled stream without a persisted map (or a lost
            // create-time write): persist the initial now so cooldown
            // clocks start; evaluate on the next tick.
            let m = SegmentMap::initial("", crate::shard::now_ms());
            let _ = segmap::save(store, &hash, &m, None).await;
            return None;
        }
        Err(_) => return None,
    };
    let limits = crate::usage::limits();
    let dt = p.eval_secs as f64;
    let alpha = 1.0 - (-(dt) / p.rate_window_secs).exp();

    let live: Vec<(u32, u64, u64)> = map
        .live()
        .map(|s| (s.seg_id, s.lo, s.hi))
        .collect();
    let now = crate::shard::now_ms();

    // Update EWMAs from the usage counters of each segment stream — but
    // only for segments whose shard THIS instance serves: the counters
    // are in-process, so only the serving instance sees real rates. Each
    // owner independently evaluates its own segments; the segmap CAS
    // arbitrates concurrent transitions.
    let owned: Vec<(u32, u64, u64)> = live
        .iter()
        .filter(|(id, _, _)| owns(&seg_stream_name(parent, *id)))
        .copied()
        .collect();
    ewmas.retain(|id, _| owned.iter().any(|(oid, _, _)| oid == id));
    for (seg_id, _, _) in &owned {
        let seg_hash = crate::crypto::stream_hash(&seg_stream_name(parent, *seg_id));
        let c = crate::usage::counters(&seg_hash);
        let cur = (
            c.bytes_in.load(std::sync::atomic::Ordering::Relaxed),
            c.requests.load(std::sync::atomic::Ordering::Relaxed),
            c.records.load(std::sync::atomic::Ordering::Relaxed),
        );
        let e = ewmas.entry(*seg_id).or_insert(SegEwma {
            bytes_rate: 0.0,
            reqs_rate: 0.0,
            recs_rate: 0.0,
            prev: cur,
            hot_streak: 0,
            cold_streak: 0,
        });
        let d_bytes = cur.0.saturating_sub(e.prev.0) as f64 / dt;
        let d_reqs = cur.1.saturating_sub(e.prev.1) as f64 / dt;
        let d_recs = cur.2.saturating_sub(e.prev.2) as f64 / dt;
        e.prev = cur;
        e.bytes_rate += alpha * (d_bytes - e.bytes_rate);
        e.reqs_rate += alpha * (d_reqs - e.reqs_rate);
        e.recs_rate += alpha * (d_recs - e.recs_rate);
        // A limit of 0 means "unenforced" — it must not participate in
        // scale decisions (0-limit hot terms made every active segment
        // look hot: runaway-split bug, e2e run 2).
        let hot = (limits.bytes_per_sec > 0.0 && e.bytes_rate > p.hot_pct * limits.bytes_per_sec)
            || (limits.reqs_per_sec > 0.0 && e.reqs_rate > p.hot_pct * limits.reqs_per_sec)
            || (limits.recs_per_sec > 0.0 && e.recs_rate > p.hot_pct * limits.recs_per_sec);
        let cold = (limits.bytes_per_sec <= 0.0 || e.bytes_rate < p.cold_pct * limits.bytes_per_sec)
            && (limits.reqs_per_sec <= 0.0 || e.reqs_rate < p.cold_pct * limits.reqs_per_sec)
            && (limits.recs_per_sec <= 0.0 || e.recs_rate < p.cold_pct * limits.recs_per_sec)
            && (limits.bytes_per_sec > 0.0 || limits.reqs_per_sec > 0.0 || limits.recs_per_sec > 0.0);
        e.hot_streak = if hot { e.hot_streak + 1 } else { 0 };
        e.cold_streak = if cold { e.cold_streak + 1 } else { 0 };
    }

    // Split: hottest eligible segment (owned only — see above).
    let split_candidate = owned
        .iter()
        .filter(|(id, _, _)| {
            let seg = map.get(*id).unwrap();
            let age_ok = now - seg.created_ms >= p.cooldown_secs * 1000;
            let hot = ewmas
                .get(id)
                .map(|e| e.hot_streak >= p.hot_evals)
                .unwrap_or(false);
            age_ok && hot && map.live().count() < p.max_segments
        })
        .max_by(|(a, _, _), (b, _, _)| {
            let ra = ewmas.get(a).map(|e| e.bytes_rate).unwrap_or(0.0);
            let rb = ewmas.get(b).map(|e| e.bytes_rate).unwrap_or(0.0);
            ra.partial_cmp(&rb).unwrap()
        })
        .copied();

    if let Some((seg_id, lo, hi)) = split_candidate {
        // Seal the segment stream first (freezes next_offset), then CAS
        // the map. A CAS loss after seal is safe: re-evaluation sees the
        // sealed child, reloads, and re-drives the map transition.
        let name = seg_stream_name(parent, seg_id);
        let Some(next) = seal(name.clone()).await else {
            return None;
        };
        // D4 fault point: simulate a scaler crash in the seal->save window
        // (the only non-atomic step; resume_split heals it on next touch).
        if std::env::var("SCALE_FAULT_POINT").ok().as_deref() == Some("after_seal") {
            return Some(format!(
                "FAULT INJECTED: crashed after sealing {name} (map not saved)"
            ));
        }
        let mid = lo + (hi - lo) / 2;
        if map.split(seg_id, mid, next, "", "", now).is_ok()
            && segmap::save(store, &hash, &map, etag).await.is_ok()
        {
            invalidate(&hash);
            ewmas.remove(&seg_id);
            return Some(format!("split seg{seg_id} of {parent} at {mid:#x} (next={next})"));
        }
        invalidate(&hash);
        return None;
    }

    // Merge: coldest adjacent live pair, both past cooldown+streak.
    // Both must be locally owned — a remote segment's local EWMA is
    // vacuously zero and would look cold when it is not.
    if owned.len() >= 2 {
        let mut sorted = owned.clone();
        sorted.sort_by_key(|(_, lo, _)| *lo);
        for w in sorted.windows(2) {
            let (a, b) = (w[0].0, w[1].0);
            let both_cold = [a, b].iter().all(|id| {
                ewmas
                    .get(id)
                    .map(|e| e.cold_streak >= p.cold_evals)
                    .unwrap_or(false)
                    && map
                        .get(*id)
                        .map(|s| now - s.created_ms >= p.cooldown_secs * 1000)
                        .unwrap_or(false)
            });
            if !both_cold {
                continue;
            }
            let (Some(na), Some(nb)) = (
                seal(seg_stream_name(parent, a)).await,
                seal(seg_stream_name(parent, b)).await,
            ) else {
                return None;
            };
            if map.merge(a, b, na, nb, "", now).is_ok()
                && segmap::save(store, &hash, &map, etag).await.is_ok()
            {
                invalidate(&hash);
                ewmas.remove(&a);
                ewmas.remove(&b);
                return Some(format!("merge seg{a}+seg{b} of {parent}"));
            }
            invalidate(&hash);
            return None;
        }
    }
    None
}

/// Work list snapshot for the loop in http.rs (which owns AppState and the
/// seal closure).
pub fn scaled_streams() -> Vec<String> {
    known_scaled().lock().unwrap().values().cloned().collect()
}

/// Per-parent EWMA state holder for the loop.
pub fn ewma_state() -> &'static Mutex<HashMap<String, HashMap<u32, SegEwma>>> {
    static E: OnceLock<Mutex<HashMap<String, HashMap<u32, SegEwma>>>> = OnceLock::new();
    E.get_or_init(|| Mutex::new(HashMap::new()))
}
