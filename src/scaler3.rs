//! Unified automatic scaler (spec §5): per-segment distribution
//! sketches fed at append admission, an evaluation loop that splits at
//! the load-weighted median (never the blind midpoint), hot-key
//! detection instead of ineffective splits, and a crash-resumable
//! two-phase transition protocol against the descriptor-resident map.
//!
//! Transition protocol (spec §5.3, hardened):
//!   Phase A  CAS the intent (`pending`) into the descriptor map —
//!            the split point survives a crash.
//!   Seal     close the parent segment IDENTITY through its committer
//!            (idempotent: re-closing returns the same frozen offset).
//!   Phase B  CAS successors live + parent sealed + pending cleared.
//! Any instance seeing `pending` can resume: the point is persisted,
//! the frozen offsets are re-read from the sealed identities.
//!
//! Scope: implicit-map and dynamic-map streams only. Legacy static
//! per-key layouts, legacy scaling=auto parents, and the queue /
//! state-protocol profiles are pinned single-segment until their
//! spec-§11/§12 integrations land.

use std::collections::HashMap;
use std::sync::{Mutex, OnceLock};

use crate::crypto::RoutingKeyHash;
use crate::registry::StreamDesc;
use crate::sketch::KeyDistribution;

/// Counters (spec §14).
pub static SEGMENT_SPLITS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
pub static SEGMENT_MERGES: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
pub static INEFFECTIVE_SPLIT_AVOIDED: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);
pub static SEGMENT_MAP_REFRESHES: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

struct SegSketch {
    dist: KeyDistribution,
    hot_streak: u32,
    profile_pinned: bool,
    last_fed_ms: i64,
}

/// Bounded sketch population (w100k finding: 100k streams x ~2 KB of
/// sketch = ~200 MB resident). The scaler only ever acts on HOT
/// segments, so cold sketches are pure ballast: idle entries evict on
/// an amortized sweep, and past the cap new streams are not sketched
/// until slots free (they re-enter on their next append once the sweep
/// runs — a hot stream feeds constantly and re-enters immediately).
const SKETCH_MAX: usize = 4_096;
const SKETCH_IDLE_MS: i64 = 600_000;
const SKETCH_SWEEP_EVERY: u64 = 4_096;
static SKETCH_TICK: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

struct State {
    sketches: HashMap<(String, u32), SegSketch>,
    /// Per-stream cooldown clock (ms of the last transition we drove or
    /// observed).
    last_transition_ms: HashMap<String, i64>,
    /// Detected unsplittable hot keys: stream → key hash.
    hot_keys: HashMap<String, RoutingKeyHash>,
}

fn state() -> &'static Mutex<State> {
    static S: OnceLock<Mutex<State>> = OnceLock::new();
    S.get_or_init(|| {
        Mutex::new(State {
            sketches: HashMap::new(),
            last_transition_ms: HashMap::new(),
            hot_keys: HashMap::new(),
        })
    })
}

/// The detected hot key for a stream, if any (observability + the
/// per-key limit surface).
pub fn hot_key(name: &str) -> Option<RoutingKeyHash> {
    state().lock().unwrap().hot_keys.get(name).copied()
}

pub fn hot_keys_all() -> Vec<(String, RoutingKeyHash)> {
    state()
        .lock()
        .unwrap()
        .hot_keys
        .iter()
        .map(|(n, k)| (n.clone(), *k))
        .collect()
}

/// Feed one admitted append into the segment's sketch. Cheap: one map
/// lookup + a few EWMA bumps under a short lock.
pub fn note_append(desc: &StreamDesc, seg: &crate::registry::SegRoute, bytes: u64, records: u64) {
    // Streams the scaler must not touch: legacy layouts and profiles
    // whose cursor/journal semantics are per-stream scalar today.
    let pinned = desc.is_per_key()
        || desc.scaling
        || matches!(
            desc.profile.as_deref(),
            Some("queue") | Some("state-protocol")
        );
    let now = crate::shard::now_ms();
    let mut g = state().lock().unwrap();
    // Amortized idle sweep keeps the population honest without a
    // per-append scan.
    if SKETCH_TICK.fetch_add(1, std::sync::atomic::Ordering::Relaxed) % SKETCH_SWEEP_EVERY == 0 {
        g.sketches
            .retain(|_, e| now - e.last_fed_ms < SKETCH_IDLE_MS);
    }
    let key = (desc.name.clone(), seg.seg_id);
    if !g.sketches.contains_key(&key) && g.sketches.len() >= SKETCH_MAX {
        return; // full: cold population; hot streams re-enter post-sweep
    }
    let e = g.sketches.entry(key).or_insert_with(|| SegSketch {
        dist: KeyDistribution::new(seg.lo, seg.hi, crate::scaler::policy().rate_window_secs),
        hot_streak: 0,
        profile_pinned: pinned,
        last_fed_ms: now,
    });
    e.last_fed_ms = now;
    e.dist.note(now, seg.point, seg.key_hash.0, bytes, records);
}

/// One evaluation pass over every sketched segment. Returns the split
/// decisions taken (stream, seg_id) — the driver executes them.
fn evaluate(now_ms: i64) -> Vec<(String, u32, u64)> {
    let pol = crate::scaler::policy();
    let lim = crate::usage::limits();
    let mut out = Vec::new();
    let mut g = state().lock().unwrap();
    let cooldowns = g.last_transition_ms.clone();
    let mut hot_updates: Vec<(String, Option<RoutingKeyHash>)> = Vec::new();
    for ((name, seg_id), sk) in g.sketches.iter_mut() {
        if sk.profile_pinned {
            continue;
        }
        let bytes_rate = sk.dist.bytes.value(now_ms);
        let reqs_rate = sk.dist.reqs.value(now_ms);
        let recs_rate = sk.dist.recs.value(now_ms);
        let hot = bytes_rate > lim.bytes_per_sec * pol.hot_pct
            || reqs_rate > lim.reqs_per_sec * pol.hot_pct
            || recs_rate > lim.recs_per_sec * pol.hot_pct;
        if !hot {
            sk.hot_streak = 0;
            hot_updates.push((name.clone(), None));
            continue;
        }
        sk.hot_streak += 1;
        if sk.hot_streak < pol.hot_evals {
            continue;
        }
        // Unsplittable single dominant key (spec §5.2): expose, apply
        // the per-key limit, never mint useless segments.
        let dominated = sk
            .dist
            .top_keys
            .top_share()
            .map(|(_, share)| share > 0.5)
            .unwrap_or(false);
        let plural = sk.dist.top_keys.keys_above(0.15) >= 2 || sk.dist.distinct.estimate() >= 8.0;
        if dominated && !plural {
            if let Some((k, _)) = sk.dist.top_keys.top_share() {
                hot_updates.push((name.clone(), Some(RoutingKeyHash(k))));
            }
            INEFFECTIVE_SPLIT_AVOIDED.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            continue;
        }
        // Cooldown.
        if now_ms - cooldowns.get(name).copied().unwrap_or(0) < pol.cooldown_secs * 1000 {
            continue;
        }
        // Both predicted children need meaningful load (≥ 15%).
        let Some((split_at, left_frac)) = sk.dist.weighted_median(now_ms) else {
            INEFFECTIVE_SPLIT_AVOIDED.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            continue;
        };
        if !(0.15..=0.85).contains(&left_frac) {
            INEFFECTIVE_SPLIT_AVOIDED.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            continue;
        }
        out.push((name.clone(), *seg_id, split_at));
        sk.hot_streak = 0;
    }
    for (name, hk) in hot_updates {
        match hk {
            Some(k) => {
                g.hot_keys.insert(name, k);
            }
            None => {
                g.hot_keys.remove(&name);
            }
        }
    }
    for (name, _, _) in &out {
        g.last_transition_ms.insert(name.clone(), now_ms);
    }
    out
}

/// Seal one segment identity through its committer: an empty close
/// append. Idempotent — re-closing a closed identity returns the same
/// frozen next offset via AppendErr::Closed.
async fn seal_identity(
    state: &std::sync::Arc<crate::http::AppState>,
    desc: &StreamDesc,
    seg_id: u32,
) -> Option<u64> {
    let identity = desc.dynamic_segment_identity(seg_id);
    // The seal must reach the engine that OWNS this segment's appends —
    // hard-coding the parent route here sealed the wrong shard for any
    // child with a real route (review blocker 1).
    let route = desc.segment_route_by_id(seg_id);
    let engine = state.engine_for_scaler(&route).await?;
    let (tx, rx) = tokio::sync::oneshot::channel();
    let req = crate::shard::AppendReq {
        enqueued_at: std::time::Instant::now(),
        hash: identity,
        route,
        entries: Vec::new(),
        usage: crate::usage::counters(&route),
        routing_key: String::new(),
        key_hash: crate::crypto::stream_hash(""),
        producer_lineage: Vec::new(),
        key_version: 0,
        subkey: [0u8; 32],
        ts_hint_ms: None,
        seq: None,
        bytes: 0,
        close: true,
        producer: None,
        deferred_error: None,
        touch: None,
        resp: tx,
    };
    if engine.try_enqueue(req).is_err() {
        return None;
    }
    match rx.await {
        Ok(Ok(ack)) => Some(ack.next_offset),
        Ok(Err(crate::shard::AppendErr::Closed { next_offset })) => Some(next_offset),
        _ => None,
    }
}

/// Execute (or resume) one split end-to-end. Idempotent at every step.
pub async fn execute_split(
    st: &std::sync::Arc<crate::http::AppState>,
    name: &str,
    seg_id: u32,
    split_at: u64,
) -> bool {
    // Phase A: persist the intent (materializing the implicit map).
    let ok = st
        .registry
        .cas_update(name, |d| {
            let map = d.segments.get_or_insert_with(|| {
                crate::segmap::SegmentMap::initial("", crate::shard::now_ms())
            });
            if map.pending.is_some() {
                return false; // an in-flight transition owns the map
            }
            let Some(seg) = map.get(seg_id) else {
                return false;
            };
            if !seg.is_live() || split_at <= seg.lo || split_at >= seg.hi {
                return false;
            }
            map.pending = Some(crate::segmap::PendingTransition {
                kind: "split".into(),
                segs: vec![seg_id],
                split_at,
                started_ms: crate::shard::now_ms(),
            });
            map.version += 1;
            true
        })
        .await
        .unwrap_or(false);
    if !ok {
        return resume(st, name).await; // maybe someone else's pending
    }
    resume(st, name).await
}

/// Complete whatever transition the descriptor's `pending` records:
/// seal the parents (idempotent), then CAS the successor publication.
/// Safe to call from any instance at any time.
pub async fn resume(st: &std::sync::Arc<crate::http::AppState>, name: &str) -> bool {
    st.registry.invalidate(name);
    let Ok(Some(desc)) = st.registry.get(name).await else {
        return false;
    };
    let Some(map) = &desc.segments else {
        return false;
    };
    let Some(p) = map.pending.clone() else {
        return false;
    };
    if p.kind != "split" || p.segs.len() != 1 {
        return false;
    }
    let seg_id = p.segs[0];
    let Some(frozen) = seal_identity(st, &desc, seg_id).await else {
        return false;
    };
    // Deterministic failpoint for the seal-to-publication gap tests:
    // while armed, EVERY resume parks here (readers only ever spawn
    // resume, so no request blocks on this).
    #[cfg(test)]
    failpoints::pause_before_publish().await;
    // Phase B: publish successors + clear the intent, with REAL routes
    // (review blocker 1: children on the parent's route add lineage but
    // zero capacity). The low child inherits the parent's route — its
    // predecessor data is already local; the high child gets a fresh
    // deterministic route (stable across CAS retries: derived from the
    // stream name and the child's seg id) that the ring spreads across
    // shard prefixes and owners.
    let prefixes = st.shard_prefixes.clone();
    let published = st
        .registry
        .cas_update(name, |d| {
            let pending_matches = d
                .segments
                .as_ref()
                .is_some_and(|m| m.pending.as_ref() == Some(&p));
            if !pending_matches {
                return false; // someone else already completed it
            }
            let low_route = d.segment_route_by_id(seg_id);
            // The high child's route must land on a DIFFERENT shard
            // prefix than the parent whenever the topology has one —
            // otherwise the "split" keeps both children behind the same
            // serial committer and adds no capacity. Deterministic
            // salting (same sequence on every CAS retry) walks candidate
            // routes until the prefix differs; a single-shard topology
            // accepts the first candidate (capacity comes when shards
            // do).
            let child_id = d.segments.as_ref().expect("checked").next_seg_id + 1;
            let parent_prefix = crate::registry::shard_for_hash(&prefixes, &low_route);
            let mut high_route = [0u8; 16];
            for salt in 0u32..16 {
                high_route = crate::crypto::stream_hash(&format!(
                    "{}\0segroute\0{}\0{}",
                    d.name, child_id, salt
                ));
                if prefixes.len() < 2
                    || crate::registry::shard_for_hash(&prefixes, &high_route) != parent_prefix
                {
                    break;
                }
            }
            let map = d.segments.as_mut().expect("checked above");
            match map.split(
                seg_id,
                p.split_at,
                frozen,
                low_route,
                high_route,
                crate::shard::now_ms(),
            ) {
                Ok(_) => {
                    map.pending = None;
                    true
                }
                Err(_) => {
                    // Already split (idempotent completion): just clear.
                    map.pending = None;
                    map.version += 1;
                    true
                }
            }
        })
        .await
        .unwrap_or(false);
    if published {
        SEGMENT_SPLITS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        // Fresh sketches for the children start on first appends; the
        // parent's sketch is retired.
        state()
            .lock()
            .unwrap()
            .sketches
            .remove(&(name.to_string(), seg_id));
        st.registry.invalidate(name);
        SEGMENT_MAP_REFRESHES.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }
    published
}

/// Test failpoints for the two-phase transition (spec review: the
/// seal-to-publication gap must be observable deterministically).
#[cfg(test)]
pub(crate) mod failpoints {
    use std::sync::OnceLock;
    use std::sync::atomic::{AtomicBool, Ordering};

    static ARMED: AtomicBool = AtomicBool::new(false);

    fn gate() -> &'static tokio::sync::Notify {
        static N: OnceLock<tokio::sync::Notify> = OnceLock::new();
        N.get_or_init(tokio::sync::Notify::new)
    }

    /// Park every resume() between parent seal and successor CAS.
    pub fn arm_before_publish() {
        ARMED.store(true, Ordering::SeqCst);
    }

    /// Release every parked resume and let future ones pass.
    pub fn release_before_publish() {
        ARMED.store(false, Ordering::SeqCst);
        gate().notify_waiters();
    }

    pub(super) async fn pause_before_publish() {
        while ARMED.load(Ordering::SeqCst) {
            let n = gate().notified();
            if !ARMED.load(Ordering::SeqCst) {
                break;
            }
            n.await;
        }
    }
}

/// The evaluation loop (one per instance).
pub fn start(st: std::sync::Weak<crate::http::AppState>) {
    tokio::spawn(async move {
        let eval = crate::scaler::policy().eval_secs.max(1);
        loop {
            tokio::time::sleep(std::time::Duration::from_secs(eval)).await;
            let Some(st) = st.upgrade() else { return };
            let decisions = evaluate(crate::shard::now_ms());
            for (name, seg_id, split_at) in decisions {
                let done = execute_split(&st, &name, seg_id, split_at).await;
                tracing::info!(
                    stream = %name,
                    seg_id,
                    split_at,
                    done,
                    "unified scaler split"
                );
            }
        }
    });
}

pub fn stats_json() -> serde_json::Value {
    use std::sync::atomic::Ordering::Relaxed;
    let hot: Vec<String> = hot_keys_all()
        .into_iter()
        .map(|(n, k)| format!("{}:{}", n, crate::crypto::hex(&k.0[..4])))
        .collect();
    serde_json::json!({
        "segment_splits": SEGMENT_SPLITS.load(Relaxed),
        "segment_merges": SEGMENT_MERGES.load(Relaxed),
        "ineffective_split_avoided": INEFFECTIVE_SPLIT_AVOIDED.load(Relaxed),
        "segment_map_refreshes": SEGMENT_MAP_REFRESHES.load(Relaxed),
        "hot_keys": hot,
    })
}
