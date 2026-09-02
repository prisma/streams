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
//! Scope: implicit-map and dynamic-map streams (the only kinds after
//! the clean switch).

use std::collections::HashMap;
use std::sync::{Mutex, OnceLock};

use crate::crypto::RoutingKeyHash;
use crate::registry::StreamDesc;
use crate::shard::ShardEngine;
use crate::sketch::KeyDistribution;

use std::sync::OnceLock as PolicyOnceLock;

/// Scaling policy knobs (moved from the deleted legacy scaler module;
/// the unified scaler is the only consumer).
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

static POLICY_EVAL_SECS_INIT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(10);
static POLICY_RATE_WINDOW_SECS_INIT: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(120.0f64.to_bits());
static POLICY_HOT_PCT_INIT: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0.75f64.to_bits());
static POLICY_COLD_PCT_INIT: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0.15f64.to_bits());
static POLICY_HOT_EVALS_INIT: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(2);
static POLICY_COLD_EVALS_INIT: std::sync::atomic::AtomicU32 =
    std::sync::atomic::AtomicU32::new(180);
static POLICY_COOLDOWN_SECS_INIT: std::sync::atomic::AtomicI64 =
    std::sync::atomic::AtomicI64::new(600);
static POLICY_MAX_SEGMENTS_INIT: std::sync::atomic::AtomicUsize =
    std::sync::atomic::AtomicUsize::new(64);

/// Composition-root seed (WP-01 PR 3.1): sized once from the owned
/// ServerConfig; un-seeded tests get the old env-unset defaults.
pub fn init_policy(cfg: &crate::config::ScaleConfig) {
    use std::sync::atomic::Ordering::Relaxed;
    POLICY_EVAL_SECS_INIT.store(cfg.eval_secs, Relaxed);
    POLICY_RATE_WINDOW_SECS_INIT.store(cfg.rate_window_secs.to_bits(), Relaxed);
    POLICY_HOT_PCT_INIT.store(cfg.hot_pct.to_bits(), Relaxed);
    POLICY_COLD_PCT_INIT.store(cfg.cold_pct.to_bits(), Relaxed);
    POLICY_HOT_EVALS_INIT.store(cfg.hot_evals, Relaxed);
    POLICY_COLD_EVALS_INIT.store(cfg.cold_evals, Relaxed);
    POLICY_COOLDOWN_SECS_INIT.store(cfg.cooldown_secs, Relaxed);
    POLICY_MAX_SEGMENTS_INIT.store(cfg.max_segments, Relaxed);
}

pub fn policy() -> &'static ScalePolicy {
    use std::sync::atomic::Ordering::Relaxed;
    static P: PolicyOnceLock<ScalePolicy> = PolicyOnceLock::new();
    P.get_or_init(|| ScalePolicy {
        eval_secs: POLICY_EVAL_SECS_INIT.load(Relaxed),
        rate_window_secs: f64::from_bits(POLICY_RATE_WINDOW_SECS_INIT.load(Relaxed)),
        hot_pct: f64::from_bits(POLICY_HOT_PCT_INIT.load(Relaxed)),
        cold_pct: f64::from_bits(POLICY_COLD_PCT_INIT.load(Relaxed)),
        hot_evals: POLICY_HOT_EVALS_INIT.load(Relaxed),
        cold_evals: POLICY_COLD_EVALS_INIT.load(Relaxed),
        cooldown_secs: POLICY_COOLDOWN_SECS_INIT.load(Relaxed),
        max_segments: POLICY_MAX_SEGMENTS_INIT.load(Relaxed),
    })
}

/// Counters (spec §14).
pub static SEGMENT_SPLITS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
pub static SEGMENT_MERGES: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
pub static INEFFECTIVE_SPLIT_AVOIDED: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);
pub static SEGMENT_MAP_REFRESHES: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);
pub static SKETCH_EVICTIONS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
pub static UNTRACKED_APPENDS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

struct SegSketch {
    /// The incarnation this sketch's heat belongs to. A name can be
    /// deleted and recreated while heat accumulates; a decision made
    /// from the old incarnation's traffic must neither split the
    /// replacement nor survive as ballast — a feed from a different
    /// epoch RESETS the sketch, and the decision carries this epoch to
    /// the fenced executor.
    epoch: String,
    dist: KeyDistribution,
    hot_streak: u32,
    /// Consecutive evaluations with ALL rates under the quiet line
    /// (merge policy input — mergers demand far more patience than
    /// splits to avoid flapping).
    cold_streak: u32,
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
    sketches: HashMap<(crate::tenant::TenantStreamRef, u32), SegSketch>,
    /// Per-stream cooldown clock (ms of the last transition we drove or
    /// observed).
    last_transition_ms: HashMap<crate::tenant::TenantStreamRef, i64>,
    /// Detected unsplittable hot keys: stream → key hash.
    hot_keys: HashMap<crate::tenant::TenantStreamRef, RoutingKeyHash>,
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
pub fn hot_key(sref: &crate::tenant::TenantStreamRef) -> Option<RoutingKeyHash> {
    state().lock().unwrap().hot_keys.get(sref).copied()
}

pub fn hot_keys_all() -> Vec<(crate::tenant::TenantStreamRef, RoutingKeyHash)> {
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
    // Fork chains stay single-segment (audit P0): stitched fork
    // reads resolve each ancestor through its ONE empty-key segment,
    // so a post-fork split would make inherited data unreadable.
    // Both a fork and a stream that HAS forks are pinned.
    if desc.forked_from.is_some() || !desc.fork_children.is_empty() {
        return;
    }
    let now = crate::shard::now_ms();
    let mut g = state().lock().unwrap();
    // Amortized idle sweep keeps the population honest without a
    // per-append scan.
    if SKETCH_TICK
        .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        .is_multiple_of(SKETCH_SWEEP_EVERY)
    {
        g.sketches
            .retain(|_, e| now - e.last_fed_ms < SKETCH_IDLE_MS);
    }
    let key = (desc.sref(), seg.seg_id);
    if !g.sketches.contains_key(&key) && g.sketches.len() >= SKETCH_MAX {
        // Automatic scaling must not silently stop at the cap (review
        // finding 8): evict the least-recently-fed sketch to admit the
        // new segment. A displaced hot segment re-enters on its next
        // append immediately.
        match g
            .sketches
            .iter()
            .min_by_key(|(_, e)| e.last_fed_ms)
            .map(|(k, _)| k.clone())
        {
            Some(victim) => {
                g.sketches.remove(&victim);
                SKETCH_EVICTIONS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }
            None => {
                UNTRACKED_APPENDS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                return;
            }
        }
    }
    let fresh = || SegSketch {
        epoch: desc.stream_epoch.clone(),
        dist: KeyDistribution::new(seg.lo, seg.hi, policy().rate_window_secs),
        hot_streak: 0,
        cold_streak: 0,
        last_fed_ms: now,
    };
    let e = g.sketches.entry(key).or_insert_with(fresh);
    if e.epoch != desc.stream_epoch {
        // The name was recreated: the accumulated heat belongs to a
        // collection that no longer exists. Start cold.
        *e = fresh();
    }
    e.last_fed_ms = now;
    e.dist.note(now, seg.point, seg.key_hash.0, bytes, records);
}

/// One evaluation pass over every sketched segment. Returns the split
/// decisions taken (stream, seg_id) — the driver executes them.
pub(crate) fn evaluate(
    now_ms: i64,
) -> (
    Vec<(crate::tenant::TenantStreamRef, String, u32, u64)>,
    Vec<(crate::tenant::TenantStreamRef, String)>,
) {
    let pol = policy();
    let lim = crate::usage::limits();
    let mut out = Vec::new();
    let mut g = state().lock().unwrap();
    let cooldowns = g.last_transition_ms.clone();
    let mut hot_updates: Vec<(crate::tenant::TenantStreamRef, Option<RoutingKeyHash>)> = Vec::new();
    for ((name, seg_id), sk) in g.sketches.iter_mut() {
        let bytes_rate = sk.dist.bytes.value(now_ms);
        let reqs_rate = sk.dist.reqs.value(now_ms);
        let recs_rate = sk.dist.recs.value(now_ms);
        let quiet = pol.hot_pct * 0.05;
        let cold = bytes_rate < lim.bytes_per_sec * quiet
            && reqs_rate < lim.reqs_per_sec * quiet
            && recs_rate < lim.recs_per_sec * quiet;
        if cold {
            sk.cold_streak = sk.cold_streak.saturating_add(1);
        } else {
            sk.cold_streak = 0;
        }
        let hot = bytes_rate > lim.bytes_per_sec * pol.hot_pct
            || reqs_rate > lim.reqs_per_sec * pol.hot_pct
            || recs_rate > lim.recs_per_sec * pol.hot_pct;
        if !hot {
            sk.hot_streak = 0;
            hot_updates.push((name.clone(), None));
            continue;
        }
        sk.cold_streak = 0;
        sk.hot_streak += 1;
        if sk.hot_streak < pol.hot_evals {
            continue;
        }
        // Unsplittable single dominant key (spec §5.2): expose, apply
        // the per-key limit, never mint useless segments.
        let top = sk.dist.top_keys_windowed();
        let dominated = top
            .top_share()
            .map(|(_, share)| share > 0.5)
            .unwrap_or(false);
        let plural = top.keys_above(0.15) >= 2 || sk.dist.distinct_windowed() >= 8.0;
        if dominated && !plural {
            if let Some((k, _)) = top.top_share() {
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
        out.push((name.clone(), sk.epoch.clone(), *seg_id, split_at));
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
    for (name, _, _, _) in &out {
        g.last_transition_ms.insert(name.clone(), now_ms);
    }
    // Merge candidates: streams with >= 2 sketched segments, EVERY one
    // cold for 4x the split patience, respecting the same cooldown. The
    // driver validates adjacency and ages against the live map.
    let mut per_stream: HashMap<&crate::tenant::TenantStreamRef, (usize, bool, String)> =
        HashMap::new();
    for ((name, _), sk) in g.sketches.iter() {
        let e = per_stream
            .entry(name)
            .or_insert((0, true, sk.epoch.clone()));
        e.0 += 1;
        e.1 &= sk.cold_streak >= pol.hot_evals * 4;
        // Segments sketched under DIFFERENT incarnations never merge:
        // the decision would be about two different collections.
        e.1 &= e.2 == sk.epoch;
    }
    let merge_candidates: Vec<(crate::tenant::TenantStreamRef, String)> = per_stream
        .into_iter()
        .filter(|(name, (n, all_cold, _))| {
            *n >= 2
                && *all_cold
                && now_ms - cooldowns.get(*name).copied().unwrap_or(0) >= pol.cooldown_secs * 1000
        })
        .map(|(name, (_, _, epoch))| (name.clone(), epoch))
        .collect();
    for (name, _) in &merge_candidates {
        g.last_transition_ms.insert(name.clone(), now_ms);
    }
    (out, merge_candidates)
}

/// Seal one segment identity through its committer: an empty close
/// append. Idempotent — re-closing a closed identity returns the same
/// frozen next offset via AppendErr::Closed.
/// Public seal of one segment identity (product lifecycle: collection
/// seal closes every live segment).
pub async fn seal_segment_identity(
    state: &std::sync::Arc<crate::http::AppState>,
    desc: &StreamDesc,
    seg_id: u32,
    seal_gen: Option<u64>,
) -> Option<u64> {
    seal_identity(state, desc, seg_id, seal_gen).await
}

async fn seal_identity(
    state: &std::sync::Arc<crate::http::AppState>,
    desc: &StreamDesc,
    seg_id: u32,
    seal_gen: Option<u64>,
) -> Option<u64> {
    let identity = desc.dynamic_segment_identity(seg_id);
    // The seal must reach the engine that OWNS this segment's appends —
    // hard-coding the parent route here sealed the wrong shard for any
    // child with a real route (review blocker 1).
    let route = desc.segment_route_by_id(seg_id);
    // Round-4 follow-up review, finding 3: the scaler resolution used
    // to be `.ok()`ed into None, so a wrong-owner response, an owner
    // convergence holdoff, an open failure and a capacity refusal were
    // indistinguishable in the logs — exactly when the segment lived
    // on ANOTHER instance. Resolve typed and name the category.
    // Round-4 follow-up review, finding 2: WRONG OWNER is not an
    // error — it is a routing fact. Relay the close to the segment's
    // owner over the fleet-internal channel instead of failing the
    // whole collection seal.
    match state.engine_for_quiet(&route).await {
        Ok(engine) => close_segment_on_engine(&engine, identity, &route, seg_id, seal_gen).await,
        Err(resp) => {
            let is_redirect = resp.status() == axum::http::StatusCode::CONFLICT
                && resp.headers().contains_key("streams-replay-to");
            if !is_redirect {
                let replay_to = resp
                    .headers()
                    .get("streams-replay-to")
                    .and_then(|v| v.to_str().ok())
                    .map(str::to_string);
                tracing::error!(
                    seg_id,
                    ?route,
                    status = %resp.status(),
                    ?replay_to,
                    "seal segment engine resolution failed"
                );
                return None;
            }
            relay_segment_close(state, desc, seg_id, seal_gen, resp).await
        }
    }
}

/// The ONE segment-close primitive: submit an empty close append for
/// this exact segment identity to the LOCAL committer. Idempotent per
/// identity (a re-close of a closed segment answers its frozen next
/// offset via AppendErr::Closed). Shared by collection sealing,
/// split/merge transitions, and the fleet-internal segment-close
/// receiver.
pub(crate) async fn close_segment_on_engine(
    engine: &std::sync::Arc<ShardEngine>,
    identity: [u8; 16],
    route: &[u8; 16],
    seg_id: u32,
    seal_gen: Option<u64>,
) -> Option<u64> {
    let (tx, rx) = tokio::sync::oneshot::channel();
    let req = crate::shard::AppendReq {
        enqueued_at: std::time::Instant::now(),
        hash: identity,
        route: *route,
        entries: Vec::new(),
        usage: crate::usage::counters(route),
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
        sealed_reject_new: None,
        touch: None,
        seal_gen,
        seal_fence_to: None,
        billing: None,
        resp: tx,
    };
    if let Err(_req) = engine.try_enqueue(req) {
        tracing::error!(
            seg_id,
            ?route,
            "seal close never enqueued (committer queue full or closed)"
        );
        return None;
    }
    match rx.await {
        Ok(Ok(ack)) => Some(ack.next_offset),
        Ok(Err(crate::shard::AppendErr::Closed { next_offset })) => Some(next_offset),
        Ok(Err(other)) => {
            tracing::error!(seg_id, "seal close refused: {other:?}");
            None
        }
        Err(_) => {
            tracing::error!(
                seg_id,
                "seal close's committer answer was dropped (responder gone)"
            );
            None
        }
    }
}

/// Relay one segment close to the segment's OWNER instance over the
/// fleet-internal channel. The peer re-derives every target fact from
/// ITS OWN descriptor before submitting (the coordinator's descriptor
/// may be stale), so this carries only the coordinates plus the bound
/// target headers. One ownership redirect is followed; a SECOND means
/// ownership moved mid-relay — fail retryable rather than chase.
async fn relay_segment_close(
    state: &std::sync::Arc<crate::http::AppState>,
    desc: &StreamDesc,
    seg_id: u32,
    seal_gen: Option<u64>,
    redirect: axum::response::Response,
) -> Option<u64> {
    let Some((owner, base)) = crate::http::replay_peer_url(state, &redirect) else {
        tracing::error!(
            seg_id,
            stream = %desc.name,
            "segment owner answered a redirect but no peer URL is configured"
        );
        return None;
    };
    let target = crate::product::InternalTarget::of(desc, seg_id)?;
    let gen_q = seal_gen.map(|g| g.to_string()).unwrap_or_default();
    let name = crate::http::encode_stream_name_path(&desc.name);
    let mk = |bearer: Option<&str>| {
        let mut req = crate::http::peer_client()
            .post(format!(
                "{base}/v1/internal/segment-close/{name}?seg_id={seg_id}&seal_gen={gen_q}"
            ))
            .timeout(std::time::Duration::from_secs(40));
        for (k, v) in target.headers() {
            req = req.header(k, v);
        }
        if let Some(t) = bearer {
            req = req.header("authorization", format!("Bearer {t}"));
        }
        req
    };
    match state.peer.send(mk).await {
        Ok(r) if r.status() == axum::http::StatusCode::OK => {
            #[derive(serde::Deserialize)]
            struct Ack {
                next_offset: u64,
            }
            match r.json::<Ack>().await {
                Ok(a) => Some(a.next_offset),
                Err(e) => {
                    tracing::error!(
                        seg_id,
                        owner = %owner,
                        "segment-close relay answer unparsable: {e}"
                    );
                    None
                }
            }
        }
        Ok(r) if r.status() == axum::http::StatusCode::CONFLICT => {
            tracing::error!(
                seg_id,
                owner = %owner,
                "segment-close relay was redirected again; ownership moved twice — \
                 refusing to chase (retry the seal)"
            );
            None
        }
        Ok(r) => {
            let status = r.status();
            let body = r.text().await.unwrap_or_default();
            tracing::error!(
                seg_id,
                owner = %owner,
                status = %status,
                "segment-close relay refused: {}",
                &body[..body.len().min(300)]
            );
            None
        }
        Err(e) => {
            tracing::error!(
                seg_id,
                owner = %owner,
                "segment-close relay to {base} failed: {e}"
            );
            None
        }
    }
}

/// Execute (or resume) one split end-to-end. Idempotent at every step.
/// Resolves the CURRENT incarnation and splits it — the entry point for
/// direct calls that just created or inspected the stream.
pub async fn execute_split(
    st: &std::sync::Arc<crate::http::AppState>,
    sref: &crate::tenant::TenantStreamRef,
    seg_id: u32,
    split_at: u64,
) -> bool {
    let Ok(Some(d)) = st.registry.get(sref).await else {
        return false;
    };
    execute_split_fenced(st, sref, &d.stream_epoch, seg_id, split_at).await
}

/// The fenced form: the split decision was computed from ONE
/// incarnation's segment map, and a replacement created under the same
/// name can coincidentally satisfy every structural guard (a fresh
/// stream's segment 0 is live and spans the full range, so any split
/// point "fits"). The autonomous scaler always calls this with the
/// epoch of the descriptor its decision came from.
pub async fn execute_split_fenced(
    st: &std::sync::Arc<crate::http::AppState>,
    sref: &crate::tenant::TenantStreamRef,
    expect_epoch: &str,
    seg_id: u32,
    split_at: u64,
) -> bool {
    // Phase A: persist the intent (materializing the implicit map).
    let ok = st
        .registry
        .cas_update_incarnation(sref, expect_epoch, |d| {
            // Fork chains stay single-segment (audit P0): stitched fork
            // reads resolve each ancestor through its ONE empty-key
            // segment, so a post-fork split would make inherited data
            // unreadable. Both a fork and a stream that HAS forks are
            // pinned — enforced HERE, at the transition itself, so a
            // direct scaler call cannot bypass it.
            if d.forked_from.is_some() || !d.fork_children.is_empty() {
                return false;
            }
            // A sealing or sealed collection has a fixed topology. A
            // transition that started just before the seal could
            // otherwise publish a successor AFTER the seal took its
            // snapshot of live segments — a new writable child under a
            // collection that already reports Sealed.
            if d.sealed || d.sealing.is_some() {
                return false;
            }
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
                seal_gen: 0, // patched below: needs the counter
            });
            map.version += 1;
            d.seal_gen_counter += 1;
            let g = d.seal_gen_counter;
            if let Some(p) = d.segments.as_mut().and_then(|m| m.pending.as_mut()) {
                p.seal_gen = g;
            }
            true
        })
        .await
        .unwrap_or(false);
    if !ok {
        return resume(st, sref).await; // maybe someone else's pending
    }
    resume(st, sref).await
}

/// Execute (or resume) one MERGE of two adjacent live segments.
/// Same two-phase discipline as split: persist the intent, seal both
/// parents, publish the child. Idempotent at every step.
pub async fn execute_merge(
    st: &std::sync::Arc<crate::http::AppState>,
    sref: &crate::tenant::TenantStreamRef,
    a_id: u32,
    b_id: u32,
) -> bool {
    let Ok(Some(d)) = st.registry.get(sref).await else {
        return false;
    };
    execute_merge_fenced(st, sref, &d.stream_epoch, a_id, b_id).await
}

/// See [`execute_split_fenced`] — the same incarnation fence, for the
/// same reason.
pub async fn execute_merge_fenced(
    st: &std::sync::Arc<crate::http::AppState>,
    sref: &crate::tenant::TenantStreamRef,
    expect_epoch: &str,
    a_id: u32,
    b_id: u32,
) -> bool {
    let ok = st
        .registry
        .cas_update_incarnation(sref, expect_epoch, |d| {
            // A sealing or sealed collection has a fixed topology. A
            // transition that started just before the seal could
            // otherwise publish a successor AFTER the seal took its
            // snapshot of live segments — a new writable child under a
            // collection that already reports Sealed.
            if d.sealed || d.sealing.is_some() {
                return false;
            }
            let map = d.segments.get_or_insert_with(|| {
                crate::segmap::SegmentMap::initial("", crate::shard::now_ms())
            });
            if map.pending.is_some() {
                return false;
            }
            let (Some(a), Some(b)) = (map.get(a_id), map.get(b_id)) else {
                return false;
            };
            let adjacent = a.hi == b.lo || b.hi == a.lo;
            if !a.is_live() || !b.is_live() || !adjacent {
                return false;
            }
            map.pending = Some(crate::segmap::PendingTransition {
                kind: "merge".into(),
                segs: vec![a_id, b_id],
                split_at: 0,
                started_ms: crate::shard::now_ms(),
                seal_gen: 0, // patched below: needs the counter
            });
            map.version += 1;
            d.seal_gen_counter += 1;
            let g = d.seal_gen_counter;
            if let Some(p) = d.segments.as_mut().and_then(|m| m.pending.as_mut()) {
                p.seal_gen = g;
            }
            true
        })
        .await
        .unwrap_or(false);
    if !ok {
        return resume(st, sref).await;
    }
    resume(st, sref).await
}

/// Complete whatever transition the descriptor's `pending` records:
/// seal the parents (idempotent), then CAS the successor publication.
/// Safe to call from any instance at any time.
pub async fn resume(
    st: &std::sync::Arc<crate::http::AppState>,
    sref: &crate::tenant::TenantStreamRef,
) -> bool {
    st.registry.invalidate(sref);
    let Ok(Some(desc)) = st.registry.get(sref).await else {
        return false;
    };
    let Some(map) = &desc.segments else {
        return false;
    };
    let Some(p) = map.pending.clone() else {
        return false;
    };
    match (p.kind.as_str(), p.segs.len()) {
        ("split", 1) => {}
        ("merge", 2) => return resume_merge(st, &desc, p).await,
        _ => return false,
    }
    let seg_id = p.segs[0];
    let tg = (p.seal_gen > 0).then_some(p.seal_gen);
    let Some(frozen) = seal_identity(st, &desc, seg_id, tg).await else {
        return false;
    };
    // Deterministic failpoint for the seal-to-publication gap tests
    // (#108 registry): parks per PARENT STREAM NAME, so parallel gap
    // tests never wake on each other's transitions.
    #[cfg(test)]
    crate::failpoints::pause_scaler_before_publish(&desc.name).await;
    // Phase B: publish successors + clear the intent, with REAL routes
    // (review blocker 1: children on the parent's route add lineage but
    // zero capacity). The low child inherits the parent's route — its
    // predecessor data is already local; the high child gets a fresh
    // deterministic route (stable across CAS retries: derived from the
    // stream name and the child's seg id) that the ring spreads across
    // shard prefixes and owners.
    let prefixes = st.shards.prefixes().to_vec();
    // Phase B is fenced to the incarnation the pending transition was
    // READ from: mid-resume, the whole collection can be deleted and
    // recreated, and a name-scoped publication would stamp successors
    // onto the replacement.
    let published = st
        .registry
        .cas_update_incarnation(&desc.sref(), &desc.stream_epoch, |d| {
            // Phase B is a SECOND durable step, so it re-checks the
            // lifecycle. Fencing only phase A left this race: publish
            // pending -> seal the parent -> pause -> the collection
            // seals (snapshotting the segments it can see) -> phase B
            // resumes and publishes live children UNDER a sealed
            // collection. Once sealed, no topology operation may create
            // another live segment.
            if d.sealed || d.sealing.is_some() {
                return false;
            }
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
                // Contract r1: route-child-v1 + project + name +
                // child_segment_id + salt — the layout-4 domain-
                // separated construction (was the "\0segroute\0"
                // delimiter string).
                high_route =
                    crate::crypto::RouteHash::for_child(&d.sref(), child_id, &salt.to_be_bytes()).0;
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
            .remove(&(desc.sref(), seg_id));
        st.registry.invalidate(&desc.sref());
        SEGMENT_MAP_REFRESHES.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }
    published
}

/// Merge completion: seal BOTH parents (idempotent), then publish the
/// merged child on the low parent's route. Crash-resumable from the
/// persisted pending intent; the seal-gap read semantics apply to both
/// parents automatically (pending.segs names them).
async fn resume_merge(
    st: &std::sync::Arc<crate::http::AppState>,
    desc: &StreamDesc,
    p: crate::segmap::PendingTransition,
) -> bool {
    let (a_id, b_id) = (p.segs[0], p.segs[1]);
    let tg = (p.seal_gen > 0).then_some(p.seal_gen);
    let Some(fa) = seal_identity(st, desc, a_id, tg).await else {
        return false;
    };
    let Some(fb) = seal_identity(st, desc, b_id, tg).await else {
        return false;
    };
    #[cfg(test)]
    crate::failpoints::pause_scaler_before_publish(&desc.name).await;
    let published = st
        .registry
        .cas_update_incarnation(&desc.sref(), &desc.stream_epoch, |d| {
            // Phase B is a SECOND durable step, so it re-checks the
            // lifecycle. Fencing only phase A left this race: publish
            // pending -> seal the parent -> pause -> the collection
            // seals (snapshotting the segments it can see) -> phase B
            // resumes and publishes live children UNDER a sealed
            // collection. Once sealed, no topology operation may create
            // another live segment.
            if d.sealed || d.sealing.is_some() {
                return false;
            }
            let pending_matches = d
                .segments
                .as_ref()
                .is_some_and(|m| m.pending.as_ref() == Some(&p));
            if !pending_matches {
                return false;
            }
            let child_route = d.segment_route_by_id(a_id);
            let map = d.segments.as_mut().expect("checked");
            match map.merge(a_id, b_id, fa, fb, child_route, crate::shard::now_ms()) {
                Ok(_) => {
                    map.pending = None;
                    true
                }
                Err(_) => {
                    // Already merged (idempotent completion): just clear.
                    map.pending = None;
                    map.version += 1;
                    true
                }
            }
        })
        .await
        .unwrap_or(false);
    if published {
        SEGMENT_MERGES.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        {
            let mut g = state().lock().unwrap();
            g.sketches.remove(&(desc.sref(), a_id));
            g.sketches.remove(&(desc.sref(), b_id));
        }
        st.registry.invalidate(&desc.sref());
        SEGMENT_MAP_REFRESHES.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }
    published
}

/// The evaluation loop (one per instance).
pub fn start(st: std::sync::Weak<crate::http::AppState>, tasks: &crate::tasks::TaskSupervisor) {
    let _ = tasks.spawn("scaler", crate::tasks::Policy::Critical, move |cancel| async move {
        let eval = policy().eval_secs.max(1);
        loop {
            tokio::select! {
                _ = cancel.cancelled() => return crate::tasks::TaskResult::Done,
                _ = tokio::time::sleep(std::time::Duration::from_secs(eval)) => {}
            }
            let Some(st) = st.upgrade() else {
                return crate::tasks::TaskResult::Done;
            };
            let (decisions, merges) = evaluate(crate::shard::now_ms());
            for (name, epoch, seg_id, split_at) in decisions {
                // Fenced to the incarnation the HEAT belongs to — a
                // decision computed from a deleted collection's
                // traffic declines instead of splitting whatever now
                // owns the name.
                let done = execute_split_fenced(&st, &name, &epoch, seg_id, split_at).await;
                if done {
                    // §12.3 topology journal: deterministic per
                    // (incarnation, parent segment) — a replayed
                    // execution is the same transition.
                    crate::ops::emit(
                        crate::ops::OpsEvent::new(
                            "split_committed",
                            format!("split/{epoch}/{seg_id}"),
                        )
                        .stream(&name, &epoch)
                        .fields(serde_json::json!({"segId": seg_id, "splitAt": split_at, "projectId": name.project_id().as_str()})),
                    );
                }
                tracing::info!(
                    project = %name.project_id().as_str(),
                    stream = %name.name().as_str(),
                    seg_id,
                    split_at,
                    done,
                    "unified scaler split"
                );
            }
            for (name, epoch) in merges {
                // Validate against the LIVE map: pick the first adjacent
                // live pair, both older than the cooldown (never merge a
                // segment the scaler just minted). The decision is
                // fenced to the incarnation the cold sketches belong
                // to — a replacement under the same name is not merged
                // on a dead collection's silence.
                let Ok(Some(desc)) = st.registry.get(&name).await else {
                    continue;
                };
                if desc.stream_epoch != epoch {
                    continue;
                }
                let Some(map) = &desc.segments else { continue };
                if map.pending.is_some() {
                    continue;
                }
                let mut live: Vec<_> = map.segments.iter().filter(|s| s.is_live()).collect();
                live.sort_by_key(|s| s.lo);
                let min_age = policy().cooldown_secs * 1000;
                let now = crate::shard::now_ms();
                let pair = live.windows(2).find(|w| {
                    w[0].hi == w[1].lo
                        && now - w[0].created_ms >= min_age
                        && now - w[1].created_ms >= min_age
                });
                if let Some(w) = pair {
                    let (a, b) = (w[0].seg_id, w[1].seg_id);
                    let done = execute_merge_fenced(&st, &name, &epoch, a, b).await;
                    if done {
                        crate::ops::emit(
                            crate::ops::OpsEvent::new(
                                "merge_committed",
                                format!("merge/{epoch}/{a}/{b}"),
                            )
                            .stream(&name, &epoch)
                            .fields(serde_json::json!({"a": a, "b": b, "projectId": name.project_id().as_str()})),
                        );
                    }
                    tracing::info!(project = %name.project_id().as_str(), stream = %name.name().as_str(), a, b, done, "unified scaler merge");
                }
            }
        }
    });
}

pub fn stats_json() -> serde_json::Value {
    use std::sync::atomic::Ordering::Relaxed;
    let hot: Vec<String> = hot_keys_all()
        .into_iter()
        .map(|(n, k)| {
            format!(
                "{}/{}:{}",
                n.project_id().as_str(),
                n.name().as_str(),
                crate::crypto::hex(&k.0[..4])
            )
        })
        .collect();
    serde_json::json!({
        "segment_splits": SEGMENT_SPLITS.load(Relaxed),
        "segment_merges": SEGMENT_MERGES.load(Relaxed),
        "ineffective_split_avoided": INEFFECTIVE_SPLIT_AVOIDED.load(Relaxed),
        "segment_map_refreshes": SEGMENT_MAP_REFRESHES.load(Relaxed),
        "sketches": state().lock().unwrap().sketches.len(),
        "sketch_evictions": SKETCH_EVICTIONS.load(Relaxed),
        "untracked_appends": UNTRACKED_APPENDS.load(Relaxed),
        "hot_keys": hot,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_desc(name: &str) -> StreamDesc {
        StreamDesc {
            seal_gen_counter: 0,
            account_id: None,
            project_id: crate::tenant::ProjectId::new("proj-test").unwrap(),
            name: name.into(),
            stream_epoch: "00000000000000000000000000000000".into(),
            key_fingerprint: "fp".into(),
            created_ms: 1,
            expires_at_ms: None,
            deleted: false,
            content_type: "application/json".into(),
            ttl_secs: None,
            segments: None,
            sealed: false,
            watch_definitions: Vec::new(),
            watch_sig_key: None,
            parent_ref_pending: false,
            soft_deleted: false,
            logical_close_ms: None,
            forked_from: None,
            fork_children: Vec::new(),
            init: None,
            sealing: None,
            seal_op: None,
            layout_version: crate::registry::LAYOUT_VERSION,
        }
    }

    /// Review finding 8: past SKETCH_MAX, new segments must still be
    /// sketched — the least-recently-fed sketch is evicted (counted),
    /// never a silent refusal to track.
    #[test]
    fn sketch_cap_evicts_instead_of_starving() {
        let over = SKETCH_MAX + 8;
        for i in 0..over {
            let desc = test_desc(&format!("cap-seg-{i}"));
            let seg = desc.resolve_segment("k");
            note_append(&desc, &seg, 100, 1);
        }
        let newest_key = (test_desc(&format!("cap-seg-{}", over - 1)).sref(), 0);
        let (tracked, has_newest) = {
            let g = state().lock().unwrap();
            (g.sketches.len(), g.sketches.contains_key(&newest_key))
        };
        assert!(tracked <= SKETCH_MAX, "population bounded: {tracked}");
        assert!(
            has_newest,
            "the newest segment must be tracked — silent starvation is the bug"
        );
        assert!(
            SKETCH_EVICTIONS.load(std::sync::atomic::Ordering::Relaxed) > 0,
            "evictions must be counted"
        );
    }
}
