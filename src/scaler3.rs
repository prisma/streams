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
use std::sync::{Arc, Mutex};

use crate::crypto::RoutingKeyHash;
use crate::registry::StreamDesc;
use crate::sketch::KeyDistribution;

type ScalePolicy = crate::config::ScaleConfig;

/// Runtime-owned policy, sketches and monotonic cooldown clock.
pub(crate) struct Scaler {
    state: Mutex<State>,
    policy: ScalePolicy,
    limits: crate::usage::Limits,
    clock: Arc<dyn crate::runtime::Clock>,
}
impl std::fmt::Debug for Scaler {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Scaler")
            .field("policy", &self.policy)
            .finish_non_exhaustive()
    }
}
impl Scaler {
    pub(crate) fn new(
        policy: &ScalePolicy,
        admission: &crate::config::AdmissionConfig,
        clock: Arc<dyn crate::runtime::Clock>,
    ) -> Self {
        Self {
            state: Mutex::new(State::default()),
            policy: policy.clone(),
            clock,
            limits: crate::usage::Limits {
                bytes_per_sec: admission.limit_bytes_per_sec,
                reqs_per_sec: admission.limit_reqs_per_sec,
                recs_per_sec: admission.limit_recs_per_sec,
                burst_secs: admission.limit_burst_secs,
            },
        }
    }
}

/// Counters (spec §14).
pub(crate) static SEGMENT_SPLITS: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);
pub(crate) static SEGMENT_MERGES: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);
pub(crate) static INEFFECTIVE_SPLIT_AVOIDED: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);
pub(crate) static SEGMENT_MAP_REFRESHES: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);
pub(crate) static SKETCH_EVICTIONS: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);
pub(crate) static UNTRACKED_APPENDS: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

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

#[derive(Default)]
struct State {
    tick: u64,
    #[cfg(test)]
    incarnation_scan_entries: usize,
    sketches: HashMap<(crate::tenant::TenantStreamRef, u32), SegSketch>,
    /// Per-stream cooldown clock (ms of the last transition we drove or
    /// observed).
    last_transition_ms: HashMap<(crate::tenant::TenantStreamRef, String), i64>,
    /// Detected unsplittable hot keys: stream → key hash.
    hot_keys: HashMap<crate::tenant::TenantStreamRef, RoutingKeyHash>,
}

impl State {
    /// Sketches, summaries, and cooldowns each have a finite retention
    /// budget. Cooldowns may survive a parent's retirement until a child
    /// receives traffic, but never beyond their useful horizon or capacity.
    fn prune(&mut self, now: i64, policy: &ScalePolicy) {
        self.sketches
            .retain(|_, sk| now.saturating_sub(sk.last_fed_ms) < SKETCH_IDLE_MS);
        self.last_transition_ms.retain(|_, at| {
            now.saturating_sub(*at) < (policy.cooldown_secs * 1000).max(SKETCH_IDLE_MS)
        });
        while self.last_transition_ms.len() > SKETCH_MAX {
            let victim = self
                .last_transition_ms
                .iter()
                .min_by_key(|((name, epoch), at)| {
                    (
                        **at,
                        name.project_id().as_str(),
                        name.name().as_str(),
                        epoch.as_str(),
                    )
                })
                .map(|(key, _)| key.clone())
                .unwrap();
            self.last_transition_ms.remove(&victim);
        }
        let live: std::collections::HashSet<_> =
            self.sketches.keys().map(|(name, _)| name).collect();
        self.hot_keys.retain(|name, _| live.contains(name));
    }

    fn forget_previous_incarnation(&mut self, name: &crate::tenant::TenantStreamRef, epoch: &str) {
        #[cfg(test)]
        let mut visited = 0;
        let changed = self.sketches.iter().any(|((n, _), sk)| {
            #[cfg(test)]
            {
                visited += 1;
            }
            n == name && sk.epoch != epoch
        });
        if changed {
            self.sketches.retain(|(n, _), sk| {
                #[cfg(test)]
                {
                    visited += 1;
                }
                n != name || sk.epoch == epoch
            });
            self.hot_keys.remove(name);
        }
        self.last_transition_ms.retain(|(n, e), _| {
            #[cfg(test)]
            {
                visited += 1;
            }
            n != name || e == epoch
        });
        #[cfg(test)]
        {
            self.incarnation_scan_entries += visited;
        }
    }
}

impl Scaler {
    pub(crate) fn hot_keys_all(&self) -> Vec<(crate::tenant::TenantStreamRef, RoutingKeyHash)> {
        let mut hot: Vec<_> = self
            .state
            .lock()
            .unwrap()
            .hot_keys
            .iter()
            .map(|(n, k)| (n.clone(), *k))
            .collect();
        hot.sort_by(|a, b| {
            (a.0.project_id().as_str(), a.0.name().as_str())
                .cmp(&(b.0.project_id().as_str(), b.0.name().as_str()))
        });
        hot
    }

    /// Feed one admitted append into the segment's sketch. A known segment
    /// uses constant-time map lookups and EWMA bumps under a short lock.
    pub(crate) fn note_append(
        &self,
        desc: &StreamDesc,
        seg: &crate::registry::SegRoute,
        bytes: u64,
        records: u64,
    ) {
        // Fork chains stay single-segment (audit P0): stitched fork
        // reads resolve each ancestor through its ONE empty-key segment,
        // so a post-fork split would make inherited data unreadable.
        // Both a fork and a stream that HAS forks are pinned.
        if desc.forked_from.is_some() || !desc.fork_children.is_empty() {
            return;
        }
        let now = self.clock.monotonic().millis();
        let mut g = self.state.lock().unwrap();
        // Amortized idle sweep keeps the population honest without a
        // per-append scan.
        g.tick = g.tick.wrapping_add(1);
        if g.tick.is_multiple_of(SKETCH_SWEEP_EVERY) {
            g.prune(now, &self.policy);
        }
        let key = (desc.sref(), seg.seg_id);
        // The common case must not scan every stream under the shared lock.
        // An existing matching sketch proves this incarnation was admitted;
        // new segments and epoch changes still clean up all obsolete siblings.
        if g.sketches
            .get(&key)
            .is_none_or(|sk| sk.epoch != desc.stream_epoch)
        {
            g.forget_previous_incarnation(&key.0, &desc.stream_epoch);
        }
        if !g.sketches.contains_key(&key) && g.sketches.len() >= SKETCH_MAX {
            // Automatic scaling must not silently stop at the cap (review
            // finding 8): evict the least-recently-fed sketch to admit the
            // new segment. A displaced hot segment re-enters on its next
            // append immediately.
            match g
                .sketches
                .iter()
                .min_by_key(|((name, seg_id), e)| {
                    (
                        e.last_fed_ms,
                        name.project_id().as_str(),
                        name.name().as_str(),
                        *seg_id,
                    )
                })
                .map(|(k, _)| k.clone())
            {
                Some(victim) => {
                    g.sketches.remove(&victim);
                    g.prune(now, &self.policy);
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
            dist: KeyDistribution::new(seg.lo, seg.hi, self.policy.rate_window_secs),
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
        &self,
    ) -> (
        Vec<(crate::tenant::TenantStreamRef, String, u32, u64)>,
        Vec<(crate::tenant::TenantStreamRef, String)>,
    ) {
        evaluate_state(
            &mut self.state.lock().unwrap(),
            self.clock.monotonic().millis(),
            &self.policy,
            &self.limits,
        )
    }
}

fn evaluate_state(
    g: &mut State,
    now_ms: i64,
    pol: &ScalePolicy,
    lim: &crate::usage::Limits,
) -> (
    Vec<(crate::tenant::TenantStreamRef, String, u32, u64)>,
    Vec<(crate::tenant::TenantStreamRef, String)>,
) {
    let mut out = Vec::new();
    g.prune(now_ms, pol);
    let mut hot_updates: HashMap<crate::tenant::TenantStreamRef, RoutingKeyHash> = HashMap::new();
    let State {
        sketches,
        last_transition_ms: cooldowns,
        ..
    } = &mut *g;
    for ((name, seg_id), sk) in sketches.iter_mut() {
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
                hot_updates
                    .entry(name.clone())
                    .and_modify(|prior| {
                        if k < prior.0 {
                            *prior = RoutingKeyHash(k);
                        }
                    })
                    .or_insert(RoutingKeyHash(k));
            }
            INEFFECTIVE_SPLIT_AVOIDED.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            continue;
        }
        // Cooldown.
        if now_ms
            - cooldowns
                .get(&(name.clone(), sk.epoch.clone()))
                .copied()
                .unwrap_or(i64::MIN / 2)
            < pol.cooldown_secs * 1000
        {
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
    // Publish one aggregate per stream after every segment was observed.
    // Cold segments contribute no key; they cannot erase another segment's
    // hot observation. Multiple hot keys use a stable hash tie-break.
    g.hot_keys = hot_updates;
    for (name, epoch, _, _) in &out {
        g.last_transition_ms
            .insert((name.clone(), epoch.clone()), now_ms);
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
        .filter(|(name, (n, all_cold, epoch))| {
            *n >= 2
                && *all_cold
                && now_ms
                    - g.last_transition_ms
                        .get(&((*name).clone(), epoch.clone()))
                        .copied()
                        .unwrap_or(i64::MIN / 2)
                    >= pol.cooldown_secs * 1000
        })
        .map(|(name, (_, _, epoch))| (name.clone(), epoch))
        .collect();
    let mut merge_candidates = merge_candidates;
    for (name, epoch) in &merge_candidates {
        g.last_transition_ms
            .insert((name.clone(), epoch.clone()), now_ms);
    }
    g.prune(now_ms, pol);
    out.sort_by(|a, b| {
        (a.0.project_id().as_str(), a.0.name().as_str(), &a.1, a.2).cmp(&(
            b.0.project_id().as_str(),
            b.0.name().as_str(),
            &b.1,
            b.2,
        ))
    });
    merge_candidates.sort_by(|a, b| {
        (a.0.project_id().as_str(), a.0.name().as_str(), &a.1).cmp(&(
            b.0.project_id().as_str(),
            b.0.name().as_str(),
            &b.1,
        ))
    });
    (out, merge_candidates)
}

// Transport compatibility entry points contain no transition decisions.
// The same topology owner serves HTTP, append, live reads and autonomous scaling.
#[cfg(test)]
pub(crate) async fn execute_split(
    st: &std::sync::Arc<crate::http::AppState>,
    sref: &crate::tenant::TenantStreamRef,
    seg_id: u32,
    split_at: u64,
) -> bool {
    crate::application::topology::execute_split(&st.topology_service(), sref, seg_id, split_at)
        .await
}

#[cfg(test)]
pub(crate) async fn execute_split_fenced(
    st: &std::sync::Arc<crate::http::AppState>,
    sref: &crate::tenant::TenantStreamRef,
    expect_epoch: &str,
    seg_id: u32,
    split_at: u64,
) -> bool {
    crate::application::topology::execute_split_fenced(
        &st.topology_service(),
        sref,
        expect_epoch,
        seg_id,
        split_at,
    )
    .await
}

#[cfg(test)]
pub(crate) async fn execute_merge(
    st: &std::sync::Arc<crate::http::AppState>,
    sref: &crate::tenant::TenantStreamRef,
    a_id: u32,
    b_id: u32,
) -> bool {
    crate::application::topology::execute_merge(&st.topology_service(), sref, a_id, b_id).await
}

#[cfg(test)]
pub(crate) async fn resume(
    st: &std::sync::Arc<crate::http::AppState>,
    sref: &crate::tenant::TenantStreamRef,
) -> bool {
    crate::application::topology::resume(&st.topology_service(), sref).await
}

pub(crate) use crate::application::topology::close_segment_on_engine;

pub(crate) mod controller;

/// The evaluation loop keeps at most 4096 pending hints and executes at most
/// 16 per turn, serially per incarnation, under a shared 60-second deadline.
pub(crate) fn start(
    st: std::sync::Weak<crate::http::AppState>,
    tasks: &crate::tasks::TaskSupervisor,
) {
    let _ = tasks.spawn(
        "scaler",
        crate::tasks::Policy::Critical,
        move |cancel| async move {
            let Some(initial) = st.upgrade() else {
                return crate::tasks::TaskResult::Done;
            };
            let eval = initial.config.scaler.eval_secs.max(1);
            let mut controller = controller::Controller::new(
                initial.topology_service(),
                initial.runtime.ops.clone(),
                initial.config.scaler.cooldown_secs,
            );
            drop(initial);
            loop {
                tokio::select! {
                    _ = cancel.cancelled() => return crate::tasks::TaskResult::Done,
                    _ = tokio::time::sleep(std::time::Duration::from_secs(eval)) => {}
                }
                let Some(st) = st.upgrade() else {
                    return crate::tasks::TaskResult::Done;
                };
                let (decisions, merges) = st.runtime.scaler.evaluate();
                for (name, epoch, segment, split_at) in decisions {
                    controller.enqueue(controller::Decision::Split(name, epoch, segment, split_at));
                }
                for (name, epoch) in merges {
                    controller.enqueue(controller::Decision::Merge(name, epoch));
                }
                drop(st);
                let report = controller
                    .pass(
                        &cancel,
                        tokio::time::Instant::now() + controller::PASS_DEADLINE,
                    )
                    .await;
                tracing::debug!(
                    attempted = report.attempted,
                    completed = report.completed,
                    deferred = report.deferred,
                    cancelled = report.cancelled,
                    "scaler iteration"
                );
                if report.cancelled {
                    return crate::tasks::TaskResult::Done;
                }
            }
        },
    );
}

impl Scaler {
    pub(crate) fn retire_segments(
        &self,
        stream: &crate::tenant::TenantStreamRef,
        segments: &[u32],
    ) {
        let mut state = self.state.lock().unwrap();
        for segment in segments {
            state.sketches.remove(&(stream.clone(), *segment));
        }
    }

    #[expect(
        clippy::unwrap_used,
        reason = "Scaler::stats_json; a poisoned scaler state may hold a partially updated segment tally; recovering it could report a topology that was never decided"
    )]
    pub(crate) fn stats_json(&self) -> serde_json::Value {
        use std::sync::atomic::Ordering::Relaxed;
        let hot: Vec<String> = self
            .hot_keys_all()
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
            "sketches": self.state.lock().unwrap().sketches.len(),
            "sketch_evictions": SKETCH_EVICTIONS.load(Relaxed),
            "untracked_appends": UNTRACKED_APPENDS.load(Relaxed),
            "hot_keys": hot,
        })
    }
}
#[cfg(test)]
mod tests {
    use super::*;

    fn test_desc(name: &str) -> StreamDesc {
        crate::registry::PersistedDescriptor {
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
        .try_into()
        .expect("valid descriptor fixture")
    }

    #[test]
    fn runtime_scalers_use_owned_monotonic_time() {
        let clock = Arc::new(crate::runtime::ManualClock::at(1000));
        let a = Scaler::new(
            &ScalePolicy::default(),
            &crate::config::AdmissionConfig::default(),
            clock.clone(),
        );
        let b = Scaler::new(
            &ScalePolicy::default(),
            &crate::config::AdmissionConfig::default(),
            clock.clone(),
        );
        let desc = test_desc("owned");
        let seg = desc.resolve_segment("key");
        a.note_append(&desc, &seg, 10, 1);
        assert_eq!(a.state.lock().unwrap().sketches.len(), 1);
        assert!(b.state.lock().unwrap().sketches.is_empty());
        clock.jump_wall(10_000_000);
        a.evaluate();
        assert_eq!(a.state.lock().unwrap().sketches.len(), 1);
        clock.jump_wall(-20_000_000);
        a.evaluate();
        assert_eq!(a.state.lock().unwrap().sketches.len(), 1);
        clock.advance_monotonic(std::time::Duration::from_millis(SKETCH_IDLE_MS as u64));
        a.evaluate();
        assert!(a.state.lock().unwrap().sketches.is_empty());
    }

    fn sketch(epoch: &str, now: i64, hot: bool, key: u8) -> SegSketch {
        let mut dist = KeyDistribution::new(0, u64::MAX, ScalePolicy::default().rate_window_secs);
        dist.note(
            now,
            1,
            [key; 16],
            if hot { 1_000_000_000_000 } else { 1 },
            1,
        );
        SegSketch {
            epoch: epoch.into(),
            dist,
            hot_streak: ScalePolicy::default().hot_evals,
            cold_streak: 0,
            last_fed_ms: now,
        }
    }

    #[test]
    fn all_scaler_state_is_bounded_and_idle_state_expires() {
        let mut s = State::default();
        for i in 0..SKETCH_MAX * 2 {
            let name = test_desc(&format!("churn-{i}")).sref();
            s.last_transition_ms
                .insert((name.clone(), "old".into()), i as i64);
            s.hot_keys.insert(name, RoutingKeyHash([1; 16]));
        }
        s.prune(SKETCH_MAX as i64 * 2, &ScalePolicy::default());
        assert_eq!(s.last_transition_ms.len(), SKETCH_MAX);
        assert!(s.hot_keys.is_empty());
        assert!(s.sketches.len() <= SKETCH_MAX);
        s.prune(10_000_000, &ScalePolicy::default());
        assert!(s.last_transition_ms.is_empty());
    }

    #[test]
    fn recreated_stream_discards_all_old_segment_heat_and_cooldown() {
        let name = test_desc("recreated").sref();
        let mut s = State::default();
        for seg in 0..3 {
            s.sketches
                .insert((name.clone(), seg), sketch("old", 1, true, 1));
        }
        s.last_transition_ms.insert((name.clone(), "old".into()), 1);
        s.hot_keys.insert(name.clone(), RoutingKeyHash([1; 16]));
        s.forget_previous_incarnation(&name, "replacement");
        assert!(s.sketches.is_empty());
        assert!(s.last_transition_ms.is_empty());
        assert!(s.hot_keys.is_empty());
    }

    #[test]
    fn repeated_appends_do_not_scan_other_streams_for_incarnation_cleanup() {
        let scaler = Scaler::new(
            &ScalePolicy::default(),
            &crate::config::AdmissionConfig::default(),
            Arc::new(crate::runtime::ManualClock::at(1000)),
        );
        let desc = test_desc("steady");
        let seg = desc.resolve_segment("key");
        {
            let mut state = scaler.state.lock().unwrap();
            state.tick = 1;
            for i in 0..SKETCH_MAX - 2 {
                state.sketches.insert(
                    (test_desc(&format!("other-{i}")).sref(), 0),
                    sketch(&desc.stream_epoch, 1000, false, 1),
                );
            }
            for segment in [seg.seg_id, seg.seg_id + 1] {
                state.sketches.insert(
                    (desc.sref(), segment),
                    sketch(&desc.stream_epoch, 1000, false, 1),
                );
            }
            state
                .last_transition_ms
                .insert((desc.sref(), desc.stream_epoch.clone()), 1000);
            state.hot_keys.insert(desc.sref(), RoutingKeyHash([1; 16]));
            assert_eq!(state.sketches.len(), SKETCH_MAX);
        }
        for _ in 0..256 {
            scaler.note_append(&desc, &seg, 100, 1);
        }
        assert_eq!(scaler.state.lock().unwrap().incarnation_scan_entries, 0);

        let mut replacement = desc.to_persisted();
        replacement.stream_epoch = crate::crypto::hex(&[42; 16]);
        let replacement = StreamDesc::try_from(replacement).unwrap();
        scaler.note_append(&replacement, &replacement.resolve_segment("key"), 100, 1);
        let state = scaler.state.lock().unwrap();
        assert!(state.incarnation_scan_entries >= SKETCH_MAX);
        assert_eq!(state.sketches.len(), SKETCH_MAX - 1);
        assert_eq!(
            state
                .sketches
                .get(&(desc.sref(), seg.seg_id))
                .unwrap()
                .epoch,
            replacement.stream_epoch
        );
        assert!(!state.sketches.contains_key(&(desc.sref(), seg.seg_id + 1)));
        assert!(state.last_transition_ms.is_empty());
        assert!(state.hot_keys.is_empty());
    }

    #[test]
    fn segment_order_cannot_erase_a_hot_key_or_change_decisions() {
        let name = test_desc("hot-and-cold").sref();
        for order in [[0, 1, 2], [2, 1, 0], [1, 0, 2], [2, 0, 1]] {
            let mut s = State::default();
            for seg in order {
                s.sketches.insert(
                    (name.clone(), seg),
                    sketch("epoch", 1000, seg != 0, seg as u8),
                );
            }
            let (splits, merges) = evaluate_state(
                &mut s,
                1001,
                &ScalePolicy::default(),
                crate::usage::limits(),
            );
            assert!(splits.is_empty());
            assert!(merges.is_empty());
            assert_eq!(s.hot_keys.get(&name), Some(&RoutingKeyHash([1; 16])));
        }
    }

    /// Review finding 8: past SKETCH_MAX, new segments must still be
    /// sketched — the least-recently-fed sketch is evicted (counted),
    /// never a silent refusal to track.
    #[test]
    fn sketch_cap_evicts_instead_of_starving() {
        let scaler = Scaler::new(
            &ScalePolicy::default(),
            &crate::config::AdmissionConfig::default(),
            Arc::new(crate::runtime::SystemClock::default()),
        );
        let over = SKETCH_MAX + 8;
        for i in 0..over {
            let desc = test_desc(&format!("cap-seg-{i}"));
            let seg = desc.resolve_segment("k");
            scaler.note_append(&desc, &seg, 100, 1);
        }
        let newest_key = (test_desc(&format!("cap-seg-{}", over - 1)).sref(), 0);
        let (tracked, has_newest) = {
            let g = scaler.state.lock().unwrap();
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
