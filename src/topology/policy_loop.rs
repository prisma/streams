//! The topology policy's loop (S5): every period, the cell as the fleet tick last read it, the
//! policy's decision (`policy::Policy::decide`), and S4's split entry for a split this instance
//! owns, or S6's merge entry for a merge. It reads no store object of its own: the beats and the
//! fleet documents' times are the tick's reads, cached in `Standing` (S5-PLAN-CHECK §3).
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use futures_util::future::BoxFuture;

use super::model::Snapshot;
use super::policy::{Axis, Decision, Instance, Leaf, Limits, Policy, Usage, View};
use crate::fleet::{FleetRepository, Heartbeat};
use crate::ownership::OwnershipService;
use crate::shard_directory::ShardDirectory;
use crate::tasks::{Cancellation, TaskResult, TaskSupervisor};

/// The heartbeat's `split_refused` while a split waits for an instance (Q4).
pub(crate) const INSTANCE_FULL: &str = "instance_full";

/// The policy's cadence.
pub(crate) const PERIOD: Duration = Duration::from_secs(10);

/// Why S4's split entry did not split: its refusal's reason code (`fleet_settling`,
/// `override_recent` and `topology_busy` for its three reasons to be busy, `not_ring_owner`,
/// `instance_full`, `unavailable`, `not_splittable`, `unknown_prefix`, `rolled_back`,
/// `unfinished`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Refused(pub &'static str);

/// S4's split entry: start the split of a leaf (S4-DESIGN §6). The DST stands a double in.
pub(crate) type SplitFn =
    Arc<dyn Fn(String) -> BoxFuture<'static, Result<(), Refused>> + Send + Sync>;

/// S4's split entry (`topology::ops`, the operator route's own path, S4-DESIGN §6) behind the
/// seam. It answers once the split has started; the split runs on as its own supervised task.
pub(crate) fn split_entry(shards: ShardDirectory) -> SplitFn {
    Arc::new(move |prefix: String| {
        let shards = shards.clone();
        Box::pin(async move {
            let started = shards.topology_ops().split(prefix).await;
            started
                .map(|_| ())
                .map_err(|refused| Refused(refusal_code(&refused)))
        })
    })
}

/// S6's merge entry: start the merge of a parent's two leaves (S6-DESIGN §7), the split entry's
/// shape with the parent's prefix. The DST stands S6's own entry in.
pub(crate) type MergeFn = SplitFn;

/// S6's merge entry (`topology::ops`, the operator merge route's own path, S6-DESIGN §7) behind
/// the seam. It answers once the merge has started; the merge runs on as its own supervised task.
pub(crate) fn merge_entry(shards: ShardDirectory) -> MergeFn {
    Arc::new(move |parent: String| {
        let shards = shards.clone();
        Box::pin(async move {
            let started = shards.topology_ops().merge(parent).await;
            started
                .map(|_| ())
                .map_err(|refused| Refused(merge_refusal_code(&refused)))
        })
    })
}

/// The reason code of S6's refusal, in S4's codes where the two share a reason (`not_mergeable`
/// is the merge's own).
fn merge_refusal_code(refused: &super::merge::Refused) -> &'static str {
    use super::merge::Refused as S6;
    match refused {
        S6::Busy("fleet") => "fleet_settling",
        S6::Busy("override") => "override_recent",
        S6::Busy(_) => "topology_busy",
        S6::NotOwner(_) => NOT_RING_OWNER,
        S6::InstanceFull { .. } => INSTANCE_FULL,
        S6::Unavailable(_) => "unavailable",
        S6::NotMergeable(_) => "not_mergeable",
        S6::UnknownPrefix => "unknown_prefix",
        S6::RolledBack(_) => "rolled_back",
        S6::Unfinished(_) => "unfinished",
    }
}

/// The reason code of S4's refusal.
fn refusal_code(refused: &super::split::Refused) -> &'static str {
    use super::split::Refused as S4;
    match refused {
        S4::Busy("fleet") => "fleet_settling",
        S4::Busy("override") => "override_recent",
        S4::Busy(_) => "topology_busy",
        S4::NotOwner(_) => NOT_RING_OWNER,
        S4::InstanceFull { .. } => INSTANCE_FULL,
        S4::Unavailable(_) => "unavailable",
        S4::NotSplittable(_) => "not_splittable",
        S4::UnknownPrefix => "unknown_prefix",
        S4::RolledBack(_) => "rolled_back",
        S4::Unfinished(_) => "unfinished",
    }
}

/// S4's refusal of a split whose parent another instance holds.
const NOT_RING_OWNER: &str = "not_ring_owner";

/// The hold a refused split answers, by S4's code: the fleet unsettled, a recent override, a live
/// intent, a parent another instance holds; any other refusal holds steady.
fn hold_for(code: &str) -> super::policy::Hold {
    use super::policy::Hold;
    match code {
        "fleet_settling" => Hold::FleetSettling,
        "override_recent" => Hold::OverrideRecent,
        "topology_busy" => Hold::IntentLive,
        NOT_RING_OWNER => Hold::PeerActs,
        _ => Hold::Steady,
    }
}

/// What one pass reads, and through what it acts.
#[derive(Clone)]
pub(crate) struct Cell {
    pub me: String,
    pub fleet: FleetRepository,
    pub shards: ShardDirectory,
    pub ownership: OwnershipService,
    /// The store `topology/intent.json` is read from (the registry's namespace).
    pub store: Arc<dyn object_store::ObjectStore>,
    pub limits: Limits,
    /// The appends axis' safe capacity, per shard.
    pub appends_ceiling: u64,
    /// The RSS shed line (MB): an instance's headroom is what it has left below it; 0 is none.
    pub rss_shed_mb: u64,
    pub split: SplitFn,
    pub merge: MergeFn,
}

/// Start the policy's loop: one pass each `period`.
pub(crate) fn start(cell: Cell, period: Duration, tasks: &TaskSupervisor) {
    let spawned = tasks.spawn(
        "topology-policy",
        crate::tasks::Policy::Noncritical,
        move |cancel| run(cell, period, cancel),
    );
    if let Err(rejected) = spawned {
        tracing::debug!(
            ?rejected,
            "runtime stopping: the topology policy was not started"
        );
    }
}

async fn run(cell: Cell, period: Duration, cancel: Cancellation) -> TaskResult {
    let mut policy = Policy::default();
    let mut seen = Seen::default();
    loop {
        tokio::select! {
            _ = cancel.cancelled() => return TaskResult::Done,
            _ = tokio::time::sleep(period) => {}
        }
        let decision = pass(&cell, &mut policy, &mut seen).await;
        tracing::debug!(?decision, "topology policy");
    }
}

/// One pass: the view, the decision, and the split it starts. A refused split is a hold, or an
/// ask for an instance.
/// The beat publishes what the pass left: a split waiting for an instance, and the shards the
/// ring gives this one.
pub(crate) async fn pass(cell: &Cell, policy: &mut Policy, seen: &mut Seen) -> Decision {
    let live = intent_live(&*cell.store).await;
    let view = view(cell, seen, live);
    let decision = act(cell, &view, policy.decide(&view, &cell.limits)).await;
    let waiting = matches!(decision, Decision::AskInstance { .. }).then_some(INSTANCE_FULL);
    let standing = cell.fleet.standing();
    standing.set_topology(waiting, resident(&view, &cell.me));
    standing.set_topology_decision(decision.reason());
    decision
}

/// `decision`, after the split or merge entry's answer to the operation it names. A refused
/// merge holds, by its refusal's code, and the next pass decides again.
async fn act(cell: &Cell, view: &View, decision: Decision) -> Decision {
    if let Decision::Merge { parent } = &decision {
        return match (cell.merge)(parent.clone()).await {
            Ok(()) => decision,
            Err(refused) => {
                tracing::info!(?refused, parent, "topology merge refused");
                Decision::Hold(hold_for(refused.0))
            }
        };
    }
    let Decision::Split { prefix, .. } = &decision else {
        return decision;
    };
    match (cell.split)(prefix.clone()).await {
        Ok(()) => decision,
        Err(refused) => {
            tracing::info!(?refused, prefix, "topology split refused");
            match refused.0 {
                INSTANCE_FULL => Decision::AskInstance {
                    resident: resident(view, &cell.me),
                    max: cell.limits.shards_per_instance_max,
                },
                code => Decision::Hold(hold_for(code)),
            }
        }
    }
}

fn resident(view: &View, me: &str) -> u32 {
    view.instances
        .iter()
        .find(|instance| instance.id == me)
        .map_or(0, |instance| instance.resident_shards)
}

/// Whether a topology operation is under way: the cell's intent is not terminal, or it cannot
/// be read (fail closed, S4 §4.4).
async fn intent_live(store: &dyn object_store::ObjectStore) -> bool {
    match super::intent::read(store).await {
        Ok(held) => held.is_some_and(|held| !held.intent.is_terminal()),
        Err(error) => {
            tracing::warn!(%error, "topology intent unreadable; every operation holds");
            true
        }
    }
}

/// What this loop has seen of the topology, on the wall clock: when each leaf a split or merge
/// made first appeared, and when the version changed (the last operation's end). A cell's first
/// leaves were born with it, and the version it started with ended no operation it knows of, so
/// a restart only delays a merge.
#[derive(Debug, Default)]
pub(crate) struct Seen {
    born: BTreeMap<String, i64>,
    version: Option<u64>,
    changed_ms: Option<i64>,
}

impl Seen {
    fn observe(&mut self, snapshot: &Snapshot, now_ms: i64) {
        self.born
            .retain(|prefix, _| snapshot.iter().any(|leaf| leaf == prefix));
        let version = snapshot.shards.version;
        if self.version.is_some_and(|seen| seen != version) {
            self.changed_ms = Some(now_ms);
        }
        self.version = Some(version);
    }

    fn born(&mut self, snapshot: &Snapshot, prefix: &str, now_ms: i64) -> i64 {
        let first = snapshot
            .shards
            .leaves
            .get(prefix)
            .and_then(|home| snapshot.shards.homes.get(home))
            .is_none_or(|home| home.born_version <= 1);
        if first {
            return 0;
        }
        *self.born.entry(prefix.to_owned()).or_insert(now_ms)
    }
}

/// The cell as this pass sees it.
fn view(cell: &Cell, seen: &mut Seen, intent_live: bool) -> View {
    let now_ms = crate::shard::now_ms();
    let standing = cell.fleet.standing();
    let stale_ms = u64::try_from(cell.limits.beat_stale_ms).unwrap_or(0);
    let (beats_fresh, beats) = match standing.cached_beats() {
        Some((age_ms, beats)) => (age_ms <= stale_ms, beats),
        None => (false, Arc::from([])),
    };
    let snapshot = cell.shards.prefixes();
    seen.observe(&snapshot, now_ms);
    let by_instance: BTreeMap<&str, &Heartbeat> = beats
        .iter()
        .map(|beat| (beat.instance.as_str(), beat))
        .collect();
    let leaves: Vec<Leaf> = snapshot
        .iter()
        .map(|prefix| {
            let owner = cell
                .ownership
                .effective_owner(prefix)
                .unwrap_or_else(|| cell.me.clone());
            let beat = by_instance.get(owner.as_str()).copied();
            Leaf {
                prefix: prefix.clone(),
                born_ms: seen.born(&snapshot, prefix, now_ms),
                load: beat
                    .map(|beat| load(cell, beat, prefix))
                    .unwrap_or_default(),
                beat_ms: beat.map(|beat| beat.ts_ms),
                owner,
            }
        })
        .collect();
    let instances = instances(cell, &leaves, &beats, now_ms);
    let docs = standing.doc_times();
    // Fleet documents the tick has not read lately are treated as just changed: no topology
    // operation starts on a stale view of the fleet (S4 §10.1 rule 1).
    let docs_fresh = docs.age_ms.is_some_and(|age| age <= stale_ms);
    let unseen = |at: Option<i64>| if docs_fresh { at } else { Some(now_ms) };
    View {
        now_ms,
        me: cell.me.clone(),
        instances,
        split_demand: standing.split_demand(),
        live: live(cell, &beats, now_ms),
        beats_fresh,
        intent_live,
        last_op_end_ms: seen.changed_ms,
        desired_computed_ms: unseen(docs.desired_computed_ms),
        youngest_override_ms: unseen(docs.youngest_override_ms),
        congested: beats
            .iter()
            .any(|beat| beat.refusals > 0 && now_ms - beat.ts_ms <= cell.limits.beat_stale_ms),
        leaves,
    }
}

/// The live instances in `beats`: fresh, not draining, not withdrawn (the members the fleet's
/// planner folds).
fn live(cell: &Cell, beats: &[Heartbeat], now_ms: i64) -> u32 {
    let live = beats.iter().filter(|beat| {
        now_ms - beat.ts_ms <= cell.limits.beat_stale_ms
            && !beat.draining
            && beat.withdrawn.is_none()
    });
    u32::try_from(live.count()).unwrap_or(u32::MAX)
}

/// `prefix`'s load as its owner's beat reports it, by axis against its safe capacity.
fn load(cell: &Cell, beat: &Heartbeat, prefix: &str) -> BTreeMap<Axis, Usage> {
    let Some(shard) = beat.shard_loads.iter().find(|shard| shard.prefix == prefix) else {
        return BTreeMap::new();
    };
    let usage = |used: u64, safe: u64| Usage {
        used,
        safe,
        top_entity: 0,
    };
    BTreeMap::from([
        (
            Axis::Appends,
            Usage {
                used: shard.appends_per_s,
                safe: cell.appends_ceiling,
                top_entity: shard.top_stream_appends_per_s,
            },
        ),
        (
            Axis::Streams,
            usage(shard.resident_streams, shard.handle_max_resident),
        ),
        (Axis::Committer, usage(shard.committer_busy_pct, 100)),
    ])
}

/// Each beating instance's room: the leaves the ring gives it, and its RSS below the shed line.
fn instances(cell: &Cell, leaves: &[Leaf], beats: &[Heartbeat], now_ms: i64) -> Vec<Instance> {
    beats
        .iter()
        .map(|beat| {
            let held = leaves
                .iter()
                .filter(|leaf| leaf.owner == beat.instance)
                .count();
            let below_shed = cell.rss_shed_mb == 0 || beat.rss_mb < cell.rss_shed_mb as f64;
            Instance {
                id: beat.instance.clone(),
                resident_shards: u32::try_from(held).unwrap_or(u32::MAX),
                accepting: now_ms - beat.ts_ms <= cell.limits.beat_stale_ms
                    && !beat.draining
                    && beat.withdrawn.is_none()
                    && below_shed,
            }
        })
        .collect()
}

/// The policy's limits from the configuration (rev 4 §4.3, Q4, S4 §10.1).
pub(crate) fn limits(config: &crate::config::ServerConfig) -> Limits {
    let cli = &config.cli;
    let ms = |secs: u64| i64::try_from(secs.saturating_mul(1_000)).unwrap_or(i64::MAX);
    Limits {
        split_line_pct: cli.shard_split_line_pct,
        split_sustain_ms: ms(cli.shard_split_sustain_secs),
        emergency_line_pct: cli.shard_emergency_line_pct,
        emergency_sustain_ms: ms(cli.shard_emergency_sustain_secs),
        merge_line_pct: cli.shard_merge_line_pct,
        merge_sustain_ms: ms(cli.shard_merge_sustain_secs),
        reversal_min_ms: ms(cli.shard_split_reversal_min_secs),
        cooldown_ms: ms(cli.shard_topology_cooldown_secs),
        beat_stale_ms: ms(cli.shard_beat_stale_secs),
        override_cooldown_ms: ms(config.fleet.rebalance_move_cooldown_secs),
        shards_per_instance_max: cli.shards_per_instance_max,
        fleet_max: u32::try_from(cli.fleet_max).unwrap_or(u32::MAX),
    }
}

#[cfg(test)]
mod tests {
    use super::{hold_for, intent_live, merge_refusal_code, refusal_code};
    use crate::topology::policy::Hold;
    use crate::topology::split::Refused as S4;

    /// S4's refusals answer the policy's holds: its three reasons for being busy apart, a parent
    /// another instance holds, and the rest steady.
    #[test]
    fn s4s_refusals_become_the_policys_holds() {
        let held = |refused: S4| hold_for(refusal_code(&refused));
        assert_eq!(held(S4::Busy("fleet")), Hold::FleetSettling);
        assert_eq!(held(S4::Busy("override")), Hold::OverrideRecent);
        assert_eq!(held(S4::Busy("intent")), Hold::IntentLive);
        assert_eq!(held(S4::NotOwner("streams-2".into())), Hold::PeerActs);
        for steady in [
            S4::Unavailable(String::new()),
            S4::NotSplittable(String::new()),
            S4::UnknownPrefix,
            S4::RolledBack(String::new()),
            S4::Unfinished(String::new()),
        ] {
            assert_eq!(held(steady), Hold::Steady);
        }
        assert_eq!(
            refusal_code(&S4::InstanceFull {
                resident: 7,
                max: 7
            }),
            "instance_full"
        );
    }

    /// S6's merge refusals answer the same holds: busy for its three reasons apart, a half another
    /// instance holds, and the rest (instance_full and not_mergeable among them) steady, so the
    /// next pass decides again.
    #[test]
    fn s6s_merge_refusals_become_the_policys_holds() {
        use crate::topology::merge::Refused as S6;
        let held = |refused: S6| hold_for(merge_refusal_code(&refused));
        assert_eq!(held(S6::Busy("fleet")), Hold::FleetSettling);
        assert_eq!(held(S6::Busy("override")), Hold::OverrideRecent);
        assert_eq!(held(S6::Busy("intent")), Hold::IntentLive);
        assert_eq!(held(S6::NotOwner("streams-2".into())), Hold::PeerActs);
        for steady in [
            S6::InstanceFull {
                resident: 8,
                max: 7,
            },
            S6::NotMergeable(String::new()),
            S6::Unavailable(String::new()),
            S6::UnknownPrefix,
            S6::RolledBack(String::new()),
            S6::Unfinished(String::new()),
        ] {
            assert_eq!(held(steady), Hold::Steady);
        }
        assert_eq!(
            merge_refusal_code(&S6::NotMergeable(String::new())),
            "not_mergeable"
        );
        assert_eq!(
            merge_refusal_code(&S6::InstanceFull {
                resident: 8,
                max: 7
            }),
            "instance_full"
        );
    }

    use crate::topology::intent::{Intent, Outcome, State};
    use object_store::{ObjectStoreExt, PutPayload, memory::InMemory, path::Path};

    fn intent(state: State) -> Intent {
        let outcome = (state != State::Preparing).then(|| Outcome {
            result: "published".into(),
            reason: String::new(),
        });
        Intent {
            kind: "split".into(),
            op_id: "op-1".into(),
            sources: vec![(String::new(), 0)],
            children: vec![1, 2],
            version_seen: 1,
            state,
            lease_owner: "streams-1".into(),
            lease_epoch: 1,
            lease_until_ms: 0,
            started_ms: 0,
            outcome,
        }
    }

    /// A Preparing intent holds every operation, a terminal one none, and an intent that does
    /// not decode holds them all (fail closed); a cell that never wrote one holds none.
    #[tokio::test(crate = "crate::test_rt")]
    async fn only_a_preparing_or_unreadable_intent_is_live() {
        let store = InMemory::new();
        assert!(!intent_live(&store).await, "no intent");
        let path = Path::from("topology/intent.json");
        for (state, live) in [
            (State::Preparing, true),
            (State::Reclaimed, false),
            (State::RolledBack, false),
        ] {
            let body = serde_json::to_vec(&intent(state)).unwrap();
            store.put(&path, PutPayload::from(body)).await.unwrap();
            assert_eq!(intent_live(&store).await, live, "{state:?}");
        }
        store
            .put(&path, PutPayload::from_static(b"{not json"))
            .await
            .unwrap();
        assert!(intent_live(&store).await, "a corrupt intent fails closed");
    }
}
