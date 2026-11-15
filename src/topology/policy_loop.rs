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

/// The heartbeat's `split_refused` while a split ready on this instance waits for the fleet to
/// settle (#261 amendment): the fleet does not shrink while a member reports it.
pub(crate) const FLEET_SETTLING: &str = "fleet_settling";

/// The policy's cadence.
pub(crate) const PERIOD: Duration = Duration::from_secs(10);

/// Why S4's split entry did not split: its refusal's reason code (`fleet_settling` and
/// `topology_busy` for its two reasons to be busy, `not_ring_owner`,
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
        S6::Busy(_) => "topology_busy",
        S6::NotOwner(_) => NOT_RING_OWNER,
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

/// The hold a refused split answers, by S4's code: the fleet unsettled, a live intent, a parent
/// another instance holds; any other refusal holds steady (an override is no refusal of its own:
/// S5's own rule holds `override_recent` on the overrides it reads).
fn hold_for(code: &str) -> super::policy::Hold {
    use super::policy::Hold;
    match code {
        "fleet_settling" => Hold::FleetSettling,
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
/// The beat publishes what the pass left: a split waiting (`split_refused`), and the shards the
/// ring gives this one.
pub(crate) async fn pass(cell: &Cell, policy: &mut Policy, seen: &mut Seen) -> Decision {
    let live = intent_live(&*cell.store).await;
    let view = view(cell, seen, live);
    let decided = policy.decide(&view, &cell.limits);
    let decision = act(cell, &view, decided).await;
    let waiting = split_refused(&decision, policy.decided_split());
    let standing = cell.fleet.standing();
    standing.set_topology(waiting, resident(&view, &cell.me));
    standing.set_topology_decision(decision.reason());
    decision
}

/// What a split waits for, for the beat's `split_refused`: an instance (an ask), or the fleet's
/// settle when a ready split of this instance (`split`, the policy's candidate) is held by the
/// policy's gate or S4's entry for a young count or override. A merge held so waits for nothing
/// the fleet must keep: shrinks are finite.
fn split_refused(decision: &Decision, split: bool) -> Option<&'static str> {
    use super::policy::Hold;
    match decision {
        Decision::AskInstance { .. } => Some(INSTANCE_FULL),
        Decision::Hold(Hold::FleetSettling | Hold::OverrideRecent) if split => Some(FLEET_SETTLING),
        _ => None,
    }
}

/// `decision`, after the split or merge entry's answer to the operation it names. A refused
/// merge holds, by its refusal's code, and the next pass decides again.
async fn act(cell: &Cell, view: &View, decision: Decision) -> Decision {
    if let Decision::Merge { parent } = &decision {
        return match (cell.merge)(parent.clone()).await {
            Ok(()) => decision,
            Err(Refused(NOT_RING_OWNER)) => colocate(cell, view, parent).await,
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

/// A merge refused because another instance holds a half (S6-DESIGN §7.2 step 0, #261 B1): both
/// halves move, in one overrides CAS, onto the first instance with room for them and the merged
/// leaf (`colocation_target`): this instance (the merge's actor), the instance holding `…1`, then
/// any other accepting instance, fewest shards first. Whichever then owns `…0` is the actor and
/// merges once the move's cooldown has passed (`override_recent` meanwhile). With no instance that
/// has room it asks the fleet for one, as a split does (the beat's `instance_full`; the count grows
/// within its clamp), and the next pass decides again. A lost CAS holds steady.
async fn colocate(cell: &Cell, view: &View, parent: &str) -> Decision {
    use super::policy::Hold;
    let halves = [format!("{parent}0"), format!("{parent}1")];
    let max = cell.limits.shards_per_instance_max;
    let Some(target) = colocation_target(view, max, &cell.me, &halves[1]) else {
        tracing::info!(
            parent,
            "topology merge: no instance has room for its halves"
        );
        return Decision::AskInstance {
            resident: resident(view, &cell.me),
            max,
        };
    };
    if crate::fleet::colocate::colocate(
        &cell.fleet,
        &cell.ownership,
        &*cell.store,
        &halves,
        &target,
    )
    .await
    {
        tracing::info!(parent, %target, "topology merge: moving both halves onto one instance first");
        return Decision::Hold(Hold::OverrideRecent);
    }
    tracing::info!(parent, "topology merge: the overrides CAS was lost");
    Decision::Hold(Hold::Steady)
}

/// Where a pair split across instances goes before its merge (see `colocate`): the first of this
/// instance `me`, the owner of the half `one` (`…1`) and every other accepting instance, fewest
/// shards first, with room under `max` for what it would hold until the merge publishes. The
/// holder of a half takes the other half and the merged leaf (its shards + 2); any other instance
/// takes both halves and the merged leaf (+ 3). The partner and the others must be accepting.
fn colocation_target(view: &View, max: u32, me: &str, one: &str) -> Option<String> {
    let partner = view
        .leaves
        .iter()
        .find(|leaf| leaf.prefix == one)
        .map(|leaf| leaf.owner.clone())
        .filter(|owner| owner != me);
    let accepting = |id: &str| view.instances.iter().any(|i| i.id == id && i.accepting);
    let room = |id: &str, takes: u32| resident(view, id).saturating_add(takes) <= max;
    if room(me, 2) {
        return Some(me.to_string());
    }
    if let Some(partner) = partner.filter(|p| accepting(p) && room(p, 2)) {
        return Some(partner);
    }
    let mut others: Vec<&Instance> = view
        .instances
        .iter()
        .filter(|i| {
            i.accepting
                && i.id != me
                && view
                    .leaves
                    .iter()
                    .all(|l| l.prefix != one || l.owner != i.id)
        })
        .collect();
    others.sort_by_key(|i| (i.resident_shards, i.id.clone()));
    others
        .into_iter()
        .find(|i| room(&i.id, 3))
        .map(|i| i.id.clone())
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
    // mt-lint: allow(name-keyed-map): shard prefix (a leaf) -> when it first appeared
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
                    .map(|beat| load(cell.appends_ceiling, beat, prefix))
                    .unwrap_or_default(),
                beat_ms: beat.map(|beat| beat.ts_ms),
                owner,
            }
        })
        .collect();
    let (stale, shed) = (cell.limits.beat_stale_ms, cell.rss_shed_mb);
    let instances = instances(&leaves, &beats, now_ms, stale, shed);
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
        live: live(&beats, now_ms, cell.limits.beat_stale_ms),
        beats_fresh,
        intent_live,
        last_op_end_ms: seen.changed_ms,
        desired_computed_ms: unseen(docs.desired_computed_ms),
        youngest_override_ms: unseen(docs.youngest_override_ms),
        congested: congested(&beats, now_ms, cell.limits.beat_stale_ms),
        leaves,
    }
}

/// Whether a beat no older than `stale_ms` counted a refusal more instances could relieve.
fn congested(beats: &[Heartbeat], now_ms: i64, stale_ms: i64) -> bool {
    beats
        .iter()
        .any(|beat| beat.refusals > 0 && now_ms - beat.ts_ms <= stale_ms)
}

/// Whether `beat` is from a member the fleet's planner folds: no older than `stale_ms`, not
/// draining, not withdrawn.
fn member(beat: &Heartbeat, now_ms: i64, stale_ms: i64) -> bool {
    now_ms - beat.ts_ms <= stale_ms && !beat.draining && beat.withdrawn.is_none()
}

/// The live instances in `beats`: the members (`member`).
fn live(beats: &[Heartbeat], now_ms: i64, stale_ms: i64) -> u32 {
    let live = beats.iter().filter(|beat| member(beat, now_ms, stale_ms));
    u32::try_from(live.count()).unwrap_or(u32::MAX)
}

/// `prefix`'s load as its owner's beat reports it, by axis against its safe capacity (appends
/// against `appends_ceiling`).
fn load(appends_ceiling: u64, beat: &Heartbeat, prefix: &str) -> BTreeMap<Axis, Usage> {
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
                safe: appends_ceiling,
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

/// Each beating instance's room: the leaves the ring gives it, and whether it accepts a shard:
/// a member (`member`) whose RSS is below the shed line `rss_shed_mb` (0: none).
fn instances(
    leaves: &[Leaf],
    beats: &[Heartbeat],
    now_ms: i64,
    stale_ms: i64,
    rss_shed_mb: u64,
) -> Vec<Instance> {
    beats
        .iter()
        .map(|beat| {
            let held = leaves
                .iter()
                .filter(|leaf| leaf.owner == beat.instance)
                .count();
            let below_shed = rss_shed_mb == 0 || beat.rss_mb < rss_shed_mb as f64;
            Instance {
                id: beat.instance.clone(),
                resident_shards: u32::try_from(held).unwrap_or(u32::MAX),
                accepting: member(beat, now_ms, stale_ms) && below_shed,
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
    use super::{
        FLEET_SETTLING, INSTANCE_FULL, NOT_RING_OWNER, hold_for, intent_live, merge_refusal_code,
        refusal_code, split_refused,
    };
    use crate::topology::policy::{Decision, Hold};
    use crate::topology::split::Refused as S4;

    /// S4's refusals answer the policy's holds: its two reasons for being busy apart, a parent
    /// another instance holds, and the rest steady.
    #[test]
    fn s4s_refusals_become_the_policys_holds() {
        let held = |refused: S4| hold_for(refusal_code(&refused));
        assert_eq!(held(S4::Busy("fleet")), Hold::FleetSettling);
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

    /// The beat's `split_refused` (#261 amendment): an ask is `instance_full`, a split held for
    /// the fleet's settle or a young override is `fleet_settling` whether the policy's gate or
    /// S4's refusal held it, and a merge held so, or any other hold, publishes nothing.
    #[test]
    fn a_split_held_for_the_fleets_settle_tells_the_fleet_and_a_merge_does_not() {
        let ask = Decision::AskInstance {
            resident: 3,
            max: 3,
        };
        assert_eq!(split_refused(&ask, true), Some(INSTANCE_FULL));
        assert_eq!(split_refused(&ask, false), Some(INSTANCE_FULL));
        let settling = [Hold::FleetSettling, Hold::OverrideRecent];
        for hold in settling {
            assert_eq!(
                split_refused(&Decision::Hold(hold), true),
                Some(FLEET_SETTLING)
            );
            assert_eq!(split_refused(&Decision::Hold(hold), false), None);
        }
        for hold in [
            Hold::Steady,
            Hold::PeerActs,
            Hold::IntentLive,
            Hold::Cooldown,
            Hold::HotEntity,
            Hold::MaxDepth,
            Hold::TopologyAtMax,
            Hold::Congested,
            Hold::BeatsStale,
        ] {
            assert_eq!(split_refused(&Decision::Hold(hold), true), None, "{hold:?}");
        }
        let started = Decision::Merge {
            parent: String::new(),
        };
        assert_eq!(split_refused(&started, true), None);
        assert_eq!(
            hold_for(refusal_code(&S4::Busy("fleet"))),
            Hold::FleetSettling
        );
    }

    /// S6's merge refusals answer the same holds: busy for its two reasons apart, and the rest
    /// (not_mergeable among them) steady, so the next pass decides again; the shard ceiling refuses
    /// no merge (#251 R5). A half
    /// another instance holds is the code `act` sends to the co-location.
    #[test]
    fn s6s_merge_refusals_become_the_policys_holds() {
        use crate::topology::merge::Refused as S6;
        let held = |refused: S6| hold_for(merge_refusal_code(&refused));
        assert_eq!(held(S6::Busy("fleet")), Hold::FleetSettling);
        assert_eq!(held(S6::Busy("intent")), Hold::IntentLive);
        // `act` routes this code to the co-location, never to a hold.
        assert_eq!(
            merge_refusal_code(&S6::NotOwner("streams-2".into())),
            NOT_RING_OWNER
        );
        for steady in [
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
    }

    /// A split pair's co-location target (`colocation_target`): the actor when it has room for the
    /// other half and the merged leaf (+2), else the partner holding `…1` (+2) when it accepts,
    /// else the accepting instance with the fewest shards that has room for both halves and the
    /// merged leaf (+3); none when no instance has room.
    #[test]
    fn a_split_pairs_halves_go_to_the_first_instance_with_room() {
        use crate::topology::policy::{Instance, Leaf, View};
        let instance = |id: &str, resident_shards: u32, accepting: bool| Instance {
            id: id.into(),
            resident_shards,
            accepting,
        };
        let leaf = |prefix: &str, owner: &str| Leaf {
            prefix: prefix.into(),
            born_ms: 0,
            owner: owner.into(),
            beat_ms: None,
            load: std::collections::BTreeMap::new(),
        };
        let view = |a: u32, b: u32, c: Option<(u32, bool)>, b_accepting: bool| View {
            now_ms: 0,
            me: "a".into(),
            instances: [instance("a", a, true), instance("b", b, b_accepting)]
                .into_iter()
                .chain(c.map(|(n, ok)| instance("c", n, ok)))
                .collect(),
            leaves: vec![leaf("00", "a"), leaf("01", "b")],
            split_demand: 0,
            live: 3,
            beats_fresh: true,
            intent_live: false,
            last_op_end_ms: None,
            desired_computed_ms: None,
            youngest_override_ms: None,
            congested: false,
        };
        let target = |v: &View| super::colocation_target(v, 3, "a", "01");
        assert_eq!(
            target(&view(1, 3, None, true)).as_deref(),
            Some("a"),
            "the actor has room"
        );
        assert_eq!(
            target(&view(2, 1, None, true)).as_deref(),
            Some("b"),
            "the partner has room"
        );
        assert_eq!(
            target(&view(2, 1, None, false)),
            None,
            "a partner that does not accept"
        );
        assert_eq!(
            target(&view(2, 2, Some((0, true)), true)).as_deref(),
            Some("c"),
            "a third with +3"
        );
        assert_eq!(
            target(&view(2, 2, Some((1, true)), true)),
            None,
            "a third with 1 + 3 > 3"
        );
        assert_eq!(
            target(&view(2, 2, Some((0, false)), true)),
            None,
            "a third that does not accept"
        );
        let mut holds_another = view(2, 2, Some((0, true)), true);
        holds_another.leaves.push(leaf("1", "c"));
        assert_eq!(
            target(&holds_another).as_deref(),
            Some("c"),
            "a third holding a leaf other than the half"
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

    use super::{Seen, congested, instances, live, load, member};
    use crate::fleet::Heartbeat;
    use crate::topology::model::{Snapshot, Topology};
    use crate::topology::policy::{Axis, Usage};

    fn snapshot(topology: &Topology) -> Snapshot {
        Snapshot {
            shards: topology.shards.clone(),
            etag: None,
            installed_at: None,
        }
    }

    fn split(topology: &Topology, prefix: &str) -> Topology {
        let (reserved, homes) = topology.reserve_homes();
        reserved.split(prefix, homes, "op").unwrap()
    }

    /// `Seen` dates an operation's end from the first pass that saw the version change, and a
    /// leaf an operation made from the first pass that saw it, kept until the leaf is gone; a
    /// cell's first leaves were born with it (0), and the version it was first seen at ended
    /// nothing.
    #[test]
    fn seen_dates_a_leaf_from_its_first_sight_and_an_operation_from_its_version_change() {
        let v1 = crate::config::validation::InitialShards::new(2)
            .unwrap()
            .topology(1 << 20);
        let v2 = split(&v1, "0");
        let v3 = split(&v2, "00");
        let (s1, s2, s3) = (snapshot(&v1), snapshot(&v2), snapshot(&v3));
        let mut seen = Seen::default();
        seen.observe(&s1, 10);
        assert_eq!(seen.changed_ms, None, "a first sight ends no operation");
        assert_eq!(
            seen.born(&s1, "0", 10),
            0,
            "a first leaf is born with the cell"
        );
        seen.observe(&s1, 20);
        assert_eq!(seen.changed_ms, None, "the same version ends no operation");
        seen.observe(&s2, 30);
        assert_eq!(seen.changed_ms, Some(30), "a new version ends an operation");
        assert_eq!(
            seen.born(&s2, "00", 30),
            30,
            "a split's leaf at first sight"
        );
        assert_eq!(seen.born(&s2, "00", 40), 30, "its first sight is kept");
        assert_eq!(seen.born(&s2, "01", 40), 40);
        assert_eq!(
            seen.born(&s2, "1", 40),
            0,
            "a first leaf stays born with the cell"
        );
        seen.observe(&s2, 50);
        assert_eq!(seen.changed_ms, Some(30), "a re-read of the version");
        seen.observe(&s3, 60);
        assert_eq!(seen.changed_ms, Some(60));
        let kept: Vec<&String> = seen.born.keys().collect();
        assert_eq!(kept, ["01"], "the split leaf's first sight is dropped");
    }

    /// A beat at `ts_ms` from `instance` holding RSS `rss_mb`, with `extra` JSON fields.
    fn beat(instance: &str, ts_ms: i64, rss_mb: f64, extra: &str) -> Heartbeat {
        let document = format!(
            r#"{{"instance":"{instance}","ts_ms":{ts_ms},"rps":0.0,"rss_mb":{rss_mb:?},"owned_shards":[],"draining":false{extra}}}"#
        );
        serde_json::from_str(&document).unwrap()
    }

    /// `instance`'s fresh beat, draining.
    fn draining(instance: &str, ts_ms: i64) -> Heartbeat {
        let mut beat = beat(instance, ts_ms, 0.0, "");
        beat.draining = true;
        beat
    }

    const NOW: i64 = 10_000;
    const STALE: i64 = 1_000;

    /// A member is a beat no older than the stale bound, not draining and not withdrawn; `live`
    /// counts the members.
    #[test]
    fn a_member_is_fresh_not_draining_and_not_withdrawn() {
        let at_bound = beat("a", NOW - STALE, 0.0, "");
        let past = beat("b", NOW - STALE - 1, 0.0, "");
        let draining = draining("c", NOW);
        let withdrawn = beat("d", NOW, 0.0, r#","withdrawn":"scale-in""#);
        let fresh = beat("e", NOW, 0.0, "");
        assert!(member(&at_bound, NOW, STALE), "a beat at the stale bound");
        assert!(!member(&past, NOW, STALE), "a beat past it");
        assert!(!member(&draining, NOW, STALE), "a draining instance");
        assert!(!member(&withdrawn, NOW, STALE), "a withdrawn instance");
        let beats = [at_bound, past, draining, withdrawn, fresh];
        assert_eq!(live(&beats, NOW, STALE), 2);
    }

    /// The cell is congested while a beat within the stale bound counted a refusal.
    #[test]
    fn only_a_fresh_beats_refusals_congest_the_cell() {
        let refusing = |ts_ms: i64, n: u64| beat("a", ts_ms, 0.0, &format!(r#","refusals":{n}"#));
        assert!(congested(&[refusing(NOW - STALE, 3)], NOW, STALE));
        assert!(!congested(&[refusing(NOW, 0)], NOW, STALE), "no refusal");
        assert!(
            !congested(&[refusing(NOW - STALE - 1, 3)], NOW, STALE),
            "a stale beat's"
        );
        assert!(!congested(&[], NOW, STALE));
    }

    /// An instance's room is the leaves it owns; it accepts a shard as a member whose RSS is below
    /// the shed line, and with no shed line (0) at any RSS.
    #[test]
    fn an_instance_accepts_as_a_member_below_its_shed_line() {
        use crate::topology::policy::{Instance, Leaf};
        let leaf = |prefix: &str, owner: &str| Leaf {
            prefix: prefix.into(),
            born_ms: 0,
            owner: owner.into(),
            beat_ms: None,
            load: std::collections::BTreeMap::new(),
        };
        let leaves = [leaf("00", "a"), leaf("01", "a"), leaf("1", "b")];
        let beats = [
            beat("a", NOW, 499.0, ""),
            beat("b", NOW, 500.0, ""),
            beat("c", NOW - STALE - 1, 0.0, ""),
            draining("d", NOW),
        ];
        let instance = |id: &str, resident_shards: u32, accepting: bool| Instance {
            id: id.into(),
            resident_shards,
            accepting,
        };
        assert_eq!(
            instances(&leaves, &beats, NOW, STALE, 500),
            [
                instance("a", 2, true),
                instance("b", 1, false),
                instance("c", 0, false),
                instance("d", 0, false),
            ]
        );
        let accepting: Vec<bool> = instances(&leaves, &beats, NOW, STALE, 0)
            .into_iter()
            .map(|i| i.accepting)
            .collect();
        assert_eq!(accepting, [true, true, false, false], "no shed line");
    }

    /// A leaf's load is its owner's report for that prefix, by axis: appends against the cell's
    /// ceiling with the busiest stream's share, streams against the shard's resident limit, the
    /// committer against 100%; nothing for a prefix the beat does not report.
    #[test]
    fn a_leafs_load_is_its_owners_report_for_its_prefix() {
        let loads = r#","shard_loads":[{"prefix":"00","appends_per_s":1},{"prefix":"01","appends_per_s":7,"committer_busy_pct":30,"resident_streams":5,"handle_max_resident":9,"top_stream_appends_per_s":4}]"#;
        let beat = beat("a", NOW, 0.0, loads);
        let usage = |used: u64, safe: u64, top_entity: u64| Usage {
            used,
            safe,
            top_entity,
        };
        assert_eq!(
            load(100, &beat, "01"),
            std::collections::BTreeMap::from([
                (Axis::Appends, usage(7, 100, 4)),
                (Axis::Streams, usage(5, 9, 0)),
                (Axis::Committer, usage(30, 100, 0)),
            ])
        );
        assert!(load(100, &beat, "1").is_empty(), "an unreported prefix");
    }
}
