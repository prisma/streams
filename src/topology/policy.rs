//! The topology policy (SCALING-DESIGN rev 4 §4.3, S5): when a leaf splits and when two
//! siblings merge, decided over one tick's view of the cell's leaves, on the tick's clock,
//! like `fleet::planning::Demand::plan`. Pure: the policy loop reads the beats, the fleet's
//! desired state and the intent, and acts on the decision.
//!
//! Each tick yields one decision for the instance that runs it. The cell's leaves and their
//! loads are the same on every instance, up to beat timing, so every instance picks the same
//! candidate and only its owner acts; the intent serialises what timing leaves over.
use std::collections::BTreeMap;

/// One axis of a leaf's load. Stored payload backlog is no axis: segment bytes are not load.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum Axis {
    Appends,
    /// Resident handles against the shard's `HANDLE_MAX_RESIDENT`.
    Streams,
    /// The committer's serial busy percent.
    Committer,
}

/// A leaf's use of one axis against the axis's measured safe capacity, and the largest share
/// any one stream or queue carries of that use. An axis with no measured capacity is no
/// constraint.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Usage {
    pub used: u64,
    pub safe: u64,
    pub top_entity: u64,
}

/// One leaf of the cell's topology as the tick sees it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Leaf {
    /// The bit-prefix string `registry::shard_for_hash` routes by; its length is its depth.
    pub prefix: String,
    /// When the split that made it published; a cell's first leaves are born with it.
    pub born_ms: i64,
    pub owner: String,
    /// The owner's latest beat carrying this leaf's load; `None` when no beat carried it.
    pub beat_ms: Option<i64>,
    pub load: BTreeMap<Axis, Usage>,
}

/// An instance's room for shards, from its latest beat.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Instance {
    pub id: String,
    pub resident_shards: u32,
    /// Fresh, not draining, not withdrawn and below its RSS shed line: one that may take a
    /// shard.
    pub accepting: bool,
}

/// One tick's view of the cell.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct View {
    pub now_ms: i64,
    /// The instance running the policy.
    pub me: String,
    pub leaves: Vec<Leaf>,
    pub instances: Vec<Instance>,
    /// The fleet's demand for instances past the shards (`planning::Need::split_demand`) on
    /// the latest fold: the fleet axis.
    pub split_demand: u64,
    /// The live instances: beating, not draining, not withdrawn. At `Limits::fleet_max` no
    /// instance can join, so a split never asks for one.
    pub live: u32,
    /// The fleet tick's cached beats are no older than the stale age. Stale, they are stale
    /// beats from every owner: merges are blocked, splits are not.
    pub beats_fresh: bool,
    /// A topology intent is non-terminal.
    pub intent_live: bool,
    /// When the last topology operation ended.
    pub last_op_end_ms: Option<i64>,
    /// `fleet/desired.json`'s `computed_at_ms`; `None` when the fleet has published none.
    pub desired_computed_ms: Option<i64>,
    /// When the youngest ownership override anywhere on the cell was made.
    pub youngest_override_ms: Option<i64>,
    /// A `503 shard_busy`, a queued-bytes refusal or a shed anywhere on the cell in the window.
    pub congested: bool,
}

/// The policy's thresholds and its per-instance ceiling (rev 4 §4.3's table; Q4).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Limits {
    pub split_line_pct: u64,
    pub split_sustain_ms: i64,
    pub emergency_line_pct: u64,
    pub emergency_sustain_ms: i64,
    pub merge_line_pct: u64,
    pub merge_sustain_ms: i64,
    pub reversal_min_ms: i64,
    pub cooldown_ms: i64,
    pub beat_stale_ms: i64,
    pub override_cooldown_ms: i64,
    pub shards_per_instance_max: u32,
    /// `FLEET_MAX`: the most instances the fleet grows to.
    pub fleet_max: u32,
}

/// The deepest prefix there is: a 24-bit leaf never splits.
const MAX_DEPTH: usize = 24;
/// The most leaves a cell has: a split that would make more is refused.
const MAX_LEAVES: usize = 64;

/// What made a split ready: the 70% line (or the fleet axis), or the 90% line. Rev 4's other
/// emergency, predicted exhaustion before an instance can join, waits for stage 2's evidence.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum SplitKind {
    Normal,
    Emergency,
}

/// Why a tick starts nothing.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Hold {
    Steady,
    PeerActs,
    IntentLive,
    Cooldown,
    FleetSettling,
    OverrideRecent,
    HotEntity,
    MaxDepth,
    TopologyAtMax,
    Congested,
    BeatsStale,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Decision {
    /// Split `prefix`; `doubling` when it is one step of splitting every leaf, and
    /// `fleet_axis` the fleet's demand when that axis made the split ready (0 otherwise).
    Split {
        prefix: String,
        kind: SplitKind,
        doubling: bool,
        fleet_axis: u64,
    },
    /// Merge the leaves `parent + "0"` and `parent + "1"`.
    Merge {
        parent: String,
    },
    /// A split waits for an instance: the actor cannot hold two children beside the parent,
    /// or no other instance can take one.
    AskInstance {
        resident: u32,
        max: u32,
    },
    Hold(Hold),
}

/// A leaf's sustain starts: since when it has been over each split line on fresh beats.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct Sustain {
    over: Option<i64>,
    emergency: Option<i64>,
}

/// How a split goes: what made it ready, and whether it is a doubling's step.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Plan {
    kind: SplitKind,
    doubling: bool,
    fleet_axis: u64,
}

/// A doubling in progress: how it started, and the leaves it still splits, in order.
#[derive(Debug)]
struct Doubling {
    plan: Plan,
    remaining: Vec<String>,
}

/// What the policy carries between ticks.
#[derive(Debug, Default)]
pub(crate) struct Policy {
    // mt-lint: allow(name-keyed-map): shard prefix (a leaf) -> its split sustain
    leaves: BTreeMap<String, Sustain>,
    /// Since when each sibling pair, by its parent's prefix, has been quiet on fresh beats.
    // mt-lint: allow(name-keyed-map): shard prefix (a sibling pair's parent) -> quiet since
    quiet: BTreeMap<String, i64>,
    /// Since when every fresh fold has shown the fleet's demand past the shards: the cell's
    /// sustain, which no topology operation restarts.
    fleet: Option<i64>,
    doubling: Option<Doubling>,
    /// The last decision's candidate was a split: its hold is a split's (#261 amendment).
    split: bool,
}

/// What a tick would start, before the gates.
enum Candidate<'a> {
    Split(&'a Leaf, Plan),
    /// The sibling pair under `parent`, acted on by the owner of its `…0` leaf.
    Merge(String, &'a Leaf),
}

impl Policy {
    /// The decision for `view`, advancing the sustains to its clock.
    pub(crate) fn decide(&mut self, view: &View, at: &Limits) -> Decision {
        self.observe(view, at);
        self.split = false;
        let held = match self.split_candidate(view, at) {
            Some(Ok(split)) => {
                self.split = true;
                return act(view, at, split);
            }
            Some(Err(held)) => held,
            None => Hold::Steady,
        };
        if !view.beats_fresh {
            return Decision::Hold(Hold::BeatsStale);
        }
        match self.merge_candidate(view, at) {
            Some(merge) => act(view, at, merge),
            None => Decision::Hold(held),
        }
    }

    /// Whether the last decision's candidate was a split, not a merge or none: a hold it ended in
    /// is a ready split's, which the loop tells the fleet about when it is the fleet's own settle.
    pub(crate) fn decided_split(&self) -> bool {
        self.split
    }

    /// Advances every sustain to `view`'s clock and forgets leaves and pairs that are gone.
    fn observe(&mut self, view: &View, at: &Limits) {
        let now = view.now_ms;
        let present = |prefix: &str| view.leaves.iter().any(|leaf| leaf.prefix == prefix);
        self.leaves.retain(|prefix, _| present(prefix));
        for leaf in &view.leaves {
            let fresh = fresh(view, at, leaf);
            let sustain = self.leaves.entry(leaf.prefix.clone()).or_default();
            sustain.over = since(
                sustain.over,
                fresh && over(&leaf.load, at.split_line_pct),
                now,
            );
            sustain.emergency = since(
                sustain.emergency,
                fresh && over(&leaf.load, at.emergency_line_pct),
                now,
            );
        }
        self.fleet = since(self.fleet, view.beats_fresh && view.split_demand > 0, now);
        let pairs = pairs(&view.leaves);
        self.quiet
            .retain(|parent, _| pairs.iter().any(|(p, _, _)| p == parent));
        for (parent, zero, one) in pairs {
            let quiet = view.beats_fresh
                && fresh(view, at, zero)
                && fresh(view, at, one)
                && quiet(&zero.load, &one.load, at.merge_line_pct);
            match (quiet, self.quiet.get(&parent)) {
                (true, None) => {
                    self.quiet.insert(parent, now);
                }
                (false, _) => {
                    self.quiet.remove(&parent);
                }
                (true, Some(_)) => {}
            }
        }
        if let Some(doubling) = &mut self.doubling {
            doubling.remaining.retain(|prefix| present(prefix));
        }
    }

    /// The leaf to split, or why a leaf ready to split does not. A doubling in progress goes
    /// on; half or more of the leaves ready starts one; otherwise the readiest leaf splits.
    /// A leaf is ready on its own line's sustain, or as one of the fleet axis's `d` busiest.
    fn split_candidate<'a>(
        &mut self,
        view: &'a View,
        at: &Limits,
    ) -> Option<Result<Candidate<'a>, Hold>> {
        if let Some(doubling) = &mut self.doubling {
            doubling
                .remaining
                .retain(|prefix| !held_at_max(view, at, prefix));
        }
        if let Some(doubling) = &self.doubling
            && let Some(next) = doubling.remaining.first()
        {
            let leaf = view.leaves.iter().find(|leaf| leaf.prefix == *next)?;
            return Some(Ok(Candidate::Split(leaf, doubling.plan)));
        }
        let d = if elapsed(self.fleet, at.split_sustain_ms, view.now_ms) {
            view.split_demand
        } else {
            0
        };
        let marked = busiest(&view.leaves, d);
        let mut held = None;
        let mut ready: Vec<(&Leaf, bool)> = Vec::new();
        for leaf in &view.leaves {
            let sustain = self.leaves.get(&leaf.prefix).copied().unwrap_or_default();
            let emergency = elapsed(sustain.emergency, at.emergency_sustain_ms, view.now_ms);
            let normal = elapsed(sustain.over, at.split_sustain_ms, view.now_ms)
                || marked.contains(&leaf.prefix.as_str());
            if !emergency && !normal {
                continue;
            }
            if hot(&leaf.load) {
                held = Some(Hold::HotEntity);
            } else if leaf.prefix.len() >= MAX_DEPTH {
                held = Some(Hold::MaxDepth);
            } else {
                ready.push((leaf, emergency));
            }
        }
        ready.sort_by(|(a, ae), (b, be)| be.cmp(ae).then(readiest(b, a)));
        let Some(&(first, emergency)) = ready.first() else {
            return held.map(Err);
        };
        let kind = if emergency {
            SplitKind::Emergency
        } else {
            SplitKind::Normal
        };
        if ready.len() * 2 >= view.leaves.len() {
            let plan = Plan {
                kind,
                doubling: true,
                fleet_axis: d,
            };
            let mut remaining = deepest_first(&view.leaves);
            remaining.retain(|prefix| !held_at_max(view, at, prefix));
            let leaf = view
                .leaves
                .iter()
                .find(|leaf| Some(&leaf.prefix) == remaining.first())?;
            self.doubling = Some(Doubling { plan, remaining });
            return Some(Ok(Candidate::Split(leaf, plan)));
        }
        let fleet_axis = if marked.contains(&first.prefix.as_str()) {
            d
        } else {
            0
        };
        Some(Ok(Candidate::Split(
            first,
            Plan {
                kind,
                doubling: false,
                fleet_axis,
            },
        )))
    }

    /// The deepest sibling pair quiet for the full sustain whose leaves are both past the
    /// split-reversal age.
    fn merge_candidate<'a>(&self, view: &'a View, at: &Limits) -> Option<Candidate<'a>> {
        let old = |leaf: &Leaf| view.now_ms - leaf.born_ms >= at.reversal_min_ms;
        pairs(&view.leaves)
            .into_iter()
            .filter(|(parent, zero, one)| {
                elapsed(
                    self.quiet.get(parent).copied(),
                    at.merge_sustain_ms,
                    view.now_ms,
                ) && old(zero)
                    && old(one)
            })
            .min_by(|(a, _, _), (b, _, _)| b.len().cmp(&a.len()).then(a.cmp(b)))
            .map(|(parent, zero, _)| Candidate::Merge(parent, zero))
    }
}

/// The decision for a candidate: the gates, then a split's reservation or a merge's guard.
fn act(view: &View, at: &Limits, candidate: Candidate<'_>) -> Decision {
    let actor = match &candidate {
        Candidate::Split(leaf, _) | Candidate::Merge(_, leaf) => leaf,
    };
    if let Some(held) = gate(view, at, actor) {
        return Decision::Hold(held);
    }
    match candidate {
        Candidate::Split(leaf, plan) => reserve(view, at).unwrap_or_else(|| Decision::Split {
            prefix: leaf.prefix.clone(),
            kind: plan.kind,
            doubling: plan.doubling,
            fleet_axis: plan.fleet_axis,
        }),
        Candidate::Merge(_, _) if view.congested => Decision::Hold(Hold::Congested),
        Candidate::Merge(parent, _) => Decision::Merge { parent },
    }
}

/// Why `actor`'s operation may not start this tick (S4 §10.1 rule 1, rev 4's serialisation).
fn gate(view: &View, at: &Limits, actor: &Leaf) -> Option<Hold> {
    let recent =
        |at_ms: Option<i64>, window_ms: i64| at_ms.is_some_and(|t| view.now_ms - t < window_ms);
    if actor.owner != view.me {
        Some(Hold::PeerActs)
    } else if view.intent_live {
        Some(Hold::IntentLive)
    } else if recent(view.last_op_end_ms, at.cooldown_ms) {
        Some(Hold::Cooldown)
    } else if recent(view.desired_computed_ms, at.cooldown_ms) {
        Some(Hold::FleetSettling)
    } else if recent(view.youngest_override_ms, at.override_cooldown_ms) {
        Some(Hold::OverrideRecent)
    } else {
        None
    }
}

/// Reserve, then split: what holds a split this instance would start. It needs the leaf
/// count's headroom, room for two children beside the parent here, and another instance with
/// room for one; short of room it asks for an instance. At `FLEET_MAX` it never asks: with room
/// here it splits without a destination, and without it holds at the topology's maximum. The
/// transition's memory is S4's split entry's own check, whose `instance_full` the loop answers the
/// same way.
fn reserve(view: &View, at: &Limits) -> Option<Decision> {
    if view.leaves.len() >= MAX_LEAVES {
        return Some(Decision::Hold(Hold::TopologyAtMax));
    }
    let max = at.shards_per_instance_max;
    let me = view
        .instances
        .iter()
        .find(|instance| instance.id == view.me);
    let resident = me.map_or(0, |instance| instance.resident_shards);
    let here = me.is_some() && room(resident, max);
    let there = view.instances.iter().any(|instance| {
        instance.id != view.me && instance.accepting && instance.resident_shards < max
    });
    match (here, there, at_fleet_max(view, at)) {
        (true, true, _) | (true, false, true) => None,
        (false, _, true) => Some(Decision::Hold(Hold::TopologyAtMax)),
        _ => Some(Decision::AskInstance { resident, max }),
    }
}

/// Whether an instance holding `resident` shards has room for a split's two children beside the
/// parent under the ceiling `max`.
fn room(resident: u32, max: u32) -> bool {
    u64::from(resident) + 2 <= u64::from(max)
}

/// Whether the fleet is at `FLEET_MAX`: no instance can join, so a split never asks for one; it
/// goes ahead on its actor with room, or holds at the topology's maximum (EDGE-261-RATIFICATION §3).
fn at_fleet_max(view: &View, at: &Limits) -> bool {
    view.live >= at.fleet_max
}

/// Whether `prefix`'s split would hold `topology_at_max` for good: the fleet at its maximum and
/// the leaf's owner without room. A doubling passes over such a leaf.
fn held_at_max(view: &View, at: &Limits, prefix: &str) -> bool {
    let owner = view
        .leaves
        .iter()
        .find(|leaf| leaf.prefix == prefix)
        .map(|leaf| &leaf.owner);
    let resident = view
        .instances
        .iter()
        .find(|instance| Some(&instance.id) == owner)
        .map_or(0, |instance| instance.resident_shards);
    at_fleet_max(view, at) && !room(resident, at.shards_per_instance_max)
}

/// Whether `leaf`'s load comes from a beat no older than the stale age.
fn fresh(view: &View, at: &Limits, leaf: &Leaf) -> bool {
    leaf.beat_ms
        .is_some_and(|beat| view.now_ms - beat <= at.beat_stale_ms)
}

/// A sustain start: kept while `holds`, started now when it begins, cleared when it ends.
fn since(start: Option<i64>, holds: bool, now: i64) -> Option<i64> {
    holds.then(|| start.unwrap_or(now))
}

fn elapsed(start: Option<i64>, sustain_ms: i64, now: i64) -> bool {
    start.is_some_and(|t| now - t >= sustain_ms)
}

/// Whether any measured axis is at `pct` or more of its safe capacity.
fn over(load: &BTreeMap<Axis, Usage>, pct: u64) -> bool {
    load.values()
        .any(|u| u.safe > 0 && u128::from(u.used) * 100 >= u128::from(pct) * u128::from(u.safe))
}

/// The binding axis: the measured one most used against its capacity.
fn binding(load: &BTreeMap<Axis, Usage>) -> Option<&Usage> {
    load.values().filter(|u| u.safe > 0).max_by(|a, b| {
        (u128::from(a.used) * u128::from(b.safe)).cmp(&(u128::from(b.used) * u128::from(a.safe)))
    })
}

/// Whether one entity carries half or more of the binding axis.
fn hot(load: &BTreeMap<Axis, Usage>) -> bool {
    binding(load).is_some_and(|u| u.used > 0 && u128::from(u.top_entity) * 2 >= u128::from(u.used))
}

/// `a` against `b` by readiness: the more used on its binding axis first, then the deeper,
/// then by prefix.
fn readiest(a: &Leaf, b: &Leaf) -> std::cmp::Ordering {
    let fraction = |leaf: &Leaf| {
        binding(&leaf.load).map_or((0, 1), |u| (u128::from(u.used), u128::from(u.safe)))
    };
    let ((ua, sa), (ub, sb)) = (fraction(a), fraction(b));
    (ua * sb)
        .cmp(&(ub * sa))
        .then(a.prefix.len().cmp(&b.prefix.len()))
        .then(b.prefix.cmp(&a.prefix))
}

/// The prefixes of the `d` busiest leaves: the fleet axis's share of the split line.
fn busiest(leaves: &[Leaf], d: u64) -> Vec<&str> {
    let mut by_readiness: Vec<&Leaf> = leaves.iter().collect();
    by_readiness.sort_by(|a, b| readiest(b, a));
    let d = usize::try_from(d).unwrap_or(usize::MAX);
    by_readiness
        .into_iter()
        .take(d)
        .map(|leaf| leaf.prefix.as_str())
        .collect()
}

/// Whether two siblings combined use less than `pct` of one leaf's safe capacity on every
/// axis either measures (the smaller capacity when both do).
fn quiet(a: &BTreeMap<Axis, Usage>, b: &BTreeMap<Axis, Usage>, pct: u64) -> bool {
    a.keys().chain(b.keys()).all(|axis| {
        let (ua, ub) = (a.get(axis), b.get(axis));
        let used = ua.map_or(0, |u| u128::from(u.used)) + ub.map_or(0, |u| u128::from(u.used));
        let safe = [ua, ub]
            .into_iter()
            .flatten()
            .map(|u| u.safe)
            .filter(|&s| s > 0)
            .min();
        safe.is_none_or(|safe| used * 100 < u128::from(pct) * u128::from(safe))
    })
}

/// The sibling leaf pairs, each by its parent's prefix.
fn pairs(leaves: &[Leaf]) -> Vec<(String, &Leaf, &Leaf)> {
    leaves
        .iter()
        .filter_map(|zero| {
            let parent = zero.prefix.strip_suffix('0')?;
            let sibling = format!("{parent}1");
            let one = leaves.iter().find(|leaf| leaf.prefix == sibling)?;
            Some((parent.to_owned(), zero, one))
        })
        .collect()
}

/// The prefixes of the leaves a doubling splits, deepest first, then in prefix order.
fn deepest_first(leaves: &[Leaf]) -> Vec<String> {
    let mut prefixes: Vec<String> = leaves
        .iter()
        .filter(|leaf| leaf.prefix.len() < MAX_DEPTH)
        .map(|leaf| leaf.prefix.clone())
        .collect();
    prefixes.sort_by(|a, b| b.len().cmp(&a.len()).then(a.cmp(b)));
    prefixes
}

mod reason;

#[cfg(test)]
mod tests;

#[cfg(test)]
mod merge_tests;
