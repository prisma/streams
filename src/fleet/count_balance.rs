//! Count rebalance: the ring's rendezvous hash spreads shards unevenly over
//! members whose names differ only in a digit (FNV-1a scores of
//! `"<shard> streams-<n>"` are correlated; 64 shards over 8 members gave
//! one member 33), and the lag-driven rebalancer moves nothing while no
//! shard stalls. An instance that owns more than the fair share
//! ceil(shards / members) moves one shard to a member below the share,
//! through the same override, retirement and eager handoff a lag move uses,
//! until every member is within the share.
//!
//! It runs as its own supervised task beside the fleet tick, on the tick's
//! period, and reads the coordination documents itself: owners come from
//! the overrides document it is about to replace, never from the mirror, so
//! one pass cannot move a shard twice. A move waits out this instance's own
//! move cooldown (counted from its boot), any override younger than the
//! cooldown anywhere in the fleet (lag moves included: the fleet makes at
//! most one count move per cooldown, and none within a cooldown of any
//! move), and an imbalance seen by two consecutive reads. It reads nothing while its mirror of the ring keeps it within the
//! share, and a read that finds no move holds the next for a cooldown.
//! Return-home admits a return only within the same share
//! ([`super::return_home_allowed`]), so the two passes never undo each other.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use object_store::UpdateVersion;

use super::planning::{self, Candidacy};
use super::{AppState, FleetDocument, Heartbeat, OverrideEntry, Overrides};
use crate::runtime::MonotonicNow;
use crate::shard::now_ms;
use crate::tasks::{Cancellation, Policy, TaskResult, TaskSupervisor};

/// Consecutive passes that must each find the same instance past the share
/// before it moves: one pass can read a ring that is still settling.
const SETTLED_PASSES: u32 = 2;
/// A heartbeat older than this carries no load a target is judged by: the
/// fleet tick's own freshness window for its peers' load.
const LOAD_FRESH_MS: i64 = 10_000;
/// The outbox bound the fleet tick keeps on the overrides document.
const MAX_PENDING_EVENTS: usize = 64;

/// The fair share of `shards` over `members`: ceil(shards / members), and
/// every shard while there is no member.
fn fair_share(shards: usize, members: usize) -> usize {
    shards.div_ceil(members.max(1))
}

/// Whether a member owning `owned` of `shards` is past the fair share over
/// `members`.
fn past_share(owned: usize, shards: usize, members: usize) -> bool {
    owned > fair_share(shards, members)
}

/// The move that brings `me` one shard closer to the fair share, given each
/// shard's owner: the first by name of `me`'s shards, to a healthy active
/// member below the share. Members past
/// the share move at the same ticks, so each takes a different target: the
/// one at its own rank, by name, among the members past the share, of the
/// members below it ordered by fewest shards. Only a shard `movable` admits
/// moves: one with no override that names a member, so a count move never
/// undoes a lag move or another count move. None when `me` is within the share, or no member
/// is below it, or none of its shards may move.
pub(crate) fn pick(
    owners: &[(String, String)],
    active: &[String],
    me: &str,
    healthy: impl Fn(&str) -> bool,
    movable: impl Fn(&str) -> bool,
) -> Option<(String, String)> {
    let share = fair_share(owners.len(), active.len());
    let mut counts: HashMap<&str, usize> = active.iter().map(|m| (m.as_str(), 0)).collect();
    for (_, owner) in owners {
        if let Some(count) = counts.get_mut(owner.as_str()) {
            *count += 1;
        }
    }
    let count = |member: &str| counts.get(member).copied().unwrap_or(0);
    let mut over: Vec<&str> = active
        .iter()
        .map(String::as_str)
        .filter(|m| count(m) > share)
        .collect();
    over.sort_unstable();
    // A member within the share, or none of the ring, has no rank.
    let rank = over.iter().position(|m| *m == me)?;
    let mut below: Vec<&String> = active
        .iter()
        .filter(|m| m.as_str() != me && healthy(m) && count(m) < share)
        .collect();
    below.sort_unstable_by_key(|m| (count(m), m.as_str()));
    let to = *below.get(rank % below.len().max(1))?;
    let prefix = owners
        .iter()
        .filter(|(prefix, owner)| owner == me && movable(prefix))
        .map(|(prefix, _)| prefix)
        .min()?;
    Some((prefix.clone(), to.clone()))
}

/// Whether this instance may give a shard away: it is not lagging. The fleet
/// tick counts a lag over the threshold towards a lag move instead.
fn may_shed(my_lag_secs: u64, lag_threshold_secs: u64) -> bool {
    my_lag_secs <= lag_threshold_secs
}

/// Whether a member may receive a shard: its lag is well under the
/// threshold, the lag move's own rule ([`super::pick_move_target`]). A
/// member near the threshold would only be handed more of what it lags on.
fn well_under(lag_secs: u64, lag_threshold_secs: u64) -> bool {
    lag_secs < lag_threshold_secs / 2
}

/// Every shard's owner as the overrides document assigns it: an override
/// names a member of `active`, or the ring's rendezvous home holds the shard
/// (the readers' own rule, `OwnershipService::effective_owner`).
fn document_owners(
    prefixes: &[String],
    active: &[String],
    overrides: &Overrides,
) -> Vec<(String, String)> {
    prefixes
        .iter()
        .filter(|_| !active.is_empty())
        .map(|prefix| {
            let owner = overrides
                .entries
                .get(prefix)
                .map(|entry| &entry.to)
                .filter(|to| active.contains(to))
                .unwrap_or_else(|| &active[crate::ownership::ring_pick(prefix, active)]);
            (prefix.clone(), owner.clone())
        })
        .collect()
}

/// When this instance may read the fleet and move. Reads wait out a hold:
/// the move cooldown from its boot and from each of its moves, the
/// cooldown again after a read that found no move, and the rest of the
/// cooldown of the fleet's newest override. A move also needs the imbalance
/// seen by [`SETTLED_PASSES`] consecutive reads; a hold restarts the count.
#[derive(Debug)]
struct Pacing {
    cooldown: Duration,
    held_from: MonotonicNow,
    hold: Duration,
    streak: u32,
}

impl Pacing {
    /// A booting instance waits a whole cooldown before its first move: its
    /// peers may have moved just before it started.
    fn booted(now: MonotonicNow, cooldown: Duration) -> Self {
        Self {
            cooldown,
            held_from: now,
            hold: cooldown,
            streak: 0,
        }
    }

    /// Whether the hold has passed at `now`.
    fn may_read(&self, now: MonotonicNow) -> bool {
        now.since(self.held_from) >= self.hold
    }

    fn hold_for(&mut self, now: MonotonicNow, hold: Duration) {
        self.held_from = now;
        self.hold = hold;
    }

    /// A pass that did not read the fleet, or could not.
    fn idle(&mut self) {
        self.streak = 0;
    }

    /// A read that found no move: the next waits a cooldown, so a fleet
    /// where this instance cannot shed is not read every pass.
    fn nothing_to_move(&mut self, now: MonotonicNow) {
        self.streak = 0;
        self.hold_for(now, self.cooldown);
    }

    /// Whether a read at `now` that found a move makes it, while the fleet's
    /// newest override is `newest_override_age_ms` old (None: no override).
    /// A younger override holds the reads until it is a cooldown old.
    fn due(&mut self, now: MonotonicNow, newest_override_age_ms: Option<i64>) -> bool {
        self.streak = self.streak.saturating_add(1);
        if self.streak < SETTLED_PASSES {
            return false;
        }
        let cooldown_ms = i64::try_from(self.cooldown.as_millis()).unwrap_or(i64::MAX);
        let young = newest_override_age_ms.map(|age| cooldown_ms.saturating_sub(age));
        match young.filter(|rest| *rest > 0) {
            Some(rest) => {
                // The reads after the hold count afresh: the two that found
                // the move must be consecutive.
                self.streak = 0;
                self.hold_for(now, Duration::from_millis(rest.unsigned_abs()));
                false
            }
            None => true,
        }
    }

    /// A move committed at `now`.
    fn moved(&mut self, now: MonotonicNow) {
        self.streak = 0;
        self.hold_for(now, self.cooldown);
    }
}

/// Every heartbeat's candidacy, and the lag of each one fresh enough to
/// carry load (younger than 10 s and not draining), as the fleet tick folds
/// them: absorb lag or wedge, whichever is longer.
// mt-lint: allow(name-keyed-map): fleet instance -> candidacy, fresh lag
type Folded = (HashMap<String, Candidacy>, HashMap<String, u64>);

fn fold(heartbeats: Vec<Heartbeat>, now: i64) -> Folded {
    let (mut candidacies, mut lags) = (HashMap::new(), HashMap::new());
    for beat in heartbeats {
        candidacies.insert(beat.instance.clone(), beat.candidacy(now));
        if now - beat.ts_ms < LOAD_FRESH_MS && !beat.draining {
            let wedge_secs = u64::try_from(beat.wedge_max_ms / 1000).unwrap_or(0);
            lags.insert(beat.instance, beat.absorb_lag_max_secs.max(wedge_secs));
        }
    }
    (candidacies, lags)
}

/// `overrides` with `prefix` moved to `to` at `now_ms` and its
/// `rebalance_move` event, within the outbox bound, as a lag move writes them.
fn moved_document(mut overrides: Overrides, prefix: &str, to: &str, now_ms: i64) -> Overrides {
    if overrides.pending_events.len() < MAX_PENDING_EVENTS {
        overrides.pending_events.push(
            crate::ops::OpsEvent::new("rebalance_move", format!("move/{prefix}/{}", now_ms / 1000))
                .shard(prefix)
                .fields(serde_json::Value::Object(serde_json::Map::from_iter([(
                    "to".to_owned(),
                    serde_json::Value::from(to),
                )]))),
        );
    }
    let entry = OverrideEntry {
        to: to.to_owned(),
        ms: now_ms,
    };
    overrides.entries.insert(prefix.to_owned(), entry);
    overrides
}

/// What one read gives a pass: the ring's active members, the fresh
/// members' lag, and the overrides document with the version its
/// replacement must match.
struct FleetRead {
    active: Vec<String>,
    // mt-lint: allow(name-keyed-map): fleet instance -> its fresh lag
    lags: HashMap<String, u64>,
    overrides: Overrides,
    version: Option<UpdateVersion>,
}

impl FleetRead {
    /// The heartbeats (peeked, as every reader but the tick does), the
    /// desired count and the overrides document; None when one is
    /// unreadable.
    async fn read(state: &AppState, me: &str) -> Option<Self> {
        let heartbeats = state
            .fleet
            .peek_heartbeat_set()
            .await
            .inspect_err(|error| tracing::debug!(%error, "count balance: heartbeats unreadable"))
            .ok()?;
        let (candidacies, lags) = fold(heartbeats, now_ms());
        let (desired, _) = state.fleet.read_desired_state().await.ok()?;
        let count = desired.map_or(1, |desired| desired.count);
        let active = planning::active_members(count, me, &candidacies);
        let (overrides, version) = state.fleet.read_overrides().await.ok()?;
        Some(Self {
            active,
            lags,
            overrides,
            version,
        })
    }

    /// The move `me` would make over `prefixes` (see [`pick`]).
    fn plan(
        &self,
        prefixes: &[String],
        me: &str,
        lag_threshold_secs: u64,
    ) -> Option<(String, String)> {
        pick(
            &document_owners(prefixes, &self.active, &self.overrides),
            &self.active,
            me,
            |member| {
                self.lags
                    .get(member)
                    .is_some_and(|lag| well_under(*lag, lag_threshold_secs))
            },
            // An override to a non-member is void (every reader ignores it),
            // so its shard moves as one with none.
            |prefix| {
                self.overrides
                    .entries
                    .get(prefix)
                    .is_none_or(|entry| !self.active.contains(&entry.to))
            },
        )
    }

    /// The age of the fleet's newest override.
    fn newest_override_age_ms(&self) -> Option<i64> {
        let now = now_ms();
        self.overrides
            .entries
            .values()
            .map(|entry| now - entry.ms)
            .min()
    }

    /// CAS-replace the overrides document with `prefix` moved to `to`.
    /// Whether it committed.
    async fn commit(self, state: &AppState, prefix: &str, to: &str) -> bool {
        let next = moved_document(self.overrides, prefix, to, now_ms());
        match serde_json::to_vec(&next) {
            Ok(body) => {
                state
                    .fleet
                    .replace_document(FleetDocument::Overrides, body, self.version)
                    .await
            }
            Err(error) => {
                tracing::warn!(%error, "count balance: overrides unserialisable");
                false
            }
        }
    }
}

/// Whether this instance owns more than the fair share in its mirror of
/// the ring: the cheap test that spares a balanced fleet every read.
fn past_share_in_mirror(state: &AppState, me: &str) -> bool {
    let active = state.ownership.ring_active();
    let prefixes = state.shards.prefixes();
    let owned = prefixes
        .iter()
        .filter(|prefix| state.ownership.effective_owner(prefix).as_deref() == Some(me))
        .count();
    past_share(owned, prefixes.len(), active.len())
}

/// One pass: read the fleet when this instance may be past the share, and
/// move one shard when the pacing allows.
async fn pass(state: &AppState, me: &str, pacing: &mut Pacing) {
    let threshold = state.config.fleet.rebalance_lag_secs;
    let (_, _, my_lag) = state.fleet_pressure();
    let now = state.runtime.clock.monotonic();
    if !pacing.may_read(now) || !may_shed(my_lag, threshold) || !past_share_in_mirror(state, me) {
        return pacing.idle();
    }
    let Some(read) = FleetRead::read(state, me).await else {
        return pacing.idle();
    };
    let Some((prefix, to)) = read.plan(state.shards.prefixes(), me, threshold) else {
        return pacing.nothing_to_move(now);
    };
    if !pacing.due(now, read.newest_override_age_ms()) {
        return;
    }
    if !read.commit(state, &prefix, &to).await {
        tracing::info!("count balance: overrides CAS lost; retry next pass");
        return;
    }
    tracing::info!(shard = %prefix, %to, "count balance: moving a shard past the fair share");
    state.ownership.set_override(&prefix, &to);
    // Stop serving at once and fail in-flight work fast (retryable); the
    // new owner fences the log when it opens the shard, as after a lag move.
    state.shards.retire(
        &prefix,
        crate::shard_directory::RetirementReason::FleetEviction,
        |_, _| true,
    );
    pacing.moved(now);
}

/// Start this runtime's count balance beside its fleet tick.
pub(super) fn start(state: Arc<AppState>, instance: String, tasks: &TaskSupervisor) {
    if let Err(rejected) = tasks.spawn("fleet-count-balance", Policy::Noncritical, move |cancel| {
        run(state, instance, cancel)
    }) {
        tracing::debug!(
            ?rejected,
            "runtime stopping: the fleet count balance was not started"
        );
    }
}

/// A pass every tick period until cancelled; a pass that overruns the
/// tick's own pass deadline is abandoned.
async fn run(state: Arc<AppState>, instance: String, cancel: Cancellation) -> TaskResult {
    let cooldown = Duration::from_secs(state.config.fleet.rebalance_move_cooldown_secs);
    let mut pacing = Pacing::booted(state.runtime.clock.monotonic(), cooldown);
    loop {
        tokio::select! {
            _ = cancel.cancelled() => return TaskResult::Done,
            _ = tokio::time::sleep(planning::TICK_PERIOD) => {}
        }
        let passed = tokio::select! {
            _ = cancel.cancelled() => return TaskResult::Done,
            passed = tokio::time::timeout(planning::PASS_DEADLINE, pass(&state, &instance, &mut pacing)) => passed,
        };
        if passed.is_err() {
            tracing::warn!("count balance: pass abandoned at its deadline");
            pacing.idle();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        FleetRead, Heartbeat, OverrideEntry, Overrides, Pacing, document_owners, fair_share, fold,
        may_shed, moved_document, past_share, pick, well_under,
    };
    use crate::runtime::{Clock, ManualClock};
    use std::time::Duration;

    fn members(n: usize) -> Vec<String> {
        (1..=n).map(|k| format!("streams-{k}")).collect()
    }

    /// 64 six-bit shards, owned as the ring assigns them.
    fn ring_owners(active: &[String]) -> Vec<(String, String)> {
        (0..64_u32)
            .map(|k| {
                let prefix = format!("{k:06b}");
                let owner = active[crate::ownership::ring_pick(&prefix, active)].clone();
                (prefix, owner)
            })
            .collect()
    }

    /// `counts[i]` shards owned by `active[i]`, numbered in order.
    fn owned(active: &[String], counts: &[usize]) -> Vec<(String, String)> {
        active
            .iter()
            .zip(counts)
            .flat_map(|(member, n)| std::iter::repeat_n(member.clone(), *n))
            .enumerate()
            .map(|(k, member)| (format!("{k:04b}"), member))
            .collect()
    }

    fn owner_of<'a>(owners: &'a [(String, String)], prefix: &str) -> Option<&'a str> {
        owners
            .iter()
            .find(|(p, _)| p == prefix)
            .map(|(_, owner)| owner.as_str())
    }

    /// One round: each member makes the move it would, against the owners
    /// as the moves before it left them. The number of moves.
    fn round(owners: &mut [(String, String)], active: &[String]) -> usize {
        let mut moves = 0;
        for me in active {
            let Some((prefix, to)) = pick(owners, active, me, |_| true, |_| true) else {
                continue;
            };
            assert_eq!(
                owner_of(owners, &prefix),
                Some(me.as_str()),
                "{me} moves its own shard"
            );
            owners
                .iter_mut()
                .filter(|entry| entry.0 == prefix)
                .for_each(|entry| entry.1.clone_from(&to));
            moves += 1;
        }
        moves
    }

    /// Apply at most 64 rounds; then no member moves. The largest count
    /// left and the number of moves.
    fn converge(active: &[String]) -> (usize, usize) {
        let mut owners = ring_owners(active);
        let mut moves = 0;
        for _ in 0..64 {
            moves += round(&mut owners, active);
        }
        assert_eq!(
            round(&mut owners, active),
            0,
            "{} members: still moving after 64 rounds",
            active.len()
        );
        let largest = active
            .iter()
            .map(|m| owners.iter().filter(|(_, o)| o == m).count())
            .max()
            .unwrap_or(0);
        (largest, moves)
    }

    #[test]
    fn the_ring_alone_gives_one_of_eight_members_more_than_three_times_its_share() {
        let active = members(8);
        let owners = ring_owners(&active);
        let largest = active
            .iter()
            .map(|m| owners.iter().filter(|(_, o)| o == m).count())
            .max()
            .unwrap_or(0);
        assert!(
            largest > 24,
            "the skew this pass exists for: {largest} of 64"
        );
    }

    #[test]
    fn moves_bring_every_member_within_the_fair_share() {
        for n in [2, 3, 4, 5, 8, 16, 64] {
            let active = members(n);
            let (largest, moves) = converge(&active);
            assert!(largest <= 64_usize.div_ceil(n), "{n} members: {largest}");
            assert!(moves <= 64, "{n} members: {moves} moves");
        }
    }

    #[test]
    fn members_past_the_share_move_to_different_targets_at_once() {
        let active = members(4);
        // streams-1 and streams-2 hold 7 each of 16; the share is 4.
        let owners = owned(&active, &[7, 7, 1, 1]);
        let first = pick(&owners, &active, "streams-1", |_| true, |_| true).expect("a move");
        let second = pick(&owners, &active, "streams-2", |_| true, |_| true).expect("a move");
        assert_ne!(first.1, second.1, "{first:?} {second:?}");
    }

    #[test]
    fn a_member_within_the_share_or_with_no_room_elsewhere_moves_nothing() {
        let active = members(2);
        let even: Vec<(String, String)> = (0..4)
            .map(|k| (format!("{k:02b}"), active[k % 2].clone()))
            .collect();
        assert_eq!(pick(&even, &active, "streams-1", |_| true, |_| true), None);
        let skewed = owned(&active, &[4, 0]);
        assert!(pick(&skewed, &active, "streams-1", |_| true, |_| true).is_some());
        assert_eq!(
            pick(&skewed, &active, "streams-1", |_| false, |_| true),
            None,
            "no healthy member to move to"
        );
        assert_eq!(
            pick(&skewed, &active, "streams-9", |_| true, |_| true),
            None,
            "not a member"
        );
        assert_eq!(
            pick(&skewed, &active, "streams-1", |_| true, |p| p == "0011"),
            Some(("0011".to_owned(), "streams-2".to_owned())),
            "only a shard without an override moves"
        );
        assert_eq!(
            pick(&skewed, &active, "streams-1", |_| true, |_| false),
            None,
            "every shard has an override"
        );
        assert_eq!(
            pick(&skewed, &[], "streams-1", |_| true, |_| true),
            None,
            "no ring"
        );
    }

    /// A member exactly at the share is not past it, so it takes no rank: the
    /// one member past the share has rank 0 and the first target below it.
    #[test]
    fn only_members_past_the_share_rank_for_targets() {
        let active = members(4);
        // 8 shards, share 2: streams-1 at the share, streams-2 past it.
        let owners = owned(&active, &[2, 4, 1, 1]);
        let (prefix, to) = pick(&owners, &active, "streams-2", |_| true, |_| true).expect("a move");
        assert_eq!(to, "streams-3", "rank 0 takes the first target");
        assert_eq!(owner_of(&owners, &prefix), Some("streams-2"));
    }

    /// A member at the share has no room: it is never a target.
    #[test]
    fn a_member_at_the_share_is_no_target() {
        let active = members(4);
        // 12 shards, share 3: streams-1 and streams-2 past it, streams-3 at it.
        let owners = owned(&active, &[5, 4, 3, 0]);
        let first = pick(&owners, &active, "streams-1", |_| true, |_| true).expect("a move");
        let second = pick(&owners, &active, "streams-2", |_| true, |_| true).expect("a move");
        assert_eq!(
            (first.1.as_str(), second.1.as_str()),
            ("streams-4", "streams-4")
        );
    }

    /// 5 shards over 2 members: a share of 3. A member at 3 is within it, so
    /// it neither moves nor reads; at 4 it is past it.
    #[test]
    fn the_fair_share_rounds_up() {
        assert_eq!(fair_share(5, 2), 3);
        assert_eq!(fair_share(5, 0), 5, "no member");
        assert!(!past_share(3, 5, 2), "a member at the share reads nothing");
        assert!(past_share(4, 5, 2));
        let active = members(2);
        let owners = owned(&active, &[3, 2]);
        assert_eq!(
            pick(&owners, &active, "streams-1", |_| true, |_| true),
            None
        );
        let owners = owned(&active, &[4, 1]);
        assert_eq!(
            pick(&owners, &active, "streams-1", |_| true, |_| true),
            Some(("0000".to_owned(), "streams-2".to_owned())),
            "the first of its shards by name"
        );
    }

    /// An override naming a member outside the ring is void: every reader
    /// gives the shard to its rendezvous home, so the count balance may move
    /// it and rewrite the entry. A void override must not pin its shard after
    /// a scale-in.
    #[test]
    fn a_shard_whose_override_names_a_non_member_may_move() {
        let entry = |to: &str| OverrideEntry {
            to: to.to_owned(),
            ms: 0,
        };
        let read = FleetRead {
            active: members(2),
            lags: [("streams-2".to_owned(), 0)].into_iter().collect(),
            overrides: Overrides {
                entries: [
                    ("00", "streams-9"),
                    ("01", "streams-9"),
                    ("10", "streams-1"),
                    ("11", "streams-1"),
                ]
                .into_iter()
                .map(|(prefix, to)| (prefix.to_owned(), entry(to)))
                .collect(),
                ..Default::default()
            },
            version: None,
        };
        let prefixes: Vec<String> = ["00", "01", "10", "11"].map(str::to_owned).to_vec();
        assert_eq!(
            read.plan(&prefixes, "streams-1", 60),
            Some(("00".to_owned(), "streams-2".to_owned()))
        );
    }

    #[test]
    fn an_instance_sheds_at_the_lag_threshold_but_not_past_it() {
        assert!(may_shed(0, 60));
        assert!(may_shed(60, 60));
        assert!(!may_shed(61, 60));
    }

    #[test]
    fn a_target_must_be_well_under_half_the_threshold() {
        assert!(well_under(29, 60));
        assert!(!well_under(30, 60));
        assert!(!well_under(0, 1), "a threshold of 1 s leaves no target");
    }

    #[test]
    fn owners_follow_the_document_and_ignore_an_override_to_a_non_member() {
        let active = members(2);
        let entry = |to: &str| OverrideEntry {
            to: to.to_owned(),
            ms: 0,
        };
        let overrides = Overrides {
            entries: [
                ("00".to_owned(), entry("streams-2")),
                ("01".to_owned(), entry("streams-9")),
            ]
            .into_iter()
            .collect(),
            ..Default::default()
        };
        let prefixes: Vec<String> = ["00", "01", "10"].iter().map(|p| (*p).to_owned()).collect();
        assert_eq!(
            document_owners(&prefixes, &active, &overrides),
            [
                ("00".to_owned(), "streams-2".to_owned()),
                ("01".to_owned(), "streams-1".to_owned()),
                ("10".to_owned(), "streams-2".to_owned()),
            ]
        );
        assert!(document_owners(&prefixes, &[], &overrides).is_empty());
    }

    const COOLDOWN: Duration = Duration::from_secs(60);

    /// A clock and a pacing booted on it, `after` its boot.
    fn pacing(after: Duration) -> (ManualClock, Pacing) {
        let clock = ManualClock::at(0);
        let pacing = Pacing::booted(clock.monotonic(), COOLDOWN);
        clock.advance_monotonic(after);
        (clock, pacing)
    }

    #[test]
    fn reads_wait_out_the_boot_cooldown() {
        let (clock, pacing) = pacing(Duration::from_millis(59_999));
        assert!(!pacing.may_read(clock.monotonic()));
        clock.advance_monotonic(Duration::from_millis(1));
        assert!(pacing.may_read(clock.monotonic()));
    }

    #[test]
    fn a_move_needs_two_consecutive_reads_that_found_it() {
        let (clock, mut pacing) = pacing(COOLDOWN);
        let now = clock.monotonic();
        assert!(!pacing.due(now, None), "one read");
        assert!(pacing.due(now, None), "the second");
        let (_, mut pacing) = self::pacing(COOLDOWN);
        assert!(!pacing.due(now, None));
        pacing.idle();
        assert!(
            !pacing.due(now, None),
            "a pass that did not read restarts the count"
        );
        assert!(pacing.due(now, None));
    }

    #[test]
    fn a_read_with_nothing_to_move_holds_the_next_for_a_cooldown() {
        let (clock, mut pacing) = pacing(COOLDOWN);
        assert!(!pacing.due(clock.monotonic(), None));
        pacing.nothing_to_move(clock.monotonic());
        clock.advance_monotonic(COOLDOWN - Duration::from_millis(1));
        assert!(!pacing.may_read(clock.monotonic()));
        clock.advance_monotonic(Duration::from_millis(1));
        assert!(pacing.may_read(clock.monotonic()));
        assert!(!pacing.due(clock.monotonic(), None), "the count restarts");
        assert!(pacing.due(clock.monotonic(), None));
    }

    #[test]
    fn a_younger_override_holds_the_reads_for_the_rest_of_its_cooldown() {
        let (clock, mut pacing) = pacing(COOLDOWN);
        assert!(!pacing.due(clock.monotonic(), None));
        assert!(!pacing.due(clock.monotonic(), Some(40_000)));
        clock.advance_monotonic(Duration::from_millis(19_999));
        assert!(!pacing.may_read(clock.monotonic()));
        clock.advance_monotonic(Duration::from_millis(1));
        assert!(pacing.may_read(clock.monotonic()));
        assert!(
            !pacing.due(clock.monotonic(), None),
            "the two reads count again after the hold"
        );
        assert!(pacing.due(clock.monotonic(), None));
    }

    #[test]
    fn an_override_a_cooldown_old_holds_nothing() {
        let (clock, mut pacing) = pacing(COOLDOWN);
        assert!(!pacing.due(clock.monotonic(), None));
        assert!(
            pacing.due(clock.monotonic(), Some(60_000)),
            "a cooldown old"
        );
        let (clock, mut pacing) = self::pacing(COOLDOWN);
        assert!(!pacing.due(clock.monotonic(), None));
        assert!(
            !pacing.due(clock.monotonic(), Some(59_999)),
            "a millisecond short"
        );
        assert!(
            !pacing.may_read(clock.monotonic()),
            "held for that millisecond"
        );
        clock.advance_monotonic(Duration::from_millis(1));
        assert!(pacing.may_read(clock.monotonic()));
    }

    #[test]
    fn a_move_restarts_the_cooldown_and_the_count() {
        let (clock, mut pacing) = pacing(COOLDOWN);
        pacing.moved(clock.monotonic());
        clock.advance_monotonic(COOLDOWN - Duration::from_millis(1));
        assert!(!pacing.may_read(clock.monotonic()));
        clock.advance_monotonic(Duration::from_millis(1));
        assert!(pacing.may_read(clock.monotonic()));
        assert!(!pacing.due(clock.monotonic(), None));
        assert!(pacing.due(clock.monotonic(), None));
    }

    fn heartbeat(instance: &str, ts_ms: i64, lag: u64, wedge_ms: i64, draining: bool) -> Heartbeat {
        serde_json::from_str(&format!(
            r#"{{"instance":"{instance}","ts_ms":{ts_ms},"rps":0.0,"owned_shards":[],"draining":{draining},"absorb_lag_max_secs":{lag},"wedge_max_ms":{wedge_ms}}}"#
        ))
        .unwrap()
    }

    #[test]
    fn a_member_carries_load_while_younger_than_ten_seconds_and_not_draining() {
        let now = 100_000;
        let (candidacies, lags) = fold(
            vec![
                heartbeat("wedged", now - 9_999, 3, 31_000, false),
                heartbeat("lagging", now, 7, 0, false),
                heartbeat("stale", now - 10_000, 0, 0, false),
                heartbeat("draining", now, 0, 0, true),
            ],
            now,
        );
        assert_eq!(candidacies.len(), 4, "every heartbeat is a candidate");
        let lags: std::collections::BTreeMap<_, _> = lags.into_iter().collect();
        assert_eq!(
            lags,
            [("lagging".to_owned(), 7), ("wedged".to_owned(), 31)]
                .into_iter()
                .collect()
        );
    }

    #[test]
    fn a_move_writes_its_entry_and_its_event_within_the_outbox_bound() {
        let moved = moved_document(Overrides::default(), "00", "streams-2", 61_500);
        let entries: Vec<(&str, &str, i64)> = moved
            .entries
            .iter()
            .map(|(prefix, entry)| (prefix.as_str(), entry.to.as_str(), entry.ms))
            .collect();
        assert_eq!(entries, [("00", "streams-2", 61_500)]);
        let events: Vec<(&str, &str)> = moved
            .pending_events
            .iter()
            .map(|event| (event.event_type.as_str(), event.event_id.as_str()))
            .collect();
        assert_eq!(events, [("rebalance_move", "move/00/61")]);
        assert_eq!(
            moved.pending_events[0].fields.to_string(),
            r#"{"to":"streams-2"}"#,
            "the lag move's event fields"
        );
        let event = |k: usize| crate::ops::OpsEvent::new("desired_changed", format!("desired/{k}"));
        let nearly_full = Overrides {
            pending_events: (0..63).map(event).collect(),
            ..Default::default()
        };
        let full = moved_document(nearly_full, "00", "streams-2", 0);
        assert_eq!(full.pending_events.len(), 64);
        let bounded = moved_document(full, "01", "streams-2", 0);
        assert_eq!(bounded.pending_events.len(), 64, "the outbox bound holds");
        assert!(
            bounded.entries.contains_key("01"),
            "and the move is still written"
        );
    }
}
