//! The fleet tick's two shard moves, return-home and the lag move (SCALING.md §4, the R4
//! rebalancer), outside the tick itself (GATE-DECISION-f6-addendum-2026-11-17, item 1).
//!
//! The tick reads the authority once: the heartbeats, the ring's active set and the overrides
//! document with the version its replacement must match. [`Tick`] is that read as the moves
//! plan over it; each move then commits by one CAS of the overrides document, which the tick
//! runs under its own pass deadline and cancellation, exactly where it ran it before.

use std::collections::HashMap;

use object_store::UpdateVersion;

use super::planning::{Candidacy, Reservations as _};
use super::{AppState, FleetDocument, Overrides, pick_move_target, return_home_allowed};
use crate::shard::now_ms;

/// C6(b): the home of `prefix` over `active` with every ordinal within `count` and every member
/// `overrides` lists (the join ruling, Addendum 6). Rendezvous over a superset of every reader's
/// ring over that document: an entry whose holder wins here is a no-op over the document's own
/// ring, so dropping it changes no owner.
fn counted_home(active: &[String], overrides: &Overrides, count: u64, prefix: &str) -> String {
    let mut counted = active.to_vec();
    let ordinals = super::record::ordinal(count).into_iter();
    counted.extend(ordinals.filter(|name| !active.contains(name)));
    let listed = overrides
        .members
        .iter()
        .flatten()
        .map(|member| &member.name);
    let unseen: Vec<String> = listed
        .filter(|name| !counted.contains(name))
        .cloned()
        .collect();
    counted.extend(unseen);
    // In ordinal order, as every ring (the join ruling, Addendum 23 §3): a rendezvous tie goes to
    // the first, so a superset in its sources' order could name another home than the ring's.
    counted.sort_by(|a, b| super::record::ordinal_key(a).cmp(&super::record::ordinal_key(b)));
    counted[crate::ownership::ring_pick(prefix, &counted)].clone()
}

/// The outbox bound every writer of the overrides document keeps (§12.4).
const MAX_PENDING_EVENTS: usize = 64;

/// What one fleet tick read, as its moves plan over it.
pub(super) struct Tick<'a> {
    state: &'a AppState,
    /// This instance.
    me: &'a str,
    /// The ring's active members this tick published (§22 C3: the ring, not the candidates).
    active: Vec<String>,
    /// The count the tick's pass read (C6(b), C8, C11).
    count: u64,
    /// Fresh peers' (cpu, effective absorb lag): the health test of both moves.
    // mt-lint: allow(name-keyed-map): fleet instance -> its fresh load
    peer_load: &'a HashMap<String, (f64, u64)>,
    /// Each heartbeat's candidacy: the members' split reservations (S4-DESIGN §4.4).
    // mt-lint: allow(name-keyed-map): fleet instance -> its candidacy
    candidacies: &'a HashMap<String, Candidacy>,
}

impl AppState {
    /// The fleet tick's read, as its moves plan over it: this instance `me`, the ring the tick
    /// published from its pass's candidates and the overrides document (§22 C3: the moves plan
    /// over that ring, never over the candidates, which `fleet::start` passes unchanged), its fresh
    /// peers' load and every heartbeat's candidacy.
    pub(super) fn moves<'a>(
        &'a self,
        me: &'a str,
        // The moves plan over the ring (§22 C3), never over the candidates; of these only the
        // count is read (C6(b), C8, C11).
        candidates: &'a super::record::Candidates,
        // mt-lint: allow(name-keyed-map): fleet instance -> its fresh load
        peer_load: &'a HashMap<String, (f64, u64)>,
        // mt-lint: allow(name-keyed-map): fleet instance -> its candidacy
        candidacies: &'a HashMap<String, Candidacy>,
    ) -> Tick<'a> {
        Tick {
            state: self,
            me,
            active: self.ownership.ring_active(),
            count: candidates.count(),
            peer_load,
            candidacies,
        }
    }
}

impl Tick<'_> {
    /// Return-home's plan: the entries of `overrides` to drop this tick, by who may remove one
    /// (§22 C6): (a) the instance it names, by a planned move, giver first, returning the shard to
    /// its ring home once that home is a ring member within the count, fresh and healthy (C8, J5;
    /// R1d), past the hysteresis window (a pin has none, C7); (b) the instance it names, as a
    /// no-op, when it is the home over the ring with every ordinal within the count; (c) anyone,
    /// when it names an instance the document does not list (an older cell's document: no ring
    /// member); (d) the stale-leaf prune (`stale`). Nobody removes or rewrites an entry naming
    /// another listed member, whatever its beats say. Otherwise moves are sticky and an instance
    /// that lagged once is drained forever (ladder p3: streams-2 owned nothing by D3).
    pub(super) fn returns(&self, overrides: &Overrides) -> Vec<String> {
        let return_secs: i64 = self.state.config.fleet.rebalance_return_secs as i64;
        let mut drop_keys: Vec<String> = Vec::new();
        // mt-lint: allow(name-keyed-map): fleet instance -> shards this tick returns to it
        let mut pending_returns: HashMap<String, usize> = HashMap::new();
        // A live topology intent's sources keep their override (S4-DESIGN §4.4).
        for (prefix, entry) in &overrides.entries {
            let standing = self.state.fleet.standing();
            let skips = standing.return_home_skips(prefix, now_ms() - entry.ms, return_secs);
            if skips || standing.shielded(prefix) {
                continue;
            }
            if self.active.is_empty() {
                continue;
            }
            if void(overrides.members.as_deref(), &self.active, &entry.to) {
                drop_keys.push(prefix.clone()); // C6(c)
                continue;
            }
            if entry.to != self.me {
                continue; // only the instance an entry names removes or rewrites it
            }
            if counted_home(&self.active, overrides, self.count, prefix) == self.me {
                drop_keys.push(prefix.clone()); // C6(b): a no-op over the counted ordinals
                continue;
            }
            let home = self.active[crate::ownership::ring_pick(prefix, &self.active)].clone();
            if !returnable(&home, self.me, self.count) || !self.takes_back(&home, &pending_returns)
            {
                continue;
            }
            *pending_returns.entry(home).or_insert(0) += 1;
            drop_keys.push(prefix.clone());
        }
        drop_keys
    }

    /// Whether `home` is healthy and may take one more shard back on top of `pending_returns`.
    fn takes_back(&self, home: &str, pending_returns: &HashMap<String, usize>) -> bool {
        let shards = &self.state.shards;
        let peer_lag = self.peer_load.get(home).map(|(_, lag)| *lag);
        if !home_healthy(peer_lag, home, self.me, shards.stall_max_secs()) {
            return false;
        }
        // Load-aware return: giving the shard back must not push the home above its fair
        // share, or the rebalancer moves it right out again and the pair flap on a
        // ~return_secs period (FLEET-CAMPAIGN.md: 4 shards over 4 instances). A drained home
        // (the ladder-p3 case this block exists for) is below the mean and still gets its
        // shards back.
        let owned = shards
            .prefixes()
            .iter()
            .filter(|p| self.state.ownership.effective_owner(p).as_deref() == Some(home))
            .count();
        let gain = pending_returns.get(home).copied().unwrap_or(0)
            + 1
            + self.candidacies.reserved_of(home);
        within_share(
            owned,
            gain,
            shards.prefixes().len(),
            &self.active,
            self.count,
        )
    }

    /// The lag move's plan: this instance's laggiest shard (`AppState::lag_victim`, over the
    /// wedge the tick measured) and the coolest HEALTHY peer for it, as `(prefix, to)`. A peer
    /// that is itself lagging must not receive more work: under global backlog every instance
    /// breaches the threshold and unguarded moves just hand the backlog around (ladder p3: 7
    /// moves in 10 min, shard ping-pong, zero net absorption gained).
    pub(super) fn lag_plan(
        &self,
        wedge_prefix: &str,
        wedge_max_ms: i64,
    ) -> Option<(String, String)> {
        let threshold_secs = self.state.config.fleet.rebalance_lag_secs;
        // C11: no planned move names a listed member beyond the count.
        let counted = |name: &&String| super::record::within_count(name, self.count);
        let within: Vec<String> = self.active.iter().filter(counted).cloned().collect();
        let unreserved = self.candidacies.unreserved(&within);
        let target = pick_move_target(self.peer_load, &unreserved, self.me, threshold_secs);
        if target.is_none() {
            let (_, _, my_lag) = self.state.fleet_pressure();
            tracing::info!("rebalancer: lag {my_lag}s but no healthy peer; holding shards");
        }
        let victim = self.state.lag_victim(wedge_prefix, wedge_max_ms);
        Some((victim?, target?))
    }
}

impl Overrides {
    /// Return-home's commit: this document with each of `drop_keys` ([`Tick::returns`]) removed,
    /// by one CAS against `version`; the document as replaced, when it committed.
    pub(crate) async fn return_home(
        &self,
        drop_keys: &[String],
        state: &AppState,
        version: Option<UpdateVersion>,
    ) -> Option<Overrides> {
        let (handed, plain): (Vec<String>, Vec<String>) = drop_keys
            .iter()
            .cloned()
            .partition(|key| self.hands_back(key, state));
        let mut replaced = None;
        if !plain.is_empty() {
            let next = returned(self.clone(), &plain, now_ms());
            if replace(state, &next, version).await {
                tracing::info!(
                    "rebalancer: returned {} shard(s) to rendezvous owners: {:?}",
                    plain.len(),
                    plain
                );
                replaced = Some(next);
            }
        }
        for key in &handed {
            let change = super::handoff::Change::Return;
            let outcome = super::handoff::hand_off_paced(&state.fleet, key, change, false).await;
            tracing::info!(shard = %key, ?outcome, "rebalancer: returned a shard to its rendezvous owner");
        }
        replaced
    }

    /// Whether return-home gives `key` back by a handoff: its entry names this instance and the
    /// ring's home is another (F6). A no-op or a void entry is dropped by the plain CAS.
    fn hands_back(&self, key: &str, state: &AppState) -> bool {
        let me = state.config.cli.instance_name.as_str();
        let active = state.ownership.ring_active();
        let named_here = self.entries.get(key).is_some_and(|entry| entry.to == me);
        let home_here =
            !active.is_empty() && active[crate::ownership::ring_pick(key, &active)] == me;
        named_here && !home_here
    }

    /// The lag move's commit: `prefix` moved to `to` ([`Tick::lag_plan`]) by one CAS against
    /// `version`, then mirrored and retired here; whether it moved.
    pub(crate) async fn lag_move(
        self,
        (prefix, to): &(String, String),
        state: &AppState,
        version: Option<UpdateVersion>,
    ) -> bool {
        let change = super::handoff::Change::To(to.clone());
        match super::handoff::hand_off_paced(&state.fleet, prefix, change, true).await {
            super::handoff::Outcome::Moved => {
                tracing::info!("rebalancer: moved shard {prefix} -> {to} giver first");
                true
            }
            super::handoff::Outcome::Evacuate => {
                self.evacuate((prefix.as_str(), to.as_str()), state, version)
                    .await
            }
            outcome => {
                tracing::info!(shard = %prefix, ?outcome, "rebalancer: lag move not made");
                false
            }
        }
    }

    /// The lag move's evacuation of a wedged shard that cannot park or flush within its bound
    /// (F6 D4, H4; edge #266): today's CAS, mirror and retirement; the new owner fences the log.
    async fn evacuate(
        self,
        (prefix, to): (&str, &str),
        state: &AppState,
        version: Option<UpdateVersion>,
    ) -> bool {
        let next = super::count_balance::moved_document(self, prefix, to, now_ms());
        if !replace(state, &next, version).await {
            tracing::info!("rebalancer: overrides CAS lost; retry next tick");
            return false;
        }
        let (_, _, my_lag) = state.fleet_pressure();
        tracing::info!("rebalancer: moving shard {prefix} -> {to} (absorb lag {my_lag}s)");
        super::view::installed(&state.fleet, &state.ownership, &next);
        // A wedged shard stops serving at once: the new owner fences the log on its first routed
        // request.
        state.shards.retire(
            prefix,
            crate::shard_directory::RetirementReason::FleetEviction,
            |_, _| true,
        );
        true
    }
}

/// Whether a home owning `owned` of `leaves` may take `gain` more back: within the fair share
/// over the members of `ring` within `count` (Addendum 41 B item 2). A leaver beyond the count
/// stays in the ring until it releases.
fn within_share(owned: usize, gain: usize, leaves: usize, ring: &[String], count: u64) -> bool {
    return_home_allowed(
        owned,
        gain,
        leaves,
        super::record::sharing(ring, count).len(),
    )
}

/// Whether return-home may give a shard back to `home`: a home with a fresh heartbeat
/// (`peer_lag`) when it reports no absorb lag; one without, only when it is this instance (`me`)
/// and none of its shards stalls (`my_stall_secs`). Another member without a fresh heartbeat is
/// never healthy.
fn home_healthy(peer_lag: Option<u64>, home: &str, me: &str, my_stall_secs: u64) -> bool {
    peer_lag.map_or(home == me && my_stall_secs == 0, |lag| lag == 0)
}

/// CAS-replace the overrides document with `next`; whether it committed.
async fn replace(state: &AppState, next: &Overrides, version: Option<UpdateVersion>) -> bool {
    match serde_json::to_vec(next) {
        Ok(body) => {
            let document = FleetDocument::Overrides;
            state.fleet.replace_document(document, body, version).await
        }
        Err(error) => {
            tracing::warn!(%error, "rebalancer: overrides unserialisable");
            false
        }
    }
}

/// `overrides` with each of `drop_keys` removed and its `rebalance_return` event, within the
/// outbox bound.
pub(super) fn returned(mut overrides: Overrides, drop_keys: &[String], now_ms: i64) -> Overrides {
    for key in drop_keys {
        if overrides.pending_events.len() < MAX_PENDING_EVENTS {
            let id = format!("return/{key}/{}", now_ms / 1000);
            let event = crate::ops::OpsEvent::new("rebalance_return", id).shard(key);
            overrides.pending_events.push(event);
        }
        overrides.entries.remove(key);
    }
    overrides
}

/// C6(c): whether an entry naming `to` is void: the document lists `members` and not `to`, or (an
/// older cell's document, no `members`) `to` is not in the ring `active`.
fn void(members: Option<&[super::Member]>, active: &[String], to: &str) -> bool {
    match members {
        Some(listed) => listed.iter().all(|member| member.name != to),
        None => !active.iter().any(|name| name == to),
    }
}

/// Whether `me` may return a shard to its ring home `home`: another instance, within the count.
fn returnable(home: &str, me: &str, count: u64) -> bool {
    home != me && super::record::within_count(home, count)
}

#[cfg(test)]
mod tests {
    use super::{counted_home, home_healthy, returnable, returned, void, within_share};

    fn names(list: &[&str]) -> Vec<String> {
        list.iter().map(|name| (*name).to_string()).collect()
    }

    fn listing(list: &[&str]) -> Overrides {
        let member = |name: &&str| crate::fleet::Member {
            name: (*name).to_string(),
            boot_id: None,
        };
        let members = Some(list.iter().map(member).collect());
        Overrides {
            members,
            ..Overrides::default()
        }
    }

    /// C6(b)'s no-op is judged over the ring, every ordinal within the count and every listed
    /// member (Addendum 6): `10` is `streams-2`'s over `[1, 2]`, and `streams-3`'s once `streams-3`
    /// is counted, or listed beyond a count of two.
    #[test]
    fn a_no_op_is_judged_over_the_ring_the_ordinals_and_the_listed() {
        let (ring, one) = (names(&["streams-1", "streams-2"]), names(&["streams-1"]));
        let unlisted = Overrides::default();
        let three = listing(&["streams-1", "streams-2", "streams-3"]);
        let two = listing(&["streams-1", "streams-2"]);
        let homes = [
            counted_home(&ring, &unlisted, 2, "10"),
            counted_home(&ring, &unlisted, 3, "10"),
            counted_home(&ring, &three, 2, "10"),
            counted_home(&one, &two, 1, "10"),
        ];
        assert_eq!(homes, ["streams-2", "streams-3", "streams-3", "streams-2"]);
    }

    /// The counted ring is in ordinal order (Addendum 23 §3): `streams-112789` and `streams-349192`
    /// tie on the leaf `0`, and the home is the lower ordinal whatever order the ring and the
    /// listing give them in.
    #[test]
    fn a_counted_home_is_judged_in_ordinal_order_so_a_tie_has_one_home() {
        let (low, high) = ("streams-112789", "streams-349192");
        let homes = [
            counted_home(&names(&[low, high]), &Overrides::default(), 0, "0"),
            counted_home(&names(&[high, low]), &Overrides::default(), 0, "0"),
            counted_home(&names(&[high]), &listing(&[low]), 0, "0"),
        ];
        assert_eq!(homes, [low, low, low]);
    }

    #[test]
    fn a_home_is_healthy_by_its_fresh_lag_or_as_this_unstalled_instance() {
        let cases = [
            (Some(0), "s2", 9),
            (Some(3), "s1", 0),
            (None, "s1", 0),
            (None, "s1", 4),
            (None, "s2", 0),
        ];
        let healthy = cases.map(|(lag, home, stall)| home_healthy(lag, home, "s1", stall));
        assert_eq!(healthy, [true, false, true, false, false]);
    }
    use crate::fleet::{OverrideEntry, Overrides};

    #[test]
    fn a_return_drops_its_entries_and_records_each_within_the_outbox_bound() {
        let entry = |to: &str| OverrideEntry {
            to: to.to_owned(),
            ms: 1,
        };
        let mut doc = Overrides::default();
        doc.entries.insert("0".to_owned(), entry("streams-1"));
        doc.entries.insert("1".to_owned(), entry("streams-2"));
        let next = returned(doc.clone(), &["1".to_owned()], 5_000);
        assert_eq!(next.entries.keys().collect::<Vec<_>>(), ["0"]);
        let events: Vec<_> = next
            .pending_events
            .iter()
            .map(|e| e.event_id.clone())
            .collect();
        assert_eq!(events, ["return/1/5"]);
        doc.pending_events = (0..64)
            .map(|n| crate::ops::OpsEvent::new("x", format!("x/{n}")))
            .collect();
        let full = returned(doc, &["0".to_owned()], 5_000);
        assert_eq!((full.entries.len(), full.pending_events.len()), (1, 64));
    }

    /// C6(c): an entry is void when the document lists members and not its target, or, with no
    /// `members` (an older cell's), when its target is not in the ring (#183's survivors in
    /// `Tick::void`: the `!` deleted and the `==` as `!=`).
    #[test]
    fn an_entry_is_void_only_when_its_target_is_neither_listed_nor_ringed() {
        let ring = names(&["streams-1", "streams-2"]);
        let listed = listing(&["streams-1"]);
        let members = listed.members.as_deref();
        let got = [
            void(members, &ring, "streams-1"),
            void(members, &ring, "streams-2"),
            void(None, &ring, "streams-2"),
            void(None, &ring, "streams-3"),
        ];
        assert_eq!(
            got,
            [false, true, false, true],
            "(listed, unlisted, ringed, unringed)"
        );
    }

    /// A shard goes back only to a home that is another instance within the count (#183's
    /// survivor in `Tick::returns`: its first `||` as `&&`).
    #[test]
    fn a_shard_returns_only_to_another_home_within_the_count() {
        let got = [
            returnable("streams-2", "streams-1", 2),
            returnable("streams-1", "streams-1", 2),
            returnable("streams-3", "streams-1", 2),
        ];
        assert_eq!(
            got,
            [true, false, false],
            "(another within, itself, beyond the count)"
        );
    }

    /// Return-home's share divides by the ring's members within the count (Addendum 41 B item 2):
    /// six leaves over `streams-1` and `streams-2` within a count of two give a share of three, so
    /// a home holding two takes one back though `streams-3`, beyond the count, is still in the ring.
    #[test]
    fn return_homes_share_divides_by_the_members_within_the_count() {
        let ring = names(&["streams-1", "streams-2", "streams-3"]);
        assert!(within_share(2, 1, 6, &ring, 2), "a share of three");
        assert!(!within_share(3, 1, 6, &ring, 2), "past the share of three");
        assert!(
            !within_share(2, 1, 6, &ring, 3),
            "within a count of three: two"
        );
        assert!(!within_share(0, 1, 6, &[], 2), "no ring");
    }
}
