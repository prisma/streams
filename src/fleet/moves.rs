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

/// The outbox bound every writer of the overrides document keeps (§12.4).
const MAX_PENDING_EVENTS: usize = 64;

/// What one fleet tick read, as its moves plan over it.
pub(super) struct Tick<'a> {
    state: &'a AppState,
    /// This instance.
    me: &'a str,
    /// The ring's active members this tick published.
    active: &'a [String],
    /// Fresh peers' (cpu, effective absorb lag): the health test of both moves.
    // mt-lint: allow(name-keyed-map): fleet instance -> its fresh load
    peer_load: &'a HashMap<String, (f64, u64)>,
    /// Each heartbeat's candidacy: the members' split reservations (S4-DESIGN §4.4).
    // mt-lint: allow(name-keyed-map): fleet instance -> its candidacy
    candidacies: &'a HashMap<String, Candidacy>,
}

impl AppState {
    /// The fleet tick's read, as its moves plan over it: this instance `me`, the `active` ring it
    /// published, its fresh peers' load and every heartbeat's candidacy.
    pub(super) fn moves<'a>(
        &'a self,
        me: &'a str,
        active: &'a [String],
        // mt-lint: allow(name-keyed-map): fleet instance -> its fresh load
        peer_load: &'a HashMap<String, (f64, u64)>,
        // mt-lint: allow(name-keyed-map): fleet instance -> its candidacy
        candidacies: &'a HashMap<String, Candidacy>,
    ) -> Tick<'a> {
        Tick {
            state: self,
            me,
            active,
            peer_load,
            candidacies,
        }
    }
}

impl Tick<'_> {
    /// Return-home's plan: the entries of `overrides` to drop this tick. An override drops once
    /// the rendezvous owner is fresh and healthy again and the entry has aged past the hysteresis
    /// window, otherwise moves are sticky and an instance that lagged once is drained forever
    /// (ladder p3: streams-2 owned nothing by D3).
    pub(super) fn returns(&self, overrides: &Overrides) -> Vec<String> {
        let return_secs: i64 = self.state.config.fleet.rebalance_return_secs as i64;
        let mut drop_keys: Vec<String> = Vec::new();
        // mt-lint: allow(name-keyed-map): fleet instance -> shards this tick returns to it
        let mut pending_returns: HashMap<String, usize> = HashMap::new();
        // A live topology intent's sources keep their override (S4-DESIGN §4.4).
        for (prefix, entry) in &overrides.entries {
            let standing = self.state.fleet.standing();
            if standing.return_home_skips(prefix, now_ms() - entry.ms, return_secs) {
                continue;
            }
            if self.active.is_empty() {
                continue;
            }
            let home = self.active[crate::ownership::ring_pick(prefix, self.active)].clone();
            if home == entry.to {
                drop_keys.push(prefix.clone()); // override is a no-op
                continue;
            }
            if !self.takes_back(&home, &pending_returns) {
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
        return_home_allowed(owned, gain, shards.prefixes().len(), self.active.len())
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
        let unreserved = self.candidacies.unreserved(self.active);
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
        let next = returned(self.clone(), drop_keys, now_ms());
        if !replace(state, &next, version).await {
            return None;
        }
        tracing::info!(
            "rebalancer: returned {} shard(s) to rendezvous owners: {:?}",
            drop_keys.len(),
            drop_keys
        );
        Some(next)
    }

    /// The lag move's commit: `prefix` moved to `to` ([`Tick::lag_plan`]) by one CAS against
    /// `version`, then mirrored and retired here; whether it moved.
    pub(crate) async fn lag_move(
        self,
        (prefix, to): &(String, String),
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
        state.ownership.set_override(prefix, to);
        // Stop serving immediately and fail all in-flight work fast (retryable): the new owner
        // fences the log on first routed request.
        state.shards.retire(
            prefix,
            crate::shard_directory::RetirementReason::FleetEviction,
            |_, _| true,
        );
        true
    }
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
fn returned(mut overrides: Overrides, drop_keys: &[String], now_ms: i64) -> Overrides {
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

#[cfg(test)]
mod tests {
    use super::{home_healthy, returned};

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
}
