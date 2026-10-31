//! Count rebalance: the ring's rendezvous hash spreads shards unevenly over
//! members whose names differ only in a digit (FNV-1a scores of
//! `"<shard> streams-<n>"` are correlated; 64 shards over 8 members gave
//! one member 33), and the lag-driven rebalancer moves nothing while no
//! shard stalls. An instance that owns more than the fair share
//! ceil(shards / members) moves one shard per move cooldown to a member
//! below the share, through the same override a lag move writes, until
//! every member is within the share. Return-home admits a
//! return only within the same share ([`super::return_home_allowed`]), so
//! the two passes never undo each other.

use std::collections::HashMap;

/// The move that brings `me` one shard closer to the fair share, given each
/// shard's owner: one of `me`'s shards, preferring one whose rendezvous home
/// is the target, to a healthy active member below the share. Members past
/// the share move at the same ticks, so each takes a different target: the
/// one at its own rank, by name, among the members past the share, of the
/// members below it ordered by fewest shards. Only a shard `movable` admits
/// moves: one with no override, so a count move never undoes a lag move
/// or another count move. None when `me` is within the share, or no member
/// is below it, or none of its shards may move.
pub(crate) fn pick(
    owners: &[(String, String)],
    active: &[String],
    me: &str,
    healthy: impl Fn(&str) -> bool,
    movable: impl Fn(&str) -> bool,
) -> Option<(String, String)> {
    if active.is_empty() || !active.iter().any(|m| m == me) {
        return None;
    }
    let share = owners.len().div_ceil(active.len());
    let mut counts: HashMap<&str, usize> = active.iter().map(|m| (m.as_str(), 0)).collect();
    for (_, owner) in owners {
        if let Some(count) = counts.get_mut(owner.as_str()) {
            *count += 1;
        }
    }
    let count = |member: &str| counts.get(member).copied().unwrap_or(0);
    if count(me) <= share {
        return None;
    }
    let mut over: Vec<&str> = active
        .iter()
        .map(String::as_str)
        .filter(|m| count(m) > share)
        .collect();
    over.sort_unstable();
    let rank = over.iter().position(|m| *m == me)?;
    let mut below: Vec<&String> = active
        .iter()
        .filter(|m| m.as_str() != me && healthy(m) && count(m) < share)
        .collect();
    below.sort_unstable_by_key(|m| (count(m), m.as_str()));
    let to = *below.get(rank % below.len().max(1))?;
    let mut mine: Vec<&str> = owners
        .iter()
        .filter(|(prefix, owner)| owner == me && movable(prefix))
        .map(|(prefix, _)| prefix.as_str())
        .collect();
    mine.sort_unstable();
    let homeward = mine
        .iter()
        .find(|prefix| active[crate::ownership::ring_pick(prefix, active)] == *to);
    let prefix = homeward.or(mine.first())?;
    Some(((*prefix).to_owned(), to.clone()))
}

/// [`pick`] over this instance's view: every shard's effective owner, and
/// the peers that are not lagging (lag at most the rebalance threshold) as
/// healthy. A count move hands over a share of the work, not a backlog, so
/// the lag move's stricter half-threshold rule does not apply.
pub(crate) fn plan(
    (shards, ownership, overrides): (
        &crate::shard_directory::ShardDirectory,
        &crate::ownership::OwnershipService,
        &super::Overrides,
    ),
    active: &[String],
    me: &str,
    peer_load: &HashMap<String, (f64, u64)>,
    lag_threshold_secs: u64,
) -> Option<(String, String)> {
    let owners: Vec<(String, String)> = shards
        .prefixes()
        .iter()
        .filter_map(|prefix| Some((prefix.clone(), ownership.effective_owner(prefix)?)))
        .collect();
    pick(
        &owners,
        active,
        me,
        |member| {
            peer_load
                .get(member)
                .is_some_and(|(_, lag)| *lag <= lag_threshold_secs)
        },
        |prefix| movable(overrides, prefix),
    )
}

/// Whether a count move may move `prefix`: it has no override, or its
/// override is a count move's own placement. A lag move's override stays
/// until return-home drops it.
fn movable(overrides: &super::Overrides, prefix: &str) -> bool {
    !overrides.entries.contains_key(prefix) || overrides.counted.contains(prefix)
}

#[cfg(test)]
mod tests {
    use super::{movable, pick};

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

    /// One round: each member makes the move it would, against the owners
    /// as the moves before it left them. The number of moves.
    fn round(owners: &mut [(String, String)], active: &[String]) -> usize {
        let mut moves = 0;
        for me in active {
            let Some((prefix, to)) = pick(owners, active, me, |_| true, |_| true) else {
                continue;
            };
            owners
                .iter_mut()
                .filter(|entry| entry.0 == prefix)
                .for_each(|entry| entry.1.clone_from(&to));
            moves += 1;
        }
        moves
    }

    /// Apply rounds until none moves: the largest count left and the
    /// number of moves.
    fn converge(active: &[String]) -> (usize, usize) {
        let mut owners = ring_owners(active);
        let mut moves = 0;
        loop {
            let made = round(&mut owners, active);
            if made == 0 {
                break;
            }
            moves += made;
        }
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
    fn a_count_placement_may_move_again_and_a_lag_move_may_not() {
        let entry = || super::super::OverrideEntry {
            to: "streams-2".to_owned(),
            ms: 0,
        };
        let mut overrides = super::super::Overrides::default();
        overrides.entries.insert("00".to_owned(), entry());
        overrides.entries.insert("01".to_owned(), entry());
        overrides.counted.insert("00".to_owned());
        assert!(movable(&overrides, "00"), "a count move's placement");
        assert!(!movable(&overrides, "01"), "a lag move's override");
        assert!(movable(&overrides, "10"), "no override");
    }

    #[test]
    fn members_past_the_share_move_to_different_targets_at_once() {
        let active = members(4);
        // streams-1 and streams-2 hold 7 each of 16; the share is 4.
        let owners: Vec<(String, String)> = (0..16)
            .map(|k| {
                let owner = match k {
                    0..7 => &active[0],
                    7..14 => &active[1],
                    14 => &active[2],
                    _ => &active[3],
                };
                (format!("{k:04b}"), owner.clone())
            })
            .collect();
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
        let skewed: Vec<(String, String)> = (0..4)
            .map(|k| (format!("{k:02b}"), active[0].clone()))
            .collect();
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
            pick(
                &skewed,
                &active,
                "streams-1",
                |_| true,
                |prefix| prefix == "11"
            ),
            Some(("11".to_owned(), "streams-2".to_owned())),
            "only a shard without an override moves"
        );
        assert_eq!(
            pick(&skewed, &active, "streams-1", |_| true, |_| false),
            None,
            "every shard has an override"
        );
    }
}
