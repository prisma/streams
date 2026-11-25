//! Who takes the shards of a member that is departing (C11; GATE-DECISION-s22-join-2026-11-20
//! Addendum 41 B): a planned drain, and the leave of a member beyond the desired count, choose one
//! of the same set.

use super::super::record::{Candidates, boot_of, within_count};
use super::super::{Member, Overrides};

/// The members a departing `candidates.me()` may hand a shard to, in the ring's order: those the
/// document's ring holds, whose ordinal is within the count its pass read, and whose beat of the
/// listed boot this instance holds and judges qualifying (R1d), itself left out. Empty while every
/// such member lacks one: the caller hands nothing off.
pub(in crate::fleet) fn successors(candidates: &Candidates, document: &Overrides) -> Vec<String> {
    let members = document.members.as_deref();
    let held = |name: &String| match members {
        Some(listed) => listed
            .iter()
            .any(|member| member.name == *name && holds(candidates, member)),
        // An older cell's document lists nobody (C17): its ring is today's, of qualifying beats.
        None => candidates.contains(name),
    };
    let mut ring = candidates.ring(members);
    ring.retain(|name| {
        name != candidates.me() && within_count(name, candidates.count()) && held(name)
    });
    ring
}

/// R1d: `candidates` holds a qualifying beat of the boot the document lists for `member`.
fn holds(candidates: &Candidates, member: &Member) -> bool {
    let listed = member.boot_id.as_deref().map(boot_of);
    let beat = candidates.beat_of(&member.name);
    listed.is_some_and(|boot| beat.is_some_and(|(held, qualifies)| held == boot && qualifies))
}

#[cfg(test)]
mod tests {
    use super::successors;
    use crate::fleet::record::{Candidates, boot_of};
    use crate::fleet::{Member, Overrides};

    fn member(name: &str, boot: &str) -> Member {
        Member {
            name: name.to_string(),
            boot_id: Some(boot.to_string()),
        }
    }

    fn document(members: Vec<Member>) -> Overrides {
        Overrides {
            members: Some(members),
            ..Overrides::default()
        }
    }

    /// `me` at `count`, holding `(name, boot, qualifies)` beats.
    fn reader(count: u64, me: &str, beats: &[(&str, &str, bool)]) -> Candidates {
        let held = beats
            .iter()
            .map(|(name, boot, ok)| ((*name).to_string(), (boot_of(boot), *ok)))
            .collect();
        Candidates::held(count, me, held).with_boot("me")
    }

    fn named(list: &[&str]) -> Vec<String> {
        list.iter().map(|name| (*name).to_string()).collect()
    }

    /// The five-to-two case: `streams-3`, `4` and `5` are listed and alive and beyond the count, so
    /// a drain of `streams-1` hands to `streams-2` alone.
    #[test]
    fn a_member_beyond_the_count_is_no_successor() {
        let listed: Vec<Member> = (1..=5)
            .map(|n| member(&format!("streams-{n}"), "b"))
            .collect();
        let beats: Vec<_> = (1..=5).map(|n| (format!("streams-{n}"), "b")).collect();
        let held: Vec<(&str, &str, bool)> = beats
            .iter()
            .map(|(name, boot)| (name.as_str(), *boot, true))
            .collect();
        let me = reader(2, "streams-1", &held);
        assert_eq!(successors(&me, &document(listed)), named(&["streams-2"]));
    }

    /// R1d: a member whose beat is missing, is another boot's, or does not qualify is no successor,
    /// though the ring still holds it; a listing without a boot id is none either.
    #[test]
    fn a_successor_needs_a_qualifying_beat_of_its_listed_boot() {
        let mut listed: Vec<Member> = ["streams-2", "streams-3", "streams-4", "streams-5"]
            .iter()
            .map(|name| member(name, "b"))
            .collect();
        listed.push(Member {
            name: "streams-6".to_string(),
            boot_id: None,
        });
        let beats = [
            ("streams-2", "b", true),
            ("streams-3", "other", true),
            ("streams-4", "b", false),
            ("streams-6", "b", true),
        ];
        let me = reader(6, "streams-1", &beats);
        assert_eq!(successors(&me, &document(listed)), named(&["streams-2"]));
    }

    /// Nobody left: the last member within the count has no successor.
    #[test]
    fn the_last_member_within_the_count_has_no_successor() {
        let listed = vec![member("streams-1", "b"), member("streams-2", "b")];
        let beats = [("streams-1", "b", true), ("streams-2", "b", true)];
        let me = reader(1, "streams-1", &beats);
        assert_eq!(successors(&me, &document(listed)), Vec::<String>::new());
    }

    /// A document without `members` (an older cell's): the qualifying ordinals within the count.
    #[test]
    fn an_unlisted_document_gives_the_qualifying_ordinals() {
        let beats = [
            ("streams-1", "b", true),
            ("streams-2", "b", true),
            ("streams-3", "b", false),
        ];
        let me = reader(3, "streams-1", &beats);
        let document = Overrides::default();
        assert_eq!(successors(&me, &document), named(&["streams-2"]));
    }
}
