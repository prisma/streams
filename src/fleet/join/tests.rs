#![cfg(test)]
//! The join's units (§22 step 5; the ruling's §9 "units": coverage, the pin plan, C6's cases are
//! `moves`', C13's table with R1a).

use std::collections::HashMap;

use super::{
    Facts, Write, answered, in_place, may_beat, may_join, owner, pins, plan, since_rise_ms,
    uncovered, unstamped,
};
use crate::fleet::record::{Candidates, boot_of};
use crate::fleet::{Heartbeat, Member, OverrideEntry, Overrides};

fn names(list: &[&str]) -> Vec<String> {
    list.iter().map(|name| (*name).to_string()).collect()
}

fn entries(list: &[(&str, &str)]) -> HashMap<String, OverrideEntry> {
    let entry = |(leaf, to): &(&str, &str)| {
        let to = (*to).to_string();
        ((*leaf).to_string(), OverrideEntry { to, ms: 1 })
    };
    list.iter().map(entry).collect()
}

fn member(name: &str, boot: Option<&str>) -> Member {
    Member {
        name: name.to_string(),
        boot_id: boot.map(str::to_string),
    }
}

/// `me` at boot `boot`, holding its own beat of that boot and a live beat of each of `peers`.
fn reader(count: u64, me: &str, boot: &str, peers: &[&str]) -> Candidates {
    let mut held = vec![(me.to_string(), (boot_of(boot), true))];
    held.extend(
        peers
            .iter()
            .map(|peer| ((*peer).to_string(), (boot_of(peer), true))),
    );
    Candidates::held(count, me, held).with_boot(boot)
}

fn document(members: Vec<Member>, list: &[(&str, &str)]) -> Overrides {
    Overrides {
        entries: entries(list),
        members: Some(members),
        ..Overrides::default()
    }
}

/// The sixteen two-hex leaves, enough for every instance to win some.
fn leaves() -> Vec<String> {
    (0..16).map(|n| format!("{n:02x}")).collect()
}

/// R2: an unpinned leaf the joiner would win is uncovered; pinned to a ring member, covered; an
/// entry naming the joiner is uncovered until C6(c) drops it. The model's `Uncovered` "evidence".
#[test]
fn coverage_counts_every_leaf_whose_owner_the_joiner_changes() {
    let ring = names(&["streams-1"]);
    let all = leaves();
    let won: Vec<String> = all
        .iter()
        .filter(|leaf| {
            owner(leaf, &names(&["streams-1", "streams-2"]), &HashMap::new()).as_deref()
                == Some("streams-2")
        })
        .cloned()
        .collect();
    assert!(!won.is_empty());
    assert_eq!(uncovered(&all, &ring, "streams-2", &HashMap::new()), won);
    let pinned: Vec<(&str, &str)> = won
        .iter()
        .map(|leaf| (leaf.as_str(), "streams-1"))
        .collect();
    assert!(uncovered(&all, &ring, "streams-2", &entries(&pinned)).is_empty());
    let named = [(won[0].as_str(), "streams-2")];
    assert!(uncovered(&all, &ring, "streams-2", &entries(&named)).contains(&won[0]));
}

/// C7: a member pins exactly the leaves the document gives it that the ring with the unlisted
/// ordinals would give another, once; a leaf already pinned to it gets no second entry.
#[test]
fn a_member_pins_what_the_prospective_ring_would_move() {
    let all = leaves();
    let ring = names(&["streams-1"]);
    let prospective = names(&["streams-1", "streams-2"]);
    let moving = uncovered(&all, &ring, "streams-2", &HashMap::new());
    assert_eq!(
        pins(&all, &ring, &prospective, "streams-1", &HashMap::new()),
        moving
    );
    let pinned: Vec<(&str, &str)> = moving
        .iter()
        .map(|leaf| (leaf.as_str(), "streams-1"))
        .collect();
    assert!(pins(&all, &ring, &prospective, "streams-1", &entries(&pinned)).is_empty());
}

/// C13's table on the previous beat: young, not draining and not withdrawn is in place; no beat, a
/// draining one, a withdrawn one or one too old to stay live through the first beat is a joiner.
#[test]
fn a_restart_is_in_place_only_on_a_young_beat_that_left_nothing() {
    let now = 1_000_000;
    let beat = |age_ms: i64, draining: bool, withdrawn: bool| {
        let document =
            r#"{"instance":"streams-1","ts_ms":0,"rps":0.0,"owned_shards":[],"draining":false}"#;
        let mut beat: Heartbeat = serde_json::from_str(document).expect("a minimal beat");
        beat.ts_ms = now - age_ms;
        beat.draining = draining;
        beat.withdrawn = withdrawn.then(|| "stopping".to_string());
        beat
    };
    let cases = [
        (None, false),
        (Some(beat(1_000, false, false)), true),
        (Some(beat(1_000, true, false)), false),
        (Some(beat(1_000, false, true)), false),
        (Some(beat(20_000, false, false)), false),
    ];
    for (previous, want) in cases {
        assert_eq!(
            in_place(previous.as_ref(), now),
            want,
            "{:?}",
            previous.map(|b| b.ts_ms)
        );
    }
}

/// C5 with R3: a joiner within the count waits while something is uncovered, joins with its boot
/// id when nothing is, and past `JOIN_PHASE` joins anyway naming what it left uncovered; beyond
/// the count, draining, or before it read its own beat of this boot, it never joins.
#[test]
fn a_joiner_joins_only_covered_within_the_count_and_under_r3() {
    let all = leaves();
    let doc = document(vec![member("streams-1", Some("streams-1"))], &[]);
    let c = reader(2, "streams-2", "b2", &["streams-1"]);
    let facts = |may_join, phase_over| Facts {
        candidates: &c,
        leaves: &all,
        may_join,
        young: Some(false),
        member: false,
        phase_over,
    };
    let waiting = plan(&facts(true, false), &doc, "b2");
    assert!(waiting.next.is_none() && waiting.uncovered_now);
    let late = plan(&facts(true, true), &doc, "b2");
    let Some((joined, Write::Joined { uncovered })) = late.next else {
        panic!("the fallback joins");
    };
    assert!(!uncovered.is_empty());
    assert_eq!(joined.members.unwrap()[1], member("streams-2", Some("b2")));
    assert!(
        plan(&facts(false, true), &doc, "b2").next.is_none(),
        "R3: a drain never joins"
    );
    let pinned: Vec<(&str, &str)> = uncovered
        .iter()
        .map(|leaf| (leaf.as_str(), "streams-1"))
        .collect();
    let covered = document(vec![member("streams-1", Some("streams-1"))], &pinned);
    let now = plan(&facts(true, false), &covered, "b2");
    assert!(matches!(now.next, Some((_, Write::Joined { ref uncovered })) if uncovered.is_empty()));
    let beyond = reader(1, "streams-2", "b2", &["streams-1"]);
    let facts = Facts {
        candidates: &beyond,
        leaves: &all,
        may_join: true,
        young: None,
        member: false,
        phase_over: true,
    };
    assert!(
        plan(&facts, &doc, "b2").next.is_none(),
        "C5: its ordinal beyond the count"
    );
    let unbeaten = Candidates::held(2, "streams-2", vec![]).with_boot("b2");
    let facts = Facts {
        candidates: &unbeaten,
        leaves: &all,
        may_join: true,
        young: None,
        member: false,
        phase_over: true,
    };
    assert!(
        plan(&facts, &doc, "b2").next.is_none(),
        "R3: no beat of this boot read"
    );
}

/// C13 and R1a: a restart listed with another boot replaces it when young (R3 met), removes its
/// listing when not, and does nothing while undecided. An unstamped listing of its own name (the
/// interim's) is another boot's (the join ruling §2): stamped only when young and R3's own beat is
/// read, else unlisted, never membership by the listing alone. An older cell's document is listed
/// by a member of today's ring (C17).
#[test]
fn a_listed_instance_decides_stamps_and_lists_as_c13_and_c17_rule() {
    let all = leaves();
    let c = reader(2, "streams-1", "a2", &["streams-2"]);
    let facts = |young| Facts {
        candidates: &c,
        leaves: &all,
        may_join: true,
        young,
        member: false,
        phase_over: false,
    };
    let restarted = document(
        vec![
            member("streams-1", Some("a1")),
            member("streams-2", Some("streams-2")),
        ],
        &[],
    );
    let in_place = plan(&facts(Some(true)), &restarted, "a2")
        .next
        .expect("in place");
    assert_eq!(
        (in_place.1, in_place.0.members.unwrap()[0].clone()),
        (Write::InPlace, member("streams-1", Some("a2")))
    );
    let unlisted = plan(&facts(Some(false)), &restarted, "a2")
        .next
        .expect("unlisted");
    assert_eq!(
        (unlisted.1, unlisted.0.members.unwrap().len()),
        (Write::Unlisted, 1)
    );
    assert!(
        plan(&facts(None), &restarted, "a2").next.is_none(),
        "undecided"
    );
    let interim = document(
        vec![member("streams-1", None), member("streams-2", None)],
        &[],
    );
    let what = |young| plan(&facts(young), &interim, "a2").next.map(|next| next.1);
    assert_eq!(
        [what(Some(true)), what(Some(false)), what(None)],
        [Some(Write::Stamped), Some(Write::Unlisted), None]
    );
    let unbeaten = Candidates::held(2, "streams-1", vec![]).with_boot("a2");
    let facts_unbeaten = Facts {
        candidates: &unbeaten,
        leaves: &all,
        may_join: true,
        young: Some(true),
        member: false,
        phase_over: false,
    };
    assert_eq!(
        plan(&facts_unbeaten, &interim, "a2")
            .next
            .map(|next| next.1),
        Some(Write::Stamped),
        "the stamp needs R3's first two clauses only: this boot writes no beat before it"
    );
    let older = Overrides::default();
    let listed = plan(&facts(None), &older, "a2").next.expect("C17");
    assert_eq!(
        (listed.1, listed.0.members.unwrap()),
        (
            Write::Listed,
            vec![member("streams-1", Some("a2")), member("streams-2", None)]
        )
    );
}

/// C7 and the interim's removal half: a member pins in one CAS and drops the listed names beyond
/// the count in the same one; with nothing to pin and nobody beyond, it writes nothing.
#[test]
fn a_member_pins_and_unlists_beyond_the_count_in_one_write() {
    // Enough leaves that `streams-2`, unlisted within the count, wins some of `streams-1`'s over
    // the ring with `streams-4`. Not `streams-3`: FNV-1a orders names that differ in their last
    // byte by its low bits, and `streams-1` beats `streams-2` on every leaf `streams-3` loses.
    let all: Vec<String> = (0..256).map(|n| format!("{n:03x}")).collect();
    let c = reader(2, "streams-1", "a1", &["streams-4"]);
    let facts = Facts {
        candidates: &c,
        leaves: &all,
        may_join: true,
        young: Some(true),
        member: false,
        phase_over: false,
    };
    let doc = document(
        vec![
            member("streams-1", Some("a1")),
            member("streams-4", Some("streams-4")),
        ],
        &[],
    );
    let Some((next, Write::Pinned(pinned, beyond))) = plan(&facts, &doc, "a1").next else {
        panic!("a member writes");
    };
    assert_eq!(beyond, names(&["streams-4"]));
    assert!(
        !pinned.is_empty(),
        "streams-2, unlisted within the count, would win some"
    );
    assert!(
        pinned
            .iter()
            .all(|leaf| next.entries[leaf].to == "streams-1"
                && next.entries[leaf].ms == super::PIN_MS)
    );
    assert_eq!(next.members.unwrap(), vec![member("streams-1", Some("a1"))]);
}

/// The interim's removal half unlists only for the count (the join ruling §3): a listed member
/// within the count stays listed when its beat of its listed boot is dead (it leaves each ring on
/// that evidence, R1); one beyond the count is unlisted whatever its beats say.
#[test]
fn the_removal_half_unlists_only_beyond_the_count_never_on_liveness() {
    let all = leaves();
    let held = vec![
        ("streams-1".to_string(), (boot_of("a1"), true)),
        ("streams-2".to_string(), (boot_of("b1"), false)),
        ("streams-3".to_string(), (boot_of("c1"), true)),
    ];
    let c = Candidates::held(2, "streams-1", held).with_boot("a1");
    let facts = Facts {
        candidates: &c,
        leaves: &all,
        may_join: true,
        young: Some(true),
        member: false,
        phase_over: false,
    };
    let doc = document(
        vec![
            member("streams-1", Some("a1")),
            member("streams-2", Some("b1")),
            member("streams-3", Some("c1")),
        ],
        &[],
    );
    let Some((next, Write::Pinned(_, beyond))) = plan(&facts, &doc, "a1").next else {
        panic!("a member unlists beyond the count");
    };
    let kept: Vec<String> = next.members.unwrap().into_iter().map(|m| m.name).collect();
    assert_eq!(
        (beyond, kept),
        (names(&["streams-3"]), names(&["streams-1", "streams-2"]))
    );
    assert_eq!(
        c.ring(Some(&doc.members.unwrap())),
        names(&["streams-1", "streams-3"])
    );
}

/// R3's stop is one-way (the join ruling §5): once a planned drain has begun handing off, the
/// drain flag never clears, a drain that ended `Kept` included, so no later pass of this process
/// joins or decides in place.
#[test]
fn a_begun_drain_keeps_r3_false_on_every_later_pass() {
    let standing = crate::fleet::standing::Standing::default();
    assert!(!standing.drain_begun());
    standing.begin_handing_off_all();
    let later: Vec<bool> = (0..3).map(|_| standing.drain_begun()).collect();
    assert_eq!(later, [true, true, true]);
}

/// The hold (the join ruling, Addendum 2): a boot beats once a document it read shows its decision.
/// Unlisted (or a document with no members): at once. Listed with its own boot id: decided.
/// Listed unstamped, or with another boot id: held.
#[test]
fn a_boot_beats_only_once_its_decision_is_read_back() {
    let c = reader(2, "streams-2", "b2", &["streams-1"]);
    let one = member("streams-1", Some("streams-1"));
    let cases = [
        (Overrides::default(), true),
        (document(vec![one.clone()], &[]), true),
        (
            document(vec![one.clone(), member("streams-2", Some("b2"))], &[]),
            true,
        ),
        (
            document(vec![one.clone(), member("streams-2", None)], &[]),
            false,
        ),
        (
            document(vec![one, member("streams-2", Some("b1"))], &[]),
            false,
        ),
    ];
    let got: Vec<bool> = cases.iter().map(|(doc, _)| may_beat(&c, doc)).collect();
    let want: Vec<bool> = cases.iter().map(|(_, want)| *want).collect();
    assert_eq!(got, want);
}

/// The `fleet joined` line's `rise_ms`: from desired.json's `computed_at_ms` to now, -1 with no
/// desired document read.
#[test]
fn the_rise_is_timed_from_the_desired_documents_computation() {
    let got = [
        since_rise_ms(Some(1_000), 4_250),
        since_rise_ms(None, 4_250),
    ];
    assert_eq!(got, [3_250, -1]);
}

/// A join CAS's answer over a store holding `stored` (`listing` `streams-2`), for a `next` that
/// lists `streams-1`: the members of the document it leaves, its version's ETag, and the stored
/// document's ETag.
async fn answered_over(
    written: Option<object_store::UpdateVersion>,
) -> (Vec<String>, Option<String>, Option<String>) {
    use object_store::ObjectStoreExt as _;
    let store: std::sync::Arc<dyn object_store::ObjectStore> =
        std::sync::Arc::new(object_store::memory::InMemory::new());
    let path = object_store::path::Path::from("fleet/overrides.json");
    let stored = br#"{"entries":{},"members":[{"name":"streams-2","boot_id":"b-2"}]}"#;
    let put = store.put(&path, stored.to_vec().into()).await.unwrap();
    let fleet = crate::fleet::FleetRepository::new(Some(store));
    let next = document(vec![member("streams-1", Some("b-1"))], &[]);
    let read = (Overrides::default(), None);
    let (left, version) = answered(&fleet, read, next, written).await;
    let members = left.members.unwrap_or_default();
    let names = members.into_iter().map(|member| member.name).collect();
    (names, version.and_then(|version| version.e_tag), put.e_tag)
}

/// A committed join CAS is published at the version the store answered, with no read (PR #173's
/// survivors in step 4's `record::listed`, carried to the join's one CAS: the guard as `false`, or
/// its `||` as `&&` with an answer carrying an ETag and no version, reads the stored document).
#[tokio::test(crate = "crate::test_rt")]
async fn a_committed_join_cas_answers_its_version_without_a_read() {
    let answer = object_store::UpdateVersion {
        e_tag: Some("e-next".to_string()),
        version: None,
    };
    let (names, version, _) = answered_over(Some(answer)).await;
    assert_eq!(
        (names, version),
        (vec!["streams-1".to_string()], Some("e-next".to_string()))
    );
}

/// A join CAS answered with neither an ETag nor a version, or lost, is read back, so the next CAS
/// is anchored on the stored version (the survivor guard as `true` would answer `next` unversioned).
#[tokio::test(crate = "crate::test_rt")]
async fn a_join_cas_answered_without_a_version_or_lost_is_read_back() {
    let bare = object_store::UpdateVersion {
        e_tag: None,
        version: None,
    };
    for written in [Some(bare), None] {
        let (names, version, stored) = answered_over(written).await;
        assert!(stored.is_some());
        assert_eq!((names, version), (vec!["streams-2".to_string()], stored));
    }
}

/// C18 (the join ruling, Addendum 33): a member found unstamped stamps its own boot id back in
/// place, whatever C13's boot-time verdict (and whatever the count:
/// `a_member_beyond_the_count_found_unstamped_stamps_itself_back`), and keeps every entry and every
/// other listing as read. Unchanged, each by its own rule: an undecided boot (C13), a listing with another boot id (R1a),
/// a name removed outright (a joiner, never restamped), and a document without `members` (C17's
/// listing, this writer stamped).
#[test]
fn a_member_found_unstamped_stamps_itself_back_and_no_other_branch_changes() {
    let all = leaves();
    let c = reader(2, "streams-1", "a1", &["streams-2"]);
    let facts = |young, member| Facts {
        candidates: &c,
        leaves: &all,
        may_join: true,
        young,
        member,
        phase_over: false,
    };
    let what = |young, member, doc: &Overrides| plan(&facts(young, member), doc, "a1").next;
    let peer = member("streams-2", Some("streams-2"));
    let unstamped = document(
        vec![member("streams-1", None), peer.clone()],
        &[("00", "streams-2")],
    );
    let shape = |doc: &Overrides| {
        let mut entries: Vec<(String, String, i64)> = doc
            .entries
            .iter()
            .map(|(leaf, entry)| (leaf.clone(), entry.to.clone(), entry.ms))
            .collect();
        entries.sort();
        entries
    };
    for young in [Some(false), Some(true), None] {
        let (next, why) = what(young, true, &unstamped).expect("restamped");
        let restored = vec![member("streams-1", Some("a1")), peer.clone()];
        assert_eq!(
            (why, next.members.clone(), shape(&next)),
            (Write::Restamped, Some(restored), shape(&unstamped)),
            "a member, C13 verdict {young:?}"
        );
    }
    let why = |young, member, doc: &Overrides| what(young, member, doc).map(|(_, why)| why);
    assert_eq!(
        [
            why(Some(false), false, &unstamped),
            why(Some(true), false, &unstamped)
        ],
        [Some(Write::Unlisted), Some(Write::Stamped)],
        "an undecided boot: C13"
    );
    assert_eq!(
        why(None, false, &unstamped),
        None,
        "undecided, no verdict yet"
    );
    let other = document(vec![member("streams-1", Some("z9")), peer.clone()], &[]);
    assert_eq!(
        why(Some(false), true, &other),
        Some(Write::Unlisted),
        "another boot id: R1a"
    );
    let removed = document(vec![peer], &[]);
    assert_ne!(
        why(Some(false), true, &removed),
        Some(Write::Restamped),
        "a name removed outright joins"
    );
    let gone = Overrides {
        entries: entries(&[("00", "streams-2")]),
        ..Overrides::default()
    };
    let (listed, listed_why) = what(Some(false), true, &gone).expect("C17's listing");
    assert_eq!(
        (listed_why, listed.members),
        (
            Write::Listed,
            Some(vec![
                member("streams-1", Some("a1")),
                member("streams-2", None)
            ])
        ),
        "no members: C17"
    );
}

/// C18 beyond the count (Addendum 41 B item 8): a decided member whose ordinal is beyond the
/// count has not released, so found unstamped it stamps itself back in place as one within it
/// does, never unlisted on C13's verdict.
#[test]
fn a_member_beyond_the_count_found_unstamped_stamps_itself_back() {
    let all = leaves();
    let beyond = reader(1, "streams-2", "b1", &["streams-1"]);
    let beyond_facts = Facts {
        candidates: &beyond,
        leaves: &all,
        may_join: true,
        young: Some(false),
        member: true,
        phase_over: false,
    };
    let past_count = document(
        vec![
            member("streams-1", Some("streams-1")),
            member("streams-2", None),
        ],
        &[],
    );
    let past = plan(&beyond_facts, &past_count, "b1").next;
    let restored = vec![
        member("streams-1", Some("streams-1")),
        member("streams-2", Some("b1")),
    ];
    assert_eq!(
        past.map(|(next, why)| (why, next.members)),
        Some((Write::Restamped, Some(restored))),
        "a member beyond the count, C13 verdict old"
    );
}

/// Item 8's clearing point (Addendum 41 B): a document that lists members without this name finds
/// it unlisted, whatever the other listings; one that lists it with any boot id, or has no
/// `members` (C17's older cell), does not.
#[test]
fn a_member_finds_itself_unlisted_only_in_a_listing_without_its_name() {
    let peer = member("streams-2", Some("b1"));
    let cases = [
        document(vec![peer.clone()], &[]),
        document(Vec::new(), &[]),
        document(vec![member("streams-1", None), peer.clone()], &[]),
        document(vec![member("streams-1", Some("z9")), peer], &[]),
        Overrides::default(),
    ];
    let got: Vec<bool> = cases
        .iter()
        .map(|doc| super::unlisted(doc, "streams-1"))
        .collect();
    assert_eq!(got, [true, true, false, false, false]);
}

/// C13's young bound is exact: a previous beat 11.999 s old stays in place, one 12 s old does not
/// (#183's survivors in `in_place`: each `+` of its margin as `-`, and `<` as `<=`).
#[test]
fn a_restart_is_in_place_below_twelve_seconds_and_not_at_them() {
    let now = 1_000_000;
    let beat = |age_ms: i64| {
        let document =
            r#"{"instance":"streams-1","ts_ms":0,"rps":0.0,"owned_shards":[],"draining":false}"#;
        let mut beat: Heartbeat = serde_json::from_str(document).expect("a minimal beat");
        beat.ts_ms = now - age_ms;
        beat
    };
    let got = (
        in_place(Some(&beat(11_999)), now),
        in_place(Some(&beat(12_000)), now),
    );
    assert_eq!(got, (true, false), "(11.999 s old, 12 s old)");
}

/// C7 with an entry naming a name outside the ring: the leaf is the ring's, so the member it rings
/// to pins it like any leaf it owns, and a leaf already pinned to `me` gets no second entry.
#[test]
fn a_member_pins_a_leaf_whose_entry_names_no_member() {
    let all = leaves();
    let ring = names(&["streams-1"]);
    let prospective = names(&["streams-1", "streams-2"]);
    let moving = pins(&all, &ring, &prospective, "streams-1", &HashMap::new());
    let first = moving[0].as_str();
    let stray = entries(&[(first, "streams-9")]);
    assert_eq!(pins(&all, &ring, &prospective, "streams-1", &stray), moving);
    let own = entries(&[(first, "streams-1")]);
    let rest: Vec<String> = moving.iter().skip(1).cloned().collect();
    assert_eq!(pins(&all, &ring, &prospective, "streams-1", &own), rest);
}

/// R1a under R3: a young restart that may not join (a drain or a stop begun) writes nothing, not
/// its boot id in place (#183's survivor: `of_restart`'s guard `facts.may_join` as `true`).
#[test]
fn a_young_restart_that_may_not_join_writes_nothing() {
    let all = leaves();
    let c = reader(2, "streams-1", "a2", &["streams-2"]);
    let doc = document(
        vec![
            member("streams-1", Some("a1")),
            member("streams-2", Some("streams-2")),
        ],
        &[],
    );
    let facts = |may_join| Facts {
        candidates: &c,
        leaves: &all,
        may_join,
        young: Some(true),
        member: false,
        phase_over: false,
    };
    let blocked = plan(&facts(false), &doc, "a2");
    let free = plan(&facts(true), &doc, "a2");
    assert_eq!(
        (
            blocked.next.is_none(),
            matches!(free.next, Some((_, Write::InPlace)))
        ),
        (true, true)
    );
}

/// A join's pass reports the joiner still waiting only when it joined past `JOIN_PHASE` with a
/// leaf uncovered, never when everything was covered (#183's survivor: `of_joiner`'s `!` deleted).
#[test]
fn a_covered_join_leaves_no_wait_and_a_late_one_does() {
    let all = leaves();
    let c = reader(2, "streams-2", "b2", &["streams-1"]);
    let facts = |phase_over| Facts {
        candidates: &c,
        leaves: &all,
        may_join: true,
        young: None,
        member: false,
        phase_over,
    };
    let doc = document(vec![member("streams-1", Some("streams-1"))], &[]);
    let late = plan(&facts(true), &doc, "b2");
    let wanted = uncovered(&all, &names(&["streams-1"]), "streams-2", &HashMap::new());
    let pinned: Vec<(&str, &str)> = wanted
        .iter()
        .map(|leaf| (leaf.as_str(), "streams-1"))
        .collect();
    let covered = document(vec![member("streams-1", Some("streams-1"))], &pinned);
    let now = plan(&facts(false), &covered, "b2");
    let joined = |planned: &super::Planned| matches!(planned.next, Some((_, Write::Joined { .. })));
    assert_eq!(
        (
            joined(&late),
            late.uncovered_now,
            joined(&now),
            now.uncovered_now
        ),
        (true, true, true, false)
    );
}

/// R3's first two clauses: a joiner may join only with no drain and no stop begun (#183's survivor
/// in `pass`: their `&&` as `||`).
#[test]
fn a_joiner_may_join_only_with_no_drain_and_no_stop_begun() {
    let stop = Some("stopping");
    let got = [
        may_join(false, None),
        may_join(true, None),
        may_join(false, stop),
        may_join(true, stop),
    ];
    assert_eq!(
        got,
        [true, false, false, false],
        "(neither, drain, stop, both)"
    );
}

/// A peer listed unstamped while this pass holds a live beat of it is counted once it reaches
/// `UNSTAMPED_PASSES` passes; this instance's own unstamped listing, a stamped peer and a peer whose
/// beat is not live are not (#183's survivors: `unstamped` as `()`, and its `&&` and `!=`).
#[test]
fn a_live_peer_left_unstamped_is_counted_at_its_passes() {
    let standing = crate::fleet::standing::Standing::default();
    let held = vec![
        // Its own beat not live, so a filter that took this instance would count nothing.
        ("streams-1".to_string(), (boot_of("a1"), false)),
        ("streams-2".to_string(), (boot_of("streams-2"), true)),
        ("streams-3".to_string(), (boot_of("streams-3"), true)),
        ("streams-4".to_string(), (boot_of("streams-4"), false)),
    ];
    let c = Candidates::held(4, "streams-1", held).with_boot("a1");
    let doc = document(
        vec![
            member("streams-1", None),
            member("streams-2", None),
            member("streams-3", Some("streams-3")),
            member("streams-4", None),
        ],
        &[],
    );
    for _ in 0..super::UNSTAMPED_PASSES {
        unstamped(&standing, &c, &doc);
    }
    assert_eq!(standing.unstamped_listings(), 1, "streams-2 alone");
}

/// The premise that makes `pins` need no "already pinned to me" filter: a member's prospective ring
/// holds its whole ring (`Planned::of_member` builds it from `ring.clone()`), so an entry naming
/// the member keeps its leaf there too. A member whose document already pins one moving leaf to it
/// writes the other pins only, never a second entry for that leaf.
#[test]
fn a_member_writes_no_second_pin_for_a_leaf_pinned_to_it() {
    let all = leaves();
    let c = reader(2, "streams-1", "a1", &["streams-2"]);
    let facts = Facts {
        candidates: &c,
        leaves: &all,
        may_join: true,
        young: Some(false),
        member: true,
        phase_over: false,
    };
    let listed = vec![member("streams-1", Some("a1"))];
    let Some((_, Write::Pinned(moving, _))) =
        plan(&facts, &document(listed.clone(), &[]), "a1").next
    else {
        panic!("a member pins what streams-2 would win");
    };
    let held = document(listed, &[(moving[0].as_str(), "streams-1")]);
    let Some((written, Write::Pinned(again, _))) = plan(&facts, &held, "a1").next else {
        panic!("the other leaves are still pinned");
    };
    assert_eq!(again, moving[1..].to_vec());
    let kept = &written.entries[&moving[0]];
    assert_eq!(
        (kept.to.as_str(), kept.ms),
        ("streams-1", 1),
        "the first pin as it was"
    );
}

/// The premise that makes the pass's `note_uncovered` need no count check: only a joiner within the
/// count reports itself waiting (`Planned::of_joiner` returns before any wait when its ordinal is
/// beyond the count). A joiner beyond the count with leaves uncovered, a member and a restart all
/// report no wait.
#[test]
fn only_a_joiner_within_the_count_reports_a_wait() {
    let all = leaves();
    let doc = document(vec![member("streams-1", Some("streams-1"))], &[]);
    let waits = |c: &Candidates, young, member| {
        let facts = Facts {
            candidates: c,
            leaves: &all,
            may_join: true,
            young,
            member,
            phase_over: false,
        };
        plan(&facts, &doc, "b2").uncovered_now
    };
    let within = reader(2, "streams-2", "b2", &["streams-1"]);
    let beyond = reader(1, "streams-2", "b2", &["streams-1"]);
    let one = reader(1, "streams-1", "streams-1", &[]);
    let restart = document(vec![member("streams-2", Some("b1"))], &[]);
    let restarted = {
        let facts = Facts {
            candidates: &within,
            leaves: &all,
            may_join: true,
            young: Some(true),
            member: false,
            phase_over: false,
        };
        plan(&facts, &restart, "b2").uncovered_now
    };
    assert_eq!(
        (
            waits(&within, None, false),
            waits(&beyond, None, false),
            waits(&one, None, true),
            restarted
        ),
        (true, false, false, false),
        "(a joiner within the count, beyond it, a member, a restart)"
    );
}

/// `me`'s facts for a joiner's pass under R3 with [`super::JOIN_PHASE`] not over.
fn joining<'a>(c: &'a Candidates, all: &'a [String]) -> Facts<'a> {
    Facts {
        candidates: c,
        leaves: all,
        may_join: true,
        young: Some(false),
        member: false,
        phase_over: false,
    }
}

/// The join ruling, Addendum 44: over a document that lists nobody no instance could pin, so
/// nothing is uncovered and a joiner within the count joins at its first pass, its entries kept as
/// read. R3 and the count still hold it back.
#[test]
fn a_joiner_joins_at_once_over_a_document_that_lists_nobody() {
    let all = leaves();
    let nobody = document(Vec::new(), &[("03", "streams-2")]);
    let c = reader(2, "streams-1", "a2", &[]);
    let planned = plan(&joining(&c, &all), &nobody, "a2");
    assert!(!planned.uncovered_now, "no wait begins");
    let (next, why) = planned.next.expect("the join is written");
    assert_eq!(why, Write::Joined { uncovered: vec![] });
    assert_eq!(next.members, Some(vec![member("streams-1", Some("a2"))]));
    let kept: Vec<_> = next.entries.iter().map(|(leaf, e)| (leaf, &e.to)).collect();
    assert_eq!(kept, [(&"03".to_string(), &"streams-2".to_string())]);
    let beyond = reader(2, "streams-3", "c1", &[]);
    assert!(
        plan(&joining(&beyond, &all), &nobody, "c1").next.is_none(),
        "C5: its ordinal beyond the count"
    );
    let unbeaten = Candidates::held(2, "streams-1", vec![]).with_boot("a2");
    assert!(
        plan(&joining(&unbeaten, &all), &nobody, "a2")
            .next
            .is_none(),
        "R3: no beat of this boot read"
    );
}

/// Addendum 44's key is the document's list, not the reader's ring: a listed member beyond the
/// count that this reader alone judges dead gives it an empty ring, and may hold engines, so a
/// joiner still waits out `JOIN_PHASE`.
#[test]
fn a_joiner_waits_while_anyone_is_listed_even_over_an_empty_ring() {
    let all = leaves();
    let beats = vec![
        ("streams-1".to_string(), (boot_of("a2"), true)),
        ("streams-3".to_string(), (boot_of("c1"), false)),
    ];
    let c = Candidates::held(1, "streams-1", beats).with_boot("a2");
    let listed = vec![member("streams-3", Some("c1"))];
    assert_eq!(c.ring(Some(&listed)), Vec::<String>::new(), "the premise");
    let planned = plan(&joining(&c, &all), &document(listed, &[]), "a2");
    assert!(planned.next.is_none() && planned.uncovered_now);
}

/// Two joiners of one document that lists nobody: the first CAS wins, and the other, planning
/// again over the winner's listing, waits for its pins (Addendum 44).
#[test]
fn the_second_joiner_of_an_empty_document_waits_for_the_firsts_pins() {
    let all = leaves();
    let nobody = document(Vec::new(), &[]);
    let first = reader(2, "streams-1", "streams-1", &["streams-2"]);
    let (won, _) = plan(&joining(&first, &all), &nobody, "streams-1")
        .next
        .expect("the first joins");
    let second = reader(2, "streams-2", "b2", &["streams-1"]);
    let waiting = plan(&joining(&second, &all), &won, "b2");
    assert!(waiting.next.is_none() && waiting.uncovered_now);
}

/// Until the leave: a member beyond the count that is the last one listed unlists itself (the
/// interim's removal half), its pins then naming nobody listed, and the joiner that finds the list
/// empty joins at once (Addendum 44 §1: E5's scale-in class).
#[test]
fn a_joiner_follows_the_interims_removal_of_the_last_listed_member() {
    let all = leaves();
    let leaver = reader(1, "streams-2", "b1", &["streams-1"]);
    let listed = document(vec![member("streams-2", Some("b1"))], &[]);
    let facts = Facts {
        member: true,
        ..joining(&leaver, &all)
    };
    let (emptied, why) = plan(&facts, &listed, "b1").next.expect("the removal");
    assert!(matches!(why, Write::Pinned(ref pins, ref beyond)
        if !pins.is_empty() && beyond == &names(&["streams-2"])));
    assert_eq!(emptied.members, Some(Vec::new()));
    let joiner = reader(1, "streams-1", "a2", &["streams-2"]);
    let (joined, why) = plan(&joining(&joiner, &all), &emptied, "a2")
        .next
        .expect("the join is written");
    assert_eq!(why, Write::Joined { uncovered: vec![] });
    assert_eq!(joined.members, Some(vec![member("streams-1", Some("a2"))]));
}
