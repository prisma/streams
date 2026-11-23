#![cfg(test)]
//! The join's units (§22 step 5; the ruling's §9 "units": coverage, the pin plan, C6's cases are
//! `moves`', C13's table with R1a).

use std::collections::HashMap;

use super::{
    Facts, Write, answered, in_place, may_beat, owner, pins, plan, since_rise_ms, uncovered,
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
