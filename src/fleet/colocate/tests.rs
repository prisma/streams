#![cfg(test)]
//! A co-location's parts: a holder's pin of its own half over an in-memory fleet store (its half
//! named for it with a fresh stamp and an event, mirrored here, the stale entries dropped; nothing
//! written for a half another instance holds, one already named, or an unreadable document); the
//! target every holder computes alike; and each holder's step.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use object_store::{ObjectStore, ObjectStoreExt};

use super::{Pinned, Step, colocation_target, pin, step};
use crate::fleet::{FleetRepository, Overrides};
use crate::ownership::OwnershipService;
use crate::topology::policy::{Instance, Leaf, View};

const OVERRIDES: &str = "fleet/overrides.json";

/// A stored topology of the leaves `00`, `01` and `1` (the two-shard cell split at `0`).
async fn split_at_zero(store: &Arc<dyn ObjectStore>) {
    let initial = crate::config::validation::InitialShards::new(2)
        .unwrap()
        .topology(1 << 20);
    let (reserved, homes) = initial.reserve_homes();
    let split = reserved.split("0", homes, "op-x").unwrap();
    let raw = serde_json::to_vec(&split).unwrap();
    store
        .put(&"topology.json".into(), raw.into())
        .await
        .unwrap();
}

/// A store holding `split_at_zero`'s topology and the overrides document `doc`.
async fn store_with(doc: &[u8]) -> Arc<dyn ObjectStore> {
    let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    split_at_zero(&store).await;
    store
        .put(&OVERRIDES.into(), doc.to_vec().into())
        .await
        .unwrap();
    store
}

async fn raw(store: &Arc<dyn ObjectStore>) -> Vec<u8> {
    let raw = store.get(&OVERRIDES.into()).await.unwrap();
    raw.bytes().await.unwrap().to_vec()
}

/// **A holder's pin names its own half for itself in one CAS**: the document names `00` for
/// `streams-2`, stamped no earlier than the call, with one `rebalance_move` event; the entry `0`,
/// which names no stored leaf, is dropped and `1`'s kept; this instance's view names `00` for it.
#[tokio::test(crate = "crate::test_rt")]
async fn a_pin_names_its_own_half_for_this_instance_in_one_cas() {
    let store =
        store_with(br#"{"entries":{"0":{"to":"streams-1","ms":1},"1":{"to":"streams-1","ms":1}}}"#)
            .await;
    let fleet = FleetRepository::new(Some(store.clone()));
    let ownership = OwnershipService::new("streams-2");
    let started = crate::shard::now_ms();
    assert_eq!(
        pin(&fleet, &ownership, &*store, "00").await,
        Pinned::Written
    );
    let doc: Overrides = serde_json::from_slice(&raw(&store).await).unwrap();
    let named: BTreeMap<&str, &str> = doc
        .entries
        .iter()
        .map(|(prefix, entry)| (prefix.as_str(), entry.to.as_str()))
        .collect();
    assert_eq!(
        named,
        BTreeMap::from([("00", "streams-2"), ("1", "streams-1")])
    );
    assert!(doc.entries["00"].ms >= started, "00 is stamped now");
    let events: Vec<(&str, Option<&str>)> = doc
        .pending_events
        .iter()
        .map(|event| (event.event_type.as_str(), event.shard.as_deref()))
        .collect();
    assert_eq!(events, [("rebalance_move", Some("00"))]);
    assert_eq!(
        ownership.overrides().get("00").map(String::as_str),
        Some("streams-2")
    );
}

/// **No instance writes an entry for a leaf it does not hold** (Addendum 7, the F6 addendum's C1):
/// a pin of `00`, which the view places on `streams-1`, by `streams-2` answers `NotHeld`; the
/// stored bytes are as they were and the view still places `00` on `streams-1`.
#[tokio::test(crate = "crate::test_rt")]
async fn a_pin_writes_no_entry_for_a_half_another_instance_holds() {
    let before = br#"{"entries":{"00":{"to":"streams-1","ms":1}}}"#;
    let store = store_with(before).await;
    let fleet = FleetRepository::new(Some(store.clone()));
    let ownership = OwnershipService::new("streams-2");
    ownership.set_ring_active(vec!["streams-1".into(), "streams-2".into()]);
    ownership.set_overrides(HashMap::from([("00".into(), "streams-1".into())]));
    assert_eq!(
        pin(&fleet, &ownership, &*store, "00").await,
        Pinned::NotHeld
    );
    assert_eq!(raw(&store).await, before);
    assert_eq!(
        ownership.effective_owner("00").as_deref(),
        Some("streams-1")
    );
}

/// **A half already named for its holder is not written again** (its stamp would hold every
/// topology operation for another move cooldown), and **an unreadable document writes nothing**:
/// the stored bytes are as they were and this instance's view holds no new entry.
#[tokio::test(crate = "crate::test_rt")]
async fn a_pin_of_a_named_half_or_over_an_unreadable_document_writes_nothing() {
    let named = br#"{"entries":{"00":{"to":"streams-2","ms":1}}}"#;
    for (before, pinned) in [
        (&named[..], Pinned::Already),
        (&b"not json"[..], Pinned::Lost),
    ] {
        let store = store_with(before).await;
        let fleet = FleetRepository::new(Some(store.clone()));
        let ownership = OwnershipService::new("streams-2");
        assert_eq!(pin(&fleet, &ownership, &*store, "00").await, pinned);
        assert_eq!(raw(&store).await, before, "{pinned:?}");
        assert!(ownership.overrides().is_empty(), "{pinned:?}");
    }
}

fn instance(id: &str, resident_shards: u32, accepting: bool) -> Instance {
    Instance {
        id: id.into(),
        resident_shards,
        accepting,
    }
}

fn leaf(prefix: &str, owner: &str) -> Leaf {
    Leaf {
        prefix: prefix.into(),
        born_ms: 0,
        owner: owner.into(),
        beat_ms: None,
        load: BTreeMap::new(),
    }
}

/// The pair `0` split over `a` (`00`) and `b` (`01`) as `me` sees it: `a` and `b` holding `na` and
/// `nb` shards (`b` accepting as `b_accepting`), and a third `c` when given.
fn view(me: &str, (na, nb): (u32, u32), c: Option<(u32, bool)>, b_accepting: bool) -> View {
    View {
        now_ms: 0,
        me: me.into(),
        instances: [instance("a", na, true), instance("b", nb, b_accepting)]
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
    }
}

fn halves() -> [String; 2] {
    ["00".to_string(), "01".to_string()]
}

/// A split pair's co-location target (`colocation_target`): the actor when it has room for the
/// other half and the merged leaf (+2), else the partner holding `…1` (+2) when it accepts, else
/// the accepting instance with the fewest shards that has room for both halves and the merged leaf
/// (+3); none when no instance has room.
#[test]
fn a_split_pairs_halves_go_to_the_first_instance_with_room() {
    let target = |v: &View| colocation_target(v, 3, &halves());
    let at = |pair, c, b_accepting| view("a", pair, c, b_accepting);
    assert_eq!(
        target(&at((1, 3), None, true)).as_deref(),
        Some("a"),
        "the actor has room"
    );
    assert_eq!(
        target(&at((2, 1), None, true)).as_deref(),
        Some("b"),
        "the partner has room"
    );
    assert_eq!(
        target(&at((2, 1), None, false)),
        None,
        "a partner that does not accept"
    );
    assert_eq!(
        target(&at((2, 2), Some((0, true)), true)).as_deref(),
        Some("c"),
        "a third with +3"
    );
    assert_eq!(
        target(&at((2, 2), Some((1, true)), true)),
        None,
        "a third with 1 + 3 > 3"
    );
    assert_eq!(
        target(&at((2, 2), Some((0, false)), true)),
        None,
        "a third that does not accept"
    );
    let mut holds_another = at((2, 2), Some((0, true)), true);
    holds_another.leaves.push(leaf("1", "c"));
    assert_eq!(
        target(&holds_another).as_deref(),
        Some("c"),
        "a third holding a leaf other than the half"
    );
}

/// **Both holders compute one target** (the F6 addendum's C2): for each layout above, the view of
/// `00`'s holder `a`, of `01`'s holder `b` and of the third `c` give the same target.
#[test]
fn a_split_pairs_target_is_the_same_from_either_holders_view() {
    let layouts = [
        ((1, 3), None, true),
        ((2, 1), None, true),
        ((2, 1), None, false),
        ((2, 2), Some((0, true)), true),
        ((2, 2), Some((1, true)), true),
    ];
    for (pair, c, b_accepting) in layouts {
        let targets: Vec<Option<String>> = ["a", "b", "c"]
            .iter()
            .map(|me| colocation_target(&view(me, pair, c, b_accepting), 3, &halves()))
            .collect();
        assert!(
            targets.iter().all(|t| *t == targets[0]),
            "{pair:?} {c:?}: {targets:?}"
        );
    }
}

/// `me`'s step for the pair `0` of `view(me, pair, c, true)` with the entries `named` mirrored.
fn step_of(me: &str, pair: (u32, u32), c: Option<(u32, bool)>, named: &[(&str, &str)]) -> Step {
    let ownership = OwnershipService::new(me);
    let named = named.iter().map(|(p, to)| ((*p).into(), (*to).into()));
    ownership.set_overrides(named.collect());
    step(&view(me, pair, c, true), 3, "0", &ownership)
}

fn hand(half: &str, to: &str) -> Step {
    Step::Hand {
        half: half.into(),
        to: to.into(),
    }
}

/// **Each holder writes for its own half only, and the other holder's half is named for the
/// target before it is handed there** (Addendum 7): with the actor `a` the target, `a` names `00`
/// for itself, then waits; `b` waits until `00` is named for `a`, then hands `01` to `a`. With the
/// partner `b` the target, the same the other way round.
#[test]
fn a_holder_hands_to_the_other_holder_only_once_that_holder_named_its_own_half() {
    let pinned_a = [("00", "a")];
    assert_eq!(step_of("a", (1, 3), None, &[]), Step::Pin("00".into()));
    assert_eq!(step_of("a", (1, 3), None, &pinned_a), Step::Wait);
    assert_eq!(step_of("b", (1, 3), None, &[]), Step::Wait);
    assert_eq!(step_of("b", (1, 3), None, &pinned_a), hand("01", "a"));
    let pinned_b = [("01", "b")];
    assert_eq!(step_of("b", (2, 1), None, &[]), Step::Pin("01".into()));
    assert_eq!(step_of("b", (2, 1), None, &pinned_b), Step::Wait);
    assert_eq!(step_of("a", (2, 1), None, &[]), Step::Wait);
    assert_eq!(step_of("a", (2, 1), None, &pinned_b), hand("00", "b"));
}

/// **A third instance takes both halves, each handed by its holder at once**; with no instance
/// that has room neither holder writes and only the actor asks for one; an instance holding
/// neither half has nothing to do.
#[test]
fn a_third_target_takes_each_half_from_its_holder_and_no_room_writes_nothing() {
    let third = Some((0, true));
    assert_eq!(step_of("a", (2, 2), third, &[]), hand("00", "c"));
    assert_eq!(step_of("b", (2, 2), third, &[]), hand("01", "c"));
    assert_eq!(step_of("c", (2, 2), third, &[]), Step::Wait);
    assert_eq!(step_of("a", (2, 2), None, &[]), Step::NoRoom);
    assert_eq!(step_of("b", (2, 2), None, &[]), Step::Wait);
}
