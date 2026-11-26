#![cfg(test)]
//! The join's boots (§22 step 5; GATE-DECISION-s22-join-2026-11-20 §2 and Addenda 1-3): the
//! upgrade from step 4's unstamped listings, a boot killed between its stamp and its first beat, an
//! unlisted boot that beats at once with its tick held, and a cold join's convergence over a 20 ms
//! store. The rigs and helpers are `fleet_join`'s.

use super::fixture_http::HttpRig;
use super::fixture_requests::hreq;
use super::fixture_runtime::RigRuntime;
use super::fixture_storage::mem;
use super::fleet_join::{
    BOUND, JOIN_BOUND, LEAVES, beat_boot, desired, listed, listed_boot, member, nothing_fenced,
    open_owned, previous_beat, stop, stored, until, until_joined,
};
use crate::dst::fault_store::{FaultPlan, FaultProfile, FaultStore};
use crate::dst::trace_store::{StoreTraceEvent, TraceOutcome, TraceStore};
use crate::dst::{ObjClass, StoreOp};
use object_store::{ObjectStore, ObjectStoreExt, PutPayload, path::Path};
use std::sync::Arc;
use std::time::Duration;

/// The stream the lone member's restart reads back, and its read from the start.
const STREAM: &str = "/v1/stream/lone";
const STREAM_FROM_START: &str = "/v1/stream/lone?offset=-1";
const JSON: (&str, &str) = ("content-type", "application/json");

/// One tick of the upgrade on one rig: its name, the document version its view was published from,
/// the ring it publishes, and whether it holds any engine.
type Sample = (String, String, Vec<String>, bool);

/// One tick of the upgrade on each of `rigs` ([`Sample`]).
fn views(rigs: &[&HttpRig]) -> Vec<Sample> {
    let view = |rig: &&HttpRig| {
        let name = rig.state.config.cli.instance_name.clone();
        let ring = rig.state.ownership.ring_active();
        let holds = !rig.state.shards.held_prefixes().is_empty();
        (name, rig.state.fleet.standing().tags().tag(), ring, holds)
    };
    rigs.iter().map(view).collect()
}

/// The upgrade's check (the join ruling, Addendum 1 as restated by Addendum 23 §1) over one
/// window's `samples`, `decided` each sampled document version's decided members and `in_place`
/// the boots with a young previous beat. Before its decision a member holds no engine and
/// publishes no ring containing itself. A decided member's ring is the decided members with every
/// boot in place: a boot in place stays in its peers' rings while it decides, a boot that will
/// join enters none before its join. Each ring is judged against the document version its view
/// was published from, or the next one it published (a sample can land between a view's ring and
/// its tag, which are set one after the other); a version never sampled is not judged. The
/// violations, as text.
fn upgrade_violations(
    samples: &[Sample],
    decided: &std::collections::HashMap<String, Vec<String>>,
    in_place: &[&str],
) -> Vec<String> {
    let expected = |tag: &String| {
        decided.get(tag).map(|names| {
            let mut ring: Vec<String> = names.clone();
            let staying = in_place.iter().map(|name| (*name).to_string());
            ring.extend(staying.filter(|name| !names.contains(name)));
            ring.sort_by_key(|name| name.trim_start_matches("streams-").parse::<u64>().ok());
            ring
        })
    };
    let mut found = Vec::new();
    for (at, (name, tag, ring, holds)) in samples.iter().enumerate() {
        let Some(names) = decided.get(tag) else {
            continue;
        };
        if !names.contains(name) {
            if *holds || ring.contains(name) {
                found.push(format!(
                    "undecided {name} holds {holds} or rings itself: {ring:?}"
                ));
            }
            continue;
        }
        let later = samples[at..].iter().filter(|later| &later.0 == name);
        let next = later.map(|later| &later.1).find(|later| *later != tag);
        let allowed = [Some(tag), next]
            .into_iter()
            .flatten()
            .filter_map(&expected);
        if !allowed.clone().any(|allowed| allowed == *ring) {
            let allowed: Vec<Vec<String>> = allowed.collect();
            found.push(format!(
                "decided {name}'s ring {ring:?} is none of {allowed:?}"
            ));
        }
    }
    found
}

/// The members `members` lists with the boot id of the rig of their name, in order.
fn decided_of(members: &[(String, Option<String>)], rigs: &[&HttpRig]) -> Vec<String> {
    let own = |name: &str| {
        let rig = rigs
            .iter()
            .find(|rig| rig.state.config.cli.instance_name == name);
        rig.map(|rig| rig.state.runtime.identity.boot_id.clone())
    };
    let stamped = members
        .iter()
        .filter(|(name, boot)| boot.is_some() && *boot == own(name));
    stamped.map(|(name, _)| name.clone()).collect()
}

/// The members of `fleet`'s document listed with the boot id of the rig of their name, in order.
async fn decided(fleet: &Arc<dyn ObjectStore>, rigs: &[&HttpRig]) -> Vec<String> {
    decided_of(&stored(fleet).await.0, rigs)
}

/// The stored document's version, as a view's tag names it, and its decided members.
async fn decided_at(
    fleet: &Arc<dyn ObjectStore>,
    rigs: &[&HttpRig],
) -> Option<(String, Vec<String>)> {
    let got = fleet.get(&Path::from("fleet/overrides.json")).await.ok()?;
    let meta = got.meta.clone();
    let doc: crate::fleet::Overrides = serde_json::from_slice(&got.bytes().await.ok()?).ok()?;
    let members: Vec<(String, Option<String>)> = doc
        .members
        .unwrap_or_default()
        .into_iter()
        .map(|member| (member.name, member.boot_id))
        .collect();
    let tag = meta.e_tag.or(meta.version).unwrap_or_default();
    Some((tag, decided_of(&members, rigs)))
}

/// Sample every 50 ms until `done`, bounded: the window's violations of the upgrade's check,
/// `in_place` the boots with a young previous beat.
async fn watched(
    what: &str,
    (fleet, in_place): (&Arc<dyn ObjectStore>, &[&str]),
    rigs: &[&HttpRig],
    mut done: impl AsyncFnMut() -> bool,
) -> Vec<String> {
    let (mut samples, mut decided) = (Vec::new(), std::collections::HashMap::new());
    let t0 = tokio::time::Instant::now();
    while !done().await {
        assert!(t0.elapsed() < BOUND, "{what}");
        let before = decided_at(fleet, rigs).await;
        samples.extend(views(rigs));
        decided.extend(before.into_iter().chain(decided_at(fleet, rigs).await));
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    upgrade_violations(&samples, &decided, in_place)
}

/// Write the overrides document as step 4's interim leaves it: `names` listed, no boot ids.
async fn interim_document(fleet: &Arc<dyn ObjectStore>, names: &[&str]) {
    let members: Vec<String> = names
        .iter()
        .map(|name| format!(r#"{{"name":"{name}"}}"#))
        .collect();
    let body = format!(r#"{{"entries":{{}},"members":[{}]}}"#, members.join(","));
    let path = Path::from("fleet/overrides.json");
    fleet.put(&path, PutPayload::from(body)).await.unwrap();
}

/// **The upgrade from step 4's interim moves nothing** (the join ruling §2 and its Addendum 1).
/// The document lists `streams-1`..`streams-3` with no boot ids; the count is three. `streams-1`
/// and `streams-2` boot on the join's code with young previous beats: each stamps its own listing
/// once (C13's in-place path: an unstamped listing is another boot's), and then no idle pass
/// writes. `streams-3` boots late with no young beat: it finds itself listed unstamped, unlists,
/// and joins by coverage; no engine is fenced and the leaves it wins arrive by handoff. At every
/// tick (Addendum 23 §1), before a member's decision it holds and publishes nothing of its own; a
/// boot in place stays in its peers' rings while it decides, and the late boot enters none before
/// its join; every decided ring is the decided members with the boots in place. Red on the draft
/// before the ruling (a boot taking the unstamped listing as membership): a tick yield at the late
/// boot.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn the_upgrade_from_unstamped_listings_moves_nothing() {
    let (data, fleet) = (mem(), mem());
    desired(&fleet, 3, 1).await;
    interim_document(&fleet, &["streams-1", "streams-2", "streams-3"]).await;
    for name in ["streams-1", "streams-2"] {
        previous_beat(&fleet, name, 1_000).await;
    }
    let a = member(&data, fleet.clone(), "streams-1", RigRuntime::first()).await;
    let b = member(
        &data,
        fleet.clone(),
        "streams-2",
        RigRuntime::incarnation(1),
    )
    .await;
    let in_place = ["streams-1", "streams-2"];
    let mut found = watched("both stamp", (&fleet, &in_place), &[&a, &b], async || {
        decided(&fleet, &[&a, &b]).await.len() == 2
    })
    .await;
    open_owned(&a).await;
    open_owned(&b).await;
    let stamped = stored(&fleet).await;
    tokio::time::sleep(Duration::from_secs(6)).await;
    assert_eq!(stored(&fleet).await, stamped, "no write on idle passes");
    let c = member(
        &data,
        fleet.clone(),
        "streams-3",
        RigRuntime::incarnation(3),
    )
    .await;
    let rigs = [&a, &b, &c];
    found.extend(
        watched(
            "streams-3 joins stamped",
            (&fleet, &in_place),
            &rigs,
            async || decided(&fleet, &rigs).await.len() == 3,
        )
        .await,
    );
    found.extend(
        watched("a tick after", (&fleet, &in_place), &rigs, {
            let t0 = tokio::time::Instant::now();
            async move || t0.elapsed() > Duration::from_secs(3)
        })
        .await,
    );
    assert_eq!(
        (found, nothing_fenced(&rigs)),
        (Vec::<String>::new(), vec![(0, 0); 3])
    );
    for rig in [a, b, c] {
        stop(rig).await;
    }
}

/// **A boot killed between its stamp and its first beat is recovered by the next boot of its
/// name** (the join ruling, Addendum 2's corner). Both members listed; `streams-2` crashes (its
/// last beat stays, not withdrawn) and boots again young: it stamps its new boot id in place, and
/// its first beat is held (`pause_first_beat`) once released; the process is stopped there, so the
/// document lists a boot with no beat of that boot in the store. The next boot of `streams-2`
/// decides (C13), beats with its own boot id, and serves: listed with that boot id, in its own
/// ring, and nothing fenced.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_boot_killed_between_its_stamp_and_its_first_beat_is_recovered_by_the_next() {
    let (data, fleet) = (mem(), mem());
    desired(&fleet, 2, 1).await;
    let a = member(&data, fleet.clone(), "streams-1", RigRuntime::first()).await;
    let b = member(
        &data,
        fleet.clone(),
        "streams-2",
        RigRuntime::incarnation(1),
    )
    .await;
    until("both listed", async || listed(&fleet).await.len() == 2).await;
    stop(b).await;
    previous_beat(&fleet, "streams-2", 500).await;
    let killed = member(
        &data,
        fleet.clone(),
        "streams-2",
        RigRuntime::incarnation(2),
    )
    .await;
    let (reached, reached_rx) = tokio::sync::oneshot::channel();
    let (_go, go) = tokio::sync::oneshot::channel::<()>();
    let point = crate::topology::gate::PausePoint { reached, go };
    killed.state.fleet.standing().pause_first_beat(point);
    let stamped = killed.state.runtime.identity.boot_id.clone();
    tokio::time::timeout(BOUND, reached_rx)
        .await
        .expect("released")
        .expect("the first beat is held");
    let window = (
        listed_boot(&fleet, "streams-2").await,
        beat_boot(&fleet, "streams-2").await,
    );
    stop(killed).await;
    assert_eq!(
        window,
        (Some(Some(stamped)), Some("before".to_string())),
        "stamped, no beat of it"
    );
    let next = member(
        &data,
        fleet.clone(),
        "streams-2",
        RigRuntime::incarnation(3),
    )
    .await;
    let boot = next.state.runtime.identity.boot_id.clone();
    until("the next boot decides and beats", async || {
        listed_boot(&fleet, "streams-2").await == Some(Some(boot.clone()))
            && beat_boot(&fleet, "streams-2").await.as_deref() == Some(boot.as_str())
    })
    .await;
    until("it serves", async || {
        next.state
            .ownership
            .ring_active()
            .contains(&"streams-2".to_string())
    })
    .await;
    assert_eq!(nothing_fenced(&[&a, &next]), [(0, 0), (0, 0)]);
    for rig in [a, next] {
        stop(rig).await;
    }
}

/// **A boot decides before its first beat is published** (GATE-DECISION-s22-join-2026-11-20
/// Addendum 39's premise, in the product's own order: `fleet::heartbeat::before_the_first_beat`
/// takes C13's verdict, then allows the beat). Both members listed; `streams-2` stops and boots
/// again over a previous boot's beat of its name, and its first beat is held once released. There
/// its verdict is taken while the store holds only the previous boot's beat; released, the beat it
/// writes carries its own boot id, the verdict still taken. `fixture_members::joined` fails on the
/// converse.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_boot_decides_before_its_first_beat_is_published() {
    let (data, fleet) = (mem(), mem());
    desired(&fleet, 2, 1).await;
    let a = member(&data, fleet.clone(), "streams-1", RigRuntime::first()).await;
    let b = member(
        &data,
        fleet.clone(),
        "streams-2",
        RigRuntime::incarnation(1),
    )
    .await;
    until("both listed", async || listed(&fleet).await.len() == 2).await;
    stop(b).await;
    previous_beat(&fleet, "streams-2", 500).await;
    let next = member(
        &data,
        fleet.clone(),
        "streams-2",
        RigRuntime::incarnation(2),
    )
    .await;
    let (reached, reached_rx) = tokio::sync::oneshot::channel();
    let (go_tx, go) = tokio::sync::oneshot::channel::<()>();
    let point = crate::topology::gate::PausePoint { reached, go };
    next.state.fleet.standing().pause_first_beat(point);
    let boot = next.state.runtime.identity.boot_id.clone();
    tokio::time::timeout(BOUND, reached_rx)
        .await
        .expect("released")
        .expect("the first beat is held");
    let held = (
        next.state.fleet.standing().boot_verdict().is_some(),
        beat_boot(&fleet, "streams-2").await,
    );
    assert_eq!(
        held,
        (true, Some("before".to_string())),
        "decided, the previous boot's beat alone"
    );
    go_tx.send(()).expect("the first beat waits");
    until("the first beat is written", async || {
        beat_boot(&fleet, "streams-2").await.as_deref() == Some(boot.as_str())
    })
    .await;
    assert!(next.state.fleet.standing().boot_verdict().is_some());
    for rig in [a, next] {
        stop(rig).await;
    }
}

/// **An unlisted boot beats at once, with no tick involved** (the join ruling, Addenda 2 and 3;
/// strengthened by Addendum 34). `streams-1` alone is listed at a count of one; the count goes to
/// two and `streams-2` boots over a fleet store armed before its boot to let exactly one read of
/// the document through, the heartbeat's boot read, and park every later one: every read its tick
/// makes, from the first. Its heartbeat finds its name unlisted and beats at once, while no pass of
/// its own tick has run (a pass begins only once the tick has read the document). Released, the
/// tick joins. If the heartbeat's boot reads of the document changed in number, the beat would
/// come from the tick (no reads: a pass would have run) or not at all (two: the second parked).
/// Red, by its assertion, with the heartbeat's boot read replaced by "listed" (`listed_at_boot ->
/// true`, #183's survivor: the tick's first read is the one let through and its pass releases the
/// beat) and with the heartbeat's release removed (the join's R-f: its read is let through and
/// releases nothing, and the tick's reads are parked, so it never beats).
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn an_unlisted_boot_beats_before_it_joins() {
    let (data, fleet) = (mem(), mem());
    desired(&fleet, 1, 1).await;
    let a = member(&data, fleet.clone(), "streams-1", RigRuntime::first()).await;
    until("streams-1 is listed", async || {
        listed(&fleet).await == ["streams-1"]
    })
    .await;
    desired(&fleet, 2, 2).await;
    let b_fleet = FaultStore::new(fleet.clone(), 0xB0, FaultProfile::clean());
    let overrides = "fleet/overrides.json";
    let seen = b_fleet.hold_under_after(StoreOp::Get, ObjClass::Fleet, overrides, 1);
    let b = member(
        &data,
        b_fleet.clone(),
        "streams-2",
        RigRuntime::incarnation(1),
    )
    .await;
    let boot = b.state.runtime.identity.boot_id.clone();
    // Bounded, not `until`: a boot that never beats fails the assertion below, by its text.
    let waited = tokio::time::Instant::now();
    let mut beat = false;
    while !beat && waited.elapsed() < super::fleet_join::BOUND / 3 {
        beat = beat_boot(&fleet, "streams-2").await.as_deref() == Some(boot.as_str());
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let read = |seen: &std::sync::atomic::AtomicU64| seen.load(std::sync::atomic::Ordering::SeqCst);
    until("streams-2's tick reaches its parked read", async || {
        read(&seen) >= 2
    })
    .await;
    let (passes, reads) = (b.state.fleet.standing().join_passes(), read(&seen));
    let unjoined = listed(&fleet).await.len() == 1;
    b_fleet.release_hold();
    until_joined("streams-2 joins", async || listed(&fleet).await.len() == 2).await;
    assert_eq!(
        (beat, passes, unjoined),
        (true, 0, true),
        "(streams-2 beat, its tick's passes by then, unjoined) with every tick read of the document parked: {reads} reads seen, one let through"
    );
    for rig in [a, b] {
        stop(rig).await;
    }
}

/// Every store operation's latency in the cold-convergence DST: 20 ms.
const COLD: FaultPlan = FaultPlan {
    latency_pct: 100,
    latency_ms: (20, 20),
    ..FaultPlan::CLEAN
};

/// The leaves `name` wins in a ring of `streams-1` and `name`.
fn won_by(name: &str) -> Vec<&'static str> {
    let both = ["streams-1".to_string(), "streams-2".to_string()];
    let wins = |leaf: &&str| both[crate::ownership::ring_pick(leaf, &both)] == name;
    LEAVES.iter().copied().filter(wins).collect()
}

/// Ask `rig` to open each of `leaves` it owns now, as a routed request would: how many are open.
async fn served(rig: &HttpRig, leaves: &[&str]) -> usize {
    let me = rig.state.config.cli.instance_name.as_str();
    for leaf in leaves {
        if rig.state.ownership.effective_owner(leaf).as_deref() == Some(me) {
            let shards = &rig.state.shards;
            drop(shards.open_or_wait(leaf, Duration::from_millis(200)).await);
        }
    }
    let open = |leaf: &&&str| rig.state.shards.is_open(leaf);
    leaves.iter().filter(open).count()
}

/// **A cold join converges, and a fresh cell's start costs exactly its sequence's writes** (the join
/// ruling, Addendum 3: cold convergence; Addendum 23 §2: the cold join counts its writes). A cell
/// with no document and a count of two, every operation of both stores 20 ms late. `streams-1`
/// boots alone: its first pass lists every counted ordinal, itself stamped (C17), and it holds
/// every leaf. `streams-2` then boots cold, with no previous beat: it finds itself listed unstamped,
/// unlists (C13), `streams-1` pins what the prospective ring gives `streams-2`, `streams-2` joins
/// by coverage, and each pin returns giver first. Printed, as `JOIN_COLD`: `streams-2`'s
/// membership passes to its join (its `fleet joined` line's `passes`), the time from its boot to
/// its join, to its first served leaf and to every leaf it won, and the document's landed writes.
/// Asserted: it joins with its boot id at a pass it counted, serves what it won within twice
/// BOUND, nothing is fenced, and the overrides document took exactly the listing, the unlisting,
/// the pin CAS, the join and one return per won leaf, with none on the idle passes after.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_cold_join_over_a_20_ms_store_converges() {
    let data: Arc<dyn ObjectStore> = FaultStore::uniform(mem(), 0xC01D, COLD);
    let traced = TraceStore::verbatim(FaultStore::uniform(mem(), 0xC01E, COLD));
    let fleet: Arc<dyn ObjectStore> = traced.clone();
    desired(&fleet, 2, 1).await;
    let a = member(&data, fleet.clone(), "streams-1", RigRuntime::first()).await;
    until("streams-1 lists the count", async || {
        listed(&fleet).await == ["streams-1", "streams-2"]
    })
    .await;
    open_owned(&a).await;
    let won = won_by("streams-2");
    assert!(!won.is_empty(), "streams-2 wins a leaf streams-1 holds");
    let rise = tokio::time::Instant::now();
    let b = member(
        &data,
        fleet.clone(),
        "streams-2",
        RigRuntime::incarnation(1),
    )
    .await;
    let (join_ms, first_ms) = converged(&b, &won, rise).await;
    let all_ms = rise.elapsed().as_millis();
    tokio::time::sleep(Duration::from_secs(4)).await;
    let writes = overrides_puts(&traced.events());
    let passes = b.state.fleet.standing().joined_at_pass();
    eprintln!(
        "JOIN_COLD store_ms=20 passes={passes:?} boot_to_join_ms={join_ms:?} \
         boot_to_first_leaf_ms={first_ms:?} boot_to_all_leaves_ms={all_ms} leaves={} writes={writes}",
        won.len()
    );
    let stamped = listed_boot(&fleet, "streams-2").await;
    let counted = passes.is_some_and(|passes| passes >= 1);
    let boot = Some(b.state.runtime.identity.boot_id.clone());
    assert_eq!(
        (stamped, counted, nothing_fenced(&[&a, &b])),
        (Some(boot), true, vec![(0, 0), (0, 0)]),
        "(its boot id listed, a counted join pass, fences)"
    );
    assert_eq!(
        writes,
        4 + won.len(),
        "the listing, the unlisting, the pins, the join and one return per won leaf"
    );
    for rig in [a, b] {
        stop(rig).await;
    }
}

/// Until `b` serves every leaf of `won`: the milliseconds from `rise` to its join and to its first
/// served leaf, bounded by twice BOUND.
async fn converged(
    b: &HttpRig,
    won: &[&str],
    rise: tokio::time::Instant,
) -> (Option<u128>, Option<u128>) {
    let (mut join_ms, mut first_ms) = (None, None);
    loop {
        let open = served(b, won).await;
        let ms = rise.elapsed().as_millis();
        if join_ms.is_none() && b.state.fleet.standing().joined_at_pass().is_some() {
            join_ms = Some(ms);
        }
        if first_ms.is_none() && open > 0 {
            first_ms = Some(ms);
        }
        if open == won.len() {
            return (join_ms, first_ms);
        }
        assert!(rise.elapsed() < 2 * BOUND, "streams-2 serves what it won");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// The PUTs of the overrides document that landed in `events`.
fn overrides_puts(events: &[StoreTraceEvent]) -> usize {
    let put = |event: &&StoreTraceEvent| {
        event.op == StoreOp::Put
            && event.path == "fleet/overrides.json"
            && matches!(event.outcome, TraceOutcome::Ok)
    };
    events.iter().filter(put).count()
}

/// **A boot whose own read of the document is parked writes no beat** (the join ruling, Addenda 3
/// and 7: item 40's stuck-tick test from the other side). `streams-1`'s fleet store parks every GET
/// of the overrides document from before its boot, so its heartbeat's boot read never answers:
/// for three beat periods no beat of it is stored. Released, the document lists no one, and it
/// beats at once.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_boot_whose_document_read_is_parked_writes_no_beat() {
    let (data, fleet) = (mem(), mem());
    desired(&fleet, 1, 1).await;
    let parked = FaultStore::new(fleet.clone(), 0xA7, FaultProfile::clean());
    let path = "fleet/overrides.json";
    let entered = parked.hold_class_under(StoreOp::Get, ObjClass::Fleet, path, u64::MAX);
    let a = member(&data, parked.clone(), "streams-1", RigRuntime::first()).await;
    until("the boot's document read is parked", async || {
        entered.load(std::sync::atomic::Ordering::SeqCst) > 0
    })
    .await;
    tokio::time::sleep(Duration::from_secs(6)).await;
    let while_parked = beat_boot(&fleet, "streams-1").await;
    parked.release_hold();
    let boot = a.state.runtime.identity.boot_id.clone();
    until("streams-1 beats once its read answers", async || {
        beat_boot(&fleet, "streams-1").await.as_deref() == Some(boot.as_str())
    })
    .await;
    assert_eq!(
        while_parked, None,
        "no beat while its own document read is parked"
    );
    stop(a).await;
}

/// **A member that restarts alone after its drain serves at once** (the join ruling, Addendum 44):
/// the redeploy of a one-instance cell. `streams-1` is the cell's only member; its drain finds no
/// peer and announces; it stops, and its next boot finds its previous beat draining, so it unlists
/// itself (C13) and the document lists nobody. Nothing is uncovered then: no instance could pin, so
/// it joins at its next pass, with no fallback counted, and serves a record it acked before.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_lone_member_restarted_after_its_drain_joins_at_once() {
    let (data, fleet) = (mem(), mem());
    desired(&fleet, 1, 1).await;
    let a = member(&data, fleet.clone(), "streams-1", RigRuntime::first()).await;
    super::fixture_members::joined(&fleet, &[&a], BOUND).await;
    let (created, _, _) = hreq(a.addr, "PUT", STREAM, &[JSON], b"").await;
    let (acked, _, _) = hreq(a.addr, "POST", STREAM, &[JSON], br#"[{"n":1}]"#).await;
    assert_eq!((created, acked), (201, 204), "the record before the drain");
    let drained = crate::fleet::drain::drain(&a.state, "streams-1", BOUND).await;
    assert_eq!(drained, crate::fleet::drain::DrainOutcome::NoPeer);
    stop(a).await;
    let a = member(
        &data,
        fleet.clone(),
        "streams-1",
        RigRuntime::incarnation(1),
    )
    .await;
    until_joined("the lone member's next boot joins", async || {
        a.state.fleet.standing().joined_at_pass().is_some()
    })
    .await;
    super::fixture_members::joined(&fleet, &[&a], BOUND).await;
    let (status, _, body) = hreq(a.addr, "GET", STREAM_FROM_START, &[], b"").await;
    assert_eq!(
        (
            status,
            body.as_slice(),
            a.state.fleet.standing().joins_unpinned()
        ),
        (200, br#"[{"n":1}]"#.as_slice(), 0),
        "(the read's status, the record before the drain, joins past JOIN_PHASE)"
    );
    stop(a).await;
}

/// **A whole cell restarted late serves at once** (the join ruling, Addendum 44). Both members'
/// previous beats are 60 s old, past C13's 12 s, so each unlists itself and the document lists
/// nobody: one joins by its CAS, the other's CAS is lost to it (or planned after it), so it waits
/// for that member's pins and joins covered. Each pinned leaf then moves to it giver first: no
/// engine is fenced, no fallback is counted, and each serves the leaves it wins.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_whole_cell_restarted_late_joins_without_a_fence() {
    let (data, fleet) = (mem(), mem());
    desired(&fleet, 2, 1).await;
    for name in ["streams-1", "streams-2"] {
        super::fixture_members::seed_joined(&fleet, name, "before").await;
        previous_beat(&fleet, name, 60_000).await;
    }
    let a = member(&data, fleet.clone(), "streams-1", RigRuntime::first()).await;
    let b = member(
        &data,
        fleet.clone(),
        "streams-2",
        RigRuntime::incarnation(1),
    )
    .await;
    until_joined("both members of the restarted cell join", async || {
        stamped(&fleet, &[&a, &b]).await == 2
    })
    .await;
    super::fixture_members::joined(&fleet, &[&a, &b], BOUND).await;
    for (rig, name) in [(&a, "streams-1"), (&b, "streams-2")] {
        let won = won_by(name);
        assert!(!won.is_empty(), "{name} wins a leaf");
        until("each member serves the leaves it wins", async || {
            served(rig, &won).await == won.len()
        })
        .await;
    }
    let fallbacks = [&a, &b].map(|rig| rig.state.fleet.standing().joins_unpinned());
    assert_eq!(
        (fallbacks, nothing_fenced(&[&a, &b])),
        ([0, 0], vec![(0, 0), (0, 0)]),
        "(joins past JOIN_PHASE, fences)"
    );
    for rig in [a, b] {
        stop(rig).await;
    }
}

/// How many of `rigs` the document lists with their own boot ids.
async fn stamped(fleet: &Arc<dyn ObjectStore>, rigs: &[&HttpRig]) -> usize {
    let members = stored(fleet).await.0;
    let own = |rig: &&&HttpRig| {
        let name = &rig.state.config.cli.instance_name;
        let boot = Some(&rig.state.runtime.identity.boot_id);
        members.iter().any(|(n, b)| n == name && b.as_ref() == boot)
    };
    rigs.iter().filter(own).count()
}

/// **A member back behind a peer that stays down serves within the join's bound** (the second gap,
/// GATE-DECISION-s22-join-2026-11-20 Addenda 45 and 46). A two-member cell stopped whole: both are
/// listed with the boot `before`, each with a beat 60 s old; `streams-1` boots again and
/// `streams-2` stays down. `streams-1` unlists its old boot and joins behind `streams-2`, whose
/// stale beat its watch settles after two beat periods: the coverage ring is empty, so it joins
/// over nothing to cover, its own ring holds it within `JOIN_BOUND`, no join is counted unpinned
/// and nothing is fenced. Before, its coverage was over C3's unfiltered fallback, which put
/// `streams-2` back, and it waited out `JOIN_PHASE` (146 s measured, `joins_unpinned` 1).
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_member_back_behind_a_peer_that_stays_down_serves_within_the_joins_bound() {
    let (data, fleet) = (mem(), mem());
    desired(&fleet, 2, 1).await;
    for name in ["streams-1", "streams-2"] {
        super::fixture_members::seed_joined(&fleet, name, "before").await;
        previous_beat(&fleet, name, 60_000).await;
    }
    let a = member(&data, fleet.clone(), "streams-1", RigRuntime::first()).await;
    let me = "streams-1".to_string();
    let t0 = tokio::time::Instant::now();
    while !a.state.ownership.ring_active().contains(&me) && t0.elapsed() < JOIN_BOUND {
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let (ring, unpinned) = (
        a.state.ownership.ring_active(),
        a.state.fleet.standing().joins_unpinned(),
    );
    assert_eq!(
        (ring, unpinned, nothing_fenced(&[&a])),
        (vec![me], 0, vec![(0, 0)]),
        "streams-1's ring within {JOIN_BOUND:?} ({:?} elapsed), joins unpinned, fences",
        t0.elapsed()
    );
    stop(a).await;
}

/// **A joiner's step does not rest on one comparison of two hosts' clocks** (Addendum 46's guard,
/// as Addendum 58 states it). A scale-out with `streams-1`'s beats stamped 40 s behind the wall
/// clock, as a joiner whose own clock is that far ahead reads them: each is stale on arrival, but
/// each read finds a newer one, so the watch never settles it. `streams-1`'s pass is held so it
/// cannot pin; `streams-2` stays unlisted while the hold lasts, then joins covered (no join
/// unpinned) by a document that carries `streams-1`'s pins of the leaves it wins. Red with the
/// watch at zero: `streams-2` joins over an empty coverage ring while `streams-1` is held. It does
/// not assert on fences: once `streams-2` is a member, R1 judges the same beats stale by the same
/// clock and both members ring themselves alone, so a writer is fenced with the guard or without
/// (4 of 5 runs and 3 of 5, 1 to 4 writers): R1's skew exposure, recorded in NEXT-WORK.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_joiners_step_does_not_rest_on_one_clock_comparison() {
    let (data, fleet) = (mem(), mem());
    desired(&fleet, 1, 1).await;
    let a = member(&data, fleet.clone(), "streams-1", RigRuntime::first()).await;
    a.state.fleet.standing().skew_beats(40_000);
    until("streams-1 is listed", async || {
        listed(&fleet).await == ["streams-1"]
    })
    .await;
    let (reached, reached_rx) = tokio::sync::oneshot::channel();
    let (go_tx, go) = tokio::sync::oneshot::channel();
    let point = crate::topology::gate::PausePoint { reached, go };
    a.state.fleet.standing().pause_view_publication(point);
    reached_rx.await.expect("streams-1's pass is held");
    desired(&fleet, 2, 2).await;
    let b = member(
        &data,
        fleet.clone(),
        "streams-2",
        RigRuntime::incarnation(1),
    )
    .await;
    tokio::time::sleep(Duration::from_secs(5)).await;
    let held = listed(&fleet).await;
    assert!(go_tx.send(()).is_ok());
    let joined = first_listing(&fleet, 2).await;
    let both = ["streams-1".to_string(), "streams-2".to_string()];
    let won: Vec<String> = LEAVES
        .iter()
        .filter(|leaf| both[crate::ownership::ring_pick(leaf, &both)] == "streams-2")
        .map(|leaf| (*leaf).to_string())
        .collect();
    let pinned: Vec<String> = won
        .iter()
        .filter(|leaf| {
            joined
                .entries
                .get(*leaf)
                .is_some_and(|entry| entry.to == "streams-1")
        })
        .cloned()
        .collect();
    let unpinned = b.state.fleet.standing().joins_unpinned();
    assert_eq!(
        (held, unpinned, pinned),
        (vec!["streams-1".to_string()], 0, won),
        "(the listing while streams-1 was held, streams-2's joins unpinned, the leaves it won that \
         the join's document pins to streams-1)"
    );
    for rig in [a, b] {
        stop(rig).await;
    }
}

/// The first overrides document on `fleet` that lists `members` members, read every 10 ms within
/// [`BOUND`].
async fn first_listing(fleet: &Arc<dyn ObjectStore>, members: usize) -> crate::fleet::Overrides {
    let t0 = tokio::time::Instant::now();
    loop {
        let got = fleet.get(&Path::from("fleet/overrides.json")).await;
        if let Ok(got) = got {
            let doc: crate::fleet::Overrides =
                serde_json::from_slice(&got.bytes().await.unwrap()).unwrap();
            if doc
                .members
                .as_ref()
                .is_some_and(|listed| listed.len() == members)
            {
                return doc;
            }
        }
        assert!(t0.elapsed() < BOUND, "a document lists {members} members");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}
