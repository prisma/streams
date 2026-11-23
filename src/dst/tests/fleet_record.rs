#![cfg(test)]
//! The membership of record (§22 steps 4 and 5; GATE-DECISION-s22-addendum-2026-11-19 C1-C5,
//! C17; GATE-DECISION-s22-join-2026-11-20). Every view's ring is the document's members less those
//! its own beats hold evidence against, and only a join CAS adds a member. These pin a count rise
//! (the newcomer joins once pinned) and a drop (the interim's removal half, today's class until the
//! leave), a listed member stale on one reader, the first listing of an older cell's document and
//! of a fresh cell's (C17: the writer stamps itself), two members creating one at once, and a write
//! whose answer was lost (C4).

use super::fixture_http::{HttpRig, HttpRigOptions, engine_shutdown, http_rig_build};
use super::fixture_runtime::RigRuntime;
use super::fixture_storage::mem;
use crate::dst::fault_store::{FaultProfile, FaultStore};
use crate::dst::trace_store::{StoreTraceEvent, TraceOutcome, TraceStore};
use crate::dst::{ObjClass, StoreOp};
use object_store::{ObjectStore, ObjectStoreExt, PutPayload, path::Path};
use std::sync::Arc;
use std::time::Duration;

/// How long a fleet step (a write, a view) may take: several 2 s ticks.
const BOUND: Duration = Duration::from_secs(20);

/// Only the test moves the count: no shrink is published before an hour, and with a fleet
/// maximum of one the tick plans no growth either (in a busy suite the shared process's CPU in
/// every beat would plan one, and the tick would publish it at once over the test's count).
fn held_count(cli: &mut crate::config::CliArgs) {
    cli.scale_in_secs = 3_600;
    cli.fleet_max = 1;
}

/// Member `name` of the cell over `data` and `fleet`, its fleet started.
async fn member(
    data: &Arc<dyn ObjectStore>,
    fleet: Arc<dyn ObjectStore>,
    name: &str,
    runtime: RigRuntime,
) -> HttpRig {
    let rig = http_rig_build(
        data.clone(),
        runtime,
        HttpRigOptions {
            fleet_store: Some(fleet),
            instance: Some(name.to_string()),
            cli: held_count,
            prefixes: vec!["0".into(), "1".into()],
            ..Default::default()
        },
    )
    .await;
    assert!(crate::fleet::start_configured(
        rig.state.clone(),
        &rig.tasks
    ));
    rig
}

async fn stop(rig: HttpRig) {
    let report = rig.tasks.shutdown(Duration::from_secs(3)).await;
    assert!(report.aborted.is_empty(), "{report:?}");
    engine_shutdown(&rig.state).await;
}

/// Publish desired.json with `count` at `epoch`, as another member's tick would.
async fn desired(fleet: &Arc<dyn ObjectStore>, count: u64, epoch: u64) {
    let body = format!(r#"{{"count":{count},"epoch":{epoch},"reason":"test","computed_at_ms":0}}"#);
    let path = Path::from("fleet/desired.json");
    fleet.put(&path, PutPayload::from(body)).await.unwrap();
}

/// The stored overrides document's members by name (every boot id absent, as the interim writes
/// them) and its ETag.
async fn stored(fleet: &Arc<dyn ObjectStore>) -> (Option<Vec<String>>, Option<String>) {
    let path = Path::from("fleet/overrides.json");
    let Ok(got) = fleet.get(&path).await else {
        return (None, None);
    };
    let e_tag = got.meta.e_tag.clone();
    let body = got.bytes().await.unwrap();
    let doc: crate::fleet::Overrides = serde_json::from_slice(&body).unwrap();
    // The join stamps each member's boot id (R1a); these read the names only.
    let members = doc
        .members
        .map(|listed| listed.into_iter().map(|member| member.name).collect());
    (members, e_tag)
}

/// The PUTs that landed on the overrides document, as `trace` recorded them.
fn overrides_writes(trace: &TraceStore) -> usize {
    let landed = |event: &&StoreTraceEvent| {
        event.op == StoreOp::Put
            && event.path == "fleet/overrides.json"
            && matches!(event.outcome, TraceOutcome::Ok)
    };
    trace.events().iter().filter(landed).count()
}

fn names(list: &[&str]) -> Vec<String> {
    list.iter().map(|name| (*name).to_string()).collect()
}

/// Wait, bounded, until the document lists `listed` and every rig's ring is `ring`; `what` names
/// the step. Every sample's rings are kept, so a ring outside `allowed` fails the step.
async fn settled(
    what: &str,
    fleet: &Arc<dyn ObjectStore>,
    rigs: &[&HttpRig],
    (listed, ring): (&[&str], &[&str]),
    allowed: &[&[&str]],
) {
    let t0 = tokio::time::Instant::now();
    loop {
        let rings: Vec<Vec<String>> = rigs
            .iter()
            .map(|rig| rig.state.ownership.ring_active())
            .collect();
        for seen in &rings {
            assert!(
                allowed.iter().any(|ring| names(ring) == *seen),
                "{what}: a ring today's rule never gives: {seen:?}"
            );
        }
        let members = stored(fleet).await.0;
        if members.as_deref() == Some(names(listed).as_slice())
            && rings.iter().all(|seen| *seen == names(ring))
        {
            return;
        }
        assert!(t0.elapsed() < BOUND, "{what}: {members:?} {rings:?}");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Every reader's ring is `today` before, while and after the holder of `1` among `readers` hands it
/// to another reader giver first (`fleet::handoff::hand_off`), as a co-location's holder does
/// (GATE-DECISION-s22-join-2026-11-20 Addendum 13: a co-location writes no pin, only its
/// handoffs). Sampled every 2 ms while the move runs, at once after it, and for a tick and a half
/// after, when each reader has published the written document whole.
async fn across_a_colocate(readers: &[&HttpRig], today: &[&str]) {
    let rings = || -> Vec<Vec<String>> {
        readers
            .iter()
            .map(|rig| rig.state.ownership.ring_active())
            .collect()
    };
    let today = vec![names(today); readers.len()];
    assert_eq!(rings(), today, "before the co-location");
    let holds = |rig: &&&HttpRig| {
        let owner = rig.state.ownership.effective_owner("1");
        owner.as_deref() == Some(rig.state.ownership.instance())
    };
    let state = &readers.iter().find(holds).expect("a reader holds 1").state;
    let me = state.ownership.instance();
    let to = readers
        .iter()
        .map(|rig| rig.state.ownership.instance())
        .find(|name| *name != me)
        .expect("another reader")
        .to_string();
    let done = std::sync::atomic::AtomicBool::new(false);
    let colocated = async {
        let change = crate::fleet::handoff::Change::To(to.clone());
        let moved = crate::fleet::handoff::hand_off(&state.fleet, "1", change, false).await;
        done.store(true, std::sync::atomic::Ordering::SeqCst);
        moved
    };
    let sampled = async {
        while !done.load(std::sync::atomic::Ordering::SeqCst) {
            assert_eq!(rings(), today, "while the co-location runs");
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    };
    let (moved, ()) = futures_util::future::join(colocated, sampled).await;
    assert!(
        matches!(moved, crate::fleet::handoff::Outcome::Moved),
        "the move commits: {moved:?}"
    );
    assert_eq!(rings(), today, "at once after the move");
    let t0 = tokio::time::Instant::now();
    while t0.elapsed() < Duration::from_secs(3) {
        assert_eq!(rings(), today, "after the co-location");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let entry = state.ownership.overrides().get("1").cloned();
    assert_eq!(entry, Some(to), "the move's entry is in the view");
}

/// **The record follows a count rise by a join and a drop by the removal half** (Addendum 14
/// condition 1, as the join changes it). Two live members on a count of one: the document lists
/// `streams-1` (stamped by its C17 write) and both views' ring is `streams-1`. The count rises to
/// two: `streams-1` pins, `streams-2` joins by its CAS, and both rings become both. At that steady
/// count the holder of `1` hands it to the other giver first, as a co-location's holder does, and
/// both rings stay both before, while and after (`across_a_colocate`). The count drops to
/// one: `streams-1`'s next pin CAS unlists `streams-2` (the removal half, today's class until the
/// leave) and both rings are `streams-1`. No sample shows a ring outside these.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn the_record_lists_todays_ring_through_a_count_rise_and_a_drop() {
    let (data, fleet) = (mem(), mem());
    desired(&fleet, 1, 1).await;
    let a = member(&data, fleet.clone(), "streams-1", RigRuntime::first()).await;
    let b = member(
        &data,
        fleet.clone(),
        "streams-2",
        RigRuntime::incarnation(1),
    )
    .await;
    let (one, both): (&[&str], &[&str]) = (&["streams-1"], &["streams-1", "streams-2"]);
    settled("a count of one", &fleet, &[&a, &b], (one, one), &[one, &[]]).await;
    desired(&fleet, 2, 2).await;
    settled(
        "the rise to two",
        &fleet,
        &[&a, &b],
        (both, both),
        &[one, both],
    )
    .await;
    // Setup only (the join ruling, Addenda 23 §2 (a) and 25 (i)): the rise settles before the
    // co-location picks the holder of `1`, so both views name the same one and nothing moves after.
    // `streams-2` wins both leaves, and the load-aware return gives a home back no more than its
    // fair share, so one of the rise's pins may stand on `streams-1`.
    super::fixture_members::settled(&fleet, &[&a, &b], BOUND).await;
    across_a_colocate(&[&a, &b], both).await;
    desired(&fleet, 1, 3).await;
    settled(
        "the drop to one",
        &fleet,
        &[&a, &b],
        (one, one),
        &[one, both],
    )
    .await;
    for rig in [a, b] {
        stop(rig).await;
    }
}

/// **An older cell's document is listed once, by a member of today's ring** (C17). The overrides
/// document has no `members`, and the count is two; `streams-1` boots alone. Its first pass lists
/// `streams-1` and `streams-2`, and its ring stays `streams-1` throughout (today's rule:
/// `streams-2` has no beat). Five more passes write nothing: the document's ETag is unchanged (an
/// idle cell's request count, C16's measure). `streams-2` stays listed unstamped (it is no
/// "unlisted ordinal", so no pin is written for it). The entries and outbox a write keeps are
/// `join::plan`'s clone; an entry here would be rewritten by return-home (a no-op entry) and an
/// event by the outbox drain, either of which moves the ETag.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn an_older_cells_document_is_listed_once_by_a_member_of_todays_ring() {
    let (data, fleet) = (mem(), mem());
    desired(&fleet, 2, 1).await;
    let older = br#"{"entries":{}}"#;
    let path = Path::from("fleet/overrides.json");
    fleet
        .put(&path, PutPayload::from_static(older))
        .await
        .unwrap();
    let a = member(&data, fleet.clone(), "streams-1", RigRuntime::first()).await;
    let (one, both): (&[&str], &[&str]) = (&["streams-1"], &["streams-1", "streams-2"]);
    settled("the upgrade", &fleet, &[&a], (both, one), &[one, &[]]).await;
    let (_, listed_tag) = stored(&fleet).await;
    tokio::time::sleep(Duration::from_secs(10)).await;
    let (members, idle_tag) = stored(&fleet).await;
    assert_eq!((members, idle_tag == listed_tag), (Some(names(both)), true));
    stop(a).await;
}

/// **A members write whose answer was lost is found by the next read** (C4, for the interim's one
/// write path). The PUT of the overrides document is held, then released with its response lost:
/// the write landed and the writer saw a failure. Its read after the CAS finds the document listing
/// the ordinal set, its ring is today's, and it writes no second time.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_members_write_whose_answer_was_lost_is_found_by_the_next_read() {
    let (data, fleet) = (mem(), mem());
    desired(&fleet, 1, 1).await;
    let held = FaultStore::new(fleet.clone(), 0x5140, FaultProfile::clean());
    let engaged = held.hold_class_under(StoreOp::Put, ObjClass::Fleet, "fleet/overrides.json", 1);
    let a = member(&data, held.clone(), "streams-1", RigRuntime::first()).await;
    let t0 = tokio::time::Instant::now();
    while engaged.load(std::sync::atomic::Ordering::SeqCst) == 0 {
        assert!(t0.elapsed() < BOUND, "the members write is held");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    held.release_hold_lost();
    let one: &[&str] = &["streams-1"];
    settled("the lost answer", &fleet, &[&a], (one, one), &[one, &[]]).await;
    let (_, found_tag) = stored(&fleet).await;
    tokio::time::sleep(Duration::from_secs(6)).await;
    assert_eq!(stored(&fleet).await.1, found_tag, "no second write");
    stop(a).await;
}

/// **A listed member whose beat is stale on one reader** (condition 1's case, §2.1; R1). Count
/// two: both boot, `streams-2` joins and both are listed with their boot ids. `streams-2`'s beats
/// then stop landing (its PUTs are held) and its last beat ages past the ring's liveness on
/// `streams-1`: evidence of its listed boot, so `streams-1`'s ring drops it, while `streams-2`'s own
/// ring keeps itself (listed with its own boot id). `members` is unchanged throughout: no reader
/// writes for a liveness judgement.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_listed_member_stale_on_one_reader_leaves_that_readers_ring_alone() {
    let (data, fleet) = (mem(), mem());
    desired(&fleet, 2, 1).await;
    let silent = FaultStore::new(fleet.clone(), 0x5141, FaultProfile::clean());
    let (first, second) = (RigRuntime::first(), RigRuntime::incarnation(1));
    // Setup only (the join ruling, Addendum 23 §2 (b)): both listed with their boot ids before
    // they boot, so the cell starts converged and the join writes nothing after `listed_tag`.
    let listed = [("streams-1", &first), ("streams-2", &second)];
    super::fixture_members::seed_members(&fleet, &listed).await;
    let a = member(&data, fleet.clone(), "streams-1", first).await;
    let b = member(&data, silent.clone(), "streams-2", second).await;
    let (one, two, both): (&[&str], &[&str], &[&str]) =
        (&["streams-1"], &["streams-2"], &["streams-1", "streams-2"]);
    // `two`: today's rule on `streams-2` at a pass that has not yet read `streams-1`'s first beat
    // (a beat waits one period, as a pass does, so `streams-2`'s first pass can run before
    // `streams-1`'s first beat lands): its candidates are itself alone.
    settled(
        "both listed",
        &fleet,
        &[&a, &b],
        (both, both),
        &[one, two, both, &[]],
    )
    .await;
    let (_, listed_tag) = stored(&fleet).await;
    let beats = "fleet/streams-2.json";
    let _held = silent.hold_class_under(StoreOp::Put, ObjClass::Fleet, beats, u64::MAX);
    let t0 = tokio::time::Instant::now();
    while a.state.ownership.ring_active() != names(one) {
        assert!(
            t0.elapsed() < Duration::from_secs(45),
            "streams-2's beat goes stale on streams-1"
        );
        assert_eq!(
            b.state.ownership.ring_active(),
            names(both),
            "streams-2 keeps itself"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let after = (stored(&fleet).await, b.state.ownership.ring_active());
    silent.release_hold();
    assert_eq!(after, ((Some(names(both)), listed_tag), names(both)));
    for rig in [a, b] {
        stop(rig).await;
    }
}

/// **A fresh cell's document is created once** (§2.4). No overrides document, a count of two, one
/// member: its first pass creates the document listing `streams-1` and `streams-2`, in exactly one
/// write, and five more passes write nothing; its ring is today's throughout.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_fresh_cells_document_is_created_listing_the_ordinals_in_one_write() {
    let (data, fleet) = (mem(), mem());
    desired(&fleet, 2, 1).await;
    let trace = TraceStore::verbatim(fleet.clone());
    let a = member(&data, trace.clone(), "streams-1", RigRuntime::first()).await;
    let (one, both): (&[&str], &[&str]) = (&["streams-1"], &["streams-1", "streams-2"]);
    settled("the creation", &fleet, &[&a], (both, one), &[one, &[]]).await;
    let created = overrides_writes(&trace);
    tokio::time::sleep(Duration::from_secs(10)).await;
    assert_eq!((created, overrides_writes(&trace)), (1, 1));
    stop(a).await;
}

/// **Two members creating a fresh cell's document at once end with one document** (§2.4, as the join
/// changes it). Both boot on an empty fleet store at a count of two: one create lands, listing its
/// writer stamped and the other unstamped; the other's create is refused and its read finds the
/// document. That one finds itself listed unstamped with no young beat (C13, the join ruling §2):
/// it unlists, and joins by coverage once the creator pins. Both end listed with their boot ids,
/// both rings are both, and idle passes then write nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn two_members_creating_a_fresh_cells_document_at_once_end_with_one() {
    let (data, fleet) = (mem(), mem());
    desired(&fleet, 2, 1).await;
    let trace = TraceStore::verbatim(fleet.clone());
    let (a, b) = futures_util::future::join(
        member(&data, trace.clone(), "streams-1", RigRuntime::first()),
        member(
            &data,
            trace.clone(),
            "streams-2",
            RigRuntime::incarnation(1),
        ),
    )
    .await;
    let (one, two, both): (&[&str], &[&str], &[&str]) =
        (&["streams-1"], &["streams-2"], &["streams-1", "streams-2"]);
    settled(
        "one document",
        &fleet,
        &[&a, &b],
        (both, both),
        &[one, two, both, &[]],
    )
    .await;
    let settled_writes = overrides_writes(&trace);
    tokio::time::sleep(Duration::from_secs(6)).await;
    assert_eq!(
        overrides_writes(&trace),
        settled_writes,
        "no write on idle passes"
    );
    for rig in [a, b] {
        stop(rig).await;
    }
}

/// The cell of two at its fair share (the join ruling, Addendum 25): `streams-1` alone, then the
/// count rises to two and `streams-2` joins. `streams-2` wins both leaves; the load-aware return
/// gives it back one, so one pin stands on `streams-1`, whichever the passes returned first. The
/// members and the pin.
async fn at_the_fair_share(
    data: &Arc<dyn ObjectStore>,
    fleet: &Arc<dyn ObjectStore>,
) -> (HttpRig, HttpRig, (String, String)) {
    desired(fleet, 1, 1).await;
    let a = member(data, fleet.clone(), "streams-1", RigRuntime::first()).await;
    let one: &[&str] = &["streams-1"];
    settled("a count of one", fleet, &[&a], (one, one), &[one, &[]]).await;
    desired(fleet, 2, 2).await;
    let b = member(data, fleet.clone(), "streams-2", RigRuntime::incarnation(1)).await;
    let pins = super::fixture_members::settled(fleet, &[&a, &b], BOUND).await;
    let [pin] = <[(String, String); 1]>::try_from(pins).expect("one pin stands at the fair share");
    assert_eq!(pin.1, "streams-1", "the giver keeps the pin");
    (a, b, pin)
}

/// Fenced engines and handoffs ended by a fence, per rig: none in either scenario below.
fn fences(rigs: &[&HttpRig]) -> Vec<(u64, u64)> {
    let fenced = |rig: &&HttpRig| {
        let handoffs = rig.state.fleet.standing().handoffs_fenced();
        (rig.state.shards.fenced_ahead(), handoffs)
    };
    rigs.iter().map(fenced).collect()
}

/// **A join's pin at the fair share stands, and the next ring change lifts it** (the join ruling,
/// Addendum 25 (i)-(iii)). Settled at two with one pin standing ([`at_the_fair_share`]), the pin
/// is judged again every pass and kept for three more. The count then rises to three and
/// `streams-3` boots: over three `0` is `streams-2`'s and `1` is `streams-3`'s. Until `streams-3`
/// is listed the pin stands as C7 has it (its holder's, the prospective ring another's); then the
/// shares change and it is lifted by a return, giver first: every view gives each leaf its home
/// over three, no pin is left and nothing is fenced.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_join_pin_at_the_fair_share_stands_until_the_next_ring_change_lifts_it() {
    let (data, fleet) = (mem(), mem());
    let (a, b, pin) = at_the_fair_share(&data, &fleet).await;
    let stood = super::fixture_members::pins(&fleet).await;
    tokio::time::sleep(Duration::from_millis(6_500)).await;
    let kept = super::fixture_members::settled(&fleet, &[&a, &b], BOUND).await;
    let one = vec![pin.clone()];
    assert_eq!((stood, kept), (one.clone(), one), "three passes keep it");
    desired(&fleet, 3, 3).await;
    let c = member(
        &data,
        fleet.clone(),
        "streams-3",
        RigRuntime::incarnation(2),
    )
    .await;
    let t0 = tokio::time::Instant::now();
    let mut held_until_listed = true;
    loop {
        let (members, _) = stored(&fleet).await;
        let listed = members
            .unwrap_or_default()
            .iter()
            .any(|name| name == "streams-3");
        let pins = super::fixture_members::pins(&fleet).await;
        if !listed {
            held_until_listed &= pins.contains(&pin);
        }
        // Every view gives each leaf its home over three.
        let homed = |rig: &&HttpRig| {
            let owner = |leaf: &str| rig.state.ownership.effective_owner(leaf);
            (owner("0"), owner("1")) == (Some("streams-2".into()), Some("streams-3".into()))
        };
        if listed && pins.is_empty() && [&a, &b, &c].iter().all(homed) {
            break;
        }
        assert!(t0.elapsed() < BOUND, "{pin:?} is lifted: {pins:?}");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert_eq!(
        (held_until_listed, fences(&[&a, &b, &c])),
        (true, vec![(0, 0); 3]),
        "(the pin stood until streams-3 was listed, fences)"
    );
    for rig in [a, b, c] {
        stop(rig).await;
    }
}

/// **A join's pin is dropped when its holder becomes the ring's home** (the join ruling,
/// Addendum 25 (iii); C6(b)). Settled at two with one pin standing on `streams-1`
/// ([`at_the_fair_share`]); the count drops to one. Over the counted ring of one the pinned leaf's
/// home is its holder, so the entry is a no-op and is dropped: the leaf never leaves `streams-1`,
/// no pin is left, and nothing is fenced.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_join_pin_is_dropped_when_its_holder_becomes_the_rings_home() {
    let (data, fleet) = (mem(), mem());
    let (a, b, pin) = at_the_fair_share(&data, &fleet).await;
    let leaf = pin.0.as_str();
    desired(&fleet, 1, 3).await;
    let t0 = tokio::time::Instant::now();
    let mut stayed = true;
    while !super::fixture_members::pins(&fleet).await.is_empty() {
        stayed &= a.state.shards.is_open(leaf);
        assert!(t0.elapsed() < BOUND, "the pin of {leaf} is dropped");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    stayed &= a.state.shards.is_open(leaf);
    assert_eq!(
        (stayed, fences(&[&a, &b])),
        (true, vec![(0, 0); 2]),
        "(the pinned leaf stayed on streams-1, fences)"
    );
    for rig in [a, b] {
        stop(rig).await;
    }
}
