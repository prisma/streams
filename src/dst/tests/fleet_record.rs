#![cfg(test)]
//! The membership of record in its interim (§22 step 4; GATE-DECISION-s22-addendum-2026-11-19 C1-C4,
//! C17; GATE-DECISION-meas-full-forward-wedge-2026-11-19 Addendum 14). Until the join lands, the
//! fleet tick keeps the overrides document's `members` equal to the ordinal set within the count it
//! read, by one CAS, and every view's ring is the listed members its own beats qualify: the set
//! today's rule computes on that reader. These pin that through a count rise and a drop and a
//! listed member stale on one reader (Addendum 14 condition 1, GATE-DECISION-s22-3b-step4-2026-11-21
//! §2.1), the first write of an older cell's document (C17), a fresh cell's document created once,
//! by one of two members at once (§2.4), and a write whose answer was lost (C4).

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
    let members = doc.members.map(|listed| {
        assert!(
            listed.iter().all(|member| member.boot_id.is_none()),
            "{listed:?}"
        );
        listed.into_iter().map(|member| member.name).collect()
    });
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

/// Every reader's ring is `today` before, while and after `actor` co-locates `0` and `1` on
/// itself (`fleet::colocate`, the policy's merge step 0, called as the policy calls it): its CAS of
/// the overrides document and its `set_override` of each half, the interim's one-entry writer
/// (GATE-DECISION-s22-3b-step4-2026-11-21 Addendum 1). Sampled every 2 ms while the co-location
/// runs, at once after it (the `set_override`s just ran), and for a tick and a half after, when
/// each reader has published the written document whole.
async fn across_a_colocate(actor: &HttpRig, readers: &[&HttpRig], today: &[&str]) {
    let rings = || -> Vec<Vec<String>> {
        readers
            .iter()
            .map(|rig| rig.state.ownership.ring_active())
            .collect()
    };
    let today = vec![names(today); readers.len()];
    assert_eq!(rings(), today, "before the co-location");
    let done = std::sync::atomic::AtomicBool::new(false);
    let halves = names(&["0", "1"]);
    let state = &actor.state;
    let colocated = async {
        let moved = crate::fleet::colocate::colocate(
            &state.fleet,
            &state.ownership,
            &*state.data_store,
            &halves,
            "streams-1",
        )
        .await;
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
    assert!(moved, "the co-location commits");
    assert_eq!(rings(), today, "at once after its set_override");
    let t0 = tokio::time::Instant::now();
    while t0.elapsed() < Duration::from_secs(3) {
        assert_eq!(rings(), today, "after the co-location");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let owner = actor.state.ownership.effective_owner("1");
    assert_eq!(
        owner.as_deref(),
        Some("streams-1"),
        "the co-location's entry is in the view"
    );
}

/// **The record lists today's ring through a count rise and a count drop** (Addendum 14,
/// condition 1). Two live members on a count of one: the document lists `streams-1` and both views'
/// ring is `streams-1`. The count rises to two: the document lists both and both rings are both.
/// At that steady count `streams-1` co-locates `0` and `1` on itself, and both rings stay both
/// before, while and after (`across_a_colocate`: the interim's `set_override` writes no ring).
/// The count drops to one: the document lists `streams-1` again (written by `streams-1`;
/// `streams-2`, outside today's ring, writes nothing) and both rings are `streams-1`. No sample
/// shows a ring today's rule would not give at that count.
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
    across_a_colocate(&a, &[&a, &b], both).await;
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
/// idle cell's request count, C16's measure for this step). The entries and outbox a write keeps
/// are `record::listed`'s clone; an entry here would be rewritten by return-home (a no-op entry)
/// and an event by the outbox drain, either of which moves the ETag.
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

/// A seeded beat of `name` from the boot "seed", `age_ms` old, as a peer's tick reads it.
async fn seeded_beat(fleet: &Arc<dyn ObjectStore>, name: &str, age_ms: i64) {
    let ts = crate::shard::now_ms() - age_ms;
    let beat = format!(
        r#"{{"instance":"{name}","boot_id":"seed","ts_ms":{ts},"rps":0.0,"cpu_pct":0.0,"inflight":0,"owned_shards":[],"draining":false,"url":""}}"#
    );
    let path = Path::from(format!("fleet/{name}.json"));
    fleet.put(&path, PutPayload::from(beat)).await.unwrap();
}

/// **A listed member whose beat is stale on one reader** (condition 1's case, §2.1). Count two,
/// both listed. `streams-2`'s beats never land (its PUTs are held) and its seeded beat ages past the
/// ring's liveness, so `streams-1`'s ring is today's without it, while `streams-2`'s own ring is
/// today's with it (a member never judges its own liveness). `members` is unchanged throughout:
/// no reader writes for a liveness judgement.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_listed_member_stale_on_one_reader_leaves_that_readers_ring_alone() {
    let (data, fleet) = (mem(), mem());
    desired(&fleet, 2, 1).await;
    // Live for eight more seconds: long enough for both views to count it first.
    seeded_beat(&fleet, "streams-2", 22_000).await;
    let silent = FaultStore::new(fleet.clone(), 0x5141, FaultProfile::clean());
    let beats = "fleet/streams-2.json";
    let _held = silent.hold_class_under(StoreOp::Put, ObjClass::Fleet, beats, u64::MAX);
    let a = member(&data, fleet.clone(), "streams-1", RigRuntime::first()).await;
    let b = member(
        &data,
        silent.clone(),
        "streams-2",
        RigRuntime::incarnation(1),
    )
    .await;
    let (one, two, both): (&[&str], &[&str], &[&str]) =
        (&["streams-1"], &["streams-2"], &["streams-1", "streams-2"]);
    // `two`: today's rule on `streams-2` at a pass that has not yet read `streams-1`'s first beat
    // (a beat waits one period, as a pass does, so `streams-2`'s first pass can run before
    // `streams-1`'s first beat lands): its candidates are itself alone.
    settled(
        "both live",
        &fleet,
        &[&a, &b],
        (both, both),
        &[one, two, both, &[]],
    )
    .await;
    let (_, listed_tag) = stored(&fleet).await;
    let t0 = tokio::time::Instant::now();
    while a.state.ownership.ring_active() != names(one) {
        assert!(
            t0.elapsed() < BOUND,
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

/// **Two members creating a fresh cell's document at once end with one document** (§2.4). Both
/// boot on an empty fleet store at a count of two: one create lands, the other is refused and its
/// read finds the document listed. Both rings are today's, and the document was written once.
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
    tokio::time::sleep(Duration::from_secs(6)).await;
    assert_eq!(overrides_writes(&trace), 1);
    for rig in [a, b] {
        stop(rig).await;
    }
}
