#![cfg(test)]
//! F6's handoff in the cases H8 lists beside the loaded moves (GATE-DECISION-f6-prep-rows-rev2
//! H1-H8): a park that misses its bound, a lag victim's evacuation, a holder with no engine, the
//! claim against a split, and an append that resolved the giver's engine before the hold.
//! Each runs one runtime's handoff runner over a member of a ring of two whose other member
//! (`streams-2`) only needs to be named: these cases are decided on the giver. The cases end with
//! an ambiguous CAS (D5) and the giver's death before and after its write (the failure table).

use super::billing_closure_debts::JSON;
use super::fixture_http::{HttpRig, HttpRigOptions, engine_shutdown, http_rig_build};
use super::fixture_requests::hreq;
use super::fixture_runtime::RigRuntime;
use super::fixture_storage::mem;
use crate::fleet::handoff::{Change, Outcome, hand_off};
use object_store::ObjectStore;
use std::sync::Arc;
use std::time::Duration;

/// `streams-1` of a ring of two over `00` and `01`, its view published by hand, its handoff runner
/// started, and `00` (its ring home) open when `open`.
async fn giver(open: bool) -> HttpRig {
    member(&mem(), mem(), "streams-1", RigRuntime::first(), open).await
}

/// `name` of a ring of `streams-1` and `streams-2` over `00` and `01` on `data` and `fleet`: its
/// view the ring and the stored overrides, published by hand; its handoff runner started; and
/// `00` open when `open`.
async fn member(
    data: &Arc<dyn ObjectStore>,
    fleet: Arc<dyn ObjectStore>,
    name: &str,
    runtime: RigRuntime,
    open: bool,
) -> HttpRig {
    let options = HttpRigOptions {
        fleet_store: Some(fleet),
        instance: Some(name.to_string()),
        prefixes: super::fixture_http::two_bit(),
        ..Default::default()
    };
    let rig = http_rig_build(data.clone(), runtime, options).await;
    let ring = vec!["streams-1".to_string(), "streams-2".to_string()];
    let doc = rig.state.fleet.read_overrides().await.unwrap().0;
    let entries = doc.entries.into_iter().map(|(prefix, e)| (prefix, e.to));
    rig.state.ownership.set_view(ring, entries.collect());
    crate::fleet::handoff::start(rig.state.clone(), &rig.tasks);
    if open {
        let opened = rig
            .state
            .shards
            .open_or_wait("00", Duration::from_secs(5))
            .await;
        assert!(
            matches!(opened, crate::sharddir::OpenOutcome::Ready(_)),
            "00 opens"
        );
    }
    rig
}

async fn stop(rig: HttpRig) {
    let report = rig.tasks.shutdown(Duration::from_secs(3)).await;
    assert!(report.aborted.is_empty(), "{report:?}");
    engine_shutdown(&rig.state).await;
}

/// The stored overrides' entry for `prefix`: its target.
async fn entry(rig: &HttpRig, prefix: &str) -> Option<String> {
    let doc = rig.state.fleet.read_overrides().await.unwrap().0;
    doc.entries.get(prefix).map(|e| e.to.clone())
}

/// **A park that misses its bound moves nothing, and a lag victim's is evacuated** (D4, H4). With
/// `00`'s committer held, the move's barrier cannot park within its bound: an ordinary move is
/// `NotStarted` (nothing written, `00` still open and served here), and a lag victim is answered
/// `Evacuate`, for the lag move's own CAS and retirement.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_park_past_its_bound_moves_nothing_and_evacuates_a_lag_victim() {
    let rig = giver(true).await;
    let engine = rig.state.shards.open("00").expect("00 is open");
    let held = engine.test_hold_commit().await;
    // A first operation the committer takes and then holds behind the test gate.
    let (tx, _rx) = tokio::sync::oneshot::channel();
    let close = crate::shard::CloseReq {
        hash: [3; 16],
        generation: Some(1),
        resp: tx,
    };
    engine.try_close(close).unwrap();
    let to = || Change::To("streams-2".to_string());
    let ordinary = hand_off(&rig.state.fleet, "00", to(), false).await;
    let victim = hand_off(&rig.state.fleet, "00", to(), true).await;
    drop(held);
    assert_eq!((ordinary, victim), (Outcome::NotStarted, Outcome::Evacuate));
    assert_eq!(entry(&rig, "00").await, None, "nothing is written");
    assert!(rig.state.shards.is_open("00"), "00 is still served here");
    stop(rig).await;
}

/// **A holder with no engine moves its entry under the hold** (D6). `00` was never opened here:
/// the move writes the entry naming the target and mirrors it, and opens nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_holder_with_no_engine_moves_its_entry() {
    let rig = giver(false).await;
    let moved = hand_off(
        &rig.state.fleet,
        "00",
        Change::To("streams-2".to_string()),
        false,
    )
    .await;
    assert_eq!(moved, Outcome::Moved);
    assert_eq!(entry(&rig, "00").await.as_deref(), Some("streams-2"));
    let owner = rig.state.ownership.effective_owner("00");
    assert_eq!(
        (owner.as_deref(), rig.state.shards.is_open("00")),
        (Some("streams-2"), false)
    );
    stop(rig).await;
}

/// **A move and a split never hold one engine together** (step 1, E4). While a move claims `00`,
/// its split is refused `topology_busy` for `"move"`; while a split's barrier holds `00`'s
/// committer, a move does not start and writes nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_move_and_a_split_never_hold_one_engine_together() {
    let rig = giver(true).await;
    let state = &rig.state;
    let engine = state.shards.open("00").expect("00 is open");
    let gate = &engine.gc_state().gate;
    assert!(gate.claim_move(), "the move claims 00");
    let clock = state.runtime.clock.clone();
    let split =
        crate::topology::split::split(&state.shards, &engine, "op-f6", "streams-1", &*clock);
    let refused = split.await.map(|_| ()).map_err(|e| e.to_string());
    assert_eq!(refused, Err("topology_busy: move".to_string()));
    gate.release_move();
    let parked = engine.barrier().await.expect("a split's barrier parks 00");
    let moved = hand_off(
        &state.fleet,
        "00",
        Change::To("streams-2".to_string()),
        false,
    )
    .await;
    assert_eq!(moved, Outcome::NotStarted, "a parked engine is not claimed");
    parked.resume();
    assert_eq!(entry(&rig, "00").await, None, "nothing is written");
    stop(rig).await;
}

/// **An append that resolved the giver's engine before the hold is answered by the hold, never as
/// outcome unknown** (H2). The append is parked between its resolve and its enqueue; the move
/// runs to its end (`00` closed, the entry naming the target); released, the append meets the
/// closed engine, re-resolves through the hold and is answered the new owner's `409` (the target
/// is not reachable here, so it is not forwarded). Nothing of it is stored on the giver's shard.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn an_append_resolved_before_the_hold_is_answered_by_the_hold() {
    use super::fixture_livefeed::wait_parked;
    let rig = giver(true).await;
    let state = &rig.state;
    let name = stream_in_00(&rig).await;
    crate::failpoints::park_append_after_resolve(&name);
    let path = format!("/v1/stream/{name}");
    let append = hreq(rig.addr, "POST", &path, &[JSON], br#"[{"n":1}]"#);
    let moved = async {
        wait_parked(crate::failpoints::Fp::AppendAfterResolve, &name, 1).await;
        let moved = hand_off(
            &state.fleet,
            "00",
            Change::To("streams-2".to_string()),
            false,
        )
        .await;
        crate::failpoints::release_append_after_resolve(&name);
        moved
    };
    let ((status, headers, body), moved) = futures_util::future::join(append, moved).await;
    assert_eq!(moved, Outcome::Moved);
    let answer = (status, headers.get("streams-replay-to").cloned());
    let body = String::from_utf8_lossy(&body);
    assert_eq!(answer, (409, Some("streams-2".to_string())), "{body}");
    stop(rig).await;
}

/// A stream created through `rig` whose route lands in `00` (a name the ring gives `streams-2` is
/// refused `409` and skipped).
async fn stream_in_00(rig: &HttpRig) -> String {
    for n in 0..400 {
        let name = format!("c-{n}");
        let (status, _, body) =
            hreq(rig.addr, "PUT", &format!("/v1/stream/{name}"), &[JSON], b"").await;
        if status == 409 {
            continue;
        }
        assert_eq!(status, 201, "{name}: {}", String::from_utf8_lossy(&body));
        let state = &rig.state;
        let desc =
            super::billing_closure_debts::stored(state, &state.deployment.raw_adapter_sref(&name))
                .await;
        if state
            .shards
            .prefix_for(&desc.resolve_segment("").shard_route)
            == "00"
        {
            return name;
        }
    }
    panic!("no stream lands in 00");
}

/// Wait, bounded, until a fault store's hold has parked an operation.
async fn engaged(counter: &std::sync::atomic::AtomicU64) {
    let t0 = tokio::time::Instant::now();
    while counter.load(std::sync::atomic::Ordering::SeqCst) == 0 {
        assert!(t0.elapsed() < Duration::from_secs(10), "the hold engages");
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

/// Append `[{"n": n}]` to `name` through `rig` for each `n` in `ns`, each acked `204`.
async fn appended(rig: &HttpRig, name: &str, ns: std::ops::Range<u32>) {
    let path = format!("/v1/stream/{name}");
    for n in ns {
        let body = format!(r#"[{{"n":{n}}}]"#);
        let (status, _, answer) = hreq(rig.addr, "POST", &path, &[JSON], body.as_bytes()).await;
        assert_eq!(status, 204, "{n}: {}", String::from_utf8_lossy(&answer));
    }
}

/// **A CAS whose answer is lost is the move done, written once** (D5). The giver's write of the
/// entry lands and its answer is lost; the next try re-reads the document, mirrors it, finds the
/// shard placed on the target and ends the move without writing again.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_cas_whose_answer_is_lost_is_the_move_done_written_once() {
    use crate::dst::fault_store::{FaultPlan, FaultStore};
    use crate::dst::{ObjClass, StoreOp};
    let fleet = FaultStore::uniform(mem(), 0xF6D5, FaultPlan::CLEAN);
    let rig = member(
        &mem(),
        fleet.clone(),
        "streams-1",
        RigRuntime::first(),
        true,
    )
    .await;
    let before = fleet.count(StoreOp::Put, ObjClass::Fleet);
    let hold = fleet.hold_class_under(StoreOp::Put, ObjClass::Fleet, "fleet/overrides", 1);
    let lose = async {
        engaged(&hold).await;
        fleet.release_hold_lost();
    };
    let to = Change::To("streams-2".to_string());
    let (moved, ()) =
        futures_util::future::join(hand_off(&rig.state.fleet, "00", to, false), lose).await;
    assert_eq!((moved, fleet.injected_lost()), (Outcome::Moved, 1));
    let writes = fleet.count(StoreOp::Put, ObjClass::Fleet) - before;
    assert_eq!(writes, 1, "the document is written once");
    assert_eq!(entry(&rig, "00").await.as_deref(), Some("streams-2"));
    let owner = rig.state.ownership.effective_owner("00");
    assert_eq!(
        (owner.as_deref(), rig.state.shards.is_open("00")),
        (Some("streams-2"), false)
    );
    stop(rig).await;
}

/// **A giver that dies before its write leaves the shard to its restart** (the addendum's failure
/// table, first row; TLA-011's first witness). `00` holds twenty acked records; the move parks,
/// flushes and closes it, and its write of the overrides is held; the giver dies there (its tasks
/// aborted, no engine shutdown). The document is unchanged, and a restart under the same name
/// reopens `00` and serves exactly the twenty records and the next one.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_giver_that_dies_before_its_write_leaves_the_shard_to_its_restart() {
    use crate::dst::fault_store::{FaultPlan, FaultStore};
    use crate::dst::{ObjClass, StoreOp};
    let (data, inner) = (mem(), mem());
    let fleet = FaultStore::uniform(inner.clone(), 0xF6DE, FaultPlan::CLEAN);
    let rig = member(&data, fleet.clone(), "streams-1", RigRuntime::first(), true).await;
    let name = stream_in_00(&rig).await;
    appended(&rig, &name, 0..20).await;
    let hold = fleet.hold_class_under(StoreOp::Put, ObjClass::Fleet, "fleet/overrides", 1);
    let to = Change::To("streams-2".to_string());
    tokio::select! {
        moved = hand_off(&rig.state.fleet, "00", to, false) => panic!("the move ended past its held write: {moved:?}"),
        () = engaged(&hold) => {}
    }
    assert!(
        !rig.state.shards.is_open("00"),
        "the giver closed 00 before its write"
    );
    // A crash: every task aborted at once, no engine shutdown.
    let _ = rig.tasks.shutdown(Duration::ZERO).await;
    drop(rig);
    let restarted = member(&data, inner, "streams-1", RigRuntime::incarnation(1), false).await;
    assert_eq!(
        entry(&restarted, "00").await,
        None,
        "the document is unchanged"
    );
    appended(&restarted, &name, 20..21).await;
    let expected: Vec<u32> = (0..21).collect();
    assert_eq!(
        super::fleet_handoff::stored_ns(restarted.addr, &name).await,
        expected
    );
    stop(restarted).await;
}

/// **A giver that dies after its write leaves the shard to its target** (the failure table's second
/// row; TLA-011's second witness). `00` holds twenty acked records; the move ends with the entry
/// naming `streams-2`, and the giver dies (its mirror and hold die with it, so a death between the
/// write and step 7 leaves what this one does). `streams-2`, booted on the document, opens `00`
/// with no writer to fence and serves exactly the twenty records and the next one.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_giver_that_dies_after_its_write_leaves_the_shard_to_its_target() {
    let (data, fleet) = (mem(), mem());
    let rig = member(&data, fleet.clone(), "streams-1", RigRuntime::first(), true).await;
    let name = stream_in_00(&rig).await;
    appended(&rig, &name, 0..20).await;
    let giver = rig.state.shards.open("00").expect("00 is open");
    let moved = hand_off(
        &rig.state.fleet,
        "00",
        Change::To("streams-2".to_string()),
        false,
    )
    .await;
    assert_eq!(moved, Outcome::Moved);
    let reason = giver.db.subscribe().borrow().close_reason;
    assert!(
        !matches!(reason, Some(slatedb::CloseReason::Fenced)),
        "{reason:?}"
    );
    drop(giver);
    let _ = rig.tasks.shutdown(Duration::ZERO).await;
    drop(rig);
    let target = member(&data, fleet, "streams-2", RigRuntime::incarnation(1), false).await;
    assert_eq!(entry(&target, "00").await.as_deref(), Some("streams-2"));
    appended(&target, &name, 20..21).await;
    let expected: Vec<u32> = (0..21).collect();
    assert_eq!(
        super::fleet_handoff::stored_ns(target.addr, &name).await,
        expected
    );
    stop(target).await;
}

/// ERROR events logged while a loaded handoff DST ran (H8, I3: no ERROR line on either member).
/// The counter is the process's default subscriber, installed by the first test that asks; any
/// later test of the process counts nothing (`None`), and the first one counts the events of every
/// test beside it. So a test only prints its count: the stress pass, which runs each loaded DST
/// alone (`--exact`), reads it. Every counted event is printed as `HANDOFF ERROR <target>: ...`.
pub(super) struct ErrorLines(Option<Arc<std::sync::atomic::AtomicU64>>);

impl ErrorLines {
    pub(super) fn install() -> Self {
        use tracing_subscriber::layer::{Layer, SubscriberExt};
        let count = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let errors =
            CountErrors(count.clone()).with_filter(tracing_subscriber::filter::LevelFilter::ERROR);
        let installed =
            tracing::subscriber::set_global_default(tracing_subscriber::registry().with(errors));
        Self(installed.is_ok().then_some(count))
    }

    /// Prints `HANDOFF errors=<n>`, or `HANDOFF errors=none` when another test installed it.
    pub(super) fn report(&self) {
        match &self.0 {
            Some(count) => eprintln!(
                "HANDOFF errors={}",
                count.load(std::sync::atomic::Ordering::SeqCst)
            ),
            None => eprintln!("HANDOFF errors=none"),
        }
    }
}

/// Counts and prints every ERROR event.
struct CountErrors(Arc<std::sync::atomic::AtomicU64>);

impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for CountErrors {
    fn on_event(&self, event: &tracing::Event<'_>, _: tracing_subscriber::layer::Context<'_, S>) {
        self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let mut fields = Vec::new();
        event.record(&mut Fields(&mut fields));
        eprintln!(
            "HANDOFF ERROR {}: {}",
            event.metadata().target(),
            fields.join(" ")
        );
    }
}

/// An event's fields as `name=value` pairs.
struct Fields<'a>(&'a mut Vec<String>);

impl tracing::field::Visit for Fields<'_> {
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        self.0.push(format!("{}={value:?}", field.name()));
    }
}
