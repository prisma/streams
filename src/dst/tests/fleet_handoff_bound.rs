//! A planned move's commit under a slow store (GATE-DECISION-s22-join-2026-11-20 Addendum 26). The
//! commit's three tries share the hold's remainder less 200 ms, not a fixed 400 ms; a won CAS is the
//! move, whatever the read after it; and a move that ran out of that bound is not tried again at
//! the next pass, but after twice the gap since its last try, at most a minute.

use super::billing_closure_debts::JSON;
use super::fixture_http::{HttpRig, HttpRigOptions, engine_shutdown, http_rig_build, two_bit};
use super::fixture_requests::hreq;
use super::fixture_runtime::RigRuntime;
use super::fixture_storage::mem;
use super::fleet_handoff::stream_in;
use crate::dst::fault_store::{FaultPlan, FaultProfile, FaultStore};
use crate::dst::{ObjClass, StoreOp};
use crate::fleet::handoff::{Change, Outcome, hand_off};
use object_store::{ObjectStore, ObjectStoreExt, PutPayload, path::Path};
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;
use tokio::time::Instant;

/// The leaf the giver holds by an hour-old entry: over the ring of two its home is `streams-2`.
const LEAF: &str = "10";

/// Write `body` at `path` on `store`.
async fn put(store: &Arc<dyn ObjectStore>, path: &str, body: String) {
    let path = Path::from(path);
    store.put(&path, PutPayload::from(body)).await.unwrap();
}

/// `streams-2`'s beat, fresh for a minute: a member with no process behind it, so no move to it
/// is ever served, only written.
async fn peer_beat(store: &Arc<dyn ObjectStore>) {
    let ts_ms = crate::shard::now_ms() + 60_000;
    let body = format!(
        r#"{{"instance":"streams-2","ts_ms":{ts_ms},"rps":0.0,"cpu_pct":0.0,"inflight":0,"owned_shards":[],"draining":false,"url":"https://streams-2.invalid"}}"#
    );
    put(store, "fleet/streams-2.json", body).await;
}

/// A cell of two by its documents: count 2, both listed, and `entries` naming `streams-1`, each an
/// hour old (past the return window).
async fn seeded(store: &Arc<dyn ObjectStore>, entries: &[&str]) {
    let desired = r#"{"count":2,"epoch":1,"reason":"seed","computed_at_ms":0}"#;
    put(store, "fleet/desired.json", desired.to_string()).await;
    let aged = crate::shard::now_ms() - 3_600_000;
    let entry = |leaf: &&str| {
        let to = "streams-1".to_string();
        (
            (*leaf).to_string(),
            crate::fleet::OverrideEntry { to, ms: aged },
        )
    };
    let member = |name: &str| crate::fleet::Member {
        name: name.to_string(),
        boot_id: None,
    };
    let overrides = crate::fleet::Overrides {
        entries: entries.iter().map(entry).collect(),
        members: Some(vec![member("streams-1"), member("streams-2")]),
        ..Default::default()
    };
    let body = serde_json::to_string(&overrides).unwrap();
    put(store, "fleet/overrides.json", body).await;
    peer_beat(store).await;
}

/// Only the test moves the count: no shrink before an hour, no growth on the test host's cores.
fn held(cli: &mut crate::config::CliArgs) {
    cli.scale_in_secs = 3_600;
    cli.fleet_max = 1;
}

/// `streams-1` over `fleet`, its fleet started.
async fn giver(fleet: Arc<dyn ObjectStore>) -> HttpRig {
    let rig = http_rig_build(
        mem(),
        RigRuntime::first(),
        HttpRigOptions {
            fleet_store: Some(fleet),
            instance: Some("streams-1".to_string()),
            cli: held,
            prefixes: two_bit(),
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

/// Append to `stream` through `addr` once a second for `window`, refreshing `streams-2`'s beat on
/// `inner` each time: how many were acked, and every other answer.
async fn appended(
    addr: std::net::SocketAddr,
    stream: &str,
    inner: &Arc<dyn ObjectStore>,
    window: Duration,
) -> (usize, Vec<(u16, String)>) {
    let path = format!("/v1/stream/{stream}");
    let (mut acked, mut refused) = (0, Vec::new());
    let t0 = Instant::now();
    while t0.elapsed() < window {
        let (status, _, body) = hreq(addr, "POST", &path, &[JSON], br#"[{"n":1}]"#).await;
        if status == 204 {
            acked += 1;
        } else {
            refused.push((status, String::from_utf8_lossy(&body).into_owned()));
        }
        peer_beat(inner).await;
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    (acked, refused)
}

/// How many times `LEAF`'s engine closed on `state` within `window`: each a planned move's try, which
/// closes the engine before its commit (an engine reopens on the next append or pass).
async fn closes_of(state: Arc<crate::http::AppState>, window: Duration) -> usize {
    let (t0, mut closes, mut was_open) = (Instant::now(), 0, false);
    while t0.elapsed() < window {
        let open = state.shards.is_open(LEAF);
        closes += usize::from(was_open && !open);
        was_open = open;
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    closes
}

/// **A move past its bound backs off** (Addendum 26 (c)). Every operation of the fleet store takes
/// 1.5 s, so a commit's read and write outlast any hold and every return of `LEAF` to `streams-2`
/// ends `Kept` by the bound. Over 120 s the giver tries it (closes its engine) at most five times,
/// where without the back-off it tries at every pass; and `LEAF` serves between the tries: every
/// append to a stream in it is acked. The cell gives `streams-1` every leaf (`11` and `LEAF` by
/// their entries), so the stream's create lands on a served leaf.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_move_past_its_commits_bound_is_not_tried_again_at_every_pass() {
    let inner = mem();
    seeded(&inner, &["11", LEAF]).await;
    let slow = FaultPlan {
        latency_pct: 100,
        latency_ms: (1_500, 1_500),
        ..FaultPlan::CLEAN
    };
    let fleet: Arc<dyn ObjectStore> = FaultStore::uniform(inner.clone(), 0xB0F, slow);
    let rig = giver(fleet).await;
    // At 1.5 s an operation the first view takes several passes: the create waits for it.
    let t0 = Instant::now();
    while !rig
        .state
        .ownership
        .ring_active()
        .iter()
        .any(|m| m == "streams-2")
    {
        assert!(
            t0.elapsed() < Duration::from_secs(90),
            "the view rings streams-2"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let mut taken = 0;
    let stream = stream_in(&rig.state, rig.addr, LEAF, &mut taken).await;
    let window = Duration::from_secs(120);
    let appends = appended(rig.addr, &stream, &inner, window);
    let (tries, (acked, refused)) =
        futures_util::future::join(closes_of(rig.state.clone(), window), appends).await;
    eprintln!(
        "HANDOFF_BOUND tries={tries} acked={acked} refused={}",
        refused.len()
    );
    assert!(
        (1..=5).contains(&tries),
        "the giver's tries of {LEAF} in 120 s: {tries}"
    );
    assert_eq!(
        (acked >= 50, refused),
        (true, Vec::new()),
        "{LEAF} serves between the tries: {acked} acked"
    );
    stop(rig).await;
}

/// **A won CAS is the move** (Addendum 26 (a)). The giver moves `00` to `streams-2`; its CAS is
/// parked, then released while the directory's revalidation lock is held for 3 s, past the hold's
/// end, so the read of the view after the write cannot finish within the commit's deadline. The
/// move is `Moved`, and the giver's mirror names `streams-2`. Red with the read after the write
/// deciding the end inside a fixed 400 ms: `Kept`, the mirror unchanged.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_won_cas_is_the_move_when_the_read_after_it_misses_the_deadline() {
    let inner = mem();
    seeded(&inner, &[]).await;
    let fleet = FaultStore::new(inner.clone(), 0xCA5, FaultProfile::clean());
    let rig = giver(fleet.clone()).await;
    let leaf = "00";
    let t0 = Instant::now();
    while !rig
        .state
        .ownership
        .ring_active()
        .iter()
        .any(|m| m == "streams-2")
    {
        assert!(
            t0.elapsed() < Duration::from_secs(20),
            "the view rings streams-2"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let opened = rig
        .state
        .shards
        .open_or_wait(leaf, Duration::from_secs(5))
        .await;
    assert!(
        matches!(opened, crate::sharddir::OpenOutcome::Ready(_)),
        "00 opens on its ring home"
    );
    let engaged = fleet.hold_class_under(StoreOp::Put, ObjClass::Fleet, "fleet/overrides", 1);
    let change = Change::To("streams-2".to_string());
    let moved = hand_off(&rig.state.fleet, leaf, change, false);
    let held_past_the_hold = async {
        let t0 = Instant::now();
        while engaged.load(Ordering::SeqCst) == 0 {
            assert!(
                t0.elapsed() < Duration::from_secs(10),
                "the move's CAS is parked"
            );
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        let lock = rig.state.shards.revalidation().lock().await;
        fleet.release_hold();
        tokio::time::sleep(Duration::from_secs(3)).await;
        drop(lock);
    };
    let (outcome, ()) = futures_util::future::join(moved, held_past_the_hold).await;
    let mirror = rig.state.ownership.effective_owner(leaf);
    assert_eq!(
        (outcome, mirror.as_deref()),
        (Outcome::Moved, Some("streams-2")),
        "(the move's end, the giver's mirror of 00)"
    );
    stop(rig).await;
}

/// **A move commits on a 600 ms store** (Addendum 26 (a)). Every operation of the fleet store
/// takes 600 ms, so the commit's read and write take 1.2 s: within the hold's remainder less
/// 200 ms (about 1.8 s), past the fixed 400 ms it had. The giver's move of `00` to `streams-2` is
/// `Moved` and its mirror names `streams-2`. Red with the fixed 400 ms: `Kept`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_move_commits_within_its_hold_on_a_600_ms_store() {
    let inner = mem();
    seeded(&inner, &[]).await;
    let slow = FaultPlan {
        latency_pct: 100,
        latency_ms: (600, 600),
        ..FaultPlan::CLEAN
    };
    let rig = giver(FaultStore::uniform(inner.clone(), 0x600, slow)).await;
    let leaf = "00";
    let t0 = Instant::now();
    while !rig
        .state
        .ownership
        .ring_active()
        .iter()
        .any(|m| m == "streams-2")
    {
        assert!(
            t0.elapsed() < Duration::from_secs(30),
            "the view rings streams-2"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let opened = rig
        .state
        .shards
        .open_or_wait(leaf, Duration::from_secs(5))
        .await;
    assert!(
        matches!(opened, crate::sharddir::OpenOutcome::Ready(_)),
        "00 opens on its ring home"
    );
    let change = Change::To("streams-2".to_string());
    let outcome = hand_off(&rig.state.fleet, leaf, change, false).await;
    let mirror = rig.state.ownership.effective_owner(leaf);
    assert_eq!(
        (outcome, mirror.as_deref()),
        (Outcome::Moved, Some("streams-2")),
        "(the move's end, the giver's mirror of 00)"
    );
    stop(rig).await;
}
