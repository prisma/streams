//! Runtime isolation.

use super::fixture_http::{HttpRigOptions, http_rig_at, http_rig_build};
use super::fixture_livefeed::{hub_sse_collect, sse_head};
use super::fixture_requests::RIG_KEY_B64;
use super::fixture_requests::{PRISMA_KEY, hreq, preq};
use super::fixture_runtime::{RIG_START_MS, RigRuntime};
use super::fixture_storage::{mem, open_engine, open_engine_with_absorber, skey};
use crate::dst::{FaultPlan, FaultProfile, FaultStore, ObjClass, OpLog, Workload};
use object_store::{GetOptions, ObjectStore, PutOptions, PutPayload};
use std::sync::Arc;

/// A fenced owner must not leave a **zombie absorber** behind.
///
/// This is not hypothetical: a fenced shard's absorber that keeps retrying
/// against a dead DB evicts the rightful owner's history handle in a
/// ping-pong — "the absorption war" (2026-07-20), which `history.rs`
/// guards against explicitly. Asserting that the old engine *reports*
/// itself closed is not the same as asserting its tasks actually exited,
/// and only the second one catches a leak.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_fenced_owners_absorber_exits() {
    let inner = mem();
    let store = FaultStore::uniform(inner.clone(), 23, FaultPlan::CLEAN);
    let cov = store.coverage();
    let key = skey();
    let hash = [11u8; 16];
    let prefix = "dst-zombie";

    let (a, absorber_a) = open_engine_with_absorber(store.clone(), prefix, hash, &key).await;
    let mut log = OpLog::default();
    let mut w = Workload::new(cov.clone());
    w.run(&a, hash, &key, &["z"], 10, false, &mut log).await;
    assert!(log.total_acked() > 0, "nothing acked before the handoff");
    assert!(
        !absorber_a.is_finished(),
        "the absorber exited before the shard was even fenced"
    );

    // The handoff: a new owner opens the same shard log.
    let (b, absorber_b) = open_engine_with_absorber(store.clone(), prefix, hash, &key).await;

    // Give the old engine a reason to notice: its next commit attempt is
    // what discovers the fence.
    let mut ghost = OpLog::default();
    let mut gw = Workload::new(cov.clone());
    gw.max_attempts = 1;
    gw.run(&a, hash, &key, &["z"], 3, false, &mut ghost).await;
    assert_eq!(
        ghost.total_acked(),
        0,
        "I4 violated: the fenced owner acknowledged writes"
    );

    // EVERY task the old engine owns must terminate — not just the
    // absorber, and not merely "is_finished" (which is also true after a
    // panic). await_terminated joins each handle and names stragglers.
    //
    // The committer is the one that used to be unable to exit at all: it
    // held the engine, the engine held its channel sender, so the channel
    // could never close. One resident committer + engine allocation per
    // shard move, forever.
    match a.await_terminated(std::time::Duration::from_secs(30)).await {
        Ok(()) => {}
        Err(e) => panic!("fenced owner left tasks behind: {e}"),
    }

    // The absorber is a separately-owned task; join it explicitly too.
    let mut exited = false;
    for _ in 0..400 {
        if absorber_a.is_finished() {
            exited = true;
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    }
    assert!(
        exited,
        "the fenced owner's absorber is still running — a zombie that will \
         fight the new owner for its history DB"
    );
    absorber_a
        .await
        .expect("absorber must exit cleanly, not panic");

    absorber_b.abort();
    let _ = b;
}

/// Queued-but-uncommitted appends must be answered when the shard closes,
/// not left to hang until each client's own timeout. `begin_close` drains
/// what is in flight; the committer drains what is still queued behind it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn closing_an_engine_answers_queued_appends_and_ends_every_task() {
    let inner = mem();
    // Slow WAL writes so requests pile up behind the in-flight commit.
    let slow = FaultPlan {
        error_pct: 0,
        lost_response_pct: 0,
        latency_pct: 100,
        latency_ms: (400, 800),
    };
    let store = FaultStore::new(
        inner.clone(),
        73,
        FaultProfile::uniform(FaultPlan::CLEAN).with_class(ObjClass::Wal, slow),
    );
    let cov = store.coverage();
    let key = skey();
    let hash = [26u8; 16];
    let engine = open_engine(store.clone(), "dst-drain").await;

    // Fire a burst without awaiting; they queue behind the slow commit.
    let mut waiters = Vec::new();
    for i in 0..16u64 {
        let e = engine.clone();
        let k = key.clone();
        let c = cov.clone();
        waiters.push(tokio::spawn(async move {
            let w = Workload::new(c);
            w.attempt_with_deadline(&e, hash, &k, "q", &format!("drain{i}"), None, None)
                .await
        }));
    }
    tokio::time::sleep(std::time::Duration::from_millis(150)).await;

    engine.begin_close();

    // Every caller must get an answer — acked (it made it) or Unknown
    // (fenced/moved) — and none may hang.
    let mut answered = 0;
    for w in waiters {
        match tokio::time::timeout(std::time::Duration::from_secs(20), w).await {
            Ok(Ok(_outcome)) => answered += 1,
            Ok(Err(e)) => panic!("waiter task panicked: {e}"),
            Err(_) => panic!("a queued append never received a response after close"),
        }
    }
    assert_eq!(answered, 16, "every queued append must be answered");

    engine
        .await_terminated(std::time::Duration::from_secs(30))
        .await
        .expect("all engine tasks must terminate after close");
}

/// PR 4.1.1 proof: the fixture's clock IS the runtime's clock — a test
/// drives deterministic time through the returned handle, in both
/// domains.
#[tokio::test]
async fn rig_clock_is_the_runtime_clock() {
    let rig = http_rig_build(mem(), RigRuntime::first(), HttpRigOptions::default()).await;
    assert_eq!(rig.state.runtime.clock.now().ms(), RIG_START_MS);
    let t0 = rig.state.runtime.clock.monotonic();
    rig.clock.advance(std::time::Duration::from_secs(90));
    assert_eq!(rig.state.runtime.clock.now().ms(), RIG_START_MS + 90_000);
    assert_eq!(
        rig.state.runtime.clock.monotonic().since(t0),
        std::time::Duration::from_secs(90)
    );
    rig.clock.jump_wall(-3_600_000);
    assert_eq!(
        rig.state.runtime.clock.now().ms(),
        RIG_START_MS + 90_000 - 3_600_000
    );
    assert_eq!(
        rig.state.runtime.clock.monotonic().since(t0),
        std::time::Duration::from_secs(90),
        "a wall step never moves the runtime's monotonic domain"
    );
}

/// The identities one rig incarnation exhibits after a fixed request
/// schedule: boot id, the generated stream epoch, the FIRST
/// touch-journal epoch (process-local: the first draw from this
/// incarnation's "touch-journal" stream), and the registry-class
/// store trace (whose paths embed the epoch). The registry class is
/// compared rather than the whole trace because WAL/manifest object
/// counts depend on flush timing, which is not a migrated input yet.
struct RigIdentities {
    boot_id: String,
    stream_epoch: String,
    touch_epoch: String,
    registry_ops: Vec<(crate::dst::StoreOp, String)>,
}

async fn observe_rig_identities(runtime: RigRuntime) -> RigIdentities {
    let trace = crate::dst::trace_store::TraceStore::verbatim(mem());
    let store: Arc<dyn ObjectStore> = trace.clone();
    let (state, addr) = http_rig_at(store, runtime).await;
    let ct = [("content-type", "application/json")];
    let (st, _, _) = hreq(addr, "PUT", "/v1/stream/repro/s", &ct, b"").await;
    assert!(st == 200 || st == 201, "create: {st}");
    let (st, _, _) = hreq(addr, "POST", "/v1/stream/repro/s", &ct, br#"[{"n":1}]"#).await;
    assert!(st == 200 || st == 204, "append: {st}");
    let desc = state
        .registry
        .get(&state.deployment.raw_adapter_sref("repro/s"))
        .await
        .unwrap()
        .expect("created");
    let journal = state.touch.journal(
        desc.storage_hash(),
        crate::crypto::RouteHash::for_stream(&desc.sref()),
    );
    let registry_ops = trace
        .events()
        .into_iter()
        .filter(|e| e.path.starts_with("registry/"))
        .map(|e| (e.op, e.path))
        .collect();
    RigIdentities {
        boot_id: state.runtime.identity.boot_id.clone(),
        stream_epoch: desc.stream_epoch.clone(),
        touch_epoch: journal.epoch.clone(),
        registry_ops,
    }
}

/// PR 4.1.1 proof (a): the same base seed and the same incarnation,
/// on separate stores under the same schedule, reproduce every
/// migrated identity — boot id, stream epoch, touch-journal epoch —
/// and the registry-class store trace.
#[tokio::test]
async fn same_incarnation_reproduces_identities_epochs_and_registry_trace() {
    let a = observe_rig_identities(RigRuntime::incarnation(0)).await;
    let b = observe_rig_identities(RigRuntime::incarnation(0)).await;
    assert_eq!(a.boot_id, b.boot_id, "boot id reproduces");
    assert_eq!(a.stream_epoch, b.stream_epoch, "stream epoch reproduces");
    assert_eq!(
        a.touch_epoch, b.touch_epoch,
        "touch-journal epoch reproduces"
    );
    assert!(
        !a.registry_ops.is_empty(),
        "the registry trace must not be vacuous"
    );
    assert_eq!(
        a.registry_ops, b.registry_ops,
        "registry-class store trace reproduces"
    );
}

/// PR 4.1.1 proof (b): the same base seed under DIFFERENT incarnations
/// yields distinct process identities — and the values are PINNED, so
/// a change to the seed derivation or the draw order shows up here
/// instead of silently re-keying every restart test.
#[tokio::test]
async fn distinct_incarnations_have_distinct_pinned_identities() {
    let a = observe_rig_identities(RigRuntime::incarnation(0)).await;
    let b = observe_rig_identities(RigRuntime::incarnation(1)).await;
    let b_again = observe_rig_identities(RigRuntime::incarnation(1)).await;
    assert_ne!(a.boot_id, b.boot_id, "distinct boot ids");
    assert_ne!(
        a.stream_epoch, b.stream_epoch,
        "distinct first stream epochs"
    );
    assert_ne!(
        a.touch_epoch, b.touch_epoch,
        "distinct first touch-journal epochs"
    );
    assert_eq!(
        b.boot_id, b_again.boot_id,
        "an incarnation reproduces itself"
    );
    assert_eq!(b.touch_epoch, b_again.touch_epoch);
    assert_eq!(
        (a.boot_id.as_str(), a.touch_epoch.as_str()),
        ("5faa1cef78f5d4b5cd55004166359f76", "01f56451db1eaf18"),
        "incarnation 0 is pinned"
    );
    assert_eq!(
        (b.boot_id.as_str(), b.touch_epoch.as_str()),
        ("50646a49dd1f31431aee147661543e10", "f83e80b818978a16"),
        "incarnation 1 is pinned"
    );
}

/// PR 6.1-D, completed by 6.1.1-C: fleet coordination state is per
/// RUNTIME, and the LOOP is part of that proof. Two runtimes with
/// different coordination stores run their fleet loops concurrently:
/// each publishes its heartbeat only to its own store, each operator
/// view sees only its own cell, and neither event drainer can read or
/// clear the other's outbox. The process-global slot this replaces was
/// overwritten by whichever runtime started last — one runtime then
/// published the other's events through its own identity and billing
/// pipeline.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_runtimes_never_share_fleet_state() {
    let fleet_a = mem();
    let fleet_b = mem();
    let rig_a = http_rig_build(
        mem(),
        RigRuntime::first(),
        HttpRigOptions {
            fleet_store: Some(fleet_a.clone()),
            instance: Some("inst-a".to_string()),
            ..Default::default()
        },
    )
    .await;
    let rig_b = http_rig_build(
        mem(),
        RigRuntime::incarnation(1),
        HttpRigOptions {
            fleet_store: Some(fleet_b.clone()),
            instance: Some("inst-b".to_string()),
            ..Default::default()
        },
    )
    .await;
    // BOTH fleet loops run — started through the SAME assembly bootstrap
    // uses (PR 6.1.2-B). There is no repository argument to get wrong:
    // each loop takes its own runtime's `state.fleet`, and this proof
    // would stop exercising production wiring if that ever diverged.
    for rig in [&rig_a, &rig_b] {
        let config = rig.state.config.clone();
        assert!(
            crate::fleet::start_configured(rig.state.clone(), &config, &rig.tasks),
            "a rig with a fleet store must start its loop"
        );
    }
    // One tick is 2s; give both loops a heartbeat.
    tokio::time::sleep(std::time::Duration::from_millis(2600)).await;

    // Each loop published ITS heartbeat to ITS store, and neither store
    // ever saw the other instance.
    let a_names = fleet_instance_docs(&fleet_a).await;
    let b_names = fleet_instance_docs(&fleet_b).await;
    assert_eq!(a_names, vec!["fleet/inst-a.json".to_string()], "A's store");
    assert_eq!(b_names, vec!["fleet/inst-b.json".to_string()], "B's store");

    // Each operator view reads its own cell only.
    let (hb_a, _) = rig_a.state.fleet.operator_snapshot().await;
    let (hb_b, _) = rig_b.state.fleet.operator_snapshot().await;
    let inst = |v: Option<Vec<serde_json::Value>>| -> Vec<String> {
        let mut names: Vec<String> = v
            .unwrap_or_default()
            .into_iter()
            .filter_map(|h| h["instance"].as_str().map(str::to_string))
            .collect();
        names.sort();
        names
    };
    assert_eq!(inst(hb_a), vec!["inst-a".to_string()]);
    assert_eq!(inst(hb_b), vec!["inst-b".to_string()]);

    // ONE pending fleet event, written to B's coordination store only.
    let desired = crate::fleet::Desired {
        count: 3,
        reason: "test".into(),
        epoch: 1,
        computed_at_ms: 1,
        pending_events: vec![crate::ops::OpsEvent::new(
            "scale_out",
            "fleet/b/only/1".to_string(),
        )],
    };
    fleet_b
        .put_opts(
            &object_store::path::Path::from("fleet/desired.json"),
            PutPayload::from(serde_json::to_vec(&desired).unwrap()),
            PutOptions::default(),
        )
        .await
        .unwrap();
    // A drains ITS repository: nothing to emit, and B's document is
    // untouched.
    let emitted_a = crate::fleet::drain_fleet_events(&rig_a.state)
        .await
        .unwrap();
    assert_eq!(emitted_a, 0, "A's drainer must never read B's outbox");
    let still = fleet_b
        .get_opts(
            &object_store::path::Path::from("fleet/desired.json"),
            GetOptions::default(),
        )
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    let doc: crate::fleet::Desired = serde_json::from_slice(&still).unwrap();
    assert_eq!(
        doc.pending_events.len(),
        1,
        "A must not clear B's event outbox"
    );

    // A runtime without fleet coordination has no repository at all.
    let plain = http_rig_build(mem(), RigRuntime::incarnation(2), HttpRigOptions::default()).await;
    assert!(rig_a.state.fleet.enabled() && rig_b.state.fleet.enabled());
    assert!(!plain.state.fleet.enabled());
    assert_eq!(
        crate::fleet::drain_fleet_events(&plain.state)
            .await
            .unwrap(),
        0
    );
    let (hb_none, desired_none) = plain.state.fleet.operator_snapshot().await;
    assert!(hb_none.is_none() && desired_none.is_none());
    for rig in [rig_a, rig_b, plain] {
        rig.tasks.shutdown(std::time::Duration::from_secs(3)).await;
    }
}

/// The instance heartbeat documents present in one coordination store.
async fn fleet_instance_docs(store: &Arc<dyn ObjectStore>) -> Vec<String> {
    use futures_util::StreamExt;
    let mut names = Vec::new();
    let mut listing = store.list(Some(&object_store::path::Path::from("fleet")));
    while let Some(meta) = listing.next().await {
        let Ok(meta) = meta else { continue };
        let loc = meta.location.as_ref().to_string();
        if loc.ends_with(".json") && !loc.ends_with("desired.json") {
            names.push(loc);
        }
    }
    names.sort();
    names
}

/// PR 6.1-A: a runtime that has shut down holds no socket — the
/// address is free for a replacement IMMEDIATELY (the accept loop
/// released the listener and joined every connection before the
/// supervisor returned).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shutdown_releases_the_listener_for_immediate_rebinding() {
    let rig = http_rig_build(mem(), RigRuntime::first(), HttpRigOptions::default()).await;
    let addr = rig.addr;
    let (st, _, _) = hreq(addr, "GET", "/health", &[], b"").await;
    assert_eq!(st, 200);
    let report = rig.tasks.shutdown(std::time::Duration::from_secs(2)).await;
    assert!(report.terminated("http"), "{report:?}");
    assert_eq!(rig.tasks.phase(), crate::tasks::Phase::Stopped);
    tokio::net::TcpListener::bind(addr)
        .await
        .expect("the old runtime's address rebinds immediately after shutdown");
}

/// PR 6.1-A: a keep-alive connection is a CHILD of the accept loop —
/// it is closed before shutdown returns, not left parked on a runtime
/// that no longer exists.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shutdown_terminates_a_live_keep_alive_connection() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let rig = http_rig_build(mem(), RigRuntime::first(), HttpRigOptions::default()).await;
    let mut sck = tokio::net::TcpStream::connect(rig.addr).await.unwrap();
    sck.write_all(b"GET /health HTTP/1.1\r\nhost: x\r\ncontent-length: 0\r\n\r\n")
        .await
        .unwrap();
    let (head, text) = sse_head(&mut sck).await;
    assert_eq!(head, 200, "{text}");
    // HTTP/1.1 keeps the connection open and idle after the response.
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    let report = rig.tasks.shutdown(std::time::Duration::from_secs(2)).await;
    assert!(report.terminated("http"), "{report:?}");
    let mut buf = [0u8; 256];
    let mut closed = false;
    for _ in 0..8 {
        match tokio::time::timeout(std::time::Duration::from_secs(1), sck.read(&mut buf)).await {
            Ok(Ok(0)) | Ok(Err(_)) => {
                closed = true;
                break;
            }
            Ok(Ok(_)) => continue, // pending response bytes drain first
            Err(_) => break,
        }
    }
    assert!(
        closed,
        "the keep-alive connection must be closed by shutdown"
    );
}

/// PR 6.1-A: a live subscription — the longest-lived connection a
/// runtime serves — ends when the runtime shuts down.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shutdown_terminates_a_live_sse_subscription() {
    use tokio::io::AsyncWriteExt;
    let rig = http_rig_build(mem(), RigRuntime::first(), HttpRigOptions::default()).await;
    let addr = rig.addr;
    let ct = ("content-type", "application/json");
    let (st, _, b) = hreq(addr, "PUT", "/v1/stream/livesub", &[ct], br#"[{"i":0}]"#).await;
    assert!(
        st == 200 || st == 201,
        "stage: {st} {}",
        String::from_utf8_lossy(&b)
    );
    let mut sck = tokio::net::TcpStream::connect(addr).await.unwrap();
    let req = format!(
        "GET /v1/stream/livesub?live=sse&offset=now HTTP/1.1\r\nhost: x\r\ncontent-length: 0\r\nstream-encryption-key: {RIG_KEY_B64}\r\n\r\n"
    );
    sck.write_all(req.as_bytes()).await.unwrap();
    let (head, text) = sse_head(&mut sck).await;
    assert_eq!(head, 200, "{text}");
    let (body, ended) = hub_sse_collect(&mut sck, 5, |t| t.contains("upToDate")).await;
    assert!(
        !ended && body.contains("upToDate"),
        "a parked live subscription:\n{body}"
    );
    let report = rig.tasks.shutdown(std::time::Duration::from_secs(2)).await;
    assert!(report.terminated("http"), "{report:?}");
    let (_, ended) = hub_sse_collect(&mut sck, 3, |_| false).await;
    assert!(ended, "the live subscription must end with the runtime");
}

/// PR 4.1.1 proof (c): a touch cursor is PROCESS-LOCAL — its epoch
/// names the journal incarnation that issued it. A restarted server (a
/// new rig incarnation over the same persisted store) recreates the
/// in-memory journal under a new epoch, so the old cursor gets the
/// existing stale answer: an explicit RESYNC, never a silent false.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn restart_invalidates_process_local_touch_cursors() {
    let store = mem();
    let rig_a = http_rig_build(
        store.clone(),
        RigRuntime::first(),
        HttpRigOptions::default(),
    )
    .await;
    let addr_a = rig_a.addr;
    let key = [("prisma-encryption-key", PRISMA_KEY)];
    let (st, _, b) = preq(
        addr_a,
        "PUT",
        "/v1/streams/wres",
        &key,
        br#"{"format":{"kind":"json"},"watches":[{"name":"by-customer","fields":["/customerId"]}]}"#,
    )
    .await;
    assert_eq!(st, 201, "{}", String::from_utf8_lossy(&b));
    let khex = crate::product::watch_key_hex(
        "by-customer",
        &["/customerId".to_string()],
        &["\"c1\"".to_string()],
    );
    // A matching append plants the journal through the real writer path.
    let (st, _, _) = preq(
        addr_a,
        "POST",
        "/v1/streams/wres/records",
        &[
            ("prisma-encryption-key", PRISMA_KEY),
            ("prisma-routing-key", "c1"),
        ],
        br#"{"customerId":"c1","total":1}"#,
    )
    .await;
    assert_eq!(st, 200);
    let path = |cursor: &str| {
        format!("/v1/streams/wres/watches/by-customer/keys/{khex}?cursor={cursor}&timeoutMs=200")
    };
    let wait = |addr: std::net::SocketAddr, cursor: String| async move {
        let (st, _, b) = preq(addr, "GET", &path(&cursor), &key, b"").await;
        assert_eq!(st, 200, "{}", String::from_utf8_lossy(&b));
        serde_json::from_slice::<serde_json::Value>(&b).unwrap()
    };
    // The append's touch is ingested after durability, so a waiter
    // parked at "now" may observe it ("changed") or time out; either
    // answer carries a cursor of THIS journal incarnation.
    let v = wait(addr_a, "now".to_string()).await;
    assert_ne!(v["reason"], "resync", "{v}");
    let cursor = v["cursor"].as_str().unwrap().to_string();
    let epoch_of = |c: &str| c.split(':').next().unwrap().to_string();
    assert!(
        cursor.contains(':'),
        "a journal cursor is <epoch>:<generation>: {cursor}"
    );
    // Valid where it was issued: waiting at the cursor's own generation
    // times out rather than resyncing.
    let v = wait(addr_a, cursor.clone()).await;
    assert_eq!(v["invalidated"], false, "{v}");

    // Restart: the old server surface and its supervised runtime loops
    // are terminated through the supervisor (PR 6-F), then a NEW
    // incarnation starts over the same store.
    let report = rig_a
        .tasks
        .shutdown(std::time::Duration::from_secs(2))
        .await;
    assert!(report.terminated("http"), "{report:?}");
    drop(rig_a);
    let (_state_b, addr_b) = http_rig_at(store, RigRuntime::incarnation(1)).await;
    let v = wait(addr_b, cursor.clone()).await;
    assert_eq!(v["invalidated"], true, "{v}");
    assert_eq!(v["reason"], "resync", "{v}");
    assert_ne!(
        epoch_of(v["cursor"].as_str().unwrap()),
        epoch_of(&cursor),
        "the restarted journal carries a NEW epoch"
    );
}

/// PR 4.1.1 proof (d): a stream epoch is PERSISTED resource identity.
/// An ordinary restart (new incarnation, same store) serves the same
/// epoch; hard deletion followed by recreation is a new resource, and
/// under the new incarnation its epoch differs from the old one.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stream_epoch_survives_restart_but_not_recreation() {
    let store = mem();
    let rig_a = http_rig_build(
        store.clone(),
        RigRuntime::first(),
        HttpRigOptions::default(),
    )
    .await;
    let addr_a = rig_a.addr;
    let key = [("prisma-encryption-key", PRISMA_KEY)];
    let body = br#"{"format":{"kind":"json"},"watches":[{"name":"by-customer","fields":["/customerId"]}]}"#;
    let epoch_of = |b: &[u8]| {
        serde_json::from_slice::<serde_json::Value>(b).unwrap()["epoch"]
            .as_str()
            .expect("epoch exposed for watches")
            .to_string()
    };
    let (st, _, b) = preq(addr_a, "PUT", "/v1/streams/inc", &key, body).await;
    assert_eq!(st, 201, "{}", String::from_utf8_lossy(&b));
    let (st, _, b) = preq(addr_a, "GET", "/v1/streams/inc", &[], b"").await;
    assert_eq!(st, 200);
    let e1 = epoch_of(&b);

    // Restart: terminate the old server surface and its supervised
    // loops, then the persisted epoch is what the new incarnation
    // serves.
    let report = rig_a
        .tasks
        .shutdown(std::time::Duration::from_secs(2))
        .await;
    assert!(report.terminated("http"), "{report:?}");
    drop(rig_a);
    let (_state_b, addr_b) = http_rig_at(store, RigRuntime::incarnation(1)).await;
    let (st, _, b) = preq(addr_b, "GET", "/v1/streams/inc", &[], b"").await;
    assert_eq!(st, 200);
    assert_eq!(
        epoch_of(&b),
        e1,
        "an ordinary restart preserves the descriptor's persisted stream epoch"
    );

    // Hard delete + recreate under the new incarnation: a new resource.
    let (st, _, b) = preq(addr_b, "DELETE", "/v1/streams/inc", &key, b"").await;
    assert!(
        st == 200 || st == 202 || st == 204,
        "delete: {st} {}",
        String::from_utf8_lossy(&b)
    );
    let (st, _, b) = preq(addr_b, "PUT", "/v1/streams/inc", &key, body).await;
    assert_eq!(st, 201, "{}", String::from_utf8_lossy(&b));
    let (st, _, b) = preq(addr_b, "GET", "/v1/streams/inc", &[], b"").await;
    assert_eq!(st, 200);
    assert_ne!(epoch_of(&b), e1, "recreation mints a new stream epoch");
}
