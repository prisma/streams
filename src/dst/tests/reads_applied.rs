//! Reads applied.

use super::fixture_http::{engine_shutdown, http_rig, http_rig_at};
use super::fixture_requests::{PRISMA_KEY, hreq, preq};
use super::fixture_runtime::RigRuntime;
use super::fixture_storage::{mem, open_engine, skey};
use crate::dst::{FaultPlan, FaultStore, Outcome, Workload};
use object_store::ObjectStore;
use std::sync::Arc;

// ---- deliver=applied: pre-durability subscribe mode -------------------

/// Copy every object from one store to another — a point-in-time
/// "disk image" for crash-shaped restarts. Callers quiesce the source
/// first (a live copy could tear a manifest against its SSTs).
async fn copy_store(src: &Arc<dyn ObjectStore>, dst: &Arc<dyn ObjectStore>) {
    use futures_util::TryStreamExt;
    // `dyn ObjectStore` has only the *_opts core methods (the get/put
    // conveniences live on ObjectStoreExt, which needs Sized).
    let mut listing = src.as_ref().list(None);
    while let Some(meta) = listing.try_next().await.expect("list") {
        let data = src
            .as_ref()
            .get_opts(&meta.location, object_store::GetOptions::default())
            .await
            .expect("get")
            .bytes()
            .await
            .expect("bytes");
        dst.as_ref()
            .put_opts(
                &meta.location,
                object_store::PutPayload::from(data),
                object_store::PutOptions::default(),
            )
            .await
            .expect("put");
    }
}

/// **deliver=applied serves the live tail before durability; the pinned
/// durable surfaces stay blind to it.** Dispatch is held (durable state
/// frozen) while appends apply: the applied read returns the suffix
/// with Prisma-Pending-From and a durable-clamped resume cursor, the
/// default read and the raw route serve only durable records, and an
/// applied LONG-POLL parked at the applied tail is woken by the next
/// APPLY — the low-latency property itself — while durability is still
/// frozen. After release everything converges.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_applied_read_and_long_poll_serve_the_tail_before_durability() {
    let store = mem();
    let (state, addr) = http_rig(store).await;
    let (st, _, _) = preq(
        addr,
        "PUT",
        "/v1/streams/lat",
        &[("prisma-encryption-key", PRISMA_KEY)],
        br#"{"format":{"kind":"json"}}"#,
    )
    .await;
    assert_eq!(st, 201);
    let (st, _, _) = preq(
        addr,
        "POST",
        "/v1/streams/lat/records",
        &[("prisma-encryption-key", PRISMA_KEY)],
        br#"{"n":0}"#,
    )
    .await;
    assert!(st == 200 || st == 202, "append r1: {st}");

    state
        .registry
        .invalidate(&state.deployment.raw_adapter_sref("lat"));
    let desc = state
        .registry
        .get(&state.deployment.raw_adapter_sref("lat"))
        .await
        .unwrap()
        .unwrap();
    let seg = desc.resolve_segment("");
    let route = desc.segment_route_by_id(seg.seg_id).unwrap();
    let engine = state.engine_for(&route).await.unwrap();
    let handle = engine.stream_handle(seg.identity).await.unwrap();
    assert_eq!(handle.state.lock().unwrap().durable.next, 1);

    // Freeze durable state; r2 applies but never dispatches.
    let guard = engine.test_hold_dispatch().await;
    let r2 = tokio::spawn(async move {
        preq(
            addr,
            "POST",
            "/v1/streams/lat/records",
            &[("prisma-encryption-key", PRISMA_KEY)],
            br#"{"n":1}"#,
        )
        .await
    });
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    loop {
        let st = handle.state.lock().unwrap();
        if st.applied.next >= 2 {
            assert_eq!(st.durable.next, 1, "dispatch is held");
            break;
        }
        drop(st);
        assert!(std::time::Instant::now() < deadline, "r2 never applied");
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    }

    // Applied read: both records, the second marked provisional, resume
    // cursor clamped to the durable frontier.
    let (st, h, b) = preq(
        addr,
        "GET",
        "/v1/streams/lat/records?deliver=applied",
        &[("prisma-encryption-key", PRISMA_KEY)],
        b"",
    )
    .await;
    assert_eq!(st, 200, "{}", String::from_utf8_lossy(&b));
    let v: serde_json::Value = serde_json::from_slice(&b).unwrap();
    assert_eq!(v.as_array().unwrap().len(), 2, "applied read sees r2: {v}");
    assert_eq!(
        h.get("prisma-pending-from").map(String::as_str),
        Some("1"),
        "r2 is provisional: {h:?}"
    );
    let next_cur = h.get("prisma-next-cursor").expect("next cursor").clone();
    let durable_cur = h
        .get("prisma-durable-cursor")
        .expect("durable cursor")
        .clone();
    assert_ne!(next_cur, durable_cur, "session cursor is past durable");

    // Default (durable) read: r1 only, no applied headers.
    let (st, h, b) = preq(
        addr,
        "GET",
        "/v1/streams/lat/records",
        &[("prisma-encryption-key", PRISMA_KEY)],
        b"",
    )
    .await;
    assert_eq!(st, 200);
    let v: serde_json::Value = serde_json::from_slice(&b).unwrap();
    assert_eq!(
        v.as_array().unwrap().len(),
        1,
        "durable read is blind to r2"
    );
    assert!(h.get("prisma-pending-from").is_none());
    assert!(h.get("prisma-durable-cursor").is_none());

    // Raw route: `deliver` does not exist on the pinned surface (serde
    // skips it) — the same query is silently durable.
    let (st, _, b) = hreq(addr, "GET", "/v1/stream/lat?deliver=applied", &[], b"").await;
    assert_eq!(st, 200);
    let v: serde_json::Value = serde_json::from_slice(&b).unwrap();
    assert_eq!(
        v.as_array().unwrap().len(),
        1,
        "raw surface stays durable: {v}"
    );

    // Low latency: an applied long-poll parked at the applied tail is
    // woken by r3's APPLY while durability is still frozen.
    let poll = {
        let next_cur = next_cur.clone();
        tokio::spawn(async move {
            preq(
                addr,
                "GET",
                &format!(
                    "/v1/streams/lat/records:long-poll?cursor={next_cur}&deliver=applied&waitMs=8000"
                ),
                &[("prisma-encryption-key", PRISMA_KEY)],
                b"",
            )
            .await
        })
    };
    tokio::time::sleep(std::time::Duration::from_millis(150)).await;
    let r3 = tokio::spawn(async move {
        preq(
            addr,
            "POST",
            "/v1/streams/lat/records",
            &[("prisma-encryption-key", PRISMA_KEY)],
            br#"{"n":2}"#,
        )
        .await
    });
    let (st, h, b) = tokio::time::timeout(std::time::Duration::from_secs(5), poll)
        .await
        .expect("applied long-poll must wake on apply, not durability")
        .expect("join");
    assert_eq!(st, 200, "{}", String::from_utf8_lossy(&b));
    let v: serde_json::Value = serde_json::from_slice(&b).unwrap();
    assert_eq!(v.as_array().unwrap().len(), 1, "woken with r3: {v}");
    assert_eq!(
        h.get("prisma-pending-from").map(String::as_str),
        Some("0"),
        "r3 is provisional at delivery: {h:?}"
    );
    assert_eq!(
        handle.state.lock().unwrap().durable.next,
        1,
        "durability still frozen when the applied poll returned"
    );

    // Release: acks flow, everything converges.
    drop(guard);
    let (st, _, _) = r2.await.unwrap();
    assert!(st == 200 || st == 202, "r2 acked after release: {st}");
    let (st, _, _) = r3.await.unwrap();
    assert!(st == 200 || st == 202, "r3 acked after release: {st}");
    let (st, _, b) = preq(
        addr,
        "GET",
        "/v1/streams/lat/records",
        &[("prisma-encryption-key", PRISMA_KEY)],
        b"",
    )
    .await;
    assert_eq!(st, 200);
    let v: serde_json::Value = serde_json::from_slice(&b).unwrap();
    assert_eq!(v.as_array().unwrap().len(), 3, "all durable after release");
    // The durable cursor minted mid-hold resumes cleanly in durable mode.
    let (st, _, b) = preq(
        addr,
        "GET",
        &format!("/v1/streams/lat/records?cursor={durable_cur}"),
        &[("prisma-encryption-key", PRISMA_KEY)],
        b"",
    )
    .await;
    assert_eq!(st, 200);
    let v: serde_json::Value = serde_json::from_slice(&b).unwrap();
    assert_eq!(
        v.as_array().unwrap().len(),
        2,
        "durable-cursor resume serves the suffix: {v}"
    );
    // Applied read at the converged tail: nothing pending.
    let (st, h, _) = preq(
        addr,
        "GET",
        "/v1/streams/lat/records?deliver=applied",
        &[("prisma-encryption-key", PRISMA_KEY)],
        b"",
    )
    .await;
    assert_eq!(st, 200);
    assert!(
        h.get("prisma-pending-from").is_none(),
        "nothing provisional once durable caught up: {h:?}"
    );
    engine_shutdown(&state).await;
}

/// **The applied suffix is genuinely volatile: a crash before the WAL
/// flush loses it, and recovery rewinds to the durable frontier.** An
/// engine with no group-commit pump and a 10-minute failsafe flush
/// applies a record that never becomes durable; Applied reads serve it,
/// Durable reads never do, and after an unclean reopen (fencing
/// takeover) BOTH watermarks are back at the durable frontier — the
/// exact loss window the mode's contract documents.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_lost_applied_suffix_rewinds_to_the_durable_frontier() {
    let store = mem();
    let key = skey();
    let hash = [61u8; 16];
    let prefix = "dst-applied-lost";

    // Epoch 1: one durable record, clean close.
    let e1 = open_engine(store.clone(), prefix).await;
    let cov = FaultStore::uniform(mem(), 1, FaultPlan::new(0, 0, 0)).coverage();
    let w = Workload::new(cov);
    let o = w
        .attempt_with_deadline(&e1, hash, &key, "", "one", None, None)
        .await;
    assert!(matches!(o, Outcome::Acked { .. }), "r1: {o:?}");
    e1.begin_close();
    e1.await_terminated(std::time::Duration::from_secs(30))
        .await
        .expect("clean close");

    // Epoch 2: no pump, 10-minute failsafe — an applied write CANNOT
    // become durable within the test's lifetime.
    let db = slatedb::Db::builder(prefix, store.clone())
        .with_settings(slatedb::config::Settings {
            flush_interval: Some(std::time::Duration::from_secs(600)),
            manifest_poll_interval: std::time::Duration::from_millis(50),
            ..Default::default()
        })
        .build()
        .await
        .expect("open db");
    let (absorb_tx, _absorb_rx) = crate::history::absorber_channel();
    // R25-A: tests use the REAL load path — a fresh DB rebuilds to
    // zero; a reopened DB restores its durable backlog, exactly as
    // the production opener does.
    let __maint = crate::shard::load_or_rebuild_maintenance(&db)
        .await
        .expect("load maintenance");
    let e2 = crate::shard::ShardEngine::start(
        prefix.to_string(),
        Arc::new(db),
        store.clone(),
        crate::shard::ShardConfig {
            wal_group_commit: false,
            ..Default::default()
        },
        absorb_tx,
        None,
        __maint,
    );
    // The engine's flush-ticker (5 s cadence, first tick immediate)
    // flushes the memtable whenever appends accumulated — WAL included.
    // Let the immediate tick pass while nothing is appended; everything
    // below then has a ~5 s guaranteed non-durability window, orders of
    // magnitude beyond the milliseconds it needs.
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    let e2c = e2.clone();
    let k2 = key.clone();
    let _hung = tokio::spawn(async move {
        // Applies immediately; the ack would need a flush that will not
        // come inside the window.
        let cov = FaultStore::uniform(mem(), 2, FaultPlan::new(0, 0, 0)).coverage();
        Workload::new(cov)
            .attempt_with_deadline(&e2c, hash, &k2, "", "two", None, None)
            .await
    });
    let handle = e2.stream_handle(hash).await.unwrap();
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(3);
    loop {
        let st = handle.state.lock().unwrap();
        if st.applied.next >= 2 {
            assert_eq!(st.durable.next, 1, "no flush inside the window");
            break;
        }
        drop(st);
        assert!(std::time::Instant::now() < deadline, "r2 never applied");
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    }
    let durable = crate::shard::read_frames(
        &e2,
        &handle,
        0,
        None,
        1 << 20,
        crate::shard::Deliver::Durable,
    )
    .await
    .expect("durable read");
    assert_eq!(
        durable.frames.len(),
        1,
        "durable reads never see the suffix"
    );
    let applied = crate::shard::read_frames(
        &e2,
        &handle,
        0,
        None,
        1 << 20,
        crate::shard::Deliver::Applied,
    )
    .await
    .expect("applied read");
    assert_eq!(applied.frames.len(), 2, "applied reads serve the suffix");

    // Crash: no close, no flush. The reopen fences e2; its memtable —
    // and r2 with it — is gone.
    let e3 = open_engine(store.clone(), prefix).await;
    let h3 = e3.stream_handle(hash).await.unwrap();
    {
        let st = h3.state.lock().unwrap();
        assert_eq!(st.durable.next, 1, "recovery is the durable frontier");
        assert_eq!(st.applied.next, 1, "applied rewinds with it");
    }
    let after =
        crate::shard::read_frames(&e3, &h3, 0, None, 1 << 20, crate::shard::Deliver::Applied)
            .await
            .expect("post-crash applied read");
    assert_eq!(after.frames.len(), 1, "the provisional record is lost");
    e3.begin_close();
    e3.await_terminated(std::time::Duration::from_secs(30))
        .await
        .expect("terminate e3");
    e2.begin_close();
    let _ = e2.await_terminated(std::time::Duration::from_secs(5)).await;
}

/// **A stale applied cursor is refused, never silently skipped past.**
/// A session cursor minted beyond the durable frontier survives a
/// crash the suffix does not. Presenting it to the restarted server in
/// applied mode is a 409 `cursor_beyond_tail`; the durable cursor from
/// the same response resumes cleanly; and durable mode's handling of
/// the same token is unchanged (guard is applied-only).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_stale_applied_cursor_is_refused_after_crash_restart() {
    let store = mem();
    // Phase 1: create + one durable record, quiesce, snapshot the store
    // — the crash-consistent image predates r2 entirely.
    let (state1, addr1) = http_rig(store.clone()).await;
    let (st, _, _) = preq(
        addr1,
        "PUT",
        "/v1/streams/sc",
        &[("prisma-encryption-key", PRISMA_KEY)],
        br#"{"format":{"kind":"json"}}"#,
    )
    .await;
    assert_eq!(st, 201);
    let (st, _, _) = preq(
        addr1,
        "POST",
        "/v1/streams/sc/records",
        &[("prisma-encryption-key", PRISMA_KEY)],
        br#"{"n":0}"#,
    )
    .await;
    assert!(st == 200 || st == 202);
    engine_shutdown(&state1).await;
    let snapshot = mem();
    copy_store(&store, &snapshot).await;

    // Phase 2: reopen the live store, hold dispatch, apply r2, mint the
    // applied-session cursor (position 2 — past the snapshot's tail).
    let (state2, addr2) = http_rig_at(store.clone(), RigRuntime::incarnation(1)).await;
    state2
        .registry
        .invalidate(&state2.deployment.raw_adapter_sref("sc"));
    let desc = state2
        .registry
        .get(&state2.deployment.raw_adapter_sref("sc"))
        .await
        .unwrap()
        .unwrap();
    let seg = desc.resolve_segment("");
    let route = desc.segment_route_by_id(seg.seg_id).unwrap();
    let engine = state2.engine_for(&route).await.unwrap();
    let handle = engine.stream_handle(seg.identity).await.unwrap();
    let guard = engine.test_hold_dispatch().await;
    let r2 = tokio::spawn(async move {
        preq(
            addr2,
            "POST",
            "/v1/streams/sc/records",
            &[("prisma-encryption-key", PRISMA_KEY)],
            br#"{"n":1}"#,
        )
        .await
    });
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    loop {
        if handle.state.lock().unwrap().applied.next >= 2 {
            break;
        }
        assert!(std::time::Instant::now() < deadline, "r2 never applied");
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    }
    let (st, h, _) = preq(
        addr2,
        "GET",
        "/v1/streams/sc/records?deliver=applied",
        &[("prisma-encryption-key", PRISMA_KEY)],
        b"",
    )
    .await;
    assert_eq!(st, 200);
    let next_cur = h.get("prisma-next-cursor").expect("next").clone();
    let durable_cur = h.get("prisma-durable-cursor").expect("durable").clone();
    drop(guard);
    let _ = r2.await.unwrap();
    engine_shutdown(&state2).await;

    // Phase 3: the "restarted server" — the snapshot without r2.
    let (state3, addr3) = http_rig_at(snapshot, RigRuntime::incarnation(2)).await;
    let (st, _, b) = preq(
        addr3,
        "GET",
        &format!("/v1/streams/sc/records?cursor={next_cur}&deliver=applied"),
        &[("prisma-encryption-key", PRISMA_KEY)],
        b"",
    )
    .await;
    assert_eq!(st, 409, "{}", String::from_utf8_lossy(&b));
    let v: serde_json::Value = serde_json::from_slice(&b).unwrap();
    assert_eq!(v["error"]["code"], "cursor_beyond_tail");
    // The durable cursor resumes cleanly (empty page at the tail).
    let (st, _, b) = preq(
        addr3,
        "GET",
        &format!("/v1/streams/sc/records?cursor={durable_cur}&deliver=applied"),
        &[("prisma-encryption-key", PRISMA_KEY)],
        b"",
    )
    .await;
    assert_eq!(st, 200, "{}", String::from_utf8_lossy(&b));
    // Durable mode's handling of the same stale token is UNCHANGED by
    // this feature (no guard): an empty page, never a 409.
    let (st, _, b) = preq(
        addr3,
        "GET",
        &format!("/v1/streams/sc/records?cursor={next_cur}"),
        &[("prisma-encryption-key", PRISMA_KEY)],
        b"",
    )
    .await;
    assert_eq!(st, 200, "{}", String::from_utf8_lossy(&b));
    engine_shutdown(&state3).await;
}
