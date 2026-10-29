//! Billing maintenance.

use super::fixture_http::{engine_shutdown, http_rig};
use super::fixture_requests::{PRISMA_KEY, hreq, preq};
use super::fixture_storage::{mem, open_engine, skey};
use crate::dst::{FaultPlan, FaultStore, Outcome, Workload};
use std::sync::Arc;

/// Round-21 blocker 6: EXPIRY closes storage without any delete call.
/// A stream whose TTL lapsed has a dead descriptor and a live gauge;
/// the drain-time reconciler resubmits the closure until the gauge
/// zeroes, and journals the lifecycle observation.
#[expect(
    clippy::let_underscore_must_use,
    reason = "expiry_closes_the_storage_gauge; the fixture drains billing debt until the gauge closes, and each pass's own result is irrelevant to the closure it polls for; a handled result would only restate the poll"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn expiry_closes_the_storage_gauge() {
    let store = mem();
    let (state, addr) = http_rig(store).await;
    let key = [("prisma-encryption-key", PRISMA_KEY)];
    // TTL 1 second.
    let (st, _, _) = preq(
        addr,
        "PUT",
        "/v1/streams/exp1",
        &key,
        br#"{"format":{"kind":"json"},"expiry":{"idle":"PT1S"}}"#,
    )
    .await;
    if st != 201 {
        // Product creation may not accept this expiry shape; fall back
        // to the raw surface's ttl header.
        let (st2, _, _) = hreq(
            addr,
            "PUT",
            "/v1/stream/exp1",
            &[
                ("stream-encryption-key", PRISMA_KEY),
                ("content-type", "application/json"),
                ("stream-ttl", "1"),
            ],
            b"",
        )
        .await;
        assert_eq!(st2, 201, "raw ttl create");
    }
    let (st, _, _) = preq(
        addr,
        "POST",
        "/v1/streams/exp1/records",
        &[
            ("prisma-encryption-key", PRISMA_KEY),
            ("prisma-routing-key", "k"),
        ],
        br#"{"n":1}"#,
    )
    .await;
    assert_eq!(st, 200);
    let desc = state
        .registry
        .get(&state.deployment.raw_adapter_sref("exp1"))
        .await
        .unwrap()
        .unwrap();
    let seg = desc.resolve_segment("k");
    let engine = state.engine_for(&seg.shard_route).await.unwrap();
    let gauge_before = engine
        .billing_meta(seg.identity)
        .await
        .unwrap()
        .owned_frame_bytes_current;
    assert!(gauge_before > 0);

    // Let the TTL lapse, then run drains: the reconciler must close.
    tokio::time::sleep(std::time::Duration::from_millis(1200)).await;
    state
        .registry
        .invalidate(&state.deployment.raw_adapter_sref("exp1"));
    let mut closed = false;
    for _ in 0..100 {
        let _ = crate::billing::drain_once(&state).await;
        if let Some(m) = engine.billing_meta(seg.identity).await
            && m.owned_frame_bytes_current == 0
        {
            closed = true;
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(30)).await;
    }
    assert!(closed, "expiry never closed the storage gauge");
    engine_shutdown(&state).await;
}

/// The stream's tail as its row is stored (the committer's staged view).
async fn stored_tail(
    engine: &crate::shard::ShardEngine,
    hash: &[u8; 16],
) -> crate::shard::TailFields {
    let raw = engine
        .db
        .get(crate::shard::tail_key(hash))
        .await
        .unwrap()
        .expect("tail row exists after acked appends");
    crate::shard::decode_tail_for_tests(&raw).expect("the stored tail decodes")
}

/// The stream's published durable tail (what readers and the seal see).
async fn durable_tail(
    engine: &crate::shard::ShardEngine,
    hash: [u8; 16],
) -> crate::shard::TailFields {
    let handle = engine.stream_handle(hash).await.expect("handle");
    handle.state.lock().unwrap().durable.clone()
}

/// R25-B: the maintenance gauge is EXACT ENCODED FRAME BYTES — the same
/// unit as the tail's `unabsorbed_bytes` — and never the logical payload
/// total. This is the regression that invalidates the 2026-08-11 soak
/// headline: the R24 accounting added uncompressed payload bytes while
/// retiring encoded frame bytes, so with FRAME_COMPRESS=1 and all-`x`
/// records a healthy absorber read as "9.4% absorption" with 3.87 GB of
/// fictional backlog. Whatever the compression setting, encoding means
/// frame bytes != payload bytes, so asserting exact equality with the
/// tail gauge and inequality with the payload total kills the class.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn maintenance_uses_frame_bytes_not_payload_bytes() {
    let store = mem();
    let engine = open_engine(store.clone(), "dst-frameunit").await;
    let key = skey();
    let hash = [7u8; 16];
    let cov = FaultStore::uniform(mem(), 1, FaultPlan::new(0, 0, 0)).coverage();
    let w = Workload::new(cov);

    // Large, highly compressible payloads — the shape the soak generator
    // produced.
    let body = "x".repeat(64 * 1024);
    let mut payload_total = 0u64;
    for _ in 0..4 {
        let out = w
            .attempt_with_deadline(&engine, hash, &key, "k", &body, None, None)
            .await;
        assert!(
            matches!(out, Outcome::Acked { .. }),
            "append must ack: {out:?}"
        );
        payload_total += body.len() as u64;
    }

    // The three views must agree EXACTLY: the stored tail row's gauge, the
    // published durable tail, and the engine's published ledger.
    let stored = stored_tail(&engine, &hash).await;
    assert!(stored.unabsorbed_bytes > 0, "appends must create backlog");
    let durable = durable_tail(&engine, hash).await;
    let snap = engine.maintenance_snapshot();
    assert_eq!(
        durable.unabsorbed_bytes, stored.unabsorbed_bytes,
        "the published tail must equal the stored tail's gauge exactly"
    );
    assert_eq!(
        snap.unabsorbed_frame_bytes, stored.unabsorbed_bytes,
        "the published ledger must equal the tail's gauge exactly"
    );
    assert_ne!(
        snap.unabsorbed_frame_bytes, payload_total,
        "the gauge must NOT be the logical payload total — that unit \
         mismatch manufactured the soak's 9.4% absorption artifact"
    );
    engine.begin_close();
}

/// R25-D: a FAILED append group leaves the stored tail row and the
/// engine's published ledger untouched. This is phantom backlog's
/// mechanism-level test — through the real committer and the real
/// group-failure path, not a mirror unit.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn maintenance_failed_append_group_is_noop() {
    let store = mem();
    let engine = open_engine(store.clone(), "dst-mnoop").await;
    let key = skey();
    let hash = [21u8; 16];
    let cov = FaultStore::uniform(mem(), 1, FaultPlan::new(0, 0, 0)).coverage();
    let w = Workload::new(cov);

    // One committed append establishes a nonzero baseline.
    let out = w
        .attempt_with_deadline(&engine, hash, &key, "k", "baseline", None, None)
        .await;
    assert!(matches!(out, Outcome::Acked { .. }));
    let tail_row = crate::shard::tail_key(&hash);
    let before_row = engine.db.get(&tail_row).await.unwrap().unwrap();
    let before_snap = engine.maintenance_snapshot();
    assert!(before_snap.unabsorbed_frame_bytes > 0);

    // Arm the failpoint; the append must FAIL through the production
    // group-failure path.
    engine.fail_next_group_for(hash);
    let out = w
        .attempt_with_deadline(&engine, hash, &key, "k", &"y".repeat(4096), None, None)
        .await;
    assert!(
        !matches!(out, Outcome::Acked { .. }),
        "armed group write must fail, got {out:?}"
    );
    assert_eq!(
        engine.group_failures_tripped(),
        1,
        "failpoint must have fired"
    );

    // Neither the stored tail row nor the published ledger may have moved.
    assert_eq!(
        engine.db.get(&tail_row).await.unwrap().unwrap(),
        before_row,
        "failed group must not touch the stored tail row"
    );
    assert_eq!(
        engine.maintenance_snapshot(),
        before_snap,
        "failed group must not touch the published state"
    );
    engine.begin_close();
}

/// R25-D, layout 6: retirement is atomic with the sealed boundary. The
/// seal's object can be in the store, but until the shard group carrying
/// the seal commits, the backlog must remain outstanding — and when that
/// group is made to FAIL, boundary and ledger must both stay put, then
/// advance TOGETHER on the retry. A restart then rebuilds a zero ledger.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn absorbed_boundary_and_maintenance_retire_atomically() {
    let store = mem();
    let engine = open_engine(store.clone(), "dst-matomic").await;
    engine.configure_seal(|s| s.auto = false);
    let key = skey();
    let hash = [22u8; 16];
    let cov = FaultStore::uniform(mem(), 1, FaultPlan::new(0, 0, 0)).coverage();
    let w = Workload::new(cov);
    for i in 0..3 {
        let out = w
            .attempt_with_deadline(&engine, hash, &key, "k", &format!("r{i}"), None, None)
            .await;
        assert!(matches!(out, Outcome::Acked { .. }));
    }
    let before = engine.maintenance_snapshot();
    let outstanding = before.unabsorbed_frame_bytes;
    assert!(outstanding > 0);

    // The armed seal group fails: the seal reports it, the backlog stays.
    engine.fail_next_seal_group();
    let failed = engine.seal_requested().await;
    assert!(
        failed.is_err(),
        "the armed seal group must fail: {failed:?}"
    );
    assert_eq!(engine.group_failures_tripped(), 1, "the failpoint fired");
    assert_eq!(
        engine.maintenance_snapshot(),
        before,
        "a failed seal group must leave the ledger untouched"
    );
    for tail in [
        durable_tail(&engine, hash).await,
        stored_tail(&engine, &hash).await,
    ] {
        assert_eq!(
            (tail.absorbed, tail.next, tail.unabsorbed_bytes),
            (0, 3, outstanding),
            "a failed seal group must leave the boundary and the gauge put"
        );
    }

    // The retry: boundary and ledger retire TOGETHER.
    let report = engine.seal_requested().await.expect("the retry seals");
    assert_eq!(report.streams, 1);
    assert_eq!(engine.maintenance_snapshot().unabsorbed_frame_bytes, 0);
    for tail in [
        durable_tail(&engine, hash).await,
        stored_tail(&engine, &hash).await,
    ] {
        assert_eq!(
            (tail.absorbed, tail.next, tail.unabsorbed_bytes),
            (3, 3, 0),
            "the boundary must advance with the retirement"
        );
    }

    // R25-D: complete sealing survives restart at zero.
    engine
        .await_terminated(std::time::Duration::from_secs(30))
        .await
        .expect("engine terminates");
    let db2 = crate::keyspace::db_builder("dst-matomic", store.clone())
        .build()
        .await
        .unwrap();
    let m = crate::shard::load_or_rebuild_maintenance(&db2)
        .await
        .unwrap();
    assert_eq!(
        m.unabsorbed_frame_bytes, 0,
        "complete sealing must restart at zero backlog"
    );
    db2.close().await.unwrap();
}

/// One commit group carrying a client append (`body`) and a seal of the
/// stream's backlog: the commit gate is held until the rider is enqueued
/// and the seal has sent its commit to the committer, so the committer
/// takes both into one group when the gate opens. Returns the seal's report.
#[expect(
    clippy::disallowed_methods,
    reason = "rider_and_seal_in_one_group; the rider append and the seal are joined after the held group is released; both must be in flight concurrently to land in one group"
)]
async fn rider_and_seal_in_one_group(
    engine: &std::sync::Arc<crate::shard::ShardEngine>,
    hash: [u8; 16],
    key: &crate::crypto::StreamKey,
    body: String,
) -> crate::shard::seal::SealReport {
    let cov = FaultStore::uniform(mem(), 1, FaultPlan::new(0, 0, 0)).coverage();
    let hold = engine.test_hold_commit().await;
    let base = engine.appends_enqueued();
    let (e2, k2) = (engine.clone(), key.clone());
    let rider = tokio::spawn(async move {
        Workload::new(cov)
            .attempt_with_deadline(&e2, hash, &k2, "k", &body, None, None)
            .await
    });
    while engine.appends_enqueued() < base + 1 {
        tokio::time::sleep(std::time::Duration::from_millis(2)).await;
    }
    let sent = engine.seal_commits_sent();
    let e3 = engine.clone();
    let seal = tokio::spawn(async move { e3.seal_requested().await });
    while engine.seal_commits_sent() == sent {
        tokio::time::sleep(std::time::Duration::from_millis(2)).await;
    }
    drop(hold);
    let out = rider.await.unwrap();
    assert!(
        matches!(out, Outcome::Acked { .. }),
        "rider append: {out:?}"
    );
    seal.await
        .unwrap()
        .expect("the seal commits with the rider")
}

/// R26-2, layout 6: a MIXED append+seal commit group records BOTH sides.
/// The R25 net-delta accounting collapsed such a group to one direction:
/// append 100 / retire 80 became "+20 added, 0 retired", so the progress
/// clock never refreshed while maintenance was keeping pace — sustained
/// ingest slightly above retirement would ride a false LagSecs latch into
/// an instance-wide shed. Composed deterministically with the commit gate:
/// one group carrying a client append AND a seal of the earlier backlog.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mixed_append_absorb_group_refreshes_the_progress_clock() {
    let store = mem();
    let engine = open_engine(store.clone(), "dst-mixed").await;
    engine.configure_seal(|s| s.auto = false);
    let key = skey();
    let hash = [24u8; 16];
    let cov = FaultStore::uniform(mem(), 1, FaultPlan::new(0, 0, 0)).coverage();

    // Group A: two plain appends establish backlog L0.
    let w = Workload::new(cov);
    for i in 0..2 {
        let out = w
            .attempt_with_deadline(&engine, hash, &key, "k", &format!("m{i}"), None, None)
            .await;
        assert!(matches!(out, Outcome::Acked { .. }));
    }
    let tail0 = durable_tail(&engine, hash).await;
    let backlog0 = tail0.unabsorbed_bytes;
    assert!(backlog0 > 0);
    let before = engine.maintenance_snapshot();
    let ingest0 = crate::shard::INGEST_FRAME_BYTES_TOTAL.load(std::sync::atomic::Ordering::Relaxed);
    let absorbed0 =
        crate::shard::ABSORBED_FRAME_BYTES_TOTAL.load(std::sync::atomic::Ordering::Relaxed);
    // A millisecond clock needs real separation to prove a refresh.
    tokio::time::sleep(std::time::Duration::from_millis(40)).await;

    // ONE group: a client append (adds page bytes) + a seal retiring ALL
    // of the pre-group backlog. Net grows, yet the group made real
    // durable progress.
    let report = rider_and_seal_in_one_group(&engine, hash, &key, "n".repeat(512)).await;
    assert_eq!(
        report.page_bytes, backlog0,
        "the seal took the pre-group backlog"
    );

    let tail1 = durable_tail(&engine, hash).await;
    assert_eq!(tail1.absorbed, tail0.next, "boundary must have advanced");
    let appended_new = tail1.unabsorbed_bytes;
    assert!(appended_new > 0, "the rider's pages are the new backlog");
    let after = engine.maintenance_snapshot();
    assert_eq!(
        after.unabsorbed_frame_bytes, appended_new,
        "published ledger must equal the tail gauge"
    );
    assert_eq!(
        after.version,
        before.version + 1,
        "the rider and the seal landed in one group"
    );
    // THE regression: net-delta accounting left last_progress_ms at the
    // group-A value because the mixed group's net was positive.
    assert!(
        after.last_progress_ms > before.last_progress_ms,
        "a mixed group that retired backlog must refresh the progress \
         clock (before={} after={})",
        before.last_progress_ms,
        after.last_progress_ms,
    );
    // Process totals move by ACTUAL work, not net: the group retired
    // backlog0 and ingested the rider's pages ("grew by at least" — the
    // totals are process-global and the suite runs in parallel).
    let ingest1 = crate::shard::INGEST_FRAME_BYTES_TOTAL.load(std::sync::atomic::Ordering::Relaxed);
    let absorbed1 =
        crate::shard::ABSORBED_FRAME_BYTES_TOTAL.load(std::sync::atomic::Ordering::Relaxed);
    assert!(
        absorbed1 - absorbed0 >= backlog0,
        "retired page bytes must count even in a net-positive group"
    );
    assert!(
        ingest1 - ingest0 >= appended_new,
        "appended page bytes must count in full, not net of retirement"
    );
    engine.begin_close();
}

/// R26-2 (companion), layout 6: a perfectly BALANCED group — append and
/// retire the same byte count — has zero net movement and still must
/// move the ledger's version and refresh the progress clock. Under net
/// accounting it vanished entirely: no write, no refresh, a false stall
/// while maintenance was keeping exact pace with ingest.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn balanced_append_absorb_group_still_writes_progress() {
    let store = mem();
    let engine = open_engine(store.clone(), "dst-balanced").await;
    engine.configure_seal(|s| s.auto = false);
    let key = skey();
    let hash = [25u8; 16];
    let cov = FaultStore::uniform(mem(), 1, FaultPlan::new(0, 0, 0)).coverage();

    // One append; its page bytes are the whole ledger.
    let w = Workload::new(cov);
    let out = w
        .attempt_with_deadline(&engine, hash, &key, "k", &"b".repeat(256), None, None)
        .await;
    assert!(matches!(out, Outcome::Acked { .. }));
    let tail0 = durable_tail(&engine, hash).await;
    let backlog0 = tail0.unabsorbed_bytes;
    assert!(backlog0 > 0);
    let before = engine.maintenance_snapshot();
    tokio::time::sleep(std::time::Duration::from_millis(40)).await;

    // ONE group: append an identical-length payload (equal page bytes —
    // the cipher is length-preserving for equal plaintext lengths) while
    // sealing the first record's page. added == retired, net == 0.
    let report = rider_and_seal_in_one_group(&engine, hash, &key, "b".repeat(256)).await;
    assert_eq!(report.page_bytes, backlog0, "the seal took the first page");

    let tail1 = durable_tail(&engine, hash).await;
    assert_eq!(tail1.absorbed, tail0.next);
    assert_eq!(
        tail1.unabsorbed_bytes, backlog0,
        "identical payload lengths encode to identical page bytes"
    );
    let after = engine.maintenance_snapshot();
    assert_eq!(after.unabsorbed_frame_bytes, backlog0);
    assert_eq!(
        after.version,
        before.version + 1,
        "a balanced group must still move the ledger, in one group"
    );
    assert!(
        after.last_progress_ms > before.last_progress_ms,
        "a balanced group made real progress and must say so"
    );
    engine.begin_close();
}

/// R25-D: ownership handoff. B fences A and loads the durable backlog;
/// A's late shutdown cannot damage B's state — structurally, because
/// each engine OWNS its state and there is no global map for a stale
/// task to delete from.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ownership_handoff_moves_backlog_without_aba() {
    let store = mem();
    let engine_a = open_engine(store.clone(), "dst-handoff").await;
    let key = skey();
    let hash = [23u8; 16];
    let cov = FaultStore::uniform(mem(), 1, FaultPlan::new(0, 0, 0)).coverage();
    let w = Workload::new(cov);
    let out = w
        .attempt_with_deadline(&engine_a, hash, &key, "k", &"z".repeat(2048), None, None)
        .await;
    assert!(matches!(out, Outcome::Acked { .. }));
    let backlog = engine_a.maintenance_snapshot().unabsorbed_frame_bytes;
    assert!(backlog > 0);
    // Make A's committed state durable for the fencing successor (the
    // test config acks from the WAL/memtable; a real fleet handoff sees
    // the same rows via WAL replay).
    engine_a.db.flush().await.unwrap();

    // B opens the same shard (fencing A at the slatedb layer) and loads
    // the durable row — while A is STILL ALIVE: the ABA shape.
    let db_b = crate::keyspace::db_builder("dst-handoff", store.clone())
        .build()
        .await
        .unwrap();
    let maint_b = crate::shard::load_or_rebuild_maintenance(&db_b)
        .await
        .expect("B loads the durable backlog");
    assert_eq!(
        maint_b.unabsorbed_frame_bytes, backlog,
        "the new owner inherits the durable backlog exactly"
    );
    let engine_b = crate::shard::ShardEngine::start(
        "dst-handoff".into(),
        Arc::new(db_b),
        store.clone(),
        crate::shard::ShardConfig::default(),
        None,
        maint_b,
    );

    // A's LATE shutdown — after B is serving — must not move B's state.
    engine_a.begin_close();
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    assert_eq!(
        engine_b.maintenance_snapshot().unabsorbed_frame_bytes,
        backlog,
        "old owner's late cleanup must not damage the new owner's state"
    );
    engine_b.begin_close();
}
