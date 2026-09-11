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

    // The three views must agree EXACTLY: the stream tail gauge, the
    // durable shard row, and the engine's published snapshot.
    let tail = engine
        .tail_fields(&hash)
        .await
        .unwrap()
        .expect("tail exists after acked appends");
    assert!(tail.unabsorbed_bytes > 0, "appends must create backlog");

    let raw = engine
        .db
        .get(crate::shard::shard_maint_key())
        .await
        .unwrap()
        .expect("durable maintenance row staged with the commit group");
    let row = crate::shard::decode_shard_maint(&raw).unwrap();
    let snap = engine.maintenance_snapshot();

    assert_eq!(
        row.unabsorbed_frame_bytes, tail.unabsorbed_bytes,
        "durable row must equal the tail's encoded-frame gauge exactly"
    );
    assert_eq!(
        snap.unabsorbed_frame_bytes, tail.unabsorbed_bytes,
        "published snapshot must equal the durable row"
    );
    assert_ne!(
        row.unabsorbed_frame_bytes, payload_total,
        "the gauge must NOT be the logical payload total — that unit \
         mismatch manufactured the soak's 9.4% absorption artifact"
    );
}

/// R25-D: a FAILED append group leaves the durable maintenance row and
/// the engine's published state untouched. This is phantom backlog's
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
    let before_row = crate::shard::decode_shard_maint(
        &engine
            .db
            .get(crate::shard::shard_maint_key())
            .await
            .unwrap()
            .unwrap(),
    )
    .unwrap();
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

    // Neither the durable row nor the published state may have moved.
    let after_row = crate::shard::decode_shard_maint(
        &engine
            .db
            .get(crate::shard::shard_maint_key())
            .await
            .unwrap()
            .unwrap(),
    )
    .unwrap();
    assert_eq!(
        after_row, before_row,
        "failed group must not touch the durable maintenance row"
    );
    assert_eq!(
        engine.maintenance_snapshot(),
        before_snap,
        "failed group must not touch the published state"
    );
    engine.begin_close();
}

/// R25-D: retirement is atomic with the absorbed boundary. The history
/// copy can be durably flushed, but until the shard group carrying the
/// AbsorbedBatch commits, the backlog must remain outstanding — and when
/// that group is made to FAIL, boundary and maintenance must both stay
/// put, then advance TOGETHER on the retry.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn absorbed_boundary_and_maintenance_retire_atomically() {
    let store = mem();
    let db = slatedb::Db::builder("dst-matomic/shard", store.clone())
        .build()
        .await
        .unwrap();
    let (absorb_tx, absorb_rx) = crate::history::absorber_channel();
    let __maint = crate::shard::load_or_rebuild_maintenance(&db)
        .await
        .expect("load maintenance");
    let engine = crate::shard::ShardEngine::start(
        "dst-matomic".into(),
        Arc::new(db),
        store.clone(),
        crate::shard::ShardConfig::default(),
        absorb_tx,
        None,
        __maint,
    );
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
    let outstanding = engine.maintenance_snapshot().unabsorbed_frame_bytes;
    assert!(outstanding > 0);

    // Arm the absorbed-group failpoint, then start the absorber. Its
    // first boundary-advancing group must fail; the backlog must remain.
    engine.fail_next_absorbed_group();
    let _absorber = crate::history::Absorber::start(
        store.clone(),
        engine.clone(),
        Arc::new(crate::history::KeyCache::default()),
        crate::history::AbsorberConfig {
            threshold_bytes: 1,
            threshold_age: std::time::Duration::from_millis(1),
            tick: std::time::Duration::from_millis(20),
            sweep_every: u32::MAX,
            ..Default::default()
        },
        absorb_rx,
    );

    // Wait for the armed failure to fire.
    let mut fired = false;
    for _ in 0..500 {
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        if engine.group_failures_tripped() >= 1 {
            fired = true;
            break;
        }
    }
    assert!(fired, "absorbed-group failpoint never fired");
    // The history copy may exist by now — but the backlog is NOT retired.
    assert_eq!(
        engine.maintenance_snapshot().unabsorbed_frame_bytes,
        outstanding,
        "a failed absorbed-boundary group must leave the backlog outstanding"
    );

    // The absorber retries; boundary and maintenance retire TOGETHER.
    let mut drained = false;
    for i in 0..500 {
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        if engine.maintenance_snapshot().unabsorbed_frame_bytes == 0 {
            drained = true;
            break;
        }
        let _ = i;
    }
    assert!(drained, "retry never retired the backlog");
    let tail = engine.tail_fields(&hash).await.unwrap().unwrap();
    assert_eq!(
        tail.absorbed, tail.next,
        "boundary must have advanced with retirement"
    );
    assert_eq!(tail.unabsorbed_bytes, 0, "tail gauge must be drained");
    let row = crate::shard::decode_shard_maint(
        &engine
            .db
            .get(crate::shard::shard_maint_key())
            .await
            .unwrap()
            .unwrap(),
    )
    .unwrap();
    assert_eq!(row.unabsorbed_frame_bytes, 0, "durable row must be drained");

    // R25-D: complete absorption survives restart at zero.
    engine.begin_close();
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    let db2 = slatedb::Db::builder("dst-matomic/shard", store.clone())
        .build()
        .await
        .unwrap();
    let m = crate::shard::load_or_rebuild_maintenance(&db2)
        .await
        .unwrap();
    assert_eq!(
        m.unabsorbed_frame_bytes, 0,
        "complete absorption must restart at zero backlog"
    );
    db2.close().await.unwrap();
}

/// R26-2: a MIXED append+absorb commit group records BOTH sides. The
/// R25 net-delta accounting collapsed such a group to one direction:
/// append 100 / absorb 80 became "+20 added, 0 retired", so the durable
/// progress clock never refreshed while the absorber was keeping pace —
/// sustained ingest slightly above retirement would ride a false
/// LagSecs latch into an instance-wide shed. Composed deterministically
/// with the commit gate: one group carrying a client append AND an
/// absorbed-boundary advance.
#[expect(
    clippy::disallowed_methods,
    reason = "maintenance group fixture; the rider request is joined after the held group is released; it must ride the group concurrently to refresh the clock it observes"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mixed_append_absorb_group_refreshes_the_progress_clock() {
    let store = mem();
    let engine = open_engine(store.clone(), "dst-mixed").await;
    let key = skey();
    let hash = [24u8; 16];
    let cov = FaultStore::uniform(mem(), 1, FaultPlan::new(0, 0, 0)).coverage();

    // Group A: two plain appends establish backlog L0.
    let w = Workload::new(cov.clone());
    for i in 0..2 {
        let out = w
            .attempt_with_deadline(&engine, hash, &key, "k", &format!("m{i}"), None, None)
            .await;
        assert!(matches!(out, Outcome::Acked { .. }));
    }
    let tail0 = engine.tail_fields(&hash).await.unwrap().unwrap();
    let backlog0 = tail0.unabsorbed_bytes;
    assert!(backlog0 > 0);
    let row_before = crate::shard::decode_shard_maint(
        &engine
            .db
            .get(crate::shard::shard_maint_key())
            .await
            .unwrap()
            .unwrap(),
    )
    .unwrap();
    let ingest0 = crate::shard::INGEST_FRAME_BYTES_TOTAL.load(std::sync::atomic::Ordering::Relaxed);
    let absorbed0 =
        crate::shard::ABSORBED_FRAME_BYTES_TOTAL.load(std::sync::atomic::Ordering::Relaxed);
    // A millisecond clock needs real separation to prove a refresh.
    tokio::time::sleep(std::time::Duration::from_millis(40)).await;

    // ONE group: a client append (adds frame bytes) + an absorbed
    // advance retiring ALL of the pre-group backlog. Net grows, yet the
    // group made real durable absorb progress.
    let hold = engine.test_hold_commit().await;
    let base = engine.appends_enqueued();
    let e2 = engine.clone();
    let w2 = Workload::new(cov.clone());
    let k2 = key.clone();
    let rider = tokio::spawn(async move {
        w2.attempt_with_deadline(&e2, hash, &k2, "k", &"n".repeat(512), None, None)
            .await
    });
    while engine.appends_enqueued() < base + 1 {
        tokio::time::sleep(std::time::Duration::from_millis(2)).await;
    }
    engine.submit_absorbed(hash, tail0.next, backlog0).await;
    drop(hold);
    let out = rider.await.unwrap();
    assert!(
        matches!(out, Outcome::Acked { .. }),
        "rider append: {out:?}"
    );

    let tail1 = engine.tail_fields(&hash).await.unwrap().unwrap();
    assert_eq!(tail1.absorbed, tail0.next, "boundary must have advanced");
    let appended_new = tail1.unabsorbed_bytes;
    assert!(appended_new > 0, "the rider's frames are the new backlog");
    let row_after = crate::shard::decode_shard_maint(
        &engine
            .db
            .get(crate::shard::shard_maint_key())
            .await
            .unwrap()
            .unwrap(),
    )
    .unwrap();
    assert_eq!(
        engine.maintenance_snapshot().unabsorbed_frame_bytes,
        appended_new,
        "published ledger must equal the tail gauge"
    );
    // THE regression: net-delta accounting left last_progress_ms at the
    // group-A value because the mixed group's net was positive.
    assert!(
        row_after.last_progress_ms > row_before.last_progress_ms,
        "a mixed group that retired backlog must refresh the durable \
         progress clock (before={} after={})",
        row_before.last_progress_ms,
        row_after.last_progress_ms,
    );
    // Process totals move by ACTUAL work, not net: the group retired
    // backlog0 and ingested the rider's frames ("grew by at least" —
    // the totals are process-global and the suite runs in parallel).
    let ingest1 = crate::shard::INGEST_FRAME_BYTES_TOTAL.load(std::sync::atomic::Ordering::Relaxed);
    let absorbed1 =
        crate::shard::ABSORBED_FRAME_BYTES_TOTAL.load(std::sync::atomic::Ordering::Relaxed);
    assert!(
        absorbed1 - absorbed0 >= backlog0,
        "retired frame bytes must count even in a net-positive group"
    );
    assert!(
        ingest1 - ingest0 >= appended_new,
        "appended frame bytes must count in full, not net of retirement"
    );
    engine.begin_close();
}

/// R26-2 (companion): a perfectly BALANCED group — append and retire the
/// same byte count — has zero net movement and still must write the
/// durable row and refresh the progress clock. Under net accounting it
/// vanished entirely: no row, no refresh, a false stall while absorption
/// was keeping exact pace with ingest.
#[expect(
    clippy::disallowed_methods,
    reason = "maintenance group fixture; the rider request is joined after the held group is released; it must ride the group concurrently to land in it"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn balanced_append_absorb_group_still_writes_progress() {
    let store = mem();
    let engine = open_engine(store.clone(), "dst-balanced").await;
    let key = skey();
    let hash = [25u8; 16];
    let cov = FaultStore::uniform(mem(), 1, FaultPlan::new(0, 0, 0)).coverage();

    // One append; its frame bytes are the whole ledger.
    let w = Workload::new(cov.clone());
    let out = w
        .attempt_with_deadline(&engine, hash, &key, "k", &"b".repeat(256), None, None)
        .await;
    assert!(matches!(out, Outcome::Acked { .. }));
    let tail0 = engine.tail_fields(&hash).await.unwrap().unwrap();
    let backlog0 = tail0.unabsorbed_bytes;
    assert!(backlog0 > 0);
    let row_before = crate::shard::decode_shard_maint(
        &engine
            .db
            .get(crate::shard::shard_maint_key())
            .await
            .unwrap()
            .unwrap(),
    )
    .unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(40)).await;

    // ONE group: append an identical-length payload (equal frame bytes —
    // the cipher is length-preserving for equal plaintext lengths) while
    // retiring the first record's bytes. added == retired, net == 0.
    let hold = engine.test_hold_commit().await;
    let base = engine.appends_enqueued();
    let e2 = engine.clone();
    let w2 = Workload::new(cov.clone());
    let k2 = key.clone();
    let rider = tokio::spawn(async move {
        w2.attempt_with_deadline(&e2, hash, &k2, "k", &"b".repeat(256), None, None)
            .await
    });
    while engine.appends_enqueued() < base + 1 {
        tokio::time::sleep(std::time::Duration::from_millis(2)).await;
    }
    engine.submit_absorbed(hash, tail0.next, backlog0).await;
    drop(hold);
    let out = rider.await.unwrap();
    assert!(
        matches!(out, Outcome::Acked { .. }),
        "rider append: {out:?}"
    );

    let tail1 = engine.tail_fields(&hash).await.unwrap().unwrap();
    assert_eq!(tail1.absorbed, tail0.next);
    assert_eq!(
        tail1.unabsorbed_bytes, backlog0,
        "identical payload lengths encode to identical frame bytes"
    );
    let row_after = crate::shard::decode_shard_maint(
        &engine
            .db
            .get(crate::shard::shard_maint_key())
            .await
            .unwrap()
            .unwrap(),
    )
    .unwrap();
    assert!(
        row_after.version > row_before.version,
        "a balanced group must still write the durable row"
    );
    assert!(
        row_after.last_progress_ms > row_before.last_progress_ms,
        "a balanced group made real absorb progress and must say so"
    );
    engine.begin_close();
}

/// R26-3: retiring more bytes than the stream's exact ledger holds is a
/// divergence, not a clamp. The whole group fails — including a client
/// append riding in it — the durable boundary and row stay put, and the
/// engine keeps serving afterward.
#[expect(
    clippy::disallowed_methods,
    reason = "maintenance group fixture; the rider request is joined after the failing group is released; it must ride the group concurrently to observe the failure"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn over_retirement_fails_the_group_and_preserves_the_boundary() {
    let store = mem();
    let engine = open_engine(store.clone(), "dst-overret").await;
    let key = skey();
    let hash = [26u8; 16];
    let cov = FaultStore::uniform(mem(), 1, FaultPlan::new(0, 0, 0)).coverage();

    let w = Workload::new(cov.clone());
    let out = w
        .attempt_with_deadline(&engine, hash, &key, "k", "base", None, None)
        .await;
    assert!(matches!(out, Outcome::Acked { .. }));
    let tail0 = engine.tail_fields(&hash).await.unwrap().unwrap();
    let backlog0 = tail0.unabsorbed_bytes;
    let row_before = crate::shard::decode_shard_maint(
        &engine
            .db
            .get(crate::shard::shard_maint_key())
            .await
            .unwrap()
            .unwrap(),
    )
    .unwrap();

    // ONE group: a client append + an absorbed advance claiming MORE
    // bytes than the ledger holds. saturating_sub would clamp silently;
    // the checked ledger fails everything together.
    let hold = engine.test_hold_commit().await;
    let base = engine.appends_enqueued();
    let e2 = engine.clone();
    let k2 = key.clone();
    let mut w2 = Workload::new(cov.clone());
    w2.max_attempts = 1;
    let rider = tokio::spawn(async move {
        w2.attempt_with_deadline(&e2, hash, &k2, "k", "doomed", None, None)
            .await
    });
    while engine.appends_enqueued() < base + 1 {
        tokio::time::sleep(std::time::Duration::from_millis(2)).await;
    }
    engine
        .submit_absorbed(hash, tail0.next, backlog0 + 999)
        .await;
    drop(hold);
    let out = rider.await.unwrap();
    assert!(
        !matches!(out, Outcome::Acked { .. }),
        "a diverged group must commit NOTHING, but the rider acked: {out:?}"
    );

    let tail1 = engine.tail_fields(&hash).await.unwrap().unwrap();
    assert_eq!(tail1.absorbed, tail0.absorbed, "boundary must not move");
    assert_eq!(tail1.next, tail0.next, "the rider append must not commit");
    assert_eq!(tail1.unabsorbed_bytes, backlog0, "ledger must not move");
    let row_after = crate::shard::decode_shard_maint(
        &engine
            .db
            .get(crate::shard::shard_maint_key())
            .await
            .unwrap()
            .unwrap(),
    )
    .unwrap();
    assert_eq!(row_after, row_before, "durable row must not move");

    // The engine is not wedged: a fresh append acks, and an EXACT
    // retirement drains the ledger to zero.
    let out = w
        .attempt_with_deadline(&engine, hash, &key, "k", "alive", None, None)
        .await;
    assert!(
        matches!(out, Outcome::Acked { .. }),
        "engine wedged: {out:?}"
    );
    let tail2 = engine.tail_fields(&hash).await.unwrap().unwrap();
    engine
        .submit_absorbed(hash, tail2.next, tail2.unabsorbed_bytes)
        .await;
    let mut drained = false;
    for _ in 0..400 {
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        if engine.maintenance_snapshot().unabsorbed_frame_bytes == 0 {
            drained = true;
            break;
        }
    }
    assert!(drained, "exact retirement after the refusal never drained");
    engine.begin_close();
}

/// R26-4: an engine opening over a LEGACY namespace — a 16-byte
/// payload-unit maintenance row AND a tail written before the exact
/// gauge existed — ignores the legacy value, repairs the tail by
/// summing the actual stored frames, and persists exact v2 state. The
/// follow-through matters as much as the numbers: with the repaired
/// ledger, a full boundary advance retires cleanly through the CHECKED
/// accounting (an unrepaired zero-gauge tail would make the first
/// advance read as over-retirement and fail groups forever).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn legacy_rows_are_rebuilt_and_legacy_tails_repaired_on_open() {
    let store = mem();
    let prefix = "dst-legacy";
    let key = skey();
    let hash = [27u8; 16];
    let cov = FaultStore::uniform(mem(), 1, FaultPlan::new(0, 0, 0)).coverage();

    // A modern engine writes real records (real frames, exact tail).
    let engine = open_engine(store.clone(), prefix).await;
    let w = Workload::new(cov.clone());
    for i in 0..3 {
        let out = w
            .attempt_with_deadline(&engine, hash, &key, "k", &format!("L{i}"), None, None)
            .await;
        assert!(matches!(out, Outcome::Acked { .. }));
    }
    let exact_tail = engine.tail_fields(&hash).await.unwrap().unwrap();
    let exact_bytes = exact_tail.unabsorbed_bytes;
    assert!(exact_bytes > 0);
    // Downgrade the namespace in place: strip the tail's gauge field
    // and replace the maintenance row with the R24 16-byte layout
    // carrying a wildly wrong payload-unit number.
    let mut wb = slatedb::WriteBatch::new();
    wb.put(
        crate::shard::tail_key(&hash),
        crate::shard::encode_tail_without_gauge_for_tests(&exact_tail),
    );
    let mut v1 = [0u8; 16];
    v1[..8].copy_from_slice(&999_999_999u64.to_le_bytes());
    wb.put(crate::shard::shard_maint_key(), v1);
    engine
        .db
        .write_with_options(wb, &slatedb::config::WriteOptions::default())
        .await
        .unwrap();
    engine.db.flush().await.unwrap();
    engine.begin_close();
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    // Reopen: the loader must rebuild EXACT state from the frames.
    let db = slatedb::Db::builder(prefix, store.clone())
        .build()
        .await
        .unwrap();
    let maint = crate::shard::load_or_rebuild_maintenance(&db)
        .await
        .expect("legacy namespace must open via rebuild");
    assert_eq!(
        maint.unabsorbed_frame_bytes, exact_bytes,
        "ledger must be the actual frame sum — not the legacy 999999999"
    );
    let raw_tail = db
        .get(crate::shard::tail_key(&hash))
        .await
        .unwrap()
        .unwrap();
    let repaired = crate::shard::decode_tail_for_tests(&raw_tail).unwrap();
    assert_eq!(
        repaired.unabsorbed_bytes, exact_bytes,
        "the durable tail must be repaired to the exact gauge"
    );
    let raw_row = db
        .get(crate::shard::shard_maint_key())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(raw_row.len(), 40, "legacy row must be replaced by v2");

    // Follow-through: a full advance retires exactly under the checked
    // accounting and drains the ledger to zero.
    let (absorb_tx, _absorb_rx) = crate::history::absorber_channel();
    let engine2 = crate::shard::ShardEngine::start(
        prefix.to_string(),
        Arc::new(db),
        store.clone(),
        crate::shard::ShardConfig::default(),
        absorb_tx,
        None,
        maint,
    );
    engine2
        .submit_absorbed(hash, repaired.next, exact_bytes)
        .await;
    let mut drained = false;
    for _ in 0..400 {
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        if engine2.maintenance_snapshot().unabsorbed_frame_bytes == 0 {
            drained = true;
            break;
        }
    }
    assert!(drained, "repaired ledger never retired cleanly");
    engine2.begin_close();
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
    let db_b = slatedb::Db::builder("dst-handoff", store.clone())
        .build()
        .await
        .unwrap();
    let (absorb_tx_b, _rx_b) = crate::history::absorber_channel();
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
        absorb_tx_b,
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

/// The bounded discovery reader must consume the marker written by the real
/// committer, not only a hand-built legacy fixture.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn r09_committer_dirty_markers_survive_bounded_discovery() {
    let engine = open_engine(mem(), "r09-current-marker").await;
    let hash = [39; 16];
    let coverage = FaultStore::uniform(mem(), 1, FaultPlan::CLEAN).coverage();
    let workload = Workload::new(coverage);
    assert!(matches!(
        workload
            .attempt_with_deadline(&engine, hash, &skey(), "k", "record", None, None)
            .await,
        Outcome::Acked { .. }
    ));
    let key = crate::shard::dirty_key(&hash);
    let marker = engine.db.get(&key).await.unwrap().unwrap();
    assert_eq!(
        marker.len(),
        32,
        "current writer includes byte and age fields"
    );
    let (rows, more) = engine.scan_dirty_streams_page(None, 1).await.unwrap();
    assert_eq!(rows, vec![(hash, 0, 1)]);
    assert!(!more);
    for width in 0..40 {
        let raw = vec![0; width];
        assert_eq!(
            crate::shard::decode_dirty_value(&raw).is_some(),
            matches!(width, 16 | 24 | 32),
            "width {width}"
        );
    }
    let corrupt = marker.slice(..17);
    engine
        .db
        .put(&key, corrupt.clone())
        .await
        .unwrap()
        .await_durable()
        .await
        .unwrap();
    assert!(engine.scan_dirty_streams_page(None, 1).await.is_err());
    assert_eq!(
        engine.db.get(&key).await.unwrap().unwrap(),
        corrupt,
        "failed discovery preserves evidence"
    );
    engine.begin_close();
}
