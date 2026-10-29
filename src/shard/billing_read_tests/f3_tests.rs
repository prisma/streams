//! Layout 6 (f3): billing out of the per-group write set — the static row
//! once per incarnation-month, the counters on the tail, durable metering
//! and the in-memory usage dirty set across a rejected group and a reopen.
#![cfg(test)]
use super::{
    Arc, ClockAt, CloseRig, CommitOp, HOUR, OPENED, Ordering, SEGMENT, ShardConfig, ShardEngine,
    ShardMaintenance, UsageAckScope, billed_append, json, open_row, stored_in, test_support,
    unbilled_append, usage_dirty_version,
};

/// Layout 6 (f3): a billed segment's static row is staged once per
/// incarnation-month and read from the store once per handle; its usage
/// counters ride the tail. The resident meta answers every later group with
/// no store read; the usage dirty set holds the newest version until the
/// ledger acknowledges it, and a fresh engine over the same store rebuilds
/// it from the tails and the same meta from the static row and the tail.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn f3_billing_counters_ride_the_tail_and_the_static_row_is_written_once_a_month() {
    let rig = CloseRig::start("f3-tail", None).await;
    let (staged_before, read_before) = test_support::billing_row_io_on(&rig.db);
    let september = ClockAt::set(OPENED).await;
    for _ in 0..40 {
        let (append, reply) = billed_append();
        rig.commit(vec![append]).await;
        reply.await.unwrap().unwrap();
    }
    let meta = rig
        .engine
        .load_billing_meta(SEGMENT)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        (
            meta.usage_version,
            meta.ingest_records_total,
            meta.ingest_payload_bytes_total
        ),
        (40, 40, 40 * 7)
    );
    assert_eq!(
        test_support::billing_row_io_on(&rig.db),
        (staged_before + 1, read_before + 1),
        "one static row staged and read once for 40 groups"
    );
    assert_eq!(rig.stored().await, (Some(json(&meta)), Some(40)));
    drop(september);
    let _october = ClockAt::set(crate::billing::month_start_ms(2026, 10) + HOUR).await;
    let (append, reply) = billed_append();
    rig.commit(vec![append]).await;
    reply.await.unwrap().unwrap();
    let rolled = rig
        .engine
        .load_billing_meta(SEGMENT)
        .await
        .unwrap()
        .unwrap();
    assert_eq!((rolled.month_month, rolled.usage_version), (10, 41));
    assert_eq!(
        test_support::billing_row_io_on(&rig.db),
        (staged_before + 2, read_before + 1),
        "the month rollover stages the static row once more"
    );
    let (finals, _) = rig
        .engine
        .usage_month_finals_page(SEGMENT, 8)
        .await
        .unwrap();
    assert_eq!(finals.len(), 1, "September's final");
    rig.commit(vec![CommitOp::UsageAck {
        hash: SEGMENT,
        scope: UsageAckScope::ThroughVersion(41),
        month_final_keys: finals.into_iter().map(|(key, _)| key).collect(),
    }])
    .await;
    assert_eq!(rig.stored().await, (Some(json(&rolled)), None));
    assert!(!rig.engine.has_billing_debt().await.unwrap());
    let (store, prefix) = (rig.store.clone(), rig.engine.prefix.clone());
    rig.stop().await;
    let reopened = CloseRig::reopen(&prefix, store).await;
    assert_eq!(stored_in(&reopened.db).await, (Some(json(&rolled)), None));
    assert_eq!(
        usage_dirty_version(&reopened.engine, SEGMENT).await,
        Some(41),
        "a fresh engine marks every billed segment"
    );
    let rebuilt = reopened
        .engine
        .load_billing_meta(SEGMENT)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(json(&rebuilt), json(&rolled));
    reopened.stop().await;
}

/// Layout 6 (f3, REVIEW-6A F4 / Fable 4a): the usage drain meters a
/// segment's durable counters only. While a billed group's WAL write is
/// held, the committer stages on its applied counters (version 2) and marks
/// the segment dirty, but `load_billing_meta`, the drain's read, answers the
/// durable version 1; an acknowledgement through 1 leaves the segment dirty;
/// once the write is durable the drain reads version 2.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_usage_drain_meters_only_durable_counters() {
    let store = crate::dst::FaultStore::new(
        Arc::new(object_store::memory::InMemory::new()),
        44,
        crate::dst::FaultProfile::clean(),
    );
    let db = Arc::new(
        crate::keyspace::db_builder("f3-durable", store.clone())
            .with_settings(slatedb::config::Settings {
                flush_interval: Some(std::time::Duration::from_millis(5)),
                ..Default::default()
            })
            .build()
            .await
            .unwrap(),
    );
    let engine = ShardEngine::start(
        "f3-durable".into(),
        db.clone(),
        store.clone(),
        ShardConfig::default(),
        None,
        ShardMaintenance::default(),
    );
    let (append, reply) = billed_append();
    engine.commit_group(vec![append]).await;
    reply.await.unwrap().unwrap();
    let durable = engine.load_billing_meta(SEGMENT).await.unwrap().unwrap();
    assert_eq!(durable.usage_version, 1);
    let engaged = store.hold_class(crate::dst::StoreOp::Put, crate::dst::ObjClass::Wal, 1);
    let (append, reply) = billed_append();
    engine.commit_group(vec![append]).await;
    while engaged.load(Ordering::SeqCst) == 0 {
        tokio::task::yield_now().await;
    }
    let applied = engine.billing_for_commit(SEGMENT).await.unwrap().unwrap();
    assert_eq!(
        applied.usage_version, 2,
        "the committer stages on applied counters"
    );
    assert_eq!(usage_dirty_version(&engine, SEGMENT).await, Some(2));
    let metered = engine.load_billing_meta(SEGMENT).await.unwrap().unwrap();
    assert_eq!(
        json(&metered),
        json(&durable),
        "the drain reads the durable version only"
    );
    let ack = CommitOp::UsageAck {
        hash: SEGMENT,
        scope: UsageAckScope::ThroughVersion(1),
        month_final_keys: vec![],
    };
    engine.commit_group(vec![ack]).await;
    assert_eq!(
        usage_dirty_version(&engine, SEGMENT).await,
        Some(2),
        "1 < 2 keeps it dirty"
    );
    store.release_hold();
    reply.await.unwrap().unwrap();
    let now = engine.load_billing_meta(SEGMENT).await.unwrap().unwrap();
    assert_eq!((now.usage_version, now.ingest_records_total), (2, 2));
    engine.begin_close();
}

/// Layout 6 (f3, Fable 4b): an acknowledgement staged in a group whose
/// write fails clears the in-memory dirty set of an engine that retires
/// with it, and deletes none of the month finals it named: a fresh engine
/// over the store holds the finals and marks the segment dirty again, so
/// the drain emits both once more (the rollup ignores what it holds).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_ack_in_a_rejected_group_leaves_its_finals_and_the_segment_is_emitted_again() {
    let rig = CloseRig::start("f3-rejected-ack", Some(&open_row())).await;
    let _october = ClockAt::set(crate::billing::month_start_ms(2026, 10) + HOUR).await;
    let (append, reply) = billed_append();
    rig.commit(vec![append]).await;
    reply.await.unwrap().unwrap();
    let (finals, _) = rig
        .engine
        .usage_month_finals_page(SEGMENT, 8)
        .await
        .unwrap();
    assert_eq!(finals.len(), 1, "September's final");
    // The failpoint trips on a group holding a client append of the
    // segment: an unbilled one, staged after the ack, which leaves the
    // ack's clear of the in-memory set standing until the write fails.
    rig.engine.fail_next_group_for(SEGMENT);
    let (append, _refused) = unbilled_append();
    let ack = CommitOp::UsageAck {
        hash: SEGMENT,
        scope: UsageAckScope::ThroughVersion(9),
        month_final_keys: finals.iter().map(|(key, _)| key.clone()).collect(),
    };
    rig.commit(vec![ack, append]).await;
    let (store, prefix) = (rig.store.clone(), rig.engine.prefix.clone());
    rig.stop().await;
    let reopened = CloseRig::reopen(&prefix, store).await;
    let (kept, _) = reopened
        .engine
        .usage_month_finals_page(SEGMENT, 8)
        .await
        .unwrap();
    let keys = |rows: &[(Vec<u8>, crate::billing::SegmentSnapshot)]| -> Vec<Vec<u8>> {
        rows.iter().map(|(key, _)| key.clone()).collect()
    };
    assert_eq!(
        keys(&kept),
        keys(&finals),
        "the rejected group deleted no final"
    );
    assert_eq!(
        usage_dirty_version(&reopened.engine, SEGMENT).await,
        Some(9)
    );
    reopened.stop().await;
}
