//! R13: failed accounting reads preserve the group and the newer dirty version.
//! A billing close that would change nothing is skipped whole.
#![cfg(test)]
use super::*;

#[expect(
    clippy::excessive_nesting,
    reason = "r13_failed_accounting_reads_preserve_group_and_newer_dirty_version; the fixture stages the failed read inside the group inside the engine it drives, and the assertions read that nesting; flattening it would separate the failure from the group it must preserve"
)]
#[expect(
    clippy::too_many_lines,
    reason = "r13_failed_accounting_reads_preserve_group_and_newer_dirty_version; the scenario pins one ordered sequence of a failed accounting read, the preserved group and the newer dirty version; helper phases would hide which step each assertion observes"
)]
#[expect(
    clippy::let_underscore_must_use,
    reason = "r13_failed_accounting_reads_preserve_group_and_newer_dirty_version; the fixture ignores a delivery or join result whose only failure is the shutdown it stages itself; treating it as fallible would add branches the pinned sequence never takes"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn r13_failed_accounting_reads_preserve_group_and_newer_dirty_version() {
    let store: Arc<dyn object_store::ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let db = Arc::new(Db::builder("r13", store.clone()).build().await.unwrap());
    let (tx, _rx) = mpsc::channel(1);
    let engine = ShardEngine::start(
        "r13".into(),
        db.clone(),
        store,
        ShardConfig::default(),
        tx,
        None,
        ShardMaintenance::default(),
    );
    let hash = [13; 16];
    let meta = crate::billing::SegmentBillingMetaV1 {
        v: 1,
        stream_id: "existing".into(),
        usage_version: 8,
        ingest_payload_bytes_total: 923,
        owned_frame_bytes_current: 876,
        ..Default::default()
    };
    let encoded = serde_json::to_vec(&meta).unwrap();
    let dirty = crate::billing::usage_dirty_key(&hash);
    let key = crate::billing::billing_meta_key(&hash);
    let final_key = crate::billing::usage_month_final_key(&hash, 2026, 7);
    for corrupt in [false, true] {
        let before = if corrupt {
            b"invalid financial state".to_vec()
        } else {
            encoded.clone()
        };
        let mut wb = WriteBatch::new();
        wb.put(key.clone(), before.clone());
        wb.put(dirty.clone(), 8u64.to_le_bytes());
        wb.put(final_key.clone(), b"owed snapshot");
        wb.put(record_key(&hash, 0), b"retained record");
        db.write(wb).await.unwrap();
        for action in 0..3 {
            if !corrupt {
                billing_read_faults()
                    .lock()
                    .unwrap()
                    .insert("r13".into(), ());
            }
            let accounting = match action {
                0 => CommitOp::UsageAck {
                    hash,
                    scope: UsageAckScope::ThroughVersion(7),
                    month_final_keys: vec![final_key.clone()],
                },
                1 => CommitOp::BillingClose {
                    hash,
                    close_ms: 1000,
                },
                _ => CommitOp::BillingRetained {
                    hash,
                    retained: true,
                },
            };
            let (tx, rx) = oneshot::channel();
            engine
                .commit_group(
                    vec![
                        CommitOp::Queue {
                            hash,
                            op: crate::queue::QueueOp::ConfigGet {
                                consumer: "c".into(),
                            },
                            resp: tx,
                        },
                        accounting,
                    ],
                    &ShardConfig::default(),
                )
                .await;
            assert!(
                rx.await.unwrap().is_err(),
                "no group success on required read failure"
            );
            assert_eq!(db.get(&key).await.unwrap().unwrap().as_ref(), &before);
            assert_eq!(
                db.get(&dirty).await.unwrap().unwrap().as_ref(),
                &8u64.to_le_bytes()
            );
            assert_eq!(
                db.get(&final_key).await.unwrap().unwrap().as_ref(),
                b"owed snapshot"
            );
            assert_eq!(
                db.get(record_key(&hash, 0))
                    .await
                    .unwrap()
                    .unwrap()
                    .as_ref(),
                b"retained record"
            );
        }
    }
    db.put(&key, encoded).await.unwrap();
    engine
        .commit_group(
            vec![CommitOp::UsageAck {
                hash,
                scope: UsageAckScope::ThroughVersion(7),
                month_final_keys: vec![],
            }],
            &ShardConfig::default(),
        )
        .await;
    assert_eq!(
        db.get(&dirty).await.unwrap().unwrap().as_ref(),
        &8u64.to_le_bytes()
    );
    engine.begin_close();
    let _ = db.close().await;
}

type Meta = crate::billing::SegmentBillingMetaV1;
const SEGMENT: [u8; 16] = [14; 16];
const HOUR: i64 = 3_600_000;
/// 2026-09-21T14:13:20Z; `CLOSED` and `LATER` are inside September 2026 too.
const OPENED: i64 = 1_790_000_000_000;
const CLOSED: i64 = OPENED + HOUR;
const LATER: i64 = CLOSED + HOUR;

fn identity() -> crate::billing::BillingIdentity {
    crate::billing::BillingIdentity {
        account_id: "acct".into(),
        project_id: "proj".into(),
        stream_id: "epoch-1".into(),
        stream_name: "orders".into(),
    }
}

/// A billed row with an open gauge of 876 bytes, accounted through `OPENED`.
fn open_row() -> Meta {
    let id = identity();
    Meta {
        v: 1,
        account_id: id.account_id,
        project_id: id.project_id,
        stream_id: id.stream_id,
        stream_name: id.stream_name,
        segment_id: 0,
        usage_version: 8,
        ingest_payload_bytes_total: 923,
        ingest_records_total: 3,
        owned_frame_bytes_current: 876,
        storage_accounted_through_ms: OPENED,
        month_year: 2026,
        month_month: 9,
        month_ingest_payload_bytes: 923,
        month_ingest_records: 3,
        month_storage_byte_ms: "0".into(),
        retained_by_forks: false,
    }
}

/// `open_row` closed once at `CLOSED`: one hour of 876 bytes.
fn closed_row() -> Meta {
    Meta {
        usage_version: 9,
        owned_frame_bytes_current: 0,
        storage_accounted_through_ms: CLOSED,
        month_storage_byte_ms: "3153600000".into(),
        ..open_row()
    }
}

fn close(close_ms: i64) -> CommitOp {
    CommitOp::BillingClose {
        hash: SEGMENT,
        close_ms,
    }
}

fn json(row: &Meta) -> String {
    serde_json::to_string(row).unwrap()
}

struct CloseRig {
    engine: Arc<ShardEngine>,
    db: Arc<Db>,
    _signals: mpsc::Receiver<AbsorbSignal>,
}

impl CloseRig {
    /// An engine over its own store. It holds `row`, acknowledged: no dirty
    /// marker. `None`: the segment has no billing row.
    async fn start(prefix: &str, row: Option<&Meta>) -> Self {
        let store: Arc<dyn object_store::ObjectStore> =
            Arc::new(object_store::memory::InMemory::new());
        let db = Arc::new(Db::builder(prefix, store.clone()).build().await.unwrap());
        if let Some(row) = row {
            let key = crate::billing::billing_meta_key(&SEGMENT);
            db.put(&key, serde_json::to_vec(row).unwrap())
                .await
                .unwrap();
        }
        let (tx, signals) = mpsc::channel(16);
        let engine = ShardEngine::start(
            prefix.into(),
            db.clone(),
            store,
            ShardConfig::default(),
            tx,
            None,
            ShardMaintenance::default(),
        );
        Self {
            engine,
            db,
            _signals: signals,
        }
    }

    async fn commit(&self, ops: Vec<CommitOp>) {
        self.engine.commit_group(ops, &ShardConfig::default()).await;
    }

    /// The stored row as JSON text, and the version its dirty marker holds.
    async fn stored(&self) -> (Option<String>, Option<u64>) {
        let row = self
            .db
            .get(crate::billing::billing_meta_key(&SEGMENT))
            .await
            .unwrap()
            .map(|row| String::from_utf8(row.to_vec()).unwrap());
        let dirty = self
            .db
            .get(crate::billing::usage_dirty_key(&SEGMENT))
            .await
            .unwrap()
            .map(|mark| u64::from_le_bytes(mark.as_ref().try_into().unwrap()));
        (row, dirty)
    }

    async fn rows(&self) -> std::collections::BTreeMap<Vec<u8>, Vec<u8>> {
        let mut out = std::collections::BTreeMap::new();
        let mut rows = self.db.scan(..).await.unwrap();
        while let Some(row) = rows.next().await.unwrap() {
            out.insert(row.key.to_vec(), row.value.to_vec());
        }
        out
    }

    async fn stop(self) {
        self.engine.begin_close();
        self.engine
            .await_terminated(std::time::Duration::from_secs(5))
            .await
            .unwrap();
    }
}

/// The injected billing clock, held exclusively and reset on drop.
struct ClockAt {
    _exclusive: tokio::sync::RwLockWriteGuard<'static, ()>,
}

impl ClockAt {
    async fn set(now_ms: i64) -> Self {
        let exclusive = crate::billing::billing_clock_lock().write().await;
        crate::billing::BILLING_CLOCK_OVERRIDE.store(now_ms, Ordering::Relaxed);
        Self {
            _exclusive: exclusive,
        }
    }
}

impl Drop for ClockAt {
    fn drop(&mut self) {
        crate::billing::BILLING_CLOCK_OVERRIDE.store(0, Ordering::Relaxed);
    }
}

fn billed_append() -> (CommitOp, oneshot::Receiver<Result<AppendAck, AppendErr>>) {
    let (resp, reply) = oneshot::channel();
    let request = AppendReq {
        hash: SEGMENT,
        route: [9; 16],
        enqueued_at: std::time::Instant::now(),
        entries: vec![Bytes::from_static(b"payload")],
        routing_key: "lane".into(),
        key_hash: [7; 16],
        producer_lineage: vec![],
        key_version: 1,
        subkey: [1; 32],
        ts_hint_ms: Some(77),
        seq: None,
        bytes: 7,
        finish: AppendFinish::Open,
        billing: Some(Arc::new(crate::billing::BillingRef {
            identity: identity(),
            segment_id: 0,
        })),
        seal_gen: None,
        producer: None,
        deferred_error: None,
        sealed_reject_new: None,
        touch: None,
        usage: Arc::new(Default::default()),
        resp,
    };
    (CommitOp::Append(request), reply)
}

/// Two closers enqueued their close before either applied: the row is
/// closed once.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn two_closes_in_one_group_close_the_row_once() {
    let rig = CloseRig::start("close-one-group", Some(&open_row())).await;
    rig.commit(vec![close(CLOSED), close(CLOSED)]).await;
    assert_eq!(
        rig.stored().await,
        (Some(json(&closed_row())), Some(9)),
        "two closes in one group close the row once"
    );
    rig.stop().await;
}

/// A close of a row that an earlier group closed through its instant writes
/// nothing: no version, no dirty marker, no row at all.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_close_of_a_row_closed_in_an_earlier_group_writes_nothing() {
    let rig = CloseRig::start("close-two-groups", Some(&open_row())).await;
    rig.commit(vec![close(CLOSED)]).await;
    assert_eq!(rig.stored().await, (Some(json(&closed_row())), Some(9)));
    rig.commit(vec![CommitOp::UsageAck {
        hash: SEGMENT,
        scope: UsageAckScope::ThroughVersion(9),
        month_final_keys: vec![],
    }])
    .await;
    assert_eq!(rig.stored().await, (Some(json(&closed_row())), None));
    let before = rig.rows().await;
    for at in [CLOSED, OPENED] {
        rig.commit(vec![close(at)]).await;
        assert_eq!(
            rig.stored().await,
            (Some(json(&closed_row())), None),
            "a close at {at} of a row closed through {CLOSED} writes nothing"
        );
        assert_eq!(rig.rows().await, before);
    }
    rig.stop().await;
}

/// A later instant on a closed row still moves the clock, at gauge 0.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_later_instant_on_a_closed_row_still_moves_the_clock() {
    let rig = CloseRig::start("close-later", Some(&closed_row())).await;
    rig.commit(vec![close(LATER)]).await;
    let want = Meta {
        usage_version: 10,
        storage_accounted_through_ms: LATER,
        ..closed_row()
    };
    assert_eq!(rig.stored().await, (Some(json(&want)), Some(10)));
    rig.stop().await;
}

/// An open gauge always closes, whatever the instant: before its clock or
/// on it, the clock and the byte-time stay and the gauge goes to zero.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_close_of_an_open_gauge_at_or_before_its_clock_still_closes() {
    let row = Meta {
        storage_accounted_through_ms: CLOSED,
        ..open_row()
    };
    for (prefix, at) in [("close-open-before", OPENED), ("close-open-on", CLOSED)] {
        let rig = CloseRig::start(prefix, Some(&row)).await;
        rig.commit(vec![close(at)]).await;
        let want = Meta {
            usage_version: 9,
            owned_frame_bytes_current: 0,
            ..row.clone()
        };
        assert_eq!(
            rig.stored().await,
            (Some(json(&want)), Some(9)),
            "a close at {at}"
        );
        rig.stop().await;
    }
}

/// A skipped close after an append in its group: the append's row is
/// written and marked, and the first close of the two closed it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_skipped_close_after_an_append_in_its_group_leaves_the_row_dirty_and_written() {
    let _clock = ClockAt::set(CLOSED).await;
    let rig = CloseRig::start("close-after-append", Some(&open_row())).await;
    let (append, reply) = billed_append();
    rig.commit(vec![append, close(CLOSED), close(CLOSED)]).await;
    let (row, dirty) = rig.stored().await;
    let row: Meta = serde_json::from_str(&row.unwrap()).unwrap();
    assert_eq!(
        (
            row.usage_version,
            row.owned_frame_bytes_current,
            row.storage_accounted_through_ms,
            row.month_storage_byte_ms.as_str(),
            row.ingest_records_total,
            dirty
        ),
        (10, 0, CLOSED, "3153600000", 4, Some(10)),
        "one version for the append, one for the close, none for the second close"
    );
    assert!(rig.db.get(record_key(&SEGMENT, 0)).await.unwrap().is_some());
    let ack = tokio::time::timeout(std::time::Duration::from_secs(5), reply)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(ack.next_offset, 1);
    rig.stop().await;
}

/// A skip never touches the mark an earlier op of its group set: the
/// retained flag's version is written although the close after it changes
/// nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_skipped_close_keeps_the_mark_an_earlier_op_of_its_group_set() {
    let rig = CloseRig::start("close-after-retained", Some(&closed_row())).await;
    rig.commit(vec![
        CommitOp::BillingRetained {
            hash: SEGMENT,
            retained: true,
        },
        close(CLOSED),
    ])
    .await;
    let want = Meta {
        usage_version: 10,
        retained_by_forks: true,
        ..closed_row()
    };
    assert_eq!(rig.stored().await, (Some(json(&want)), Some(10)));
    rig.stop().await;
}

/// A close that carries no instant closes at the billing clock.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_close_without_an_instant_closes_at_the_billing_clock() {
    let _clock = ClockAt::set(LATER).await;
    let rig = CloseRig::start("close-at-clock", Some(&open_row())).await;
    rig.commit(vec![close(0)]).await;
    let want = Meta {
        usage_version: 9,
        owned_frame_bytes_current: 0,
        storage_accounted_through_ms: LATER,
        month_storage_byte_ms: "6307200000".into(),
        ..open_row()
    };
    assert_eq!(rig.stored().await, (Some(json(&want)), Some(9)));
    rig.stop().await;
}

/// A close that crosses a month boundary stages the closed month's final.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_close_across_a_month_boundary_stages_the_closed_months_final() {
    let october = crate::billing::month_start_ms(2026, 10);
    let at = october + HOUR;
    let rig = CloseRig::start("close-across-months", Some(&open_row())).await;
    rig.commit(vec![close(at)]).await;
    let want = Meta {
        usage_version: 9,
        owned_frame_bytes_current: 0,
        storage_accounted_through_ms: at,
        month_month: 10,
        month_ingest_payload_bytes: 0,
        month_ingest_records: 0,
        month_storage_byte_ms: "3153600000".into(),
        ..open_row()
    };
    assert_eq!(rig.stored().await, (Some(json(&want)), Some(9)));
    let september = crate::billing::SegmentSnapshot {
        identity: identity(),
        segment_id: 0,
        usage_version: 8,
        month: "2026-09".into(),
        month_final: true,
        ingest_payload_bytes_month: 923,
        ingest_records_month: 3,
        owned_frame_bytes_current: 876,
        storage_byte_ms_month: (u128::from(876u32) * u128::try_from(october - OPENED).unwrap())
            .to_string(),
        storage_accounted_through_ms: october,
        retained_by_forks: false,
    };
    let staged = rig
        .db
        .get(crate::billing::usage_month_final_key(&SEGMENT, 2026, 9))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(staged.as_ref(), serde_json::to_vec(&september).unwrap());
    rig.stop().await;
}

/// A close of a segment that has no billing row writes nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_close_of_a_segment_without_a_billing_row_writes_nothing() {
    let rig = CloseRig::start("close-no-row", None).await;
    let before = rig.rows().await;
    rig.commit(vec![close(CLOSED)]).await;
    assert_eq!(rig.stored().await, (None, None));
    assert_eq!(rig.rows().await, before);
    rig.stop().await;
}
