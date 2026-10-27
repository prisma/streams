//! A stream's billing row and consumer records stay on its resident handle
//! once a commit group read or staged them: the next group takes them from
//! there, not from the store. A group refused before its write leaves no
//! row behind.
#![cfg(test)]
use super::*;
use crate::queue::{ConsumerConfig, ConsumerLifecycle, ConsumerRecord, QueueOp, QueueOut};

const SEGMENT: [u8; 16] = [0x5e; 16];

struct Rig {
    engine: Arc<ShardEngine>,
    db: Arc<Db>,
    _signals: mpsc::Receiver<AbsorbSignal>,
}

impl Rig {
    async fn start(prefix: &str) -> Self {
        let store: Arc<dyn object_store::ObjectStore> =
            Arc::new(object_store::memory::InMemory::new());
        let db = Arc::new(Db::builder(prefix, store.clone()).build().await.unwrap());
        let (tx, signals) = mpsc::channel(64);
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

    /// One billed append of `payload` to `SEGMENT` as its own group; its reply.
    async fn append(&self, payload: &'static [u8]) -> Result<AppendAck, AppendErr> {
        let (append, reply) = append_op(payload);
        self.engine
            .commit_group(vec![append], &ShardConfig::default())
            .await;
        tokio::time::timeout(std::time::Duration::from_secs(5), reply)
            .await
            .unwrap()
            .unwrap()
    }

    /// One queue op on `SEGMENT` as its own group; its reply.
    async fn queue(&self, op: QueueOp) -> Result<QueueOut, String> {
        let (op, reply) = queue_op(op);
        self.engine
            .commit_group(vec![op], &ShardConfig::default())
            .await;
        tokio::time::timeout(std::time::Duration::from_secs(5), reply)
            .await
            .unwrap()
            .unwrap()
    }

    /// `consumer`'s record as a config read answers it.
    async fn config(&self, consumer: &str) -> Option<ConsumerRecord> {
        match self
            .queue(QueueOp::ConfigGet {
                consumer: consumer.into(),
            })
            .await
        {
            Ok(QueueOut::Config { rec, .. }) => rec,
            other => panic!("a config read answers its record: {other:?}"),
        }
    }

    /// The stored billing row of `SEGMENT`.
    async fn billing_row(&self) -> crate::billing::SegmentBillingMetaV1 {
        let raw = self
            .db
            .get(crate::billing::billing_meta_key(&SEGMENT))
            .await
            .unwrap()
            .unwrap();
        serde_json::from_slice(&raw).unwrap()
    }

    async fn stop(self) {
        self.engine.begin_close();
        self.engine
            .await_terminated(std::time::Duration::from_secs(5))
            .await
            .unwrap();
    }
}

type AppendReply = oneshot::Receiver<Result<AppendAck, AppendErr>>;

/// A billed append of `payload` to `SEGMENT`, and its reply.
fn append_op(payload: &'static [u8]) -> (CommitOp, AppendReply) {
    let (resp, reply) = oneshot::channel();
    let request = AppendReq {
        hash: SEGMENT,
        route: [9; 16],
        enqueued_at: std::time::Instant::now(),
        entries: vec![Bytes::from_static(payload)],
        routing_key: "lane".into(),
        key_hash: [7; 16],
        producer_lineage: vec![],
        key_version: 1,
        subkey: [1; 32],
        ts_hint_ms: Some(77),
        seq: None,
        bytes: payload.len(),
        finish: AppendFinish::Open,
        billing: Some(Arc::new(crate::billing::BillingRef {
            identity: crate::billing::BillingIdentity {
                account_id: "acct".into(),
                project_id: "proj".into(),
                stream_id: "epoch-1".into(),
                stream_name: "orders".into(),
            },
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

/// A queue op on `SEGMENT`, and its reply.
fn queue_op(op: QueueOp) -> (CommitOp, oneshot::Receiver<Result<QueueOut, String>>) {
    let (resp, reply) = oneshot::channel();
    (
        CommitOp::Queue {
            hash: SEGMENT,
            op,
            resp,
        },
        reply,
    )
}

/// A valid stored billing row of `SEGMENT`: five records, version 8.
fn planted_row() -> crate::billing::SegmentBillingMetaV1 {
    crate::billing::SegmentBillingMetaV1 {
        v: 1,
        account_id: "acct".into(),
        project_id: "proj".into(),
        stream_id: "epoch-1".into(),
        stream_name: "orders".into(),
        usage_version: 8,
        ingest_payload_bytes_total: 35,
        ingest_records_total: 5,
        ..Default::default()
    }
}

fn config() -> ConsumerConfig {
    ConsumerConfig {
        visibility_timeout_ms: 30_000,
        max_attempts: 5,
        dead_letter_stream: None,
        dead_letter_epoch: None,
        max_batch_records: 10,
    }
}

/// The second billed group never reads the row the first one wrote, nor
/// does a group between them that bills nothing let go of it: an unreadable
/// stored row does not refuse the second, and the row it writes carries both
/// appends' totals.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_group_bills_on_the_row_its_predecessor_left_on_the_handle() {
    let rig = Rig::start("resident-billing").await;
    assert_eq!(rig.append(b"payload").await.unwrap().next_offset, 1);
    let first = rig.billing_row().await;
    assert_eq!(
        (first.ingest_records_total, first.usage_version),
        (1, 1),
        "the first group wrote the row"
    );
    assert_eq!(rig.config("x").await, None, "a group that bills nothing");
    rig.db
        .put(
            crate::billing::billing_meta_key(&SEGMENT),
            b"unreadable".to_vec(),
        )
        .await
        .unwrap();
    assert_eq!(rig.append(b"payload").await.unwrap().next_offset, 2);
    let second = rig.billing_row().await;
    assert_eq!(
        (
            second.ingest_records_total,
            second.ingest_payload_bytes_total,
            second.month_ingest_records,
            second.usage_version,
            second.stream_id.as_str(),
        ),
        (2, 14, 2, 2, "epoch-1")
    );
    rig.stop().await;
}

/// A group refused before its write leaves the handle's rows as the last
/// written group left them: the next group bills two appends, not three,
/// and the consumer the refused group created does not exist.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_refused_group_leaves_no_billing_row_on_the_handle() {
    let rig = Rig::start("resident-billing-refused").await;
    rig.append(b"payload").await.unwrap();
    assert_eq!(
        rig.engine
            .evict_idle_handles(std::time::Duration::from_secs(600), 0),
        0,
        "a handle a group just used is not idle"
    );
    assert_eq!(rig.engine.resident_streams(), 1);
    rig.engine.fail_next_group_for(SEGMENT);
    let (append, appended) = append_op(b"payload");
    let (put, created) = queue_op(QueueOp::ConfigPut {
        consumer: "c".into(),
        cfg: config(),
    });
    rig.engine
        .commit_group(vec![append, put], &ShardConfig::default())
        .await;
    assert!(appended.await.unwrap().is_err(), "the group is refused");
    assert!(created.await.unwrap().is_err(), "with all its ops");
    assert_eq!(rig.billing_row().await.ingest_records_total, 1);
    assert_eq!(rig.config("c").await, None, "the refused put is not held");
    assert_eq!(rig.append(b"payload").await.unwrap().next_offset, 2);
    let row = rig.billing_row().await;
    assert_eq!((row.ingest_records_total, row.usage_version), (2, 2));
    rig.stop().await;
}

/// A record a config read loaded, and one a group wrote, are answered from
/// the handle afterwards; a lifecycle change replaces the held record, and
/// a deleted one is let go of.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_consumer_record_stays_on_the_handle_after_its_group() {
    let rig = Rig::start("resident-config").await;
    let stored = ConsumerRecord {
        generation: 3,
        state: ConsumerLifecycle::Active,
        config: config(),
    };
    let key = crate::queue::config_key(&SEGMENT, "loaded");
    rig.db
        .put(&key, serde_json::to_vec(&stored).unwrap())
        .await
        .unwrap();
    assert_eq!(rig.config("loaded").await, Some(stored.clone()));
    rig.db.delete(&key).await.unwrap();
    assert_eq!(
        rig.config("loaded").await,
        Some(stored),
        "the loaded record is held"
    );
    let created = match rig
        .queue(QueueOp::ConfigPut {
            consumer: "c".into(),
            cfg: config(),
        })
        .await
    {
        Ok(QueueOut::Config {
            rec: Some(rec),
            created: true,
            ..
        }) => rec,
        other => panic!("the put creates generation 1: {other:?}"),
    };
    let key = crate::queue::config_key(&SEGMENT, "c");
    rig.db.delete(&key).await.unwrap();
    assert_eq!(
        rig.config("c").await,
        Some(created.clone()),
        "written, held"
    );
    rig.queue(QueueOp::ConfigLifecycle {
        consumer: "c".into(),
        expect_gen: 1,
        deleting: true,
    })
    .await
    .unwrap();
    let deleting = ConsumerRecord {
        state: ConsumerLifecycle::Deleting,
        ..created
    };
    assert_eq!(rig.config("c").await, Some(deleting.clone()));
    let raw = rig.db.get(&key).await.unwrap().unwrap();
    assert_eq!(
        serde_json::from_slice::<ConsumerRecord>(&raw).unwrap(),
        deleting
    );
    rig.queue(QueueOp::ConfigLifecycle {
        consumer: "c".into(),
        expect_gen: 1,
        deleting: false,
    })
    .await
    .unwrap();
    rig.db.delete(&key).await.unwrap();
    assert_eq!(rig.config("c").await, None, "a deleted record is not held");
    rig.stop().await;
}

/// A consumer with no record is not held: a record that appears in the store
/// afterwards is read.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_consumer_without_a_record_is_read_again() {
    let rig = Rig::start("resident-config-absent").await;
    assert_eq!(rig.config("c").await, None);
    let stored = ConsumerRecord {
        generation: 1,
        state: ConsumerLifecycle::Active,
        config: config(),
    };
    rig.db
        .put(
            crate::queue::config_key(&SEGMENT, "c"),
            serde_json::to_vec(&stored).unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(rig.config("c").await, Some(stored));
    rig.stop().await;
}

/// A group that writes nothing still leaves the row it read on the handle.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_group_that_writes_nothing_leaves_its_billing_row_on_the_handle() {
    let rig = Rig::start("resident-billing-no-write").await;
    let key = crate::billing::billing_meta_key(&SEGMENT);
    rig.db
        .put(&key, serde_json::to_vec(&planted_row()).unwrap())
        .await
        .unwrap();
    rig.engine
        .commit_group(
            vec![CommitOp::UsageAck {
                hash: SEGMENT,
                scope: UsageAckScope::ThroughVersion(7),
                month_final_keys: vec![],
            }],
            &ShardConfig::default(),
        )
        .await;
    assert_eq!(rig.billing_row().await.usage_version, 8, "nothing written");
    rig.db.put(&key, b"unreadable".to_vec()).await.unwrap();
    rig.append(b"payload").await.unwrap();
    let row = rig.billing_row().await;
    assert_eq!((row.ingest_records_total, row.usage_version), (6, 9));
    rig.stop().await;
}

/// A resident handle that holds no billing row reads the store's.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_resident_handle_without_a_billing_row_reads_the_store() {
    let rig = Rig::start("resident-billing-unheld").await;
    assert_eq!(rig.config("c").await, None);
    assert_eq!(rig.engine.resident_streams(), 1);
    rig.db
        .put(
            crate::billing::billing_meta_key(&SEGMENT),
            serde_json::to_vec(&planted_row()).unwrap(),
        )
        .await
        .unwrap();
    rig.append(b"payload").await.unwrap();
    let row = rig.billing_row().await;
    assert_eq!(
        (
            row.ingest_records_total,
            row.ingest_payload_bytes_total,
            row.usage_version
        ),
        (6, 42, 9)
    );
    rig.stop().await;
}
