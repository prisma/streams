//! A stream's billing meta and consumer records stay on its resident
//! handle once a commit group read or staged them: the next group takes
//! them from there, not from the store (the billing meta on the applied
//! tail, layout 6 f3; the records in `HotRows`). A group refused before its
//! write leaves nothing behind.
#![cfg(test)]
use super::*;
use crate::queue::{ConsumerConfig, ConsumerLifecycle, ConsumerRecord, QueueOp, QueueOut};

const SEGMENT: [u8; 16] = [0x5e; 16];

struct Rig {
    engine: Arc<ShardEngine>,
    db: Arc<Db>,
}

impl Rig {
    async fn start(prefix: &str) -> Self {
        let store: Arc<dyn object_store::ObjectStore> =
            Arc::new(object_store::memory::InMemory::new());
        let db = Arc::new(
            crate::keyspace::db_builder(prefix, store.clone())
                .build()
                .await
                .unwrap(),
        );
        let maintenance = load_or_rebuild_maintenance(&db).await.unwrap();
        let engine = ShardEngine::start(
            prefix.into(),
            db.clone(),
            store,
            ShardConfig::default(),
            None,
            maintenance,
        );
        Self { engine, db }
    }

    /// One billed append of `payload` to `SEGMENT` as its own group; its reply.
    async fn append(&self, payload: &'static [u8]) -> Result<AppendAck, AppendErr> {
        let (append, reply) = append_op(payload);
        self.engine.commit_group(vec![append]).await;
        tokio::time::timeout(std::time::Duration::from_secs(5), reply)
            .await
            .unwrap()
            .unwrap()
    }

    /// One queue op on `SEGMENT` as its own group; its reply.
    async fn queue(&self, op: QueueOp) -> Result<QueueOut, String> {
        let (op, reply) = queue_op(op);
        self.engine.commit_group(vec![op]).await;
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

    /// The stored billing meta of `hash`: its static row with the usage
    /// counters its tail carries (layout 6, f3).
    async fn billing_of(&self, hash: &[u8; 16]) -> crate::billing::SegmentBillingMetaV1 {
        super::test_support::stored_billing(&self.db, hash)
            .await
            .expect("a stored billing row")
    }

    /// The stored billing meta of `SEGMENT`.
    async fn billing_row(&self) -> crate::billing::SegmentBillingMetaV1 {
        self.billing_of(&SEGMENT).await
    }

    /// How many static billing rows the engine read from the store.
    fn static_reads(&self) -> u64 {
        super::test_support::billing_row_io_on(&self.db).1
    }

    /// Store `planted_row()` as `hash`'s billing meta, before any engine
    /// group touches the stream: its static row and a tail carrying its
    /// counters.
    async fn plant(&self, hash: &[u8; 16]) {
        let mut batch = WriteBatch::new();
        super::test_support::seed_billing(&mut batch, hash, &planted_row(), &TailFields::default());
        self.db.write(batch).await.unwrap();
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
    append_op_to(SEGMENT, payload)
}

/// A billed append of `payload` to `hash`, and its reply.
fn append_op_to(hash: [u8; 16], payload: &'static [u8]) -> (CommitOp, AppendReply) {
    let (resp, reply) = oneshot::channel();
    let request = AppendReq {
        hash,
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

/// The second billed group never reads the row the first one staged, nor
/// does a group between them that bills nothing let go of it: the engine
/// reads the stream's static row from the store once, and the second
/// group's counters carry both appends' totals.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_group_bills_on_the_row_its_predecessor_left_on_the_handle() {
    let rig = Rig::start("resident-billing").await;
    assert_eq!(rig.append(b"payload").await.unwrap().next_offset, 1);
    let first = rig.billing_row().await;
    assert_eq!(
        (first.ingest_records_total, first.usage_version),
        (1, 1),
        "the first group staged the row"
    );
    let reads = rig.static_reads();
    assert_eq!(rig.config("x").await, None, "a group that bills nothing");
    assert_eq!(rig.append(b"payload").await.unwrap().next_offset, 2);
    assert_eq!(rig.static_reads(), reads, "the second group read no row");
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
    rig.engine.commit_group(vec![append, put]).await;
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

/// A group that writes nothing still leaves the row it read on the handle:
/// the next group reads no row and bills on the planted totals.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_group_that_writes_nothing_leaves_its_billing_row_on_the_handle() {
    let rig = Rig::start("resident-billing-no-write").await;
    rig.plant(&SEGMENT).await;
    rig.engine
        .commit_group(vec![CommitOp::UsageAck {
            hash: SEGMENT,
            scope: UsageAckScope::ThroughVersion(7),
            month_final_keys: vec![],
        }])
        .await;
    assert_eq!(rig.billing_row().await.usage_version, 8, "nothing written");
    let reads = rig.static_reads();
    assert_eq!(reads, 1, "the acknowledgement read the row");
    rig.append(b"payload").await.unwrap();
    assert_eq!(rig.static_reads(), reads, "the append read no row");
    let row = rig.billing_row().await;
    assert_eq!((row.ingest_records_total, row.usage_version), (6, 9));
    rig.stop().await;
}

/// A handle made resident by a group that bills nothing holds no billing
/// meta: the first billed group reads the stream's static row, once, and
/// bills from it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_resident_handle_without_a_billing_row_reads_the_store() {
    let rig = Rig::start("resident-billing-unheld").await;
    rig.plant(&SEGMENT).await;
    assert_eq!(rig.config("c").await, None);
    assert_eq!(rig.engine.resident_streams(), 1);
    assert_eq!(rig.static_reads(), 0, "the config read read no billing row");
    rig.append(b"payload").await.unwrap();
    rig.append(b"payload").await.unwrap();
    assert_eq!(rig.static_reads(), 1, "read once, then held");
    let row = rig.billing_row().await;
    assert_eq!(
        (
            row.ingest_records_total,
            row.ingest_payload_bytes_total,
            row.usage_version
        ),
        (7, 49, 10)
    );
    rig.stop().await;
}

/// A group of cold streams reads their billing rows together, not one
/// after another on the serial commit path: with every read held open,
/// all eight are in flight at once, and each stream bills on its own row.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_group_reads_its_cold_streams_billing_rows_together() {
    const STREAMS: u8 = 8;
    let rig = Rig::start("resident-cold").await;
    let hashes: Vec<[u8; 16]> = (0..STREAMS).map(|n| [0x60 + n; 16]).collect();
    for hash in &hashes {
        rig.plant(hash).await;
    }
    let held = super::test_support::hold_billing_reads("resident-cold");
    let (ops, replies): (Vec<_>, Vec<_>) = hashes
        .iter()
        .map(|hash| append_op_to(*hash, b"payload"))
        .unzip();
    let group = rig.engine.commit_group(ops);
    let watch = async {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
        while held.in_flight() < usize::from(STREAMS) && std::time::Instant::now() < deadline {
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
        let together = held.most_in_flight();
        held.release();
        together
    };
    let ((), together) = tokio::join!(group, watch);
    assert_eq!(
        together,
        usize::from(STREAMS),
        "every cold row in flight at once"
    );
    assert_eq!(
        rig.engine.resident_streams(),
        usize::from(STREAMS),
        "each cold stream's handle is resident"
    );
    for reply in replies {
        assert_eq!(reply.await.unwrap().unwrap().next_offset, 1);
    }
    for hash in &hashes {
        let row = rig.billing_of(hash).await;
        assert_eq!((row.ingest_records_total, row.usage_version), (6, 9));
    }
    rig.stop().await;
}

/// A group reads no billing row for a stream it does not bill: with every
/// read held open, an unbilled append and a config read still complete.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_group_reads_no_billing_row_for_a_stream_it_does_not_bill() {
    let rig = Rig::start("resident-unbilled").await;
    let held = super::test_support::hold_billing_reads("resident-unbilled");
    let (mut append, appended) = append_op(b"payload");
    if let CommitOp::Append(request) = &mut append {
        request.billing = None;
    }
    let (get, got) = queue_op(QueueOp::ConfigGet {
        consumer: "c".into(),
    });
    let group = rig.engine.commit_group(vec![append, get]);
    tokio::time::timeout(std::time::Duration::from_secs(2), group)
        .await
        .expect("no billing read held the group");
    assert_eq!(appended.await.unwrap().unwrap().next_offset, 1);
    assert!(matches!(
        got.await.unwrap(),
        Ok(QueueOut::Config { rec: None, .. })
    ));
    assert_eq!(held.most_in_flight(), 0);
    held.release();
    rig.stop().await;
}

/// A group's cold consumer-group streams load their queue state and the
/// records of the consumers their ops name together, each once: with every
/// queue scan and record read held open, all eight streams' are in flight
/// at once, two config reads of one consumer on a stream read its record
/// once, and each answers the stored record.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_group_loads_its_cold_streams_queue_state_together() {
    const STREAMS: u8 = 8;
    let rig = Rig::start("resident-cold-queue").await;
    let hashes: Vec<[u8; 16]> = (0..STREAMS).map(|n| [0x70 + n; 16]).collect();
    let stored = ConsumerRecord {
        generation: 1,
        state: crate::queue::ConsumerLifecycle::Active,
        config: config(),
    };
    for hash in &hashes {
        rig.db
            .put(
                crate::queue::config_key(hash, "c"),
                serde_json::to_vec(&stored).unwrap(),
            )
            .await
            .unwrap();
    }
    let scans = super::test_support::hold_queue_reads("resident-cold-queue");
    let records = super::test_support::hold_config_reads("resident-cold-queue");
    let (ops, replies): (Vec<_>, Vec<_>) = hashes
        .iter()
        .flat_map(|hash| [*hash, *hash])
        .map(|hash| {
            let (resp, reply) = oneshot::channel();
            let op = CommitOp::Queue {
                hash,
                op: QueueOp::ConfigGet {
                    consumer: "c".into(),
                },
                resp,
            };
            (op, reply)
        })
        .unzip();
    let group = rig.engine.commit_group(ops);
    let watch = async {
        let mut together = Vec::new();
        for held in [&scans, &records] {
            together.push(released_at(held, usize::from(STREAMS)).await);
        }
        together
    };
    let ((), together) = tokio::join!(group, watch);
    let streams = usize::from(STREAMS);
    assert_eq!(
        together,
        [streams, streams],
        "every cold stream's reads in flight at once"
    );
    assert_eq!(
        (scans.total(), records.total()),
        (3 * streams, streams),
        "three scans and one record read a stream"
    );
    for reply in replies {
        assert!(matches!(
            reply.await.unwrap(),
            Ok(QueueOut::Config { rec: Some(rec), .. }) if rec == stored
        ));
    }
    rig.stop().await;
}

/// Once `n` reads are held, or after 2 s, the most held at once; then every
/// held read goes through.
async fn released_at(held: &super::test_support::HeldReads, n: usize) -> usize {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
    while held.in_flight() < n && std::time::Instant::now() < deadline {
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    }
    let most = held.most_in_flight();
    held.release();
    most
}
