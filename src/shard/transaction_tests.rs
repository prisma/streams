#![cfg(test)]
//! R03-A: every row and batch-dependent effect of one mixed transaction.
use super::*;
use crate::queue::{ConsumerConfig, ConsumerLifecycle, ConsumerRecord, QueueOp, QueueOut};
use std::{collections::BTreeMap, time::Duration};
const HASH: [u8; 16] = [103; 16];
const FINAL: [u8; 16] = [104; 16];
const KEY: [u8; 16] = [7; 16];
type Reply = oneshot::Receiver<Result<AppendAck, AppendErr>>;
type QueueReply = oneshot::Receiver<Result<QueueOut, String>>;

struct Fixture {
    engine: Arc<ShardEngine>,
    cfg: ShardConfig,
    usage: Arc<crate::usage::Counters>,
    journal: Arc<crate::touch::TouchJournal>,
    signals: mpsc::Receiver<AbsorbSignal>,
}
impl Fixture {
    async fn new() -> Self {
        let store: Arc<dyn object_store::ObjectStore> =
            Arc::new(object_store::memory::InMemory::new());
        let db = Arc::new(
            Db::builder("r03a-mixed", store.clone())
                .build()
                .await
                .unwrap(),
        );
        let (tx, signals) = mpsc::channel(16);
        let cfg = ShardConfig {
            tail_ring_bytes: 1 << 20,
            ..Default::default()
        };
        let engine = ShardEngine::start(
            "r03a-mixed".into(),
            db,
            store,
            cfg.clone(),
            tx,
            None,
            Default::default(),
        );
        let journal = crate::touch::TouchJournal::start(&crate::runtime::OsEntropy);
        Self {
            engine,
            cfg,
            usage: Arc::new(Default::default()),
            journal,
            signals,
        }
    }
    fn append(&self, hash: [u8; 16], sequence: u64, finish: AppendFinish) -> (CommitOp, Reply) {
        let (resp, reply) = oneshot::channel();
        (
            CommitOp::Append(AppendReq {
                hash,
                route: [9; 16],
                enqueued_at: std::time::Instant::now(),
                entries: vec![Bytes::from_static(b"payload")],
                routing_key: "lane".into(),
                key_hash: KEY,
                producer_lineage: vec![],
                key_version: 1,
                subkey: [1; 32],
                ts_hint_ms: Some(77),
                seq: Some("alpha".into()),
                bytes: 7,
                finish,
                billing: None,
                seal_gen: Some(10),
                producer: Some(ProducerReq {
                    id: "writer".into(),
                    epoch: 1,
                    seq: sequence,
                    request_hash: Some([5; 16]),
                }),
                deferred_error: None,
                sealed_reject_new: None,
                touch: Some(TouchFeed {
                    journal: self.journal.clone(),
                    key_ids: vec![3],
                    next_offset: 0,
                }),
                usage: self.usage.clone(),
                resp,
            }),
            reply,
        )
    }
    fn group(&self, final_key: Vec<u8>) -> (Vec<CommitOp>, Vec<Reply>, Vec<QueueReply>) {
        let (append, reply) = self.append(HASH, 0, AppendFinish::Open);
        let (last, last_reply) = self.append(FINAL, 0, AppendFinish::Close);
        let (duplicate, dup_reply) = self.append(HASH, 0, AppendFinish::Close);
        let (refused, refused_reply) = self.append(HASH, 1, AppendFinish::Open);
        let (close, closed) = oneshot::channel();
        let (fence, fenced) = oneshot::channel();
        let (create, created) = oneshot::channel();
        let (conflict, conflicted) = oneshot::channel();
        (
            vec![
                append,
                CommitOp::Close(CloseReq {
                    hash: HASH,
                    generation: Some(10),
                    resp: close,
                }),
                CommitOp::SealFence(SealFenceReq {
                    hash: HASH,
                    generation: 8,
                    resp: fence,
                }),
                duplicate,
                refused,
                last,
                CommitOp::Queue {
                    hash: HASH,
                    op: QueueOp::ConfigPut {
                        consumer: "c".into(),
                        cfg: Default::default(),
                    },
                    resp: create,
                },
                CommitOp::Queue {
                    hash: HASH,
                    op: QueueOp::ConfigLifecycle {
                        consumer: "c".into(),
                        expect_gen: 2,
                        deleting: true,
                    },
                    resp: conflict,
                },
                CommitOp::UsageAck {
                    hash: HASH,
                    scope: UsageAckScope::ThroughVersion(7),
                    month_final_keys: vec![final_key],
                },
            ],
            vec![reply, closed, fenced, dup_reply, refused_reply, last_reply],
            vec![created, conflicted],
        )
    }
    async fn rows(&self) -> BTreeMap<Vec<u8>, Vec<u8>> {
        let mut out = BTreeMap::new();
        let mut rows = self.engine.db.scan(..).await.unwrap();
        while let Some(row) = rows.next().await.unwrap() {
            out.insert(row.key.to_vec(), row.value.to_vec());
        }
        out
    }
    async fn assert_quiet(&mut self) {
        assert_eq!(self.usage.plaintext_bytes.load(Ordering::SeqCst), 0);
        assert_eq!(self.usage.frame_bytes.load(Ordering::SeqCst), 0);
        assert!(matches!(
            self.journal
                .wait(
                    &format!("{}:0", self.journal.epoch),
                    vec![3],
                    Duration::ZERO
                )
                .await,
            crate::touch::WaitOutcome::Timeout { end_offset: 0, .. }
        ));
        assert!(self.signals.try_recv().is_err());
        for hash in [HASH, FINAL] {
            let handle = self.engine.stream_handle(hash).await.unwrap();
            assert_eq!(handle.state.lock().unwrap().durable.next, 0);
            assert_eq!(handle.ring.lock().unwrap().bytes, 0);
        }
    }
    async fn assert_rows(
        &self,
        mut expected: BTreeMap<Vec<u8>, Vec<u8>>,
        final_key: &[u8],
        start: i64,
    ) -> u64 {
        expected.remove(final_key);
        let actual = self.rows().await;
        let mut total = 0;
        for hash in [HASH, FINAL] {
            let key = record_key(&hash, 0);
            let frame = actual[&key].clone();
            let decoded = crate::crypto::decode_frame(&frame).unwrap();
            let plaintext =
                crate::crypto::decrypt_frame(&[1; 32], &hash, &decoded, &frame).unwrap();
            let header = &decoded.header;
            assert_eq!(
                (header.offset, header.ts_ms, header.routing_key),
                (0, 77, "lane")
            );
            assert_eq!(plaintext, b"payload");
            let bytes = frame.len() as u64;
            total += bytes;
            expected.insert(key, frame);
            let tail = TailFields {
                next: 1,
                logical: 7,
                ts: 77,
                closed: true,
                seq: Some("alpha".into()),
                route: [9; 16],
                unabsorbed_bytes: bytes,
                ..Default::default()
            };
            expected.insert(tail_key(&hash), encode_tail(&tail));
            expected.insert(
                dirty_key(&hash),
                dirty_value(&StreamMaintenance {
                    next: 1,
                    unabsorbed_bytes: bytes,
                    ..Default::default()
                })
                .to_vec(),
            );
            expected.insert(seq_key(&hash, &KEY), b"alpha".to_vec());
            let mut producer = Vec::new();
            for value in [1u64, 0, 0] {
                producer.extend(value.to_le_bytes());
            }
            producer.extend([5; 16]);
            expected.insert(producer_key(&hash, &KEY, "writer"), producer);
        }
        expected.insert(
            crate::queue::config_key(&HASH, "c"),
            serde_json::to_vec(&ConsumerRecord {
                generation: 1,
                state: ConsumerLifecycle::Active,
                config: ConsumerConfig::default(),
            })
            .unwrap(),
        );
        let maintenance = self.engine.maintenance_snapshot();
        assert_eq!(maintenance.unabsorbed_frame_bytes, total);
        assert!((start..=now_ms()).contains(&maintenance.backlog_started_ms));
        assert!((start..=now_ms()).contains(&maintenance.last_progress_ms));
        expected.insert(
            shard_maint_key().to_vec(),
            encode_shard_maint(&maintenance).to_vec(),
        );
        assert_eq!(
            actual, expected,
            "complete key/value set, including unchanged accounting and absence of extra writes"
        );
        total
    }
}

#[expect(
    clippy::too_many_lines,
    reason = "r03a_mixed_transaction_preserves_every_row_reply_and_publication; the fixture stages every op kind in one group so their replies and publication are judged together; splitting it would separate the ops from the group they share"
)]
#[expect(
    clippy::excessive_nesting,
    reason = "r03a_mixed_transaction_preserves_every_row_reply_and_publication; the fixture nests the per-reply and per-stream assertions inside the retired and applied phases it stages; flattening them would separate the assertions from the phase they judge"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn r03a_mixed_transaction_preserves_every_row_reply_and_publication() {
    let mut fixture = Fixture::new().await;
    let final_key = crate::billing::usage_month_final_key(&HASH, 2026, 7);
    let mut seed = WriteBatch::new();
    seed.put(
        crate::billing::billing_meta_key(&HASH),
        serde_json::to_vec(&crate::billing::SegmentBillingMetaV1 {
            v: 1,
            stream_id: "owed".into(),
            usage_version: 8,
            ..Default::default()
        })
        .unwrap(),
    );
    seed.put(crate::billing::usage_dirty_key(&HASH), 8u64.to_le_bytes());
    seed.put(&final_key, b"owed snapshot");
    seed.put(b"unrelated", b"untouched");
    fixture.engine.db.write(seed).await.unwrap();
    let before = fixture.rows().await;
    for failure in [Some("required-read"), Some("write"), None] {
        let (ops, mut replies, mut queue) = fixture.group(final_key.clone());
        if failure == Some("required-read") {
            billing_read_faults()
                .lock()
                .unwrap()
                .insert("r03a-mixed".into(), ());
        }
        if failure == Some("write") {
            fixture.engine.fail_next_group_for(HASH);
        }
        let engine = fixture.engine.clone();
        let dispatch = engine.test_hold_dispatch().await;
        let start = now_ms();
        fixture.engine.commit_group(ops, &fixture.cfg).await;
        fixture.assert_quiet().await;
        if let Some(failure) = failure {
            assert_eq!(
                fixture.rows().await,
                before,
                "failed {failure} changes no stored row"
            );
            assert!(
                fixture
                    .engine
                    .in_flight
                    .lock()
                    .unwrap()
                    .pending()
                    .is_empty()
            );
            for reply in replies {
                assert!(matches!(reply.await.unwrap(), Err(AppendErr::Internal(_))));
            }
            for reply in queue {
                assert!(reply.await.unwrap().is_err());
            }
            for hash in [HASH, FINAL] {
                let handle = fixture.engine.stream_handle(hash).await.unwrap();
                let state = handle.state.lock().unwrap();
                assert_eq!(state.applied.next, 0);
                assert!(!state.applied.closed);
                assert!(state.producers.is_empty());
                assert!(state.seqs.is_empty());
                assert!(state.queue.consumers.is_empty());
            }
            assert_eq!(
                engine.seal_fences.lock().unwrap().get(&HASH).copied(),
                if failure == "required-read" {
                    None
                } else {
                    Some(8)
                },
                "only a staged fence may conservatively survive failure"
            );
            assert_eq!(
                fixture.engine.maintenance_snapshot(),
                ShardMaintenance::default()
            );
            drop(dispatch);
            continue;
        }
        let total = fixture.assert_rows(before.clone(), &final_key, start).await;
        for reply in &mut replies {
            assert!(matches!(
                reply.try_recv(),
                Err(oneshot::error::TryRecvError::Empty)
            ));
        }
        for reply in &mut queue {
            assert!(matches!(
                reply.try_recv(),
                Err(oneshot::error::TryRecvError::Empty)
            ));
        }
        {
            let inflight = fixture.engine.in_flight.lock().unwrap();
            let effects = &inflight.pending().last().unwrap().effects;
            assert_eq!(
                (
                    effects.acks.len(),
                    effects.queue_acks.len(),
                    effects.tails.len(),
                    effects.ring_pub.len(),
                    effects.signals.len(),
                    effects.touches.len(),
                    effects.usage.len()
                ),
                (6, 2, 2, 2, 2, 2, 2)
            );
        }
        drop(dispatch);
        for (index, reply) in replies.into_iter().enumerate() {
            let result = tokio::time::timeout(Duration::from_secs(5), reply)
                .await
                .unwrap()
                .unwrap();
            if index == 4 {
                assert!(matches!(result, Err(AppendErr::Closed { next_offset: 1 })));
            } else {
                let ack = result.unwrap();
                assert_eq!(
                    (ack.last_offset, ack.next_offset, ack.duplicate),
                    (0, 1, index == 3)
                );
                assert_eq!(ack.closed, index != 0);
            }
        }
        let conflict = queue.pop().unwrap().await.unwrap().unwrap_err();
        assert!(conflict.starts_with("consumer_generation_conflict"));
        assert!(matches!(
            queue.pop().unwrap().await.unwrap().unwrap(),
            QueueOut::Config {
                created: true,
                conflict: false,
                ..
            }
        ));
        let _dispatch_finished = engine.test_hold_dispatch().await;
        assert_eq!(fixture.usage.plaintext_bytes.load(Ordering::SeqCst), 14);
        assert_eq!(fixture.usage.frame_bytes.load(Ordering::SeqCst), total);
        assert!(matches!(
            fixture
                .journal
                .wait(
                    &format!("{}:0", fixture.journal.epoch),
                    vec![3],
                    Duration::from_secs(1)
                )
                .await,
            crate::touch::WaitOutcome::Touched { end_offset: 1, .. }
        ));
        let mut signals = BTreeMap::new();
        for _ in 0..2 {
            let signal = fixture.signals.try_recv().unwrap();
            signals.insert(signal.hash, signal.appended_bytes);
        }
        assert_eq!(signals.len(), 2);
        assert_eq!(signals.values().sum::<u64>(), total);
        assert!(fixture.signals.try_recv().is_err());
        for hash in [HASH, FINAL] {
            let handle = fixture.engine.stream_handle(hash).await.unwrap();
            let state = handle.state.lock().unwrap();
            assert_eq!((state.durable.next, state.durable.closed), (1, true));
            assert_eq!(state.producers[&(KEY, "writer".into())], (1, 0, 0, [5; 16]));
            assert_eq!(state.seqs[&KEY], "alpha");
            let ring = handle.ring.lock().unwrap();
            assert_eq!(ring.batches.len(), 1);
            assert_eq!(ring.batches[0].frames[0].0, 0);
        }
    }
    fixture.journal.close();
    fixture.engine.begin_close();
    fixture
        .engine
        .await_terminated(Duration::from_secs(5))
        .await
        .unwrap();
}
