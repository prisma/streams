#![cfg(test)]
//! R17-B: stop a real transaction after staging/write, with its WAL held.
use super::*;
use crate::queue::QueueOp;
use slatedb::config::{DurabilityLevel, ReadOptions};
use std::time::Duration;
use tokio::sync::Semaphore;

const HASH: [u8; 16] = [117; 16];
const SECOND: [u8; 16] = [118; 16];
type Reply = oneshot::Receiver<Result<AppendAck, AppendErr>>;

#[derive(Clone, Copy, PartialEq)]
pub(super) enum CompletionPhase {
    NoWrite,
    Written,
    Durable,
}
pub(super) struct CompletionPause {
    phase: CompletionPhase,
    entered: Semaphore,
    resume: Semaphore,
}
impl CompletionPause {
    async fn entered(&self) {
        tokio::time::timeout(Duration::from_secs(10), self.entered.acquire())
            .await
            .unwrap()
            .unwrap()
            .forget();
    }
    fn release(&self) {
        self.resume.add_permits(1);
    }
}
impl ShardEngine {
    fn pause_completion(&self, phase: CompletionPhase) -> Arc<CompletionPause> {
        let pause = Arc::new(CompletionPause {
            phase,
            entered: Semaphore::new(0),
            resume: Semaphore::new(0),
        });
        *self.completion_pause.lock().unwrap() = Some(pause.clone());
        pause
    }
    pub(super) async fn completion_checkpoint(&self, phase: CompletionPhase) {
        let pause = {
            let mut slot = self.completion_pause.lock().unwrap();
            if slot.as_ref().is_some_and(|p| p.phase == phase) {
                slot.take()
            } else {
                None
            }
        };
        if let Some(pause) = pause {
            pause.entered.add_permits(1);
            pause.resume.acquire().await.unwrap().forget();
        }
    }
}

struct Fixture {
    engine: Arc<ShardEngine>,
    store: Arc<crate::dst::FaultStore>,
    usage: Arc<crate::usage::Counters>,
    journal: Arc<crate::touch::TouchJournal>,
    signals: mpsc::Receiver<AbsorbSignal>,
}
impl Fixture {
    async fn new(name: &str) -> Self {
        let store = crate::dst::FaultStore::new(
            Arc::new(object_store::memory::InMemory::new()),
            1718,
            crate::dst::FaultProfile::clean(),
        );
        let db = Arc::new(
            Db::builder(name, store.clone())
                .with_settings(slatedb::config::Settings {
                    flush_interval: Some(Duration::from_millis(5)),
                    ..Default::default()
                })
                .build()
                .await
                .unwrap(),
        );
        let (tx, signals) = mpsc::channel(16);
        let engine = ShardEngine::start(
            name.into(),
            db,
            store.clone(),
            ShardConfig {
                tail_ring_bytes: 1 << 20,
                wal_group_commit: false,
                ..Default::default()
            },
            tx,
            None,
            Default::default(),
        );
        Self {
            engine,
            store,
            signals,
            usage: Arc::new(Default::default()),
            journal: crate::touch::TouchJournal::start(&crate::runtime::OsEntropy, &[]),
        }
    }
    fn append(&self) -> (AppendReq, Reply) {
        let (resp, result) = oneshot::channel();
        (
            AppendReq {
                hash: HASH,
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
                billing: None,
                seal_gen: Some(1),
                producer: Some(ProducerReq {
                    id: "writer".into(),
                    epoch: 1,
                    seq: 0,
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
            },
            result,
        )
    }
    async fn remote_missing(&self) {
        assert!(
            self.engine
                .db
                .get_with_options(
                    tail_key(&HASH),
                    &ReadOptions {
                        durability_filter: DurabilityLevel::Remote,
                        ..Default::default()
                    }
                )
                .await
                .unwrap()
                .is_none(),
            "real remote WAL barrier is still held"
        );
    }
    async fn finish(&self) {
        self.store.release_hold();
        self.engine
            .await_terminated(Duration::from_secs(10))
            .await
            .unwrap();
        self.engine
            .await_terminated(Duration::from_secs(1))
            .await
            .unwrap();
    }
}
async fn until(mut condition: impl FnMut() -> bool) {
    tokio::time::timeout(Duration::from_secs(10), async {
        while !condition() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}
fn assert_moved(result: Result<AppendAck, AppendErr>) {
    let error = result.expect_err("retirement cannot acknowledge applied-only truth");
    assert!(matches!(error, AppendErr::Moved));
    let failure = crate::application::append::AppendFailure::from_commit(0, false, error);
    assert!(
        !failure.definitively_rejected(),
        "late writes have an unknown outcome"
    );
    assert_eq!(
        failure.code,
        crate::application::append::AppendCode::ShardMoving
    );
    let response = crate::http::render_append(Err(failure));
    assert_eq!(
        response.status(),
        axum::http::StatusCode::SERVICE_UNAVAILABLE
    );
    assert_eq!(response.headers()["retry-after"], "1");
    assert!(!response.headers().contains_key("stream-next-offset"));
}

async fn duplicate_order(retire: bool, attach_first: bool) {
    let fixture = Fixture::new(if attach_first {
        "r17b-attached"
    } else {
        "r17b-staged"
    })
    .await;
    let engine = &fixture.engine;
    let handle = engine.stream_handle(HASH).await.unwrap();
    let status = engine.db.subscribe();
    let durable_before = status.borrow().durable_seq;
    let entered = fixture
        .store
        .hold_class(crate::dst::StoreOp::Put, crate::dst::ObjClass::Wal, 1);
    let (request, mut first) = fixture.append();
    engine.try_enqueue(request).unwrap();
    until(|| entered.load(Ordering::SeqCst) > 0 && handle.state.lock().unwrap().applied.next == 1)
        .await;
    fixture.remote_missing().await;
    let pause = engine.pause_completion(CompletionPhase::NoWrite);
    let commit = engine.test_hold_commit().await;
    let (request, mut retry) = fixture.append();
    let (tx, mut config) = oneshot::channel();
    engine.try_enqueue(request).unwrap();
    engine
        .tx
        .try_send(CommitOp::Queue {
            hash: HASH,
            op: QueueOp::ConfigGet {
                consumer: "c".into(),
            },
            resp: tx,
        })
        .unwrap();
    drop(commit);
    pause.entered().await;
    if attach_first {
        pause.release();
        until(|| {
            engine
                .in_flight
                .lock()
                .unwrap()
                .pending()
                .last()
                .is_some_and(|g| g.effects.acks.len() == 2 && g.effects.queue_acks.len() == 1)
        })
        .await;
    }
    assert!(first.try_recv().is_err() && retry.try_recv().is_err() && config.try_recv().is_err());
    if retire {
        engine.begin_close();
    }
    if !attach_first {
        pause.release();
    }
    if !retire {
        until(|| {
            engine
                .in_flight
                .lock()
                .unwrap()
                .pending()
                .last()
                .is_some_and(|g| g.effects.acks.len() == 2)
        })
        .await;
        assert!(matches!(
            retry.try_recv(),
            Err(oneshot::error::TryRecvError::Empty)
        ));
    }
    // Close can already be awaiting its held WAL flush, at which point
    // ordinary Db reads correctly refuse Closed(Clean). The retained status
    // remains observable and proves retirement did not advance durability.
    assert_eq!(status.borrow().durable_seq, durable_before);
    if !retire {
        fixture.remote_missing().await;
        fixture.store.release_hold();
    }
    let first = tokio::time::timeout(Duration::from_secs(1), first).await;
    let retry = tokio::time::timeout(Duration::from_secs(1), retry).await;
    let config = tokio::time::timeout(Duration::from_secs(1), config).await;
    let empty = engine.in_flight.lock().unwrap().pending().is_empty();
    fixture.finish().await;
    let first = first.unwrap().unwrap();
    let retry = retry.unwrap().unwrap();
    let config = config.unwrap().unwrap();
    assert!(empty, "no sender survives its terminal handoff");
    if retire {
        assert_moved(first);
        assert_moved(retry);
        assert!(config.is_err());
    } else {
        assert!(!first.unwrap().duplicate);
        assert!(retry.unwrap().duplicate);
        assert!(config.is_ok());
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn r17b_retirement_before_duplicate_attachment_cannot_erase_remote_dependency() {
    duplicate_order(true, false).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn r17b_retirement_after_duplicate_attachment_owns_every_reply() {
    duplicate_order(true, true).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn r17b_live_duplicate_waits_for_actual_remote_durability() {
    duplicate_order(false, true).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn r17b_durable_dispatch_claim_before_retirement_keeps_its_completion() {
    let fixture = Fixture::new("r17b-durable-claim").await;
    let engine = &fixture.engine;
    let handle = engine.stream_handle(HASH).await.unwrap();
    let pause = engine.pause_completion(CompletionPhase::Durable);
    let entered = fixture
        .store
        .hold_class(crate::dst::StoreOp::Put, crate::dst::ObjClass::Wal, 1);
    let (request, mut result) = fixture.append();
    engine.try_enqueue(request).unwrap();
    until(|| entered.load(Ordering::SeqCst) > 0).await;
    fixture.remote_missing().await;
    fixture.store.release_hold();
    pause.entered().await;
    assert!(matches!(
        result.try_recv(),
        Err(oneshot::error::TryRecvError::Empty)
    ));
    assert!(
        engine
            .db
            .get_with_options(
                tail_key(&HASH),
                &ReadOptions {
                    durability_filter: DurabilityLevel::Remote,
                    ..Default::default()
                }
            )
            .await
            .unwrap()
            .is_some()
    );
    engine.begin_close();
    pause.release();
    let result = tokio::time::timeout(Duration::from_secs(1), result)
        .await
        .unwrap()
        .unwrap();
    fixture.finish().await;
    assert_eq!(result.unwrap().next_offset, 1);
    assert_eq!(handle.state.lock().unwrap().durable.next, 1);
    assert!(handle.ring.lock().unwrap().bytes > 0);
    assert_eq!(fixture.usage.plaintext_bytes.load(Ordering::SeqCst), 7);
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
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn r17b_late_successful_write_settles_without_publishing_retired_effects() {
    let mut fixture = Fixture::new("r17b-late-write").await;
    let engine = fixture.engine.clone();
    let handle = engine.stream_handle(HASH).await.unwrap();
    let secondary = engine.stream_handle(SECOND).await.unwrap();
    let quotas = crate::quota::QuotaRegistry::default();
    let project = crate::tenant::ProjectId::new("r17b-pressure").unwrap();
    drop(
        quotas
            .admit(&project, &Default::default(), now_ms())
            .unwrap(),
    );
    let pressure = quotas.pressure_handle(&project).unwrap();
    handle.bind_pressure(pressure.clone());
    let status = engine.db.subscribe();
    let durable_before = status.borrow().durable_seq;
    let pause = engine.pause_completion(CompletionPhase::Written);
    let entered = fixture
        .store
        .hold_class(crate::dst::StoreOp::Put, crate::dst::ObjClass::Wal, 1);
    let commit = engine.test_hold_commit().await;
    let (mut request, result) = fixture.append();
    request.billing = Some(Arc::new(crate::billing::BillingRef {
        identity: crate::billing::BillingIdentity {
            account_id: "account".into(),
            project_id: "project".into(),
            stream_id: "incarnation".into(),
            stream_name: "stream".into(),
        },
        segment_id: 0,
    }));
    let (config_tx, config) = oneshot::channel();
    let (close_tx, closed) = oneshot::channel();
    engine.try_enqueue(request).unwrap();
    engine
        .tx
        .try_send(CommitOp::Queue {
            hash: HASH,
            op: QueueOp::ConfigPut {
                consumer: "c".into(),
                cfg: Default::default(),
            },
            resp: config_tx,
        })
        .unwrap();
    engine
        .try_close(CloseReq {
            hash: SECOND,
            generation: Some(1),
            resp: close_tx,
        })
        .unwrap();
    drop(commit);
    pause.entered().await;
    until(|| entered.load(Ordering::SeqCst) > 0).await;
    fixture.remote_missing().await;
    assert!(
        engine
            .db
            .get_with_options(
                tail_key(&HASH),
                &ReadOptions {
                    durability_filter: DurabilityLevel::Memory,
                    ..Default::default()
                }
            )
            .await
            .unwrap()
            .is_some()
    );
    let acker = engine.tasks.task_handle_for_test("acker");
    let mut cancelled = Box::pin(engine.await_terminated(Duration::from_secs(30)));
    assert!(futures_util::poll!(cancelled.as_mut()).is_pending());
    drop(cancelled);
    until(|| acker.is_finished()).await;
    pause.release();
    let result = tokio::time::timeout(Duration::from_millis(500), result).await;
    let config = tokio::time::timeout(Duration::from_millis(500), config).await;
    let closed = tokio::time::timeout(Duration::from_millis(500), closed).await;
    engine.await_workers(Duration::from_secs(2)).await.unwrap();
    // The backend publishes Closed before awaiting its held final WAL flush.
    // Its retained watch remains observable after ordinary reads are refused.
    assert_eq!(status.borrow().durable_seq, durable_before);
    let empty = engine.in_flight.lock().unwrap().pending().is_empty();
    eprintln!(
        "R17B late write: append={result:?}, queue={config:?}, close={closed:?}, pending_after_workers={}",
        !empty
    );
    fixture.finish().await;
    assert!(
        empty,
        "late successful write stranded acknowledgements after dispatcher exit"
    );
    assert_moved(result.expect("append settles promptly").unwrap());
    assert_moved(closed.expect("close settles promptly").unwrap());
    assert!(config.expect("queue settles promptly").unwrap().is_err());
    {
        let state = handle.state.lock().unwrap();
        assert_eq!(state.applied.next, 0);
        assert_eq!(state.durable.next, 0);
        assert!(state.producers.is_empty());
    }
    assert!(!secondary.state.lock().unwrap().applied.closed);
    assert_eq!(handle.ring.lock().unwrap().bytes, 0);
    assert_eq!(pressure.unabsorbed_frame_bytes_now(), 0);
    assert_eq!(pressure.dirty_streams_now(), 0);
    assert_eq!(engine.stats_appended.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.usage.plaintext_bytes.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.usage.frame_bytes.load(Ordering::SeqCst), 0);
    assert_eq!(engine.maintenance_snapshot().unabsorbed_frame_bytes, 0);
    assert!(fixture.signals.try_recv().is_err());
    assert!(matches!(
        fixture
            .journal
            .wait(
                &format!("{}:0", fixture.journal.epoch),
                vec![3],
                Duration::ZERO
            )
            .await,
        crate::touch::WaitOutcome::Timeout { end_offset: 0, .. }
    ));
    // An unknown result is not an absence claim: a replacement recovers the
    // complete successful storage transaction, but never the old live mirrors.
    let replacement = Db::builder(engine.prefix.as_str(), fixture.store.clone())
        .build()
        .await
        .unwrap();
    assert_eq!(
        stored_tail(&replacement.get(tail_key(&HASH)).await.unwrap().unwrap())
            .unwrap()
            .next,
        1
    );
    assert!(
        replacement
            .get(crate::queue::config_key(&HASH, "c"))
            .await
            .unwrap()
            .is_some()
    );
    assert!(
        stored_tail(&replacement.get(tail_key(&SECOND)).await.unwrap().unwrap())
            .unwrap()
            .closed
    );
    let billing: crate::billing::SegmentBillingMetaV1 = serde_json::from_slice(
        &replacement
            .get(crate::billing::billing_meta_key(&HASH))
            .await
            .unwrap()
            .unwrap(),
    )
    .unwrap();
    assert_eq!(
        (
            billing.usage_version,
            billing.ingest_records_total,
            billing.ingest_payload_bytes_total
        ),
        (1, 1, 7)
    );
    let frame = replacement
        .get(record_key(&HASH, 0))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(billing.owned_frame_bytes_current, frame.len() as u64);
    assert_eq!(
        replacement
            .get(crate::billing::usage_dirty_key(&HASH))
            .await
            .unwrap()
            .unwrap()
            .as_ref(),
        1u64.to_le_bytes()
    );
    replacement.close().await.unwrap();
}
