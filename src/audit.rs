//! Durable, bounded access audit sink.
//!
//! Create/delete records are written synchronously as immutable objects so a
//! successful control-plane response is never emitted without its audit
//! record. Sampled data-plane records use a bounded channel and one-second
//! NDJSON batches to avoid turning high request rates into object explosions.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use bytes::Bytes;
use object_store::path::Path as ObjPath;
use object_store::{ObjectStore, PutMode, PutOptions, PutPayload};
use serde::Serialize;
use tokio::sync::mpsc;

const QUEUE_CAPACITY: usize = 8_192;
const BATCH_CAPACITY: usize = 256;

#[derive(Clone, Serialize)]
pub struct AuditEvent {
    pub timestamp_ms: i64,
    pub customer_id: String,
    pub token_id: String,
    pub stream: String,
    pub method: String,
    pub status: u16,
    pub duration_us: u64,
}

pub struct AuditLog {
    store: Arc<dyn ObjectStore>,
    instance_hash: String,
    tx: mpsc::Sender<AuditEvent>,
    sequence: AtomicU64,
    healthy: AtomicBool,
    dropped: AtomicU64,
}

impl AuditLog {
    pub fn start(store: Arc<dyn ObjectStore>, instance: &str) -> Arc<Self> {
        let (tx, rx) = mpsc::channel(QUEUE_CAPACITY);
        let log = Arc::new(Self {
            store,
            instance_hash: crate::crypto::hex(&crate::crypto::stream_hash(instance)),
            tx,
            sequence: AtomicU64::new(0),
            healthy: AtomicBool::new(true),
            dropped: AtomicU64::new(0),
        });
        tokio::spawn(Self::batch_loop(log.clone(), rx));
        log
    }

    pub fn ready(&self) -> bool {
        self.healthy.load(Ordering::Acquire)
    }

    pub fn dropped(&self) -> u64 {
        self.dropped.load(Ordering::Relaxed)
    }

    pub fn record_sampled(&self, event: AuditEvent) {
        if self.tx.try_send(event).is_err() {
            self.dropped.fetch_add(1, Ordering::Relaxed);
        }
    }

    pub async fn record_durable(&self, event: &AuditEvent) -> object_store::Result<()> {
        let sequence = self.sequence.fetch_add(1, Ordering::Relaxed);
        let path = ObjPath::from(format!(
            "audit/control/{}/{:020}-{:020}-{:032x}.json",
            self.instance_hash,
            event.timestamp_ms.max(0),
            sequence,
            rand::random::<u128>()
        ));
        let body = Bytes::from(serde_json::to_vec(event).expect("serialize audit event"));
        let result = self
            .store
            .put_opts(
                &path,
                PutPayload::from(body),
                PutOptions {
                    mode: PutMode::Create,
                    ..Default::default()
                },
            )
            .await
            .map(|_| ());
        self.healthy.store(result.is_ok(), Ordering::Release);
        result
    }

    async fn batch_loop(log: Arc<Self>, mut rx: mpsc::Receiver<AuditEvent>) {
        let mut interval = tokio::time::interval(Duration::from_secs(1));
        let mut pending = Vec::with_capacity(BATCH_CAPACITY);
        loop {
            interval.tick().await;
            while pending.len() < BATCH_CAPACITY {
                match rx.try_recv() {
                    Ok(event) => pending.push(event),
                    Err(mpsc::error::TryRecvError::Empty) => break,
                    Err(mpsc::error::TryRecvError::Disconnected) if pending.is_empty() => return,
                    Err(mpsc::error::TryRecvError::Disconnected) => break,
                }
            }
            if pending.is_empty() {
                continue;
            }
            let mut body = Vec::new();
            for event in &pending {
                serde_json::to_writer(&mut body, event).expect("serialize audit event");
                body.push(b'\n');
            }
            let sequence = log.sequence.fetch_add(1, Ordering::Relaxed);
            let timestamp_ms = crate::shard::now_ms().max(0);
            let path = ObjPath::from(format!(
                "audit/batches/{}/{:020}-{:020}-{:032x}.ndjson",
                log.instance_hash,
                timestamp_ms,
                sequence,
                rand::random::<u128>()
            ));
            match log
                .store
                .put_opts(
                    &path,
                    PutPayload::from(Bytes::from(body)),
                    PutOptions {
                        mode: PutMode::Create,
                        ..Default::default()
                    },
                )
                .await
            {
                Ok(_) => {
                    pending.clear();
                    log.healthy.store(true, Ordering::Release);
                }
                Err(error) => {
                    log.healthy.store(false, Ordering::Release);
                    tracing::error!("audit batch write failed: {error}");
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_util::TryStreamExt;
    use object_store::ObjectStoreExt;

    fn event() -> AuditEvent {
        AuditEvent {
            timestamp_ms: 1,
            customer_id: "customer".into(),
            token_id: "token".into(),
            stream: "stream".into(),
            method: "PUT".into(),
            status: 201,
            duration_us: 10,
        }
    }

    #[tokio::test]
    async fn control_events_are_immutable_and_durable() {
        let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let audit = AuditLog::start(store.clone(), "instance");

        audit.record_durable(&event()).await.unwrap();

        let objects: Vec<_> = store
            .list(Some(&ObjPath::from("audit/control")))
            .try_collect()
            .await
            .unwrap();
        assert_eq!(objects.len(), 1);
        let body = store
            .get(&objects[0].location)
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&body).unwrap()["customer_id"],
            "customer"
        );
        assert!(audit.ready());
    }
}
