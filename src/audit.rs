//! Durable, bounded access audit sink.
//!
//! Control mutations are written synchronously as immutable objects so a
//! successful control-plane response is never emitted without its audit
//! record. Sampled data-plane records use one-second NDJSON batches;
//! full-fidelity read-only operator records use a separate, longer bounded
//! window that also flushes at capacity, so normal metrics scraping does not
//! become one object per request.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use bytes::Bytes;
use futures_util::TryStreamExt;
use object_store::path::Path as ObjPath;
use object_store::{
    GetOptions, ObjectStore, ObjectStoreExt, PutMode, PutOptions, PutPayload, UpdateVersion,
};
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;

const QUEUE_CAPACITY: usize = 8_192;
const BATCH_CAPACITY: usize = 256;
const MAX_CURSOR_BYTES: usize = 4 * 1024;

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct MaintenanceCursor {
    version: u32,
    after: Option<String>,
}

struct LoadedCursor {
    after: Option<ObjPath>,
    update: Option<UpdateVersion>,
}

async fn put_immutable(
    store: &Arc<dyn ObjectStore>,
    path: &ObjPath,
    body: Bytes,
) -> object_store::Result<()> {
    store
        .put_opts(
            path,
            PutPayload::from(body),
            PutOptions {
                mode: PutMode::Create,
                ..Default::default()
            },
        )
        .await
        .map(|_| ())
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_millis().min(i64::MAX as u128) as i64)
        .unwrap_or(0)
}

struct PendingBatch {
    path: ObjPath,
    body: Bytes,
    primary_done: bool,
    mirror_done: bool,
}

#[derive(Clone)]
pub struct AuditConfig {
    pub mirror: Option<Arc<dyn ObjectStore>>,
    pub sample_denominator: u32,
    pub operator_batch_interval: Duration,
    pub primary_retention: Duration,
    pub mirror_retention: Duration,
    pub maintenance_interval: Duration,
    pub maintenance_objects_per_interval: usize,
    pub maintenance_max_object_bytes: u64,
}

impl Default for AuditConfig {
    fn default() -> Self {
        Self {
            mirror: None,
            sample_denominator: 100,
            operator_batch_interval: Duration::from_secs(60),
            primary_retention: Duration::from_secs(30 * 24 * 60 * 60),
            mirror_retention: Duration::from_secs(365 * 24 * 60 * 60),
            maintenance_interval: Duration::from_secs(300),
            maintenance_objects_per_interval: 1_000,
            maintenance_max_object_bytes: 8 * 1024 * 1024,
        }
    }
}

#[derive(Clone, Serialize)]
pub struct AuditEvent {
    pub format_version: u32,
    pub request_id: String,
    pub timestamp_ms: i64,
    pub customer_id: String,
    pub token_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub approval_customer_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub approval_token_id: Option<String>,
    pub stream: String,
    pub method: String,
    pub status: u16,
    pub duration_us: u64,
}

pub struct AuditLog {
    store: Arc<dyn ObjectStore>,
    mirror: Option<Arc<dyn ObjectStore>>,
    instance_hash: String,
    tx: mpsc::Sender<AuditEvent>,
    operator_tx: mpsc::Sender<AuditEvent>,
    sequence: AtomicU64,
    control_healthy: AtomicBool,
    batch_healthy: AtomicBool,
    operator_batch_healthy: AtomicBool,
    maintenance_healthy: AtomicBool,
    dropped: AtomicU64,
    sample_denominator: u32,
}

impl AuditLog {
    pub fn start_with_config(
        store: Arc<dyn ObjectStore>,
        instance: &str,
        config: AuditConfig,
    ) -> Arc<Self> {
        assert!((1..=1_000_000).contains(&config.sample_denominator));
        assert!(!config.operator_batch_interval.is_zero());
        assert!(!config.primary_retention.is_zero());
        assert!(config.mirror_retention >= config.primary_retention);
        assert!(!config.maintenance_interval.is_zero());
        assert!(config.maintenance_objects_per_interval > 0);
        assert!(config.maintenance_max_object_bytes > 0);
        let (tx, rx) = mpsc::channel(QUEUE_CAPACITY);
        let (operator_tx, operator_rx) = mpsc::channel(QUEUE_CAPACITY);
        let log = Arc::new(Self {
            store,
            mirror: config.mirror.clone(),
            instance_hash: crate::crypto::hex(&crate::crypto::stream_hash(instance)),
            tx,
            operator_tx,
            sequence: AtomicU64::new(0),
            control_healthy: AtomicBool::new(true),
            batch_healthy: AtomicBool::new(true),
            operator_batch_healthy: AtomicBool::new(true),
            maintenance_healthy: AtomicBool::new(config.mirror.is_none()),
            dropped: AtomicU64::new(0),
            sample_denominator: config.sample_denominator,
        });
        tokio::spawn(Self::batch_loop(
            log.clone(),
            rx,
            "batches",
            Duration::from_secs(1),
            false,
        ));
        tokio::spawn(Self::batch_loop(
            log.clone(),
            operator_rx,
            "operator-batches",
            config.operator_batch_interval,
            true,
        ));
        tokio::spawn(Self::maintenance_loop(log.clone(), config));
        log
    }

    pub fn ready(&self) -> bool {
        self.control_healthy.load(Ordering::Acquire)
            && self.batch_healthy.load(Ordering::Acquire)
            && self.operator_batch_healthy.load(Ordering::Acquire)
            && self.maintenance_healthy.load(Ordering::Acquire)
    }

    pub fn dropped(&self) -> u64 {
        self.dropped.load(Ordering::Relaxed)
    }

    pub fn mirror_configured(&self) -> bool {
        self.mirror.is_some()
    }

    pub fn should_sample(&self) -> bool {
        rand::random_ratio(1, self.sample_denominator)
    }

    fn enqueue_batched(
        &self,
        tx: &mpsc::Sender<AuditEvent>,
        healthy: &AtomicBool,
        event: AuditEvent,
    ) -> anyhow::Result<()> {
        tx.try_send(event).map_err(|error| {
            self.dropped.fetch_add(1, Ordering::Relaxed);
            healthy.store(false, Ordering::Release);
            anyhow::anyhow!("audit batch queue rejected event: {error}")
        })
    }

    pub fn record_sampled(&self, event: AuditEvent) {
        let _ = self.enqueue_batched(&self.tx, &self.batch_healthy, event);
    }

    /// Queue an unsampled read-only operator event. The longer window reduces
    /// normal metrics-scrape object cost; a full 256-event batch flushes
    /// immediately. Rejection is visible to the caller and fails readiness.
    pub fn record_operator_read(&self, event: AuditEvent) -> anyhow::Result<()> {
        self.enqueue_batched(&self.operator_tx, &self.operator_batch_healthy, event)
    }

    pub async fn record_durable(&self, event: &AuditEvent) -> anyhow::Result<()> {
        let sequence = self.sequence.fetch_add(1, Ordering::Relaxed);
        let path = ObjPath::from(format!(
            "audit/control/{}/{:020}-{:020}-{:032x}.json",
            self.instance_hash,
            event.timestamp_ms.max(0),
            sequence,
            rand::random::<u128>()
        ));
        let body = Bytes::from(serde_json::to_vec(event).expect("serialize audit event"));
        let primary = put_immutable(&self.store, &path, body.clone());
        let result = if let Some(mirror) = &self.mirror {
            let (primary, mirror) = tokio::join!(primary, put_immutable(mirror, &path, body));
            match (primary, mirror) {
                (Ok(()), Ok(())) => Ok(()),
                (Err(error), _) => Err(error.into()),
                (Ok(()), Err(error)) => Err(error.into()),
            }
        } else {
            primary.await.map_err(anyhow::Error::from)
        };
        self.control_healthy
            .store(result.is_ok(), Ordering::Release);
        result
    }

    async fn batch_loop(
        log: Arc<Self>,
        mut rx: mpsc::Receiver<AuditEvent>,
        prefix: &'static str,
        window: Duration,
        operator: bool,
    ) {
        loop {
            let Some(first) = rx.recv().await else {
                return;
            };
            let mut events = Vec::with_capacity(BATCH_CAPACITY);
            events.push(first);
            let deadline = tokio::time::sleep(window);
            tokio::pin!(deadline);
            let mut disconnected = false;
            while events.len() < BATCH_CAPACITY {
                tokio::select! {
                    received = rx.recv() => match received {
                        Some(event) => events.push(event),
                        None => {
                            disconnected = true;
                            break;
                        }
                    },
                    () = &mut deadline => break,
                }
            }
            let mut body = Vec::new();
            for event in &events {
                serde_json::to_writer(&mut body, event).expect("serialize audit event");
                body.push(b'\n');
            }
            let sequence = log.sequence.fetch_add(1, Ordering::Relaxed);
            let timestamp_ms = now_ms().max(0);
            let mut pending = PendingBatch {
                path: ObjPath::from(format!(
                    "audit/{}/{}/{:020}-{:020}-{:032x}.ndjson",
                    prefix,
                    log.instance_hash,
                    timestamp_ms,
                    sequence,
                    rand::random::<u128>()
                )),
                body: Bytes::from(body),
                primary_done: false,
                mirror_done: log.mirror.is_none(),
            };
            loop {
                let healthy = if operator {
                    &log.operator_batch_healthy
                } else {
                    &log.batch_healthy
                };
                if !pending.primary_done {
                    match put_immutable(&log.store, &pending.path, pending.body.clone()).await {
                        Ok(()) => pending.primary_done = true,
                        Err(error) => {
                            healthy.store(false, Ordering::Release);
                            tracing::error!(path = %pending.path, "primary audit batch write failed: {error}");
                        }
                    }
                }
                if !pending.mirror_done
                    && let Some(mirror) = &log.mirror
                {
                    match put_immutable(mirror, &pending.path, pending.body.clone()).await {
                        Ok(()) => pending.mirror_done = true,
                        Err(error) => {
                            healthy.store(false, Ordering::Release);
                            tracing::error!(path = %pending.path, "mirror audit batch write failed: {error}");
                        }
                    }
                }
                if pending.primary_done && pending.mirror_done {
                    healthy.store(true, Ordering::Release);
                    break;
                }
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
            if disconnected {
                return;
            }
        }
    }

    async fn maintenance_loop(log: Arc<Self>, config: AuditConfig) {
        let mut interval = tokio::time::interval(config.maintenance_interval);
        let mut primary_reconciled = [config.mirror.is_none(); 3];
        loop {
            interval.tick().await;
            let result = maintain_once(&log, &config, &mut primary_reconciled).await;
            log.maintenance_healthy.store(
                result.is_ok() && primary_reconciled.into_iter().all(|done| done),
                Ordering::Release,
            );
            if let Err(error) = result {
                tracing::error!("audit mirror/retention maintenance failed: {error:#}");
            }
        }
    }
}

fn cursor_path(instance_hash: &str, name: &str) -> ObjPath {
    ObjPath::from(format!(
        "audit/maintenance/{instance_hash}/{name}-cursor.json"
    ))
}

async fn load_cursor(
    store: &Arc<dyn ObjectStore>,
    instance_hash: &str,
    name: &str,
    prefix: &ObjPath,
) -> anyhow::Result<LoadedCursor> {
    let path = cursor_path(instance_hash, name);
    match store.get(&path).await {
        Ok(result) => {
            let update = UpdateVersion {
                e_tag: result.meta.e_tag.clone(),
                version: result.meta.version.clone(),
            };
            anyhow::ensure!(
                result.meta.size <= MAX_CURSOR_BYTES as u64,
                "audit maintenance cursor is too large: {path}"
            );
            let bytes = result.bytes().await?;
            let cursor: MaintenanceCursor = serde_json::from_slice(&bytes)?;
            anyhow::ensure!(cursor.version == 1, "unsupported audit maintenance cursor");
            let after = cursor.after.map(ObjPath::from);
            if let Some(after) = &after {
                let expected_prefix = format!("{prefix}/");
                anyhow::ensure!(
                    after.to_string().starts_with(&expected_prefix),
                    "audit maintenance cursor escaped its prefix"
                );
            }
            Ok(LoadedCursor {
                after,
                update: Some(update),
            })
        }
        Err(object_store::Error::NotFound { .. }) => Ok(LoadedCursor {
            after: None,
            update: None,
        }),
        Err(error) => Err(error.into()),
    }
}

async fn save_cursor(
    store: &Arc<dyn ObjectStore>,
    instance_hash: &str,
    name: &str,
    after: Option<&ObjPath>,
    update: Option<UpdateVersion>,
) -> anyhow::Result<()> {
    let path = cursor_path(instance_hash, name);
    let body = serde_json::to_vec(&MaintenanceCursor {
        version: 1,
        after: after.map(ToString::to_string),
    })?;
    store
        .put_opts(
            &path,
            PutPayload::from(Bytes::from(body)),
            PutOptions::from(match update {
                Some(version) => PutMode::Update(version),
                None => PutMode::Create,
            }),
        )
        .await?;
    Ok(())
}

async fn provider_now(
    store: &Arc<dyn ObjectStore>,
    instance_hash: &str,
    provider: &str,
) -> anyhow::Result<chrono::DateTime<chrono::Utc>> {
    let path = ObjPath::from(format!(
        "audit/maintenance/{instance_hash}/{provider}-clock.json"
    ));
    let body = Bytes::from(format!("{{\"nonce\":\"{:032x}\"}}", rand::random::<u128>()));
    store
        .put_opts(
            &path,
            PutPayload::from(body),
            PutOptions {
                mode: PutMode::Overwrite,
                ..Default::default()
            },
        )
        .await?;
    Ok(store.get(&path).await?.meta.last_modified)
}

async fn stable_body(
    store: &Arc<dyn ObjectStore>,
    meta: &object_store::ObjectMeta,
    max_object_bytes: u64,
) -> anyhow::Result<Bytes> {
    anyhow::ensure!(
        meta.size <= max_object_bytes,
        "audit object exceeds maintenance bound: {}",
        meta.location
    );
    let etag = meta
        .e_tag
        .clone()
        .ok_or_else(|| anyhow::anyhow!("audit object lacks ETag: {}", meta.location))?;
    let result = store
        .get_opts(
            &meta.location,
            GetOptions {
                if_match: Some(etag),
                ..Default::default()
            },
        )
        .await?;
    anyhow::ensure!(
        result.meta.size == meta.size,
        "audit object size changed during maintenance: {}",
        meta.location
    );
    Ok(result.bytes().await?)
}

async fn ensure_mirror(
    mirror: &Arc<dyn ObjectStore>,
    path: &ObjPath,
    expected: &Bytes,
) -> anyhow::Result<()> {
    match mirror.get(path).await {
        Ok(result) => {
            anyhow::ensure!(
                result.meta.size == expected.len() as u64,
                "audit mirror size mismatch: {path}"
            );
            anyhow::ensure!(
                result.bytes().await? == *expected,
                "audit mirror content mismatch: {path}"
            );
            Ok(())
        }
        Err(object_store::Error::NotFound { .. }) => {
            match put_immutable(mirror, path, expected.clone()).await {
                Ok(()) => Ok(()),
                Err(object_store::Error::AlreadyExists { .. }) => {
                    let result = mirror.get(path).await?;
                    anyhow::ensure!(
                        result.meta.size == expected.len() as u64
                            && result.bytes().await? == *expected,
                        "raced audit mirror content mismatch: {path}"
                    );
                    Ok(())
                }
                Err(error) => Err(error.into()),
            }
        }
        Err(error) => Err(error.into()),
    }
}

async fn maintain_primary_prefix(
    log: &AuditLog,
    prefix: &ObjPath,
    cursor_name: &str,
    cutoff: chrono::DateTime<chrono::Utc>,
    config: &AuditConfig,
) -> anyhow::Result<bool> {
    let cursor = load_cursor(&log.store, &log.instance_hash, cursor_name, prefix).await?;
    let mut listed = match &cursor.after {
        Some(after) => log.store.list_with_offset(Some(prefix), after),
        None => log.store.list(Some(prefix)),
    };
    let mut processed = 0usize;
    let mut last = None;
    while processed < config.maintenance_objects_per_interval {
        let Some(meta) = listed.try_next().await? else {
            save_cursor(
                &log.store,
                &log.instance_hash,
                cursor_name,
                None,
                cursor.update,
            )
            .await?;
            return Ok(true);
        };
        let body = stable_body(&log.store, &meta, config.maintenance_max_object_bytes).await?;
        if let Some(mirror) = &log.mirror {
            ensure_mirror(mirror, &meta.location, &body).await?;
        }
        if meta.last_modified < cutoff {
            log.store.delete(&meta.location).await?;
        }
        last = Some(meta.location);
        processed += 1;
    }
    let complete = listed.try_next().await?.is_none();
    save_cursor(
        &log.store,
        &log.instance_hash,
        cursor_name,
        (!complete).then_some(last.as_ref().expect("processed a full page")),
        cursor.update,
    )
    .await?;
    Ok(complete)
}

async fn prune_mirror_prefix(
    log: &AuditLog,
    mirror: &Arc<dyn ObjectStore>,
    prefix: &ObjPath,
    cursor_name: &str,
    cutoff: chrono::DateTime<chrono::Utc>,
    limit: usize,
) -> anyhow::Result<()> {
    let cursor = load_cursor(&log.store, &log.instance_hash, cursor_name, prefix).await?;
    let mut listed = match &cursor.after {
        Some(after) => mirror.list_with_offset(Some(prefix), after),
        None => mirror.list(Some(prefix)),
    };
    let mut processed = 0usize;
    let mut last = None;
    while processed < limit {
        let Some(meta) = listed.try_next().await? else {
            save_cursor(
                &log.store,
                &log.instance_hash,
                cursor_name,
                None,
                cursor.update,
            )
            .await?;
            return Ok(());
        };
        if meta.last_modified < cutoff {
            mirror.delete(&meta.location).await?;
        }
        last = Some(meta.location);
        processed += 1;
    }
    let complete = listed.try_next().await?.is_none();
    save_cursor(
        &log.store,
        &log.instance_hash,
        cursor_name,
        (!complete).then_some(last.as_ref().expect("processed a full page")),
        cursor.update,
    )
    .await?;
    Ok(())
}

async fn maintain_once(
    log: &AuditLog,
    config: &AuditConfig,
    primary_reconciled: &mut [bool; 3],
) -> anyhow::Result<()> {
    let primary_now = provider_now(&log.store, &log.instance_hash, "primary").await?;
    let primary_cutoff = primary_now
        - chrono::Duration::from_std(config.primary_retention)
            .map_err(|_| anyhow::anyhow!("primary audit retention is out of range"))?;
    for (index, kind) in ["control", "batches", "operator-batches"]
        .into_iter()
        .enumerate()
    {
        let prefix = ObjPath::from(format!("audit/{kind}/{}/", log.instance_hash));
        if maintain_primary_prefix(log, &prefix, kind, primary_cutoff, config).await? {
            primary_reconciled[index] = true;
        }
    }
    if let Some(mirror) = &log.mirror {
        let mirror_now = provider_now(mirror, &log.instance_hash, "mirror").await?;
        let mirror_cutoff = mirror_now
            - chrono::Duration::from_std(config.mirror_retention)
                .map_err(|_| anyhow::anyhow!("mirror audit retention is out of range"))?;
        for kind in ["control", "batches", "operator-batches"] {
            let prefix = ObjPath::from(format!("audit/{kind}/{}/", log.instance_hash));
            prune_mirror_prefix(
                log,
                mirror,
                &prefix,
                &format!("mirror-{kind}"),
                mirror_cutoff,
                config.maintenance_objects_per_interval,
            )
            .await?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_util::TryStreamExt;
    use object_store::ObjectStoreExt;

    fn event() -> AuditEvent {
        AuditEvent {
            format_version: 1,
            request_id: "00000000000000000000000000000001".into(),
            timestamp_ms: 1,
            customer_id: "customer".into(),
            token_id: "token".into(),
            approval_customer_id: None,
            approval_token_id: None,
            stream: "stream".into(),
            method: "PUT".into(),
            status: 201,
            duration_us: 10,
        }
    }

    #[tokio::test]
    async fn control_events_are_immutable_and_durable() {
        let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let audit = AuditLog::start_with_config(store.clone(), "instance", AuditConfig::default());

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

    #[tokio::test]
    async fn control_events_are_exactly_dual_written() {
        let primary: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let mirror: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let audit = AuditLog::start_with_config(
            primary.clone(),
            "instance",
            AuditConfig {
                mirror: Some(mirror.clone()),
                maintenance_interval: Duration::from_secs(3_600),
                ..AuditConfig::default()
            },
        );

        audit.record_durable(&event()).await.unwrap();
        let primary_objects: Vec<_> = primary
            .list(Some(&ObjPath::from("audit/control")))
            .try_collect()
            .await
            .unwrap();
        let mirror_objects: Vec<_> = mirror
            .list(Some(&ObjPath::from("audit/control")))
            .try_collect()
            .await
            .unwrap();
        assert_eq!(primary_objects.len(), 1);
        assert_eq!(mirror_objects.len(), 1);
        assert_eq!(primary_objects[0].location, mirror_objects[0].location);
        assert_eq!(
            primary
                .get(&primary_objects[0].location)
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap(),
            mirror
                .get(&mirror_objects[0].location)
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap()
        );
    }

    #[tokio::test]
    async fn operator_reads_share_a_window_and_flush_immediately_at_capacity() {
        let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let audit = AuditLog::start_with_config(
            store.clone(),
            "windowed-instance",
            AuditConfig {
                operator_batch_interval: Duration::from_millis(500),
                maintenance_interval: Duration::from_secs(3_600),
                ..AuditConfig::default()
            },
        );
        audit.record_operator_read(event()).unwrap();
        tokio::time::sleep(Duration::from_millis(20)).await;
        let mut second = event();
        second.request_id = "00000000000000000000000000000002".into();
        audit.record_operator_read(second).unwrap();
        tokio::time::sleep(Duration::from_millis(600)).await;
        let objects: Vec<_> = store
            .list(Some(&ObjPath::from("audit/operator-batches")))
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
        assert_eq!(body.iter().filter(|byte| **byte == b'\n').count(), 2);

        let capacity_store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let capacity_audit = AuditLog::start_with_config(
            capacity_store.clone(),
            "capacity-instance",
            AuditConfig {
                operator_batch_interval: Duration::from_secs(3_600),
                maintenance_interval: Duration::from_secs(3_600),
                ..AuditConfig::default()
            },
        );
        for sequence in 0..BATCH_CAPACITY {
            let mut item = event();
            item.request_id = format!("{sequence:032x}");
            capacity_audit.record_operator_read(item).unwrap();
        }
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let objects: Vec<_> = capacity_store
                    .list(Some(&ObjPath::from("audit/operator-batches")))
                    .try_collect()
                    .await
                    .unwrap();
                if objects.len() == 1 {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("capacity batch did not flush immediately");
    }

    fn maintenance_log(primary: Arc<dyn ObjectStore>, mirror: Arc<dyn ObjectStore>) -> AuditLog {
        let (tx, _rx) = mpsc::channel(1);
        let (operator_tx, _operator_rx) = mpsc::channel(1);
        AuditLog {
            store: primary,
            mirror: Some(mirror),
            instance_hash: "instance-hash".to_string(),
            tx,
            operator_tx,
            sequence: AtomicU64::new(0),
            control_healthy: AtomicBool::new(true),
            batch_healthy: AtomicBool::new(true),
            operator_batch_healthy: AtomicBool::new(true),
            maintenance_healthy: AtomicBool::new(true),
            dropped: AtomicU64::new(0),
            sample_denominator: 100,
        }
    }

    #[test]
    fn batched_queue_rejection_is_visible_and_fails_readiness() {
        let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let (tx, _rx) = mpsc::channel(1);
        let (operator_tx, _operator_rx) = mpsc::channel(1);
        let audit = AuditLog {
            store,
            mirror: None,
            instance_hash: "instance-hash".to_string(),
            tx,
            operator_tx,
            sequence: AtomicU64::new(0),
            control_healthy: AtomicBool::new(true),
            batch_healthy: AtomicBool::new(true),
            operator_batch_healthy: AtomicBool::new(true),
            maintenance_healthy: AtomicBool::new(true),
            dropped: AtomicU64::new(0),
            sample_denominator: 100,
        };

        audit.record_operator_read(event()).unwrap();
        assert!(audit.record_operator_read(event()).is_err());
        assert_eq!(audit.dropped(), 1);
        assert!(!audit.ready());
    }

    #[tokio::test]
    async fn retention_verifies_the_mirror_before_deleting_primary() {
        let primary: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let mirror: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let log = maintenance_log(primary.clone(), mirror.clone());
        let prefix = ObjPath::from("audit/control/instance-hash");
        let path = ObjPath::from("audit/control/instance-hash/0001.json");
        put_immutable(&primary, &path, Bytes::from_static(b"source"))
            .await
            .unwrap();
        put_immutable(&mirror, &path, Bytes::from_static(b"wrong"))
            .await
            .unwrap();
        let config = AuditConfig {
            mirror: Some(mirror.clone()),
            maintenance_objects_per_interval: 10,
            ..AuditConfig::default()
        };
        let future = chrono::Utc::now() + chrono::Duration::days(1);

        assert!(
            maintain_primary_prefix(&log, &prefix, "test", future, &config)
                .await
                .is_err()
        );
        assert!(primary.get(&path).await.is_ok());

        mirror.delete(&path).await.unwrap();
        maintain_primary_prefix(&log, &prefix, "test", future, &config)
            .await
            .unwrap();
        assert!(matches!(
            primary.get(&path).await,
            Err(object_store::Error::NotFound { .. })
        ));
        assert_eq!(
            mirror.get(&path).await.unwrap().bytes().await.unwrap(),
            Bytes::from_static(b"source")
        );
    }

    #[tokio::test]
    async fn durable_cursor_reconciles_every_primary_object() {
        let primary: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let mirror: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let log = maintenance_log(primary.clone(), mirror.clone());
        let prefix = ObjPath::from("audit/control/instance-hash");
        for index in 0..3 {
            let path = ObjPath::from(format!("audit/control/instance-hash/{index:04}.json"));
            put_immutable(&primary, &path, Bytes::from(format!("event-{index}")))
                .await
                .unwrap();
        }
        let config = AuditConfig {
            mirror: Some(mirror.clone()),
            maintenance_objects_per_interval: 1,
            ..AuditConfig::default()
        };
        let past = chrono::Utc::now() - chrono::Duration::days(1);

        assert!(
            !maintain_primary_prefix(&log, &prefix, "paged", past, &config)
                .await
                .unwrap()
        );
        assert!(
            !maintain_primary_prefix(&log, &prefix, "paged", past, &config)
                .await
                .unwrap()
        );
        assert!(
            maintain_primary_prefix(&log, &prefix, "paged", past, &config)
                .await
                .unwrap()
        );

        let mirrored: Vec<_> = mirror.list(Some(&prefix)).try_collect().await.unwrap();
        assert_eq!(mirrored.len(), 3);
    }
}
