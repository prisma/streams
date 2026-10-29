//! Process-wide helpers shared by the read and write paths: the stream
//! key cache, the cumulative ingest counter, and the in-memory L0 view
//! of a SlateDB manifest.

use std::collections::HashMap;
use std::sync::{Mutex, atomic::AtomicU64};
use std::time::{Duration, Instant};

use crate::crypto::StreamKey;

/// Per-partition L0 facts from the db's IN-MEMORY manifest snapshot
/// (`Db::manifest()` — no object-store request). The manifest types
/// only expose Serialize at this pin, so this walks the serde view
/// tolerantly: (l0_sst_count, l0_bytes_est, compacted_runs,
/// manifest_id). The review's leading indicator — "history L0
/// approaching 64 × 4 MiB" — is exactly l0_sst_count here.
pub(crate) fn history_l0_stats(db: &slatedb::Db) -> (u64, u64, u64, u64) {
    fn sum_sizes(v: &serde_json::Value) -> u64 {
        match v {
            serde_json::Value::Object(m) => m
                .iter()
                .map(|(k, v)| {
                    if v.is_u64() && (k.contains("size") || k.ends_with("_bytes")) {
                        v.as_u64().unwrap_or(0)
                    } else {
                        sum_sizes(v)
                    }
                })
                .sum(),
            serde_json::Value::Array(a) => a.iter().map(sum_sizes).sum(),
            _ => 0,
        }
    }
    fn find<'a>(v: &'a serde_json::Value, key: &str) -> Option<&'a serde_json::Value> {
        match v {
            serde_json::Value::Object(m) => {
                m.get(key).or_else(|| m.values().find_map(|x| find(x, key)))
            }
            serde_json::Value::Array(a) => a.iter().find_map(|x| find(x, key)),
            _ => None,
        }
    }
    let vm = db.manifest();
    let Ok(j) = serde_json::to_value(&vm) else {
        return (0, 0, 0, 0);
    };
    let id = find(&j, "id").and_then(|v| v.as_u64()).unwrap_or(0);
    let (l0n, l0b) = find(&j, "l0")
        .and_then(|v| v.as_array())
        .map(|a| (a.len() as u64, a.iter().map(sum_sizes).sum()))
        .unwrap_or((0, 0));
    let runs = find(&j, "compacted")
        .and_then(|v| v.as_array())
        .map(|a| a.len() as u64)
        .unwrap_or(0);
    (l0n, l0b, runs, id)
}

/// Cumulative appended bytes, for rate derivation between ops snapshots.
pub(crate) static INGEST_BYTES_TOTAL: AtomicU64 = AtomicU64::new(0);

// ---- key cache (transient; fed by keyed requests) ----

pub(crate) struct KeyEntry {
    pub key: StreamKey,
    pub epoch: [u8; 16],
    pub at: Instant,
}

#[derive(Default)]
pub(crate) struct KeyCache {
    map: Mutex<HashMap<[u8; 16], KeyEntry>>,
}

const KEY_TTL: Duration = Duration::from_secs(900);
/// Cardinality bound: v2 absorption never consults this cache, so on a
/// wide shard it is pure retention — expired entries used to return
/// None but stay resident forever (static-audit memory finding).
const KEY_CACHE_MAX: usize = 65_536;

impl KeyCache {
    #[expect(
        clippy::unwrap_used,
        reason = "KeyCache::put; a poisoned key cache may hold a partially inserted key entry; recovering it could decrypt or index with a key that was never fully installed"
    )]
    pub(crate) fn put(&self, hash: [u8; 16], key: StreamKey, epoch: [u8; 16]) {
        let mut map = self.map.lock().unwrap();
        if map.len() >= KEY_CACHE_MAX && !map.contains_key(&hash) {
            map.retain(|_, e| e.at.elapsed() <= KEY_TTL);
            if map.len() >= KEY_CACHE_MAX
                && let Some(oldest) = map.iter().min_by_key(|(_, e)| e.at).map(|(h, _)| *h)
            {
                map.remove(&oldest);
            }
        }
        map.insert(
            hash,
            KeyEntry {
                key,
                epoch,
                at: Instant::now(),
            },
        );
    }

    #[expect(
        clippy::unwrap_used,
        reason = "KeyCache::get; a poisoned key cache may hold a partially inserted key entry; recovering it could decrypt or index with a key that was never fully installed"
    )]
    pub(crate) fn get(&self, hash: &[u8; 16]) -> Option<(StreamKey, [u8; 16])> {
        let mut map = self.map.lock().unwrap();
        let expired = map.get(hash).is_some_and(|e| e.at.elapsed() > KEY_TTL);
        if expired {
            map.remove(hash);
            return None;
        }
        let e = map.get(hash)?;
        Some((e.key.clone(), e.epoch))
    }

    #[expect(
        clippy::unwrap_used,
        reason = "KeyCache::len; a poisoned key cache may hold a partially inserted key entry; recovering it could decrypt or index with a key that was never fully installed"
    )]
    pub(crate) fn len(&self) -> usize {
        self.map.lock().unwrap().len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::{ObjectStore, PutOptions, PutPayload, PutResult, path::Path as OPath};

    #[derive(Debug)]
    struct SlowPuts(Arc<dyn ObjectStore>);
    impl std::fmt::Display for SlowPuts {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "SlowPuts")
        }
    }
    #[async_trait::async_trait]
    impl ObjectStore for SlowPuts {
        async fn put_opts(
            &self,
            location: &OPath,
            payload: PutPayload,
            opts: PutOptions,
        ) -> object_store::Result<PutResult> {
            // Stall only WAL flushes: setup (manifest writes) stays fast,
            // and the flusher wedges exactly like a slow-store day.
            if location.as_ref().contains("wal") {
                tokio::time::sleep(Duration::from_secs(20)).await;
            }
            self.0.put_opts(location, payload, opts).await
        }
        async fn put_multipart_opts(
            &self,
            location: &OPath,
            opts: object_store::PutMultipartOptions,
        ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
            self.0.put_multipart_opts(location, opts).await
        }
        async fn get_opts(
            &self,
            location: &OPath,
            options: object_store::GetOptions,
        ) -> object_store::Result<object_store::GetResult> {
            self.0.get_opts(location, options).await
        }
        fn delete_stream(
            &self,
            locations: futures_util::stream::BoxStream<'static, object_store::Result<OPath>>,
        ) -> futures_util::stream::BoxStream<'static, object_store::Result<OPath>> {
            self.0.delete_stream(locations)
        }
        fn list(
            &self,
            prefix: Option<&OPath>,
        ) -> futures_util::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>>
        {
            self.0.list(prefix)
        }
        async fn list_with_delimiter(
            &self,
            prefix: Option<&OPath>,
        ) -> object_store::Result<object_store::ListResult> {
            self.0.list_with_delimiter(prefix).await
        }
        async fn copy_opts(
            &self,
            from: &OPath,
            to: &OPath,
            options: object_store::CopyOptions,
        ) -> object_store::Result<()> {
            self.0.copy_opts(from, to, options).await
        }
    }

    use crate::shard::{ShardConfig, ShardEngine};
    use std::sync::Arc;

    /// Wedge detector: a stale in-progress db.write reads as blocked;
    /// idle (0) and fresh writes do not.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn commit_blocked_ms_tracks_stale_writes() {
        let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let db = crate::keyspace::db_builder("w/shard", store.clone())
            .build()
            .await
            .unwrap();
        let engine = ShardEngine::start(
            "w".into(),
            Arc::new(db),
            store.clone(),
            ShardConfig::default(),
            None,
            crate::shard::ShardMaintenance::default(),
        );
        assert_eq!(engine.commit_blocked_ms(), 0, "idle engine must read 0");
        engine.set_commit_write_started_ms(crate::shard::now_ms() - 5_000);
        assert!(
            engine.commit_blocked_ms() >= 4_500,
            "stale write must read blocked"
        );
        engine.set_commit_write_started_ms(crate::shard::now_ms());
        assert!(
            engine.commit_blocked_ms() < 2_000,
            "fresh write must not trip the shed"
        );
        engine.set_commit_write_started_ms(0);
        assert_eq!(engine.commit_blocked_ms(), 0);
    }

    /// End-to-end wedge detection under REAL SlateDB byte backpressure: a
    /// store whose PUTs stall blocks the commit db.write once the unflushed
    /// cap fills, and commit_blocked_ms() must cross the shed threshold.
    #[expect(
        clippy::disallowed_methods,
        reason = "commit_blocked_detects_real_flush_stall; the fixture spawns the task it then aborts or joins before asserting; a supervised spawn would tie the fixture's teardown to a supervisor it never builds"
    )]
    #[expect(
        clippy::let_underscore_must_use,
        reason = "commit_blocked_detects_real_flush_stall; the fixture ignores a join or enqueue result whose only failure is the shutdown it stages itself; treating it as fallible would add branches the pinned sequence never takes"
    )]
    #[expect(
        clippy::excessive_nesting,
        reason = "commit_blocked_detects_real_flush_stall; the fixture nests the stalled flush inside the blocked commit inside the feeding task it stages; flattening it would separate the stall from the commit it must block"
    )]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn commit_blocked_detects_real_flush_stall() {
        let mem: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let slow: Arc<dyn ObjectStore> = Arc::new(SlowPuts(mem));
        let db = crate::keyspace::db_builder("s/shard", slow.clone())
            .with_settings(slatedb::config::Settings {
                flush_interval: Some(Duration::from_millis(25)),
                max_unflushed_bytes: 8 * 1024,
                // 0.15 validates max_unflushed > l0_sst_size; keep the tiny
                // unflushed cap (the stall trigger) and shrink L0 under it.
                l0_sst_size_bytes: 4 * 1024,
                ..Default::default()
            })
            .build()
            .await
            .unwrap();
        let engine = ShardEngine::start(
            "s".into(),
            Arc::new(db),
            slow.clone(),
            ShardConfig::default(),
            None,
            crate::shard::ShardMaintenance::default(),
        );

        // Continuous feed: a LATER db.write must find the unflushed cap
        // full (the first group is admitted regardless) and block there.
        let feeder = engine.clone();
        let feed = tokio::spawn(async move {
            for i in 0..4096u64 {
                let (tx, _rx) = tokio::sync::oneshot::channel();
                let req = crate::shard::AppendReq {
                    usage: Default::default(),
                    hash: [9u8; 16],
                    route: [0u8; 16],
                    enqueued_at: Instant::now(),
                    entries: vec![bytes::Bytes::from(vec![b'x'; 1024])],
                    routing_key: String::new(),
                    key_hash: crate::crypto::stream_hash(""),
                    producer_lineage: Vec::new(),
                    key_version: 1,
                    subkey: [0u8; 32],
                    ts_hint_ms: Some(i as i64),
                    seq: None,
                    bytes: 1024,
                    finish: crate::shard::AppendFinish::Open,
                    producer: None,
                    deferred_error: None,
                    sealed_reject_new: None,
                    touch: None,
                    seal_gen: None,
                    billing: None,
                    resp: tx,
                };
                let _ = feeder.try_enqueue(req);
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        });

        let deadline = Instant::now() + Duration::from_secs(15);
        let mut blocked = 0;
        while Instant::now() < deadline {
            blocked = engine.wedge_ms();
            if blocked > 5_000 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        feed.abort();
        assert!(
            blocked > 5_000,
            "wedge_ms never crossed the shed threshold (last {blocked})"
        );
    }

    /// The stale-durability wedge mode: db.write keeps succeeding (default
    /// unflushed cap is huge) while WAL flushes stall — committed groups
    /// age in in_flight and oldest_inflight_ms must cross the threshold.
    /// This is the mode the 2026-07-22 cloud gate proved commit_blocked_ms
    /// alone cannot see.
    #[expect(
        clippy::let_underscore_must_use,
        reason = "wedge_detects_stale_durability; the fixture ignores a join or enqueue result whose only failure is the shutdown it stages itself; treating it as fallible would add branches the pinned sequence never takes"
    )]
    #[expect(
        clippy::excessive_nesting,
        reason = "wedge_detects_stale_durability; the fixture nests the stale durability report inside the wedged engine it stages; flattening it would separate the report from the wedge it must detect"
    )]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn wedge_detects_stale_durability() {
        let mem: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let slow: Arc<dyn ObjectStore> = Arc::new(SlowPuts(mem));
        // Default (large) unflushed cap: writes are admitted, durability stalls.
        let db = crate::keyspace::db_builder("d/shard", slow.clone())
            .with_settings(slatedb::config::Settings {
                flush_interval: Some(Duration::from_millis(25)),
                ..Default::default()
            })
            .build()
            .await
            .unwrap();
        let engine = ShardEngine::start(
            "d".into(),
            Arc::new(db),
            slow.clone(),
            ShardConfig::default(),
            None,
            crate::shard::ShardMaintenance::default(),
        );
        for i in 0..8u64 {
            let (tx, _rx) = tokio::sync::oneshot::channel();
            let req = crate::shard::AppendReq {
                usage: Default::default(),
                hash: [7u8; 16],
                route: [0u8; 16],
                enqueued_at: Instant::now(),
                entries: vec![bytes::Bytes::from(vec![b'y'; 512])],
                routing_key: String::new(),
                key_hash: crate::crypto::stream_hash(""),
                producer_lineage: Vec::new(),
                key_version: 1,
                subkey: [0u8; 32],
                ts_hint_ms: Some(i as i64),
                seq: None,
                bytes: 512,
                finish: crate::shard::AppendFinish::Open,
                producer: None,
                deferred_error: None,
                sealed_reject_new: None,
                touch: None,
                seal_gen: None,
                billing: None,
                resp: tx,
            };
            let _ = engine.try_enqueue(req);
        }
        let deadline = Instant::now() + Duration::from_secs(15);
        let mut wedge = 0;
        while Instant::now() < deadline {
            wedge = engine.wedge_ms();
            if wedge > 5_000 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        assert!(
            wedge > 5_000,
            "oldest_inflight_ms never crossed the shed threshold (last {wedge})"
        );
        assert!(
            engine.oldest_inflight_ms() > 5_000,
            "the stale-durability component specifically must be the signal"
        );
    }
}
