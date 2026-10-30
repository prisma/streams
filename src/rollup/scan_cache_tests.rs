//! The telemetry databases' scans keep what they read: a scan that read a
//! table's blocks from the store reads them from the telemetry cache the
//! next time, and a drain round reads only the rows the ledger holds.
#![cfg(test)]

use crate::runtime::block_cache::BlockCache;
use async_trait::async_trait;
use object_store::path::Path as ObjPath;
use object_store::{ObjectStore, memory::InMemory};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

/// Counts the GETs of compacted tables (L0s and sorted runs), the reads a
/// cache miss costs; manifests and writes are not counted.
#[derive(Debug, Default)]
pub(crate) struct TableGets {
    inner: InMemory,
    gets: AtomicU64,
}

impl TableGets {
    pub(crate) fn count(&self) -> u64 {
        self.gets.load(Ordering::SeqCst)
    }
}

impl std::fmt::Display for TableGets {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "TableGets")
    }
}

#[async_trait]
impl ObjectStore for TableGets {
    async fn put_opts(
        &self,
        location: &ObjPath,
        payload: object_store::PutPayload,
        opts: object_store::PutOptions,
    ) -> object_store::Result<object_store::PutResult> {
        self.inner.put_opts(location, payload, opts).await
    }
    async fn put_multipart_opts(
        &self,
        location: &ObjPath,
        opts: object_store::PutMultipartOptions,
    ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }
    async fn get_opts(
        &self,
        location: &ObjPath,
        options: object_store::GetOptions,
    ) -> object_store::Result<object_store::GetResult> {
        if location.as_ref().contains("compacted") {
            self.gets.fetch_add(1, Ordering::SeqCst);
        }
        self.inner.get_opts(location, options).await
    }
    fn delete_stream(
        &self,
        locations: futures_util::stream::BoxStream<'static, object_store::Result<ObjPath>>,
    ) -> futures_util::stream::BoxStream<'static, object_store::Result<ObjPath>> {
        self.inner.delete_stream(locations)
    }
    fn list(
        &self,
        prefix: Option<&ObjPath>,
    ) -> futures_util::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>>
    {
        self.inner.list(prefix)
    }
    async fn list_with_delimiter(
        &self,
        prefix: Option<&ObjPath>,
    ) -> object_store::Result<object_store::ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }
    async fn copy_opts(
        &self,
        from: &ObjPath,
        to: &ObjPath,
        options: object_store::CopyOptions,
    ) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, options).await
    }
}

/// A configuration whose telemetry databases flush and compact as the
/// deterministic server's do.
pub(crate) fn config() -> crate::config::ServerConfig {
    crate::config::ServerConfig::load(
        crate::config::CliArgs::deterministic(),
        &crate::config::MapEnvironment::empty(),
    )
}

/// A rollup outbox scan reads its tables' blocks from the store once, 256
/// KiB a GET: after the cache dropped them (the idle release, or a sorted
/// run whose blocks compaction never cached), the first scan reads them and
/// the second reads none.
#[tokio::test(flavor = "multi_thread", worker_threads = 2, crate = "crate::test_rt")]
async fn a_second_outbox_scan_reads_no_table_from_the_store() {
    let store = Arc::new(TableGets::default());
    let cache = Arc::new(BlockCache::with_capacity(16 << 20));
    let rollup = super::UsageRollup::open_with_cache(store.clone(), "", &config(), cache.clone())
        .await
        .unwrap();
    for i in 0..400u32 {
        let key = format!("artifact-pending/2026-10/project/stream-{i:04}");
        rollup.db.put(key.as_bytes(), &[7u8; 200]).await.unwrap();
    }
    rollup.db.flush().await.unwrap();
    cache.release();
    let before = store.count();
    assert_eq!(rollup.pending_artifacts(1_000).await.unwrap().len(), 0);
    let first = store.count() - before;
    // The table's index and its ~80 KB of blocks in one 256 KiB read, not
    // one GET per 4 KiB block (about 21).
    assert!(
        (1..=3).contains(&first),
        "the first scan read {first} times from the store"
    );
    let before = store.count();
    rollup.pending_artifacts(1_000).await.unwrap();
    assert_eq!(
        store.count() - before,
        0,
        "the second scan reads from the cache"
    );
    rollup.db.close().await.unwrap();
}
