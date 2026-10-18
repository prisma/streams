//! A released block cache frees what it held, and fills again on reads.
#![cfg(test)]

use super::BlockCache;
use async_trait::async_trait;
use object_store::path::Path as ObjPath;
use object_store::{ObjectStore, memory::InMemory};
use slatedb::config::{FlushOptions, FlushType, Settings};
use slatedb::db_cache::foyer::FoyerCacheOptions;
use slatedb::db_cache::{CachedEntry, CachedKey, DbCache};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

/// One foyer shard, so what fits under the capacity does not depend on how
/// keys hash across shards.
fn options(max_capacity: u64) -> FoyerCacheOptions {
    FoyerCacheOptions {
        max_capacity,
        shards: 1,
    }
}

/// Counts the reads of compacted tables (L0 and sorted runs), the GETs a
/// cache miss costs. WAL replay, manifests and writes are not counted.
#[derive(Debug)]
struct TableGets {
    inner: InMemory,
    gets: AtomicU64,
}

impl TableGets {
    fn count(&self) -> u64 {
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

fn table_gets() -> Arc<TableGets> {
    Arc::new(TableGets {
        inner: InMemory::new(),
        gets: AtomicU64::new(0),
    })
}

/// A database over `store` whose block cache is `cache`, with no compactor:
/// the tables a test flushes stay the tables it reads.
async fn open(store: Arc<dyn ObjectStore>, cache: Arc<dyn DbCache>) -> slatedb::Db {
    slatedb::Db::builder("block-cache", store)
        .with_settings(Settings {
            compactor_options: None,
            ..Default::default()
        })
        .with_db_cache(cache)
        .build()
        .await
        .unwrap()
}

fn key(i: u32) -> Vec<u8> {
    format!("key-{i:06}").into_bytes()
}

fn value(i: u32) -> Vec<u8> {
    let mut v = vec![0u8; 1000];
    v[..4].copy_from_slice(&i.to_be_bytes());
    v
}

/// Writes keys `range` (1,000-byte values) and flushes them to one L0
/// table, which the flush inserts into the cache (cache-on-write).
async fn write_flushed(db: &slatedb::Db, range: std::ops::Range<u32>) {
    let mut batch = slatedb::WriteBatch::new();
    for i in range {
        batch.put(key(i), value(i));
    }
    let written = db.write(batch).await.unwrap();
    db.flush_with_options(FlushOptions {
        flush_type: FlushType::MemTable,
    })
    .await
    .unwrap();
    written.await_durable().await.unwrap();
}

async fn read(db: &slatedb::Db, i: u32) {
    let got = db.get(key(i)).await.unwrap().expect("a written key");
    assert_eq!(got.as_ref(), value(i).as_slice(), "key {i}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_release_frees_the_generation_that_held_the_blocks() {
    let cache = Arc::new(BlockCache::new(options(4 << 20)));
    let db = open(Arc::new(InMemory::new()), cache.clone()).await;
    write_flushed(&db, 0..256).await;
    let inserted = cache.inserted_bytes();
    assert!(inserted > 256_000, "the flush cached {inserted} bytes");
    let held = Arc::downgrade(&cache.current.load_full());

    assert_eq!(cache.release(), inserted);
    assert!(
        held.upgrade().is_none(),
        "the released generation is still referenced"
    );
    assert_eq!(cache.inserted_bytes(), 0);
    assert_eq!(cache.releases(), 1);
    assert_eq!(cache.release(), 0, "an empty generation releases nothing");
    assert_eq!(cache.releases(), 2);
    db.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_first_read_after_a_release_reads_the_store_once_and_the_next_hits() {
    let store = table_gets();
    let cache = Arc::new(BlockCache::new(options(4 << 20)));
    let db = open(store.clone(), cache.clone()).await;
    write_flushed(&db, 0..64).await;

    let before = store.count();
    read(&db, 7).await;
    assert_eq!(store.count(), before, "a flushed table is read from cache");

    cache.release();
    read(&db, 7).await;
    let after_release = store.count();
    assert!(
        after_release > before,
        "the first read after a release must read the store"
    );
    read(&db, 7).await;
    assert_eq!(
        store.count(),
        after_release,
        "the read after it hits the refilled generation"
    );
    db.close().await.unwrap();
}

/// The GETs of two passes over keys 0..96, which sit in the first of four
/// tables and take about half of a 256 KiB cache.
async fn sweep(db: &slatedb::Db, store: &TableGets) -> (u64, u64) {
    let mut passes = [0; 2];
    for pass in &mut passes {
        let before = store.count();
        for i in 0..96 {
            read(db, i).await;
        }
        *pass = store.count() - before;
    }
    (passes[0], passes[1])
}

/// foyer's `clear()` keeps its usage counter, so a cleared cache that was
/// full admits one entry at a time: its second pass would read the store
/// again. A released cache holds a working set of half its capacity, every
/// time it is released: after writes overfilled it, and after reads did.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_released_cache_admits_a_full_generation_again() {
    let store = table_gets();
    let cache = Arc::new(BlockCache::new(options(256 << 10)));
    let db = open(store.clone(), cache.clone()).await;
    for table in 0..4 {
        write_flushed(&db, table * 128..(table + 1) * 128).await;
    }
    assert!(
        cache.inserted_bytes() > 2 * (256 << 10),
        "writes overfill it"
    );

    cache.release();
    let first = sweep(&db, &store).await;
    for i in 128..512 {
        read(&db, i).await;
    }
    cache.release();
    let second = sweep(&db, &store).await;

    assert!(first.0 > 0, "a released cache misses first");
    assert_eq!(first.1, 0, "the working set stayed cached");
    assert_eq!(second, first, "the second release admits as much");
    assert_eq!(cache.entry_count(), 0, "foyer keeps no entry count");
    db.close().await.unwrap();
}

/// Records the entries a database inserts, so a test can hand a real one to
/// a cache directly.
struct Captured {
    inner: BlockCache,
    entries: std::sync::Mutex<Vec<CachedEntry>>,
}

#[async_trait]
impl DbCache for Captured {
    async fn get_block(&self, key: &CachedKey) -> Result<Option<CachedEntry>, slatedb::Error> {
        self.inner.get_block(key).await
    }
    async fn get_index(&self, key: &CachedKey) -> Result<Option<CachedEntry>, slatedb::Error> {
        self.inner.get_index(key).await
    }
    async fn get_filter(&self, key: &CachedKey) -> Result<Option<CachedEntry>, slatedb::Error> {
        self.inner.get_filter(key).await
    }
    async fn get_stats(&self, key: &CachedKey) -> Result<Option<CachedEntry>, slatedb::Error> {
        self.inner.get_stats(key).await
    }
    async fn insert(&self, key: CachedKey, value: CachedEntry) {
        self.entries.lock().unwrap().push(value.clone());
        self.inner.insert(key, value).await;
    }
    async fn remove(&self, key: &CachedKey) {
        self.inner.remove(key).await;
    }
    fn entry_count(&self) -> u64 {
        self.inner.entry_count()
    }
}

/// A data block, index or filter a flush cached.
async fn a_cached_entry() -> CachedEntry {
    let captured = Arc::new(Captured {
        inner: BlockCache::new(options(4 << 20)),
        entries: std::sync::Mutex::new(Vec::new()),
    });
    let db = open(Arc::new(InMemory::new()), captured.clone()).await;
    write_flushed(&db, 0..8).await;
    db.close().await.unwrap();
    let entries = captured.entries.lock().unwrap();
    entries.first().expect("the flush cached its table").clone()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn every_lookup_answers_from_the_generation_in_force() {
    let entry = a_cached_entry().await;
    let cache = BlockCache::new(options(4 << 20));
    let key: CachedKey = (slatedb::manifest::SsTableId::Wal(7), 0).into();
    cache.insert(key.clone(), entry.clone()).await;
    assert_eq!(cache.inserted_bytes(), entry.size() as u64);
    assert!(cache.get_block(&key).await.unwrap().is_some());
    assert!(cache.get_index(&key).await.unwrap().is_some());
    assert!(cache.get_filter(&key).await.unwrap().is_some());
    assert!(cache.get_stats(&key).await.unwrap().is_some());

    cache.remove(&key).await;
    assert!(cache.get_block(&key).await.unwrap().is_none(), "removed");
    cache.insert(key.clone(), entry).await;
    cache.release();
    assert!(cache.get_block(&key).await.unwrap().is_none(), "released");
}

/// The bytes a caller asks for bound the cache, not foyer's default; the
/// shards stay foyer's default (the host's CPU count).
#[test]
fn a_cache_with_a_capacity_is_bounded_at_the_bytes_asked_for() {
    let bytes = 3 << 20;
    let default = FoyerCacheOptions::default();
    assert_ne!(
        bytes, default.max_capacity,
        "the test asks for a non-default"
    );
    let cache = BlockCache::with_capacity(bytes);
    assert_eq!(cache.options.max_capacity, bytes);
    assert_eq!(cache.options.shards, default.shards);
}
