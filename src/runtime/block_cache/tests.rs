//! A released block cache frees what it held, and fills again on reads.
#![cfg(test)]

use super::BlockCache;
use async_trait::async_trait;
use bytes::Bytes;
use futures_util::{StreamExt, future, stream};
use object_store::path::Path as ObjPath;
use object_store::{GetResult, GetResultPayload, ObjectStore, memory::InMemory};
use slatedb::config::{FlushOptions, FlushType, ScanOptions, Settings};
use slatedb::db_cache::foyer::FoyerCacheOptions;
use slatedb::db_cache::{CachedEntry, CachedKey, DbCache};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::Duration;

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
    /// When set, every table read answers with its body at the head of a
    /// buffer `PADDING` bytes longer, and this counts the bytes such buffers
    /// keep alive.
    padded: Option<Arc<AtomicUsize>>,
}

impl TableGets {
    fn count(&self) -> u64 {
        self.gets.load(Ordering::SeqCst)
    }
}

/// A GET reply's body is a slice of a larger buffer: hyper hands out a
/// slice of its connection read buffer (8 KiB up to 408 KiB), and a
/// multi-block read hands every block a slice of one reply.
const PADDING: usize = 64 << 10;

/// A table read's body at the head of a buffer `PADDING` bytes longer,
/// counted in `alive` while it lives.
struct PaddedBody {
    buf: Vec<u8>,
    len: usize,
    alive: Arc<AtomicUsize>,
}

impl PaddedBody {
    fn new(body: &[u8], alive: Arc<AtomicUsize>) -> Self {
        let mut buf = vec![0; body.len() + PADDING];
        buf[..body.len()].copy_from_slice(body);
        alive.fetch_add(buf.len(), Ordering::SeqCst);
        Self {
            buf,
            len: body.len(),
            alive,
        }
    }
}

impl AsRef<[u8]> for PaddedBody {
    fn as_ref(&self) -> &[u8] {
        &self.buf[..self.len]
    }
}

impl Drop for PaddedBody {
    fn drop(&mut self) {
        self.alive.fetch_sub(self.buf.len(), Ordering::SeqCst);
    }
}

/// `reply` answered as one chunk that slices a `PaddedBody`.
async fn padded(reply: GetResult, alive: Arc<AtomicUsize>) -> object_store::Result<GetResult> {
    let meta = reply.meta.clone();
    let range = reply.range.clone();
    let attributes = reply.attributes.clone();
    let extensions = reply.extensions.clone();
    let body = reply.bytes().await?;
    let body = Bytes::from_owner(PaddedBody::new(&body, alive));
    Ok(GetResult {
        payload: GetResultPayload::Stream(stream::once(future::ready(Ok(body))).boxed()),
        meta,
        range,
        attributes,
        extensions,
    })
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
        if !location.as_ref().contains("compacted") {
            return self.inner.get_opts(location, options).await;
        }
        self.gets.fetch_add(1, Ordering::SeqCst);
        let reply = self.inner.get_opts(location, options).await?;
        match &self.padded {
            Some(alive) => padded(reply, alive.clone()).await,
            None => Ok(reply),
        }
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
        padded: None,
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2, crate = "crate::test_rt")]
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2, crate = "crate::test_rt")]
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
#[tokio::test(flavor = "multi_thread", worker_threads = 2, crate = "crate::test_rt")]
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

/// Reads every key of 0..1000 in order with a read-ahead of many blocks
/// that caches what it reads: one GET serves a run of blocks, and the scan
/// inserts each of them. Returns the rows seen.
async fn scan_all(db: &slatedb::Db) -> u32 {
    let options = ScanOptions::new()
        .with_read_ahead_bytes(1 << 20)
        .with_cache_blocks(true);
    let mut rows = db
        .scan_with_options(key(0)..key(1000), &options)
        .await
        .unwrap();
    let mut seen = 0;
    while let Some(row) = rows.next().await.unwrap() {
        assert_eq!(row.key.as_ref(), key(seen).as_slice());
        assert_eq!(row.value.as_ref(), value(seen).as_slice(), "key {seen}");
        seen += 1;
    }
    seen
}

/// The padded bytes still alive once work in flight has let go of them
/// (waits up to a second).
async fn settled(alive: &AtomicUsize) -> usize {
    for _ in 0..100 {
        if alive.load(Ordering::SeqCst) == 0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    alive.load(Ordering::SeqCst)
}

/// A read that misses caches a copy of what it read, never a slice of the
/// GET reply: a cached block or index would keep the whole reply buffer
/// alive for as long as it stays cached. Point gets fill the cache through
/// `fetch_*`, a scan that caches its blocks through `insert`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2, crate = "crate::test_rt")]
async fn a_cached_read_keeps_no_reply_buffer_alive() {
    let alive = Arc::new(AtomicUsize::new(0));
    let store = Arc::new(TableGets {
        inner: InMemory::new(),
        gets: AtomicU64::new(0),
        padded: Some(alive.clone()),
    });
    let cache = Arc::new(BlockCache::new(options(64 << 20)));
    let db = open(store.clone(), cache.clone()).await;
    write_flushed(&db, 0..1000).await;

    cache.release();
    for i in 0..1000 {
        read(&db, i).await;
    }
    let fetched = store.count();
    assert!(fetched > 2, "the gets read the store ({fetched} GETs)");
    assert_eq!(
        settled(&alive).await,
        0,
        "point gets cached slices of their replies"
    );
    for i in 0..1000 {
        read(&db, i).await;
    }
    assert_eq!(store.count(), fetched, "the gets' copies stayed cached");

    cache.release();
    assert_eq!(scan_all(&db).await, 1000);
    let scanned = store.count();
    assert!(scanned > fetched, "the scan read the store");
    assert_eq!(
        settled(&alive).await,
        0,
        "a scan cached slices of its replies"
    );
    assert_eq!(scan_all(&db).await, 1000);
    assert_eq!(store.count(), scanned, "the scan's copies stayed cached");
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2, crate = "crate::test_rt")]
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
