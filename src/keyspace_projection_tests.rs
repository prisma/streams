//! ASM-SLATEDB-PROJECTION (S4's first step): a projected clone of an
//! initialized shard log, built with Streams' merge operator, holds exactly
//! the source's rows in its range at the checkpoint, merged values and
//! deletions included; a child's own operand merges onto an ancestral value,
//! through a compaction of the projected tables too. A checkpoint of the
//! durable state alone leaves data in the WAL, which a projection refuses.

use std::ops::{Bound, RangeBounds};
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use object_store::ObjectStore;
use object_store::memory::InMemory;
use slatedb::admin::AdminBuilder;
use slatedb::compactor::{CompactionSpec, CompactionStatus, SourceId};
use slatedb::config::{CheckpointOptions, CheckpointScope, FlushOptions, FlushType};
use slatedb::{CloneSourceSpec, Db};

use crate::keyspace::{SENTINEL, db_builder, stream_row};
use crate::spill::pending::pending_key;

const STREAMS: u8 = 64;
const SPLIT: u8 = 0x80;

fn hash(i: u8) -> [u8; 16] {
    let mut h = [i.wrapping_mul(4); 16];
    h[15] = i;
    h
}

fn row(i: u8, tag: u8) -> Vec<u8> {
    let mut k = stream_row(&hash(i), 1);
    k.push(tag);
    k
}

fn memtable() -> FlushOptions {
    FlushOptions {
        flush_type: FlushType::MemTable,
    }
}

async fn rows(db: &Db) -> Vec<(Bytes, Bytes)> {
    let mut scan = db.scan(..).await.unwrap();
    let mut out = Vec::new();
    while let Some(kv) = scan.next().await.unwrap() {
        out.push((kv.key, kv.value));
    }
    out
}

/// The source: tails, pending operands across an L0 table and the WAL,
/// columns, tombstones, and the sentinel; returns it open.
async fn source(store: &Arc<dyn ObjectStore>) -> Db {
    let db = db_builder("p/src", store.clone()).build().await.unwrap();
    for i in 0..STREAMS {
        db.put(row(i, b't'), format!("tail-{i}")).await.unwrap();
        db.merge(pending_key(&hash(i)), format!("a{i};"))
            .await
            .unwrap();
    }
    db.put(SENTINEL, b"sentinel").await.unwrap();
    db.flush_with_options(memtable()).await.unwrap();
    for i in 0..STREAMS {
        db.merge(pending_key(&hash(i)), format!("b{i};"))
            .await
            .unwrap();
        db.put(row(i, b'c'), format!("col-{i}")).await.unwrap();
        if i % 5 == 0 {
            db.delete(row(i, b't')).await.unwrap();
        }
    }
    db.flush().await.unwrap();
    db
}

fn spec(
    path: &str,
    checkpoint: &slatedb::CheckpointCreateResult,
    range: (Bound<Bytes>, Bound<Bytes>),
) -> CloneSourceSpec {
    CloneSourceSpec::with_checkpoint(path, checkpoint.id).with_projection_range(range)
}

fn low() -> (Bound<Bytes>, Bound<Bytes>) {
    let split = Bytes::from(stream_row(&[SPLIT; 16], 0));
    (Bound::Unbounded, Bound::Excluded(split))
}

fn high() -> (Bound<Bytes>, Bound<Bytes>) {
    let split = Bytes::from(stream_row(&[SPLIT; 16], 0));
    (Bound::Included(split), Bound::Unbounded)
}

async fn clone_into(
    store: &Arc<dyn ObjectStore>,
    to: &str,
    from: CloneSourceSpec,
) -> Result<(), slatedb::Error> {
    AdminBuilder::new(to, store.clone())
        .build()
        .create_clone_builder_from_source(from)
        .build()
        .await
}

/// A store that records each request's kind and path, so the test can
/// print what the pause's checkpoint, clones and child opens cost.
#[derive(Debug, Default)]
struct Recording {
    inner: InMemory,
    log: std::sync::Mutex<Vec<String>>,
    latency: std::sync::atomic::AtomicBool,
    since: std::sync::Mutex<Option<std::time::Instant>>,
}

const RTT: Duration = Duration::from_millis(10);

impl Recording {
    fn note(&self, op: &str, path: &object_store::path::Path) {
        self.log.lock().unwrap().push(format!("{op} {path}"));
    }

    async fn rtt(&self) {
        if self.latency.load(std::sync::atomic::Ordering::Relaxed) {
            tokio::time::sleep(RTT).await;
        }
    }

    fn take(&self) -> Vec<String> {
        *self.since.lock().unwrap() = Some(std::time::Instant::now());
        std::mem::take(&mut *self.log.lock().unwrap())
    }
}

impl std::fmt::Display for Recording {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "recording")
    }
}

#[async_trait::async_trait]
impl ObjectStore for Recording {
    async fn put_opts(
        &self,
        location: &object_store::path::Path,
        payload: object_store::PutPayload,
        opts: object_store::PutOptions,
    ) -> object_store::Result<object_store::PutResult> {
        let op = if matches!(opts.mode, object_store::PutMode::Overwrite) {
            "PUT"
        } else {
            "CAS"
        };
        self.note(op, location);
        self.rtt().await;
        self.inner.put_opts(location, payload, opts).await
    }
    async fn put_multipart_opts(
        &self,
        location: &object_store::path::Path,
        opts: object_store::PutMultipartOptions,
    ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
        self.note("MPU", location);
        self.inner.put_multipart_opts(location, opts).await
    }
    async fn get_opts(
        &self,
        location: &object_store::path::Path,
        options: object_store::GetOptions,
    ) -> object_store::Result<object_store::GetResult> {
        self.note(if options.head { "HEAD" } else { "GET" }, location);
        self.rtt().await;
        self.inner.get_opts(location, options).await
    }
    fn delete_stream(
        &self,
        locations: futures_core::stream::BoxStream<
            'static,
            object_store::Result<object_store::path::Path>,
        >,
    ) -> futures_core::stream::BoxStream<'static, object_store::Result<object_store::path::Path>>
    {
        self.log.lock().unwrap().push("DELETE".into());
        self.inner.delete_stream(locations)
    }
    fn list(
        &self,
        prefix: Option<&object_store::path::Path>,
    ) -> futures_core::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>>
    {
        self.note("LIST", &prefix.cloned().unwrap_or_default());
        self.inner.list(prefix)
    }
    async fn list_with_delimiter(
        &self,
        prefix: Option<&object_store::path::Path>,
    ) -> object_store::Result<object_store::ListResult> {
        self.note("LISTD", &prefix.cloned().unwrap_or_default());
        self.rtt().await;
        self.inner.list_with_delimiter(prefix).await
    }
    async fn copy_opts(
        &self,
        from: &object_store::path::Path,
        to: &object_store::path::Path,
        opts: object_store::CopyOptions,
    ) -> object_store::Result<()> {
        self.note("COPY", to);
        self.inner.copy_opts(from, to, opts).await
    }
}

fn print_phase(rec: &Recording, phase: &str) {
    let elapsed = rec
        .since
        .lock()
        .unwrap()
        .map(|t| t.elapsed())
        .unwrap_or_default();
    let log = rec.take();
    let mut kinds = std::collections::BTreeMap::<&str, usize>::new();
    for l in &log {
        *kinds.entry(l.split(' ').next().unwrap_or("")).or_default() += 1;
    }
    println!(
        "PHASE {phase}: {} requests {kinds:?}, {:.1} sequential round trips",
        log.len(),
        elapsed.as_secs_f64() / RTT.as_secs_f64()
    );
    for l in &log {
        println!("  {l}");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2, crate = "crate::test_rt")]
async fn a_projected_clone_holds_exactly_the_sources_rows_in_its_range() {
    let rec = Arc::new(Recording::default());
    let store: Arc<dyn ObjectStore> = rec.clone();
    let src = source(&store).await;
    rec.latency
        .store(true, std::sync::atomic::Ordering::Relaxed);
    rec.take();

    let durable = src
        .create_checkpoint(CheckpointScope::Durable, &CheckpointOptions::default())
        .await
        .unwrap();
    let refused = clone_into(&store, "p/refused", spec("p/src", &durable, low())).await;
    assert!(
        refused
            .as_ref()
            .is_err_and(|e| e.to_string().contains("WAL")),
        "a durable-scope checkpoint leaves data in the WAL: {refused:?}"
    );

    rec.take();
    let all = src
        .create_checkpoint(CheckpointScope::All, &CheckpointOptions::default())
        .await
        .unwrap();
    print_phase(&rec, "checkpoint(All)");
    let at_checkpoint = rows(&src).await;
    // Writes after the checkpoint reach neither child.
    for i in 0..STREAMS {
        src.merge(pending_key(&hash(i)), b"late;".to_vec())
            .await
            .unwrap();
    }
    src.flush_with_options(memtable()).await.unwrap();

    rec.take();
    // In parallel, as the pause would run them: both CAS the source's
    // manifest (each clone's final checkpoint).
    let (l, h) = futures_util::future::join(
        clone_into(&store, "p/low", spec("p/src", &all, low())),
        clone_into(&store, "p/high", spec("p/src", &all, high())),
    )
    .await;
    l.unwrap();
    h.unwrap();
    print_phase(&rec, "two projected clones");
    let split = stream_row(&[SPLIT; 16], 0);
    let (want_low, want_high): (Vec<_>, Vec<_>) = at_checkpoint
        .iter()
        .cloned()
        .partition(|(k, _)| k[..] < split[..]);
    assert!(want_low.len() > 80 && want_high.len() > 80);
    assert_eq!(want_high.last().map(|(k, _)| &k[..]), Some(&SENTINEL[..]));
    assert_eq!(
        want_low
            .iter()
            .find(|(k, _)| k[..] == pending_key(&hash(3))[..])
            .map(|(_, v)| v.clone()),
        Some(Bytes::from_static(b"a3;b3;"))
    );
    assert!(!want_low.iter().any(|(k, _)| k[..] == row(5, b't')[..]));

    let low_db = db_builder("p/low", store.clone()).build().await.unwrap();
    print_phase(&rec, "open low child");
    assert_eq!(rows(&low_db).await, want_low, "the low child");
    low_db.close().await.unwrap();
    let high_db = db_builder("p/high", store.clone()).build().await.unwrap();
    assert_eq!(rows(&high_db).await, want_high, "the high child");
    high_db.close().await.unwrap();
    src.close().await.unwrap();

    child_merges_through_compaction(&store, want_low).await;
}

/// The low child's own operand merges onto the ancestral pending value,
/// read before and after a compaction of every table it holds.
async fn child_merges_through_compaction(
    store: &Arc<dyn ObjectStore>,
    mut want: Vec<(Bytes, Bytes)>,
) {
    let db = db_builder("p/low", store.clone())
        .with_settings(compacting())
        .build()
        .await
        .unwrap();
    for i in 0..STREAMS {
        if stream_row(&hash(i), 0) < stream_row(&[SPLIT; 16], 0) {
            db.merge(pending_key(&hash(i)), format!("c{i};"))
                .await
                .unwrap();
        }
    }
    db.flush_with_options(memtable()).await.unwrap();
    for (k, v) in want.iter_mut() {
        if k.len() == 18 && k[17] == b'P' {
            let i = k[15];
            *v = Bytes::from(format!("a{i};b{i};c{i};"));
        }
    }
    assert_eq!(rows(&db).await, want, "child operands before compaction");
    compact_all(store, "p/low", &db).await;
    assert_eq!(rows(&db).await, want, "child operands after compaction");
    db.close().await.unwrap();
}

/// Settings whose compactor runs submitted compactions promptly.
fn compacting() -> slatedb::config::Settings {
    slatedb::config::Settings {
        compactor_options: Some(slatedb::config::CompactorOptions {
            poll_interval: Duration::from_millis(100),
            commit_compacted_interval: Duration::from_millis(100),
            worker: Some(slatedb::config::CompactionWorkerOptions {
                compactions_poll_interval: Duration::from_millis(100),
                ..Default::default()
            }),
            ..Default::default()
        }),
        ..Default::default()
    }
}

/// Compact every L0 table and sorted run `db` (at `path`) holds into one
/// sorted run, and wait until the manifest shows exactly that.
async fn compact_all(store: &Arc<dyn ObjectStore>, path: &str, db: &Db) {
    let manifest = db.manifest();
    let mut sources: Vec<SourceId> = manifest
        .l0()
        .iter()
        .map(|v| SourceId::SstView(v.id))
        .collect();
    let runs: Vec<u32> = manifest.compacted().iter().map(|r| r.id).collect();
    sources.extend(runs.iter().map(|&id| SourceId::SortedRun(id)));
    assert!(sources.len() >= 2, "projected and own tables: {sources:?}");
    let destination = runs.iter().copied().min().unwrap_or(0);
    let admin = AdminBuilder::new(path, store.clone()).build();
    let spec = CompactionSpec::new(sources, destination);
    let mut compaction = None;
    for _ in 0..100 {
        if let Ok(c) = admin.submit_compaction(spec.clone()).await {
            compaction = Some(c);
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let compaction = compaction.expect("the compactor accepts the submission");
    let mut completed = false;
    for _ in 0..200 {
        tokio::time::sleep(Duration::from_millis(100)).await;
        match admin.read_compaction(compaction.id(), None).await {
            Ok(Some(c)) if c.status() == CompactionStatus::Failed => panic!("compaction failed"),
            Ok(Some(c)) if c.status() != CompactionStatus::Completed => continue,
            _ => {
                completed = true;
                break;
            }
        }
    }
    assert!(completed, "the compaction finished");
    db.refresh_manifest().await.unwrap();
    let after = db.manifest();
    assert!(after.l0().is_empty(), "every L0 table compacted");
    assert_eq!(after.compacted().len(), 1, "one sorted run");
}

/// Depth 2, the 1 -> 2 -> 4 ladder: a projected clone of a projected clone
/// holds exactly the parent's rows in its range at the parent's checkpoint
/// (the grandparent's rows and the parent's own writes, merged); it
/// references both ancestors, each holding the final checkpoint the clones
/// wrote; and a compaction of every table it holds leaves it referencing
/// no ancestor SST.
#[tokio::test(flavor = "multi_thread", worker_threads = 2, crate = "crate::test_rt")]
async fn a_projected_clone_of_a_projected_clone_holds_its_range_of_both() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let src = source(&store).await;
    let all = src
        .create_checkpoint(CheckpointScope::All, &CheckpointOptions::default())
        .await
        .unwrap();
    src.close().await.unwrap();
    clone_into(&store, "p/low", spec("p/src", &all, low()))
        .await
        .unwrap();
    let quarter = stream_row(&[0x40; 16], 0);
    let child = db_builder("p/low", store.clone()).build().await.unwrap();
    for i in 0..STREAMS {
        if stream_row(&hash(i), 0) < quarter {
            child
                .merge(pending_key(&hash(i)), format!("c{i};"))
                .await
                .unwrap();
        }
    }
    // The pause's sequence, twice as a retry would run it: the WAL flushed,
    // then a checkpoint of everything (which flushes the memtable). Each
    // checkpoint leaves no data WAL past its replay point, so each clones.
    // (A checkpoint of everything with writes still buffered in the WAL
    // writes them past the point the memtable flush covers, and the fork
    // then refuses every projected clone of the child until newer writes
    // are flushed: the pause flushes the WAL first.)
    let lowest = || {
        (
            Bound::Unbounded,
            Bound::Excluded(Bytes::from(quarter.clone())),
        )
    };
    for attempt in 0..2 {
        child.flush().await.unwrap();
        let cp = child
            .create_checkpoint(CheckpointScope::All, &CheckpointOptions::default())
            .await
            .unwrap();
        clone_into(
            &store,
            &format!("p/lowest-{attempt}"),
            spec("p/low", &cp, lowest()),
        )
        .await
        .unwrap();
    }
    let want: Vec<_> = rows(&child)
        .await
        .into_iter()
        .filter(|(k, _)| k[..] < quarter[..])
        .collect();
    child.close().await.unwrap();
    assert!(want.len() > 40);
    assert!(want.contains(&(
        Bytes::from(pending_key(&hash(1))),
        Bytes::from_static(b"a1;b1;c1;")
    )));
    let grand = db_builder("p/lowest-1", store.clone())
        .with_settings(compacting())
        .build()
        .await
        .unwrap();
    assert_eq!(rows(&grand).await, want, "the grandchild");
    let ancestors: Vec<(String, bool)> = grand
        .manifest()
        .external_dbs()
        .iter()
        .map(|e| (e.path.clone(), e.final_checkpoint_id.is_some()))
        .collect();
    assert_eq!(
        ancestors,
        vec![("p/src".to_string(), true), ("p/low".to_string(), true)],
        "both ancestors, each with a final checkpoint"
    );
    for e in grand.manifest().external_dbs() {
        let held = AdminBuilder::new(e.path.as_str(), store.clone())
            .build()
            .list_checkpoints(None)
            .await
            .unwrap();
        assert!(
            held.iter().any(|c| Some(c.id) == e.final_checkpoint_id),
            "{} holds the grandchild's final checkpoint",
            e.path
        );
    }
    compact_all(&store, "p/lowest-1", &grand).await;
    assert_eq!(rows(&grand).await, want, "the grandchild after compaction");
    let referenced: Vec<(String, usize)> = grand
        .manifest()
        .external_dbs()
        .iter()
        .map(|e| (e.path.clone(), e.sst_ids.len()))
        .collect();
    assert_eq!(
        referenced,
        vec![("p/src".to_string(), 0), ("p/low".to_string(), 0)],
        "no ancestor SST referenced (detach GC drops the entries later)"
    );
    grand.close().await.unwrap();
}

/// The fork's checkpoint-coverage case (FORK.md known issue K1, fixed by patch 10): a checkpoint
/// of everything taken while writes are still buffered in the WAL writer stamps the frozen
/// memtable with the WAL id from before its own flush, so those writes' WAL lies past the
/// checkpoint's replay point although the writes are in its L0. The clone refuses a source's WAL
/// only for a record above the source's last L0 sequence number, so a projected clone of that
/// checkpoint succeeds and holds exactly the source's buffered rows in its range. A durable
/// checkpoint of writes whose WAL is uploaded but whose memtable is not flushed names WAL records
/// above its L0, and a projected clone of it is still refused.
#[tokio::test(flavor = "multi_thread", worker_threads = 2, crate = "crate::test_rt")]
async fn a_projected_clone_takes_buffered_writes_from_l0_and_refuses_one_above_it() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    // No background WAL flush, and writes that do not wait for one: they
    // stay buffered in the WAL writer until something flushes it.
    let quiet = || slatedb::config::Settings {
        flush_interval: Some(Duration::from_secs(3600)),
        ..Default::default()
    };
    let db = db_builder("p/buffered", store.clone())
        .with_settings(quiet())
        .build()
        .await
        .unwrap();
    buffered_writes(&db).await;
    let everything = db
        .create_checkpoint(CheckpointScope::All, &CheckpointOptions::default())
        .await
        .unwrap();
    clone_into(&store, "p/taken", spec("p/buffered", &everything, low()))
        .await
        .expect("every buffered write is in the checkpoint's L0");
    let taken = db_builder("p/taken", store.clone()).build().await.unwrap();
    let in_low = |key: &Bytes| low().contains(key);
    let expected: Vec<_> = rows(&db)
        .await
        .into_iter()
        .filter(|(k, _)| in_low(k))
        .collect();
    let buffered: Vec<Bytes> = (0..STREAMS)
        .map(|i| Bytes::from(pending_key(&hash(i))))
        .filter(|key| in_low(key))
        .collect();
    // Streams 0..=32: `hash(32)` shares the split's first 15 bytes (0x80) and sorts below it.
    assert_eq!(buffered.len(), 33, "the writes below the split");
    assert!(
        buffered
            .iter()
            .all(|key| expected.iter().any(|(row, _)| row == key)),
        "every buffered write in the range is among the source's rows"
    );
    assert_eq!(
        rows(&taken).await,
        expected,
        "the clone holds the buffered rows in its range"
    );
    taken.close().await.unwrap();

    let above = db_builder("p/above", store.clone())
        .with_settings(quiet())
        .build()
        .await
        .unwrap();
    buffered_writes(&above).await;
    above.flush().await.unwrap();
    let durable = above
        .create_checkpoint(CheckpointScope::Durable, &CheckpointOptions::default())
        .await
        .unwrap();
    let refused = clone_into(&store, "p/refused", spec("p/above", &durable, low())).await;
    assert!(
        refused
            .as_ref()
            .is_err_and(|e| e.to_string().contains("WAL")),
        "a WAL record above the source's L0: {refused:?}"
    );
    db.close().await.unwrap();
    above.close().await.unwrap();
}

/// Merges on every stream's pending row, returning without waiting for the
/// WAL to be durable (the default): with no background flush they stay
/// buffered in the WAL writer.
async fn buffered_writes(db: &Db) {
    for i in 0..STREAMS {
        db.merge_with_options(
            pending_key(&hash(i)),
            format!("a{i};"),
            &slatedb::config::MergeOptions::default(),
            &slatedb::config::WriteOptions::default(),
        )
        .await
        .unwrap();
    }
}
