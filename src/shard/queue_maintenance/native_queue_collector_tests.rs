//! The native-queue collector (docs/QUEUES-DESIGN.md §8.2) over in-memory
//! stores: the trigger at its boundaries, the debt a pass captures, the
//! compaction it submits and waits for, the garbage collection it waits
//! for, and the tick that stops its loops.
#![cfg(test)]
use std::sync::Arc;
use std::time::Duration;

use object_store::ObjectStore;
use slatedb::Db;
use slatedb::compactor::{CompactionSpec, SourceId};
use tokio::sync::mpsc;

use super::{QUEUE_TREE, pass_due, single_pass_fits};
use crate::native_queue::rows::StateRow;
use crate::native_queue::{NqOp, QueueConfig, keys};
use crate::shard::{AbsorbSignal, ShardConfig, ShardEngine};

const MIB: u64 = 1 << 20;
const MIN: i64 = 60_000;
const HOUR: i64 = crate::native_queue::HOUR_MS;
/// How long a test waits on a collector step that should finish quickly.
const STEP_WITHIN: Duration = Duration::from_secs(30);

/// A raw key in the queue segment's tree (layout 6).
fn qk(name: &[u8]) -> Vec<u8> {
    [&QUEUE_TREE[..], name].concat()
}

/// A raw key in the stream segment's tree.
fn sk(name: &[u8]) -> Vec<u8> {
    [&[crate::keyspace::STREAM][..], name].concat()
}

/// The queue tree's L0 view ids in `manifest` (none before its first
/// flush).
fn queue_l0(manifest: &slatedb::VersionedManifest) -> Vec<slatedb::manifest::SsTableView> {
    manifest
        .segment(&QUEUE_TREE)
        .map(|t| t.l0().iter().cloned().collect())
        .unwrap_or_default()
}

/// The queue tree's sorted runs in `manifest`.
fn queue_runs(manifest: &slatedb::VersionedManifest) -> Vec<slatedb::manifest::SortedRun> {
    manifest
        .segment(&QUEUE_TREE)
        .map(|t| t.compacted().clone())
        .unwrap_or_default()
}

/// A tiered spec in the queue tree.
fn queue_spec(sources: Vec<SourceId>, destination: u32) -> CompactionSpec {
    CompactionSpec::for_segment(bytes::Bytes::from_static(&QUEUE_TREE), sources, destination)
}

struct Rig {
    engine: Arc<ShardEngine>,
    db: Arc<Db>,
    store: Arc<dyn ObjectStore>,
    _absorb: mpsc::Receiver<AbsorbSignal>,
}

/// An engine over a fresh in-memory store; a `name` with a `/` puts the
/// database at the shard path the collector's admin reads.
async fn rig(name: &str, settings: Option<slatedb::config::Settings>) -> Rig {
    let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    rig_over(name, settings, store).await
}

/// An engine over `store`.
async fn rig_over(
    name: &str,
    settings: Option<slatedb::config::Settings>,
    store: Arc<dyn ObjectStore>,
) -> Rig {
    let settings = settings.unwrap_or_default();
    let builder =
        crate::native_queue::compaction_guard::shard_log_builder(name, store.clone(), settings);
    let db = Arc::new(builder.build().await.unwrap());
    let (tx, absorb) = mpsc::channel(1);
    let engine = ShardEngine::start(
        name.to_owned(),
        db.clone(),
        store.clone(),
        ShardConfig::default(),
        tx,
        None,
        Default::default(),
    );
    Rig {
        engine,
        db,
        store,
        _absorb: absorb,
    }
}

/// A compactor that validates and runs submitted compactions every `poll`.
fn compactor_every(poll: Duration) -> slatedb::config::Settings {
    slatedb::config::Settings {
        flush_interval: Some(Duration::from_millis(10)),
        compactor_options: Some(slatedb::config::CompactorOptions {
            poll_interval: poll,
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

/// Submit once the compactor has created its compactions file.
async fn submit(
    admin: &slatedb::admin::Admin,
    spec: &CompactionSpec,
) -> slatedb::compactor::Compaction {
    for _ in 0..100 {
        if let Ok(c) = admin.submit_compaction(spec.clone()).await {
            return c;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("the compactor never accepted a submission");
}

#[test]
fn native_queue_collection_starts_on_size_or_age_and_never_without_debt() {
    let now = 100 * HOUR;
    assert!(!pass_due(0, None, now, 0), "no debt, no pass");
    assert!(pass_due(256 * MIB, Some(now), now, 0), "256 MiB at once");
    assert!(!pass_due(256 * MIB - 1, Some(now), now, 0));
    // 16 MiB once 15 minutes old.
    assert!(pass_due(16 * MIB, Some(now - 15 * MIN), now, 0));
    assert!(!pass_due(16 * MIB, Some(now - 15 * MIN + 1), now, 0));
    assert!(!pass_due(16 * MIB - 1, Some(now - 15 * MIN), now, 0));
    // Smaller debt waits a day.
    assert!(pass_due(1, Some(now - 24 * HOUR), now, 0));
    assert!(!pass_due(1, Some(now - 24 * HOUR + 1), now, 0));
    // The age trigger's floor rises to a twentieth of the compacted log.
    assert!(pass_due(100 * MIB, Some(now - 20 * MIN), now, 1_024 * MIB));
    assert!(!pass_due(
        100 * MIB,
        Some(now - 20 * MIN),
        now,
        20_480 * MIB
    ));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn native_queue_collection_captures_every_roster_queue_with_debt_and_only_those() {
    let r = rig("nqc-capture", None).await;
    let oldest = crate::shard::now_ms();
    for (tag, debt) in [(1u8, 300u64), (2, 0)] {
        let s = StateRow {
            debt_bytes: debt,
            debt_generation: 7,
            debt_oldest_at: oldest,
            ..StateRow::default()
        };
        r.db.put(keys::state(&[tag; 16]), s.encode()).await.unwrap();
    }
    // Three roster rows the sweeper never finds due; the third queue has no
    // state row.
    for tag in [1u8, 2, 3] {
        let key = keys::roster(&[tag; 16]);
        r.db.put(key, i64::MAX.to_be_bytes()).await.unwrap();
    }
    let got = r.engine.capture_debt().await.unwrap();
    let got: Vec<_> = got
        .iter()
        .map(|(c, at)| (c.hash, c.generation, c.bytes, *at))
        .collect();
    assert_eq!(got, vec![([1; 16], 7, 300, oldest)]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn native_queue_maintenance_stops_ticking_once_its_close_signal_is_raised() {
    let r = rig("nqc-tick", None).await;
    let (tx, mut closed) = tokio::sync::watch::channel(false);
    let mut interval = tokio::time::interval(Duration::from_millis(1));
    assert!(r.engine.queue_tick(&mut interval, &mut closed).await);
    tx.send(true).unwrap();
    assert!(
        !r.engine.queue_tick(&mut interval, &mut closed).await,
        "a raised close signal stops the loop although the engine is open"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn native_queue_collection_waits_until_every_superseded_sst_but_the_residual_is_gone() {
    let r = rig("nqc-gc", None).await;
    let base = crate::sharddir::shard_db_path("nqc-gc");
    for id in ["a", "b", "c"] {
        let path = object_store::path::Path::from(format!("{base}/compacted/{id}.sst"));
        let body = object_store::PutPayload::from_static(b"sst");
        object_store::ObjectStoreExt::put(&*r.store, &path, body)
            .await
            .unwrap();
    }
    let ids = |v: &[&str]| v.iter().map(|s| (*s).to_owned()).collect::<Vec<_>>();
    let e = &r.engine;
    let (two, three) = (ids(&["a", "b", "gone"]), ids(&["a", "b", "c"]));
    assert_eq!(e.superseded_left(&two, None).await, 2);
    let residual = Some("b".to_owned());
    let left = e.superseded_left(&three, residual.as_ref()).await;
    assert_eq!(left, 2, "the residual does not count");
    let (gone, kept) = (ids(&["gone"]), ids(&["a"]));
    let done = tokio::time::timeout(STEP_WITHIN, e.await_collection(&gone, &[]));
    assert_eq!(done.await.unwrap(), Ok(()), "nothing left: done at once");
    let present = tokio::time::timeout(Duration::from_secs(1), e.await_collection(&kept, &[]));
    assert!(
        present.await.is_err(),
        "an SST still present keeps it waiting"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn native_queue_collection_compacts_every_l0_into_the_last_run_and_names_what_it_superseded()
{
    let r = rig(
        "t/nqc-compact",
        Some(compactor_every(Duration::from_millis(100))),
    )
    .await;
    r.db.put(qk(b"k"), b"v").await.unwrap();
    let memtable = slatedb::config::FlushOptions {
        flush_type: slatedb::config::FlushType::MemTable,
    };
    r.db.flush_with_options(memtable).await.unwrap();
    let l0: Vec<String> = queue_l0(&r.db.manifest())
        .iter()
        .map(|v| v.sst.id.unwrap_compacted_id().to_string())
        .collect();
    assert!(!l0.is_empty(), "the flush wrote an L0 SST");
    let pass = tokio::time::timeout(STEP_WITHIN, r.engine.compact_queue_tree());
    let superseded = pass.await.unwrap().unwrap().superseded;
    for id in &l0 {
        assert!(superseded.contains(id), "{id} is named superseded");
    }
    let manifest = r.engine.queue_admin().read_manifest(None).await;
    let manifest = manifest.unwrap().expect("a manifest");
    assert!(queue_l0(&manifest).is_empty(), "every L0 SST was compacted");
    assert_eq!(queue_runs(&manifest).len(), 1, "into one sorted run");
}

/// Layout 6A's point: a pass compacts the queue tree alone. A stream row's
/// L0 table is neither named, rewritten nor superseded, and the stream
/// tree's runs are untouched.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn native_queue_collection_never_names_or_rewrites_the_stream_tree() {
    let r = rig(
        "t/nqc-trees",
        Some(compactor_every(Duration::from_millis(100))),
    )
    .await;
    r.db.put(qk(b"queue-row"), b"v").await.unwrap();
    r.db.put(sk(b"stream-row"), b"v").await.unwrap();
    let memtable = slatedb::config::FlushOptions {
        flush_type: slatedb::config::FlushType::MemTable,
    };
    r.db.flush_with_options(memtable).await.unwrap();
    let before = r.db.manifest();
    let stream = before
        .segment(&[crate::keyspace::STREAM])
        .expect("a stream tree");
    let stream_l0: Vec<String> = stream
        .l0()
        .iter()
        .map(|v| v.sst.id.unwrap_compacted_id().to_string())
        .collect();
    assert_eq!(
        stream_l0.len(),
        1,
        "one flush wrote one L0 per touched tree"
    );
    assert_eq!(queue_l0(&before).len(), 1);
    let pass = tokio::time::timeout(STEP_WITHIN, r.engine.compact_queue_tree());
    let report = pass.await.unwrap().unwrap();
    assert_eq!((report.l0s, report.runs, report.steps), (1, 0, 1));
    for id in &stream_l0 {
        assert!(
            !report.superseded.contains(id),
            "the stream L0 is not superseded"
        );
    }
    let after = manifest(&r).await;
    let stream = after
        .segment(&[crate::keyspace::STREAM])
        .expect("a stream tree");
    let kept: Vec<String> = stream
        .l0()
        .iter()
        .map(|v| v.sst.id.unwrap_compacted_id().to_string())
        .collect();
    assert_eq!(kept, stream_l0, "the stream tree's L0 is untouched");
    assert!(
        stream.compacted().is_empty(),
        "and no stream run was written"
    );
    assert!(queue_l0(&after).is_empty());
    assert_eq!(queue_runs(&after).len(), 1);
    assert!(
        after.l0().is_empty() && after.compacted().is_empty(),
        "the root tree is empty"
    );
    for key in [qk(b"queue-row"), sk(b"stream-row")] {
        assert_eq!(r.db.get(&key).await.unwrap().as_deref(), Some(&b"v"[..]));
    }
}

/// The split rule (DESIGN-6A F5): a pass whose rewrite lets the L0s that
/// arrive meanwhile reach the line goes in two steps.
#[test]
fn native_queue_collection_splits_a_pass_that_would_reach_the_l0_line() {
    const RATE: u64 = 30 << 20;
    assert!(single_pass_fits(3, 0, RATE));
    // 28 L0s free below the line at 4 margin: 140 s of flushes.
    assert!(single_pass_fits(0, 140 * RATE, RATE));
    assert!(!single_pass_fits(0, 141 * RATE, RATE));
    assert!(single_pass_fits(10, 90 * RATE, RATE));
    assert!(!single_pass_fits(10, 91 * RATE, RATE));
    assert!(!single_pass_fits(29, 0, RATE));
    assert!(
        !single_pass_fits(0, 1, 0),
        "an unmeasured zero rate never fits"
    );
}

/// A split pass: the L0s into a fresh run, then every run into the last
/// run; the queue tree ends in one run and every source is superseded.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn native_queue_collection_split_pass_ends_in_the_last_run() {
    let r = rig(
        "t/nqc-split",
        Some(compactor_every(Duration::from_millis(100))),
    )
    .await;
    let admin = r.engine.queue_admin();
    flushed_into_run(&r, &admin, &qk(b"a"), 0).await;
    r.db.put(qk(b"b"), b"v").await.unwrap();
    r.db.put(sk(b"s"), b"v").await.unwrap();
    let memtable = slatedb::config::FlushOptions {
        flush_type: slatedb::config::FlushType::MemTable,
    };
    r.db.flush_with_options(memtable).await.unwrap();
    // One byte a second: no single pass fits.
    let pass = tokio::time::timeout(STEP_WITHIN, r.engine.compact_flushed_queue_tree(1));
    let report = pass.await.unwrap().unwrap();
    assert!(report.split);
    assert_eq!((report.steps, report.l0s), (2, 1));
    let m = manifest(&r).await;
    assert!(queue_l0(&m).is_empty());
    let runs: Vec<u32> = queue_runs(&m).iter().map(|run| run.id).collect();
    assert_eq!(runs, [0], "into the last run");
    for key in [qk(b"a"), qk(b"b"), sk(b"s")] {
        assert_eq!(r.db.get(&key).await.unwrap().as_deref(), Some(&b"v"[..]));
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn native_queue_collection_reads_a_failed_compaction_as_failed_and_a_trimmed_one_as_finished()
{
    let busy = rig(
        "t/nqc-busy",
        Some(compactor_every(Duration::from_millis(100))),
    )
    .await;
    let admin = busy.engine.queue_admin();
    // A spec naming a run that does not exist: the coordinator fails it.
    let failed = submit(&admin, &missing_run()).await;
    let outcome = ShardEngine::await_compaction(&admin, &failed);
    let outcome = tokio::time::timeout(STEP_WITHIN, outcome).await;
    assert_eq!(outcome, Ok(false), "a failed compaction is not waited out");
    // A record no compactions file holds was trimmed: it finished.
    let elsewhere = slatedb::admin::AdminBuilder::new("t/nqc-none", busy.store.clone()).build();
    let trimmed = ShardEngine::await_compaction(&elsewhere, &failed);
    let trimmed = tokio::time::timeout(STEP_WITHIN, trimmed).await;
    assert_eq!(trimmed, Ok(true));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn native_queue_collection_waits_for_a_compaction_no_compactor_has_taken() {
    let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let db = Db::builder("t/nqc-idle", store.clone())
        .with_settings(compactor_every(Duration::from_millis(100)))
        .build()
        .await
        .unwrap();
    let admin = slatedb::admin::AdminBuilder::new("t/nqc-idle", store.clone()).build();
    // The compactor is live once it has failed a probe; then it stops, and
    // a compaction submitted after stays submitted.
    let probe = submit(&admin, &missing_run()).await;
    let probed = ShardEngine::await_compaction(&admin, &probe);
    assert_eq!(tokio::time::timeout(STEP_WITHIN, probed).await, Ok(false));
    db.close().await.unwrap();
    let queued = submit(&admin, &missing_run()).await;
    let waiting = ShardEngine::await_compaction(&admin, &queued);
    let waiting = tokio::time::timeout(Duration::from_millis(1_500), waiting);
    assert!(
        waiting.await.is_err(),
        "a submitted compaction is waited for"
    );
}

/// The queue's stored state row.
async fn stored(r: &Rig, hash: [u8; 16]) -> StateRow {
    let raw = r.db.get(keys::state(&hash)).await.unwrap();
    StateRow::decode(&raw.expect("a state row")).unwrap()
}

/// A queue created through the engine with `debt` bytes of deletion debt
/// recorded now (generation 1), which a state change puts on the roster.
/// Returns its stored state row.
async fn indebted(r: &Rig, hash: [u8; 16], debt: u64) -> StateRow {
    let create = NqOp::Create {
        cfg: QueueConfig::default(),
    };
    r.engine.submit_native_queue(hash, create).await.unwrap();
    let mut s = stored(r, hash).await;
    s.debt_bytes = debt;
    s.debt_generation = 1;
    s.debt_oldest_at = crate::shard::now_ms();
    r.db.put(keys::state(&hash), s.encode()).await.unwrap();
    let change = NqOp::Pause { paused: false };
    r.engine.submit_native_queue(hash, change).await.unwrap();
    let roster = r.db.get(keys::roster(&hash)).await.unwrap();
    assert!(roster.is_some(), "debt puts the queue on the roster");
    stored(r, hash).await
}

/// The shard log's manifest as the collector's admin reads it.
async fn manifest(r: &Rig) -> slatedb::VersionedManifest {
    let manifest = r.engine.queue_admin().read_manifest(None).await;
    manifest.unwrap().expect("a manifest")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn native_queue_collection_leaves_debt_that_is_not_due_and_compacts_nothing() {
    let every = compactor_every(Duration::from_millis(100));
    let r = rig("t/nqc-not-due", Some(every)).await;
    // Small and young: neither the size trigger nor the age trigger.
    let before = indebted(&r, [5; 16], MIB).await;
    let pass = tokio::time::timeout(Duration::from_secs(5), r.engine.maybe_collect());
    assert_eq!(pass.await, Ok(Ok(())), "a pass that is not due returns");
    assert_eq!(stored(&r, [5; 16]).await, before, "the debt is untouched");
    let m = manifest(&r).await;
    assert!(queue_l0(&m).is_empty(), "nothing was flushed");
    assert!(queue_runs(&m).is_empty(), "nothing was compacted");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn native_queue_collection_of_due_debt_compacts_the_log_and_clears_the_debt_it_captured() {
    let every = compactor_every(Duration::from_millis(100));
    let r = rig("t/nqc-due", Some(every)).await;
    let hash = [6; 16];
    // The size trigger. The log is the memtable alone, so the pass's flush
    // writes one L0 SST: its compaction supersedes only the residual the
    // manifest keeps (D22), and the pass has no garbage to wait for.
    indebted(&r, hash, 256 * MIB).await;
    // Two passes at once, as when the engine's own collector loop ticks beside
    // this call (the mutation baseline on CI, 2026-10-24). They share the
    // instance's permit, and the one that waits captures the debt again under
    // it and finds none left; capturing only before the wait, it rewrote the
    // first pass's sorted run and waited 30 s for GC to delete it.
    let both = futures_util::future::join(r.engine.maybe_collect(), r.engine.maybe_collect());
    let passes = tokio::time::timeout(STEP_WITHIN, both).await;
    assert_eq!(passes, Ok((Ok(()), Ok(()))));
    let s = stored(&r, hash).await;
    assert_eq!(
        (s.debt_bytes, s.debt_oldest_at),
        (0, 0),
        "the debt is cleared"
    );
    let roster = r.db.get(keys::roster(&hash)).await.unwrap();
    assert!(roster.is_none(), "nothing pending: off the roster");
    let m = manifest(&r).await;
    assert!(queue_l0(&m).is_empty(), "the flushed L0 SST was compacted");
    assert_eq!(queue_runs(&m).len(), 1, "into one sorted run");
}

fn missing_run() -> CompactionSpec {
    queue_spec(vec![SourceId::SortedRun(4_242)], 4_242)
}

/// An in-memory store whose reads of the held tables wait until the test
/// releases them, so a compaction that reads them stays running. Writes,
/// manifests and the compactions file are never held.
#[derive(Debug, Default)]
struct HeldTables {
    inner: object_store::memory::InMemory,
    held: std::sync::Mutex<Vec<String>>,
}

impl HeldTables {
    fn hold(&self, tables: Vec<String>) {
        *self.held.lock().unwrap() = tables;
    }

    fn release(&self) {
        self.held.lock().unwrap().clear();
    }

    fn holds(&self, path: &object_store::path::Path) -> bool {
        let held = self.held.lock().unwrap();
        held.iter()
            .any(|table| path.as_ref().contains(table.as_str()))
    }
}

impl std::fmt::Display for HeldTables {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "HeldTables")
    }
}

#[async_trait::async_trait]
impl ObjectStore for HeldTables {
    async fn put_opts(
        &self,
        path: &object_store::path::Path,
        payload: object_store::PutPayload,
        opts: object_store::PutOptions,
    ) -> object_store::Result<object_store::PutResult> {
        self.inner.put_opts(path, payload, opts).await
    }
    async fn put_multipart_opts(
        &self,
        path: &object_store::path::Path,
        opts: object_store::PutMultipartOptions,
    ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
        self.inner.put_multipart_opts(path, opts).await
    }
    async fn get_opts(
        &self,
        path: &object_store::path::Path,
        opts: object_store::GetOptions,
    ) -> object_store::Result<object_store::GetResult> {
        while self.holds(path) {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        self.inner.get_opts(path, opts).await
    }
    fn delete_stream(
        &self,
        paths: futures_util::stream::BoxStream<
            'static,
            object_store::Result<object_store::path::Path>,
        >,
    ) -> futures_util::stream::BoxStream<'static, object_store::Result<object_store::path::Path>>
    {
        self.inner.delete_stream(paths)
    }
    fn list(
        &self,
        prefix: Option<&object_store::path::Path>,
    ) -> futures_util::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>>
    {
        self.inner.list(prefix)
    }
    async fn list_with_delimiter(
        &self,
        prefix: Option<&object_store::path::Path>,
    ) -> object_store::Result<object_store::ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }
    async fn copy_opts(
        &self,
        from: &object_store::path::Path,
        to: &object_store::path::Path,
        opts: object_store::CopyOptions,
    ) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, opts).await
    }
}

/// Flush `key` into an L0 table and compact that table alone into the
/// fresh sorted run `run`.
async fn flushed_into_run(r: &Rig, admin: &slatedb::admin::Admin, key: &[u8], run: u32) {
    r.db.put(key, b"v").await.unwrap();
    let memtable = slatedb::config::FlushOptions {
        flush_type: slatedb::config::FlushType::MemTable,
    };
    r.db.flush_with_options(memtable).await.unwrap();
    let l0 = queue_l0(&r.db.manifest())
        .iter()
        .map(|v| v.id)
        .collect::<Vec<_>>();
    assert_eq!(l0.len(), 1, "one flushed L0 table");
    let spec = queue_spec(vec![SourceId::SstView(l0[0])], run);
    let compaction = submit(admin, &spec).await;
    let done = ShardEngine::await_compaction(admin, &compaction);
    assert_eq!(tokio::time::timeout(STEP_WITHIN, done).await, Ok(true));
}

/// The ids of the compactions the shard's compactions file holds live.
async fn live_compactions(admin: &slatedb::admin::Admin) -> Vec<String> {
    let compactions = admin.read_compactions(None).await.unwrap();
    let compactions = compactions.expect("a compactions file");
    let live = compactions.recent_compactions().filter(|c| c.active());
    live.map(|c| c.id().to_string()).collect()
}

/// SR 1 and SR 0 (the last run), each from one flushed L0 table, and the
/// size-tiered shape [SR 1, SR 0] -> SR 0 running, held mid-read until the
/// test releases `store`.
async fn regular_job_into_the_last_run(
    r: &Rig,
    store: &HeldTables,
    admin: &slatedb::admin::Admin,
) -> slatedb::compactor::Compaction {
    flushed_into_run(r, admin, &qk(b"a"), 0).await;
    flushed_into_run(r, admin, &qk(b"b"), 1).await;
    let runs = queue_runs(&manifest(r).await);
    assert_eq!(runs.iter().map(|run| run.id).collect::<Vec<_>>(), [1, 0]);
    let tables = runs.iter().flat_map(|run| run.sst_views().iter());
    store.hold(
        tables
            .map(|v| v.sst.id.unwrap_compacted_id().to_string())
            .collect(),
    );
    let both = vec![SourceId::SortedRun(1), SourceId::SortedRun(0)];
    let regular = submit(admin, &queue_spec(both, 0)).await;
    let running = async {
        while admin
            .read_compaction(regular.id(), None)
            .await
            .unwrap()
            .map(|c| c.status())
            != Some(slatedb::compactor::CompactionStatus::Running)
        {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    };
    tokio::time::timeout(STEP_WITHIN, running).await.unwrap();
    regular
}

/// The incident of 2026-10-24 (100x queue capacity run, shard 10): a regular
/// sorted-run compaction into the last run is running when the collector
/// passes. The pass's spec (every L0 table and every run into the last run)
/// passes SlateDB's validation, which checks no other job's destination, and
/// the worker's executor asserts that no running job writes the same run: the
/// compaction worker panics and the shard log closes. The pass must wait the
/// regular job out and then compact everything into the last run.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_queue_collection_waits_out_a_compaction_into_the_last_run_before_it_submits() {
    let store = Arc::new(HeldTables::default());
    let every = Some(compactor_every(Duration::from_millis(100)));
    let r = rig_over("t/nqc-last-run", every, store.clone()).await;
    let admin = r.engine.queue_admin();
    let regular = regular_job_into_the_last_run(&r, &store, &admin).await;
    // The pass runs while the regular job does: twenty compactor polls and
    // worker polls later the shard log is open and the regular job is the
    // only live one. Released, the regular job finishes and the pass
    // compacts everything into the last run.
    r.db.put(qk(b"c"), b"v").await.unwrap();
    let meanwhile = async {
        tokio::time::sleep(Duration::from_secs(2)).await;
        let probe = r.db.put(qk(b"probe"), b"v").await;
        probe.expect("the shard log stays open while the pass waits");
        assert_eq!(live_compactions(&admin).await, [regular.id().to_string()]);
        store.release();
    };
    let both = futures_util::future::join(r.engine.compact_queue_tree(), meanwhile);
    let (pass, ()) = tokio::time::timeout(STEP_WITHIN, both).await.unwrap();
    let superseded = pass.unwrap().superseded;
    assert!(!superseded.is_empty(), "the pass names what it superseded");
    let m = manifest(&r).await;
    assert!(queue_l0(&m).is_empty(), "every L0 table was compacted");
    let runs: Vec<u32> = queue_runs(&m).iter().map(|run| run.id).collect();
    assert_eq!(runs, [0], "into the last run");
    assert!(live_compactions(&admin).await.is_empty());
    for key in [qk(b"a"), qk(b"b"), qk(b"c"), qk(b"probe")] {
        let value = r.db.get(&key).await.unwrap();
        assert_eq!(value.as_deref(), Some(&b"v"[..]), "{key:?} kept");
    }
}

/// The shard log's compactor refuses a submitted spec that shares a run with
/// a live job, so a pass that submits after the compactor scheduled a regular
/// job, but before the pass read the compactions file, still cannot run
/// beside it: the spec fails, the shard log stays open and the regular job
/// completes.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_queue_collection_spec_beside_a_live_compaction_is_refused_by_the_shard_log_compactor()
 {
    let store = Arc::new(HeldTables::default());
    let every = Some(compactor_every(Duration::from_millis(100)));
    let r = rig_over("t/nqc-guard", every, store.clone()).await;
    let admin = r.engine.queue_admin();
    let regular = regular_job_into_the_last_run(&r, &store, &admin).await;
    r.db.put(qk(b"c"), b"v").await.unwrap();
    let memtable = slatedb::config::FlushOptions {
        flush_type: slatedb::config::FlushType::MemTable,
    };
    r.db.flush_with_options(memtable).await.unwrap();
    let l0 = queue_l0(&r.db.manifest())
        .iter()
        .map(|v| v.id)
        .collect::<Vec<_>>();
    assert_eq!(l0.len(), 1, "one flushed L0 table");
    let everything = vec![
        SourceId::SstView(l0[0]),
        SourceId::SortedRun(1),
        SourceId::SortedRun(0),
    ];
    let refused = submit(&admin, &queue_spec(everything, 0)).await;
    let outcome = ShardEngine::await_compaction(&admin, &refused);
    let outcome = tokio::time::timeout(STEP_WITHIN, outcome).await;
    assert_eq!(outcome, Ok(false), "the compactor fails the spec");
    let probe = r.db.put(qk(b"probe"), b"v").await;
    probe.expect("the shard log stays open");
    assert_eq!(live_compactions(&admin).await, [regular.id().to_string()]);
    store.release();
    let done = ShardEngine::await_compaction(&admin, &regular);
    assert_eq!(tokio::time::timeout(STEP_WITHIN, done).await, Ok(true));
}

/// Flush one row into the queue tree and one into the stream tree.
async fn flush_both(r: &Rig, i: u32) {
    r.db.put(qk(&i.to_be_bytes()), b"v").await.unwrap();
    r.db.put(sk(&i.to_be_bytes()), b"v").await.unwrap();
    let memtable = slatedb::config::FlushOptions {
        flush_type: slatedb::config::FlushType::MemTable,
    };
    r.db.flush_with_options(memtable).await.unwrap();
}

/// The scheduler never compacts the queue tree (it does compact the
/// stream tree), and the collector merges the queue tree's L0s once they
/// reach the trigger (16).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_queue_collection_merges_queue_l0s_at_the_trigger_and_the_scheduler_leaves_them() {
    // The shard log's L0 line (32, `config::validation`): SlateDB's
    // default would stall the queue tree's ninth flush below the trigger.
    let settings = slatedb::config::Settings {
        l0_max_ssts: 32,
        l0_max_ssts_per_key: 32,
        ..compactor_every(Duration::from_millis(100))
    };
    let r = rig("t/nqc-merge", Some(settings)).await;
    for i in 0..15 {
        flush_both(&r, i).await;
    }
    let stream_compacted = || {
        let m = r.db.manifest();
        let stream = m
            .segment(&[crate::keyspace::STREAM])
            .expect("a stream tree");
        !stream.compacted().is_empty()
    };
    let stream_compacted = async {
        while !stream_compacted() {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    };
    tokio::time::timeout(STEP_WITHIN, stream_compacted)
        .await
        .expect("the scheduler compacts the stream tree");
    // Two merger ticks after the stream tree compacted: below the trigger
    // the queue tree's L0s are never merged, by the scheduler or the merger.
    tokio::time::sleep(Duration::from_secs(4)).await;
    let m = r.db.manifest();
    assert_eq!(
        queue_l0(&m).len(),
        15,
        "below the trigger nothing merges the queue L0s"
    );
    assert!(
        queue_runs(&m).is_empty(),
        "the scheduler wrote no queue-tree run"
    );
    flush_both(&r, 15).await;
    let merged_now = || {
        let m = r.db.manifest();
        queue_l0(&m).is_empty() && !queue_runs(&m).is_empty()
    };
    let merged = async {
        while !merged_now() {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    };
    tokio::time::timeout(STEP_WITHIN, merged)
        .await
        .expect("the L0s merge");
    for i in 0..16u32 {
        let v = r.db.get(qk(&i.to_be_bytes())).await.unwrap();
        assert_eq!(v.as_deref(), Some(&b"v"[..]));
    }
}
