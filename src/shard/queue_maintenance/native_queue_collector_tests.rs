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

use super::pass_due;
use crate::native_queue::rows::{STATE_ROW_VERSION, StateRow};
use crate::native_queue::{NqOp, QueueConfig, keys};
use crate::shard::{AbsorbSignal, ShardConfig, ShardEngine};

const MIB: u64 = 1 << 20;
const MIN: i64 = 60_000;
const HOUR: i64 = crate::native_queue::HOUR_MS;
/// How long a test waits on a collector step that should finish quickly.
const STEP_WITHIN: Duration = Duration::from_secs(30);

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
    let mut builder = Db::builder(name, store.clone());
    if let Some(settings) = settings {
        builder = builder.with_settings(settings);
    }
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
            v: STATE_ROW_VERSION,
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
    let done = tokio::time::timeout(STEP_WITHIN, e.await_collection(&gone));
    assert_eq!(done.await.unwrap(), Ok(()), "nothing left: done at once");
    let present = tokio::time::timeout(Duration::from_secs(1), e.await_collection(&kept));
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
    r.db.put(b"k", b"v").await.unwrap();
    let memtable = slatedb::config::FlushOptions {
        flush_type: slatedb::config::FlushType::MemTable,
    };
    r.db.flush_with_options(memtable).await.unwrap();
    let l0: Vec<String> =
        r.db.manifest()
            .l0()
            .iter()
            .map(|v| v.sst.id.unwrap_compacted_id().to_string())
            .collect();
    assert!(!l0.is_empty(), "the flush wrote an L0 SST");
    let pass = tokio::time::timeout(STEP_WITHIN, r.engine.compact_everything());
    let superseded = pass.await.unwrap().unwrap();
    for id in &l0 {
        assert!(superseded.contains(id), "{id} is named superseded");
    }
    let manifest = r.engine.queue_admin().read_manifest(None).await;
    let manifest = manifest.unwrap().expect("a manifest");
    assert!(manifest.l0().is_empty(), "every L0 SST was compacted");
    assert_eq!(manifest.compacted().len(), 1, "into one sorted run");
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
    assert!(m.l0().is_empty(), "nothing was flushed");
    assert!(m.compacted().is_empty(), "nothing was compacted");
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
    let pass = tokio::time::timeout(STEP_WITHIN, r.engine.maybe_collect());
    assert_eq!(pass.await, Ok(Ok(())));
    let s = stored(&r, hash).await;
    assert_eq!(
        (s.debt_bytes, s.debt_oldest_at),
        (0, 0),
        "the debt is cleared"
    );
    let roster = r.db.get(keys::roster(&hash)).await.unwrap();
    assert!(roster.is_none(), "nothing pending: off the roster");
    let m = manifest(&r).await;
    assert!(m.l0().is_empty(), "the flushed L0 SST was compacted");
    assert_eq!(m.compacted().len(), 1, "into one sorted run");
}

fn missing_run() -> CompactionSpec {
    CompactionSpec::new(vec![SourceId::SortedRun(4_242)], 4_242)
}
