#![cfg(test)]
//! Real process-crash cuts across WAL, history publication, and original-key
//! reclamation. Each reopen has new runtimes, handles, and caches. Successful
//! local object operations survive process death; this is not a power-loss test.

use super::fixture_process::{CRASH_EXIT, child_plan, crash, run_child, witness};
use super::fixture_storage::{append_sized, skey};
use crate::dst::{FaultPlan, FaultStore, ObjClass, StoreOp};
use crate::history::{Absorber, AbsorberConfig, KeyCache};
use crate::shard::{ShardConfig, ShardEngine, TailFields};
use futures_util::TryStreamExt;
use object_store::{ObjectStore, ObjectStoreExt};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

const HASH: [u8; 16] = [0xc7; 16];
const CHILD_TEST: &str = "dst::dst_tests::crash_recovery::crash_recovery_child";

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
enum Cut {
    AckedWal,
    HistorySstPending,
    HistoryDurable,
    BoundaryDurable,
    PartialTrim,
    FullTrim,
}

impl Cut {
    fn records(self) -> u64 {
        match self {
            Self::PartialTrim | Self::FullTrim => 5,
            Self::AckedWal
            | Self::HistorySstPending
            | Self::HistoryDurable
            | Self::BoundaryDurable => 4,
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
enum Role {
    Seed,
    RecoveryManifestCrash,
    RecoverAndReclaim,
    VerifyCold,
}

#[derive(Debug, Deserialize, Serialize)]
struct ChildPlan {
    root: PathBuf,
    cut: Cut,
    role: Role,
}

/// Six causal cuts, each followed by a second crash during SlateDB recovery's
/// manifest read, recovery/retirement, and a final cold read after actual deletes.
#[tokio::test]
async fn process_crashes_preserve_history_through_reclamation() {
    let unique = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let root = std::env::temp_dir().join(format!(
        "streams-crash-recovery-{}-{unique}",
        std::process::id()
    ));
    for cut in [
        Cut::AckedWal,
        Cut::HistorySstPending,
        Cut::HistoryDurable,
        Cut::BoundaryDurable,
        Cut::PartialTrim,
        Cut::FullTrim,
    ] {
        let case = root.join(format!("{cut:?}"));
        std::fs::create_dir_all(case.join("objects")).unwrap();
        for (role, expected_exit) in [
            (Role::Seed, CRASH_EXIT),
            (Role::RecoveryManifestCrash, CRASH_EXIT),
            (Role::RecoverAndReclaim, CRASH_EXIT),
            (Role::VerifyCold, 0),
        ] {
            run_child(
                &case.join(format!("{role:?}")),
                CHILD_TEST,
                &ChildPlan {
                    root: case.clone(),
                    cut,
                    role,
                },
                expected_exit,
            )
            .await;
        }
    }
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn crash_recovery_child() {
    let Some(plan) = child_plan::<ChildPlan>() else {
        return; // The parent alone authorizes this subprocess-only fixture.
    };
    let local: Arc<dyn ObjectStore> = Arc::new(
        object_store::local::LocalFileSystem::new_with_prefix(plan.root.join("objects")).unwrap(),
    );
    let store = FaultStore::uniform(local.clone(), 0xc7, FaultPlan::CLEAN);
    if matches!(plan.role, Role::RecoveryManifestCrash) {
        crash_during_manifest_recovery(&plan, store).await;
    }
    let engine = open_engine(local, store.clone()).await;
    match plan.role {
        Role::Seed => seed_until_cut(&plan, &engine, &store).await,
        Role::RecoverAndReclaim => recover_and_reclaim(&plan, &engine, &store).await,
        Role::VerifyCold => {
            assert_originals_absent(&engine, plan.cut.records()).await;
            assert_original_wals_absent(&plan, store.as_ref()).await;
            assert_payloads(&engine, plan.cut.records() + 1).await;
            assert_eq!(engine.maintenance_snapshot().unabsorbed_frame_bytes, 0);
            witness(&plan);
            engine
                .await_terminated(Duration::from_secs(20))
                .await
                .unwrap();
        }
        Role::RecoveryManifestCrash => unreachable!("the recovery cut exits the process"),
    }
}

/// The shard DB uses the plain store and the history DB uses the fault wrapper
/// over the same persistent storage. A periodic shard memtable flush cannot
/// satisfy the history SST hold's witness.
async fn open_engine(
    shard_store: Arc<dyn ObjectStore>,
    history_store: Arc<FaultStore>,
) -> Arc<ShardEngine> {
    let db = slatedb::Db::builder("crash-shard", shard_store)
        .with_settings(slatedb::config::Settings {
            flush_interval: Some(Duration::from_millis(5)),
            manifest_poll_interval: Duration::from_millis(50),
            ..Default::default()
        })
        .build()
        .await
        .unwrap();
    let maintenance = crate::shard::load_or_rebuild_maintenance(&db)
        .await
        .unwrap();
    let (absorb_tx, _absorb_rx) = crate::history::absorber_channel();
    ShardEngine::start(
        "crash-shard".into(),
        Arc::new(db),
        history_store,
        ShardConfig {
            trim_global_budget: 1,
            max_trim_per_op: 1,
            tail_ring_bytes: 0,
            ..Default::default()
        },
        absorb_tx,
        None,
        maintenance,
    )
}

async fn wait_engaged(entered: &AtomicU64) {
    tokio::time::timeout(Duration::from_secs(20), async {
        while entered.load(Ordering::SeqCst) == 0 {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .expect("the selected storage operation must actually park");
}

async fn crash_during_manifest_recovery(plan: &ChildPlan, store: Arc<FaultStore>) -> ! {
    // Unlike WAL replay, manifest recovery always occurs even if the seed's
    // periodic shard flush already moved all records into L0.
    let entered = store.hold_class(StoreOp::Get, ObjClass::Manifest, 1);
    let store: Arc<dyn ObjectStore> = store;
    tokio::select! {
        result = slatedb::Db::builder("crash-shard", store).build() => {
            panic!("manifest recovery must hit its held GET before open completes: {}", result.is_ok());
        }
        () = wait_engaged(&entered) => crash(plan),
    }
}

fn absorber(engine: &Arc<ShardEngine>, store: &Arc<FaultStore>) -> Absorber {
    let keys = Arc::new(KeyCache::default());
    keys.put(HASH, skey(), HASH);
    Absorber::new(
        store.clone(),
        engine.clone(),
        keys,
        AbsorberConfig::default(),
    )
}

async fn append_at(engine: &Arc<ShardEngine>, offset: u64) {
    let bytes = usize::try_from(offset).unwrap() + 17;
    assert_eq!(append_sized(engine, HASH, &skey(), "", bytes).await, offset);
}

async fn tail(engine: &ShardEngine) -> TailFields {
    let handle = engine.stream_handle(HASH).await.unwrap();
    handle.state.lock().unwrap().durable.clone()
}

async fn wait_tail(engine: &ShardEngine, ready: impl Fn(&TailFields) -> bool) -> TailFields {
    tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            let tail = tail(engine).await;
            if ready(&tail) {
                return tail;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .expect("durable tail must reach the selected boundary")
}

async fn seed_until_cut(plan: &ChildPlan, engine: &Arc<ShardEngine>, store: &Arc<FaultStore>) {
    for offset in 0..4 {
        append_at(engine, offset).await;
    }
    assert_eq!(tail(engine).await.next, 4);
    snapshot_original_wals(plan, store.as_ref()).await;
    if matches!(plan.cut, Cut::AckedWal) {
        crash(plan);
    }
    let absorb = absorber(engine, store);
    // Open before arming SST hold: the witness must be the gather's write,
    // not the partition's creation. No shard memtable flush is requested.
    let history = engine.history_partition().await.unwrap();
    if matches!(plan.cut, Cut::HistorySstPending) {
        let entered = store.hold_class(StoreOp::Put, ObjClass::Sst, 1);
        tokio::select! {
            result = absorb.absorb_gather_v2(&[HASH]) => panic!("gather escaped SST hold: {}", result.is_ok()),
            () = wait_engaged(&entered) => {
                assert_eq!(tail(engine).await.absorbed, 0);
                crash(plan);
            }
        }
    }
    if matches!(plan.cut, Cut::HistoryDurable) {
        let _commit = engine.test_hold_commit().await;
        assert_eq!(
            absorb
                .absorb_gather_v2(&[HASH])
                .await
                .unwrap()
                .advanced
                .len(),
            1
        );
        assert_eq!(tail(engine).await.absorbed, 0);
        assert!(
            history
                .get(crate::history::hist2_record_key(
                    crate::crypto::RouteHash(HASH),
                    crate::crypto::SegmentHash(HASH),
                    0,
                ))
                .await
                .unwrap()
                .is_some()
        );
        crash(plan);
    }
    absorb.absorb_gather_v2(&[HASH]).await.unwrap();
    let first = wait_tail(engine, |t| t.absorbed == 4).await;
    assert_eq!((first.trim_safe_to, first.trimmed), (0, 0));
    if matches!(plan.cut, Cut::BoundaryDurable) {
        crash(plan);
    }
    append_at(engine, 4).await;
    absorb.absorb_gather_v2(&[HASH]).await.unwrap();
    let second = wait_tail(engine, |t| t.absorbed == 5).await;
    assert_eq!(second.trim_safe_to, 4);
    if matches!(plan.cut, Cut::PartialTrim) {
        assert!(second.trimmed > 0 && second.trimmed < second.trim_safe_to);
        assert_originals_absent(engine, second.trimmed).await;
        assert!(
            engine
                .db
                .get(crate::shard::record_key(&HASH, 3))
                .await
                .unwrap()
                .is_some()
        );
        crash(plan);
    }
    drain_trim(engine, 4).await;
    crash(plan);
}

async fn assert_originals_absent(engine: &ShardEngine, upto: u64) {
    for offset in 0..upto {
        assert!(
            engine
                .db
                .get(crate::shard::record_key(&HASH, offset))
                .await
                .unwrap()
                .is_none(),
            "original record {offset} must actually be deleted before the history read"
        );
    }
}

async fn drain_trim(engine: &ShardEngine, upto: u64) {
    tokio::time::timeout(Duration::from_secs(20), async {
        while tail(engine).await.trimmed < upto {
            engine.pump_trim_tick();
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    })
    .await
    .expect("trim debt must drain");
    assert_originals_absent(engine, upto).await;
}

async fn recover_and_reclaim(plan: &ChildPlan, engine: &Arc<ShardEngine>, store: &Arc<FaultStore>) {
    let count = plan.cut.records();
    let recovered = tail(engine).await;
    assert_eq!(recovered.next, count);
    assert_eq!(
        engine.maintenance_snapshot().unabsorbed_frame_bytes,
        recovered.unabsorbed_bytes,
        "cold admission accounting must match the recovered durable tail"
    );
    assert_eq!(recovered.unabsorbed_bytes > 0, recovered.absorbed < count);
    // Read before opening an absorber: durable promises must survive on their
    // own, without recovery maintenance repairing the reader's input first.
    assert_payloads(engine, count).await;
    let absorb = absorber(engine, store);
    absorb.absorb_gather_v2(&[HASH]).await.unwrap();
    wait_tail(engine, |t| t.absorbed == count).await;
    append_at(engine, count).await;
    absorb.absorb_gather_v2(&[HASH]).await.unwrap();
    let final_tail = wait_tail(engine, |t| t.absorbed == count + 1).await;
    assert_eq!(final_tail.trim_safe_to, count);
    drain_trim(engine, count).await;
    assert_payloads(engine, count + 1).await;
    collect_original_wals(plan, engine, store).await;
    crash(plan);
}

async fn snapshot_original_wals(plan: &ChildPlan, store: &dyn ObjectStore) {
    let objects: Vec<_> = store
        .list(Some(&object_store::path::Path::from("crash-shard/wal")))
        .try_collect()
        .await
        .unwrap();
    let original: Vec<_> = objects
        .into_iter()
        .filter(|object| object.size > 0)
        .map(|object| object.location.to_string())
        .collect();
    assert!(
        !original.is_empty(),
        "acknowledged appends must have WAL objects"
    );
    std::fs::write(
        plan.root.join("original-wals.json"),
        serde_json::to_vec(&original).unwrap(),
    )
    .unwrap();
}

async fn collect_original_wals(plan: &ChildPlan, engine: &ShardEngine, store: &Arc<FaultStore>) {
    // Replace replay dependencies with a durable shard memtable containing
    // the record tombstones, then ask the real collector to retire old WALs.
    engine
        .db
        .flush_with_options(slatedb::config::FlushOptions {
            flush_type: slatedb::config::FlushType::MemTable,
        })
        .await
        .unwrap();
    let admin = slatedb::admin::Admin::builder("crash-shard", store.clone()).build();
    admin
        .run_gc_once(slatedb::config::GarbageCollectorOptions {
            wal_options: Some(slatedb::config::GarbageCollectorDirectoryOptions {
                min_age: Duration::ZERO,
                ..Default::default()
            }),
            // Only regular WAL objects: leave writer fences and other directories
            // under their default safety rules. This fixture has no surviving old owner.
            manifest_options: None,
            wal_fence_options: None,
            compacted_options: None,
            compactions_options: None,
            detach_options: None,
            ..Default::default()
        })
        .await
        .unwrap();
    assert_original_wals_absent(plan, store.as_ref()).await;
}

async fn assert_original_wals_absent(plan: &ChildPlan, store: &dyn ObjectStore) {
    let original: Vec<String> =
        serde_json::from_slice(&std::fs::read(plan.root.join("original-wals.json")).unwrap())
            .unwrap();
    assert!(!original.is_empty());
    for path in original {
        assert!(
            matches!(
                store
                    .head(&object_store::path::Path::from(path.clone()))
                    .await,
                Err(object_store::Error::NotFound { .. })
            ),
            "the original WAL object must be physically deleted: {path}"
        );
    }
}

async fn assert_payloads(engine: &Arc<ShardEngine>, count: u64) {
    let handle = engine.stream_handle(HASH).await.unwrap();
    for filter in [None, Some("")] {
        let page = crate::http::read_merged(
            &skey(),
            &HASH,
            &handle,
            engine,
            0,
            filter,
            4096,
            crate::shard::Deliver::Durable,
        )
        .await
        .unwrap();
        assert!(page.completed);
        assert_eq!(page.watermarks.durable, count, "filter={filter:?}");
        assert_eq!(page.last, Some(count - 1));
        assert_eq!(
            page.recs.len(),
            usize::try_from(count).unwrap(),
            "filter={filter:?}, returned offsets={:?}",
            page.recs
                .iter()
                .map(|record| record.off)
                .collect::<Vec<_>>()
        );
        for (offset, record) in page.recs.iter().enumerate() {
            assert_eq!(record.off, u64::try_from(offset).unwrap());
            assert_eq!(record.rkey, "");
            assert_eq!(record.payload.as_ref(), vec![0x5a; offset + 17]);
        }
    }
}
