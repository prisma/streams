#![cfg(test)]
//! Real checkpoint pins protect obsolete SSTs through compaction and process
//! death. A copied single-DB checkpoint is independently audited and cold-read.
//! This is not a service-wide backup protocol or an application fork pin.

use super::fixture_process::{CRASH_EXIT, child_plan, crash, run_child, witness};
use crate::crypto::{FrameCipher, FrameCompression, RouteHash, SegmentHash};
use crate::history::{hist2_record_key, read_history2, read_history2_keyed_cached};
use crate::postings::{PostingRun, encode_page, postings_key, rk_hash};
use object_store::{ObjectStore, ObjectStoreExt, path::Path as ObjectPath};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use slatedb::admin::Admin;
use slatedb::compactor::{CompactionSpec, CompactionStatus, SourceId};
use slatedb::config::{
    CheckpointOptions, CheckpointScope, GarbageCollectorDirectoryOptions, GarbageCollectorOptions,
};
use slatedb::{Db, DbReader, DbReaderMode, PathResolver, VersionedManifest};
use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime};

const DB_PATH: &str = "checkpoint-history";
const CHILD_TEST: &str = "dst::dst_tests::checkpoint_reclamation::checkpoint_reclamation_child";
const ROUTE: RouteHash = RouteHash([0xd5; 16]);
const INC: SegmentHash = SegmentHash([0xd6; 16]);
const KEY: [u8; 32] = [0x62; 32];
const CHECKPOINT_ROWS: u64 = 4;
const BEFORE_COPY_ROWS: u64 = 6;
const LIVE_ROWS: u64 = 7;

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
enum Phase {
    CompactAndCopy,
    ReleasePin,
    VerifyCold,
    MissingObject,
}

#[derive(Debug, Deserialize, Serialize)]
struct Plan {
    root: PathBuf,
    phase: Phase,
}

#[derive(Debug, Deserialize, Serialize)]
struct ObjectDigest {
    path: String,
    size: u64,
    sha256: String,
}

#[derive(Debug, Deserialize, Serialize)]
struct Image {
    checkpoint: String,
    manifest: u64,
    originals: BTreeSet<String>,
    replacements: BTreeSet<String>,
    objects: Vec<ObjectDigest>,
}

#[tokio::test]
async fn checkpoint_pins_old_ssts_until_release_and_restores_exact_cut() {
    let unique = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let root = std::env::temp_dir().join(format!(
        "streams-checkpoint-{}-{unique}",
        std::process::id()
    ));
    for phase in [
        Phase::CompactAndCopy,
        Phase::ReleasePin,
        Phase::VerifyCold,
        Phase::MissingObject,
    ] {
        let expected = match phase {
            Phase::CompactAndCopy | Phase::ReleasePin => CRASH_EXIT,
            Phase::VerifyCold | Phase::MissingObject => 0,
        };
        run_child(
            &root.join(format!("{phase:?}")),
            CHILD_TEST,
            &Plan {
                root: root.clone(),
                phase,
            },
            expected,
        )
        .await;
    }
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn checkpoint_reclamation_child() {
    let Some(plan) = child_plan::<Plan>() else {
        return;
    };
    match plan.phase {
        Phase::CompactAndCopy => compact_and_copy(&plan).await,
        Phase::ReleasePin => release_pin(&plan).await,
        Phase::VerifyCold => verify_cold(&plan).await,
        Phase::MissingObject => missing_object(&plan).await,
    }
    witness(&plan);
}

fn store(root: &Path, name: &str) -> Arc<dyn ObjectStore> {
    let path = root.join(name);
    std::fs::create_dir_all(&path).unwrap();
    Arc::new(object_store::local::LocalFileSystem::new_with_prefix(path).unwrap())
}

async fn open_db(store: Arc<dyn ObjectStore>) -> Arc<Db> {
    Arc::new(
        Db::builder(DB_PATH, store)
            .with_settings(slatedb::config::Settings {
                wal_enabled: false,
                flush_interval: None,
                manifest_poll_interval: Duration::from_millis(10),
                garbage_collector_options: None,
                compactor_options: Some(slatedb::config::CompactorOptions {
                    poll_interval: Duration::from_millis(10),
                    commit_compacted_interval: Duration::from_millis(10),
                    worker: Some(slatedb::config::CompactionWorkerOptions {
                        compactions_poll_interval: Duration::from_millis(10),
                        ..Default::default()
                    }),
                    // The fixture submits exactly the observed L0s. Keep automatic
                    // scheduling out of its source-selection/manifest witness.
                    scheduler_options: [
                        ("min_compaction_sources".into(), "64".into()),
                        ("max_compaction_sources".into(), "64".into()),
                    ]
                    .into(),
                    ..Default::default()
                }),
                ..Default::default()
            })
            .build()
            .await
            .unwrap(),
    )
}

fn route_key(offset: u64) -> &'static str {
    if offset.is_multiple_of(2) {
        "wanted"
    } else {
        "other"
    }
}

async fn write_rows(db: &Db, from: u64, upto: u64) {
    let cipher = FrameCipher::new(&KEY, &INC.0, FrameCompression::Disabled);
    let mut batch = slatedb::WriteBatch::new();
    for offset in from..upto {
        let routing_key = route_key(offset);
        let frame = cipher.encrypt(
            &INC.0,
            offset,
            1,
            0,
            routing_key,
            format!("checkpoint-payload-{offset}").as_bytes(),
        );
        let page = encode_page(
            offset,
            &[PostingRun {
                gap_offsets: 0,
                record_count: 1,
                matching_frame_bytes: u64::try_from(frame.len()).unwrap(),
                gap_frame_bytes_before: 0,
            }],
        );
        batch.put(hist2_record_key(ROUTE, INC, offset), frame);
        batch.put(
            postings_key(ROUTE, INC, &rk_hash(routing_key), 0, offset),
            page,
        );
    }
    db.write(batch).await.unwrap();
    db.flush().await.unwrap();
}

/// Traverse the actual manifest's typed SST graph, including named segments.
/// External DBs need their own checkpoint/copy protocol and are refused here.
fn referenced_ssts(manifest: &VersionedManifest) -> BTreeSet<String> {
    assert!(
        manifest.external_dbs().is_empty(),
        "cross-DB references need a coordinated image"
    );
    let resolver = PathResolver::new(DB_PATH, manifest);
    let mut paths = BTreeSet::new();
    for view in manifest
        .l0()
        .iter()
        .chain(manifest.compacted().iter().flat_map(|run| run.sst_views()))
    {
        paths.insert(resolver.sst_path(&view.sst.id).to_string());
    }
    for segment in manifest.segments() {
        for view in segment
            .l0()
            .iter()
            .chain(segment.compacted().iter().flat_map(|run| run.sst_views()))
        {
            paths.insert(resolver.sst_path(&view.sst.id).to_string());
        }
    }
    paths
}

fn manifest_path(id: u64) -> String {
    // SlateDB's pinned transactional-object manifest filename format.
    format!("{DB_PATH}/manifest/{id:020}.manifest")
}

async fn compact_and_copy(plan: &Plan) {
    let source = store(&plan.root, "objects");
    let db = open_db(source.clone()).await;
    write_rows(&db, 0, 2).await;
    write_rows(&db, 2, CHECKPOINT_ROWS).await;
    // The running owner flushes its own state and pins the resulting manifest.
    // Opening a second writer here would fence the source rather than back it up.
    let checkpoint = db
        .create_checkpoint(CheckpointScope::All, &CheckpointOptions::default())
        .await
        .unwrap();
    let admin = Admin::builder(DB_PATH, source.clone()).build();
    let pinned = admin
        .read_manifest(Some(checkpoint.manifest_id))
        .await
        .unwrap()
        .unwrap();
    let originals = referenced_ssts(&pinned);
    assert!(
        originals.len() >= 2,
        "the checkpoint must pin real original SSTs"
    );
    assert_eq!(
        pinned.checkpoints().len(),
        1,
        "this single-root image has no older checkpoint roots"
    );
    write_rows(&db, CHECKPOINT_ROWS, BEFORE_COPY_ROWS).await;
    let before = admin.read_manifest(None).await.unwrap().unwrap();
    let inputs = referenced_ssts(&before);
    let unpinned: BTreeSet<_> = inputs.difference(&originals).cloned().collect();
    assert!(
        !unpinned.is_empty(),
        "the same collector must have eligible unpinned controls"
    );
    let replacements = compact_inputs(&db, &admin, &before).await;
    collect(&admin).await;
    assert_present(source.as_ref(), &originals).await;
    assert_absent(source.as_ref(), &unpinned).await;
    assert_history(&db, BEFORE_COPY_ROWS).await;
    let image = Image {
        checkpoint: checkpoint.id.to_string(),
        manifest: checkpoint.manifest_id,
        originals,
        replacements,
        objects: vec![],
    };
    copy_image(plan, source.as_ref(), &db, &pinned, image).await;
    // The writer was live throughout copying. Its newer records are deliberately
    // outside the checkpoint and must not appear in the restored cut.
    assert_history(&db, LIVE_ROWS).await;
    crash(plan);
}

async fn compact_inputs(db: &Db, admin: &Admin, before: &VersionedManifest) -> BTreeSet<String> {
    let newest = before
        .l0()
        .iter()
        .map(|view| view.sst.id.unwrap_compacted_id().datetime())
        .max()
        .unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        while SystemTime::now().duration_since(newest).unwrap_or_default()
            < Duration::from_millis(1)
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let compaction = admin
        .submit_compaction(CompactionSpec::new(
            before
                .l0()
                .iter()
                .map(|view| SourceId::SstView(view.id))
                .collect(),
            0,
        ))
        .await
        .unwrap();
    assert!(
        compaction.id().datetime() > newest,
        "all input IDs precede the compaction GC watermark"
    );
    tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            let current = admin
                .read_compaction(compaction.id(), None)
                .await
                .unwrap()
                .unwrap();
            assert_ne!(current.status(), CompactionStatus::Failed);
            if current.status() == CompactionStatus::Completed {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    db.refresh_manifest().await.unwrap();
    let compacted = admin.read_manifest(None).await.unwrap().unwrap();
    let replacements = referenced_ssts(&compacted);
    assert!(!replacements.is_empty());
    assert!(
        referenced_ssts(before).is_disjoint(&replacements),
        "real compaction must rewrite the inputs"
    );
    retire_compactor_grace(admin, before, &compacted).await;
    // A newer L0 is the collector's second publication barrier. It contains no
    // history record and therefore cannot supply the checked payloads.
    db.put(b"~gc-publication-barrier", b"published after compaction")
        .await
        .unwrap();
    db.flush().await.unwrap();
    let barrier = admin.read_manifest(None).await.unwrap().unwrap();
    assert!(
        barrier
            .l0()
            .iter()
            .any(|view| view.sst.id.unwrap_compacted_id().datetime() > newest)
    );
    replacements
}

/// SlateDB also pins the old manifest for 15 minutes to protect old iterators.
/// This controlled fixture has never opened a reader at this phase; the writer
/// refreshed and the only compaction completed. Retire precisely those new
/// grace pins through the public API, preserving every preexisting checkpoint.
async fn retire_compactor_grace(
    admin: &Admin,
    before: &VersionedManifest,
    after: &VersionedManifest,
) {
    let compactions = admin.read_compactions(None).await.unwrap().unwrap();
    assert!(
        compactions
            .recent_compactions()
            .all(|compaction| compaction.status() == CompactionStatus::Completed)
    );
    let prior: BTreeSet<_> = before
        .checkpoints()
        .iter()
        .map(|checkpoint| checkpoint.id)
        .collect();
    let grace: Vec<_> = after
        .checkpoints()
        .iter()
        .filter(|checkpoint| !prior.contains(&checkpoint.id))
        .collect();
    assert!(
        !grace.is_empty(),
        "the compactor must have installed its reader grace pin"
    );
    for checkpoint in grace {
        assert!(checkpoint.expire_time.is_some());
        let pinned = admin
            .read_manifest(Some(checkpoint.manifest_id))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            referenced_ssts(&pinned),
            referenced_ssts(before),
            "retire only the completed compaction's exact input graph"
        );
        admin.delete_checkpoint(checkpoint.id).await.unwrap();
    }
    let current = admin.read_manifest(None).await.unwrap().unwrap();
    assert_eq!(
        current
            .checkpoints()
            .iter()
            .map(|checkpoint| checkpoint.id)
            .collect::<BTreeSet<_>>(),
        prior
    );
}

async fn copy_image(
    plan: &Plan,
    source: &dyn ObjectStore,
    db: &Db,
    pinned: &VersionedManifest,
    mut image: Image,
) {
    let backup = store(&plan.root, "backup");
    let broken = store(&plan.root, "broken");
    let mut paths = referenced_ssts(pinned);
    paths.insert(manifest_path(image.manifest));
    for (index, path) in paths.into_iter().enumerate() {
        let location = ObjectPath::from(path.as_str());
        let bytes = source.get(&location).await.unwrap().bytes().await.unwrap();
        let digest = ObjectDigest {
            path,
            size: u64::try_from(bytes.len()).unwrap(),
            sha256: format!("{:x}", Sha256::digest(&bytes)),
        };
        backup.put(&location, bytes.clone().into()).await.unwrap();
        broken.put(&location, bytes.into()).await.unwrap();
        image.objects.push(digest);
        if index == 0 {
            // Copying does not quiesce the writer: a new durable history row
            // lands between object copies, after the checkpoint's exact cut.
            write_rows(db, BEFORE_COPY_ROWS, LIVE_ROWS).await;
        }
    }
    audit_image(backup.clone(), &image).await.unwrap();
    std::fs::write(
        plan.root.join("image.json"),
        serde_json::to_vec_pretty(&image).unwrap(),
    )
    .unwrap();
}

fn load_image(plan: &Plan) -> Image {
    serde_json::from_slice(&std::fs::read(plan.root.join("image.json")).unwrap()).unwrap()
}

async fn collect(admin: &Admin) {
    admin
        .run_gc_once(GarbageCollectorOptions {
            compacted_options: Some(GarbageCollectorDirectoryOptions {
                min_age: Duration::ZERO,
                ..Default::default()
            }),
            manifest_options: None,
            wal_options: None,
            wal_fence_options: None,
            compactions_options: None,
            detach_options: None,
            ..Default::default()
        })
        .await
        .unwrap();
}

async fn assert_present(store: &dyn ObjectStore, paths: &BTreeSet<String>) {
    assert!(!paths.is_empty());
    for path in paths {
        assert!(
            store
                .head(&ObjectPath::from(path.as_str()))
                .await
                .unwrap()
                .size
                > 0
        );
    }
}

async fn assert_absent(store: &dyn ObjectStore, paths: &BTreeSet<String>) {
    assert!(!paths.is_empty());
    for path in paths {
        assert!(
            matches!(
                store.head(&ObjectPath::from(path.as_str())).await,
                Err(object_store::Error::NotFound { .. })
            ),
            "old SST must be physically absent: {path}"
        );
    }
}

async fn audit_image(store: Arc<dyn ObjectStore>, image: &Image) -> anyhow::Result<()> {
    let manifest = Admin::builder(DB_PATH, store.clone())
        .build()
        .read_manifest(Some(image.manifest))
        .await?
        .ok_or_else(|| anyhow::anyhow!("missing checkpoint manifest"))?;
    anyhow::ensure!(
        manifest
            .checkpoints()
            .iter()
            .any(|checkpoint| checkpoint.id.to_string() == image.checkpoint
                && checkpoint.manifest_id == image.manifest),
        "image manifest does not contain its claimed checkpoint root"
    );
    let mut expected = referenced_ssts(&manifest);
    expected.insert(manifest_path(image.manifest));
    anyhow::ensure!(
        expected == image.objects.iter().map(|o| o.path.clone()).collect(),
        "image inventory differs from manifest reference graph"
    );
    for object in &image.objects {
        let bytes = store
            .get(&ObjectPath::from(object.path.as_str()))
            .await?
            .bytes()
            .await?;
        anyhow::ensure!(
            u64::try_from(bytes.len())? == object.size,
            "object length changed: {}",
            object.path
        );
        anyhow::ensure!(
            format!("{:x}", Sha256::digest(&bytes)) == object.sha256,
            "object checksum changed: {}",
            object.path
        );
    }
    Ok(())
}

fn check_frame(raw: &[u8], offset: u64) {
    let frame = crate::shard::record::decode_at(raw, offset).unwrap();
    assert_eq!(frame.header.routing_key, route_key(offset));
    assert_eq!(
        crate::crypto::decrypt_frame(&KEY, &INC.0, &frame, raw).unwrap(),
        format!("checkpoint-payload-{offset}").as_bytes()
    );
}

async fn assert_history(db: &Arc<Db>, count: u64) {
    let (frames, last, completed) = read_history2(db, ROUTE, INC, 0, count, None, 1 << 20)
        .await
        .unwrap();
    assert!(completed);
    assert_eq!(last, Some(count - 1));
    assert_eq!(frames.len(), usize::try_from(count).unwrap());
    for (offset, frame) in frames.iter().enumerate() {
        check_frame(frame, u64::try_from(offset).unwrap());
    }
    let cache = crate::postings_cache::PostingsCache::new(1 << 20);
    let (frames, last, completed) =
        read_history2_keyed_cached(&cache, db, ROUTE, INC, "wanted", 0, count, count, 1 << 20)
            .await
            .unwrap();
    assert!(completed);
    assert_eq!(last, Some(count - 1));
    assert_eq!(frames.len(), usize::try_from(count.div_ceil(2)).unwrap());
    for (offset, frame) in frames.iter().enumerate() {
        check_frame(frame, u64::try_from(offset).unwrap() * 2);
    }
}

async fn read_checkpoint(store: Arc<dyn ObjectStore>, image: &Image) -> anyhow::Result<()> {
    let reader = DbReader::builder(DB_PATH, store)
        .with_reader_mode(DbReaderMode::Checkpoint(image.checkpoint.parse()?))
        .with_db_cache_disabled()
        .build()
        .await?;
    let result = async {
        let mut rows = reader
            .scan(hist2_record_key(ROUTE, INC, 0)..hist2_record_key(ROUTE, INC, u64::MAX))
            .await?;
        let mut count = 0;
        while let Some(row) = rows.next().await? {
            check_frame(&row.value, count);
            count += 1;
        }
        anyhow::ensure!(
            count == CHECKPOINT_ROWS,
            "checkpoint contains {count} records instead of its exact cut"
        );
        Ok(())
    }
    .await;
    reader.close().await.unwrap();
    result
}

async fn release_pin(plan: &Plan) {
    let image = load_image(plan);
    let source = store(&plan.root, "objects");
    assert_present(source.as_ref(), &image.originals).await;
    read_checkpoint(source.clone(), &image).await.unwrap();
    let db = open_db(source.clone()).await;
    assert_history(&db, LIVE_ROWS).await;
    let admin = Admin::builder(DB_PATH, source.clone()).build();
    admin
        .delete_checkpoint(image.checkpoint.parse().unwrap())
        .await
        .unwrap();
    assert!(
        admin
            .read_manifest(None)
            .await
            .unwrap()
            .unwrap()
            .checkpoints()
            .is_empty()
    );
    db.refresh_manifest().await.unwrap();
    collect(&admin).await;
    assert_absent(source.as_ref(), &image.originals).await;
    assert_present(source.as_ref(), &image.replacements).await;
    assert_history(&db, LIVE_ROWS).await;
    crash(plan);
}

async fn verify_cold(plan: &Plan) {
    let image = load_image(plan);
    let source = store(&plan.root, "objects");
    assert_absent(source.as_ref(), &image.originals).await;
    let db = open_db(source.clone()).await;
    let manifest = Admin::builder(DB_PATH, source.clone())
        .build()
        .read_manifest(None)
        .await
        .unwrap()
        .unwrap();
    assert_present(source.as_ref(), &referenced_ssts(&manifest)).await;
    assert_history(&db, LIVE_ROWS).await;
    let backup = store(&plan.root, "backup");
    audit_image(backup.clone(), &image).await.unwrap();
    read_checkpoint(backup, &image).await.unwrap();
    db.close().await.unwrap();
}

async fn missing_object(plan: &Plan) {
    let image = load_image(plan);
    let broken = store(&plan.root, "broken");
    let missing = image.originals.first().unwrap();
    broken
        .delete(&ObjectPath::from(missing.as_str()))
        .await
        .unwrap();
    assert_absent(broken.as_ref(), &BTreeSet::from([missing.clone()])).await;
    assert!(
        audit_image(broken.clone(), &image).await.is_err(),
        "a missing referenced SST must fail the graph audit"
    );
    assert!(
        read_checkpoint(broken, &image).await.is_err(),
        "actual SlateDB recovery must not silently complete over a missing SST"
    );
    audit_image(store(&plan.root, "backup"), &image)
        .await
        .unwrap();
}
