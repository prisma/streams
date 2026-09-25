//! Repeated history reads retain key checks and delete/recreate isolation,
//! and the TLA-019 pin: a stale writer view survives a compaction GC.
use super::fixture_http::{HttpRigOptions, engine_shutdown, http_rig_build};
use super::fixture_requests::{PRISMA_KEY, preq};
use super::fixture_runtime::RigRuntime;
use super::fixture_storage::{
    append_sized, mem, open_engine_with_absorber_cfg, skey, wait_all_absorbed,
};
use std::time::Duration;
#[expect(
    clippy::too_many_lines,
    reason = "history lifecycle regression; one stream is read, deleted and recreated to prove the incarnation transition; splitting the scenario would disconnect its before/after assertions"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn history_reads_keep_key_checks_and_delete_recreate_isolation() {
    let rig = http_rig_build(
        mem(),
        RigRuntime::first(),
        HttpRigOptions {
            shard: crate::shard::ShardConfig {
                tail_ring_bytes: 0,
                ..Default::default()
            },
            absorber: Some(crate::history::AbsorberConfig {
                threshold_bytes: 1,
                threshold_age: Duration::from_millis(1),
                tick: Duration::from_millis(20),
                ..Default::default()
            }),
            ..Default::default()
        },
    )
    .await;
    let headers = [("prisma-encryption-key", PRISMA_KEY)];
    let create = "/v1/streams/history-lifecycle";
    let records = "/v1/streams/history-lifecycle/records?routingKey=hot";
    assert_eq!(
        preq(
            rig.addr,
            "PUT",
            create,
            &headers,
            br#"{"format":{"kind":"bytes"}}"#
        )
        .await
        .0,
        201
    );
    assert_eq!(
        preq(
            rig.addr,
            "POST",
            "/v1/streams/history-lifecycle/records",
            &[
                ("prisma-encryption-key", PRISMA_KEY),
                ("prisma-routing-key", "hot")
            ],
            b"old payload"
        )
        .await
        .0,
        200
    );
    let desc = rig
        .state
        .registry
        .get(&rig.state.deployment.raw_adapter_sref("history-lifecycle"))
        .await
        .unwrap()
        .unwrap();
    let engine = rig
        .state
        .engine_for(&desc.segment_route_by_id(0).unwrap())
        .await
        .unwrap();
    wait_all_absorbed(&engine, &[desc.storage_hash()]).await;
    for _ in 0..2 {
        let (status, h, body) = preq(rig.addr, "GET", records, &headers, b"").await;
        assert_eq!(status, 200);
        assert_eq!(body, b"old payload");
        assert_eq!(h.get("prisma-up-to-date").map(String::as_str), Some("true"));
    }
    let wrong = preq(
        rig.addr,
        "GET",
        records,
        &[(
            "prisma-encryption-key",
            "CQkJCQkJCQkJCQkJCQkJCQkJCQkJCQkJCQkJCQkJCQk",
        )],
        b"",
    )
    .await;
    assert_ne!(wrong.0, 200);
    assert_ne!(wrong.2, b"old payload");
    assert!(matches!(
        preq(rig.addr, "DELETE", create, &headers, b"").await.0,
        200 | 204
    ));
    assert_ne!(preq(rig.addr, "GET", records, &headers, b"").await.0, 200);
    assert_eq!(
        preq(
            rig.addr,
            "PUT",
            create,
            &headers,
            br#"{"format":{"kind":"bytes"}}"#
        )
        .await
        .0,
        201
    );
    assert_eq!(
        preq(
            rig.addr,
            "POST",
            "/v1/streams/history-lifecycle/records",
            &[
                ("prisma-encryption-key", PRISMA_KEY),
                ("prisma-routing-key", "hot")
            ],
            b"new payload"
        )
        .await
        .0,
        200
    );
    assert_eq!(
        preq(rig.addr, "GET", records, &headers, b"").await.2,
        b"new payload"
    );
    engine_shutdown(&rig.state).await;
    rig.tasks.shutdown(Duration::from_secs(5)).await;
}

/// Polls the stored manifest until the embedded compactor has replaced the L0s.
async fn compacted_manifest(admin: &slatedb::admin::Admin) -> slatedb::VersionedManifest {
    for _ in 0..1500 {
        let stored = admin.read_manifest(None).await.unwrap().unwrap();
        if stored.l0().is_empty() && stored.last_compacted_l0_sst_view_id().is_some() {
            return stored;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("the embedded compactor never merged the four L0s");
}

/// The object ids of the L0 SSTs a manifest view names.
fn l0_ids(view: &slatedb::VersionedManifest) -> Vec<String> {
    let ids = view.l0().iter().map(|l0| l0.sst.id.unwrap_compacted_id());
    ids.map(|id| id.to_string()).collect()
}

/// The checkpoint whose manifest still names every L0 of the stale view.
async fn protecting_checkpoint(
    admin: &slatedb::admin::Admin,
    stored: &slatedb::VersionedManifest,
    stale: &[String],
) -> slatedb::Checkpoint {
    for checkpoint in stored.checkpoints() {
        let pinned = admin.read_manifest(Some(checkpoint.manifest_id)).await;
        let pinned = l0_ids(&pinned.unwrap().unwrap());
        if stale.iter().all(|id| pinned.contains(id)) {
            return checkpoint.clone();
        }
    }
    panic!("no checkpoint names the L0s the writer view reads");
}

/// One pass of the compacted-SST collector with a zero min_age, standing in
/// for the replaced inputs being older than the production min_age.
async fn collect_compacted(admin: &slatedb::admin::Admin) {
    let compacted = slatedb::config::GarbageCollectorDirectoryOptions {
        interval: None,
        min_age: Duration::ZERO,
        dry_run: false,
    };
    let only_compacted = slatedb::config::GarbageCollectorOptions {
        manifest_options: None,
        wal_options: None,
        wal_fence_options: None,
        compacted_options: Some(compacted),
        compactions_options: None,
        detach_options: None,
        ..Default::default()
    };
    admin.run_gc_once(only_compacted).await.unwrap();
}

/// Pin for TLA-019 (verification/tla/history, ReachGC; finding F1). History
/// reads use the writer Db, whose manifest view merges the embedded
/// compactor's commit only on its poll ticker, so a quiet partition keeps
/// reading L0s the compactor replaced. The pinned slatedb protects them: the
/// compactor checkpoints the pre-compaction manifest before each commit and
/// the GC keeps whatever a checkpoint names. Foyer keeps each shard's newest
/// entry even over capacity, so the block cache is one one-byte shard: it
/// holds one block and cannot hide a deleted L0. Deleting the checkpoint is
/// the control that it, not timing, keeps the stale read whole.
///
/// Invalid when a slatedb bump drops that checkpoint, shortens it to within
/// two poll intervals or stops the GC honouring it: F1 is then open and needs
/// a repository fix (refresh or pin the read view), not a weaker assertion.
async fn stale_view_read_across_compaction_gc(selector: Option<&str>) {
    use crate::application::read::read_merged;
    use futures_util::TryStreamExt;
    use slatedb::db_cache::foyer::{FoyerCache, FoyerCacheOptions};
    let store = mem();
    let (prefix, hash, key) = ("tla-019-pin", [0x19u8; 16], skey());
    let mut history = crate::history::HistoryResources::new(&Default::default(), usize::MAX);
    let one_block = FoyerCacheOptions {
        max_capacity: 1,
        shards: 1,
    };
    history.cache = std::sync::Arc::new(FoyerCache::new_with_opts(one_block));
    let cfg = crate::shard::ShardConfig {
        shared_history: Some(std::sync::Arc::new(history)),
        compactor_options: slatedb::config::CompactorOptions {
            poll_interval: Duration::from_millis(20),
            ..crate::config::EngineConfig::default().compactor_options()
        },
        ..Default::default()
    };
    // The engine builds the partition from this same settings owner.
    let settings = crate::history::history2_settings(&cfg.history, &cfg.compactor_options);
    let poll = settings.manifest_poll_interval;
    let (engine, absorber) = open_engine_with_absorber_cfg(store.clone(), prefix, cfg).await;
    for _ in 0..4 {
        append_sized(&engine, hash, &key, "k", 64).await;
        wait_all_absorbed(&engine, &[hash]).await;
    }
    let path = crate::sharddir::history2_path(prefix);
    let admin = slatedb::admin::AdminBuilder::new(path.as_str(), store.clone()).build();
    let stored = compacted_manifest(&admin).await;
    let part = engine.history_partition().await.unwrap();
    let stale = l0_ids(&part.manifest());
    assert_eq!(stale.len(), 4, "the writer view names the replaced L0s");
    let protecting = protecting_checkpoint(&admin, &stored, &stale).await;
    let expiry = protecting.expire_time.expect("an expiring checkpoint");
    let lifetime = (expiry - protecting.create_time).to_std().unwrap();
    assert!(
        lifetime > poll * 2,
        "checkpoint lifetime {lifetime:?} must outlast the {poll:?} stale view by a poll interval"
    );
    let objects = format!("{path}/compacted").into();
    let ssts = async || {
        let listed = store.list(Some(&objects)).try_collect::<Vec<_>>().await;
        listed.unwrap().len()
    };
    let handle = engine.stream_handle(hash).await.unwrap();
    let max = 8 * 1024 * 1024;
    let read = async || {
        let durable = crate::shard::Deliver::Durable;
        let page = read_merged(&key, &hash, &handle, &engine, 0, selector, max, durable);
        let page = page.await?;
        Ok::<_, String>(page.recs.iter().map(|rec| rec.off).collect::<Vec<u64>>())
    };
    let before = ssts().await;
    collect_compacted(&admin).await;
    assert_eq!(ssts().await, before, "the checkpoint kept every L0");
    assert_eq!(read().await, Ok(vec![0, 1, 2, 3]), "stale view read");
    // Control: the same stale view without the checkpoint.
    admin.delete_checkpoint(protecting.id).await.unwrap();
    collect_compacted(&admin).await;
    assert!(ssts().await < before, "the GC collected replaced L0s");
    assert_eq!(l0_ids(&part.manifest()), stale, "the view is still stale");
    let err = read().await.expect_err("a read over a collected L0 fails");
    let mut missing = stale.iter().map(|id| format!("{id}.sst not found"));
    assert!(missing.any(|id| err.contains(&id)), "not NotFound: {err}");
    part.refresh_manifest().await.unwrap();
    assert_eq!(read().await, Ok(vec![0, 1, 2, 3]), "refreshed view read");
    engine.begin_close();
    absorber.abort();
}

/// TLA-019 pin, unfiltered history scan (see the scenario above).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn tla019_pin_history_scan_survives_compaction_gc_on_stale_view() {
    stale_view_read_across_compaction_gc(None).await;
}

/// TLA-019 pin, keyed history read (see the scenario above).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn tla019_pin_keyed_history_read_survives_compaction_gc_on_stale_view() {
    stale_view_read_across_compaction_gc(Some("k")).await;
}
