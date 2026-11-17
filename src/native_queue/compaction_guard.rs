//! The shard log's database builder. Its compactions run under SlateDB's
//! size-tiered scheduler; SlateDB itself refuses a submitted spec that shares
//! a sorted run or SST view with a scheduled, running or committing job
//! (slatedb/slatedb#2191, carried by the pinned fork), so the queue
//! collector's spec (every L0 view and every sorted run into the last run)
//! can no longer run beside a regular compaction into the last run, which
//! closed the shard log on every open in the 100x run of 2026-10-24. Streams'
//! own guard over the scheduler went once the pin carried #2191 (the owner's
//! decision of 2026-10-26); the module keeps its path for `bootstrap::run`.
use std::sync::Arc;

/// The shard log's database builder: `settings`' compactor options run under
/// SlateDB's own scheduler (layout 7: one tree, `crate::keyspace`), and the
/// log's memtable freeze size is recorded for its sealer.
pub(crate) fn shard_log_builder(
    path: &str,
    store: Arc<dyn object_store::ObjectStore>,
    settings: slatedb::config::Settings,
) -> ShardLogBuilder {
    crate::spill::metrics::for_log(path).note_freeze_bytes(settings.l0_sst_size_bytes);
    ShardLogBuilder {
        builder: crate::keyspace::db_builder(path, store).with_settings(settings),
        scope: crate::runtime::block_cache::scope_of(path),
        path: path.to_string(),
    }
}

/// A shard log's builder whose shared block cache is scoped to its path, so
/// two logs sharing the process cache never share an entry.
pub(crate) struct ShardLogBuilder {
    builder: slatedb::DbBuilder<object_store::path::Path>,
    scope: u64,
    path: String,
}

impl ShardLogBuilder {
    /// The builder reads through `cache` under the log's scope.
    pub(crate) fn with_db_cache(self, cache: Arc<dyn slatedb::db_cache::DbCache>) -> Self {
        Self {
            builder: self.builder.with_db_cache(cache, self.scope),
            ..self
        }
    }

    /// Opens the shard log and checks its identity (`shard::check_identity`): a log of another
    /// layout or home is closed again and refused, before any engine reads it. The open's phases
    /// are logged (GATE-DECISION-meas149-rerun-10000-2026-11-17, M): SlateDB's build (the
    /// manifest, the writer fence and the WAL replay) with the WAL objects it replayed (from the
    /// manifest it opened at) and the memtable bytes they left, the store GETs it made, then the
    /// identity check.
    pub(crate) async fn build(self) -> Result<slatedb::Db, slatedb::Error> {
        let metrics = crate::spill::metrics::for_log(&self.path);
        let gets = metrics.reader_gets();
        let requests = metrics.requests();
        let building = std::time::Instant::now();
        let db = self.builder.build().await?;
        let built = building.elapsed();
        let checking = std::time::Instant::now();
        let identity = crate::shard::check_identity(&db, &self.path).await;
        let before = (gets, &requests);
        log_open(
            &self.path,
            &db,
            &metrics,
            before,
            (built, checking.elapsed()),
        );
        if let Err(refusal) = identity {
            if let Err(error) = db.close().await {
                tracing::warn!(path = %self.path, %error, "closing a refused shard log failed");
            }
            return Err(refusal);
        }
        Ok(db)
    }

    /// Tests: opens a log at a path that is no shard's (a label or an explicit root), without the
    /// identity check, for tests of the builder's scheduler and freeze size.
    #[cfg(test)]
    pub(crate) async fn build_without_identity(self) -> Result<slatedb::Db, slatedb::Error> {
        self.builder.build().await
    }
}
/// The open's phases at INFO: the build's time, the WAL objects it replayed (the manifest's next WAL
/// id less one, less the last WAL id its memtables already held) and the memtable bytes after it,
/// the store GETs it made (manifests, the WAL objects, SST metadata), its requests of every kind
/// by component (`requests`: the build's fences, puts, heads and lists, and the compactor's start
/// within it), and the identity check's time.
fn log_open(
    path: &str,
    db: &slatedb::Db,
    metrics: &crate::spill::metrics::ShardLogMetrics,
    (gets_before, requests_before): (u64, &std::collections::BTreeMap<String, u64>),
    (built, identity): (std::time::Duration, std::time::Duration),
) {
    let manifest = db.manifest();
    let replayed = manifest
        .next_wal_sst_id()
        .saturating_sub(manifest.replay_after_wal_id())
        .saturating_sub(1);
    let memtable_bytes = metrics.gauge(slatedb::db_stats::TOTAL_MEM_SIZE_BYTES);
    let gets = metrics.reader_gets().saturating_sub(gets_before);
    let requests = crate::spill::metrics::requests_since(requests_before, &metrics.requests());
    tracing::info!(
        path,
        build_ms = built.as_millis(),
        wal_objects_replayed = replayed,
        memtable_bytes,
        store_gets = gets,
        requests,
        identity_ms = identity.as_millis(),
        "shard log opened, by phase (build: manifest, fence, WAL replay)"
    );
}
