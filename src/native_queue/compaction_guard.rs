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
    }
}

/// A shard log's builder whose shared block cache is scoped to its path, so
/// two logs sharing the process cache never share an entry.
pub(crate) struct ShardLogBuilder {
    builder: slatedb::DbBuilder<object_store::path::Path>,
    scope: u64,
}

impl ShardLogBuilder {
    /// The builder reads through `cache` under the log's scope.
    pub(crate) fn with_db_cache(self, cache: Arc<dyn slatedb::db_cache::DbCache>) -> Self {
        Self {
            builder: self.builder.with_db_cache(cache, self.scope),
            scope: self.scope,
        }
    }

    /// Opens the shard log.
    pub(crate) async fn build(self) -> Result<slatedb::Db, slatedb::Error> {
        self.builder.build().await
    }
}
