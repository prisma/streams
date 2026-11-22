//! The split's warm check (F5 part 1b, GATE-DECISION-f5-checkpoint-compaction-2026-11-17 and its
//! addenda), last in Preparing after the parent's log settled (`topology::settle`). Preparing's
//! log read leaves the blocks the pause reads cached. A compaction that lands after it replaces
//! tables with output no read has warmed (a flush caches what it writes, a compaction does not),
//! and the pause then reads it cold (115 table GETs over 400 queues on Linux). So Preparing ends
//! with a warm check: the manifest refreshed (the pause's checkpoint reloads it, and the writer's
//! own view can be a poll stale), its compacted tables compared with those its read saw, and the
//! log read again, its result dropped, while they differ, at most `WARM_READS` times.

use crate::topology::settle::{SETTLE, Settled, elapsed_ms, settled_within};
use std::collections::BTreeSet;

/// How many times the warm check reads the log again before the split parks anyway.
const WARM_READS: usize = 3;

/// The views of the tables in `parent`'s sorted runs (the compactor's output), every tree's: the
/// tables a read made now warms.
pub(super) fn compacted_views(parent: &crate::shard::ShardEngine) -> BTreeSet<u128> {
    let manifest = parent.db.manifest();
    let runs = manifest
        .segments()
        .iter()
        .flat_map(|segment| segment.compacted().iter())
        .chain(manifest.compacted().iter());
    runs.flat_map(|run| run.sst_views().iter().map(|view| view.id.0))
        .collect()
}

/// Preparing's warm check, last before the park: `parent`'s manifest refreshed, its L0 settled
/// again if a flush brought it to its line (within what is left of `SETTLE`), and its log read
/// again while its compacted tables differ from `seen`, those Preparing's read saw, at most
/// `WARM_READS` times. `settled` with the reads it made.
pub(super) async fn warmed(
    parent: &crate::shard::ShardEngine,
    mut settled: Settled,
    mut seen: BTreeSet<u128>,
) -> Result<Settled, String> {
    let line = parent.l0_settle_line();
    loop {
        parent
            .db
            .refresh_manifest()
            .await
            .map_err(|e| format!("manifest refresh: {e}"))?;
        if parent.l0_now() >= line {
            let left = SETTLE.saturating_sub(settled.started.elapsed());
            settled_within(&parent.db, || parent.l0_now(), line, left).await?;
            settled.ms = elapsed_ms(settled.started);
        }
        let now = compacted_views(parent);
        if now == seen || settled.warm_reads == WARM_READS {
            settled.l0 = parent.l0_now();
            return Ok(settled);
        }
        seen = now;
        drop(super::reads::log_reads(parent, None).await?);
        settled.warm_reads += 1;
    }
}
