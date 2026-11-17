//! The split's reads of the parent's log (GATE-DECISION-f1b-trim-race-2026-11-16; F5,
//! GATE-DECISION-f5-pause-log-reads-2026-11-17): the sentinel rows and the trimmed intervals of each
//! queue they name, read from one snapshot at one sequence, by Preparing and again in the pause.

use super::phases::Phases;
use crate::keyspace::SENTINEL;
use crate::topology::sentinel::{self, Row};

/// What a plan read of the parent's log, at one sequence (`log_reads`).
pub(super) struct LogReads {
    /// Its sentinel rows.
    pub(super) rows: Vec<Row>,
    /// The entries those rows and its queues' `'k'` rows release.
    pub(super) released: sentinel::Released,
}

/// The parent's sentinel rows and its queues' trimmed intervals, read from one snapshot of its log
/// (the memtable's batches included) so both reads see the same batches: a trim pass writes its
/// `'R'` decrements and `'k'` intervals in one batch, which commits at one sequence, so sums that
/// do not hold at it are a stable fact (GATE-DECISION-f1b-trim-race-2026-11-16). Never `Remote`
/// here: its cap is recomputed per read, so two reads could see different batches. The snapshot is
/// dropped on return, before the run-index GETs: it holds the log's compaction back while it lives.
/// The pause's call marks its phases on `phases` (`snapshot`, `sentinel`, `intervals`) and notes
/// the rows it read and the queues whose intervals it read (`rows`, `heads`); Preparing's marks
/// none.
pub(super) async fn log_reads(
    parent: &crate::shard::ShardEngine,
    phases: Option<&Phases>,
) -> Result<LogReads, String> {
    let snapshot = parent.db.snapshot().map_err(|e| format!("snapshot: {e}"))?;
    mark(phases, "snapshot");
    let mut scan = snapshot
        .scan_prefix(&SENTINEL[..], ..)
        .await
        .map_err(|e| format!("sentinel scan: {e}"))?;
    let mut rows = Vec::new();
    while let Some(kv) = scan
        .next()
        .await
        .map_err(|e| format!("sentinel scan: {e}"))?
    {
        rows.push((kv.key.to_vec(), kv.value.to_vec()));
    }
    mark(phases, "sentinel");
    let mut released = sentinel::Released::of(&rows).map_err(|e| e.to_string())?;
    let heads = released.streams();
    for hash in &heads {
        let intervals = crate::shard::seal::gc::trim::intervals_at(&snapshot, hash).await?;
        released.add_intervals(*hash, &intervals);
    }
    mark(phases, "intervals");
    if let Some(phases) = phases {
        phases.note("rows", rows.len());
        phases.note("heads", heads.len());
    }
    Ok(LogReads { rows, released })
}

/// Mark `phase` on `phases`, if the call keeps them.
fn mark(phases: Option<&Phases>, phase: &str) {
    if let Some(phases) = phases {
        phases.mark(phase);
    }
}
