//! A source log settled before a topology operation parks it (F5 part 1b,
//! GATE-DECISION-f5-checkpoint-compaction-2026-11-17 and its addenda; for the merge,
//! GATE-DECISION-s22-join-2026-11-20 Addendum 17). The pause's checkpoint forces the memtable to
//! L0, and that upload waits while the log holds `l0_max_ssts` L0 tables, until a compaction frees
//! one (on Linux a compaction running through the pause cost 7.7 s in the checkpoint phase). So a
//! split's Preparing, for its parent, and a merge's, for each of its two sources, starts by flushing
//! the memtable to L0 and waiting until the log's L0 is below its L0 watermark `low` mark (half its
//! line: 16 in production), which leaves the pause's upload room. Past the bound the operation is
//! refused before the park, like a split's failed sum check, and rolls back.

use std::time::{Duration, Instant};

#[cfg(test)]
mod tests;

/// How long Preparing waits for a log to settle before it refuses the operation.
#[cfg(not(test))]
pub(super) const SETTLE: Duration = Duration::from_secs(30);
/// Tests: the bound, short enough for a test to pass it. Read from code: a released compaction
/// waits for the embedded worker's claim poll, jittered over 2.5 to 7.5 s, then lands on the
/// coordinator's 1 s commit tick; the tests that land a compaction inside the bound run a worker
/// of their own polling every 50 ms (GATE-DECISION-f5-resettle-margin-2026-11-18).
#[cfg(test)]
pub(super) const SETTLE: Duration = Duration::from_secs(12);

/// How often the wait reads the log's L0.
const POLL: Duration = Duration::from_millis(20);

/// How Preparing's start ended: when its wait started, how long it waited and the L0 length it
/// ended at.
#[derive(Debug)]
pub(super) struct Settled {
    pub(super) started: Instant,
    pub(super) ms: u64,
    pub(super) l0: usize,
    /// How many times a split's warm check read the log again (`split::warm`); a merge makes none.
    pub(super) warm_reads: usize,
}

/// `log`'s engine settled below its L0 settle line within `SETTLE` ([`settled_within`]).
pub(super) async fn settled(log: &crate::shard::ShardEngine) -> Result<Settled, String> {
    let started = Instant::now();
    let line = log.l0_settle_line();
    let l0 = settled_within(&log.db, || log.l0_now(), line, SETTLE).await?;
    Ok(Settled {
        started,
        ms: elapsed_ms(started),
        l0,
        warm_reads: 0,
    })
}

/// The milliseconds since `started`.
pub(super) fn elapsed_ms(started: Instant) -> u64 {
    u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX)
}

/// `log`'s memtable flushed to L0, then its L0 (`l0`) waited below `line`, both within `bound`
/// (a flush can itself wait for a compaction, when L0 is at SlateDB's `l0_max_ssts`):
/// where L0 ended; past the bound, why the operation is refused.
pub(super) async fn settled_within(
    log: &slatedb::Db,
    l0: impl Fn() -> usize,
    line: usize,
    bound: Duration,
) -> Result<usize, String> {
    let started = Instant::now();
    let memtable = slatedb::config::FlushOptions {
        flush_type: slatedb::config::FlushType::MemTable,
    };
    match tokio::time::timeout(bound, log.flush_with_options(memtable)).await {
        Ok(flushed) => flushed.map_err(|e| format!("memtable flush: {e}"))?,
        Err(_) => return Err(unsettled(bound, l0(), line)),
    }
    loop {
        let now = l0();
        if now < line {
            return Ok(now);
        }
        if started.elapsed() >= bound {
            return Err(unsettled(bound, now, line));
        }
        tokio::time::sleep(POLL).await;
    }
}

/// Why an operation whose source log did not settle within `bound` is refused: `l0` L0 tables,
/// not below `line`.
fn unsettled(bound: Duration, l0: usize, line: usize) -> String {
    format!("the log did not settle within {bound:?}: {l0} L0 tables, its line {line}")
}
