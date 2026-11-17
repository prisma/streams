//! The parent's log settled before the split parks it (F5 part 1b,
//! GATE-DECISION-f5-checkpoint-compaction-2026-11-17 and its addenda). The pause's checkpoint
//! forces the memtable to L0, and that upload waits while the log holds `l0_max_ssts` L0 tables,
//! until a compaction frees one (on Linux a compaction running through the pause cost 7.7 s in the
//! checkpoint phase). So Preparing starts by flushing the memtable to L0 and waiting until the
//! log's L0 is below the parent's L0 watermark `low` mark (half its line: 16 in production), which
//! leaves the pause's upload room. Past the bound the split is refused before the park, like a
//! failed sum check.

use std::time::{Duration, Instant};

#[cfg(test)]
mod tests;

/// How long Preparing waits for the parent's log to settle before it refuses the split.
#[cfg(not(test))]
const SETTLE: Duration = Duration::from_secs(30);
/// Tests: the bound, short enough for a test to pass it, and long enough for the compactor's 5 s
/// polls to land a compaction (inferred from a 6 s wait, not traced).
#[cfg(test)]
const SETTLE: Duration = Duration::from_secs(12);

/// How often the wait reads the log's L0.
const POLL: Duration = Duration::from_millis(20);

/// How Preparing's start ended: how long it waited and the L0 length it ended at.
#[derive(Debug)]
pub(super) struct Settled {
    pub(super) ms: u64,
    pub(super) l0: usize,
}

/// `parent`'s log settled below its L0 settle line within `SETTLE` ([`settled_within`]).
pub(super) async fn settled(parent: &crate::shard::ShardEngine) -> Result<Settled, String> {
    let started = Instant::now();
    let line = parent.l0_settle_line();
    let l0 = settled_within(&parent.db, || parent.l0_now(), line, SETTLE).await?;
    Ok(Settled {
        ms: elapsed_ms(started),
        l0,
    })
}

/// The milliseconds since `started`.
fn elapsed_ms(started: Instant) -> u64 {
    u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX)
}

/// `log`'s memtable flushed to L0, then its L0 (`l0`) waited below `line`, both within `bound`
/// (a flush can itself wait for a compaction, when L0 is at SlateDB's `l0_max_ssts`):
/// where L0 ended; past the bound, why the split is refused.
async fn settled_within(
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

/// Why a split whose parent's log did not settle within `bound` is refused: `l0` L0 tables, not
/// below `line`.
fn unsettled(bound: Duration, l0: usize, line: usize) -> String {
    format!("the parent's log did not settle within {bound:?}: {l0} L0 tables, its line {line}")
}
