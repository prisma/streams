//! A commit group's cold native queues (docs/QUEUES-DESIGN.md §6.1-§6.2):
//! what the group's operations on a queue will read from the shard log is
//! read before the group stages, together with its other cold streams'
//! loads (`prepare`), so the operations find it in the block cache instead
//! of waiting for the store one after another on the shard's one serial
//! commit path. A queue touched once an hour is cold at every touch, and
//! at every sweep its debt brings it to an hour later: its state row, its
//! index blocks and its settled rows have left the memtable and the cache.
use std::collections::BTreeSet;

use super::stage::{NativeStage, index_range, key_time};
use crate::native_queue::{NqOp, keys};
use crate::shard::transaction::CommitTransaction;
use crate::shard::{ShardEngine, now_ms};

/// What a group's operations on one native queue read before they stage.
#[derive(Default)]
pub(in crate::shard::transaction) struct NativeReads {
    /// The largest claim the group makes of the queue; 0 when it makes none.
    claim: usize,
    /// The offsets the group's settles name.
    settles: BTreeSet<u64>,
    /// Whether the group sweeps the queue.
    sweep: bool,
}

/// The families a sweep walks (`nq_sweep`), in its order.
const SWEPT: [u8; 5] = [
    keys::LEASES,
    keys::RETRIES,
    keys::AVAIL,
    keys::FAIL_EXPIRY,
    keys::DEDUP_EXPIRY,
];

impl NativeReads {
    /// Add what `op` will read.
    pub(in crate::shard::transaction) fn add(&mut self, op: &NqOp) {
        match op {
            NqOp::Claim { max, .. } => self.claim = self.claim.max(*max),
            NqOp::Settle { items } => {
                self.settles
                    .extend(items.iter().map(|item| item.token.offset));
            }
            NqOp::Sweep => self.sweep = true,
            _ => {}
        }
    }
}

impl CommitTransaction<'_> {
    /// Read what the group's operations on the native queue `hash` will
    /// read: its state row, the stored rows a claim's or a sweep's walk of
    /// each index reads, and each settled delivery's metadata and delivery
    /// rows. The
    /// reads are the operations' own, from the same shard log, so the
    /// operations stage exactly as they would without them; a read that
    /// fails here is read again, and refuses its own operation, when the
    /// operation stages.
    pub(in crate::shard::transaction) async fn warm_native(
        engine: &ShardEngine,
        hash: &[u8; 16],
        reads: &NativeReads,
    ) {
        let Ok(mut st) = NativeStage::load(engine, hash).await else {
            return;
        };
        if !st.exists {
            return;
        }
        if reads.claim > 0 {
            Self::warm_claim(engine, hash, &mut st, reads.claim).await;
        }
        if reads.sweep {
            Self::warm_sweep(engine, hash, &mut st).await;
        }
        Self::warm_settles(engine, hash, &reads.settles).await;
    }

    /// The stored rows a claim of at most `max` messages walks, of each
    /// index whose floor says something is due (`nq_candidates`).
    async fn warm_claim(engine: &ShardEngine, hash: &[u8; 16], st: &mut NativeStage, max: usize) {
        let t = now_ms().max(st.state.clock);
        let want = super::claim::due_want(max);
        for fam in super::claim::FAMILIES {
            if !st.state.floors.of(fam).is_some_and(|floor| *floor > t) {
                Self::warm_index(engine, hash, st, fam, (want, Some(t))).await;
            }
        }
    }

    /// The stored rows a sweep walks: every family it sweeps, the retry
    /// index only once a retry's cycle deadline has come (`nq_sweep`).
    async fn warm_sweep(engine: &ShardEngine, hash: &[u8; 16], st: &mut NativeStage) {
        let t = now_ms().max(st.state.clock);
        for fam in SWEPT {
            if fam != keys::RETRIES || st.state.floors.retry_cycle <= t {
                let step = crate::native_queue::SWEEP_STEP;
                Self::warm_index(engine, hash, st, fam, (step, None)).await;
            }
        }
    }

    /// The metadata and delivery rows of each settled delivery
    /// (`nq_settle_one`).
    async fn warm_settles(engine: &ShardEngine, hash: &[u8; 16], offsets: &BTreeSet<u64>) {
        let keys = offsets.iter().flat_map(|offset| {
            [keys::META, keys::DELIVERY].map(|fam| keys::by_offset(hash, fam, *offset))
        });
        for key in keys {
            #[cfg(test)]
            crate::shard::test_support::held_native_row_read(&engine.prefix).await;
            if engine.db.get(key).await.is_err() {
                return;
            }
        }
    }

    /// The stored rows a walk of `fam` reads: from where it starts
    /// (`index_range`), at most `want` rows, through the first entry past
    /// `stop` when one is given (a claim's, `nq_due_scan`; a sweep's reads
    /// every row up to its limit, `nq_index_scan`).
    async fn warm_index(
        engine: &ShardEngine,
        hash: &[u8; 16],
        st: &mut NativeStage,
        fam: u8,
        read: (usize, Option<i64>),
    ) {
        let (want, stop) = read;
        let Some((from, to)) = index_range(hash, fam, st) else {
            return;
        };
        #[cfg(test)]
        crate::shard::test_support::held_native_index_read(&engine.prefix).await;
        let options = crate::native_queue::scan_options();
        let Ok(mut rows) = engine.db.scan_with_options(from..to, &options).await else {
            return;
        };
        for _ in 0..want {
            let Ok(Some(row)) = rows.next().await else {
                return;
            };
            #[cfg(test)]
            crate::shard::test_support::counted_native_index_row(&engine.prefix);
            if !stop.is_none_or(|t| key_time(&row.key).is_some_and(|at| at <= t)) {
                return;
            }
        }
    }
}
