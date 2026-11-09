//! The committer's side of the shard's queue gauges (edge change #246,
//! `shard::layout::queue_gauges`): read once, at the engine's first queue
//! operation; moved and put in the batch by every operation that changes a
//! queue's bytes in a category; asked whether a queue is over its share at
//! a category's line. A group's first queue operation settles an earlier
//! group's staged moves by the generation the shard log holds.
use super::super::CommitTransaction;
use super::super::overlay::StreamOverlay;
use super::stage::NativeStage;
use crate::native_queue::NqRefusal;
use crate::shard::layout::queue_gauges;

impl CommitTransaction<'_> {
    /// Read the persisted gauges if this engine has not yet; settle the
    /// moves an earlier group staged: written if the shard log holds their
    /// generation, dropped if not (that group was rejected).
    pub(super) async fn nq_load_gauges(&self, local: &StreamOverlay) -> Result<(), NqRefusal> {
        let gauges = &self.engine.layout.queue_gauges;
        if gauges.loaded() {
            let Some(generation) = gauges.staged_generation() else {
                return Ok(());
            };
            if self.nq_group_staged(local) {
                return Ok(());
            }
            let held = self
                .engine
                .db
                .get(queue_gauges::generation_key())
                .await
                .map_err(|e| NqRefusal::Internal(e.to_string()))?;
            let held = held.as_deref().and_then(queue_gauges::read_generation);
            gauges.settle(held == Some(generation));
            return Ok(());
        }
        let (from, to) = queue_gauges::key_range();
        let mut it = self
            .engine
            .db
            .scan_with_options(from..to, &crate::native_queue::scan_options())
            .await
            .map_err(|e| NqRefusal::Internal(e.to_string()))?;
        let mut rows = Vec::new();
        while let Some(kv) = it
            .next()
            .await
            .map_err(|e| NqRefusal::Internal(e.to_string()))?
        {
            rows.push((kv.key.to_vec(), kv.value.to_vec()));
        }
        gauges.load(&rows);
        Ok(())
    }

    /// Move the gauges by what this operation changed in the queue's
    /// bytes per category (none for a queue that no longer exists), and
    /// put the rows that changed in the batch.
    pub(super) fn nq_flush_gauges(&mut self, st: &mut NativeStage) {
        let after = if st.exists {
            queue_gauges::categories(&st.state)
        } else {
            [0; queue_gauges::CATEGORIES]
        };
        if after == st.gauged {
            return;
        }
        let rows = self.engine.layout.queue_gauges.apply(
            &self.engine.prefix,
            st.state.container,
            (st.gauged, after),
        );
        if rows.is_empty() {
            return;
        }
        for (key, value) in rows {
            self.batch.put(&key, &value);
        }
        self.extra_writes = true;
        st.gauged = after;
        st.gauges_staged = true;
    }

    /// Whether this group staged gauge moves: one of its queues' stages
    /// did (each queue's stage lives for its group only).
    fn nq_group_staged(&self, local: &StreamOverlay) -> bool {
        let staged = |o: &StreamOverlay| o.queue.native.as_ref().is_some_and(|s| s.gauges_staged);
        staged(local) || self.streams.values().any(staged)
    }

    /// Whether the queue is over its share at the line of `cat`.
    pub(super) fn nq_limited(&self, st: &NativeStage, cat: usize) -> bool {
        let bytes = queue_gauges::categories(&st.state)[cat];
        self.engine
            .layout
            .queue_gauges
            .limited(st.state.container, cat, bytes)
    }
}
