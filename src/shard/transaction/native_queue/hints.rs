//! The committer's trim hints (S3b-DESIGN §7.3, `native_queue::hints`):
//! a group that moves a queue's frontier past sealed offsets, resolves a
//! reference (an open delivery, an unresolved scheduled message) at a
//! sealed offset or seals a queue's pages puts hint rows naming those
//! offsets, under the engine's sequence. A queue's rows for one group keep
//! their keys while its operations add to them, so a group writes each
//! range once.
use std::sync::atomic::Ordering;

use super::super::CommitTransaction;
use super::stage::NativeStage;
use crate::native_queue::hints;

impl CommitTransaction<'_> {
    /// Seed the engine's hint sequence from the shard log, once: above the
    /// last hint key and the trim's cursor (`hints::seed`). Only the
    /// committer writes it.
    pub(in crate::shard::transaction) async fn seed_trim_seq(&self) -> Result<(), String> {
        let seq = &self.engine.layout.trim_seq;
        if seq.load(Ordering::Relaxed) != 0 {
            return Ok(());
        }
        let (from, to) = hints::key_range();
        let descending = slatedb::config::ScanOptions {
            order: slatedb::IterationOrder::Descending,
            ..crate::native_queue::scan_options()
        };
        let db = &self.engine.db;
        let mut last = db
            .scan_with_options(from..to, &descending)
            .await
            .map_err(|e| e.to_string())?;
        let last = last.next().await.map_err(|e| e.to_string())?;
        let cursor = db
            .get(hints::cursor_key())
            .await
            .map_err(|e| e.to_string())?;
        let seed = hints::seed(last.as_ref().map(|kv| &kv.key[..]), cursor.as_deref())?;
        seq.store(seed, Ordering::Relaxed);
        Ok(())
    }

    /// The next hint key's sequence (seeded by `seed_trim_seq`).
    fn next_trim_seq(&self) -> u64 {
        self.engine.layout.trim_seq.fetch_add(1, Ordering::Relaxed)
    }

    /// Put the queue's hint rows for this group so far: the references it
    /// resolved and the offsets its frontier passed, below the stream's
    /// sealed boundary `absorbed` (the committer holds the pages above it,
    /// and a seal names them when it takes them). A row this group no
    /// longer needs, its ranges having merged, is deleted.
    pub(super) fn nq_flush_hints(&mut self, hash: &[u8; 16], st: &mut NativeStage, absorbed: u64) {
        if !st.exists {
            return;
        }
        let mut named = st.hints.clone();
        named.add(st.loaded_frontier, st.state.frontier);
        let ranges = named.below(absorbed);
        let rows: Vec<&[(u64, u64)]> = ranges.chunks(hints::MAX_RANGES).collect();
        while st.hint_seqs.len() < rows.len() {
            let gseq = self.next_trim_seq();
            st.hint_seqs.push(gseq);
        }
        for (i, gseq) in st.hint_seqs.iter().enumerate() {
            let key = hints::key(*gseq, hash);
            match rows.get(i) {
                Some(row) => self.batch.put(key, hints::encode(row)),
                None => self.batch.delete(key),
            }
            self.extra_writes = true;
        }
    }

    /// A seal of a queue stream's `[from, upto)` names them for the trim.
    pub(in crate::shard::transaction) fn stage_seal_hint(
        &mut self,
        hash: &[u8; 16],
        sealed: (u64, u64),
    ) {
        let gseq = self.next_trim_seq();
        self.batch
            .put(hints::key(gseq, hash), hints::encode(&[sealed]));
        self.extra_writes = true;
    }
}
