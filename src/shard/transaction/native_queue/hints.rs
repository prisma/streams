//! The committer's trim hints (S3b-DESIGN §7.3, `native_queue::hints`):
//! a group that moves a queue's frontier past sealed offsets, resolves a
//! reference (an open delivery, an unresolved scheduled message) at a
//! sealed offset or seals a queue's pages puts hint rows naming those
//! offsets, under the engine's sequence. A queue's rows for one group keep
//! their keys while its operations add to them, so a group writes each
//! range once.
use super::super::CommitTransaction;
use super::stage::NativeStage;
use crate::native_queue::hints;

impl CommitTransaction<'_> {
    /// Seed the engine's hint sequence (`ShardEngine::seed_trim_seq`).
    pub(in crate::shard::transaction) async fn seed_trim_seq(&self) -> Result<(), String> {
        self.engine.seed_trim_seq().await
    }

    /// The next hint key's sequence (seeded by `seed_trim_seq`).
    fn next_trim_seq(&self) -> u64 {
        self.engine.next_trim_seq()
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
