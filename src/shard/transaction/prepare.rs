use super::*;
use futures_util::StreamExt;

/// At most this many cold streams of a group load at once.
const COLD_LOADS: usize = 16;
impl CommitTransaction<'_> {
    #[expect(
        clippy::excessive_nesting,
        reason = "CommitTransaction::expand; the expansion nests the trim cursor walk inside the trim-tick arm of the op match; flattening it would separate the walk from the tick that schedules it"
    )]
    #[expect(
        clippy::unwrap_used,
        reason = "CommitTransaction::expand; a poisoned trim debt or cursor may hold a half-recorded stream set; recovering it could trim a stream twice or never"
    )]
    pub(super) fn expand(engine: &ShardEngine, ops: Vec<CommitOp>) -> Vec<CommitOp> {
        const TRIM_STREAMS_PER_TICK: usize = 64;
        let mut expanded: Vec<CommitOp> = Vec::with_capacity(ops.len());
        for op in ops {
            match op {
                CommitOp::AbsorbedBatch { streams, v2 } => {
                    expanded.extend(streams.into_iter().map(|(hash, upto, bytes)| {
                        CommitOp::Absorbed {
                            hash,
                            upto,
                            bytes,
                            v2,
                        }
                    }));
                }
                CommitOp::TrimTick => {
                    use std::ops::Bound;
                    let debt = engine.trim_debt.lock().unwrap();
                    if debt.is_empty() {
                        continue;
                    }
                    let mut cur = engine.trim_cursor.lock().unwrap();
                    let picked: Vec<[u8; 16]> = debt
                        .range((Bound::Excluded(*cur), Bound::Unbounded))
                        .chain(debt.range((Bound::Unbounded, Bound::Included(*cur))))
                        .take(TRIM_STREAMS_PER_TICK)
                        .copied()
                        .collect();
                    if let Some(last) = picked.last() {
                        *cur = *last;
                    }
                    expanded.extend(picked.into_iter().map(|hash| CommitOp::TrimStep { hash }));
                }
                other => expanded.push(other),
            }
        }
        expanded
    }
    /// The group's billing rows, with every stream the group stages made
    /// resident first. A cold stream's handle (its tail) and billing row are
    /// store reads on the shard's one serial commit path, and after a
    /// restart, a placement or an eviction the first groups' streams are all
    /// cold, so the group issues those reads together, at most `COLD_LOADS`
    /// at once, instead of one after another as its ops stage. A handle that
    /// fails to load here loads again, and refuses its own op, when the op
    /// stages; a billing row that fails refuses the group.
    pub(super) async fn billing_rows(
        engine: &ShardEngine,
        ops: &[CommitOp],
    ) -> Result<HashMap<[u8; 16], Option<crate::billing::SegmentBillingMetaV1>>, String> {
        let mut streams: HashMap<[u8; 16], bool> = HashMap::new();
        for (hash, billed) in ops.iter().filter_map(Self::staged_stream) {
            *streams.entry(hash).or_default() |= billed;
        }
        let mut loads = futures_util::stream::iter(streams)
            .map(|(hash, billed)| Self::warm(engine, hash, billed))
            .buffer_unordered(COLD_LOADS);
        let mut billing_rows = HashMap::new();
        while let Some(load) = loads.next().await {
            if let Some((hash, row)) = load {
                billing_rows.insert(hash, row.map_err(|error| error.to_string())?);
            }
        }
        Ok(billing_rows)
    }

    /// The stream `op` stages, and whether the op bills it.
    fn staged_stream(op: &CommitOp) -> Option<([u8; 16], bool)> {
        match op {
            CommitOp::Append(r) => Some((r.hash, r.billing.is_some())),
            CommitOp::UsageAck { hash, .. }
            | CommitOp::BillingClose { hash, .. }
            | CommitOp::BillingRetained { hash, .. } => Some((*hash, true)),
            CommitOp::Close(r) => Some((r.hash, false)),
            CommitOp::SealFence(r) => Some((r.hash, false)),
            CommitOp::Absorbed { hash, .. }
            | CommitOp::Queue { hash, .. }
            | CommitOp::NativeQueue { hash, .. }
            | CommitOp::TrimStep { hash } => Some((*hash, false)),
            CommitOp::AbsorbedBatch { .. } | CommitOp::TrimTick => None,
        }
    }

    /// Make `hash`'s handle resident and, when the group bills it, read its
    /// billing row.
    async fn warm(
        engine: &ShardEngine,
        hash: [u8; 16],
        billed: bool,
    ) -> Option<(
        [u8; 16],
        anyhow::Result<Option<crate::billing::SegmentBillingMetaV1>>,
    )> {
        drop(engine.stream_handle(hash).await);
        if billed {
            Some((hash, engine.billing_row(hash).await))
        } else {
            None
        }
    }
}
