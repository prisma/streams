use super::*;
use futures_util::StreamExt;

/// At most this many cold streams of a group load at once.
const COLD_LOADS: usize = 16;

/// What a group needs of one stream before it stages.
#[derive(Default)]
struct Cold {
    billed: bool,
    queue: bool,
    // mt-lint: allow(name-keyed-map): consumer names of the one tenant-qualified stream hash this entry is keyed by
    consumers: std::collections::BTreeSet<String>,
}

/// The consumer a queue op names.
fn queue_consumer(op: &crate::queue::QueueOp) -> &str {
    use crate::queue::QueueOp;
    match op {
        QueueOp::Receive { consumer, .. }
        | QueueOp::Settle { consumer, .. }
        | QueueOp::ConfigPut { consumer, .. }
        | QueueOp::ConfigGet { consumer }
        | QueueOp::ConfigLifecycle { consumer, .. }
        | QueueOp::ConfigDeleteStep { consumer, .. } => consumer,
    }
}
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
    /// resident first. A cold stream's handle (its tail), queue state,
    /// consumer records and billing row are store reads on the shard's one
    /// serial commit path, and after a restart, a placement or an eviction
    /// the first groups' streams are all cold, so the group issues those
    /// reads together, at most `COLD_LOADS` streams at once, instead of one
    /// after another as its ops stage. A handle, queue state or record that
    /// fails to load here loads again, and refuses its own op, when the op
    /// stages; a billing row that fails refuses the group.
    pub(super) async fn billing_rows(
        engine: &ShardEngine,
        ops: &[CommitOp],
    ) -> Result<HashMap<[u8; 16], Option<crate::billing::SegmentBillingMetaV1>>, String> {
        let mut streams: HashMap<[u8; 16], Cold> = HashMap::new();
        for op in ops {
            Self::needs(op, &mut streams);
        }
        let mut loads = futures_util::stream::iter(streams)
            .map(|(hash, cold)| Self::warm(engine, hash, cold))
            .buffer_unordered(COLD_LOADS);
        let mut billing_rows = HashMap::new();
        while let Some(load) = loads.next().await {
            if let Some((hash, row)) = load {
                billing_rows.insert(hash, row.map_err(|error| error.to_string())?);
            }
        }
        Ok(billing_rows)
    }

    /// What `op` needs of the stream it stages, added to `streams`.
    fn needs(op: &CommitOp, streams: &mut HashMap<[u8; 16], Cold>) {
        let (hash, billed, queue) = match op {
            CommitOp::Append(r) => (r.hash, r.billing.is_some(), None),
            CommitOp::UsageAck { hash, .. }
            | CommitOp::BillingClose { hash, .. }
            | CommitOp::BillingRetained { hash, .. } => (*hash, true, None),
            CommitOp::Queue { hash, op, .. } => (*hash, false, Some(queue_consumer(op))),
            CommitOp::Close(r) => (r.hash, false, None),
            CommitOp::SealFence(r) => (r.hash, false, None),
            CommitOp::Absorbed { hash, .. }
            | CommitOp::NativeQueue { hash, .. }
            | CommitOp::TrimStep { hash } => (*hash, false, None),
            CommitOp::AbsorbedBatch { .. } | CommitOp::TrimTick => return,
        };
        let cold = streams.entry(hash).or_default();
        cold.billed |= billed;
        if let Some(consumer) = queue {
            cold.queue = true;
            cold.consumers.insert(consumer.to_owned());
        }
    }

    /// Make `hash`'s handle resident, with its queue state and named
    /// consumers' records when the group has queue ops on it, and read its
    /// billing row when the group bills it.
    async fn warm(
        engine: &ShardEngine,
        hash: [u8; 16],
        cold: Cold,
    ) -> Option<(
        [u8; 16],
        anyhow::Result<Option<crate::billing::SegmentBillingMetaV1>>,
    )> {
        let handle = engine.stream_handle(hash).await;
        if cold.queue
            && let Ok(handle) = &handle
        {
            Self::warm_queue(engine, handle, &cold.consumers).await;
        }
        if cold.billed {
            Some((hash, engine.billing_row(hash).await))
        } else {
            None
        }
    }
}
