//! One atomic commit transaction and its provisional state.
//!
//! Required accounting reads finish before staging. Stream overlays and all
//! batch-dependent replies belong to this owner until the single batch write.
//! Applied publication follows successful local write; DurableEffects move to
//! the existing remote-WAL/dispatch barrier. Dropping or rejecting this owner
//! cannot publish provisional successes. Conservative generation fences may
//! ratchet before the write, as in the actor's existing fail-closed protocol.
use super::*;
mod append;
mod finalize;
mod maintenance;
mod overlay;
mod prepare;
mod publish;
mod queue;
use overlay::StreamOverlay;
#[derive(Default)]
struct GroupStats {
    records: u64,
    appended_bytes: u64,
}
pub(super) struct CommitTransaction<'a> {
    engine: &'a ShardEngine,
    cfg: &'a ShardConfig,
    batch: WriteBatch,
    effects: DurableEffects,
    streams: HashMap<[u8; 16], StreamOverlay>,
    billing_rows: HashMap<[u8; 16], Option<crate::billing::SegmentBillingMetaV1>>,
    stats: GroupStats,
    trim_budget: u64,
    extra_writes: bool,
    changed: bool,
    accounting_diverged: Option<String>,
    maintenance_added: u64,
    maintenance_retired: u64,
    started: std::time::Instant,
    queue_wait_us: u32,
    reqs: u32,
    #[cfg(test)]
    client_append_hashes: std::collections::HashSet<[u8; 16]>,
    #[cfg(test)]
    group_has_absorbed: bool,
}
impl<'a> CommitTransaction<'a> {
    #[expect(
        clippy::cast_possible_truncation,
        reason = "CommitTransaction::run; the queue wait is clamped to u32::MAX and the request count is bounded by the group the committer drained; checked conversions would only restate those bounds"
    )]
    pub(super) async fn run(engine: &'a ShardEngine, ops: Vec<CommitOp>, cfg: &'a ShardConfig) {
        let ops = Self::expand(engine, ops);
        if engine.is_closed() {
            for op in ops {
                Self::reject_op(op, AppendErr::Moved);
            }
            return;
        }
        let billing_rows = match Self::billing_rows(engine, &ops).await {
            Ok(rows) => rows,
            Err(error) => {
                tracing::error!(shard = %engine.prefix, "accounting group read failed: {error}");
                for op in ops {
                    Self::reject_op(op, AppendErr::Internal(error.clone()));
                }
                return;
            }
        };
        let started = std::time::Instant::now();
        let oldest = ops
            .iter()
            .filter_map(|op| match op {
                CommitOp::Append(req) => Some(req.enqueued_at),
                _ => None,
            })
            .min();
        let queue_wait_us = oldest
            .map(|at| started.duration_since(at).as_micros().min(u32::MAX as u128) as u32)
            .unwrap_or(0);
        let mut transaction = Self {
            engine,
            cfg,
            batch: WriteBatch::new(),
            effects: DurableEffects::default(),
            streams: HashMap::new(),
            billing_rows,
            stats: GroupStats::default(),
            trim_budget: cfg.trim_global_budget,
            extra_writes: false,
            changed: false,
            accounting_diverged: None,
            maintenance_added: 0,
            maintenance_retired: 0,
            started,
            queue_wait_us,
            reqs: ops.len() as u32,
            #[cfg(test)]
            client_append_hashes: Default::default(),
            #[cfg(test)]
            group_has_absorbed: false,
        };
        for op in ops {
            transaction.stage(op).await;
        }
        transaction.finish().await;
    }
    #[expect(
        clippy::let_underscore_must_use,
        reason = "CommitTransaction::reject_op; a reply is a oneshot whose send fails only when the requester already went away; a handled result would only restate that nobody waits"
    )]
    fn reject_op(op: CommitOp, error: AppendErr) {
        match op {
            CommitOp::Append(req) => {
                let _ = req.resp.send(Err(error));
            }
            CommitOp::Close(CloseReq { resp, .. })
            | CommitOp::SealFence(SealFenceReq { resp, .. }) => {
                let _ = resp.send(Err(error));
            }
            CommitOp::Queue { resp, .. } => {
                let message = match error {
                    AppendErr::Internal(message) => message,
                    AppendErr::Moved => "shard fenced/moved; retry".into(),
                    other => format!("{other:?}"),
                };
                let _ = resp.send(Err(message));
            }
            _ => {}
        }
    }
    #[expect(
        clippy::match_same_arms,
        reason = "CommitTransaction::stage; the hash arms stay separate so the mutation harness never selects the whole stage as one mutant, whose blank form hangs every waiting reply instead of failing a test; folding the arms would put the dispatch under a mutant the harness cannot bound"
    )]
    async fn stage(&mut self, op: CommitOp) {
        let hash = match &op {
            CommitOp::Append(r) => r.hash,
            CommitOp::Close(r) => r.hash,
            CommitOp::SealFence(r) => r.hash,
            CommitOp::Absorbed { hash, .. } => *hash,
            CommitOp::Queue { hash, .. } => *hash,
            CommitOp::TrimStep { hash } => *hash,
            CommitOp::UsageAck { hash, .. } => *hash,
            CommitOp::BillingClose { hash, .. } => *hash,
            CommitOp::BillingRetained { hash, .. } => *hash,
            CommitOp::AbsorbedBatch { .. } | CommitOp::TrimTick => return,
        };

        // Temporarily take one overlay to stage through the owner without
        // aliases to its map. It returns before the next ordered operation.
        let mut local = match self.streams.remove(&hash) {
            Some(local) => local,
            None => match self.engine.stream_handle(hash).await {
                Ok(handle) => StreamOverlay::new(handle, self.billing_rows.remove(&hash).flatten()),
                Err(error) => {
                    Self::reject_op(op, AppendErr::Internal(error.to_string()));
                    return;
                }
            },
        };
        match op {
            CommitOp::Append(req) => self.append(&mut local, hash, req).await,
            CommitOp::Close(req) => self.close(&mut local, hash, req),
            CommitOp::SealFence(req) => self.fence(&mut local, hash, req),
            CommitOp::Queue { op, resp, .. } => self.queue(&mut local, hash, op, resp).await,
            CommitOp::UsageAck {
                scope,
                month_final_keys,
                ..
            } => self.usage_ack(&mut local, hash, scope, month_final_keys),
            CommitOp::BillingClose { close_ms, .. } => {
                self.billing_close(&mut local, hash, close_ms)
            }
            CommitOp::BillingRetained { retained, .. } => {
                self.billing_retained(&mut local, hash, retained)
            }
            CommitOp::Absorbed {
                upto, bytes, v2, ..
            } => self.absorbed(&mut local, hash, upto, bytes, v2),
            CommitOp::TrimStep { .. } => self.trim(&mut local, hash),
            CommitOp::AbsorbedBatch { .. } | CommitOp::TrimTick => {
                unreachable!("expanded before staging")
            }
        }
        self.streams.insert(hash, local);
    }
    fn reject(self, message: &str) {
        self.effects
            .reject(&AppendErr::Internal(message.to_owned()));
    }
}
