//! The engine's billing rows: the usage outbox it reads (the dirty index, its
//! month finals and the residency probe over both), the billing row it loads
//! for the committer and the sweep, and the awaited billing commands its
//! committer applies to that row, each answered applied or refused
//! (`BillingReply`).
#[cfg(test)]
use super::billing_read_faults;
use super::{
    AppendAck, AppendErr, CommitOp, DurableEffects, ShardEngine, TailFields, decode_cursor,
};
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::oneshot;

/// Billing ops (a close or a retention flag) the shard dropped without
/// applying them, each answered to its submitter as a retryable refusal: a
/// committer that retired before it applied the op, or a queue already
/// closed. Process-wide; tests read it, no debug surface reports it.
pub(crate) static BILLING_OPS_REFUSED: AtomicU64 = AtomicU64::new(0);

impl ShardEngine {
    /// Usage-dirty index scan (§6.3): every segment whose durable
    /// billing state has versions `_usage` has not acknowledged.
    /// (hash, unacked version). One prefix scan; the drainer's
    /// discovery path after restart or ownership move.
    #[cfg(test)]
    pub(crate) async fn usage_dirty_scan(&self) -> anyhow::Result<Vec<([u8; 16], u64)>> {
        let mut pfx = Vec::with_capacity(17);
        pfx.extend_from_slice(&crate::billing::USAGE_DIRTY_SENTINEL);
        pfx.push(b'U');
        let mut out = Vec::new();
        let mut iter = self.db.scan_prefix(&pfx[..], ..).await?;
        while let Some(kv) = iter.next().await? {
            if kv.key.len() != 33 || kv.value.len() < 8 {
                continue;
            }
            let mut h = [0u8; 16];
            h.copy_from_slice(&kv.key[17..33]);
            let v = u64::from_le_bytes(kv.value[..8].try_into().unwrap());
            out.push((h, v));
        }
        Ok(out)
    }

    /// Presence probe for residency decisions: at most one row from
    /// each outbox index, including orphaned final rows.
    pub(crate) async fn has_billing_debt(&self) -> anyhow::Result<bool> {
        for tag in *b"UV" {
            let mut prefix = crate::billing::USAGE_DIRTY_SENTINEL.to_vec();
            prefix.push(tag);
            if self
                .db
                .scan_prefix(prefix, ..)
                .await?
                .next()
                .await?
                .is_some()
            {
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// One bounded page of the dirty index, with an exclusive identity
    /// continuation. Only the caller's finite page is materialized.
    pub(crate) async fn usage_dirty_page(
        &self,
        after: Option<[u8; 16]>,
        limit: usize,
    ) -> anyhow::Result<(Vec<([u8; 16], u64)>, bool)> {
        use std::ops::Bound;
        anyhow::ensure!(limit > 0, "dirty page limit must be positive");
        let mut prefix = crate::billing::USAGE_DIRTY_SENTINEL.to_vec();
        prefix.push(b'U');
        let range = (
            after.map_or(Bound::Unbounded, |h| Bound::Excluded(h.to_vec())),
            Bound::<Vec<u8>>::Unbounded,
        );
        let mut scan = self.db.scan_prefix(&prefix, range).await?;
        let mut rows = Vec::new();
        while let Some(kv) = scan.next().await? {
            if rows.len() == limit {
                return Ok((rows, true));
            }
            let hash: [u8; 16] = kv
                .key
                .get(17..)
                .ok_or_else(|| anyhow::anyhow!("invalid usage dirty key"))?
                .try_into()?;
            rows.push((hash, decode_cursor(&kv.value)?));
        }
        Ok((rows, false))
    }

    /// Bounded finals for a single dirty segment. More finals keep its
    /// dirty marker alive even after this page's exact keys are acked.
    pub(crate) async fn usage_month_finals_page(
        &self,
        hash: [u8; 16],
        limit: usize,
    ) -> anyhow::Result<(Vec<(Vec<u8>, crate::billing::SegmentSnapshot)>, bool)> {
        anyhow::ensure!(limit > 0, "final page limit must be positive");
        let mut prefix = crate::billing::USAGE_DIRTY_SENTINEL.to_vec();
        prefix.push(b'V');
        prefix.extend_from_slice(&hash);
        let mut scan = self.db.scan_prefix(&prefix, ..).await?;
        let mut rows = Vec::new();
        while let Some(kv) = scan.next().await? {
            if rows.len() == limit {
                return Ok((rows, true));
            }
            rows.push((kv.key.to_vec(), serde_json::from_slice(&kv.value)?));
        }
        Ok((rows, false))
    }

    /// Missing means never billed. Read errors and invalid rows remain errors.
    pub(crate) async fn load_billing_meta(
        &self,
        hash: [u8; 16],
    ) -> anyhow::Result<Option<crate::billing::SegmentBillingMetaV1>> {
        #[cfg(test)]
        if billing_read_faults()
            .lock()
            .unwrap()
            .remove(&self.prefix)
            .is_some()
        {
            anyhow::bail!("injected billing metadata read failure");
        }
        self.db
            .get(crate::billing::billing_meta_key(&hash))
            .await?
            .map(|v| {
                let meta: crate::billing::SegmentBillingMetaV1 = serde_json::from_slice(&v)
                    .map_err(|e| anyhow::anyhow!("invalid billing metadata: {e}"))?;
                anyhow::ensure!(
                    meta.v == 1 && !meta.stream_id.is_empty(),
                    "invalid billing metadata identity/version"
                );
                anyhow::ensure!(
                    meta.month_storage_byte_ms.is_empty()
                        || meta.month_storage_byte_ms.parse::<u128>().is_ok(),
                    "invalid billing byte-time"
                );
                anyhow::ensure!(
                    meta.storage_accounted_through_ms == 0 || (1..=12).contains(&meta.month_month),
                    "invalid billing month"
                );
                Ok(meta)
            })
            .transpose()
    }

    /// Legacy test convenience; production must handle missing and failed reads.
    #[cfg(test)]
    pub(crate) async fn billing_meta(
        &self,
        hash: [u8; 16],
    ) -> Option<crate::billing::SegmentBillingMetaV1> {
        self.load_billing_meta(hash)
            .await
            .expect("valid billing metadata in fixture")
    }

    /// Closed-month final snapshots awaiting ledger acknowledgment
    /// (sentinel-'V' rows): (exact key, snapshot).
    #[cfg(test)]
    pub(crate) async fn usage_month_finals(
        &self,
    ) -> anyhow::Result<Vec<(Vec<u8>, crate::billing::SegmentSnapshot)>> {
        let mut pfx = Vec::with_capacity(17);
        pfx.extend_from_slice(&crate::billing::USAGE_DIRTY_SENTINEL);
        pfx.push(b'V');
        let mut out = Vec::new();
        let mut iter = self.db.scan_prefix(&pfx[..], ..).await?;
        while let Some(kv) = iter.next().await? {
            out.push((kv.key.to_vec(), serde_json::from_slice(&kv.value)?));
        }
        Ok(out)
    }

    /// Terminal storage closure for a hard-deleted or expired segment
    /// (§6.2), accounted to the persisted logical close instant.
    /// AWAITED (round-22 item 7): `Ok` once the committer applied the close
    /// and its group is durable. `Err` is a refusal that changed nothing:
    /// the queue was closed, or the engine retired before its committer
    /// applied the close (`BillingReply`). A full queue is backpressure,
    /// never a drop; the caller retries a refusal at the same persisted
    /// instant (the registry-persisted debt plus the sweep reconciler).
    pub(crate) async fn submit_billing_close(
        &self,
        hash: [u8; 16],
        close_ms: i64,
    ) -> Result<(), String> {
        let (resp, answer) = BillingReply::new(hash, "close");
        self.tx
            .send(CommitOp::BillingClose {
                hash,
                close_ms,
                resp,
            })
            .await
            .map_err(|_| "committer queue closed".to_string())?;
        answered(answer).await
    }

    /// Durably persist the fork-retention flag on the billing row
    /// (round-22 item 7); awaited and answered like the closure.
    pub(crate) async fn submit_billing_retained(
        &self,
        hash: [u8; 16],
        retained: bool,
    ) -> Result<(), String> {
        let (resp, answer) = BillingReply::new(hash, "retention flag");
        self.tx
            .send(CommitOp::BillingRetained {
                hash,
                retained,
                resp,
            })
            .await
            .map_err(|_| "committer queue closed".to_string())?;
        answered(answer).await
    }

    /// Ops waiting in the committer's queue; one it has taken is not.
    #[cfg(test)]
    pub(crate) fn queued_ops(&self) -> usize {
        self.tx.max_capacity() - self.tx.capacity()
    }
}

/// The answer a billing op (a close or a retention flag) owes its
/// submitter. The op's own group answers it once staged (`applied`): `Ok`
/// at the group's durable dispatch, or the group's refusal. An op dropped
/// before a group staged it (the drain of a retiring committer, `reject_op`,
/// a group refused on a closed engine, a queue already closed) answers as it
/// drops: a retryable refusal (`Moved`), a warning, and one count of
/// `BILLING_OPS_REFUSED`. Never silently.
pub(crate) struct BillingReply {
    hash: [u8; 16],
    op: &'static str,
    resp: Option<oneshot::Sender<Result<AppendAck, AppendErr>>>,
}

impl BillingReply {
    /// The reply `op` on segment `hash` owes, and the answer its submitter
    /// awaits.
    fn new(
        hash: [u8; 16],
        op: &'static str,
    ) -> (Self, oneshot::Receiver<Result<AppendAck, AppendErr>>) {
        let (resp, answer) = oneshot::channel();
        let reply = Self {
            hash,
            op,
            resp: Some(resp),
        };
        (reply, answer)
    }

    /// A reply nobody awaits and nothing counts, for a fixture that reads
    /// only what its group wrote.
    #[cfg(test)]
    pub(super) fn detached(hash: [u8; 16]) -> Self {
        Self {
            hash,
            op: "fixture",
            resp: None,
        }
    }

    /// The op is staged in its group: the group's replies answer it, `Ok`
    /// with the stream's tail once durable (as a seal fence is answered), or
    /// the group's refusal.
    pub(super) fn applied(mut self, effects: &mut DurableEffects, tail: &TailFields) {
        if let Some(resp) = self.resp.take() {
            let ack = AppendAck {
                last_offset: tail.next.wrapping_sub(1),
                next_offset: tail.next,
                closed: tail.closed,
                producer: None,
                duplicate: false,
            };
            effects.acks.push((resp, Ok(ack)));
        }
    }
}

impl Drop for BillingReply {
    fn drop(&mut self) {
        if let Some(resp) = self.resp.take() {
            BILLING_OPS_REFUSED.fetch_add(1, Ordering::Relaxed);
            let waiting = resp.send(Err(AppendErr::Moved)).is_ok();
            tracing::warn!(
                segment = %crate::crypto::hex(&self.hash[..4]),
                op = self.op,
                waiting,
                "the shard dropped a billing op without applying it: refused, for its submitter to retry"
            );
        }
    }
}

/// A billing op's answer as its submitter sees it: `Ok` once applied and
/// durable, otherwise a refusal to retry.
async fn answered(answer: oneshot::Receiver<Result<AppendAck, AppendErr>>) -> Result<(), String> {
    match answer.await {
        Ok(Ok(_)) => Ok(()),
        Ok(Err(refusal)) => Err(format!("refused unapplied ({refusal:?}); retry")),
        Err(_) => Err("dropped unanswered; retry".to_string()),
    }
}
