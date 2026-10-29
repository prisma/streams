//! The engine's billing rows: the usage outbox it reads (the dirty index, its
//! month finals and the residency probe over both), the billing row it loads
//! for the committer and the sweep, and the awaited billing commands its
//! committer applies to that row, each answered applied or refused
//! (`BillingReply`).
#[cfg(test)]
use super::billing_read_faults;
use super::layout::StaticBilling;
use super::{AppendAck, AppendErr, CommitOp, DurableEffects, ShardEngine, TailFields};
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::oneshot;

/// Billing ops (a close or a retention flag) the shard dropped without
/// applying them, each answered to its submitter as a retryable refusal: a
/// committer that retired before it applied the op, or a queue already
/// closed. Process-wide; tests read it, no debug surface reports it.
pub(crate) static BILLING_OPS_REFUSED: AtomicU64 = AtomicU64::new(0);

impl ShardEngine {
    /// The usage dirty set whole (§6.3, layout 6): every segment whose
    /// usage counters hold versions `_usage` has not acknowledged, as
    /// (hash, unacked version).
    #[cfg(test)]
    pub(crate) async fn usage_dirty_scan(&self) -> anyhow::Result<Vec<([u8; 16], u64)>> {
        self.seal_ready().await?;
        Ok(self.layout.usage_page(None, usize::MAX).0)
    }

    /// Presence probe for residency decisions: the usage dirty set, then
    /// at most one month-final row, including orphaned ones.
    pub(crate) async fn has_billing_debt(&self) -> anyhow::Result<bool> {
        self.seal_ready().await?;
        if self.layout.usage_dirty() {
            return Ok(true);
        }
        let mut prefix = crate::billing::USAGE_DIRTY_SENTINEL.to_vec();
        prefix.push(b'V');
        Ok(self
            .db
            .scan_prefix(prefix, ..)
            .await?
            .next()
            .await?
            .is_some())
    }

    /// One bounded page of the usage dirty set, with an exclusive identity
    /// continuation, and whether more follow.
    pub(crate) async fn usage_dirty_page(
        &self,
        after: Option<[u8; 16]>,
        limit: usize,
    ) -> anyhow::Result<(Vec<([u8; 16], u64)>, bool)> {
        anyhow::ensure!(limit > 0, "dirty page limit must be positive");
        self.seal_ready().await?;
        Ok(self.layout.usage_page(after, limit))
    }

    /// Bounded finals for a single dirty segment. More finals keep it in the
    /// usage dirty set even after this page's exact keys are acked.
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

    /// The billing meta the usage drain, the tombstone walk and the debt
    /// pass meter (layout 6, f3): the segment's **durable** static meta and
    /// usage counters, never a group's applied but not yet durable state (a
    /// write that fails retires the engine, and a version it named must not
    /// reach the ledger). A resident handle answers from its durable tail; a
    /// stream with no resident handle is read from the store at remote
    /// durability, without loading a handle. Missing means never billed;
    /// read errors and invalid rows remain errors.
    pub(crate) async fn load_billing_meta(
        &self,
        hash: [u8; 16],
    ) -> anyhow::Result<Option<crate::billing::SegmentBillingMetaV1>> {
        #[cfg(test)]
        super::test_support::held_billing_read(&self.prefix).await;
        #[cfg(test)]
        if billing_read_faults()
            .lock()
            .unwrap()
            .remove(&self.prefix)
            .is_some()
        {
            anyhow::bail!("injected billing metadata read failure");
        }
        let resident = self
            .streams
            .lock()
            .ok()
            .and_then(|map| map.get(&hash).cloned());
        if let Some(StaticBilling::Read(meta)) =
            resident.map(|handle| billing_view(&handle, Visibility::Durable))
        {
            return Ok(meta);
        }
        let durable = slatedb::config::ReadOptions {
            durability_filter: slatedb::config::DurabilityLevel::Remote,
            ..Default::default()
        };
        let Some(mut meta) = self.static_billing_row(hash, &durable).await? else {
            return Ok(None);
        };
        if let Some(raw) = self
            .db
            .get_with_options(super::tail_key(&hash), &durable)
            .await?
            && let Some(usage) = super::stored_tail(&raw)?.usage
        {
            usage.apply(&mut meta);
        }
        checked(meta).map(Some)
    }

    /// The billing meta a commit group stages on (layout 6, f3): the
    /// handle's **applied** static meta and counters. The static row is read
    /// from the store once per handle and kept on its tails.
    pub(super) async fn billing_for_commit(
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
        let handle = self.stream_handle(hash).await?;
        if let StaticBilling::Read(meta) = billing_view(&handle, Visibility::Applied) {
            return Ok(meta);
        }
        let stored = self
            .static_billing_row(hash, &slatedb::config::ReadOptions::default())
            .await?;
        cache_static_billing(&handle, &stored);
        billing_view(&handle, Visibility::Applied)
            .into_meta()
            .map(checked)
            .transpose()
    }

    /// The segment's static billing row, decoded with zero counters.
    async fn static_billing_row(
        &self,
        hash: [u8; 16],
        options: &slatedb::config::ReadOptions,
    ) -> anyhow::Result<Option<crate::billing::SegmentBillingMetaV1>> {
        #[cfg(test)]
        super::test_support::held_billing_read(&self.prefix).await;
        #[cfg(test)]
        super::test_support::count_static_row_read(std::sync::Arc::as_ptr(&self.db));
        let Some(raw) = self
            .db
            .get_with_options(crate::billing::billing_meta_key(&hash), options)
            .await?
        else {
            return Ok(None);
        };
        let meta = crate::billing::stored_meta::decode_static(&raw)
            .ok_or_else(|| anyhow::anyhow!("invalid billing metadata"))?;
        anyhow::ensure!(
            meta.v == 1 && !meta.stream_id.is_empty(),
            "invalid billing metadata identity/version"
        );
        Ok(Some(meta))
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
        let mut pfx = Vec::with_capacity(18);
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
            #[cfg(test)]
            super::test_support::count_billing_op_refused(self.hash);
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

/// Which of a handle's tails a billing view reads.
#[derive(Clone, Copy)]
enum Visibility {
    Applied,
    Durable,
}

/// The billing meta `handle`'s applied or durable tail holds: its static
/// meta with that tail's counters; `Unread` when the static row was never
/// read or written on this handle.
#[expect(
    clippy::unwrap_used,
    reason = "billing_view; a poisoned stream state may hold half-applied fields; recovering it could bill counters the write never covered, whereas panicking fails the group or the drain that read it, and the next read reloads the stream's billing from the store"
)]
fn billing_view(handle: &super::StreamHandle, visibility: Visibility) -> StaticBilling {
    let st = handle.state.lock().unwrap();
    let tail = match visibility {
        Visibility::Applied => &st.applied,
        Visibility::Durable => &st.durable,
    };
    match &tail.billing {
        StaticBilling::Unread => StaticBilling::Unread,
        StaticBilling::Read(meta) => StaticBilling::Read(meta.clone().map(|mut meta| {
            if let Some(usage) = &tail.usage {
                usage.apply(&mut meta);
            }
            meta
        })),
    }
}

/// Keep a static row read from the store on both of `handle`'s tails,
/// unless a group published one meanwhile (which is newer).
#[expect(
    clippy::unwrap_used,
    reason = "cache_static_billing; a poisoned stream state may hold half-applied fields; recovering it could bill counters the write never covered, whereas panicking fails the group or the drain that read it, and the next read reloads the stream's billing from the store"
)]
fn cache_static_billing(
    handle: &super::StreamHandle,
    stored: &Option<crate::billing::SegmentBillingMetaV1>,
) {
    let mut st = handle.state.lock().unwrap();
    let st = &mut *st;
    for tail in [&mut st.applied, &mut st.durable] {
        if matches!(tail.billing, StaticBilling::Unread) {
            tail.billing = StaticBilling::Read(stored.clone());
        }
    }
}

/// A meta whose storage clock has started names a month.
fn checked(
    meta: crate::billing::SegmentBillingMetaV1,
) -> anyhow::Result<crate::billing::SegmentBillingMetaV1> {
    anyhow::ensure!(
        meta.storage_accounted_through_ms == 0 || (1..=12).contains(&meta.month_month),
        "invalid billing month"
    );
    Ok(meta)
}
