//! The engine's billing rows: the usage outbox it reads (the dirty index, its
//! month finals and the residency probe over both), the billing row it loads
//! for the committer and the sweep, and the awaited billing commands its
//! committer applies to that row.
#[cfg(test)]
use super::billing_read_faults;
use super::{CommitOp, ShardEngine, decode_cursor};

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
    /// AWAITED submission (round-22 item 7): the caller knows whether
    /// the closure entered the committer queue — a full queue is
    /// backpressure, never a silent drop; the registry-persisted debt
    /// plus the sweep reconciler retry anything that still fails.
    pub(crate) async fn submit_billing_close(
        &self,
        hash: [u8; 16],
        close_ms: i64,
    ) -> Result<(), String> {
        self.tx
            .send(CommitOp::BillingClose { hash, close_ms })
            .await
            .map_err(|_| "committer queue closed".to_string())
    }

    /// Durably persist the fork-retention flag on the billing row
    /// (round-22 item 7); awaited like the closure.
    pub(crate) async fn submit_billing_retained(
        &self,
        hash: [u8; 16],
        retained: bool,
    ) -> Result<(), String> {
        self.tx
            .send(CommitOp::BillingRetained { hash, retained })
            .await
            .map_err(|_| "committer queue closed".to_string())
    }
}
