//! Queue lifecycle and operator actions (docs/QUEUES-DESIGN.md §6.6-§6.7,
//! §7.2-§7.3, §8.2 "Queue delete").
use super::deliveries::view;
use super::stage::{NativeStage, family_range};
use super::*;
use crate::native_queue::rows::{DState, MetaRow, StateRow};
use crate::native_queue::{DeliveryView, GroupConfig, QueueConfig};

/// Every row family a queue owns, deleted family by family (§8.2).
const FAMILIES: [u8; 10] = [
    keys::META,
    keys::AVAIL,
    keys::DELIVERY,
    keys::LEASES,
    keys::RETRIES,
    keys::FAILED,
    keys::FAIL_EXPIRY,
    keys::DEDUP,
    keys::DEDUP_EXPIRY,
    keys::STATE,
];

impl CommitTransaction<'_> {
    pub(super) fn nq_create(
        &mut self,
        local: &mut StreamOverlay,
        hash: [u8; 16],
        st: &mut NativeStage,
        cfg: QueueConfig,
    ) -> NqResult {
        cfg.validate().map_err(NqRefusal::InvalidConfig)?;
        if st.exists {
            if st.state.deleting {
                return Err(NqRefusal::Deleting);
            }
            return if st.state.cfg.fixed_fields_match(&cfg) {
                Ok(NqOut::Config {
                    cfg: st.state.cfg.clone(),
                    created: false,
                })
            } else {
                Err(NqRefusal::ConfigConflict)
            };
        }
        let seed = u64::from_le_bytes(hash[..8].try_into().unwrap_or([7u8; 8]));
        st.state = StateRow {
            v: 2,
            cfg: cfg.clone(),
            jitter: seed | 1,
            ..StateRow::default()
        };
        st.exists = true;
        // A queue is absorbed in place from its first row (§5.6).
        local.fields.absorbed = local.fields.next;
        Self::nq_tick(st, now_ms());
        Ok(NqOut::Config { cfg, created: true })
    }

    pub(super) fn nq_update(
        &mut self,
        st: &mut NativeStage,
        group: GroupConfig,
        max_queued_bytes: u64,
        max_queued_messages: u64,
    ) -> NqResult {
        let mut cfg = st.state.cfg.clone();
        cfg.group = group;
        cfg.max_queued_bytes = max_queued_bytes;
        cfg.max_queued_messages = max_queued_messages;
        cfg.validate().map_err(NqRefusal::InvalidConfig)?;
        st.state.cfg = cfg.clone();
        st.state_dirty = true;
        Ok(NqOut::Config {
            cfg,
            created: false,
        })
    }

    pub(super) fn nq_begin_delete(st: &mut NativeStage, t: i64) -> NqOut {
        st.state.deleting = true;
        Self::nq_tick(st, t);
        NqOut::Deleting
    }

    /// One bounded step of a queue delete: pages first (through the
    /// metadata rows that name them), then every other family, the state
    /// row last. `remaining` is false once nothing is left.
    pub(super) async fn nq_delete_step(
        &mut self,
        local: &mut StreamOverlay,
        hash: [u8; 16],
        st: &mut NativeStage,
        max_rows: usize,
    ) -> NqResult {
        let t = now_ms().max(st.state.clock);
        let mut budget = max_rows.max(1);
        for fam in FAMILIES {
            let rows = self
                .nq_scan(st, family_range(&hash, fam), budget)
                .await
                .map_err(NqRefusal::Internal)?;
            let prefix = keys::family(&hash, fam).len();
            let n = rows.len();
            for row in rows {
                self.nq_delete_any(&hash, st, (fam, prefix, t), row);
            }
            budget -= n;
            if budget == 0 {
                return Ok(NqOut::DeleteStepped { remaining: true });
            }
        }
        st.exists = false;
        st.state_dirty = false;
        local.fields.absorbed = local.fields.next;
        self.batch.delete(keys::roster(&hash));
        Ok(NqOut::DeleteStepped { remaining: false })
    }

    /// Delete one row of a deleting queue; a metadata row takes its
    /// message's page with it and records the debt.
    fn nq_delete_any(
        &mut self,
        hash: &[u8; 16],
        st: &mut NativeStage,
        family: (u8, usize, i64),
        row: (Vec<u8>, Vec<u8>),
    ) {
        let (fam, prefix, t) = family;
        let (k, v) = row;
        if let Some(off) = (fam == keys::META)
            .then(|| keys::decode_offset_key(prefix, &k))
            .flatten()
        {
            self.batch
                .delete(crate::crypto_page::shard_page_key(hash, off));
            let bytes = MetaRow::decode(&v).map_or(0, |m| u64::from(m.bytes));
            Self::nq_debt(st, bytes, t);
        }
        self.nq_delete(st, k);
    }

    /// Cancel queue-wide (§6.7): a message nobody holds is deleted; a held
    /// one ends, its holder seeing `stale`.
    pub(super) async fn nq_cancel(
        &mut self,
        hash: [u8; 16],
        st: &mut NativeStage,
        offsets: Vec<u64>,
        t: i64,
    ) -> NqResult {
        let mut n = 0u64;
        for off in offsets {
            let Some(meta) = self.nq_meta(&hash, st, off).await? else {
                continue;
            };
            let row = self.nq_delivery(&hash, st, off).await?;
            let undone = match &row {
                Some(r) => {
                    self.nq_unindex(&hash, st, off, r);
                    r.state != DState::Failed
                }
                None => true,
            };
            self.nq_delete_message(&hash, st, (off, &meta), (undone, t));
            st.state.counters.cancelled += 1;
            n += 1;
        }
        Self::nq_tick(st, t);
        Ok(NqOut::Count(n))
    }

    /// Redrive failed deliveries into a new cycle (§7.3), each reserving
    /// its place in the admission budget first (§6.6).
    pub(super) async fn nq_redrive(
        &mut self,
        hash: [u8; 16],
        st: &mut NativeStage,
        args: (Vec<u64>, bool, usize),
        t: i64,
    ) -> NqResult {
        let (offsets, all, limit) = args;
        let offsets = if all {
            self.nq_failed_offsets(&hash, st, None, limit.max(1))
                .await?
        } else {
            offsets
        };
        let (mut redriven, mut capacity) = (0u64, false);
        for off in offsets {
            let (Some(meta), Some(mut row)) = (
                self.nq_meta(&hash, st, off).await?,
                self.nq_delivery(&hash, st, off).await?,
            ) else {
                continue;
            };
            if row.state != DState::Failed {
                continue;
            }
            let cfg = &st.state.cfg;
            if st.state.queued_messages + 1 > cfg.max_queued_messages
                || st.state.queued_bytes + u64::from(meta.bytes) > cfg.max_queued_bytes
            {
                capacity = true;
                break;
            }
            self.nq_unindex(&hash, st, off, &row);
            row.cycle += 1;
            row.cycle_deadline = meta.hard_expires_at;
            row.attempts = 0;
            row.releases = 0;
            row.cause = None;
            row.code = None;
            row.failure_expires_at = 0;
            self.nq_retry_at(&hash, st, (off, &mut row), t);
            st.state.queued_messages += 1;
            st.state.queued_bytes += u64::from(meta.bytes);
            st.state.counters.redriven += 1;
            redriven += 1;
        }
        Self::nq_tick(st, t);
        Ok(NqOut::Redriven {
            redriven,
            remaining: st.state.failed,
            capacity,
        })
    }

    pub(super) async fn nq_discard(
        &mut self,
        hash: [u8; 16],
        st: &mut NativeStage,
        offsets: Vec<u64>,
        t: i64,
    ) -> NqResult {
        let mut n = 0u64;
        for off in offsets {
            let (Some(meta), Some(row)) = (
                self.nq_meta(&hash, st, off).await?,
                self.nq_delivery(&hash, st, off).await?,
            ) else {
                continue;
            };
            if row.state != DState::Failed {
                continue;
            }
            self.nq_unindex(&hash, st, off, &row);
            self.nq_delete_message(&hash, st, (off, &meta), (false, t));
            st.state.counters.discarded += 1;
            n += 1;
        }
        Self::nq_tick(st, t);
        Ok(NqOut::Count(n))
    }

    async fn nq_failed_offsets(
        &self,
        hash: &[u8; 16],
        st: &NativeStage,
        after: Option<u64>,
        limit: usize,
    ) -> Result<Vec<u64>, NqRefusal> {
        let prefix = keys::family(hash, keys::FAILED).len();
        let rows = self
            .nq_scan(
                st,
                family_range(hash, keys::FAILED),
                limit.saturating_add(after.map_or(0, |_| 10_000)),
            )
            .await
            .map_err(NqRefusal::Internal)?;
        let mut out: Vec<u64> = rows
            .iter()
            .filter_map(|(k, _)| keys::decode_time_key(prefix, k).map(|x| x.1))
            .collect();
        if let Some(pos) = after.and_then(|a| out.iter().position(|o| *o == a)) {
            out.drain(..=pos);
        }
        out.truncate(limit);
        Ok(out)
    }

    pub(super) async fn nq_list_failed(
        &self,
        hash: [u8; 16],
        st: &NativeStage,
        after: Option<u64>,
        limit: usize,
    ) -> NqResult {
        let offsets = self.nq_failed_offsets(&hash, st, after, limit).await?;
        let mut items = Vec::with_capacity(offsets.len());
        for off in &offsets {
            if let (Some(meta), Some(row)) = (
                self.nq_meta(&hash, st, *off).await?,
                self.nq_delivery(&hash, st, *off).await?,
            ) {
                items.push(view(*off, &meta, Some(&row)));
            }
        }
        let next = (offsets.len() == limit)
            .then(|| offsets.last().copied())
            .flatten();
        Ok(NqOut::Deliveries { items, next })
    }

    /// Peek (§6.7): open deliveries and messages not yet taken, by offset.
    pub(super) async fn nq_peek(
        &self,
        hash: [u8; 16],
        st: &NativeStage,
        after: Option<u64>,
        limit: usize,
    ) -> NqResult {
        let from = keys::by_offset(&hash, keys::META, after.map_or(0, |a| a.saturating_add(1)));
        let to = family_range(&hash, keys::META).1;
        let prefix = keys::family(&hash, keys::META).len();
        let rows = self
            .nq_scan(st, (from, to), limit)
            .await
            .map_err(NqRefusal::Internal)?;
        let mut items: Vec<DeliveryView> = Vec::with_capacity(rows.len());
        for (k, v) in &rows {
            let Some(off) = keys::decode_offset_key(prefix, k) else {
                continue;
            };
            let meta = MetaRow::decode(v).map_err(NqRefusal::Internal)?;
            let row = self.nq_delivery(&hash, st, off).await?;
            items.push(view(off, &meta, row.as_ref()));
        }
        let next = (items.len() == limit)
            .then(|| items.last().map(|v| v.offset))
            .flatten();
        Ok(NqOut::Deliveries { items, next })
    }

    pub(super) async fn nq_message(
        &self,
        hash: [u8; 16],
        st: &NativeStage,
        offset: u64,
    ) -> NqResult {
        let Some(meta) = self.nq_meta(&hash, st, offset).await? else {
            return Err(NqRefusal::Reclaimed);
        };
        let row = self.nq_delivery(&hash, st, offset).await?;
        Ok(NqOut::Message(view(offset, &meta, row.as_ref())))
    }
}
