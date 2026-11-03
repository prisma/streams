//! Queue lifecycle and operator actions (docs/QUEUES-DESIGN.md §6.6-§6.7,
//! §7.2-§7.3, §8.2 "Queue delete").
use super::deliveries::view;
use super::lane::LanePage;
use super::stage::{NativeStage, family_range};
use super::*;
use crate::native_queue::rows::{DState, MessageFacts, StateRow};
use crate::native_queue::{DeliveryView, GroupConfig, QueueConfig};

/// Every row family a queue owns, deleted family by family (§8.2), its
/// pages first and its state row last.
const FAMILIES: [u8; 12] = [
    keys::PAGE,
    keys::SCHEDULED_AT,
    keys::AVAIL,
    keys::DELIVERY,
    keys::LEASES,
    keys::RETRIES,
    keys::FAILED,
    keys::FAIL_EXPIRY,
    keys::RANGE,
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
        st.state = StateRow {
            cfg: cfg.clone(),
            jitter: keys::jitter_seed(&hash),
            frontier: local.fields.next,
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

    /// One bounded step of a queue delete: pages first, then every other
    /// family, the state row last. `remaining` is false once nothing is
    /// left.
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
            let n = rows.len();
            for row in rows {
                self.nq_delete_any(st, (fam, t), row);
            }
            budget -= n;
            if budget == 0 {
                return Ok(NqOut::DeleteStepped { remaining: true });
            }
        }
        st.exists = false;
        st.state_dirty = false;
        local.fields.absorbed = local.fields.next;
        if let Some(at) = st.state.roster_at {
            self.batch.delete(keys::roster(at, &hash));
        }
        Ok(NqOut::DeleteStepped { remaining: false })
    }

    /// Delete one row of a deleting queue; a page records its bytes as
    /// debt.
    fn nq_delete_any(
        &mut self,
        st: &mut NativeStage,
        family: (u8, i64),
        row: (Vec<u8>, Vec<u8>),
    ) {
        let (fam, t) = family;
        let (k, v) = row;
        if fam == keys::PAGE {
            Self::nq_debt(st, v.len() as u64, t);
        }
        self.nq_delete(st, k);
    }

    /// Cancel queue-wide (§6.7; S3b-DESIGN §5.6): a held message ends, its
    /// holder seeing `stale`; an unresolved scheduled one ends; an untaken
    /// immediate one is marked for the frontier, which passes it and takes
    /// its bytes off the queued gauge then (a cancel reads no page).
    pub(super) async fn nq_cancel(
        &mut self,
        hash: [u8; 16],
        st: &mut NativeStage,
        offsets: Vec<u64>,
        clock: (i64, u64),
    ) -> NqResult {
        let (t, next) = clock;
        let mut n = 0u64;
        for off in offsets {
            if self.nq_cancel_one(&hash, st, off, (t, next)).await? {
                st.state.counters.cancelled += 1;
                n += 1;
            }
        }
        Self::nq_tick(st, t);
        Ok(NqOut::Count(n))
    }

    async fn nq_cancel_one(
        &mut self,
        hash: &[u8; 16],
        st: &mut NativeStage,
        off: u64,
        clock: (i64, u64),
    ) -> Result<bool, NqRefusal> {
        let (t, next) = clock;
        if let Some(row) = self.nq_delivery(hash, st, off).await? {
            self.nq_unindex(hash, st, off, &row);
            let undone = row.state != DState::Failed;
            self.nq_end_message(hash, st, (off, &row), (undone, t))
                .await?;
            return Ok(true);
        }
        if let Some(sched) = self.nq_scheduled(hash, st, off).await? {
            self.nq_end_scheduled(hash, st, (off, &sched.facts), t)
                .await?;
            return Ok(true);
        }
        if off < st.state.frontier || off >= next {
            return Ok(false);
        }
        let mut range = self.nq_range(hash, st, off).await?;
        if range.skip.contains(off) || range.cancel.contains(off) {
            return Ok(false);
        }
        range.cancel.set(off);
        Self::nq_set_range(st, off, range);
        st.state.queued_messages = st.state.queued_messages.saturating_sub(1);
        Ok(true)
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
            let Some(mut row) = self.nq_delivery(&hash, st, off).await? else {
                continue;
            };
            if row.state != DState::Failed {
                continue;
            }
            let cfg = st.state.cfg.clone();
            if st.state.queued_messages + 1 > cfg.max_queued_messages
                || st.state.queued_bytes + u64::from(row.bytes) > cfg.max_queued_bytes
            {
                capacity = true;
                break;
            }
            self.nq_unindex(&hash, st, off, &row);
            row.cycle += 1;
            row.cycle_deadline = row.facts().hard_expires_at(&cfg);
            row.attempts = 0;
            row.releases = 0;
            row.cause = None;
            row.code = None;
            row.failure_expires_at = 0;
            self.nq_retry_at(&hash, st, (off, &mut row), t);
            st.state.queued_messages += 1;
            st.state.queued_bytes += u64::from(row.bytes);
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
            let Some(row) = self.nq_delivery(&hash, st, off).await? else {
                continue;
            };
            if row.state != DState::Failed {
                continue;
            }
            self.nq_unindex(&hash, st, off, &row);
            self.nq_end_message(&hash, st, (off, &row), (false, t))
                .await?;
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
            if let Some(row) = self.nq_delivery(&hash, st, *off).await? {
                items.push(view(*off, &row.facts(), Some(&row)));
            }
        }
        let next = (offsets.len() == limit)
            .then(|| offsets.last().copied())
            .flatten();
        Ok(NqOut::Deliveries { items, next })
    }

    /// Peek (§6.7): open deliveries, unresolved scheduled messages and the
    /// lane's untaken messages, by offset.
    pub(super) async fn nq_peek(
        &self,
        hash: [u8; 16],
        st: &mut NativeStage,
        page: (Option<u64>, usize),
        next: u64,
    ) -> NqResult {
        let (after, limit) = page;
        let from = after.map_or(0, |a| a.saturating_add(1));
        let mut items: std::collections::BTreeMap<u64, DeliveryView> =
            std::collections::BTreeMap::new();
        for (off, row) in self.nq_rows_from(&hash, st, (keys::DELIVERY, from), limit).await? {
            let row = crate::native_queue::rows::DeliveryRow::decode(&row)
                .map_err(NqRefusal::Internal)?;
            items.insert(off, view(off, &row.facts(), Some(&row)));
        }
        for (off, _) in self
            .nq_rows_from(&hash, st, (keys::SCHEDULED_AT, from), limit)
            .await?
        {
            if let (std::collections::btree_map::Entry::Vacant(slot), Some(sched)) =
                (items.entry(off), self.nq_scheduled(&hash, st, off).await?)
            {
                slot.insert(view(off, &sched.facts, None));
            }
        }
        let lane_from = from.max(st.state.frontier);
        if lane_from < next {
            for v in self.nq_lane_views(&hash, st, lane_from, limit).await? {
                items.entry(v.offset).or_insert(v);
            }
        }
        let items: Vec<DeliveryView> = items.into_values().take(limit).collect();
        let next = (items.len() == limit)
            .then(|| items.last().map(|v| v.offset))
            .flatten();
        Ok(NqOut::Deliveries { items, next })
    }

    /// Up to `limit` rows of a by-offset family from an offset on.
    async fn nq_rows_from(
        &self,
        hash: &[u8; 16],
        st: &NativeStage,
        at: (u8, u64),
        limit: usize,
    ) -> Result<Vec<(u64, Vec<u8>)>, NqRefusal> {
        let (fam, from) = at;
        let start = keys::by_offset(hash, fam, from);
        let end = family_range(hash, fam).1;
        let prefix = keys::family(hash, fam).len();
        let rows = self
            .nq_scan(st, (start, end), limit)
            .await
            .map_err(NqRefusal::Internal)?;
        Ok(rows
            .into_iter()
            .filter_map(|(k, v)| keys::decode_offset_key(prefix, &k).map(|off| (off, v)))
            .collect())
    }

    /// The lane's untaken messages from `from` on, at most `limit` pages
    /// read, as pending views.
    async fn nq_lane_views(
        &self,
        hash: &[u8; 16],
        st: &mut NativeStage,
        from: u64,
        limit: usize,
    ) -> Result<Vec<DeliveryView>, NqRefusal> {
        let mut out = Vec::new();
        for p in self.nq_lane_pages(hash, st, from, limit).await? {
            out.extend(self.nq_lane_view(hash, st, &p).await?);
        }
        Ok(out)
    }

    /// An untaken lane message as a pending view, or `None` for an offset
    /// the frontier will pass (scheduled, or cancelled).
    async fn nq_lane_view(
        &self,
        hash: &[u8; 16],
        st: &mut NativeStage,
        page: &LanePage,
    ) -> Result<Option<DeliveryView>, NqRefusal> {
        let range = self.nq_range(hash, st, page.offset).await?;
        if range.skip.contains(page.offset) || range.cancel.contains(page.offset) {
            return Ok(None);
        }
        let facts = MessageFacts {
            enqueued_at: page.ts,
            available_at: page.ts,
            bytes: page.bytes(),
        };
        Ok(Some(view(page.offset, &facts, None)))
    }

    /// One message by offset: its open delivery, its scheduled entry, or
    /// its untaken page; `Reclaimed` when it is resolved.
    pub(super) async fn nq_message(
        &self,
        hash: [u8; 16],
        st: &mut NativeStage,
        offset: u64,
        next: u64,
    ) -> NqResult {
        if let Some(row) = self.nq_delivery(&hash, st, offset).await? {
            return Ok(NqOut::Message(view(offset, &row.facts(), Some(&row))));
        }
        if let Some(sched) = self.nq_scheduled(&hash, st, offset).await? {
            return Ok(NqOut::Message(view(offset, &sched.facts, None)));
        }
        if offset >= st.state.frontier && offset < next {
            let pages = self.nq_lane_pages(&hash, st, offset, 1).await?;
            if let Some(p) = pages.first().filter(|p| p.offset == offset)
                && let Some(v) = self.nq_lane_view(&hash, st, p).await?
            {
                return Ok(NqOut::Message(v));
            }
        }
        Err(NqRefusal::Reclaimed)
    }
}
