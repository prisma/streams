//! The lane (S3b-DESIGN §4): an untaken immediate message has no row. The
//! frontier stands at the lowest immediate offset not yet processed for its
//! first delivery and walks the queue's pages from there; the range rows
//! hold the sparse unresolved state: open and scheduled references, the
//! scheduled and cancelled offsets the frontier passes. Every offset below
//! the frontier is resolved (no row) or referenced (an open delivery, an
//! unresolved scheduled message).
//!
//! The committer reads a page's clear header only: its offset, time and
//! length. The envelope is sealed under a key that only requests carry.
use super::super::CommitTransaction;
use super::stage::NativeStage;
use crate::native_queue::range::{RangeRow, index_of};
use crate::native_queue::{NqRefusal, keys};

/// A page at or past the frontier, as the committer reads it.
pub(super) struct LanePage {
    pub offset: u64,
    /// Its send time (the header's `ts_ms`): an immediate message's
    /// `availableAt`.
    pub ts: i64,
    pub raw: bytes::Bytes,
}

impl LanePage {
    /// The stored page's length, what the queued gauges count.
    pub(super) fn bytes(&self) -> u32 {
        u32::try_from(self.raw.len()).unwrap_or(u32::MAX)
    }
}

/// What the frontier does at a page it reaches (S3b-DESIGN §5.3).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum AtFrontier {
    /// A scheduled offset: pass it; its page goes too when the scheduled
    /// message is already resolved.
    Scheduled { resolved: bool },
    /// Cancelled untaken: pass it, its page and its bytes go.
    Cancelled,
    /// Untaken past its retention end: into the gap.
    Expired,
    /// Untaken and live: lease it, or stop.
    Live,
}

/// The frontier's step at one page, from its range row and the clock.
pub(super) fn at_frontier(
    range: &RangeRow,
    page: &LanePage,
    retention_ms: i64,
    t: i64,
) -> AtFrontier {
    if range.skip.contains(page.offset) {
        AtFrontier::Scheduled {
            resolved: !range.sbits.contains(page.offset),
        }
    } else if range.cancel.contains(page.offset) {
        AtFrontier::Cancelled
    } else if page.ts.saturating_add(retention_ms) <= t {
        AtFrontier::Expired
    } else {
        AtFrontier::Live
    }
}

impl CommitTransaction<'_> {
    /// The range row holding `offset` (empty when absent), through the
    /// stage's cache so a group's operations see each other's changes.
    pub(super) async fn nq_range(
        &self,
        hash: &[u8; 16],
        st: &mut NativeStage,
        offset: u64,
    ) -> Result<RangeRow, NqRefusal> {
        let index = index_of(offset);
        if let Some(row) = st.ranges.get(&index) {
            return Ok(row.clone());
        }
        let key = keys::range(hash, offset);
        let raw = self.nq_get(st, &key).await.map_err(NqRefusal::Internal)?;
        let row = match &raw {
            Some(raw) => RangeRow::decode(raw).map_err(NqRefusal::Internal)?,
            None => RangeRow::default(),
        };
        let stored = raw.map_or(0, |raw| super::stage::stored_bytes(&key, &raw));
        st.range_stored.insert(index, stored);
        st.ranges.insert(index, row.clone());
        Ok(row)
    }

    /// Stage the range row holding `offset`; written (or deleted, when it
    /// holds nothing) with the state row at the operation's end.
    pub(super) fn nq_set_range(st: &mut NativeStage, offset: u64, row: RangeRow) {
        let index = index_of(offset);
        st.ranges.insert(index, row);
        st.ranges_dirty.insert(index);
    }

    /// Up to `limit` pages from `from` on, in offset order, staged pages
    /// included. A page whose header does not name its own offset is
    /// refused: the committer never serves a page it cannot place.
    pub(super) async fn nq_lane_pages(
        &self,
        hash: &[u8; 16],
        st: &NativeStage,
        from: u64,
        limit: usize,
    ) -> Result<Vec<LanePage>, NqRefusal> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let start = keys::page(hash, from);
        let end = super::stage::family_range(hash, keys::PAGE).1;
        let prefix = keys::family(hash, keys::PAGE).len();
        let rows = self
            .nq_scan(st, (start, end), limit)
            .await
            .map_err(NqRefusal::Internal)?;
        let mut out = Vec::with_capacity(rows.len());
        for (k, v) in rows {
            let offset = keys::decode_offset_key(prefix, &k)
                .ok_or_else(|| NqRefusal::Internal("queue page key".into()))?;
            let raw = bytes::Bytes::from(v);
            let checked = crate::crypto_page::CheckedPage::admit(raw.clone(), offset)
                .map_err(|e| NqRefusal::Internal(format!("queue page {offset}: {e:?}")))?;
            out.push(LanePage {
                offset,
                ts: checked.ts_ms(),
                raw,
            });
        }
        Ok(out)
    }

    /// Move the frontier one offset on, past `page`, and set when the lane
    /// next expires: the following page's time when the caller has it,
    /// else this one's (a lower bound: send times never fall), or none
    /// when the lane is now empty.
    pub(super) fn nq_advance(
        st: &mut NativeStage,
        page: &LanePage,
        following: Option<i64>,
        next: u64,
    ) {
        st.state.frontier = page.offset + 1;
        st.state.lane_expire_at = if st.state.frontier >= next {
            0
        } else {
            following
                .unwrap_or(page.ts)
                .saturating_add(st.state.cfg.retention_ms)
        };
        st.state_dirty = true;
    }

    /// Delete a page the frontier has passed or whose message ended
    /// behind it, recording its bytes as debt.
    pub(super) fn nq_delete_page(
        &mut self,
        hash: &[u8; 16],
        st: &mut NativeStage,
        page: (u64, u32),
        t: i64,
    ) {
        let (offset, bytes) = page;
        self.nq_delete(st, keys::page(hash, offset));
        Self::nq_debt(st, u64::from(bytes), t);
    }
}
