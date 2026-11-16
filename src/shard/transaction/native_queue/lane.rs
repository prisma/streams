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
use super::super::overlay::StreamOverlay;
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

    /// Up to `limit` of the queue stream's unsealed pages from `from` on,
    /// in offset order: the pages its handle holds, then the group's own
    /// (S3b-DESIGN §6.1). A sealed offset (below the boundary) is not among
    /// them. A page whose header does not name its own offset is refused:
    /// the committer never serves a page it cannot place.
    pub(super) fn nq_lane_pages(
        local: &StreamOverlay,
        from: u64,
        limit: usize,
    ) -> Result<Vec<LanePage>, NqRefusal> {
        let st = local
            .handle
            .state
            .lock()
            .map_err(|_| NqRefusal::Internal("a queue stream's state is poisoned".into()))?;
        let group = local.frames.pending.iter().filter(|p| p.1 >= from);
        let mut out = Vec::new();
        for (first, _, raw) in st.pending.from(from).chain(group).take(limit) {
            let checked = crate::crypto_page::CheckedPage::admit(raw.clone(), *first)
                .map_err(|e| NqRefusal::Internal(format!("queue page {first}: {e:?}")))?;
            out.push(LanePage {
                offset: *first,
                ts: checked.ts_ms(),
                raw: raw.clone(),
            });
        }
        Ok(out)
    }

    /// The lane's run from `from`: the pages prepared outside the
    /// committer below the stream's sealed boundary, then its unsealed ones
    /// (`nq_lane_pages`), at most `limit`, in offset order and without a gap
    /// (S3b-DESIGN §6.2, §6.3). The run ends at the first offset it holds no
    /// page for, so no walk steps over a message; a prepared page is served
    /// only at the offset its header names, and one behind `from` is dropped.
    pub(super) fn nq_lane_run(
        local: &StreamOverlay,
        prepared: &[(u64, bytes::Bytes)],
        from: u64,
        limit: usize,
    ) -> Result<Vec<LanePage>, NqRefusal> {
        let sealed = local.fields.absorbed;
        let mut out = Vec::new();
        let mut expect = from;
        for (offset, raw) in prepared.iter().filter(|(o, _)| (from..sealed).contains(o)) {
            if out.len() >= limit || *offset != expect {
                return Ok(out);
            }
            let checked = crate::crypto_page::CheckedPage::admit(raw.clone(), *offset)
                .map_err(|e| NqRefusal::Internal(format!("queue page {offset}: {e:?}")))?;
            out.push(LanePage {
                offset: *offset,
                ts: checked.ts_ms(),
                raw: raw.clone(),
            });
            expect += 1;
        }
        if expect < sealed || out.len() >= limit {
            return Ok(out);
        }
        for page in Self::nq_lane_pages(local, expect, limit - out.len())? {
            if page.offset != expect {
                break;
            }
            out.push(page);
            expect += 1;
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
}
