//! Claim (docs/QUEUES-DESIGN.md §6.1): lease the due deliveries in
//! availability order from the merge of the availability index past the
//! cursor and the retry index, within `maxInFlight` and the byte limit. An
//! untaken message past its retention end is skipped into the retention
//! gap and deleted (§8.1). An empty claim writes nothing.
use super::deliveries::view;
use super::stage::NativeStage;
use super::*;
use crate::native_queue::DeliveryView;
use crate::native_queue::rows::{Cause, DState, DeliveryRow, decode_avail};

/// A due candidate: (due time, offset, its availability entry's value, or
/// `None` from the retry index).
type Candidate = (i64, u64, Option<Vec<u8>>);

struct ClaimBudget {
    max: usize,
    max_bytes: u64,
    bytes: u64,
    lease_until: i64,
    /// The queue clock of this claim.
    t: i64,
}

impl CommitTransaction<'_> {
    pub(super) async fn nq_claim(
        &mut self,
        hash: [u8; 16],
        st: &mut NativeStage,
        args: (usize, u64, i64),
        t: i64,
    ) -> NqResult {
        let (max, max_bytes, lease_ms) = args;
        if st.state.paused {
            return Ok(NqOut::Claimed {
                deliveries: Vec::new(),
                paused: true,
            });
        }
        let room = usize::try_from(
            u64::from(st.state.cfg.group.max_in_flight).saturating_sub(st.state.leased),
        )
        .unwrap_or(usize::MAX);
        let max = max
            .min(room)
            .min(usize::try_from(st.state.cfg.group.max_batch).unwrap_or(usize::MAX));
        if max == 0 {
            return Ok(NqOut::Claimed {
                deliveries: Vec::new(),
                paused: false,
            });
        }
        let candidates = self.nq_candidates(&hash, st, max, t).await?;
        let mut budget = ClaimBudget {
            max,
            max_bytes,
            bytes: 0,
            lease_until: t + lease_ms,
            t,
        };
        let mut out: Vec<DeliveryView> = Vec::new();
        for (due, offset, entry) in candidates {
            if out.len() >= budget.max {
                break;
            }
            let leased = match entry {
                None => self.nq_claim_retry(&hash, st, offset, &mut budget).await?,
                Some(entry) => self.nq_claim_new(&hash, st, (due, offset, &entry), &mut budget)?,
            };
            if let Some(v) = leased {
                out.push(v);
            }
            if budget.bytes >= budget.max_bytes {
                break;
            }
        }
        if !out.is_empty() {
            st.state.last_useful_claim_at = t;
            st.state.notify_backoff_ms = 1_000;
        }
        // A claim that moved the cursor past expired or cancelled entries
        // changed state even if it leased nothing; it advances the clock
        // so no later send can sort below the cursor (§5.5).
        if st.state_dirty || !out.is_empty() {
            Self::nq_tick(st, t);
        }
        Ok(NqOut::Claimed {
            deliveries: out,
            paused: false,
        })
    }

    /// Due entries of both sources, merged in (time, offset) order, each
    /// read from its floor (the availability index from the cursor).
    async fn nq_candidates(
        &self,
        hash: &[u8; 16],
        st: &mut NativeStage,
        max: usize,
        t: i64,
    ) -> Result<Vec<Candidate>, NqRefusal> {
        let want = max.saturating_mul(2).saturating_add(16);
        let mut out: Vec<Candidate> = Vec::new();
        for fam in [keys::AVAIL, keys::RETRIES] {
            let prefix = keys::family(hash, fam).len();
            let rows = self
                .nq_index_scan(hash, st, fam, want)
                .await
                .map_err(NqRefusal::Internal)?;
            let due = rows.into_iter().map_while(|(k, v)| {
                let (at, off) = keys::decode_time_key(prefix, &k)?;
                (at <= t).then(|| (at, off, (fam == keys::AVAIL).then_some(v)))
            });
            out.extend(due);
        }
        out.sort_unstable_by_key(|c| (c.0, c.1));
        Ok(out)
    }

    /// Lease an untaken message from its availability entry, which holds
    /// its metadata as sent (an untaken message's metadata row never
    /// changes, and a cancel deletes the entry) and maybe its page.
    fn nq_claim_new(
        &mut self,
        hash: &[u8; 16],
        st: &mut NativeStage,
        at: (i64, u64, &[u8]),
        budget: &mut ClaimBudget,
    ) -> Result<Option<DeliveryView>, NqRefusal> {
        let (due, offset, entry) = at;
        let t = budget.t;
        let (mut meta, page) = decode_avail(entry).map_err(NqRefusal::Internal)?;
        if budget.bytes > 0 && budget.bytes + u64::from(meta.bytes) > budget.max_bytes {
            // Not taken: the cursor stays before it.
            budget.max = 0;
            return Ok(None);
        }
        st.state.cursor_at = due;
        st.state.cursor_off = offset;
        st.state.cursor_started = true;
        st.state_dirty = true;
        if meta.retention_end <= t {
            st.state.gap_count += 1;
            st.state.counters.expired += 1;
            self.nq_delete_message(hash, st, (offset, &meta), (true, t));
            return Ok(None);
        }
        budget.bytes += u64::from(meta.bytes);
        meta.taken = true;
        meta.open = 1;
        self.nq_put_meta(hash, st, offset, &meta);
        let mut row = DeliveryRow {
            state: DState::Leased,
            cycle: 1,
            cycle_deadline: meta.retention_end,
            attempts: 0,
            releases: 0,
            lease_gen: 0,
            lease_deadline: 0,
            due_at: 0,
            first_delivered_at: t,
            failed_at: 0,
            failure_expires_at: 0,
            cause: None,
            code: None,
        };
        let until = budget.lease_until.min(meta.hard_expires_at);
        self.nq_lease(hash, st, (offset, &mut row), until);
        let mut leased = view(offset, &meta, Some(&row));
        leased.page = (!page.is_empty()).then(|| bytes::Bytes::copy_from_slice(page));
        Ok(Some(leased))
    }

    async fn nq_claim_retry(
        &mut self,
        hash: &[u8; 16],
        st: &mut NativeStage,
        offset: u64,
        budget: &mut ClaimBudget,
    ) -> Result<Option<DeliveryView>, NqRefusal> {
        let t = budget.t;
        let (Some(meta), Some(mut row)) = (
            self.nq_meta(hash, st, offset).await?,
            self.nq_delivery(hash, st, offset).await?,
        ) else {
            return Ok(None);
        };
        if row.state != DState::Retrying {
            return Ok(None);
        }
        if budget.bytes > 0 && budget.bytes + u64::from(meta.bytes) > budget.max_bytes {
            budget.max = 0;
            return Ok(None);
        }
        self.nq_unindex(hash, st, offset, &row);
        if t >= row.cycle_deadline {
            self.nq_fail(
                hash,
                st,
                (offset, &mut row, &meta),
                (Cause::Retention, None, t),
            );
            Self::nq_tick(st, t);
            return Ok(None);
        }
        budget.bytes += u64::from(meta.bytes);
        let until = budget.lease_until.min(meta.hard_expires_at);
        self.nq_lease(hash, st, (offset, &mut row), until);
        Ok(Some(view(offset, &meta, Some(&row))))
    }
}
