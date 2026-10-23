//! Claim (docs/QUEUES-DESIGN.md §6.1): lease the due deliveries in
//! availability order from the merge of the availability index past the
//! cursor and the retry index, within `maxInFlight` and the byte limit. An
//! untaken message past its retention end is skipped into the retention
//! gap and deleted (§8.1). An empty claim writes nothing.
use super::deliveries::view;
use super::stage::{NativeStage, family_range, time_range_from};
use super::*;
use crate::native_queue::DeliveryView;
use crate::native_queue::rows::{Cause, DState, DeliveryRow};

/// A due candidate: (due time, offset, from the retry index).
type Candidate = (i64, u64, bool);

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
        for (due, offset, from_retry) in candidates {
            if out.len() >= budget.max {
                break;
            }
            let leased = if from_retry {
                self.nq_claim_retry(&hash, st, offset, &mut budget).await?
            } else {
                self.nq_claim_new(&hash, st, (due, offset), &mut budget)
                    .await?
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
            st.state.notify_backoff_ms = 0;
            Self::nq_tick(st, t);
        }
        Ok(NqOut::Claimed {
            deliveries: out,
            paused: false,
        })
    }

    /// Due entries of both sources, merged in (time, offset) order.
    async fn nq_candidates(
        &self,
        hash: &[u8; 16],
        st: &NativeStage,
        max: usize,
        t: i64,
    ) -> Result<Vec<Candidate>, NqRefusal> {
        let want = max.saturating_mul(2).saturating_add(16);
        let s = &st.state;
        let a_range = if s.cursor_started {
            time_range_from(
                hash,
                keys::AVAIL,
                s.cursor_at,
                s.cursor_off.saturating_add(1),
            )
        } else {
            family_range(hash, keys::AVAIL)
        };
        let a_prefix = keys::family(hash, keys::AVAIL).len();
        let r_prefix = keys::family(hash, keys::RETRIES).len();
        let mut out: Vec<Candidate> = Vec::new();
        for (k, _) in self
            .nq_scan(st, a_range, want)
            .await
            .map_err(NqRefusal::Internal)?
        {
            match keys::decode_time_key(a_prefix, &k) {
                Some((at, off)) if at <= t => out.push((at, off, false)),
                _ => break,
            }
        }
        let r_range = family_range(hash, keys::RETRIES);
        for (k, _) in self
            .nq_scan(st, r_range, want)
            .await
            .map_err(NqRefusal::Internal)?
        {
            match keys::decode_time_key(r_prefix, &k) {
                Some((at, off)) if at <= t => out.push((at, off, true)),
                _ => break,
            }
        }
        out.sort_unstable();
        Ok(out)
    }

    async fn nq_claim_new(
        &mut self,
        hash: &[u8; 16],
        st: &mut NativeStage,
        at: (i64, u64),
        budget: &mut ClaimBudget,
    ) -> Result<Option<DeliveryView>, NqRefusal> {
        let (due, offset) = at;
        let t = budget.t;
        let meta = self.nq_meta(hash, st, offset).await?;
        let fits = meta.as_ref().is_none_or(|m| {
            budget.bytes == 0 || budget.bytes + u64::from(m.bytes) <= budget.max_bytes
        });
        if !fits {
            // Not taken: the cursor stays before it.
            budget.max = 0;
            return Ok(None);
        }
        st.state.cursor_at = due;
        st.state.cursor_off = offset;
        st.state.cursor_started = true;
        st.state_dirty = true;
        let Some(mut meta) = meta else {
            return Ok(None);
        };
        if meta.cancelled {
            return Ok(None);
        }
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
        Ok(Some(view(offset, &meta, Some(&row))))
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
