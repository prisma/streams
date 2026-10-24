//! Claim (docs/QUEUES-DESIGN.md §6.1): lease the due deliveries in
//! availability order from the merge of the availability index past the
//! cursor and the retry index, within `maxInFlight` and the byte limit. An
//! untaken message past its retention end is skipped into the retention
//! gap and deleted (§8.1). An empty claim writes nothing.
//!
//! A family whose floor is past the claim's clock has nothing due
//! (invariant F) and is not read. A read stops at the first entry past the
//! clock, and the claim then raises each family's floor to the first entry
//! it left: the first due one it did not consume, else that later entry.
use super::deliveries::view;
use super::stage::NativeStage;
use super::*;
use crate::native_queue::DeliveryView;
use crate::native_queue::rows::{Cause, DState, DeliveryRow, decode_avail};

/// A due candidate: (due time, offset, its availability entry's value, or
/// `None` from the retry index).
type Candidate = (i64, u64, Option<Vec<u8>>);

/// The families a claim reads, in the order of [`family_of`].
const FAMILIES: [u8; 2] = [keys::AVAIL, keys::RETRIES];

/// The index into [`FAMILIES`] of a candidate's family.
fn family_of(c: &Candidate) -> usize {
    usize::from(c.2.is_none())
}

/// Whether the claim consumed a candidate, so that no later read of its
/// family finds it: the cursor passed an availability entry, or a retry
/// entry was deleted.
fn consumed(hash: &[u8; 16], st: &NativeStage, c: &Candidate) -> bool {
    let (at, off, entry) = c;
    if entry.is_some() {
        let s = &st.state;
        return s.cursor_started && (*at, *off) <= (s.cursor_at, s.cursor_off);
    }
    st.rows.get(&keys::by_time(hash, keys::RETRIES, *at, *off)) == Some(&None)
}

/// Raise each family's floor to the first entry the claim left (invariant
/// F holds: every live entry of the family is at or past it): its first
/// due candidate not consumed, else `next`, the first entry its read found
/// past the clock (`None`: unknown, no raise).
fn raise_floors(
    hash: &[u8; 16],
    st: &mut NativeStage,
    candidates: &[Candidate],
    next: [Option<i64>; 2],
) {
    let mut left = [None; 2];
    for c in candidates {
        if !consumed(hash, st, c) {
            left[family_of(c)].get_or_insert(c.0);
        }
    }
    for (fam, (left, next)) in FAMILIES.into_iter().zip(left.into_iter().zip(next)) {
        if let Some(at) = left.or(next) {
            st.raise_floor(fam, Some(at));
        }
    }
}

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
        let (candidates, next) = self.nq_candidates(&hash, st, max, t).await?;
        let mut budget = ClaimBudget {
            max,
            max_bytes,
            bytes: 0,
            lease_until: t + lease_ms,
            t,
        };
        let mut out: Vec<DeliveryView> = Vec::new();
        for (due, offset, entry) in &candidates {
            if out.len() >= budget.max {
                break;
            }
            let leased = match entry {
                None => self.nq_claim_retry(&hash, st, *offset, &mut budget).await?,
                Some(entry) => self.nq_claim_new(&hash, st, (*due, *offset, entry), &mut budget)?,
            };
            if let Some(v) = leased {
                out.push(v);
            }
            if budget.bytes >= budget.max_bytes {
                break;
            }
        }
        raise_floors(&hash, st, &candidates, next);
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
    /// read from its floor (the availability index from the cursor), with
    /// each family's first entry past `t` as [`Self::nq_due_scan`] found it
    /// (`None` for a family not read).
    async fn nq_candidates(
        &self,
        hash: &[u8; 16],
        st: &mut NativeStage,
        max: usize,
        t: i64,
    ) -> Result<(Vec<Candidate>, [Option<i64>; 2]), NqRefusal> {
        let want = max.saturating_mul(2).saturating_add(16);
        let mut out: Vec<Candidate> = Vec::new();
        let mut next = [None; 2];
        for (fam, found) in FAMILIES.into_iter().zip(&mut next) {
            if st.state.floors.of(fam).is_some_and(|floor| *floor > t) {
                continue;
            }
            let prefix = keys::family(hash, fam).len();
            let (rows, after) = self
                .nq_due_scan(hash, st, fam, (want, t))
                .await
                .map_err(NqRefusal::Internal)?;
            *found = after;
            let due = rows.into_iter().map_while(|(k, v)| {
                let (at, off) = keys::decode_time_key(prefix, &k)?;
                (at <= t).then(|| (at, off, (fam == keys::AVAIL).then_some(v)))
            });
            out.extend(due);
        }
        out.sort_unstable_by_key(|c| (c.0, c.1));
        Ok((out, next))
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
