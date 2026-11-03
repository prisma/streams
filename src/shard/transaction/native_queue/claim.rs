//! Claim (docs/QUEUES-DESIGN.md §6.1; S3b-DESIGN §5.3): lease the due work
//! of three sources in (due, offset) order, within `maxInFlight` and the
//! byte limit: due retries (`R`), due scheduled messages (`A_s`, from the
//! scheduled cursor) and the lane, the immediate messages from the
//! frontier, each due at its send. The frontier passes the scheduled and
//! cancelled offsets it meets, expires an untaken message past its
//! retention end into the gap (§8.1), and never passes a live untaken
//! message without leasing it. Every claim of a group takes the lane from
//! the frontier the group's earlier claims left. An empty claim writes
//! nothing.
//!
//! An index whose floor is past the claim's clock has nothing due
//! (invariant F) and is not read. A read stops at the first entry past the
//! clock, and the claim then raises each index's floor to the first entry
//! it left: the first due one it did not consume, else that later entry.
use super::deliveries::{first_delivery, view};
use super::lane::{AtFrontier, LanePage, at_frontier};
use super::stage::NativeStage;
use super::*;
use crate::native_queue::DeliveryView;
use crate::shard::layout::queue_gauges;
use crate::native_queue::rows::{Cause, DState, decode_scheduled};

/// A due candidate.
enum Candidate {
    /// An entry of the scheduled index: its time, offset and value.
    Scheduled(i64, u64, Vec<u8>),
    /// An entry of the retry index.
    Retry(i64, u64),
    /// A page at or past the frontier.
    Lane(LanePage),
}

impl Candidate {
    fn key(&self) -> (i64, u64) {
        match self {
            Candidate::Scheduled(at, off, _) | Candidate::Retry(at, off) => (*at, *off),
            Candidate::Lane(p) => (p.ts, p.offset),
        }
    }
}

/// The indexes a claim reads, in the order of [`family_of`].
pub(super) const FAMILIES: [u8; 2] = [keys::AVAIL, keys::RETRIES];

/// How many rows a claim of `max` reads from each source: enough to step
/// past what it skips (consumed, expired, cancelled) and still fill `max`.
pub(super) fn due_want(max: usize) -> usize {
    max.saturating_mul(2).saturating_add(16)
}

/// A due entry of an index as a candidate.
fn index_candidate(fam: u8, at: (i64, u64), value: Vec<u8>) -> Candidate {
    let (at, off) = at;
    if fam == keys::AVAIL {
        Candidate::Scheduled(at, off, value)
    } else {
        Candidate::Retry(at, off)
    }
}

/// The index into [`FAMILIES`] of an index candidate.
fn family_of(c: &Candidate) -> Option<usize> {
    match c {
        Candidate::Scheduled(..) => Some(0),
        Candidate::Retry(..) => Some(1),
        Candidate::Lane(_) => None,
    }
}

/// Whether the claim consumed an index candidate, so that no later read of
/// its index finds it: the cursor passed a scheduled entry, or a retry
/// entry was deleted.
fn consumed(hash: &[u8; 16], st: &NativeStage, c: &Candidate) -> bool {
    match c {
        Candidate::Scheduled(at, off, _) => {
            let s = &st.state;
            s.cursor_started && (*at, *off) <= (s.cursor_at, s.cursor_off)
        }
        Candidate::Retry(at, off) => {
            st.rows.get(&keys::by_time(hash, keys::RETRIES, *at, *off)) == Some(&None)
        }
        Candidate::Lane(_) => true,
    }
}

/// Raise each index's floor to the first entry the claim left (invariant
/// F holds: every live entry of the index is at or past it): its first
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
        if let Some(fam) = family_of(c)
            && !consumed(hash, st, c)
        {
            left[fam].get_or_insert(c.key().0);
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

impl ClaimBudget {
    /// Whether a message of `bytes` still fits; a claim always takes its
    /// first message.
    fn fits(&mut self, bytes: u32) -> bool {
        if self.bytes > 0 && self.bytes + u64::from(bytes) > self.max_bytes {
            self.max = 0;
            return false;
        }
        true
    }
}

impl CommitTransaction<'_> {
    pub(super) async fn nq_claim(
        &mut self,
        hash: [u8; 16],
        st: &mut NativeStage,
        args: (usize, u64, i64),
        clock: (i64, u64),
    ) -> NqResult {
        let (max, max_bytes, lease_ms) = args;
        let (t, next) = clock;
        if st.state.paused {
            return Ok(NqOut::Claimed {
                deliveries: Vec::new(),
                paused: true,
                limited: false,
            });
        }
        let room = usize::try_from(
            u64::from(st.state.cfg.group.max_in_flight).saturating_sub(st.state.leased),
        )
        .unwrap_or(usize::MAX);
        let max = max
            .min(room)
            .min(usize::try_from(st.state.cfg.group.max_batch).unwrap_or(usize::MAX));
        // At the open-delivery or the failed cap a claim leases nothing new
        // (S3b-DESIGN §8.1): any new lease could become either.
        let s = &st.state;
        let capped = s.leased + s.retrying >= s.cfg.max_open_deliveries
            || s.failed >= s.cfg.max_failed_messages;
        if max == 0 || capped {
            return Ok(NqOut::Claimed {
                deliveries: Vec::new(),
                paused: false,
                limited: false,
            });
        }
        // Over its project's share at the shard's active or failed line
        // (S3b-DESIGN §8.2-§8.3): nothing new, and the reply says why.
        if self.nq_limited(st, queue_gauges::ACTIVE) || self.nq_limited(st, queue_gauges::FAILED) {
            return Ok(NqOut::Claimed {
                deliveries: Vec::new(),
                paused: false,
                limited: true,
            });
        }
        let (candidates, after) = self.nq_candidates(&hash, st, max, t).await?;
        let mut budget = ClaimBudget {
            max,
            max_bytes,
            bytes: 0,
            lease_until: t + lease_ms,
            t,
        };
        let out = self
            .nq_take(&hash, st, &candidates, (&mut budget, next))
            .await?;
        raise_floors(&hash, st, &candidates, after);
        if !out.is_empty() {
            st.state.last_useful_claim_at = t;
            st.state.notify_backoff_ms = 1_000;
        }
        // A claim that moved the cursor or the frontier past expired,
        // cancelled or scheduled offsets changed state even if it leased
        // nothing; it advances the clock so no later send can sort below
        // the cursor (§5.5).
        if st.state_dirty || !out.is_empty() {
            Self::nq_tick(st, t);
        }
        Ok(NqOut::Claimed {
            deliveries: out,
            paused: false,
            limited: false,
        })
    }

    /// Take the candidates in order until the claim is full.
    async fn nq_take(
        &mut self,
        hash: &[u8; 16],
        st: &mut NativeStage,
        candidates: &[Candidate],
        claim: (&mut ClaimBudget, u64),
    ) -> Result<Vec<DeliveryView>, NqRefusal> {
        let (budget, next) = claim;
        let mut out: Vec<DeliveryView> = Vec::new();
        for (i, c) in candidates.iter().enumerate() {
            if out.len() >= budget.max {
                break;
            }
            let leased = match c {
                Candidate::Retry(_, offset) => self.nq_claim_retry(hash, st, *offset, budget).await?,
                Candidate::Scheduled(at, offset, entry) => {
                    self.nq_claim_scheduled(hash, st, (*at, *offset, entry), budget)
                        .await?
                }
                Candidate::Lane(page) => {
                    let following = candidates[i + 1..].iter().find_map(|c| match c {
                        Candidate::Lane(p) => Some(p.ts),
                        _ => None,
                    });
                    self.nq_claim_lane(hash, st, page, (budget, following, next))
                        .await?
                }
            };
            if let Some(v) = leased {
                out.push(v);
            }
            if budget.bytes >= budget.max_bytes {
                break;
            }
        }
        Ok(out)
    }

    /// Due entries of both indexes and the lane's pages from the frontier,
    /// merged in (time, offset) order, each index read from its floor (the
    /// scheduled index from the cursor), with each index's first entry past
    /// `t` as [`Self::nq_due_scan`] found it (`None` for an index not
    /// read).
    async fn nq_candidates(
        &self,
        hash: &[u8; 16],
        st: &mut NativeStage,
        max: usize,
        t: i64,
    ) -> Result<(Vec<Candidate>, [Option<i64>; 2]), NqRefusal> {
        let want = due_want(max);
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
                (at <= t).then(|| index_candidate(fam, (at, off), v))
            });
            out.extend(due);
        }
        let lane = self
            .nq_lane_pages(hash, st, st.state.frontier, want)
            .await?;
        out.extend(lane.into_iter().map(Candidate::Lane));
        out.sort_unstable_by_key(Candidate::key);
        Ok((out, next))
    }

    /// The frontier's step at one of the lane's pages: pass a scheduled or
    /// cancelled offset, expire a dead one, lease a live one (or stop when
    /// it does not fit). A page an earlier claim of the group already
    /// passed is left alone.
    async fn nq_claim_lane(
        &mut self,
        hash: &[u8; 16],
        st: &mut NativeStage,
        page: &LanePage,
        claim: (&mut ClaimBudget, Option<i64>, u64),
    ) -> Result<Option<DeliveryView>, NqRefusal> {
        let (budget, following, next) = claim;
        if page.offset != st.state.frontier {
            return Ok(None);
        }
        let t = budget.t;
        let mut range = self.nq_range(hash, st, page.offset).await?;
        let step = at_frontier(&range, page, st.state.cfg.retention_ms, t);
        if step == AtFrontier::Live && !budget.fits(page.bytes()) {
            return Ok(None);
        }
        self.nq_frontier_step(hash, st, page, (step, &mut range, t));
        Self::nq_advance(st, page, following, next);
        if step != AtFrontier::Live {
            Self::nq_set_range(st, page.offset, range);
            return Ok(None);
        }
        budget.bytes += u64::from(page.bytes());
        let facts = crate::native_queue::rows::MessageFacts {
            enqueued_at: page.ts,
            available_at: page.ts,
            bytes: page.bytes(),
        };
        let mut row = first_delivery(&facts, (false, false), t, &st.state.cfg);
        let until = budget.lease_until.min(facts.hard_expires_at(&st.state.cfg));
        self.nq_lease(hash, st, (page.offset, &mut row), until);
        range.open += 1;
        Self::nq_set_range(st, page.offset, range);
        let mut leased = view(page.offset, &facts, Some(&row));
        leased.page = Some(page.raw.clone());
        Ok(Some(leased))
    }

    /// What passing a page that is not leased does: a resolved scheduled
    /// message's page goes; a cancelled one's page and bytes go; an expired
    /// one enters the gap.
    pub(super) fn nq_frontier_step(
        &mut self,
        hash: &[u8; 16],
        st: &mut NativeStage,
        page: &LanePage,
        at: (AtFrontier, &mut crate::native_queue::range::RangeRow, i64),
    ) {
        let (step, range, t) = at;
        match step {
            AtFrontier::Scheduled { resolved } => {
                range.skip.clear(page.offset);
                if resolved {
                    self.nq_delete_page(hash, st, (page.offset, page.bytes()), t);
                }
            }
            AtFrontier::Cancelled => {
                range.cancel.clear(page.offset);
                st.state.queued_bytes = st
                    .state
                    .queued_bytes
                    .saturating_sub(u64::from(page.bytes()));
                self.nq_delete_page(hash, st, (page.offset, page.bytes()), t);
            }
            AtFrontier::Expired => {
                st.state.gap_count += 1;
                st.state.counters.expired += 1;
                st.state.queued_messages = st.state.queued_messages.saturating_sub(1);
                st.state.queued_bytes = st
                    .state
                    .queued_bytes
                    .saturating_sub(u64::from(page.bytes()));
                self.nq_delete_page(hash, st, (page.offset, page.bytes()), t);
            }
            AtFrontier::Live => {}
        }
    }

    /// Lease a scheduled message from its entry, which holds its facts as
    /// sent and maybe its page; the entry and its bits stay until the
    /// message ends. One past its retention end is resolved into the gap.
    async fn nq_claim_scheduled(
        &mut self,
        hash: &[u8; 16],
        st: &mut NativeStage,
        at: (i64, u64, &[u8]),
        budget: &mut ClaimBudget,
    ) -> Result<Option<DeliveryView>, NqRefusal> {
        let (due, offset, entry) = at;
        let t = budget.t;
        let (facts, page) = decode_scheduled(due, entry).map_err(NqRefusal::Internal)?;
        if !budget.fits(facts.bytes) {
            // Not taken: the cursor stays before it.
            return Ok(None);
        }
        st.state.cursor_at = due;
        st.state.cursor_off = offset;
        st.state.cursor_started = true;
        st.state_dirty = true;
        if facts.retention_end(&st.state.cfg) <= t {
            st.state.gap_count += 1;
            st.state.counters.expired += 1;
            self.nq_end_scheduled(hash, st, (offset, &facts, !page.is_empty()), t)
                .await?;
            return Ok(None);
        }
        budget.bytes += u64::from(facts.bytes);
        let mut row = first_delivery(&facts, (true, !page.is_empty()), t, &st.state.cfg);
        let until = budget.lease_until.min(facts.hard_expires_at(&st.state.cfg));
        self.nq_lease(hash, st, (offset, &mut row), until);
        self.nq_open(hash, st, offset).await?;
        let mut leased = view(offset, &facts, Some(&row));
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
        let Some(mut row) = self.nq_delivery(hash, st, offset).await? else {
            return Ok(None);
        };
        if row.state != DState::Retrying || !budget.fits(row.bytes) {
            return Ok(None);
        }
        self.nq_unindex(hash, st, offset, &row);
        if t >= row.cycle_deadline {
            self.nq_fail(hash, st, (offset, &mut row), (Cause::Retention, None, t));
            Self::nq_tick(st, t);
            return Ok(None);
        }
        budget.bytes += u64::from(row.bytes);
        let facts = row.facts();
        let until = budget.lease_until.min(facts.hard_expires_at(&st.state.cfg));
        self.nq_lease(hash, st, (offset, &mut row), until);
        Ok(Some(view(offset, &facts, Some(&row))))
    }
}
