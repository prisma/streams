//! Delivery transitions shared by claim, settle and the sweep (§6-§8):
//! each moves a delivery between leased, retrying and failed and keeps its
//! index entries (`L`, `R`, `F`, `E`), its range row's references and the
//! queue's counts in step, and ends a message by deleting every row it owns
//! in the same batch, which records the deleted bytes as collection debt
//! (§8.2). A message's facts ride its delivery row or its scheduled entry:
//! there is no metadata row (S3b-DESIGN §3.2).
use super::stage::NativeStage;
use super::*;
use crate::native_queue::rows::{Cause, DState, DeliveryRow, MessageFacts, decode_scheduled};
use crate::native_queue::{DeliveryState, DeliveryView, FailureView};

/// Fixed row charge for a message's delivery, index and scheduled rows
/// (§8.3), added to the debt when it ends.
pub(super) const ROW_CHARGE: u64 = 256;

/// A scheduled message as its entry holds it.
pub(super) struct Scheduled {
    pub facts: MessageFacts,
    /// Whether its page rides inline in the entry.
    pub inline: bool,
}

impl CommitTransaction<'_> {
    pub(super) async fn nq_delivery(
        &self,
        hash: &[u8; 16],
        st: &NativeStage,
        offset: u64,
    ) -> Result<Option<DeliveryRow>, NqRefusal> {
        let raw = self
            .nq_get(st, &keys::by_offset(hash, keys::DELIVERY, offset))
            .await
            .map_err(NqRefusal::Internal)?;
        raw.map(|r| DeliveryRow::decode(&r).map_err(NqRefusal::Internal))
            .transpose()
    }

    /// An unresolved scheduled message at `offset`, found through its
    /// `SCHEDULED_AT` row.
    pub(super) async fn nq_scheduled(
        &self,
        hash: &[u8; 16],
        st: &NativeStage,
        offset: u64,
    ) -> Result<Option<Scheduled>, NqRefusal> {
        let at = self
            .nq_get(st, &keys::by_offset(hash, keys::SCHEDULED_AT, offset))
            .await
            .map_err(NqRefusal::Internal)?;
        let Some(at) = at else { return Ok(None) };
        let at = i64::from_le_bytes(
            at.as_slice()
                .try_into()
                .map_err(|_| NqRefusal::Internal("queue scheduled offset row".into()))?,
        );
        let entry = self
            .nq_get(st, &keys::by_time(hash, keys::AVAIL, at, offset))
            .await
            .map_err(NqRefusal::Internal)?
            .ok_or_else(|| NqRefusal::Internal(format!("scheduled entry {offset} missing")))?;
        let (facts, page) = decode_scheduled(at, &entry).map_err(NqRefusal::Internal)?;
        Ok(Some(Scheduled {
            facts,
            inline: !page.is_empty(),
        }))
    }

    pub(super) fn nq_put_delivery(
        &mut self,
        hash: &[u8; 16],
        st: &mut NativeStage,
        offset: u64,
        row: &DeliveryRow,
    ) {
        self.nq_put(
            st,
            keys::by_offset(hash, keys::DELIVERY, offset),
            row.encode(),
        );
    }

    /// Remove the index entry the delivery's current state owns, and its
    /// count.
    pub(super) fn nq_unindex(
        &mut self,
        hash: &[u8; 16],
        st: &mut NativeStage,
        offset: u64,
        row: &DeliveryRow,
    ) {
        match row.state {
            DState::Leased => {
                self.nq_delete(
                    st,
                    keys::by_time(hash, keys::LEASES, row.lease_deadline, offset),
                );
                st.state.leased = st.state.leased.saturating_sub(1);
            }
            DState::Retrying => {
                self.nq_delete(st, keys::by_time(hash, keys::RETRIES, row.due_at, offset));
                st.state.retrying = st.state.retrying.saturating_sub(1);
            }
            DState::Failed => {
                self.nq_delete(st, keys::by_time(hash, keys::FAILED, row.failed_at, offset));
                self.nq_delete(
                    st,
                    keys::by_time(hash, keys::FAIL_EXPIRY, row.failure_expires_at, offset),
                );
                st.state.failed = st.state.failed.saturating_sub(1);
            }
        }
    }

    /// Lease the delivery `row` until `until` (never past the message's
    /// hard lifetime; the caller clamps): attempts and the lease
    /// generation rise.
    pub(super) fn nq_lease(
        &mut self,
        hash: &[u8; 16],
        st: &mut NativeStage,
        target: (u64, &mut DeliveryRow),
        until: i64,
    ) {
        let (offset, row) = target;
        row.state = DState::Leased;
        row.attempts += 1;
        row.lease_gen = row.lease_gen.wrapping_add(1);
        row.lease_deadline = until;
        row.due_at = 0;
        self.nq_put(
            st,
            keys::by_time(hash, keys::LEASES, until, offset),
            Vec::new(),
        );
        self.nq_put_delivery(hash, st, offset, row);
        st.state.leased += 1;
        st.state.counters.delivered += 1;
    }

    /// A message's first delivery opens: its range row counts the
    /// reference until the message ends.
    pub(super) async fn nq_open(
        &mut self,
        hash: &[u8; 16],
        st: &mut NativeStage,
        offset: u64,
    ) -> Result<(), NqRefusal> {
        let mut range = self.nq_range(hash, st, offset).await?;
        range.open += 1;
        Self::nq_set_range(st, offset, range);
        Ok(())
    }

    /// Schedule the delivery again at `due` (already clamped by the caller).
    pub(super) fn nq_retry_at(
        &mut self,
        hash: &[u8; 16],
        st: &mut NativeStage,
        target: (u64, &mut DeliveryRow),
        due: i64,
    ) {
        let (offset, row) = target;
        row.state = DState::Retrying;
        row.lease_gen = row.lease_gen.wrapping_add(1);
        row.due_at = due;
        st.state.floors.retry_cycle = st.state.floors.retry_cycle.min(row.cycle_deadline);
        st.state_dirty = true;
        self.nq_put(
            st,
            keys::by_time(hash, keys::RETRIES, due, offset),
            Vec::new(),
        );
        self.nq_put_delivery(hash, st, offset, row);
        st.state.retrying += 1;
    }

    /// Move the delivery to failed (§7.1): keyless, in place, out of the
    /// admission budget, kept until its failure expiry.
    pub(super) fn nq_fail(
        &mut self,
        hash: &[u8; 16],
        st: &mut NativeStage,
        target: (u64, &mut DeliveryRow),
        failure: (Cause, Option<String>, i64),
    ) {
        let (offset, row) = target;
        let (cause, code, t) = failure;
        let hard = row.facts().hard_expires_at(&st.state.cfg);
        row.state = DState::Failed;
        row.lease_gen = row.lease_gen.wrapping_add(1);
        row.failed_at = t;
        row.failure_expires_at = (t + st.state.cfg.failure_retention_ms).min(hard);
        row.cause = Some(cause);
        row.code = code;
        self.nq_put(st, keys::by_time(hash, keys::FAILED, t, offset), Vec::new());
        self.nq_put(
            st,
            keys::by_time(hash, keys::FAIL_EXPIRY, row.failure_expires_at, offset),
            Vec::new(),
        );
        self.nq_put_delivery(hash, st, offset, row);
        st.state.failed += 1;
        st.state.counters.failed += 1;
        st.state.queued_messages = st.state.queued_messages.saturating_sub(1);
        st.state.queued_bytes = st.state.queued_bytes.saturating_sub(u64::from(row.bytes));
    }

    /// End a message with an open delivery (already out of its index):
    /// delete its delivery row and, for a scheduled message, its scheduled
    /// rows; its page goes too once the frontier is past it (else when the
    /// frontier passes). `undone` says whether it still counted in the
    /// admission budget.
    pub(super) async fn nq_end_message(
        &mut self,
        hash: &[u8; 16],
        st: &mut NativeStage,
        message: (u64, &DeliveryRow),
        undone: (bool, i64),
    ) -> Result<(), NqRefusal> {
        let (offset, row) = message;
        let (undone, t) = undone;
        st.hints.add(offset, offset + 1);
        self.nq_delete(st, keys::by_offset(hash, keys::DELIVERY, offset));
        let mut range = self.nq_range(hash, st, offset).await?;
        range.open = range.open.saturating_sub(1);
        if row.scheduled {
            self.nq_unschedule(hash, st, (offset, &row.facts(), row.inline), &mut range);
        }
        Self::nq_set_range(st, offset, range);
        if undone {
            st.state.queued_messages = st.state.queued_messages.saturating_sub(1);
            st.state.queued_bytes = st.state.queued_bytes.saturating_sub(u64::from(row.bytes));
        }
        Self::nq_debt(st, ROW_CHARGE, t);
        Ok(())
    }

    /// End an unresolved scheduled message no delivery opened (expired
    /// untaken, or cancelled): its scheduled rows go. Its page stays in the
    /// queue's stream until its chunk is retired (S3b-DESIGN §7.3).
    pub(super) async fn nq_end_scheduled(
        &mut self,
        hash: &[u8; 16],
        st: &mut NativeStage,
        message: (u64, &MessageFacts, bool),
        t: i64,
    ) -> Result<(), NqRefusal> {
        let (offset, facts, inline) = message;
        st.hints.add(offset, offset + 1);
        let mut range = self.nq_range(hash, st, offset).await?;
        self.nq_unschedule(hash, st, (offset, facts, inline), &mut range);
        Self::nq_set_range(st, offset, range);
        st.state.queued_messages = st.state.queued_messages.saturating_sub(1);
        st.state.queued_bytes = st.state.queued_bytes.saturating_sub(u64::from(facts.bytes));
        Self::nq_debt(st, ROW_CHARGE, t);
        Ok(())
    }

    /// A scheduled message resolves: its entry, its by-offset row and its
    /// unresolved bit go (its `skip` bit stays for the frontier).
    fn nq_unschedule(
        &mut self,
        hash: &[u8; 16],
        st: &mut NativeStage,
        message: (u64, &MessageFacts, bool),
        range: &mut crate::native_queue::range::RangeRow,
    ) {
        let (offset, facts, inline) = message;
        if inline {
            st.state.inline_bytes = st.state.inline_bytes.saturating_sub(u64::from(facts.bytes));
        }
        self.nq_delete(
            st,
            keys::by_time(hash, keys::AVAIL, facts.available_at, offset),
        );
        self.nq_delete(st, keys::by_offset(hash, keys::SCHEDULED_AT, offset));
        range.sbits.clear(offset);
        range.sched = range.sched.saturating_sub(1);
        st.state.scheduled = st.state.scheduled.saturating_sub(1);
    }

    /// The next retry time for a delivery that failed an attempt: the
    /// policy's backoff clamped to the cycle deadline, or `None` when the
    /// attempts are spent (§6.3).
    pub(super) fn nq_policy_retry(st: &mut NativeStage, row: &DeliveryRow, t: i64) -> Option<i64> {
        if row.attempts >= st.state.cfg.group.max_attempts || t >= row.cycle_deadline {
            return None;
        }
        let jitter = st.state.next_jitter();
        let delay =
            crate::native_queue::backoff_ms(&st.state.cfg.group.backoff, row.attempts, jitter);
        Some((t + delay).min(row.cycle_deadline))
    }
}

/// A new delivery row for a message's first lease.
pub(super) fn first_delivery(
    facts: &MessageFacts,
    flags: (bool, bool),
    t: i64,
    cfg: &crate::native_queue::QueueConfig,
) -> DeliveryRow {
    let (scheduled, inline) = flags;
    DeliveryRow {
        state: DState::Leased,
        enqueued_at: facts.enqueued_at,
        available_at: facts.available_at,
        bytes: facts.bytes,
        scheduled,
        inline,
        cycle: 1,
        cycle_deadline: facts.retention_end(cfg),
        attempts: 0,
        releases: 0,
        internal_unleases: 0,
        lease_gen: 0,
        lease_deadline: 0,
        due_at: 0,
        first_delivered_at: t,
        failed_at: 0,
        failure_expires_at: 0,
        cause: None,
        code: None,
    }
}

pub(super) fn view(offset: u64, facts: &MessageFacts, row: Option<&DeliveryRow>) -> DeliveryView {
    let Some(row) = row else {
        return DeliveryView {
            offset,
            state: DeliveryState::Pending,
            lease_gen: 0,
            attempt: 0,
            cycle: 1,
            enqueued_at: facts.enqueued_at,
            available_at: facts.available_at,
            lease_deadline: 0,
            failure: None,
            page: None,
            bytes: facts.bytes,
        };
    };
    let state = match row.state {
        DState::Leased => DeliveryState::Leased,
        DState::Retrying => DeliveryState::Retrying,
        DState::Failed => DeliveryState::Failed,
    };
    DeliveryView {
        offset,
        state,
        lease_gen: row.lease_gen,
        attempt: row.attempts,
        cycle: row.cycle,
        enqueued_at: facts.enqueued_at,
        available_at: facts.available_at,
        lease_deadline: row.lease_deadline,
        failure: (row.state == DState::Failed).then(|| FailureView {
            cause: row.cause.map_or("explicit", Cause::name),
            code: row.code.clone(),
            attempts: row.attempts,
            failed_at: row.failed_at,
            failure_expires_at: row.failure_expires_at,
        }),
        page: None,
        bytes: facts.bytes,
    }
}
