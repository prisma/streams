//! Delivery transitions shared by claim, settle and the sweep (§6-§8):
//! each moves a delivery between leased, retrying and failed and keeps its
//! index entries (`L`, `R`, `F`, `E`) and the queue's counts in step, and
//! ends a message by deleting every row it owns in the same batch, which
//! records the deleted bytes as collection debt (§8.2).
use super::stage::NativeStage;
use super::*;
use crate::native_queue::rows::{Cause, DState, DeliveryRow, MetaRow};
use crate::native_queue::{DeliveryState, DeliveryView, FailureView};

/// Fixed row charge for a message's metadata, index and delivery rows
/// (§8.3), added to its record bytes when it is deleted.
pub(super) const ROW_CHARGE: u64 = 256;

impl CommitTransaction<'_> {
    pub(super) async fn nq_meta(
        &self,
        hash: &[u8; 16],
        st: &NativeStage,
        offset: u64,
    ) -> Result<Option<MetaRow>, NqRefusal> {
        let raw = self
            .nq_get(st, &keys::by_offset(hash, keys::META, offset))
            .await
            .map_err(NqRefusal::Internal)?;
        raw.map(|r| MetaRow::decode(&r).map_err(NqRefusal::Internal))
            .transpose()
    }

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

    pub(super) fn nq_put_meta(
        &mut self,
        hash: &[u8; 16],
        st: &mut NativeStage,
        offset: u64,
        meta: &MetaRow,
    ) {
        self.nq_put(st, keys::by_offset(hash, keys::META, offset), meta.encode());
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

    /// Lease the delivery `row` until `t + lease_ms` (never past the
    /// message's hard lifetime): attempts and the lease generation rise.
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
        rows: (u64, &mut DeliveryRow, &MetaRow),
        failure: (Cause, Option<String>, i64),
    ) {
        let (offset, row, meta) = rows;
        let (cause, code, t) = failure;
        row.state = DState::Failed;
        row.lease_gen = row.lease_gen.wrapping_add(1);
        row.failed_at = t;
        row.failure_expires_at = (t + st.state.cfg.failure_retention_ms).min(meta.hard_expires_at);
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
        st.state.queued_bytes = st.state.queued_bytes.saturating_sub(u64::from(meta.bytes));
    }

    /// End a message: delete its page and every row it owns (the dedup
    /// entry keeps its own lifetime, §8.3), and record the debt. `undone`
    /// says whether it still counted in the admission budget.
    pub(super) fn nq_delete_message(
        &mut self,
        hash: &[u8; 16],
        st: &mut NativeStage,
        message: (u64, &MetaRow),
        undone: (bool, i64),
    ) {
        let (offset, meta) = message;
        let (undone, t) = undone;
        self.batch
            .delete(crate::crypto_page::shard_page_key(hash, offset));
        self.nq_delete(st, keys::by_offset(hash, keys::META, offset));
        self.nq_delete(
            st,
            keys::by_time(hash, keys::AVAIL, meta.available_at, offset),
        );
        self.nq_delete(st, keys::by_offset(hash, keys::DELIVERY, offset));
        if undone {
            st.state.queued_messages = st.state.queued_messages.saturating_sub(1);
            st.state.queued_bytes = st.state.queued_bytes.saturating_sub(u64::from(meta.bytes));
        }
        Self::nq_debt(st, u64::from(meta.bytes) + ROW_CHARGE, t);
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

pub(super) fn view(offset: u64, meta: &MetaRow, row: Option<&DeliveryRow>) -> DeliveryView {
    let Some(row) = row else {
        return DeliveryView {
            offset,
            state: DeliveryState::Pending,
            lease_gen: 0,
            attempt: 0,
            cycle: 1,
            enqueued_at: meta.enqueued_at,
            available_at: meta.available_at,
            lease_deadline: 0,
            failure: None,
            page: None,
            bytes: meta.bytes,
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
        enqueued_at: meta.enqueued_at,
        available_at: meta.available_at,
        lease_deadline: row.lease_deadline,
        failure: (row.state == DState::Failed).then(|| FailureView {
            cause: row.cause.map_or("explicit", Cause::name),
            code: row.code.clone(),
            attempts: row.attempts,
            failed_at: row.failed_at,
            failure_expires_at: row.failure_expires_at,
        }),
        page: None,
        bytes: meta.bytes,
    }
}
