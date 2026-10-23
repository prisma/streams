//! Settle (docs/QUEUES-DESIGN.md §6.2-§6.3): every operation but `extend`
//! ends the lease (the lease generation rises), so a replayed settle finds
//! a different generation and is `stale`. A settle after the deadline is
//! accepted while the sweep has not yet moved the lease, marked late; an
//! extend after the deadline is refused `expired`.
use super::stage::NativeStage;
use super::*;
use crate::native_queue::rows::{Cause, DState, DeliveryRow, MetaRow};
use crate::native_queue::{FREE_RELEASES, SettleItem, SettleKind, SettleOutcome, SettleResult};

impl CommitTransaction<'_> {
    pub(super) async fn nq_settle(
        &mut self,
        hash: [u8; 16],
        st: &mut NativeStage,
        items: Vec<SettleItem>,
        t: i64,
    ) -> NqResult {
        let mut out = Vec::with_capacity(items.len());
        for item in items {
            out.push(self.nq_settle_one(&hash, st, item, t).await?);
        }
        Self::nq_tick(st, t);
        Ok(NqOut::Settled(out))
    }

    async fn nq_settle_one(
        &mut self,
        hash: &[u8; 16],
        st: &mut NativeStage,
        item: SettleItem,
        t: i64,
    ) -> Result<SettleResult, NqRefusal> {
        let offset = item.token.offset;
        let stale = SettleResult {
            outcome: SettleOutcome::Stale,
            accepted_late: false,
        };
        let (Some(meta), Some(mut row)) = (
            self.nq_meta(hash, st, offset).await?,
            self.nq_delivery(hash, st, offset).await?,
        ) else {
            st.state.counters.stale += 1;
            return Ok(stale);
        };
        if row.state != DState::Leased || row.lease_gen != item.token.lease_gen {
            st.state.counters.stale += 1;
            return Ok(stale);
        }
        let late = t > row.lease_deadline;
        if let SettleKind::Extend { lease_ms } = item.kind {
            if late {
                return Ok(SettleResult {
                    outcome: SettleOutcome::Expired,
                    accepted_late: false,
                });
            }
            self.nq_delete(
                st,
                keys::by_time(hash, keys::LEASES, row.lease_deadline, offset),
            );
            row.lease_deadline = (t + lease_ms).min(meta.hard_expires_at);
            self.nq_put(
                st,
                keys::by_time(hash, keys::LEASES, row.lease_deadline, offset),
                Vec::new(),
            );
            self.nq_put_delivery(hash, st, offset, &row);
            return Ok(SettleResult {
                outcome: SettleOutcome::Ok,
                accepted_late: false,
            });
        }
        self.nq_unindex(hash, st, offset, &row);
        self.nq_end_lease(hash, st, (offset, &mut row, &meta), (item, t));
        Ok(SettleResult {
            outcome: SettleOutcome::Ok,
            accepted_late: late,
        })
    }

    /// Apply a lease-ending settle to a delivery already out of `L`.
    fn nq_end_lease(
        &mut self,
        hash: &[u8; 16],
        st: &mut NativeStage,
        target: (u64, &mut DeliveryRow, &MetaRow),
        settle: (SettleItem, i64),
    ) {
        let (offset, row, meta) = target;
        let (item, t) = settle;
        match item.kind {
            SettleKind::Ack | SettleKind::Discard => {
                if item.kind == SettleKind::Ack {
                    st.state.counters.acked += 1;
                } else {
                    st.state.counters.discarded += 1;
                }
                self.nq_delete_message(hash, st, (offset, meta), (true, t));
            }
            SettleKind::Fail => {
                self.nq_fail(
                    hash,
                    st,
                    (offset, row, meta),
                    (Cause::Explicit, item.code, t),
                );
            }
            SettleKind::Release if row.releases < FREE_RELEASES => {
                row.attempts = row.attempts.saturating_sub(1);
                row.releases += 1;
                let due = (t + 1_000).min(row.cycle_deadline);
                self.nq_retry_at(hash, st, (offset, row), due);
            }
            SettleKind::Retry {
                delay_ms: Some(delay),
            } => {
                row.releases = 0;
                st.state.counters.retried += 1;
                if row.attempts >= st.state.cfg.group.max_attempts {
                    self.nq_fail(
                        hash,
                        st,
                        (offset, row, meta),
                        (Cause::Exhausted, item.code, t),
                    );
                } else if t + delay > row.cycle_deadline {
                    self.nq_fail(
                        hash,
                        st,
                        (offset, row, meta),
                        (Cause::Retention, item.code, t),
                    );
                } else {
                    self.nq_retry_at(hash, st, (offset, row), t + delay);
                }
            }
            SettleKind::Retry { delay_ms: None } | SettleKind::Release => {
                row.releases = 0;
                st.state.counters.retried += 1;
                self.nq_retry_or_fail(hash, st, (offset, row, meta), (item.code, t));
            }
            SettleKind::Extend { .. } => unreachable!("extend keeps the lease"),
        }
    }

    /// Policy retry, or failure when the attempts or the cycle are spent.
    pub(super) fn nq_retry_or_fail(
        &mut self,
        hash: &[u8; 16],
        st: &mut NativeStage,
        target: (u64, &mut DeliveryRow, &MetaRow),
        failure: (Option<String>, i64),
    ) {
        let (offset, row, meta) = target;
        let (code, t) = failure;
        match Self::nq_policy_retry(st, row, t) {
            Some(due) => self.nq_retry_at(hash, st, (offset, row), due),
            None => {
                let cause = if t >= row.cycle_deadline {
                    Cause::Retention
                } else {
                    Cause::Exhausted
                };
                self.nq_fail(hash, st, (offset, row, meta), (cause, code, t));
            }
        }
    }
}
