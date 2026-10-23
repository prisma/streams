//! The committer side of native queues (docs/QUEUES-DESIGN.md §5-§8): every
//! queue operation reads the queue's rows through the group's stage and
//! writes them into the group's single batch, so it is durable on the
//! group's watermark and its reply is released only then (G1).
use super::*;
use crate::native_queue::{NqOp, NqOut, NqRefusal, NqResult, keys};

mod admin;
mod claim;
mod deliveries;
mod send;
mod settle;
pub(super) mod stage;
mod sweep;

use stage::NativeStage;

pub(super) type NativeReply = oneshot::Sender<NqResult>;

/// What an operation tells the roster: it changed something (sweep soon),
/// or it was a sweep that computed the next deadline.
enum RosterHint {
    Changed,
    Swept(Option<i64>),
}

impl CommitTransaction<'_> {
    pub(super) async fn native_queue(
        &mut self,
        local: &mut StreamOverlay,
        hash: [u8; 16],
        op: NqOp,
        resp: NativeReply,
    ) {
        let mut st = match local.queue.native.take() {
            Some(st) => st,
            None => match self.nq_load(&hash).await {
                Ok(st) => st,
                Err(e) => {
                    self.effects
                        .native_acks
                        .push((resp, Err(NqRefusal::Internal(e))));
                    return;
                }
            },
        };
        let rows_before = st.rows.len();
        let t = now_ms().max(st.state.clock);
        let out = self.nq_dispatch(local, hash, &mut st, op).await;
        let changed = st.state_dirty || st.rows.len() != rows_before;
        self.nq_flush_state(&hash, &mut st);
        let hint = match &out {
            Ok(NqOut::Swept { next_deadline, .. }) => RosterHint::Swept(*next_deadline),
            _ => RosterHint::Changed,
        };
        if st.exists && (changed || matches!(hint, RosterHint::Swept(_))) {
            self.nq_roster(&hash, &st, &hint, t).await;
        }
        local.queue.native = Some(st);
        self.effects.native_acks.push((resp, out));
    }

    async fn nq_dispatch(
        &mut self,
        local: &mut StreamOverlay,
        hash: [u8; 16],
        st: &mut NativeStage,
        op: NqOp,
    ) -> NqResult {
        if let NqOp::Create { cfg } = op {
            return self.nq_create(local, hash, st, cfg);
        }
        if !st.exists {
            return Err(NqRefusal::NotFound);
        }
        let t = now_ms().max(st.state.clock);
        if st.state.deleting
            && !matches!(op, NqOp::DeleteStep { .. } | NqOp::Get | NqOp::BeginDelete)
        {
            return Err(NqRefusal::Deleting);
        }
        match op {
            NqOp::Create { .. } => unreachable!("handled above"),
            NqOp::Get => Ok(NqOut::Stats(Box::new(st.state.clone()))),
            NqOp::Update {
                group,
                max_queued_bytes,
                max_queued_messages,
            } => self.nq_update(st, group, max_queued_bytes, max_queued_messages),
            NqOp::BeginDelete => Ok(Self::nq_begin_delete(st, t)),
            NqOp::DeleteStep { max_rows } => self.nq_delete_step(local, hash, st, max_rows).await,
            NqOp::Send {
                messages,
                subkey,
                key_version,
            } => {
                self.nq_send(local, hash, st, (messages, subkey, key_version, t))
                    .await
            }
            NqOp::Claim {
                max,
                max_bytes,
                lease_ms,
            } => self.nq_claim(hash, st, (max, max_bytes, lease_ms), t).await,
            NqOp::Settle { items } => self.nq_settle(hash, st, items, t).await,
            NqOp::Pause { paused } => {
                st.state.paused = paused;
                st.state_dirty = true;
                Ok(NqOut::Paused(paused))
            }
            NqOp::Cancel { offsets } => self.nq_cancel(hash, st, offsets, t).await,
            NqOp::Redrive {
                offsets,
                all,
                limit,
            } => self.nq_redrive(hash, st, (offsets, all, limit), t).await,
            NqOp::Discard { offsets } => self.nq_discard(hash, st, offsets, t).await,
            NqOp::ListFailed { after, limit } => self.nq_list_failed(hash, st, after, limit).await,
            NqOp::Peek { after, limit } => self.nq_peek(hash, st, after, limit).await,
            NqOp::Message { offset } => self.nq_message(hash, st, offset).await,
            NqOp::Sweep => self.nq_sweep(hash, st, t).await,
            NqOp::ClearDebt {
                generation,
                bytes,
                captured_at,
            } => {
                if st.state.debt_generation == generation {
                    st.state.debt_bytes = 0;
                    st.state.debt_oldest_at = 0;
                } else {
                    // Debt recorded after the capture stays, aged from no
                    // earlier than the capture: the pass covered everything
                    // older.
                    st.state.debt_bytes = st.state.debt_bytes.saturating_sub(bytes);
                    st.state.debt_oldest_at = st.state.debt_oldest_at.max(captured_at);
                }
                st.state_dirty = true;
                Ok(NqOut::Count(st.state.debt_bytes))
            }
            NqOp::SetWakeup { url, name } => {
                st.state.wakeup_version = st.state.wakeup_version.wrapping_add(1);
                st.state.wakeup_url = url.clone();
                st.state.wakeup_name = name;
                st.state_dirty = true;
                Ok(NqOut::Wakeup {
                    url,
                    version: st.state.wakeup_version,
                })
            }
        }
    }

    /// Advance the queue clock to `t` (§5.5); called by every operation
    /// that changes queue state, never by one that only reads.
    pub(super) fn nq_tick(st: &mut NativeStage, t: i64) {
        if t > st.state.clock {
            st.state.clock = t;
        }
        st.state_dirty = true;
    }

    /// Record deleted bytes as collection debt (§8.2 stage 2).
    pub(super) fn nq_debt(st: &mut NativeStage, bytes: u64, t: i64) {
        if st.state.debt_bytes == 0 {
            st.state.debt_oldest_at = t;
        }
        st.state.debt_bytes = st.state.debt_bytes.saturating_add(bytes);
        st.state.debt_generation = st.state.debt_generation.wrapping_add(1);
        st.state_dirty = true;
    }

    /// The queue's roster row (§6.8): present while it has work that time
    /// alone moves on or debt to collect, so the sweep and the collector
    /// find it after a restart or move. Its value is when the sweep should
    /// look next: now after any change, the computed next deadline after a
    /// sweep (an hour out when only debt remains).
    async fn nq_roster(&mut self, hash: &[u8; 16], st: &NativeStage, hint: &RosterHint, t: i64) {
        let target = Self::nq_roster_target(st, hint, t);
        if matches!(hint, RosterHint::Swept(_)) {
            // A sweep that moved nothing writes only a changed deadline.
            let stored = self.engine.db.get(keys::roster(hash)).await.ok().flatten();
            let stored = stored
                .and_then(|v| <[u8; 8]>::try_from(&v[..]).ok())
                .map(i64::from_be_bytes);
            if stored == target {
                return;
            }
        }
        match target {
            Some(at) => self.batch.put(keys::roster(hash), at.to_be_bytes()),
            None => self.batch.delete(keys::roster(hash)),
        }
        self.extra_writes = true;
    }

    /// When the sweep should look next, or `None` when the queue has
    /// nothing time moves on and no debt.
    fn nq_roster_target(st: &NativeStage, hint: &RosterHint, t: i64) -> Option<i64> {
        let s = &st.state;
        let pending = s.queued_messages > 0
            || s.leased > 0
            || s.retrying > 0
            || s.failed > 0
            || s.dedup_entries > 0
            || s.deleting;
        (pending || s.debt_bytes > 0).then_some(match *hint {
            // Work already due waits for a claim, not the sweep: look
            // again in a minute rather than every second.
            RosterHint::Swept(Some(next)) if next <= t => t + 60_000,
            RosterHint::Swept(Some(next)) => next,
            RosterHint::Swept(None) => t + crate::native_queue::HOUR_MS,
            RosterHint::Changed => t,
        })
    }
}
