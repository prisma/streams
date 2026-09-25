//! Settling replaced incarnations' closure debts (`registry/replaced.rs`):
//! the tombstone walk's counterpart for an incarnation whose descriptor a
//! recreation replaced. Every instance runs it and closes only the segments
//! it owns, at the persisted instant the debt carries, exactly as the walk
//! closes a terminal descriptor it can still find.
use super::{
    AppState, WALK_CLOSE_SUBMITS, sweep_resident_budget, walk_engine_budgeted, walk_settle,
};
use crate::registry::StreamDesc;
use crate::registry::replaced::DebtEntry;
use std::sync::Arc;

/// Debts one sweep examines; the rest wait for the next sweep.
const DEBTS_PER_SWEEP: usize = 64;

/// Whether the pass goes on to the next debt or segment, or stops until the
/// next sweep (a paused read, a deferred open).
#[derive(PartialEq)]
enum Pass {
    Next,
    Stop,
}

/// One pass over the cell's closure debts. A debt whose incarnation is
/// still the stored one is judged by the stored descriptor: live (or
/// retained for its forks) means the recreation that wrote it lost to a
/// renewal and replaced nothing, so the debt is dropped; dead means the
/// walk's terminal path closes it, so the debt waits. Otherwise the
/// incarnation was replaced: each segment this instance owns is closed at
/// the debt's instant while its gauge is open, and marked settled once its
/// owner finds nothing of that incarnation left open. The last segment's
/// settlement removes the debt.
pub(crate) async fn settle_replaced(state: &Arc<AppState>) {
    if state.billing.usage_key().is_none() {
        return;
    }
    let after = state.billing.debt_cursor().await;
    let page = match state
        .registry
        .replaced_page(after.as_deref(), DEBTS_PER_SWEEP)
        .await
    {
        Ok(page) => page,
        Err(error) => {
            tracing::warn!("closure-debt pass paused (registry list): {error}");
            return;
        }
    };
    // Resume after the last debt this pass finished; a short page is the
    // end of the listing, so the next sweep wraps to the start.
    let full = page.len() == DEBTS_PER_SWEEP;
    let mut done = after;
    for entry in page {
        if settle_debt(state, &entry).await == Pass::Stop {
            state.billing.set_debt_cursor(done).await;
            return;
        }
        done = Some(entry.key);
    }
    state
        .billing
        .set_debt_cursor(if full { done } else { None })
        .await;
}

async fn settle_debt(state: &Arc<AppState>, entry: &DebtEntry) -> Pass {
    let Ok(dead) = entry.debt.incarnation() else {
        tracing::error!(key = entry.key, "closure debt names an invalid descriptor");
        return Pass::Next;
    };
    let stored = match state.registry.get(&dead.sref()).await {
        Ok(stored) => stored,
        Err(error) => {
            tracing::warn!("closure-debt pass paused (descriptor read): {error}");
            return Pass::Stop;
        }
    };
    if let Some(stored) = stored.filter(|s| s.stream_epoch == dead.stream_epoch) {
        judge_unreplaced(state, entry, &stored).await;
        return Pass::Next;
    }
    let Ok(segments) = entry.debt.segments() else {
        return Pass::Next;
    };
    for sid in segments
        .into_iter()
        .filter(|s| !entry.debt.settled.contains(s))
    {
        if settle_segment(state, entry, &dead, sid).await == Pass::Stop {
            return Pass::Stop;
        }
    }
    Pass::Next
}

/// The debt's incarnation is still stored: nothing replaced it yet. Live
/// or retained, the recreation lost and the debt is spurious; dead, the
/// walk's terminal path closes it and the debt waits for a replacement.
async fn judge_unreplaced(state: &Arc<AppState>, entry: &DebtEntry, stored: &StreamDesc) {
    let now = super::billing_now_ms();
    let terminal = stored.deleted || stored.expires_at_ms.is_some_and(|e| now >= e);
    let retained = stored.soft_deleted && !stored.deleted;
    if (!terminal || retained)
        && let Err(error) = state.registry.drop_replaced(&entry.key).await
    {
        tracing::warn!("closure debt {} not dropped: {error}", entry.key);
    }
}

async fn settle_segment(
    state: &Arc<AppState>,
    entry: &DebtEntry,
    dead: &StreamDesc,
    sid: u32,
) -> Pass {
    let Some(route) = dead.segment_route_by_id(sid) else {
        tracing::error!(
            segment = sid,
            key = entry.key,
            "closure debt names a missing segment"
        );
        return Pass::Next;
    };
    let budget = sweep_resident_budget(&state.config.billing);
    let Some((engine, ours)) = walk_engine_budgeted(state, &route, budget).await else {
        // Deferred or not ours to open now: the next sweep retries.
        return Pass::Stop;
    };
    let hash = dead.dynamic_segment_identity(sid);
    let pass = match engine.load_billing_meta(hash).await {
        Ok(Some(meta))
            if meta.stream_id == dead.stream_epoch && meta.owned_frame_bytes_current > 0 =>
        {
            tracing::info!(
                "closure debt: closing replaced {}#{sid} ({} B) at persisted {}",
                dead.sref(),
                meta.owned_frame_bytes_current,
                entry.debt.close_ms
            );
            match engine.submit_billing_close(hash, entry.debt.close_ms).await {
                Ok(()) => {
                    WALK_CLOSE_SUBMITS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                }
                Err(error) => tracing::warn!("closure-debt close failed: {error}"),
            }
            Pass::Next
        }
        Ok(_) => {
            if let Err(error) = state.registry.settle_replaced(&entry.key, sid).await {
                tracing::warn!("closure debt {} not settled: {error}", entry.key);
            }
            Pass::Next
        }
        Err(error) => {
            tracing::error!("closure-debt metadata read failed: {error}");
            Pass::Stop
        }
    };
    if ours {
        walk_settle(state, &state.shards.prefix_for(&route)).await;
    }
    pass
}
