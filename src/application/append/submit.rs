use super::{APPEND_TIMEOUT, AppendCode, AppendFailure, AppendService, FailureClass, fail};
use crate::registry::{SegRoute, StreamDesc};
use crate::shard::{AppendAck, AppendErr, AppendReq};
use tokio::sync::oneshot;

pub(super) async fn submit(
    state: &AppendService,
    desc: &StreamDesc,
    seg: &SegRoute,
    req: AppendReq,
    rx: oneshot::Receiver<Result<AppendAck, AppendErr>>,
) -> Result<Result<AppendAck, AppendErr>, AppendFailure> {
    let hash = seg.identity;
    let sref = desc.sref();
    let name = sref.name().as_str();
    let has_entries = !req.entries.is_empty();
    let close_only = matches!(req.finish, crate::shard::AppendFinish::Close) && !has_entries;
    let engine = match state
        .shards
        .resolve(&seg.shard_route, crate::shard_directory::Adoption::External)
        .await
    {
        Ok(e) => e,
        Err(e) => return Err(AppendFailure::from_resolve(e)),
    };
    // Round-13: bind this SEGMENT's durable-write pressure attribution
    // to the project's admission entry (once per resident handle
    // incarnation; seeded from the applied tail's exact
    // unabsorbed_bytes — never from zero when durable debt exists).
    if let Some(adm) = state.quotas.pressure_handle(sref.project_id())
        && let Ok(h) = engine.stream_handle(hash).await
    {
        h.bind_pressure(adm);
    }
    // R25-C: THE maintenance admission point — one, in the shared append
    // core, after `engine_for` resolved ownership. A non-owner already
    // received its Streams-Replay-To above and never reaches this, so a
    // stale local latch cannot answer for someone else's backlog. Both
    // public append surfaces converge here (raw /v1/stream/{*name}
    // including hierarchical names, product append and appendMany, every
    // routing key, split children on their own shard routes), so there
    // is no second copy of the route grammar to drift.
    //
    // Skips: close-only operations carry no entries and must stay
    // admitted (an operator closing a stream is REDUCING future work),
    // and reserved system streams stay admitted because overload
    // recovery must not deadlock on its own system-of-record writes.
    if !close_only && has_entries && !crate::billing::is_reserved_stream(name) {
        let limits = state.maintenance_limits();
        if let Some(cause) = state.admission.admit_maintenance(&engine, &limits) {
            state.admission.note_maintenance_shed();
            return fail(
                FailureClass::Unavailable,
                AppendCode::MaintenanceBackpressure,
                &format!("{}; retry after maintenance catches up", cause.as_str()),
            )
            .map_err(|e| e.retry(5));
        }
    }
    // Wedge shed: if the shard's durability pipeline is stalled — either
    // the commit db.write is blocked (unflushed-full) or committed groups
    // have waited on the durable watermark beyond the threshold (WAL flush
    // stalled behind L0-full) — reject with a retryable 429 instead of
    // queueing. Without this, appends hang until the platform front door
    // kills them at ~30 s (8-minute wedge, 2026-07-21; detector missed the
    // stale-durability mode on 2026-07-22 when it watched db.write only).
    // 5 s: healthy durable waits under load peak ~1.5 s; a real wedge
    // climbs to 30 s+, so 5 s discriminates cleanly without false sheds.
    let blocked = engine.wedge_ms();
    if blocked > 5_000 {
        state.admission.note_wedge_shed();
        return fail(
            FailureClass::Capacity,
            AppendCode::EngineBackpressure,
            "commit pipeline blocked (compaction lag); retry",
        )
        .map_err(|e| e.retry(2));
    }
    if engine.try_enqueue(req).is_err() {
        return fail(
            FailureClass::Capacity,
            AppendCode::Overloaded,
            "append queue full",
        );
    }
    match tokio::time::timeout(APPEND_TIMEOUT, rx).await {
        Ok(Ok(o)) => Ok(o),
        _ => fail(
            FailureClass::Timeout,
            AppendCode::AppendTimeout,
            "append timed out; outcome unknown",
        ),
    }
}
