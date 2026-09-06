use super::{AppendCode, AppendFailure, AppendService, FailureClass, fail};
use crate::registry::{SegRoute, StreamDesc};

pub(super) async fn resolve_segment(
    state: &AppendService,
    desc: &mut StreamDesc,
    routing_key: &str,
) -> Result<SegRoute, AppendFailure> {
    let sref = desc.sref();
    let mut seg = desc.resolve_segment(routing_key);
    if seg.sealed {
        state.registry.invalidate(&sref);
        match state.registry.get(&sref).await {
            Ok(Some(d2)) if state.alive(&d2) && d2.stream_epoch == desc.stream_epoch => {
                *desc = d2;
                seg = desc.resolve_segment(routing_key);
            }
            _ => {}
        }
        if seg.sealed {
            return fail(
                FailureClass::Unavailable,
                AppendCode::SegmentTransition,
                "segment map transition in progress; retry",
            )
            .map_err(|e| e.retry(1));
        }
    }
    Ok(seg)
}
