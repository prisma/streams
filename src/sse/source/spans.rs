//! The lineage's engine-free pieces: the linearization rule and which
//! remote span refusals end a feed here.
use super::*;
use crate::application::read_remote::RemoteSpanError;

/// The linearization rule (engine-free, so the mapping itself is
/// unit-testable): a linearized one-past offset maps to the span
/// covering it; the boundary one-past a sealed span's cap belongs to
/// the NEXT span at local 0, and the last span, sealed or live, absorbs
/// everything past its start through the fallback below.
#[expect(
    clippy::expect_used,
    reason = "locate_in_spans; a lineage is built with at least one span, so the last span exists; a fallible tail would add a branch no lineage reaches"
)]
pub(super) fn locate_in_spans(
    spans: &[(u32, u64, Option<u64>)],
    logical_after: u64,
) -> WirePosition {
    for (seg, start, cap) in spans {
        match cap.map(|c| start + c) {
            Some(e) if logical_after >= e => continue,
            _ => {
                return WirePosition {
                    seg_id: *seg,
                    local_after: logical_after - start,
                };
            }
        }
    }
    let (seg_id, start, _) = spans.last().copied().expect("non-empty lineage");
    WirePosition {
        seg_id,
        local_after: logical_after.saturating_sub(start),
    }
}

/// A remote owner's refusal of one sealed-span page, as the verdict the
/// feed owes it (round-11.2): fleet auth after the forced refresh, a gone
/// or mismatched target and a second redirect are not fixed by reading
/// the same bound again, so they are typed cutoffs; everything else is
/// the owner's transient state, retried on the session's backoff.
pub(super) fn remote_span_verdict(seg_id: u32, refusal: RemoteSpanError) -> SourceReadError {
    match refusal {
        RemoteSpanError::Unauthorized => SourceReadError::Fatal(SourceCutoff::FleetAuth),
        RemoteSpanError::TargetGone => SourceReadError::Fatal(SourceCutoff::IncarnationChanged),
        RemoteSpanError::TargetMismatch => SourceReadError::Fatal(SourceCutoff::TargetMismatch),
        RemoteSpanError::RedirectLoop { first, second } => {
            tracing::warn!(span = seg_id, %first, %second, "sealed span redirect loop refused");
            SourceReadError::Fatal(SourceCutoff::RedirectLoop)
        }
        RemoteSpanError::Retryable { status, code } => SourceReadError::Retryable(anyhow::anyhow!(
            "remote span {seg_id}: retryable {status} {code:?}"
        )),
        RemoteSpanError::Transport(m) => {
            SourceReadError::Retryable(anyhow::anyhow!("remote span {seg_id}: transport {m}"))
        }
        RemoteSpanError::InvalidResponse(m) => SourceReadError::Retryable(anyhow::anyhow!(
            "remote span {seg_id}: invalid response {m}"
        )),
        RemoteSpanError::WrongOwner { owner } => SourceReadError::Retryable(anyhow::anyhow!(
            "remote span {seg_id}: unresolved owner {owner}"
        )),
    }
}
