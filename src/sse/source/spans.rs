//! The lineage's engine-free pieces: the linearization rule, the ownership
//! check and the fatal cutoff that rides through anyhow.
use super::*;

/// Round-11.2: a FATAL span error carried through anyhow — the feed
/// downcasts it and turns the source's lifecycle into the typed
/// cutoff instead of retrying forever.
#[derive(Debug, Clone, Copy)]
pub(crate) struct FatalSpanCutoff(pub(crate) crate::sse::feed::SourceCutoff);

impl std::fmt::Display for FatalSpanCutoff {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "fatal span cutoff: {:?}", self.0)
    }
}

impl std::error::Error for FatalSpanCutoff {}

/// Is this instance the effective owner of `route`'s shard? None-ring
/// (single instance) counts as ours.
pub(super) fn owned_here(state: &crate::application::read::ReadService, route: &[u8; 16]) -> bool {
    state.ownership.is_mine(&state.shards.prefix_for(route))
}

/// The linearization rule (engine-free, so the mapping itself is
/// unit-testable): a linearized one-past offset maps to the span
/// covering it; the boundary one-past a sealed span's cap belongs to
/// the NEXT span at local 0, and the last span, sealed or live, absorbs
/// everything past its start.
#[expect(
    clippy::expect_used,
    reason = "locate_in_spans; a lineage is built with at least one span, so the last span exists; a fallible tail would add a branch no lineage reaches"
)]
pub(super) fn locate_in_spans(
    spans: &[(u32, u64, Option<u64>)],
    logical_after: u64,
) -> WirePosition {
    for (i, (seg, start, cap)) in spans.iter().enumerate() {
        let last = i + 1 == spans.len();
        match cap.map(|c| start + c) {
            Some(e) if logical_after >= e && !last => continue,
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
