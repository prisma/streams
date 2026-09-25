use super::*;

/// The linearization rule maps linearized offsets back to
/// (seg_id, segment-local), with sealed-cap boundaries owned by
/// the NEXT span.
#[test]
fn linearized_cursor_space_roundtrips() {
    let spans = [(0u32, 0u64, Some(5)), (1, 5, Some(3)), (2, 8, None)];
    // Inside the first span.
    let wp = |seg, local| WirePosition {
        seg_id: seg,
        local_after: local,
    };
    assert_eq!(locate_in_spans(&spans, 0), wp(0, 0));
    assert_eq!(locate_in_spans(&spans, 4), wp(0, 4));
    // The cap boundary belongs to the next span at local 0.
    assert_eq!(locate_in_spans(&spans, 5), wp(1, 0));
    assert_eq!(locate_in_spans(&spans, 7), wp(1, 2));
    assert_eq!(locate_in_spans(&spans, 8), wp(2, 0));
    // The live tail is open-ended.
    assert_eq!(locate_in_spans(&spans, 100), wp(2, 92));
}

#[test]
fn sig_compatibility_rules() {
    // A live span may gain its sealed cap; spans may be appended.
    let old = [(0u32, 0u64, None)];
    let new = [(0u32, 0u64, Some(5)), (1, 5, None)];
    assert!(sig_compatible(&old, &new));
    // A different segment id is never a continuation.
    let bad_seg = [(1u32, 0u64, Some(5)), (2, 5, None)];
    assert!(!sig_compatible(&old, &bad_seg));
    // A span may not vanish.
    let old2 = [(0u32, 0u64, Some(5)), (1, 5, None)];
    let shrunk = [(0u32, 0u64, Some(5))];
    assert!(!sig_compatible(&old2, &shrunk));
    // A sealed cap may not change.
    let changed = [(0u32, 0u64, Some(6)), (1, 6, None)];
    assert!(!sig_compatible(&old2, &changed));
    // Identity.
    assert!(sig_compatible(&old2, &old2));
}

/// Item 87: which remote span refusals end the feed here (typed cutoffs)
/// and which the same bound retries, with the cause it logs.
#[test]
fn remote_span_refusals_split_into_cutoffs_and_retries() {
    use crate::application::read_remote::RemoteSpanError as R;
    let verdict = |refusal| match super::spans::remote_span_verdict(3, refusal) {
        SourceReadError::Fatal(cut) => Ok(cut),
        SourceReadError::Retryable(cause) => Err(cause.to_string()),
    };
    assert_eq!(verdict(R::Unauthorized), Ok(SourceCutoff::FleetAuth));
    assert_eq!(verdict(R::TargetGone), Ok(SourceCutoff::IncarnationChanged));
    assert_eq!(verdict(R::TargetMismatch), Ok(SourceCutoff::TargetMismatch));
    let looped = R::RedirectLoop {
        first: "a".into(),
        second: "b".into(),
    };
    assert_eq!(verdict(looped), Ok(SourceCutoff::RedirectLoop));
    let busy = R::Retryable {
        status: 503,
        code: None,
    };
    assert_eq!(
        verdict(busy),
        Err("remote span 3: retryable 503 None".into())
    );
    let reset = R::Transport("reset".into());
    assert_eq!(verdict(reset), Err("remote span 3: transport reset".into()));
    let garbled = R::InvalidResponse("json".into());
    assert_eq!(
        verdict(garbled),
        Err("remote span 3: invalid response json".into())
    );
    let moved = R::WrongOwner {
        owner: "inst-c".into(),
    };
    assert_eq!(
        verdict(moved),
        Err("remote span 3: unresolved owner inst-c".into())
    );
}

/// The linearization rule: a one-past offset maps to the span covering
/// it, the boundary one-past a sealed cap belongs to the next span at
/// local 0, and the live tail absorbs everything past the last cap.
#[test]
fn linearized_offsets_map_to_the_span_that_covers_them() {
    let spans = [(1u32, 0u64, Some(10u64)), (2, 10, Some(5)), (3, 15, None)];
    let at = |logical_after: u64| {
        let pos = super::spans::locate_in_spans(&spans, logical_after);
        (pos.seg_id, pos.local_after)
    };
    assert_eq!(at(7), (1, 7), "inside the first sealed span");
    assert_eq!(at(10), (2, 0), "the boundary belongs to the next span");
    assert_eq!(at(12), (2, 2), "inside the second sealed span");
    assert_eq!(at(15), (3, 0), "the second cap hands over to the live tail");
    assert_eq!(at(40), (3, 25), "the live tail is open-ended");
}
