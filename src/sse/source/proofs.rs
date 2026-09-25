//! Kani proofs for the lineage's linearization rule: KANI-005. The harness
//! calls the production `locate_in_spans` over lineages shaped as
//! `Lineage::build` (`src/sse/source.rs`) makes them: one to four spans, the
//! first at logical 0, each next one starting where the previous sealed span
//! ends, every span sealed but the tail, which may be live, and no start
//! plus cap past `u64::MAX` (ASM-LINEAGE-CONTRACT). The properties are
//! positional, not a restatement of the loop: the answer names a span that
//! starts at or before the position, its local offset is relative to that
//! span's start (so `logicalize` inverts it), a sealed span below the tail
//! holds only positions before its end, and no earlier sealed span still
//! holds the position. Kani's overflow and underflow checks stay on, so an
//! unchecked `start + cap` or `logical_after - start` is reported as well.
use super::spans::locate_in_spans;

/// At most four spans, so `unwind(6)` covers every loop.
const MAX_SPANS: u32 = 4;

/// `(seg_id, logical_start, cap)` as the constructor lays spans out; span
/// `i` has segment id `i + 1`, so an answer names its span.
fn constructed_lineage() -> Vec<(u32, u64, Option<u64>)> {
    let len: u32 = kani::any_where(|len: &u32| (1..=MAX_SPANS).contains(len));
    let mut spans = Vec::with_capacity(MAX_SPANS as usize);
    let mut logical: u64 = 0;
    for seg in 1..=len {
        let cap: Option<u64> = if seg == len {
            kani::any()
        } else {
            Some(kani::any())
        };
        spans.push((seg, logical, cap));
        if let Some(cap) = cap {
            let next = logical.checked_add(cap);
            kani::assume(next.is_some());
            logical = next.unwrap_or(logical);
        }
    }
    spans
}

/// KANI-005: every position maps to exactly the span the linearization
/// rule names, with the one-past boundary of a sealed span belonging to
/// the next span at local zero and the tail absorbing everything past its
/// start.
#[kani::proof]
#[kani::unwind(6)]
fn kani_005_a_position_maps_to_the_span_that_holds_it() {
    let spans = constructed_lineage();
    let logical_after: u64 = kani::any();
    let answer = locate_in_spans(&spans, logical_after);
    let index = spans.iter().position(|(seg, _, _)| *seg == answer.seg_id);
    assert!(index.is_some(), "the answer names a span of the lineage");
    let index = index.unwrap_or(0);
    let (_, start, cap) = spans[index];
    let tail = index + 1 == spans.len();
    assert!(
        start <= logical_after,
        "the answer's span starts at or before the position"
    );
    assert!(
        answer.local_after == logical_after - start,
        "the local offset is relative to its span's start"
    );
    if let (Some(cap), false) = (cap, tail) {
        assert!(
            logical_after - start < cap,
            "a sealed span below the tail holds only positions before its end"
        );
    }
    assert!(
        spans[..index]
            .iter()
            .all(|(_, start, cap)| cap.is_some_and(|cap| logical_after - start >= cap)),
        "no earlier sealed span still holds the position"
    );
    kani::cover!(
        index > 0 && answer.local_after == 0 && spans[index - 1].2.is_some(),
        "a sealed span's one-past boundary belongs to the next span at local zero"
    );
    kani::cover!(
        tail && cap.is_some_and(|cap| answer.local_after >= cap),
        "a sealed tail absorbs positions past its end"
    );
    kani::cover!(tail && cap.is_none(), "a live tail holds the position");
    kani::cover!(
        spans.len() == MAX_SPANS as usize,
        "a four-span lineage is located"
    );
}
