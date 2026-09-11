//! The postings codec's and span planner's tests.
#![cfg(test)]
use super::{
    AbsRun, BUCKET_OFFSETS, GAP_UNKNOWN, PAGE_MAX_ENCODED_BYTES, PageBuilder, PlanCfg, PostingRun,
    RoutingKeyHash, decode_page, decode_page_abs, encode_page, get_varint, plan_spans, put_varint,
};

fn run(start: u64, count: u32, bytes: u64, gap_bytes: u64) -> AbsRun {
    AbsRun {
        start,
        count,
        matching_bytes: bytes,
        gap_bytes_before: gap_bytes,
    }
}

/// A page holds every run of its bucket until the encoded-size cap:
/// three gapped runs in one bucket are one page, not three.
#[test]
fn small_pages_keep_their_runs_together() {
    let key = RoutingKeyHash([7; 16]);
    let mut builder = PageBuilder::default();
    for offset in [0, 10, 20] {
        builder.note_frame(key, offset, 100);
    }
    let (pages, _) = builder.finish();
    assert_eq!(pages.len(), 1, "one page per bucket below the cap");
    let (_, _, first, encoded) = pages.first().expect("one page");
    assert_eq!(*first, 0);
    assert_eq!(decode_page(encoded).map(|page| page.runs.len()), Some(3));
}

/// A bucket whose gapped runs would exceed the encoded-size cap is split
/// into more than one page at a run boundary, and every page decodes.
#[test]
fn a_full_page_splits_at_the_next_run_boundary() {
    let key = RoutingKeyHash([9; 16]);
    let mut builder = PageBuilder::default();
    let runs = 2_800u64;
    for i in 0..runs {
        // Three contiguous frames per run: the page fills while a
        // run is open, and the split must wait for the next gap.
        for frame in 0..3 {
            builder.note_frame(key, i * 4 + frame, 64);
        }
    }
    let (pages, _) = builder.finish();
    assert!(
        pages.len() >= 2,
        "the cap splits the bucket: {} pages",
        pages.len()
    );
    let decoded: usize = pages
        .iter()
        .map(|(_, _, _, encoded)| {
            assert!(encoded.len() <= PAGE_MAX_ENCODED_BYTES);
            let page = decode_page(encoded).expect("every page decodes");
            assert!(
                page.runs.iter().all(|run| run.record_count == 3),
                "no run is cut in two by the page cap"
            );
            page.runs.len()
        })
        .sum();
    assert_eq!(decoded as u64, runs);
}

/// A known gap wider than max_gap_bytes opens a new span even when the
/// amplification ratio would have allowed the coalesce.
#[test]
fn a_gap_past_the_byte_cap_opens_a_new_span() {
    let cfg = PlanCfg::default();
    let wide = cfg.max_gap_bytes + 1;
    let plan = plan_spans(
        &[run(0, 1, 1 << 20, 0), run(100, 1, 1 << 20, wide)],
        101,
        &cfg,
    );
    assert_eq!(plan.spans.len(), 2, "the gap exceeds max_gap_bytes");
    assert!(plan.complete);
}

/// A run that exactly fills the scan budget is planned in full; only a
/// run that exceeds it is cut to a bounded prefix.
#[test]
fn a_run_filling_the_scan_budget_exactly_is_complete() {
    let cfg = PlanCfg {
        max_scan_bytes: 4096,
        ..PlanCfg::default()
    };
    let plan = plan_spans(&[run(0, 4, 4096, 0)], 4, &cfg);
    assert_eq!(plan.spans.len(), 1);
    assert!(plan.complete, "exactly the budget is not over budget");
    assert_eq!(plan.consumed_to, 4);
    let over = plan_spans(&[run(0, 4, 4097, 0)], 4, &cfg);
    assert!(
        !over.complete,
        "one byte over the budget is a bounded prefix"
    );
}

/// Review finding 6: tiny matches around a large-but-under-64KiB
/// gap must NOT coalesce into one high-amplification scan — the
/// hard 4x (and target 2x) ratio gates it.
#[test]
fn amplification_bound_refuses_expensive_gaps() {
    let cfg = PlanCfg::default();
    // The reviewer's example: 1 KiB + 60 KiB gap + 1 KiB = ~31x.
    let plan = plan_spans(
        &[run(0, 1, 1024, 0), run(100, 1, 1024, 60 * 1024)],
        101,
        &cfg,
    );
    assert_eq!(plan.spans.len(), 2, "exact spans, not one 62 KiB scan");
    assert!(plan.complete);
    for s in &plan.spans {
        assert!(
            (s.scan_bytes as f64) <= (s.matching_bytes as f64) * cfg.hard_amplification,
            "span amp {}/{} exceeds hard bound",
            s.scan_bytes,
            s.matching_bytes,
        );
    }

    // A cheap gap (ratio under target) still coalesces.
    let plan = plan_spans(&[run(0, 4, 4096, 0), run(100, 4, 4096, 2048)], 104, &cfg);
    assert_eq!(plan.spans.len(), 1, "cheap gap coalesces");
    assert!(plan.complete);
}

/// One tiny match per postings page across many pages: every span
/// stays exact and the plan stays span-bounded with honest partials.
#[test]
fn fragmented_key_stays_exact_and_bounded() {
    let cfg = PlanCfg::default();
    let mut runs = Vec::new();
    for i in 0..12u64 {
        // 256 B matches separated by 32 KiB gaps: 129x if coalesced.
        runs.push(run(i * 1000, 1, 256, if i == 0 { 0 } else { 32 * 1024 }));
    }
    let plan = plan_spans(&runs, 12_000, &cfg);
    assert_eq!(plan.spans.len(), cfg.max_spans, "span-bounded");
    assert!(!plan.complete, "honest partial past the span budget");
    assert_eq!(
        plan.consumed_to, 7_001,
        "cursor covers exactly the planned prefix"
    );
    for s in &plan.spans {
        assert_eq!(s.scan_bytes, s.matching_bytes, "every span exact");
    }
}

/// Contiguous runs never trip the amplification guard.
#[test]
fn contiguous_runs_coalesce_regardless_of_amp() {
    let cfg = PlanCfg::default();
    let plan = plan_spans(&[run(0, 2, 200, 0), run(2, 2, 200, GAP_UNKNOWN)], 4, &cfg);
    assert_eq!(plan.spans.len(), 1);
    assert!(plan.complete);
}

/// Review blocker: a first run fatter than the whole scan budget
/// must still plan a bounded prefix — never zero spans.
#[test]
fn oversized_first_run_plans_bounded_prefix() {
    let cfg = PlanCfg {
        max_scan_bytes: 16 * 1024 * 1024,
        ..Default::default()
    };
    // One 32 MiB record: budget/record floor still allows exactly it.
    let plan = plan_spans(&[run(10, 1, 32 * 1024 * 1024, 0)], 11, &cfg);
    let [span] = plan.spans.as_slice() else {
        panic!("the oversized first record must produce exactly one span");
    };
    assert_eq!((span.start, span.end), (10, 11));
    assert!(!plan.complete);
    assert_eq!(plan.consumed_to, 11);

    // A 24 MiB contiguous run of 1 MiB records: ~16 records fit.
    let plan = plan_spans(&[run(0, 24, 24 * 1024 * 1024, 0)], 24, &cfg);
    let [span] = plan.spans.as_slice() else {
        panic!("a bounded prefix must produce exactly one span");
    };
    assert_eq!((span.start, span.end), (0, 16), "budget/per-record prefix");
    assert!(!plan.complete);
    assert_eq!(plan.consumed_to, 16, "cursor advances to the prefix end");
}

/// A fat run AFTER planned spans returns an honest partial whose
/// consumed_to covers the planned prefix only.
#[test]
fn oversized_mid_run_partial_keeps_progress() {
    let cfg = PlanCfg {
        max_scan_bytes: 1024,
        ..Default::default()
    };
    let plan = plan_spans(
        &[run(0, 2, 400, 0), run(100, 1, 10_000, GAP_UNKNOWN)],
        101,
        &cfg,
    );
    assert_eq!(plan.spans.len(), 1);
    assert!(!plan.complete);
    assert_eq!(plan.consumed_to, 2, "progress = what was actually planned");
}

/// No runs in the window: the plan consumes the whole match-free
/// range (overall completion is the caller's provable_to gate).
#[test]
fn empty_window_consumes_range() {
    let plan = plan_spans(&[], 500, &PlanCfg::default());
    assert!(plan.spans.is_empty());
    assert!(plan.complete);
    assert_eq!(plan.consumed_to, 500);
}

#[test]
fn varint_roundtrip_edges() {
    for x in [0u64, 1, 127, 128, 300, u32::MAX as u64, u64::MAX] {
        let mut v = Vec::new();
        put_varint(&mut v, x);
        let mut input = v.as_slice();
        assert_eq!(get_varint(&mut input), Some(x));
        assert!(input.is_empty());
    }
}

#[test]
fn page_codec_roundtrip() {
    let runs = vec![
        PostingRun {
            gap_offsets: 0,
            record_count: 3,
            matching_frame_bytes: 900,
            gap_frame_bytes_before: 0,
        },
        PostingRun {
            gap_offsets: 41,
            record_count: 1,
            matching_frame_bytes: 128,
            gap_frame_bytes_before: 17_000,
        },
    ];
    let v = encode_page(1000, &runs);
    let page = decode_page(&v).unwrap();
    assert_eq!(page.runs, runs);
    assert_eq!(page.first_offset, 1000);
    assert_eq!(page.last_offset_exclusive, 1000 + 3 + 41 + 1);
    assert_eq!(page.matching_frame_bytes, 1028);
    let abs = decode_page_abs(1000, &v).unwrap();
    assert_eq!(abs, [run(1000, 3, 900, 0), run(1044, 1, 128, 17_000)]);
    // Key/header disagreement = corruption.
    assert!(decode_page_abs(999, &v).is_none());
    // Header/runs disagreement = corruption.
    let mut bad = v;
    let (_, [last_byte, ..]) = bad.split_at_mut(10) else {
        panic!("the encoded page must contain its last-offset header");
    };
    *last_byte ^= 1; // perturb last_offset_exclusive
    assert!(decode_page(&bad).is_none());
}

#[test]
fn builder_splits_runs_and_buckets() {
    let ka = RoutingKeyHash([1u8; 16]);
    let kb = RoutingKeyHash([2u8; 16]);
    let mut b = PageBuilder::default();
    // Interleaved: a a b a  | gap |  a, then a crosses a bucket edge.
    b.note_frame(ka, 10, 100);
    b.note_frame(ka, 11, 100);
    b.note_frame(kb, 12, 50);
    b.note_frame(ka, 13, 100);
    b.note_frame(kb, 20, 60);
    let edge = BUCKET_OFFSETS;
    b.note_frame(ka, edge - 1, 100);
    b.note_frame(ka, edge, 100);
    let (pages, total) = b.finish();
    assert!(total > 0);
    assert_eq!(pages.len(), 3, "two A buckets and one B bucket");
    let a_pages: Vec<_> = pages.iter().filter(|p| p.0 == ka).collect();
    assert_eq!(a_pages.len(), 2, "bucket edge must split the page");
    let p0 = a_pages.iter().find(|p| p.1 == 0).unwrap();
    let abs = decode_page_abs(p0.2, &p0.3).unwrap();
    // Runs for a in bucket 0: [10,12) at 10..11, [13,14), [edge-1,edge).
    // The gaps contain kb@12 (50 bytes) and kb@20 (60 bytes).
    assert_eq!(
        abs,
        [
            run(10, 2, 200, 0),
            run(13, 1, 100, 50),
            run(edge - 1, 1, 100, 60)
        ]
    );
    let p1 = a_pages.iter().find(|p| p.1 == 1).unwrap();
    let abs1 = decode_page_abs(p1.2, &p1.3).unwrap();
    assert_eq!(abs1, [run(edge, 1, 100, 0)]);

    let b_pages: Vec<_> = pages.iter().filter(|p| p.0 == kb).collect();
    let [b_page] = b_pages.as_slice() else {
        panic!("B must occupy exactly one page");
    };
    let abs_b = decode_page_abs(b_page.2, &b_page.3).unwrap();
    // Gap before b@20: frame a@13 (100 bytes).
    assert_eq!(abs_b, [run(12, 1, 50, 0), run(20, 1, 60, 100)]);
}

#[test]
fn cross_page_seams_never_coalesce() {
    let cfg = PlanCfg::default();
    // Two singleton runs from DIFFERENT pages, 20k offsets apart,
    // seam bytes unknown: must be two spans, never one giant scan.
    let runs = vec![
        run(5, 1, 1_000, 0),
        AbsRun {
            start: 20_005,
            count: 1,
            matching_bytes: 1_000,
            gap_bytes_before: GAP_UNKNOWN,
        },
    ];
    let plan = plan_spans(&runs, 40_000, &cfg);
    let [first, second] = plan.spans.as_slice() else {
        panic!("unknown seams must open a second span");
    };
    assert_eq!((first.start, first.end), (5, 6));
    assert_eq!((second.start, second.end), (20_005, 20_006));
    // Truly contiguous across a seam still merges.
    let runs = vec![
        run(5, 1, 1_000, 0),
        AbsRun {
            start: 6,
            count: 1,
            matching_bytes: 1_000,
            gap_bytes_before: GAP_UNKNOWN,
        },
    ];
    let plan = plan_spans(&runs, 40_000, &cfg);
    assert_eq!(plan.spans.len(), 1, "contiguous offsets are one span");
}

#[test]
fn planner_coalesces_cheap_gaps_and_bounds_spans() {
    let cfg = PlanCfg::default();
    // Two runs separated by a tiny gap coalesce into one span.
    let plan = plan_spans(&[run(0, 10, 4_000, 0), run(15, 5, 2_000, 1_000)], 100, &cfg);
    let [span] = plan.spans.as_slice() else {
        panic!("the cheap gap must coalesce into one span");
    };
    assert_eq!((span.start, span.end), (0, 20));
    assert!(plan.complete);
    assert_eq!(plan.consumed_to, 100, "match-free tail is consumed");

    // A big gap opens a second span.
    let plan = plan_spans(
        &[run(0, 10, 4_000, 0), run(50_000, 5, 2_000, 10_000_000)],
        60_000,
        &cfg,
    );
    assert_eq!(plan.spans.len(), 2);

    // Span budget: the ninth distinct run is deferred honestly.
    let mut runs9 = Vec::new();
    for i in 0..9u64 {
        runs9.push(run(i * 1_000_000, 1, 100, 10_000_000));
    }
    let plan = plan_spans(&runs9, 9_000_000, &cfg);
    assert_eq!(plan.spans.len(), 8);
    assert!(!plan.complete);
    assert_eq!(
        plan.consumed_to,
        7 * 1_000_000 + 1,
        "cursor resumes at the first unplanned run"
    );
}
