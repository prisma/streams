use super::*;
use crate::postings::{PlanCfg, plan_spans, plan_spans_iter};
use std::sync::Arc;

#[expect(
    clippy::arithmetic_side_effects,
    clippy::cast_possible_truncation,
    reason = "old_clip; the reference clipper reproduces the pre-owner arithmetic on the fixture's small runs; saturating or checked forms would change the oracle it exists to reproduce"
)]
fn old_clip(runs: &[AbsRun], from: u64, upto: u64) -> Vec<AbsRun> {
    runs.iter()
        .filter_map(|r| {
            let start = r.start.max(from);
            let end = (r.start + r.count as u64).min(upto);
            (start < end).then(|| AbsRun {
                start,
                count: (end - start) as u32,
                ..*r
            })
        })
        .collect()
}

#[expect(
    clippy::cast_possible_truncation,
    reason = "o4_window_matches_double_clipping_and_every_plan_field; the fixture's run counts are small numbers it generated; a checked conversion would only restate the fixture"
)]
#[test]
fn o4_window_matches_double_clipping_and_every_plan_field() {
    let mut seed = 0x04a7e2070u64;
    let mut next = || {
        seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
        seed
    };
    for case in 0..2048 {
        let mut runs = Vec::new();
        let mut offset = 0u64;
        for _ in 0..case % 129 {
            offset += next() % 32;
            let count = (next() % 64 + 1) as u32;
            runs.push(AbsRun {
                start: offset,
                count,
                matching_bytes: (next() % 65536 + 1) * count as u64,
                gap_bytes_before: if next() % 7 == 0 {
                    GAP_UNKNOWN
                } else {
                    next() % 131072
                },
            });
            offset += count as u64;
        }
        let runs = ValidatedRuns::new(runs).unwrap();
        for _ in 0..20 {
            let from = next() % (offset + 2);
            let upto = from + next() % (offset + 2);
            let provable_to = from + next() % (upto - from + 1);
            let cfg = PlanCfg {
                max_spans: (next() % 9) as usize,
                max_scan_bytes: next() % (8 * 1024 * 1024),
                max_gap_bytes: next() % 131072,
                ..Default::default()
            };
            let old = old_clip(&old_clip(&runs, from, provable_to), from, provable_to);
            let window = RunWindow::new(runs.clone(), from, provable_to);
            assert!(Arc::ptr_eq(&runs.0, &window.owner.0));
            assert_eq!(window.iter().collect::<Vec<_>>(), old);
            assert_eq!(
                plan_spans_iter(window.iter(), provable_to, &cfg),
                plan_spans(&old, provable_to, &cfg),
                "case {case}: {from}..{provable_to}/{upto}"
            );
        }
    }
}

#[test]
fn o4_late_window_seeks_and_retains_whole_run_estimates() {
    let owner = ValidatedRuns::new(
        (0..100_000)
            .map(|i| AbsRun {
                start: i * 16,
                count: 8,
                matching_bytes: 8192,
                gap_bytes_before: 4096,
            })
            .collect::<Vec<_>>(),
    )
    .unwrap();
    let window = RunWindow::new(owner.clone(), 99_990 * 16 + 3, 99_992 * 16 + 1);
    assert!(Arc::ptr_eq(&window.owner.0, &owner.0));
    assert_eq!(window.indices, 99_990..99_993);
    let selected = window.iter().collect::<Vec<_>>();
    assert_eq!(
        selected.iter().map(|r| r.count).collect::<Vec<_>>(),
        vec![5, 8, 1]
    );
    assert!(
        selected
            .iter()
            .all(|r| r.matching_bytes == 8192 && r.gap_bytes_before == 4096)
    );
}

#[expect(
    clippy::indexing_slicing,
    reason = "o4a_owner_rejects_invalid_runs_and_preserves_valid_extension; the fixture indexes the two runs it just extended; a checked index would only restate the length it asserted"
)]
#[test]
fn o4a_owner_rejects_invalid_runs_and_preserves_valid_extension() {
    let r = |start, count, bytes| AbsRun {
        start,
        count,
        matching_bytes: bytes,
        gap_bytes_before: 0,
    };
    for runs in [
        vec![r(0, 0, 1)],
        vec![r(u64::MAX, 1, 1)],
        vec![r(0, 100, 100), r(10, 1, 1)],
        vec![r(0, 1, u64::MAX), r(1, 1, 1)],
    ] {
        assert!(ValidatedRuns::new(runs).is_none());
    }
    let old = ValidatedRuns::new(vec![r(0, 59, 5900)]).unwrap();
    let fresh = ValidatedRuns::new(vec![r(50, 40, 4000)]).unwrap();
    let extended = old.extend_after(&fresh, 59).unwrap();
    assert_eq!(
        extended
            .iter()
            .map(|r| (r.start, r.count))
            .collect::<Vec<_>>(),
        vec![(0, 59), (59, 31)]
    );
    assert_eq!(
        extended[1].matching_bytes, 4000,
        "retain conservative whole-run weight"
    );
    assert!(old.extend_after(&fresh, 58).is_none());
}

/// The proven prefix must end at or before the cut: a prefix reaching past
/// it is refused even when the fresh runs start beyond it and would not
/// overlap, and a prefix ending before it extends over the gap.
#[test]
fn extend_after_refuses_a_prefix_past_the_cut_and_accepts_one_before_it() {
    let run = |start: u64, count: u32| AbsRun {
        start,
        count,
        matching_bytes: u64::from(count) * 8,
        gap_bytes_before: 0,
    };
    let fresh = ValidatedRuns::new(vec![run(20, 2)]).expect("fresh runs validate");
    let past = ValidatedRuns::new(vec![run(0, 10)]).expect("prefix validates");
    assert!(
        past.extend_after(&fresh, 5).is_none(),
        "a prefix reaching past the cut is refused"
    );
    let before = ValidatedRuns::new(vec![run(0, 4)]).expect("prefix validates");
    let extended = before
        .extend_after(&fresh, 8)
        .expect("a prefix ending before the cut extends");
    assert_eq!(extended.len(), 2);
    assert_eq!(extended.last().map(|r| r.start), Some(20));
}

/// `clipped_to` keeps exactly the offsets below the cut, returns the same
/// allocation when nothing crosses it, keeps a straddler's whole-run weight,
/// and leaves a prefix `extend_after` accepts at that same cut.
#[test]
fn clipped_to_cuts_at_the_boundary_and_keeps_whole_run_weight() {
    let r = |start, count, bytes| AbsRun {
        start,
        count,
        matching_bytes: bytes,
        gap_bytes_before: 0,
    };
    let owner = ValidatedRuns::new(vec![r(0, 10, 1000), r(20, 10, 2000), r(40, 10, 3000)])
        .expect("runs validate");
    let shape = |runs: &ValidatedRuns| {
        runs.iter()
            .map(|x| (x.start, x.count, x.matching_bytes))
            .collect::<Vec<_>>()
    };
    for cut in [50, 51, u64::MAX] {
        assert!(Arc::ptr_eq(&owner.clone().clipped_to(cut).0, &owner.0));
    }
    assert_eq!(
        shape(&owner.clone().clipped_to(49)),
        vec![(0, 10, 1000), (20, 10, 2000), (40, 9, 3000)]
    );
    assert_eq!(
        shape(&owner.clone().clipped_to(40)),
        vec![(0, 10, 1000), (20, 10, 2000)]
    );
    assert_eq!(
        shape(&owner.clone().clipped_to(25)),
        vec![(0, 10, 1000), (20, 5, 2000)]
    );
    assert!(owner.clone().clipped_to(0).is_empty());
    assert!(ValidatedRuns::empty().clipped_to(7).is_empty());
    let rejoined = owner
        .clone()
        .clipped_to(25)
        .extend_after(&owner, 25)
        .expect("a clipped prefix seams at its cut");
    assert_eq!(
        rejoined
            .iter()
            .map(|x| (x.start, x.count))
            .collect::<Vec<_>>(),
        vec![(0, 10), (20, 5), (25, 5), (40, 10)]
    );
}

proptest::proptest! {
    #![proptest_config(proptest::test_runner::Config::with_cases(1024))]

    #[test]
    #[expect(clippy::arithmetic_side_effects, reason = "clip property oracle; 48 generated gaps and counts below 64 stay far below u64::MAX; reusing checked production arithmetic would weaken the independent oracle")]
    fn quality_clipped_runs_are_exactly_the_offsets_below_the_cut(
        specs in proptest::collection::vec((0u64..64, 1u32..64, 1u64..4096), 0..48),
        cut in 0u64..6200,
    ) {
        let mut next = 0u64;
        let runs: Vec<AbsRun> = specs.iter().map(|&(gap, count, matching_bytes)| {
            let start = next + gap;
            next = start + u64::from(count);
            AbsRun { start, count, matching_bytes, gap_bytes_before: 0 }
        }).collect();
        let offsets = |rs: &[AbsRun]| rs.iter().flat_map(|r| r.start..r.start + u64::from(r.count)).collect::<Vec<u64>>();
        let owner = ValidatedRuns::new(runs.clone()).unwrap();
        let clipped = owner.clone().clipped_to(cut);
        proptest::prop_assert!(ValidatedRuns::new(clipped.to_vec()).is_some(), "a clip must stay admissible");
        let below: Vec<u64> = offsets(&runs).into_iter().filter(|o| *o < cut).collect();
        proptest::prop_assert_eq!(offsets(&clipped), below);
        proptest::prop_assert!(clipped.iter().all(|c| runs.iter().any(|r| r.start == c.start && r.matching_bytes == c.matching_bytes)), "whole-run weight");
        let rejoined = clipped.extend_after(&owner, cut).unwrap();
        proptest::prop_assert_eq!(offsets(&rejoined), offsets(&runs));
    }
}
