use super::*;
use crate::postings::{PlanCfg, plan_spans, plan_spans_iter};
use std::sync::Arc;

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
