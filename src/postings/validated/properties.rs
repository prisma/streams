//! Generated inputs reach the production admission/codec, including invalid
//! arrays. Proptest persists minimized failures beside this owner.
use super::*;
use crate::postings::{PostingRun, decode_page, decode_page_abs, encode_page};
use proptest::prelude::*;

#[test]
fn quality_minimized_nonzero_prefix_extension() {
    // Minimized from both extension-boundary mutation failures:
    // start = 1, count = 1, extra = 1.
    let old = ValidatedRuns::new(vec![AbsRun {
        start: 1,
        count: 1,
        matching_bytes: 1,
        gap_bytes_before: 0,
    }])
    .unwrap();
    let fresh = ValidatedRuns::new(vec![AbsRun {
        start: 1,
        count: 2,
        matching_bytes: 2,
        gap_bytes_before: 0,
    }])
    .unwrap();
    let extended = old.extend_after(&fresh, 2).unwrap();
    assert_eq!(
        extended
            .iter()
            .map(|r| (r.start, r.count))
            .collect::<Vec<_>>(),
        [(1, 1), (2, 1)]
    );
    assert_eq!(&*old.extend_after(&old, 2).unwrap(), &*old);
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 1024, .. ProptestConfig::default() })]

    #[test]
    fn quality_valid_page_roundtrip(first in any::<u32>(), specs in prop::collection::vec((0u16..1024, 1u16..1024, 1u16..8192), 1..64)) {
        let runs: Vec<_> = specs.iter().enumerate().map(|(i, &(gap, count, bytes))| PostingRun {
            gap_offsets: if i == 0 { 0 } else { u64::from(gap) },
            record_count: u32::from(count), matching_frame_bytes: u64::from(bytes),
            gap_frame_bytes_before: 0,
        }).collect();
        let encoded = encode_page(u64::from(first), &runs);
        let page = decode_page(&encoded).unwrap();
        prop_assert_eq!(page.first_offset, u64::from(first));
        prop_assert_eq!(page.runs, runs);
        let absolute = decode_page_abs(u64::from(first), &encoded).unwrap();
        prop_assert!(ValidatedRuns::new(absolute).is_some());
        prop_assert!(decode_page_abs(u64::from(first) + 1, &encoded).is_none());
    }

    #[test]
    #[expect(clippy::arithmetic_side_effects, reason = "postings property oracle; generated u32 offsets fit in u64 and 64 u64 terms fit in u128; reusing checked production arithmetic would weaken the independent oracle")]
    fn quality_arbitrary_runs_match_wide_integer_oracle(values in prop::collection::vec((any::<u64>(), any::<u32>(), any::<u64>(), any::<u64>()), 0..64)) {
        let runs: Vec<_> = values.into_iter().map(|(start,count,matching_bytes,gap_bytes_before)| AbsRun {start,count,matching_bytes,gap_bytes_before}).collect();
        let mut previous = 0u128;
        let mut bytes = 0u128;
        let mut valid = true;
        for run in &runs {
            let end = u128::from(run.start) + u128::from(run.count);
            bytes += u128::from(run.matching_bytes);
            if run.gap_bytes_before != GAP_UNKNOWN { bytes += u128::from(run.gap_bytes_before); }
            valid &= run.count > 0 && run.matching_bytes > 0 && u128::from(run.start) >= previous
                && end <= u128::from(u64::MAX) && bytes <= u128::from(u64::MAX);
            previous = end;
        }
        prop_assert_eq!(ValidatedRuns::new(runs).is_some(), valid);
    }

    #[test]
    #[expect(clippy::arithmetic_side_effects, reason = "postings property oracle; generated u32 offsets fit in u64 and 64 u64 terms fit in u128; reusing checked production arithmetic would weaken the independent oracle")]
    fn quality_zero_overflow_overlap_are_rejected(start in any::<u32>(), bytes in 1u64..u64::MAX) {
        let valid = AbsRun { start:u64::from(start), count:2, matching_bytes:bytes, gap_bytes_before:0 };
        prop_assert!(ValidatedRuns::new(vec![AbsRun {count:0,..valid}]).is_none(), "invalid run was accepted");
        prop_assert!(ValidatedRuns::new(vec![AbsRun {start:u64::MAX,..valid}]).is_none(), "invalid run was accepted");
        prop_assert!(ValidatedRuns::new(vec![valid,AbsRun {start:valid.start + 1,..valid}]).is_none(), "invalid run was accepted");
        prop_assert!(ValidatedRuns::new(vec![AbsRun {matching_bytes:u64::MAX,..valid},AbsRun {start:valid.start + 2,..valid}]).is_none(), "invalid run was accepted");
    }

    #[test]
    fn quality_truncation_and_trailing_data_fail(count in 1u32..10000, bytes in 1u64..100000, cut in 0usize..34) {
        let encoded = encode_page(7, &[PostingRun {gap_offsets:0,record_count:count,matching_frame_bytes:bytes,gap_frame_bytes_before:0}]);
        prop_assert!(decode_page(encoded.get(..cut.min(encoded.len().saturating_sub(1))).unwrap()).is_none());
        let mut trailing = encoded;
        trailing.push(0);
        prop_assert!(decode_page(&trailing).is_none());
    }

    #[test]
    fn quality_arbitrary_decoder_bytes_never_panic(raw in prop::collection::vec(any::<u8>(), 0..4096)) {
        if let Some(page) = decode_page(&raw) {
            prop_assert!(!page.runs.is_empty());
            prop_assert!(page.last_offset_exclusive > page.first_offset);
            if let Some(runs) = decode_page_abs(page.first_offset, &raw) {
                prop_assert!(ValidatedRuns::new(runs).is_some());
            }
        }
    }
    #[test]
    fn quality_nonzero_prefix_extension_keeps_both_intervals(start in 1u32..u32::MAX, count in 1u16..1024, extra in 1u16..1024) {
        let start = u64::from(start);
        let count = u32::from(count);
        let extra = u32::from(extra);
        let cut = start.checked_add(u64::from(count)).unwrap();
        let old = ValidatedRuns::new(vec![AbsRun { start, count, matching_bytes:1, gap_bytes_before:0 }]).unwrap();
        let fresh = ValidatedRuns::new(vec![AbsRun { start, count:count.checked_add(extra).unwrap(), matching_bytes:2, gap_bytes_before:0 }]).unwrap();
        let extended = old.extend_after(&fresh, cut).unwrap();
        prop_assert_eq!(extended.iter().map(|run| (run.start, run.count)).collect::<Vec<_>>(), vec![(start,count),(cut,extra)]);
        let prefix_only = old.extend_after(&old, cut).unwrap();
        prop_assert_eq!(&*prefix_only, &*old);
    }

}
