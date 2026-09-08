use super::*;
use crate::application::read_retention_probe::Probe;
use futures_util::FutureExt;

#[test]
fn o2a_full_append_then_selection_preserves_block_ranges() {
    let probe = Probe::default();
    probe
        .scope(async {
            let mut assembled = PlainBatch::default();
            assert!(assembled.is_empty());
            assert!(assembled.contiguous().is_none());
            assembled.push_decoded(
                b"ab".to_vec(),
                [(0, 0..1, "keep".into()), (1, 1..2, "keep".into())],
            );
            assert!(!assembled.is_empty());
            let mut incoming = PlainBatch::default();
            incoming.push_decoded(
                b"cde".to_vec(),
                [
                    (2, 0..1, "keep".into()),
                    (3, 1..2, "keep".into()),
                    (4, 2..3, "keep".into()),
                ],
            );
            incoming.push_decoded(b"z".to_vec(), [(5, 0..1, "drop".into())]);
            let admission =
                assembled.append_selected(incoming, 2..6, None, &mut PageBudget::new(100), 10);
            assert_eq!(admission.last, Some(5));
            assert!(admission.withheld.is_none());
            assert_eq!(assembled.len(), 6);
            assert_eq!(assembled.retained_capacity(), 6);
            assert_eq!(probe.live(), 6);
            // Partial selection after concatenation exercises the transferred
            // block coordinates, including a complete three-record middle block.
            let mut selected = PlainBatch::default();
            let admission = selected.append_selected(
                assembled,
                1..15,
                Some("keep"),
                &mut PageBudget::new(100),
                0,
            );
            assert_eq!(admission.last, Some(14));
            assert!(admission.withheld.is_none());
            assert_eq!(
                (&selected).into_iter().map(|r| r.off).collect::<Vec<_>>(),
                [1, 12, 13, 14]
            );
            assert_eq!(
                (&selected)
                    .into_iter()
                    .flat_map(|r| r.payload.iter().copied())
                    .collect::<Vec<_>>(),
                b"bcde"
            );
            assert_eq!(selected.retained_capacity(), 4);
            assert_eq!(probe.live(), 4);
            assert!(selected.contiguous().is_none());
            drop(selected);
            assert_eq!(probe.live(), 0);
        })
        .now_or_never()
        .expect("pure ownership scope does not suspend");
}

#[test]
fn o2a_subset_selection_compacts_owner_and_full_transfer_keeps_identity() {
    let probe = Probe::default();
    probe
        .scope(async {
            let mut source = PlainBatch::default();
            let mut bytes = vec![b'x'; 1 << 20];
            bytes[0] = b'a';
            bytes[(1 << 20) - 1] = b'z';
            source.push_decoded(
                bytes,
                vec![
                    (0, 0..1, "keep".into()),
                    (1, 1..(1 << 20) - 1, "drop".into()),
                    (2, (1 << 20) - 1..1 << 20, "keep".into()),
                ],
            );
            let original = source.contiguous().unwrap();
            let mut subset = PlainBatch::default();
            let admitted = subset.append_selected(
                source,
                0..3,
                Some("keep"),
                &mut PageBudget::new(8 << 20),
                10,
            );
            assert!(admitted.withheld.is_none());
            assert_eq!(admitted.last, Some(2));
            assert_eq!(subset.iter().map(|r| r.off).collect::<Vec<_>>(), [10, 12]);
            assert_eq!(subset.contiguous().unwrap().as_ref(), b"az");
            assert_eq!(subset.retained_capacity(), 2);
            assert_ne!(subset.contiguous().unwrap().as_ptr(), original.as_ptr());
            assert_eq!(
                probe.live(),
                (1 << 20) + 2,
                "the held original remains charged exactly once"
            );
            drop(original);
            assert_eq!(probe.live(), 2);
            let pointer = subset.contiguous().unwrap().as_ptr();
            let mut complete = PlainBatch::default();
            complete.append_selected(subset, 0..20, None, &mut PageBudget::new(10), 0);
            assert_eq!(
                complete.contiguous().unwrap().as_ptr(),
                pointer,
                "full-batch transfer must share the same owner"
            );
            let body = complete.contiguous().unwrap();
            drop(complete);
            assert_eq!(probe.live(), 2);
            drop(body);
            assert_eq!(probe.live(), 0);
        })
        .now_or_never()
        .expect("pure ownership scope does not suspend");
}

#[test]
fn o2a_outer_budget_withholding_drops_the_unselected_owner() {
    let probe = Probe::default();
    probe
        .scope(async {
            let mut output = PlainBatch::default();
            let mut budget = PageBudget::new(3);
            assert!(output.admit_owned(0, vec![b'a'; 2], String::new(), &mut budget));
            let mut source = PlainBatch::default();
            source.push_decoded(
                vec![b'b'; 1 << 20],
                vec![(1, 0..1, String::new()), (2, 1..1 << 20, String::new())],
            );
            let admitted = output.append_selected(source, 1..3, None, &mut budget, 0);
            assert_eq!(admitted.withheld, Some(2));
            assert_eq!(output.retained_capacity(), 3);
            assert_eq!(probe.live(), 3);
            drop(output);
            assert_eq!(probe.live(), 0);
        })
        .now_or_never()
        .expect("pure ownership scope does not suspend");
}

use proptest::prelude::*;
proptest! {
    #![proptest_config(ProptestConfig { cases: 1024, .. ProptestConfig::default() })]
    #[test]
    fn quality_generated_selection_charges_only_owned_capacity(sizes in prop::collection::vec(1usize..128, 1..32), requested in 1usize..2048) {
        let probe = Probe::default();
        probe.scope(async {
            let mut source = PlainBatch::default();
            let mut source_budget = PageBudget::new(8 << 20);
            for (offset, size) in sizes.iter().enumerate() {
                assert!(source.admit_owned(u64::try_from(offset).unwrap(), vec![0; *size], "k".to_owned(), &mut source_budget));
            }
            let mut expected = 0usize;
            let mut count = 0usize;
            for size in sizes {
                let next = expected.saturating_add(size);
                if count > 0 && next > requested { break; }
                expected = next;
                count = count.saturating_add(1);
            }
            let mut result = PlainBatch::default();
            result.append_selected(source, 0..u64::MAX, None, &mut PageBudget::new(requested), 0);
            assert_eq!(result.len(), count);
            assert_eq!(result.retained_capacity(), expected);
            assert_eq!(probe.live(), expected);
            drop(result);
            assert_eq!(probe.live(), 0);
        }).now_or_never().unwrap();
    }
}
