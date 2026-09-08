use super::*;
use crate::application::read_retention_probe::Probe;

#[tokio::test]
async fn o2a_subset_selection_compacts_owner_and_full_transfer_keeps_identity() {
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
        .await;
}

#[tokio::test]
async fn o2a_outer_budget_withholding_drops_the_unselected_owner() {
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
        .await;
}
