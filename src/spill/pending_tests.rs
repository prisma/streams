//! The pending rows' merge operator (layout 6, b1).
#![cfg(test)]
use bytes::Bytes;
use slatedb::MergeOperator;

use super::pending::{PendingMerge, pending_key};

#[test]
fn pending_rows_concatenate_oldest_first_and_associatively() {
    let op = PendingMerge;
    let key = Bytes::from(pending_key(&[9u8; 16]));
    let p = Bytes::from_static;
    let one = op
        .merge_batch(&key, Some(p(b"ab")), &[p(b"c"), p(b"de")])
        .unwrap();
    assert_eq!(&one[..], b"abcde");
    let ab = op.merge_batch(&key, Some(p(b"ab")), &[p(b"c")]).unwrap();
    let left = op.merge_batch(&key, Some(ab), &[p(b"de")]).unwrap();
    let cd = op.merge_batch(&key, None, &[p(b"c"), p(b"de")]).unwrap();
    let right = op.merge_batch(&key, Some(p(b"ab")), &[cd]).unwrap();
    assert_eq!((left, right), (one.clone(), one));
    assert_eq!(op.merge(&key, None, p(b"x")).unwrap(), p(b"x"));
    assert!(op.merge_batch(&key, None, &[]).is_err(), "an empty batch");
}

#[test]
fn a_merge_on_any_other_key_is_refused() {
    let op = PendingMerge;
    let x = [Bytes::from_static(b"x")];
    for other in [
        crate::shard::tail_key(&[9u8; 16]),
        crate::shard::seal_fence_key(&[9u8; 16]),
        crate::shard::queue_tail_key(&[9u8; 16]),
        pending_key(&[9u8; 16])[1..].to_vec(),
    ] {
        assert!(op.merge_batch(&Bytes::from(other), None, &x).is_err());
    }
}
