//! An invalid install cannot publish a warm absence proof.
#![cfg(test)]
use super::*;
#[test]
fn o4a_invalid_install_cannot_publish_a_warm_absence_proof() {
    let cache = PostingsCache::new(1 << 20);
    let inc = SegmentHash([87; 16]);
    let kh = crate::postings::rk_hash("wanted");
    let run = |start, count| AbsRun {
        start,
        count,
        matching_bytes: 100,
        gap_bytes_before: 0,
    };
    cache.install_chunk(inc, 0, 10, vec![(kh.0, vec![run(0, 1)])]);
    cache.install_chunk(inc, 10, 100, vec![(kh.0, vec![run(10, 90), run(20, 1)])]);
    let inner = cache.inner.lock().unwrap();
    assert!(!inner.warm.contains_key(&inc.0));
    assert!(inner.slices.is_empty());
    assert_eq!(inner.total_bytes, 0);
}
