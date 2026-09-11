use super::*;
use crate::crypto::SegmentHash;

/// Round-13 CODE-RED unit red: an install run STRADDLING the
/// slice's extension cut must contribute its [cut, end) tail —
/// the old filter dropped the whole run and the slice then proved
/// a match-free hole over durable records (11 lost in field leg
/// A1v2).
#[test]
fn straddling_install_run_is_split_not_dropped() {
    let cache = Arc::new(PostingsCache::new(1 << 20));
    let inc = SegmentHash([9u8; 16]);
    let kh = crate::postings::rk_hash("");
    // Seed a slice covering [0, 59).
    cache.install_chunk(
        inc,
        0,
        59,
        vec![(
            kh.0,
            vec![AbsRun {
                start: 0,
                count: 59,
                matching_bytes: 59 * 100,
                gap_bytes_before: 0,
            }],
        )],
    );
    // Extend with a chunk whose run STRADDLES the cut: [50, 90).
    cache.install_chunk(
        inc,
        59,
        90,
        vec![(
            kh.0,
            vec![AbsRun {
                start: 50,
                count: 40,
                matching_bytes: 40 * 100,
                gap_bytes_before: 0,
            }],
        )],
    );
    let covered: Vec<(u64, u64)> = cache
        .runs_for_test(inc, kh)
        .iter()
        .map(|r| (r.start, r.start + r.count as u64))
        .collect();
    let holds = |q: u64| covered.iter().any(|(a, b)| q >= *a && q < *b);
    for q in 0..90 {
        assert!(
            holds(q),
            "offset {q} lost by the straddle drop: {covered:?}"
        );
    }
}
