//! KANI-108 (S6-DESIGN rev 6 §8, `refcount_merge_sums`): a merge's `'R'` row for an object both
//! sources count is the exact sum of theirs, over every input. The chunks and bytes add without
//! wrapping (a sum that would overflow refuses), the earlier creation is kept, and the two must
//! name one length.

use super::{Refcount, merged_refcount};

fn any_refcount() -> Refcount {
    Refcount {
        live_bytes: kani::any(),
        live_chunks: kani::any(),
        created_ms: kani::any(),
        object_len: kani::any(),
    }
}

/// KANI-108: the merged row is the sum, or the merge refuses exactly when the lengths differ or a
/// sum overflows.
#[kani::proof]
fn kani_108_a_merged_refcount_is_the_exact_sum_or_refused() {
    let (a, b) = (any_refcount(), any_refcount());
    let bytes = u128::from(a.live_bytes) + u128::from(b.live_bytes);
    let chunks = u128::from(a.live_chunks) + u128::from(b.live_chunks);
    let fits = bytes <= u128::from(u64::MAX) && chunks <= u128::from(u64::MAX);
    match merged_refcount(a, b) {
        Some(m) => {
            assert!(
                a.object_len == b.object_len && fits,
                "a merged row only for one length and a sum that fits"
            );
            assert!(
                u128::from(m.live_bytes) == bytes && u128::from(m.live_chunks) == chunks,
                "the merged row is the exact sum"
            );
            assert!(
                m.created_ms == a.created_ms.min(b.created_ms),
                "the merged row keeps the earlier creation"
            );
            assert!(
                m.object_len == a.object_len,
                "the merged row keeps the length"
            );
        }
        None => assert!(
            a.object_len != b.object_len || !fits,
            "a merge refuses only two lengths or an overflow"
        ),
    }
}
