//! Kani proofs for the segment map's partition: KANI-028. `validate` refuses
//! a segment whose `lo` is not below its `hi` and accepts terminal segments
//! only if `tiles_keyspace` accepts their ranges; `check_partition` applies
//! the same rule to the live ones. Each harness takes N (one to four) such
//! ranges with full-width symbolic bounds, keeps those `tiles_keyspace`
//! accepts, and checks every `u64` routing point: exactly one range holds it
//! by `SegmentDesc::contains` (`KEYSPACE_END` in the range ending there, a
//! boundary in the range starting there), and a map of N terminal segments
//! over those ranges, each live or a sealed leaf, answers `route` with that
//! segment. So `Registry::resolve_segment`'s `unreachable!` cannot be
//! reached for a validated map. The number of ranges is fixed per harness:
//! a symbolic length sends the checker through every sort strategy. The
//! whole-map `validate` is not called here (duplicate identities, seal
//! metadata and lineage are unit-tested; lineage is KANI-031's).
use super::{KEYSPACE_END, SegmentDesc, SegmentMap, tiles_keyspace};

/// A live segment over `[lo, hi)`.
fn segment(seg_id: u32, lo: u64, hi: u64) -> SegmentDesc {
    SegmentDesc {
        seg_id,
        lo,
        hi,
        shard_prefix: String::new(),
        route_hash: [0; 16],
        created_ms: 0,
        predecessors: Vec::new(),
        successors: Vec::new(),
        sealed_ms: None,
        sealed_next_offset: None,
    }
}

/// N nonempty ranges `tiles_keyspace` accepts, a routing point, and the
/// checks; returns the sorted ranges and the point for the harness's covers.
fn tiled<const N: usize>() -> ([(u64, u64); N], u64) {
    let mut ranges: [(u64, u64); N] = kani::any();
    kani::assume(ranges.iter().all(|(lo, hi)| lo < hi));
    kani::assume(tiles_keyspace(&mut ranges));
    let k: u64 = kani::any();
    let holders = (0u32..)
        .zip(ranges)
        .filter(|(id, (lo, hi))| segment(*id, *lo, *hi).contains(k));
    let mut holders = holders.map(|(id, _)| id);
    let holder = holders.next();
    assert!(
        holder.is_some() && holders.next().is_none(),
        "every key lies in exactly one terminal segment"
    );
    let map = SegmentMap {
        version: 1,
        next_seg_id: u32::try_from(N).unwrap_or(u32::MAX),
        segments: (0u32..).zip(ranges).map(terminal).collect(),
        pending: None,
    };
    let routed = map.route(k);
    assert!(
        routed.is_some_and(|s| Some(s.seg_id) == holder),
        "the route answers every key with the segment that holds it"
    );
    kani::cover!(k == KEYSPACE_END, "the maximum key routes");
    kani::cover!(
        routed.is_some_and(|s| !s.is_live()),
        "a key with no live cover routes to its sealed leaf"
    );
    (ranges, k)
}

/// A terminal segment over a range: live, or sealed before its successors
/// are published, created at any time.
fn terminal((seg_id, (lo, hi)): (u32, (u64, u64))) -> SegmentDesc {
    let sealed: bool = kani::any();
    SegmentDesc {
        created_ms: kani::any(),
        sealed_ms: sealed.then_some(0),
        sealed_next_offset: sealed.then_some(0),
        ..segment(seg_id, lo, hi)
    }
}

/// KANI-028 with one segment: it must span the whole key space.
#[kani::proof]
#[kani::unwind(6)]
fn kani_028_one_segment_holds_every_key() {
    tiled::<1>();
}

/// KANI-028 with two segments.
#[kani::proof]
#[kani::unwind(6)]
fn kani_028_two_segments_hold_every_key_once() {
    let (ranges, k) = tiled::<2>();
    kani::cover!(
        k > 0 && ranges.iter().any(|range| range.0 == k),
        "a boundary key routes to the range starting there"
    );
}

/// KANI-028 with three segments.
#[kani::proof]
#[kani::unwind(6)]
fn kani_028_three_segments_hold_every_key_once() {
    let (ranges, k) = tiled::<3>();
    kani::cover!(
        k > 0 && ranges.iter().any(|range| range.0 == k),
        "a boundary key routes to the range starting there"
    );
}

/// KANI-028 with four segments.
#[kani::proof]
#[kani::unwind(6)]
fn kani_028_four_segments_hold_every_key_once() {
    let (ranges, k) = tiled::<4>();
    kani::cover!(
        k > 0 && ranges.iter().any(|range| range.0 == k),
        "a boundary key routes to the range starting there"
    );
}
