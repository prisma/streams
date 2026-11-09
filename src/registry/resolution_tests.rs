//! ROUTING-V3 resolution (docs/ROUTING-V3.md §1-2): the implicit total
//! order, live-cover selection and the segment map wire round trip.
#![cfg(test)]
use super::tests::desc;
use super::*;
use crate::crypto::stream_hash;

/// Total-order streams (segments: None, no legacy fields) resolve
/// every key to segment 0 with identity == storage_hash() — the
/// zero-move migration guarantee.
#[test]
fn implicit_map_is_the_old_total_order_layout() {
    let d = desc("t", "00000000000000000000000000000001", false);
    for rk in ["", "a", "user-42", "\u{1F600}"] {
        let r = d.resolve_segment(rk);
        assert_eq!(r.seg_id, 0);
        assert_eq!(r.identity, d.storage_hash());
        // Layout 4: the parent route is project-qualified and
        // domain-separated — NOT the bare name hash.
        assert_eq!(
            r.shard_route,
            crate::crypto::RouteHash::for_stream(&d.sref()).0
        );
        assert_ne!(r.shard_route, stream_hash("t"));
        assert!(!r.sealed);
    }
}

/// Descriptor-resident dynamic maps: the live segment containing
/// the key point wins; seg 0's identity is storage_hash() (old data
/// stays addressable); sealed segments surface `sealed` so the
/// caller refreshes; a segment with its own shard_prefix routes to
/// that shard.
#[test]
fn dynamic_map_resolution_selects_the_live_cover() {
    let mut d = desc("dyn", "00000000000000000000000000000003", false);
    let mut map = crate::segmap::SegmentMap::initial("", 1);
    // Split the keyspace in half: seg 0 sealed, children 1 and 2.
    let mid = u64::MAX / 2;
    map.segments[0].sealed_ms = Some(2);
    map.segments[0].sealed_next_offset = Some(10);
    map.segments.push(crate::segmap::SegmentDesc {
        seg_id: 1,
        lo: 0,
        hi: mid,
        shard_prefix: String::new(),
        route_hash: [0u8; 16],
        created_ms: 2,
        predecessors: vec![0],
        successors: Vec::new(),
        sealed_ms: None,
        sealed_next_offset: None,
    });
    map.segments.push(crate::segmap::SegmentDesc {
        seg_id: 2,
        lo: mid,
        hi: crate::segmap::KEYSPACE_END,
        shard_prefix: "shard-07".into(),
        route_hash: [0u8; 16],
        created_ms: 2,
        predecessors: vec![0],
        successors: Vec::new(),
        sealed_ms: None,
        sealed_next_offset: None,
    });
    map.next_seg_id = 3;
    map.version = 2;
    d.segments = Some(map);

    assert_eq!(d.dynamic_segment_identity(0), d.storage_hash());

    // Find one key on each side of the midpoint.
    let mut lo_key = None;
    let mut hi_key = None;
    for i in 0..64 {
        let k = format!("k{i}");
        if StreamDesc::key_point(&k) < mid {
            lo_key.get_or_insert(k);
        } else {
            hi_key.get_or_insert(k);
        }
        if lo_key.is_some() && hi_key.is_some() {
            break;
        }
    }
    let (lo_key, hi_key) = (lo_key.unwrap(), hi_key.unwrap());

    let r = d.resolve_segment(&lo_key);
    assert_eq!((r.seg_id, r.sealed), (1, false));
    assert_eq!(r.identity, d.dynamic_segment_identity(1));
    assert_ne!(r.identity, d.storage_hash());
    assert_eq!(
        r.shard_route,
        crate::crypto::RouteHash::for_stream(&d.sref()).0,
        "empty prefix = parent route"
    );

    let r = d.resolve_segment(&hi_key);
    assert_eq!((r.seg_id, r.sealed), (2, false));
    assert_eq!(r.shard_route, stream_hash("shard-07"));

    // Seal child 1 with no successor yet (mid-transition crash
    // shape): resolution surfaces sealed=true instead of failing.
    d.segments.as_mut().unwrap().segments[1].sealed_ms = Some(3);
    let r = d.resolve_segment(&lo_key);
    assert_eq!((r.seg_id, r.sealed), (1, true));
}

/// Serde: fresh descriptors stay byte-lean (no "segments" key);
/// pre-v3 JSON (no field at all) parses; a materialized map
/// round-trips.
#[test]
fn descriptor_segments_serde_roundtrip() {
    let d = desc("s", "00000000000000000000000000000004", false);
    let j = serde_json::to_string(&d).unwrap();
    assert!(
        !j.contains("\"segments\""),
        "implicit map must cost zero bytes"
    );
    let legacy: StreamDesc = serde_json::from_str(&j).unwrap();
    assert!(legacy.segments.is_none());

    let mut with_map = desc("s2", "00000000000000000000000000000005", false);
    with_map.segments = Some(crate::segmap::SegmentMap::initial("", 7));
    let j2 = serde_json::to_string(&with_map).unwrap();
    let back: StreamDesc = serde_json::from_str(&j2).unwrap();
    assert_eq!(back.segments, with_map.segments);
    // Layout 8 routes every segment by its route: a segment pinned to a shard
    // prefix (layout 7's legacy placement) is refused.
    let mut pinned = with_map;
    pinned.segments = Some(crate::segmap::SegmentMap::initial("sh", 7));
    let refused = serde_json::from_str::<StreamDesc>(&serde_json::to_string(&pinned).unwrap());
    let refusal = refused
        .expect_err("a pinned segment is refused")
        .to_string();
    assert!(refusal.contains("unsupported_storage_layout"), "{refusal}");
}

/// Segment 0 keeps its route when a stream's first map is created, and every segment of a split
/// map leads its identity with the route the registry places it by (S3a-DESIGN §1.1), so no
/// segment's rows leave the shard that serves it.
#[test]
fn a_new_map_keeps_segment_zeros_route_and_its_children_share_theirs() {
    let route_bytes = crate::keyspace::ROUTE_BYTES;
    let mut d = desc("seg0", "00000000000000000000000000000005", false);
    let before = d.resolve_segment("k");
    assert_eq!((before.seg_id, before.identity), (0, d.storage_hash()));
    d.segments = Some(crate::segmap::SegmentMap::initial("", 1));
    let created = d.resolve_segment("k");
    assert_eq!(
        (created.seg_id, created.identity, created.shard_route),
        (0, before.identity, before.shard_route)
    );
    let mid = u64::MAX / 2;
    let high = crate::crypto::RouteHash::for_child(&d.sref(), 2, &0u32.to_be_bytes()).0;
    let map = d.segments.as_mut().unwrap();
    assert_eq!(
        map.split(0, mid, 10, before.shard_route, high, 2),
        Ok((1, 2))
    );
    assert_eq!(d.dynamic_segment_identity(0), d.storage_hash());
    let mut seen = std::collections::BTreeSet::new();
    for i in 0..64 {
        let r = d.resolve_segment(&format!("k{i}"));
        assert_eq!(
            r.identity[..route_bytes],
            r.shard_route[..route_bytes],
            "segment {}",
            r.seg_id
        );
        seen.insert((r.seg_id, r.shard_route));
    }
    let expected = [(1, before.shard_route), (2, high)];
    assert_eq!(seen.into_iter().collect::<Vec<_>>(), expected);
}
