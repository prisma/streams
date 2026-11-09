//! Kani proofs for layout 8's key ranges: KANI-106 (S3a-DESIGN §1, §1.1; ROUTE-HASH-DECISION-2026-11-08
//! §6). A shard's prefix holds a row exactly when it holds the row's segment route, because the
//! entity hash every per-entity key leads with carries the route's first `ROUTE_PREFIX_BITS`. And a
//! per-entity key (a stream row's) lies in its hash's range and never in the sentinel's. Together, a
//! shard's rows are the rows of the segments the registry routes to it, in one key interval.
//!
//! The harnesses take any prefix of up to `ROUTE_PREFIX_BITS` bits (`Range::from_bits`, not the
//! string parse), any route and identity, and any entity hash outside the sentinel.
use super::range::Range;
use super::{EntityHash, ROUTE_PREFIX_BITS, SENTINEL, entity_hash, stream_row};
use crate::crypto::{RouteHash, SegmentHash};

/// Any shard prefix the registry routes on.
fn prefix() -> Range {
    let len: u32 = kani::any_where(|l: &u32| (*l as usize) <= ROUTE_PREFIX_BITS);
    Range::from_bits(kani::any(), len)
}

/// Any entity hash: every minter refuses the sentinel's (S3a-DESIGN §1, F12).
fn entity() -> [u8; 16] {
    let hash: [u8; 16] = kani::any();
    kani::assume(hash != SENTINEL);
    hash
}

#[kani::proof]
fn kani_106_a_prefix_holds_an_entity_exactly_when_it_holds_its_route() {
    let route: [u8; 16] = kani::any();
    let identity: [u8; 16] = kani::any();
    let entity = entity_hash(&RouteHash(route), &SegmentHash(identity));
    let range = prefix();
    assert!(
        range.contains(entity) == range.holds_route(&route),
        "a prefix holds an entity exactly when it holds its route"
    );
}

#[kani::proof]
fn kani_106_an_entity_key_leads_with_its_hash_outside_the_sentinel() {
    let hash = entity();
    let key = stream_row(&hash, 0);
    let mut lead = [0u8; 16];
    lead.copy_from_slice(&key[..16]);
    let range = prefix();
    assert!(
        range.contains(EntityHash::from_stored(lead))
            == range.contains(EntityHash::from_stored(hash)),
        "an entity key lies in its hash's range"
    );
    assert!(lead != SENTINEL, "an entity key never lies in the sentinel");
}
