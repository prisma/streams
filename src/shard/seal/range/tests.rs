#![cfg(test)]
//! A shard's range agrees with the registry's prefix match for every bit
//! prefix, and a non-bit prefix (a test rig's label) covers every hash.

use rand::{RngCore, SeedableRng, rngs::StdRng};

use super::Range;
use crate::registry::shard_prefix_matches;

#[test]
fn a_bit_prefix_matches_exactly_the_registrys_hashes() {
    let mut rng = StdRng::seed_from_u64(149);
    let prefixes = [
        "",
        "0",
        "1",
        "01",
        "10",
        "110",
        "0111",
        "1010101",
        "00000000",
        "100000001",
    ];
    for _ in 0..2_000 {
        let mut hash = [0u8; 16];
        rng.fill_bytes(&mut hash);
        for p in prefixes {
            assert_eq!(
                Range::of(p).contains(&hash),
                shard_prefix_matches(p, &hash),
                "prefix {p:?}, hash {hash:02x?}"
            );
        }
    }
}

#[test]
fn the_edges_of_a_range() {
    assert!(Range::of("1").contains(&[0x80; 16]));
    assert!(!Range::of("1").contains(&[0x7F; 16]));
    assert!(Range::of("0").contains(&[0x7F; 16]));
    let full = "1".repeat(128);
    assert!(Range::of(&full).contains(&[0xFF; 16]));
    assert!(!Range::of(&full).contains(&{
        let mut h = [0xFF; 16];
        h[15] = 0xFE;
        h
    }));
}

#[test]
fn a_label_or_the_root_covers_every_hash() {
    assert_eq!(Range::of("seal-clean-1a2b-0"), Range::ALL);
    assert_eq!(Range::of(""), Range::ALL);
    assert!(Range::ALL.contains(&[0x00; 16]) && Range::ALL.contains(&[0xFF; 16]));
}
