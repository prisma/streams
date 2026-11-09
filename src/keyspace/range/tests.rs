#![cfg(test)]
//! A shard's range agrees with the registry's string match, kept as a test-only reference, for every
//! bit prefix; the registry's match refuses what is not one (a rig's label, a sign, more bits than it
//! routes on), and the seal's `of` reads it as every hash.

use rand::{RngCore, SeedableRng, rngs::StdRng};

use super::Range;
use crate::registry::shard_prefix_matches;

/// The registry's match before it used the range, kept as the reference: a hash's first 24 bits, as a
/// string of `'0'`/`'1'`, start with the prefix.
fn reference(prefix: &str, hash: &[u8; 16]) -> bool {
    let mut bits = String::with_capacity(24);
    for byte in hash.iter().take(3) {
        bits.push_str(&format!("{byte:08b}"));
    }
    bits.starts_with(prefix)
}

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
                Range::of(p).holds_route(&hash),
                reference(p, &hash),
                "prefix {p:?}, hash {hash:02x?}"
            );
        }
    }
}

#[test]
fn the_edges_of_a_range() {
    assert!(Range::of("1").holds_route(&[0x80; 16]));
    assert!(!Range::of("1").holds_route(&[0x7F; 16]));
    assert!(Range::of("0").holds_route(&[0x7F; 16]));
    let full = "1".repeat(crate::keyspace::ROUTE_PREFIX_BITS);
    assert!(Range::of(&full).holds_route(&[0xFF; 16]));
    assert!(!Range::of(&full).holds_route(&{
        let mut h = [0xFF; 16];
        h[2] = 0xFE;
        h
    }));
}

#[test]
fn a_label_or_the_root_covers_every_hash() {
    assert_eq!(Range::of("seal-clean-1a2b-0"), Range::ALL);
    assert_eq!(Range::of(""), Range::ALL);
    assert!(Range::ALL.holds_route(&[0x00; 16]) && Range::ALL.holds_route(&[0xFF; 16]));
}

/// A run past the route bits an entity hash carries (and a run of more than 128,
/// whose leading zeros would fit a `u128`) and a sign parse too, so none may be
/// read as a bit prefix: `parse` refuses each, and the seal's `of` reads them as
/// every hash.
#[test]
fn a_prefix_longer_than_a_hash_or_signed_covers_every_hash() {
    let deepest = crate::keyspace::ROUTE_PREFIX_BITS;
    assert_eq!(Range::parse(&"0".repeat(deepest + 1)), None);
    assert_eq!(Range::parse(&"0".repeat(129)), None);
    assert_eq!(Range::parse("+1"), None);
    assert_eq!(Range::of(&"0".repeat(deepest + 1)), Range::ALL);
    assert_eq!(Range::of("+1"), Range::ALL);
    assert!(!Range::of(&"0".repeat(deepest)).holds_route(&[0x80; 16]));
}

/// The registry's match is exactly its string match before the range, for every bit prefix up to 12
/// bits, and for the prefixes that are not one: labels, signs, and runs past the 24 bits it routes on.
#[test]
fn the_registrys_match_is_its_string_match_for_every_prefix() {
    let mut rng = StdRng::seed_from_u64(253);
    let hashes: Vec<[u8; 16]> = (0..64)
        .map(|_| {
            let mut hash = [0u8; 16];
            rng.fill_bytes(&mut hash);
            hash
        })
        .chain([[0x00; 16], [0xFF; 16]])
        .collect();
    let mut prefixes = vec![String::new()];
    for len in 1..=12u32 {
        for value in 0..(1u32 << len) {
            prefixes.push(format!("{value:0width$b}", width = len as usize));
        }
    }
    let others = ["seal-clean-1a2b-0", "+1", "-1", "01x", " 0", "2"];
    prefixes.extend(others.iter().map(|p| (*p).to_string()));
    prefixes.extend([
        "0".repeat(24),
        "1".repeat(24),
        "0".repeat(25),
        "1".repeat(129),
    ]);
    for hash in &hashes {
        for p in &prefixes {
            assert_eq!(
                shard_prefix_matches(p, hash),
                reference(p, hash),
                "prefix {p:?}, hash {hash:02x?}"
            );
        }
    }
    for p in others {
        assert_eq!(Range::parse(p), None, "{p:?} is no bit prefix");
    }
    assert_eq!(Range::parse(""), Some(Range::ALL), "the root is every hash");
}

/// The proof's constructor (KANI-106 builds its ranges with `from_bits`) is the range `parse` reads
/// from the same prefix, for the root, short prefixes and the longest route prefix.
#[test]
fn a_range_from_bits_is_the_range_its_prefix_parses_to() {
    let longest = "1".repeat(super::super::ROUTE_PREFIX_BITS);
    for prefix in ["", "1", "0", "0110", &longest] {
        let len = u32::try_from(prefix.len()).unwrap();
        let bits = match len {
            0 => 0,
            _ => u128::from_str_radix(prefix, 2).unwrap() << (128 - len),
        };
        assert_eq!(
            Some(Range::from_bits(bits, len)),
            Range::parse(prefix),
            "{prefix:?}"
        );
    }
}
