#![cfg(test)]
//! The topology loop's pieces that need no rig (the loop itself: `dst_tests::split_k`).

use super::route_of;

/// **A prefix's first route hash is its bits, then zeros.** The root's is all zeros; `1` sets the
/// top bit; `01` the second; a 24-bit prefix fills three bytes.
#[test]
fn a_prefixs_first_route_is_its_bits_then_zeros() {
    assert_eq!(route_of(""), [0; 16]);
    assert_eq!(route_of("1")[0], 0x80);
    assert_eq!(route_of("01")[0], 0x40);
    let deep = route_of("101010101010101010101010");
    assert_eq!(deep[..4], [0xAA, 0xAA, 0xAA, 0x00]);
}
