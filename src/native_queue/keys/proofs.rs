//! Kani proofs for a queue's public ids: KANI-111 (S3b-DESIGN §3.2, §11.3).
//! A message id carries the incarnation tag (the entity hash's low 32
//! bits), the lane and the offset; a lease token adds the lease
//! generation. Over any hash, offset and generation, on the ids'
//! fixed-size cores (the `String` layer, a prefix and ASCII characters, is
//! outside the proof; native_queue::tests pins ids character for character):
//! - an id's base32 characters decode to exactly its bytes;
//! - its bytes name exactly what the id was made for, under its own hash;
//! - under a hash of another incarnation (another low 32 bits) they name
//!   nothing;
//! - bytes naming another lane name nothing (only lane 0 exists).
use super::{LANE, base32, lease_of, lease_raw, message_offset, message_raw, unbase32};
use crate::native_queue::Token;

/// Any hash, and one that differs from it in its incarnation tag.
fn hashes() -> ([u8; 16], [u8; 16]) {
    let hash: [u8; 16] = kani::any();
    let mut other = hash;
    let i: usize = kani::any();
    kani::assume((12..16).contains(&i));
    let flip: u8 = kani::any();
    kani::assume(flip != 0);
    other[i] ^= flip;
    (hash, other)
}

#[kani::proof]
#[kani::unwind(30)]
fn kani_111_a_message_id_names_its_offset_under_its_own_incarnation_only() {
    let (hash, other) = hashes();
    let offset: u64 = kani::any();
    let raw = message_raw(&hash, offset);
    let chars: [u8; 21] = base32(&raw);
    assert!(
        unbase32::<13>(&chars) == Some(raw),
        "an id's characters decode to its bytes"
    );
    assert!(
        message_offset(&hash, &raw) == Some(offset),
        "a message id parses back to its offset"
    );
    assert!(
        message_offset(&other, &raw).is_none(),
        "an id names nothing under another incarnation"
    );
}

#[kani::proof]
#[kani::unwind(30)]
fn kani_111_a_lease_token_names_its_lease_under_its_own_incarnation_only() {
    let (hash, other) = hashes();
    let token = Token {
        offset: kani::any(),
        lease_gen: kani::any(),
    };
    let raw = lease_raw(&hash, token);
    let chars: [u8; 28] = base32(&raw);
    assert!(
        unbase32::<17>(&chars) == Some(raw),
        "a token's characters decode to its bytes"
    );
    assert!(
        lease_of(&hash, &raw) == Some(token),
        "a lease token parses back to its lease"
    );
    assert!(
        lease_of(&other, &raw).is_none(),
        "a token names nothing under another incarnation"
    );
}

#[kani::proof]
fn kani_111_an_id_of_another_lane_names_nothing() {
    let hash: [u8; 16] = kani::any();
    let lane: u8 = kani::any();
    kani::assume(lane != LANE);
    let mut raw = message_raw(&hash, kani::any());
    raw[4] = lane;
    assert!(
        message_offset(&hash, &raw).is_none(),
        "an id of another lane names nothing"
    );
}
