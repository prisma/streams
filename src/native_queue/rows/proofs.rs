//! Kani proofs for a queue's fixed-layout entries: KANI-111 (S3b-DESIGN
//! §3.2, §11.3). Over any values:
//! - a dedup entry `'u'` (offset, expiry, lane) round-trips, and an entry
//!   one byte short, one byte long or of another lane is refused;
//! - a scheduled entry `'a'` (send time, stored length, inline page)
//!   round-trips at each inline length checked, and an entry shorter than
//!   its fixed fields is refused.
use super::{decode_dedup, decode_scheduled, encode_dedup, encode_scheduled};

#[kani::proof]
fn kani_111_a_dedup_entry_round_trips_and_refuses_other_lengths_and_lanes() {
    let offset: u64 = kani::any();
    let expires_at: i64 = kani::any();
    let raw = encode_dedup(offset, expires_at);
    assert!(
        decode_dedup(&raw) == Ok((offset, expires_at)),
        "a dedup entry round-trips"
    );
    assert!(
        decode_dedup(&raw[..raw.len() - 1]).is_err(),
        "a short dedup entry is refused"
    );
    let mut long = raw.clone();
    long.push(kani::any());
    assert!(
        decode_dedup(&long).is_err(),
        "a long dedup entry is refused"
    );
    let mut lane = raw;
    let other: u8 = kani::any();
    kani::assume(other != crate::native_queue::keys::LANE);
    *lane.last_mut().unwrap() = other;
    assert!(
        decode_dedup(&lane).is_err(),
        "a dedup entry of another lane is refused"
    );
}

/// One inline length: the entry round-trips, and its fixed fields cut
/// short are refused.
fn scheduled_round_trip<const N: usize>() {
    let enqueued_at: i64 = kani::any();
    let bytes: u32 = kani::any();
    let available_at: i64 = kani::any();
    let page: [u8; N] = kani::any();
    let raw = encode_scheduled(enqueued_at, bytes, &page);
    let (facts, inline) = decode_scheduled(available_at, &raw).unwrap();
    assert!(
        (facts.enqueued_at, facts.available_at, facts.bytes) == (enqueued_at, available_at, bytes),
        "a scheduled entry's facts round-trip"
    );
    assert!(
        inline == page,
        "a scheduled entry's inline page round-trips"
    );
    let cut: usize = kani::any();
    kani::assume(cut < 12);
    assert!(
        decode_scheduled(available_at, &raw[..cut]).is_err(),
        "a short scheduled entry is refused"
    );
}

#[kani::proof]
fn kani_111_a_scheduled_entry_round_trips_with_no_inline_page() {
    scheduled_round_trip::<0>();
}

#[kani::proof]
fn kani_111_a_scheduled_entry_round_trips_with_an_inline_page() {
    scheduled_round_trip::<4>();
}
