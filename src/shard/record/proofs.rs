//! Kani proofs for stored-record admission: KANI-017. `decode_row` admits a
//! row read under a stream's key only if the key is that stream's prefix and
//! one offset, the row is exactly one whole frame with its tag, and the
//! frame's own offset is the key's. Each harness offers a row of `ROW`
//! symbolic bytes (so every version byte, timestamp, offset and ciphertext
//! length field) under a key of any length up to 26 symbolic bytes and the
//! prefix `record_key` gives a symbolic stream hash, and checks what an
//! admitted row must satisfy. The routing-key length field is fixed per
//! harness (0 and 4 bytes, the key's bytes symbolic): a symbolic length
//! sends the checker through UTF-8 validation of every length, which it
//! did not finish in 40 minutes. So a valid frame stored under another
//! row's key is refused, whatever its bytes.
use super::{RecordCorruption, decode_row};
use crate::crypto::decode_frame;
use crate::shard::record_key;

/// Room for a version-4 frame with a 4-byte routing key and a 21-byte
/// ciphertext, or a legacy frame with a longer one.
const ROW: usize = 64;

fn admission<const RK: u16>() {
    let hash: [u8; 16] = kani::any();
    let canonical = record_key(&hash, 0);
    let prefix = &canonical[..17];
    let key: [u8; 26] = kani::any();
    let key = &key[..kani::any_where(|len: &usize| *len <= 26)];
    let mut raw: [u8; ROW] = kani::any();
    [raw[21], raw[22]] = RK.to_be_bytes();
    let admitted = decode_row(key, prefix, &raw);
    kani::cover!(
        decode_frame(&raw).is_some() && matches!(admitted, Err(RecordCorruption::Offset { .. })),
        "a whole frame under another row's key is refused"
    );
    let Ok(frame) = admitted else {
        return;
    };
    assert!(
        key.len() == 25 && key.starts_with(prefix),
        "an admitted row's key is its stream's prefix and one offset"
    );
    let mut offset = [0u8; 8];
    offset.copy_from_slice(&key[17..25]);
    let offset = u64::from_be_bytes(offset);
    assert!(
        frame.header.offset == offset,
        "an admitted frame carries its key's offset"
    );
    assert!(
        frame.ciphertext.len() >= 16 && frame.header_len + 4 + frame.ciphertext.len() == ROW,
        "an admitted row is exactly one whole frame with its tag"
    );
    kani::cover!(offset == u64::MAX, "the maximum offset is admitted");
}

/// KANI-017 for rows whose frames carry no routing key.
#[kani::proof]
#[kani::unwind(20)]
fn kani_017_a_keyless_row_is_admitted_only_under_its_own_key() {
    admission::<0>();
}

/// KANI-017 for rows whose frames carry a 4-byte routing key.
#[kani::proof]
#[kani::unwind(20)]
fn kani_017_a_keyed_row_is_admitted_only_under_its_own_key() {
    admission::<4>();
}
