//! Kani proofs for stored-row admission: KANI-017. `CheckedPage::from_row`
//! admits a row read under a stream's shard-log page prefix only if the key
//! is that prefix and one last offset, the row is exactly one whole layout 5
//! page with its tag, and the page's first offset and record count end at
//! the key's offset. Each harness offers a row of `ROW` symbolic bytes (so
//! every version byte, first offset, timestamp, key version, nonce and
//! ciphertext length) under a key of any length up to 26 symbolic bytes and
//! the prefix `shard_page_prefix` gives a symbolic stream hash, and checks
//! what an admitted row must satisfy. The record count is a one-byte varint
//! (1 to 127 records) and the routing-key length field is fixed per harness
//! (0 and 4 bytes, the key's bytes symbolic), so every later field sits at
//! a fixed position; a symbolic routing-key length sends the checker through
//! UTF-8 validation of every length. So a valid page stored under another
//! row's key is refused, whatever its bytes.
use crate::crypto_page::{CheckedPage, PageCorruption, shard_page_prefix};

/// Room for a page with a 4-byte routing key and a 20-byte ciphertext, or
/// a keyless page with a 24-byte one.
const ROW: usize = 64;

/// The clear header's length up to and including the nonce, for a one-byte
/// record count and an `rk`-byte routing key.
const fn header_len(rk: u16) -> usize {
    1 + 8 + 1 + 8 + 4 + 2 + rk as usize + 12
}

fn admission<const RK: u16>() {
    let hash: [u8; 16] = kani::any();
    let prefix = shard_page_prefix(&hash);
    let key: [u8; 26] = kani::any();
    let key = &key[..kani::any_where(|len: &usize| *len <= 26)];
    let mut raw: [u8; ROW] = kani::any();
    kani::assume(raw[9] < 0x80);
    [raw[22], raw[23]] = RK.to_be_bytes();
    let admitted = CheckedPage::from_row(key, &prefix, bytes::Bytes::copy_from_slice(&raw));
    kani::cover!(
        matches!(admitted, Err(PageCorruption::Offset { .. })),
        "a whole frame under another row's key is refused"
    );
    let Ok(page) = admitted else {
        return;
    };
    assert!(
        key.len() == 25 && key.starts_with(&prefix),
        "an admitted row's key is its stream's prefix and one offset"
    );
    let mut offset = [0u8; 8];
    offset.copy_from_slice(&key[17..25]);
    let offset = u64::from_be_bytes(offset);
    let span = u64::try_from(page.count() - 1).ok();
    assert!(
        page.last() == offset
            && span.and_then(|span| page.first().checked_add(span)) == Some(offset),
        "an admitted frame carries its key's offset"
    );
    let at = header_len(RK);
    let ct_len = u32::from_be_bytes([raw[at], raw[at + 1], raw[at + 2], raw[at + 3]]);
    let ct_len = usize::try_from(ct_len).unwrap_or(usize::MAX);
    assert!(
        ct_len >= 16 && at + 4 + ct_len == ROW && page.raw().len() == ROW,
        "an admitted row is exactly one whole frame with its tag"
    );
    kani::cover!(offset == u64::MAX, "the maximum offset is admitted");
}

/// KANI-017 for rows whose pages carry no routing key.
#[kani::proof]
#[kani::unwind(20)]
fn kani_017_a_keyless_row_is_admitted_only_under_its_own_key() {
    admission::<0>();
}

/// KANI-017 for rows whose pages carry a 4-byte routing key.
#[kani::proof]
#[kani::unwind(20)]
fn kani_017_a_keyed_row_is_admitted_only_under_its_own_key() {
    admission::<4>();
}
