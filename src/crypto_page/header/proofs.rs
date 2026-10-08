//! Kani proofs for stored-row admission: KANI-017. `CheckedPage::from_row`
//! is `row_last` (the key is the canonical page prefix and one last
//! offset) followed by `page_at` (the row is exactly one whole layout 5
//! page with its tag, of 1 to 4,096 records, whose first offset and count
//! end at that offset without passing u64::MAX); `CheckedPage::admit` only
//! wraps the admitted fields around the row's bytes. Each harness offers a
//! row of `ROW` symbolic bytes (every version byte, first offset,
//! timestamp, key version, nonce and ciphertext length) under a key of any
//! length up to 26 symbolic bytes and the prefix `shard_page_prefix` gives a
//! symbolic stream hash, and checks what an admitted row must satisfy. The
//! record count and the routing-key length are fixed per harness, so every
//! later field sits at a position the checker knows (a symbolic count
//! varint or key length sends it through every field position and UTF-8
//! validation of every length, which it cannot finish): two records (one
//! count byte, so the key's offset is the page's last and not its first),
//! one record with a 4-byte routing key, 4,096 records (the two-byte count
//! at the bound) and 4,097 records (the first count past it, which must be
//! refused). So a valid page stored under another row's key is refused,
//! whatever its bytes.
use super::{ClearPage, page_at, row_last};
use crate::crypto_page::{PAGE_MAX_RECORDS, PageCorruption, shard_page_prefix};

/// Room for a page with a 4-byte routing key and a 19-byte ciphertext, or
/// a keyless page with a 23-byte one, at either count width.
const ROW: usize = 64;

/// One symbolic row's admission: its count varint is `count`, its routing
/// key `RK` symbolic bytes long, its key symbolic. Calls `f` with the
/// verdict (the admitted page and the key's offset), the key and the
/// prefix.
fn admit<const RK: u16>(
    count: &[u8],
    f: impl FnOnce(Result<(ClearPage<'_>, u64), PageCorruption>, &[u8], &[u8]),
) {
    let hash: [u8; 16] = kani::any();
    let prefix = shard_page_prefix(&hash);
    let key: [u8; 26] = kani::any();
    let key_len = kani::any_where(|len: &usize| *len <= 26);
    let key = &key[..key_len];
    let mut raw: [u8; ROW] = kani::any();
    raw[9..9 + count.len()].copy_from_slice(count);
    let rk_at = 1 + 8 + count.len() + 8 + 4;
    [raw[rk_at], raw[rk_at + 1]] = RK.to_be_bytes();
    let verdict =
        row_last(key, &prefix).and_then(|last| page_at(&raw, last).map(|page| (page, last)));
    f(verdict, key, &prefix);
}

/// What every admitted row satisfies, for a count varint of `count`.
fn admission<const RK: u16>(count: &[u8]) {
    admit::<RK>(count, |verdict, key, prefix| {
        kani::cover!(
            matches!(verdict, Err(PageCorruption::Offset { .. })),
            "a whole page under another row's key is refused"
        );
        let Ok((page, last)) = verdict else {
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
            (1..=PAGE_MAX_RECORDS).contains(&page.count),
            "an admitted page holds 1 to 4,096 records"
        );
        let span = u64::try_from(page.count - 1).ok();
        assert!(
            last == offset && span.and_then(|span| page.first.checked_add(span)) == Some(offset),
            "an admitted page carries its key's offset"
        );
        assert!(
            page.ct_len >= 16 && page.header_len + 4 + page.ct_len == ROW,
            "an admitted row is exactly one whole page with its tag"
        );
        kani::cover!(offset == u64::MAX, "the maximum offset is admitted");
    });
}

/// KANI-017 for keyless rows of two records (one count byte).
#[kani::proof]
#[kani::unwind(20)]
fn kani_017_a_keyless_row_is_admitted_only_under_its_own_key() {
    admission::<0>(&[2]);
}

/// KANI-017 for rows of one record with a 4-byte routing key.
#[kani::proof]
#[kani::unwind(20)]
fn kani_017_a_keyed_row_is_admitted_only_under_its_own_key() {
    admission::<4>(&[1]);
}

/// KANI-017 for keyless rows of 4,096 records, the two-byte count at the
/// bound.
#[kani::proof]
#[kani::unwind(20)]
fn kani_017_a_row_of_4096_records_is_admitted_only_under_its_own_key() {
    admission::<0>(&[0x80, 0x20]);
}

/// KANI-017: a row whose count is 4,097, the first past the bound, is never
/// admitted, whatever its other bytes and its key.
#[kani::proof]
#[kani::unwind(20)]
fn kani_017_a_row_of_4097_records_is_refused() {
    admit::<0>(&[0x81, 0x20], |verdict, _, _| {
        assert!(
            verdict.is_err(),
            "an admitted page holds 1 to 4,096 records"
        );
        kani::cover!(
            matches!(verdict, Err(PageCorruption::Count)),
            "the count refuses it"
        );
    });
}
