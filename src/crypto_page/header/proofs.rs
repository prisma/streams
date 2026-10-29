//! Kani proofs for stored-page admission: KANI-017. `CheckedPage::admit`
//! is `page_at` (the bytes are exactly one whole page with its tag, of 1
//! to 4,096 records, whose first offset and count end at the last offset
//! its holder names, without passing u64::MAX) wrapped around the page's
//! bytes. Layout 6 holds a page under that last offset in a pending row or
//! a segment object chunk (each page's own extent names it) and in the tail
//! ring. Each harness offers `ROW` symbolic bytes (every version byte, first
//! offset, timestamp, key version, nonce and ciphertext length) under a
//! symbolic last offset, and checks what an admitted page must satisfy. The
//! record count and the routing-key length are fixed per harness, so every
//! later field sits at a position the checker knows (a symbolic count
//! varint or key length sends it through every field position and UTF-8
//! validation of every length, which it cannot finish): two records (one
//! count byte, so the named offset is the page's last and not its first),
//! one record with a 4-byte routing key, 4,096 records (the two-byte count
//! at the bound) and 4,097 records (the first count past it, which must be
//! refused). So a valid page named by another page's offset is refused,
//! whatever its bytes.
use super::{ClearPage, page_at};
use crate::crypto_page::{PAGE_MAX_RECORDS, PageCorruption};

/// Room for a page with a 4-byte routing key and a 19-byte ciphertext, or
/// a keyless page with a 23-byte one, at either count width.
const ROW: usize = 64;

/// One symbolic page's admission: its count varint is `count`, its routing
/// key `RK` symbolic bytes long, the offset naming it symbolic. Calls `f`
/// with the verdict and that offset.
fn admit<const RK: u16>(count: &[u8], f: impl FnOnce(Result<ClearPage<'_>, PageCorruption>, u64)) {
    let last: u64 = kani::any();
    let mut raw: [u8; ROW] = kani::any();
    raw[9..9 + count.len()].copy_from_slice(count);
    let rk_at = 1 + 8 + count.len() + 8 + 4;
    [raw[rk_at], raw[rk_at + 1]] = RK.to_be_bytes();
    f(page_at(&raw, last), last);
}

/// What every admitted page satisfies, for a count varint of `count`.
fn admission<const RK: u16>(count: &[u8]) {
    admit::<RK>(count, |verdict, last| {
        kani::cover!(
            matches!(verdict, Err(PageCorruption::Offset { .. })),
            "a whole page named by another page's offset is refused"
        );
        let Ok(page) = verdict else {
            return;
        };
        assert!(
            (1..=PAGE_MAX_RECORDS).contains(&page.count),
            "an admitted page holds 1 to 4,096 records"
        );
        let span = u64::try_from(page.count - 1).ok();
        assert!(
            span.and_then(|span| page.first.checked_add(span)) == Some(last),
            "an admitted page ends at the offset naming it"
        );
        assert!(
            page.ct_len >= 16 && page.header_len + 4 + page.ct_len == ROW,
            "an admitted page is exactly one whole page with its tag"
        );
        kani::cover!(last == u64::MAX, "the maximum offset is admitted");
    });
}

/// KANI-017 for keyless pages of two records (one count byte).
#[kani::proof]
#[kani::unwind(20)]
fn kani_017_a_keyless_row_is_admitted_only_under_its_own_key() {
    admission::<0>(&[2]);
}

/// KANI-017 for pages of one record with a 4-byte routing key.
#[kani::proof]
#[kani::unwind(20)]
fn kani_017_a_keyed_row_is_admitted_only_under_its_own_key() {
    admission::<4>(&[1]);
}

/// KANI-017 for keyless pages of 4,096 records, the two-byte count at the
/// bound.
#[kani::proof]
#[kani::unwind(20)]
fn kani_017_a_row_of_4096_records_is_admitted_only_under_its_own_key() {
    admission::<0>(&[0x80, 0x20]);
}

/// KANI-017: a page whose count is 4,097, the first past the bound, is
/// never admitted, whatever its other bytes and the offset naming it.
#[kani::proof]
#[kani::unwind(20)]
fn kani_017_a_row_of_4097_records_is_refused() {
    admit::<0>(&[0x81, 0x20], |verdict, _| {
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
