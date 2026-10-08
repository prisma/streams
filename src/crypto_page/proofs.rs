//! Kani proofs for the layout 5 page-body parser: KANI-097. `parse_body`
//! cuts every opened page into its records after authentication, so a
//! body it accepts must describe exactly one sequence of records.
//!
//! `exact` offers a body of up to BODY symbolic bytes, a symbolic header
//! timestamp and a fixed record count per harness (1, 2 and 3), and checks
//! what an accepted body must satisfy: exactly `count` spans, none longer
//! than the record cap, the first at the header's timestamp and none
//! earlier than the one before it, tables that are exactly the minimal
//! LEB128 encoding of every length and of every delta to the record before
//! (an oracle encoder of its own, independent of `put_varint`), and lengths
//! that tile the payload bytes after them; so nothing is skipped, overlong
//! or trailing. The `build_body` round trip is not here: a symbolic build
//! allocates a buffer of symbolic size, so unit and property tests pin it.
//! Every harness loop runs over a constant count, and a varint takes at most
//! ten bytes, so `unwind(12)` covers them with Kani's unwinding checks left
//! on.
use super::MAX_RECORD_PLAINTEXT;
use super::body::parse_body;

/// The minimal LEB128 encoding of `value` into `out`, and its length: the
/// oracle the tables are compared with.
fn oracle(value: u64, out: &mut [u8; 10]) -> usize {
    let mut rest = value;
    let mut n = 0;
    loop {
        let low = (rest & 0x7f) as u8;
        rest >>= 7;
        if rest == 0 {
            out[n] = low;
            return n + 1;
        }
        out[n] = low | 0x80;
        n += 1;
    }
}

/// Append the oracle encoding of `value` to `again`, of which `n` bytes are
/// written; false when it does not fit.
fn push_oracle<const BODY: usize>(again: &mut [u8; BODY], n: &mut usize, value: u64) -> bool {
    let mut enc = [0u8; 10];
    let k = oracle(value, &mut enc);
    let mut fits = true;
    for (j, byte) in enc.iter().enumerate() {
        if j < k {
            if *n < BODY {
                again[*n] = *byte;
                *n += 1;
            } else {
                fits = false;
            }
        }
    }
    fits
}

fn exact<const COUNT: usize, const BODY: usize>() {
    let bytes: [u8; BODY] = kani::any();
    let len = kani::any_where(|len: &usize| *len <= BODY);
    let body = &bytes[..len];
    let ts_ms: i64 = kani::any();
    let Ok(table) = parse_body(body, COUNT, ts_ms) else {
        kani::cover!(len == BODY, "a full-size body can be refused");
        return;
    };
    assert!(
        table.records.len() == COUNT,
        "an accepted body holds exactly its count of records"
    );
    let mut again = [0u8; BODY];
    let mut n = 0usize;
    let mut fits = true;
    let mut payloads = 0usize;
    for i in 0..COUNT {
        let span = table.records[i];
        assert!(
            span.len <= MAX_RECORD_PLAINTEXT,
            "no accepted record passes the record cap"
        );
        fits &= push_oracle(&mut again, &mut n, span.len as u64);
        payloads += span.len;
    }
    let mut previous = ts_ms;
    let mut moved = false;
    for i in 0..COUNT {
        let span = table.records[i];
        assert!(
            if i == 0 {
                span.ts_ms == ts_ms
            } else {
                span.ts_ms >= previous
            },
            "the first record is at the page timestamp and none goes back"
        );
        fits &= push_oracle(&mut again, &mut n, span.ts_ms.abs_diff(previous));
        moved |= span.ts_ms > previous;
        previous = span.ts_ms;
    }
    let mut same = true;
    for i in 0..BODY {
        same &= i >= n || again[i] == bytes[i];
    }
    assert!(
        fits && n == table.payload_start && same,
        "an accepted body is exactly the minimal encoding of its tables and payloads"
    );
    assert!(
        table.payload_start <= len && payloads == len - table.payload_start,
        "the lengths tile the payload bytes exactly"
    );
    kani::cover!(payloads > 0, "a body with payload bytes is accepted");
    // A one-record page has no delta to move: the cover holds trivially.
    kani::cover!(COUNT == 1 || moved, "a nonzero delta is accepted");
}

/// KANI-097: body exactness for a single-record page (bodies of up to 4
/// bytes: two table bytes and two payload bytes, or a longer table).
#[kani::proof]
#[kani::unwind(12)]
fn kani_097_a_one_record_body_parses_exactly() {
    exact::<1, 4>();
}

/// KANI-097: body exactness for a two-record page (up to 6 bytes).
#[kani::proof]
#[kani::unwind(12)]
fn kani_097_a_two_record_body_parses_exactly() {
    exact::<2, 6>();
}

/// KANI-097: body exactness for a three-record page (up to 7 bytes: the
/// third record is the one whose delta tells the record before from the
/// header).
#[kani::proof]
#[kani::unwind(12)]
fn kani_097_a_three_record_body_parses_exactly() {
    exact::<3, 7>();
}
