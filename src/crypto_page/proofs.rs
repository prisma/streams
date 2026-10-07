//! Kani proofs for the layout 5 page-body parser (a proposed obligation; it
//! has no manifest entry, receipt or negative controls yet). `parse_body`
//! is what every opened page's records are cut by, after authentication.
//!
//! `exact` offers a body of up to BODY symbolic bytes, a symbolic header
//! timestamp and a fixed record count per harness (1, 2 and 3), and checks
//! what an accepted body must satisfy: exactly `count` spans, timestamps
//! that never precede the header's, lengths that tile the payload bytes,
//! and, re-encoded with the production `put_varint`, the very same bytes,
//! so the tables are minimal varints and nothing is skipped or trailing.
//! `round_trip` builds a body from symbolic records of up to two bytes with
//! the production `build_body` and checks it parses back to them. Loops are
//! bounded by the count and by the ten-byte varint, so `unwind(12)` covers
//! them with Kani's unwinding checks left on.
use super::body::{build_body, parse_body, put_varint};

/// Room for three records' tables and a few payload bytes.
const BODY: usize = 8;

fn exact<const COUNT: usize>() {
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
    assert!(
        table.records.iter().all(|span| span.ts_ms >= ts_ms),
        "no record precedes the page timestamp"
    );
    let payloads: usize = table.records.iter().map(|span| span.len).sum();
    assert!(
        table.payload_start <= len && payloads == len - table.payload_start,
        "the lengths tile the payload bytes exactly"
    );
    let mut again = Vec::new();
    for span in &table.records {
        put_varint(&mut again, span.len as u64);
    }
    for span in &table.records {
        let delta = i128::from(span.ts_ms) - i128::from(ts_ms);
        put_varint(&mut again, delta as u64);
    }
    again.extend_from_slice(&body[table.payload_start..]);
    assert!(
        again.as_slice() == body,
        "an accepted body is exactly the minimal encoding of its tables and payloads"
    );
    kani::cover!(payloads > 0, "a body with payload bytes is accepted");
    kani::cover!(
        table.records.iter().any(|span| span.ts_ms > ts_ms),
        "a nonzero delta is accepted"
    );
}

fn round_trip<const COUNT: usize>() {
    let payloads: [[u8; 2]; COUNT] = kani::any();
    let lens: [usize; COUNT] = kani::any();
    for len in lens {
        kani::assume(len <= 2);
    }
    let records: Vec<&[u8]> = payloads
        .iter()
        .zip(lens)
        .map(|(payload, len)| &payload[..len])
        .collect();
    let ts_ms: i64 = kani::any();
    let body = build_body(&records);
    let Ok(table) = parse_body(&body, COUNT, ts_ms) else {
        panic!("a built body parses");
    };
    assert!(
        table.records.iter().map(|span| span.len).eq(lens),
        "every record keeps its length"
    );
    assert!(
        table.records.iter().all(|span| span.ts_ms == ts_ms),
        "every record keeps the page timestamp"
    );
    assert!(
        body[table.payload_start..] == records.concat(),
        "the payloads follow the tables in order"
    );
    kani::cover!(lens.iter().all(|len| *len == 2), "full records round-trip");
}

/// Page-body exactness for a single-record page.
#[kani::proof]
#[kani::unwind(12)]
fn kani_page_body_one_record_parses_exactly() {
    exact::<1>();
}

/// Page-body exactness for a two-record page.
#[kani::proof]
#[kani::unwind(12)]
fn kani_page_body_two_records_parse_exactly() {
    exact::<2>();
}

/// Page-body exactness for a three-record page.
#[kani::proof]
#[kani::unwind(12)]
fn kani_page_body_three_records_parse_exactly() {
    exact::<3>();
}

/// A built two-record body parses back to its records.
#[kani::proof]
#[kani::unwind(12)]
fn kani_page_body_two_records_round_trip() {
    round_trip::<2>();
}
