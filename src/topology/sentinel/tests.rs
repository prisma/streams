#![cfg(test)]
//! The helper's routing (S4-DESIGN §7, S3a §2.8): every tag's rule, on rows of each kind.

use super::{PlanError, Row, route};
use crate::keyspace::range::Range;
use crate::keyspace::{SENTINEL, tag};

/// A sentinel row of `tag` with `payload` after it.
fn row(t: u8, payload: &[u8]) -> Row {
    let mut key = SENTINEL.to_vec();
    key.push(t);
    key.extend_from_slice(payload);
    (key, vec![t])
}

/// An entity hash in the lower (`0x00..`) or upper (`0x80..`) half.
fn hash(upper: bool, n: u8) -> [u8; 16] {
    let mut h = [n; 16];
    h[0] = if upper { 0x80 | n } else { n & 0x7F };
    h
}

/// **Each child gets exactly its share.** Two children, `0` and `1`: `'K'` and `'I'` go to both
/// as they are; a head row (by the hash after its tag) and a roster row (by its trailing hash) go
/// to the child of their hash; `'G'`, `'L'`, `'R'`, `'O'`, `'Y'` and `'N'` go to neither.
#[test]
fn each_child_gets_the_rows_its_rules_give_it() {
    let (low, high) = (hash(false, 1), hash(true, 2));
    let mut roster = 7_i64.to_be_bytes().to_vec();
    roster.extend_from_slice(&high);
    let k = row(tag::DIRECTORY, &[0; 24]);
    let i = row(tag::POINTER, &[1; 24]);
    let h_low = row(tag::HEAD, &low);
    let q_high = row(tag::ROSTER, &roster);
    let rows = [
        k.clone(),
        i.clone(),
        h_low.clone(),
        q_high.clone(),
        row(tag::GARBAGE, &[2; 24]),
        row(tag::CURSOR, &[3; 2]),
        row(tag::REFCOUNT, &[4; 24]),
        row(tag::ORPHAN, &[5; 24]),
        row(tag::IDENTITY, &[]),
        row(b'N', &low),
    ];
    let children = [Range::of("0"), Range::of("1")];
    let routed = route(&rows, &children).unwrap();
    assert_eq!(routed[0], [k.clone(), i.clone(), h_low]);
    assert_eq!(routed[1], [k, i, q_high]);
}

/// **A source the helper cannot split is refused.** `'T'` (a retired parent), a tag with no
/// rule, a key outside the sentinel range, and a routed hash neither child holds.
#[test]
fn a_source_the_helper_cannot_split_is_refused() {
    let children = [Range::of("00"), Range::of("01")];
    let refused = |rows: &[Row]| route(rows, &children).unwrap_err();
    assert_eq!(refused(&[row(b'T', &[])]), PlanError::Retired);
    assert_eq!(refused(&[row(b'#', &[])]), PlanError::UnknownTag(b'#'));
    let plain = (vec![1, 2, 3], vec![]);
    assert_eq!(
        refused(std::slice::from_ref(&plain)),
        PlanError::NotSentinel(plain.0)
    );
    let outside = row(tag::HEAD, &hash(true, 3));
    assert_eq!(
        refused(std::slice::from_ref(&outside)),
        PlanError::Unrouted(outside.0)
    );
    let short = row(tag::HEAD, &[0; 3]);
    assert_eq!(
        refused(std::slice::from_ref(&short)),
        PlanError::Unrouted(short.0)
    );
}
