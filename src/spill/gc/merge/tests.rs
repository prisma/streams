#![cfg(test)]
//! The merged leaf's sentinel rows, tag by tag: unioned indexes and entity rows land once and
//! refuse differing values; per-object counts sum; dropped and fresh tags are not carried; an
//! unknown key refuses the rebuild.

use super::{MergeRefusal, Rows, merged_sentinel};
use crate::keyspace::tag;
use crate::spill::directory::sentinel_key;
use crate::spill::gc::{self, ObjectKind, Refcount};
use crate::spill::object::ObjectId;
use crate::spill::runindex::{Orphans, encode_orphans, orphan_key};

fn id(home: u32, seq: u64) -> ObjectId {
    ObjectId {
        home,
        epoch: 1,
        seq,
    }
}

fn rc(bytes: u64, chunks: u64, created: u64) -> Vec<u8> {
    gc::encode_refcount(&Refcount {
        live_bytes: bytes,
        live_chunks: chunks,
        created_ms: created,
        object_len: 100,
    })
}

fn rows(entries: &[(Vec<u8>, Vec<u8>)]) -> Rows {
    entries.iter().cloned().collect()
}

/// Both children carry the parent's copied `'K'` and `'I'` rows: a key both hold with equal values
/// lands once, byte-identical; a differing value is corruption and refuses the merge
/// (K-SPLIT-DECISION-2026-11-09 §3).
#[test]
fn a_directory_key_both_sources_hold_lands_once_and_a_differing_one_refuses() {
    let k = sentinel_key(tag::DIRECTORY, id(0, 7));
    let own = sentinel_key(tag::DIRECTORY, id(1, 2));
    let a = rows(&[
        (k.clone(), b"row-7".to_vec()),
        (own.clone(), b"row-1-2".to_vec()),
    ]);
    let b = rows(&[(k.clone(), b"row-7".to_vec())]);
    let m = merged_sentinel(&a, &b).unwrap();
    assert_eq!(
        m,
        rows(&[(k.clone(), b"row-7".to_vec()), (own, b"row-1-2".to_vec())])
    );
    let differs = rows(&[(k.clone(), b"row-7'".to_vec())]);
    assert_eq!(merged_sentinel(&a, &differs), Err(MergeRefusal::Differs(k)));
    let p = sentinel_key(tag::POINTER, id(0, 9));
    let (x, y) = (
        rows(&[(p.clone(), b"ptr".to_vec())]),
        rows(&[(p.clone(), b"ptr2".to_vec())]),
    );
    assert_eq!(merged_sentinel(&x, &y), Err(MergeRefusal::Differs(p)));
}

/// An object's counts sum across the sources (each counted its own range's chunks, L2): the
/// earlier creation, one length; a count only one source holds is carried; two lengths refuse.
#[test]
fn an_objects_counts_sum_and_two_lengths_refuse() {
    let shared = gc::refcount_key(id(0, 7));
    let only = gc::refcount_key(id(0, 8));
    let a = rows(&[(shared.clone(), rc(5, 1, 30)), (only.clone(), rc(9, 2, 10))]);
    let b = rows(&[(shared.clone(), rc(3, 1, 20))]);
    let m = merged_sentinel(&a, &b).unwrap();
    assert_eq!(
        m,
        rows(&[(shared.clone(), rc(8, 2, 20)), (only, rc(9, 2, 10))])
    );
    let other_len = gc::encode_refcount(&Refcount {
        live_bytes: 3,
        live_chunks: 1,
        created_ms: 20,
        object_len: 101,
    });
    let b = rows(&[(shared.clone(), other_len)]);
    assert_eq!(merged_sentinel(&a, &b), Err(MergeRefusal::Count(shared)));
    let o = orphan_key(id(0, 7));
    let orphans = |chunks, bytes| encode_orphans(Orphans { chunks, bytes });
    let (x, y) = (
        rows(&[(o.clone(), orphans(1, 10))]),
        rows(&[(o.clone(), orphans(2, 5))]),
    );
    assert_eq!(
        merged_sentinel(&x, &y).unwrap(),
        rows(&[(o, orphans(3, 15))])
    );
    // S4's split gives an 'O' row whole to the child holding its object's first entry, so a
    // merge of the two children meets it in one source and carries it whole (the sum agrees).
    let whole = orphan_key(id(0, 8));
    let one = rows(&[(whole.clone(), orphans(4, 40))]);
    assert_eq!(
        merged_sentinel(&one, &Rows::new()).unwrap(),
        rows(&[(whole, orphans(4, 40))])
    );
}

/// A source's own garbage, pending releases, cursors, identity and retirement are not carried; an
/// entity row of one source is; one held by both with differing values refuses.
#[test]
fn dropped_and_fresh_tags_are_not_carried_and_entity_rows_union() {
    let ff = crate::keyspace::SENTINEL.to_vec();
    let at = |t: u8, rest: &[u8]| [&ff[..], &[t], rest].concat();
    let dropped = [
        gc::garbage_key(id(1, 3)),
        gc::release_key(ObjectKind::Segment, id(0, 4)),
        gc::sweep_key(ObjectKind::Segment),
        at(b'C', &[0, 0, 0, 0, 0]),
        at(tag::IDENTITY, &[]),
        at(b'T', &[]),
    ];
    let head = gc::head_key(&[0x11; 16]);
    let a: Rows = dropped
        .iter()
        .map(|k| (k.clone(), b"x".to_vec()))
        .chain([(head.clone(), b"h".to_vec())])
        .collect();
    let b = rows(&[(gc::head_key(&[0x22; 16]), b"h2".to_vec())]);
    let m = merged_sentinel(&a, &b).unwrap();
    assert_eq!(
        m,
        rows(&[
            (head.clone(), b"h".to_vec()),
            (gc::head_key(&[0x22; 16]), b"h2".to_vec())
        ])
    );
    let both = rows(&[(head.clone(), b"other".to_vec())]);
    assert_eq!(merged_sentinel(&a, &both), Err(MergeRefusal::Differs(head)));
}

/// A key that is no sentinel row, or of a byte that is no sentinel tag, refuses the rebuild.
#[test]
fn an_unknown_row_refuses_the_rebuild() {
    let entity = [&[7u8; 16][..], b"S", b"t"].concat();
    let a = rows(&[(entity.clone(), b"x".to_vec())]);
    assert_eq!(
        merged_sentinel(&a, &Rows::new()),
        Err(MergeRefusal::Unknown(entity))
    );
    let unknown = [&crate::keyspace::SENTINEL[..], b"A"].concat();
    let a = rows(&[(unknown.clone(), b"x".to_vec())]);
    assert_eq!(
        merged_sentinel(&a, &Rows::new()),
        Err(MergeRefusal::Unknown(unknown))
    );
}

/// Counts whose sum would overflow refuse the merge rather than wrap (a wrapped count is a row
/// that releases its object too early).
#[test]
fn counts_that_would_overflow_refuse_the_merge() {
    let key = gc::refcount_key(id(0, 7));
    let big = |bytes, chunks| {
        gc::encode_refcount(&Refcount {
            live_bytes: bytes,
            live_chunks: chunks,
            created_ms: 1,
            object_len: u64::MAX,
        })
    };
    let (x, y) = (
        rows(&[(key.clone(), big(u64::MAX, 1))]),
        rows(&[(key.clone(), big(1, 1))]),
    );
    assert_eq!(
        merged_sentinel(&x, &y),
        Err(MergeRefusal::Count(key.clone()))
    );
    let (x, y) = (
        rows(&[(key.clone(), big(1, u64::MAX))]),
        rows(&[(key.clone(), big(1, 1))]),
    );
    assert_eq!(merged_sentinel(&x, &y), Err(MergeRefusal::Count(key)));
}
