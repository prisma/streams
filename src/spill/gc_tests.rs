#![cfg(test)]
//! Package 6's rows and liveness rule (`spill::gc`).

use super::directory::{DirEntry, KeySummary};
use super::gc::{
    Decrements, GARBAGE_TAG, Garbage, HEAD_TAG, Head, HeadState, ObjectKind, Refcount, SWEEP_TAG,
    decode_garbage, decode_head, decode_refcount, decrement, encode_garbage, encode_head,
    encode_refcount, garbage_key, head_key, less, live_entries, parse_head_key,
};
use super::object::ObjectId;

fn id(seq: u64) -> ObjectId {
    ObjectId { epoch: 3, seq }
}

fn entry(object: u64, off: u64, first: u64, last: u64, len: u32) -> DirEntry {
    DirEntry {
        object: id(object),
        chunk_off: off,
        chunk_len: len,
        first,
        last,
        page_count: 1,
        keys: KeySummary::Keys(Vec::new()),
        anchors: Vec::new(),
    }
}

#[test]
fn rows_round_trip_and_refuse_every_other_byte_string() {
    let r = Refcount {
        live_bytes: 300,
        live_chunks: 2,
        created_ms: 1_700_000_000_000,
        object_len: 420,
    };
    assert_eq!(decode_refcount(&encode_refcount(&r)), Some(r));
    let g = Garbage {
        kind: ObjectKind::RunIndex,
        object_len: 77,
        due_ms: 9,
    };
    assert_eq!(decode_garbage(&encode_garbage(&g)), Some(g));
    let h = Head {
        state: HeadState::Purged,
        not_before_ms: 5,
        head: 900,
        reclaimed: 400,
    };
    assert_eq!(decode_head(&encode_head(&h)), Some(h));
    // A trailing byte, a short value, another version: refused.
    let mutations = |raw: Vec<u8>| {
        let mut long = raw.clone();
        long.push(0);
        let mut other = raw.clone();
        other[0] = 2;
        [long, other, raw[..raw.len() - 1].to_vec()]
    };
    for bad in mutations(encode_refcount(&r)) {
        assert_eq!(decode_refcount(&bad), None);
    }
    for bad in mutations(encode_garbage(&g)) {
        assert_eq!(decode_garbage(&bad), None);
    }
    for bad in mutations(encode_head(&h)) {
        assert_eq!(decode_head(&bad), None);
    }
    // A row of no live chunk, more live bytes than the object, a head
    // reclaimed past itself: not rows any writer makes.
    let empty = Refcount {
        live_chunks: 0,
        ..r
    };
    assert_eq!(decode_refcount(&encode_refcount(&empty)), None);
    let over = Refcount {
        live_bytes: 421,
        ..r
    };
    assert_eq!(decode_refcount(&encode_refcount(&over)), None);
    let past = Head {
        reclaimed: 901,
        ..h
    };
    assert_eq!(decode_head(&encode_head(&past)), None);
}

#[test]
fn head_keys_name_their_stream() {
    let hash = [9u8; 16];
    assert_eq!(parse_head_key(&head_key(&hash)), Some(hash));
    assert_eq!(head_key(&hash).len(), 34);
    assert_eq!(parse_head_key(&garbage_key(id(1))), None);
}

#[test]
fn live_entries_drop_superseded_and_duplicate_chunks() {
    // Eight originals in objects 1 and 2, consolidated into object 9; the
    // folded copy of one original seen twice (directory and run index).
    let mut entries: Vec<DirEntry> = (0..8)
        .map(|i| entry(1 + i / 4, i * 100, i * 10, i * 10 + 9, 100))
        .collect();
    entries.push(entry(1, 0, 0, 9, 100));
    entries.push(entry(9, 0, 0, 79, 700));
    entries.push(entry(5, 0, 80, 89, 50));
    let live = live_entries(entries).unwrap();
    let spans: Vec<(u64, u64, u64)> = live
        .iter()
        .map(|e| (e.object.seq, e.first, e.last))
        .collect();
    assert_eq!(spans, vec![(9, 0, 79), (5, 80, 89)]);
}

#[test]
fn two_chunks_of_one_span_are_refused() {
    let err = live_entries(vec![entry(1, 0, 0, 9, 10), entry(2, 0, 0, 9, 10)]).unwrap_err();
    assert!(err.contains("two chunks hold [0, 9]"), "{err}");
}

#[test]
fn decrements_sum_per_object_and_never_go_below_zero() {
    let mut d = Decrements::new();
    decrement(&mut d, &entry(1, 0, 0, 9, 10));
    decrement(&mut d, &entry(1, 10, 10, 19, 20));
    decrement(&mut d, &entry(2, 0, 20, 29, 5));
    assert_eq!(d.get(&id(1)), Some(&(30, 2)));
    assert_eq!(d.get(&id(2)), Some(&(5, 1)));
    let r = Refcount {
        live_bytes: 30,
        live_chunks: 2,
        created_ms: 0,
        object_len: 64,
    };
    assert_eq!(
        less(r, (30, 2)).map(|r| (r.live_bytes, r.live_chunks)),
        Some((0, 0))
    );
    assert_eq!(less(r, (31, 2)), None);
    assert_eq!(less(r, (30, 3)), None);
}

#[test]
fn every_gc_tag_sorts_below_the_usage_finals() {
    // The drain scans 'S' ‖ 0xFF×16 ‖ 'V' ‖ hash per billed segment; a
    // sentinel key above 'V' would make each scan read a data block of
    // every SST that holds one (package 5's S10 finding).
    for tag in [
        GARBAGE_TAG,
        HEAD_TAG,
        SWEEP_TAG,
        crate::spill::directory::REFCOUNT_TAG,
    ] {
        assert!(tag < b'V', "tag {} sorts at or above 'V'", tag as char);
    }
}

/// A segment object's refcount row: the format version, then its live
/// bytes, live chunks, creation instant and object length as minimal
/// varints, at pinned bytes.
#[test]
fn a_refcount_row_holds_its_live_bytes_chunks_creation_and_length_at_pinned_bytes() {
    let row = encode_refcount(&Refcount {
        live_bytes: 300,
        live_chunks: 2,
        created_ms: 1_760_000_000_000,
        object_len: 420,
    });
    assert_eq!(crate::crypto::hex(&row), "01ac02028080b3c19c33a403");
}
