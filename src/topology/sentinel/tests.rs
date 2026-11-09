#![cfg(test)]
//! The helper's routing (S4-DESIGN §7, S3a §2.8): every tag's rule, on rows of each kind.

use super::{PlanError, Row, check_sums, child_counts, route};
use crate::keyspace::range::Range;
use crate::keyspace::{SENTINEL, tag};
use crate::spill::directory::split::decode_row_in_range;
use crate::spill::directory::{KeySummary, directory_key, encode_row};
use crate::spill::gc::{self, Refcount};
use crate::spill::object::{ChunkInput, ObjectId, encode};
use crate::spill::runindex::{self, Orphans};
use crate::spill::test_pages::keyed_page;

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

const OBJECT: ObjectId = ObjectId {
    home: 0,
    epoch: 1,
    seq: 1,
};
const OTHER: ObjectId = ObjectId {
    home: 0,
    epoch: 1,
    seq: 2,
};

/// A one-page chunk of stream `hash`.
fn chunk(hash: [u8; 16]) -> ChunkInput {
    ChunkInput {
        hash,
        first: 0,
        max_ts: 7,
        pages: vec![keyed_page(&hash, "k", 0, &[b"r".to_vec()], 7)],
    }
}

/// **Each child counts exactly its own entries, and the two sum to the parent.** One `'K'` row of
/// `OBJECT` holds a chunk of a lower-half and an upper-half stream; the parent's `'R'` counts both,
/// and the run index holds one more upper-half entry of `OTHER`. Both objects have `'O'` rows (the
/// chunks a consolidation or a cleaner's copy superseded), which name no stream. The lower child
/// counts one chunk of `OBJECT` and takes its `'O'` (its first entry is the lower stream's); the
/// upper counts the other and `OTHER`'s, and takes `OTHER`'s `'O'`. Per object the two sum to the
/// parent's chunks and bytes, with its creation time and length.
#[test]
fn each_child_counts_its_own_entries_and_the_two_sum_to_the_parent() {
    let (low, high) = ([0x10; 16], [0x90; 16]);
    let inputs = [chunk(low), chunk(high)];
    let object = encode(&inputs).unwrap();
    let summaries: Vec<_> = inputs.iter().map(|c| KeySummary::of(c).unwrap()).collect();
    let k = encode_row(&[
        (&object.chunks[0], &summaries[0]),
        (&object.chunks[1], &summaries[1]),
    ]);
    let lens = [object.chunks[0].entry.len, object.chunks[1].entry.len].map(u64::from);
    let parent = Refcount {
        live_bytes: lens[0] + lens[1],
        live_chunks: 2,
        created_ms: 7,
        object_len: 900,
    };
    let orphans = Orphans {
        chunks: 3,
        bytes: 30,
    };
    let shared = Orphans {
        chunks: 1,
        bytes: 10,
    };
    let rows = [
        (directory_key(OBJECT), k),
        (gc::refcount_key(OBJECT), gc::encode_refcount(&parent)),
        (
            runindex::orphan_key(OBJECT),
            runindex::encode_orphans(shared),
        ),
        (
            runindex::orphan_key(OTHER),
            runindex::encode_orphans(orphans),
        ),
    ];
    let mut run = decode_row_in_range(OBJECT, &rows[0].1, Range::ALL).unwrap()[1].clone();
    run.1.object = OTHER;
    let (lower, upper) = (Range::of("0"), Range::of("1"));
    let lo = child_counts(&rows, std::slice::from_ref(&run), lower).unwrap();
    let hi = child_counts(&rows, std::slice::from_ref(&run), upper).unwrap();
    let row = |live_bytes, live_chunks, object_len| Refcount {
        live_bytes,
        live_chunks,
        created_ms: if object_len == 0 { 0 } else { 7 },
        object_len,
    };
    assert_eq!(lo.refcounts, [(OBJECT, row(lens[0], 1, 900))]);
    assert_eq!(lo.orphans, [(OBJECT, shared)], "one child takes it, whole");
    assert_eq!(
        hi.refcounts,
        [(OBJECT, row(lens[1], 1, 900)), (OTHER, row(lens[1], 1, 0))]
    );
    assert_eq!(hi.orphans, [(OTHER, orphans)]);
    let sum = lo.refcounts[0].1.live_bytes + hi.refcounts[0].1.live_bytes;
    assert_eq!((sum, 1 + 1), (parent.live_bytes, parent.live_chunks));
}

/// **A row that does not decode is refused.** A `'K'` row cut short.
#[test]
fn an_undecodable_count_row_is_refused() {
    let key = directory_key(OBJECT);
    let rows = [(key.clone(), vec![1, 2])];
    assert_eq!(
        child_counts(&rows, &[], Range::ALL),
        Err(PlanError::Corrupt(key))
    );
}

/// **The children's counts must sum to the source's.** The split of the two-stream row, whose `'O'`
/// row only the lower child (its first entry's) takes, checks out;
/// a child that lost a chunk of `OBJECT`, or one that counts an object the source does not, or an
/// `'O'` row a child dropped, is a mismatch naming that object.
#[test]
fn the_childrens_counts_must_sum_to_the_sources() {
    let (low, high) = ([0x10; 16], [0x90; 16]);
    let inputs = [chunk(low), chunk(high)];
    let object = encode(&inputs).unwrap();
    let summaries: Vec<_> = inputs.iter().map(|c| KeySummary::of(c).unwrap()).collect();
    let k = encode_row(&[
        (&object.chunks[0], &summaries[0]),
        (&object.chunks[1], &summaries[1]),
    ]);
    let lens = [object.chunks[0].entry.len, object.chunks[1].entry.len].map(u64::from);
    let parent = Refcount {
        live_bytes: lens[0] + lens[1],
        live_chunks: 2,
        created_ms: 7,
        object_len: 900,
    };
    let orphans = Orphans {
        chunks: 3,
        bytes: 30,
    };
    let rows = [
        (directory_key(OBJECT), k),
        (gc::refcount_key(OBJECT), gc::encode_refcount(&parent)),
        (
            runindex::orphan_key(OBJECT),
            runindex::encode_orphans(orphans),
        ),
    ];
    let children = [Range::of("0"), Range::of("1")].map(|r| child_counts(&rows, &[], r).unwrap());
    assert_eq!(check_sums(&rows, &children), Ok(()));
    let mut short = children.clone();
    short[1].refcounts.clear();
    assert_eq!(check_sums(&rows, &short), Err(PlanError::Mismatch(OBJECT)));
    let mut extra = children.clone();
    extra[0].refcounts.push((OTHER, parent));
    assert_eq!(check_sums(&rows, &extra), Err(PlanError::Mismatch(OTHER)));
    let taken = (children[0].orphans.len(), children[1].orphans.len());
    assert_eq!(
        taken,
        (1, 0),
        "only the first entry's child takes the 'O' row"
    );
    let mut lost = children;
    lost.iter_mut().for_each(|c| c.orphans.clear());
    assert_eq!(check_sums(&rows, &lost), Err(PlanError::Mismatch(OBJECT)));
}
