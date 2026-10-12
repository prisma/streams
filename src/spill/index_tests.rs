//! Index and refcount rows: the free tag, prefix isolation at the
//! adversarial hashes, the value codec, and random consecutive seals whose
//! index rows tile `[0, next)` exactly.
use std::collections::{BTreeMap, HashMap};

use bytes::Bytes;
use rand::{Rng, SeedableRng, rngs::StdRng};

use super::index::{
    INDEX_FIXED_LEN, INDEX_TAG, IndexError, IndexRow, REFCOUNT_TAG, RefcountRow, index_key, index_prefix,
    parse_index_key, refcount_key,
};
use super::object::{ChunkInput, ObjectId, decode_chunk, encode};
use super::test_pages::sealed_pages;
use crate::crypto_page::CheckedPage;

/// Every row family today's shard DB holds, built by its own constructor:
/// the byte after the 16-byte namespace is the family's tag.
fn todays_rows(hash: &[u8; 16]) -> Vec<(&'static str, Vec<u8>)> {
    let k = [3u8; 16];
    vec![
        ("tail", crate::shard::tail_key(hash)),
        ("seq", crate::shard::seq_key(hash, &k)),
        ("seal fence", crate::shard::seal_fence_key(hash)),
        ("producer", crate::shard::producer_key(hash, &k, "p")),
        ("billing meta", crate::billing::billing_meta_key(hash)),
        ("page", crate::crypto_page::shard_page_key(hash, 9)),
        ("cursor", crate::queue::cursor_key(hash, "g", 1)),
        ("lease", crate::queue::lease_key(hash, "g", 1, 9)),
        ("ack", crate::queue::ack_key(hash, "g", 1, 9)),
        ("consumer config", crate::queue::config_key(hash, "g")),
        ("consumer fence", crate::queue::fence_key(hash, "g")),
        ("dirty", crate::shard::dirty_key(hash)),
        ("shard maintenance", crate::shard::shard_maint_key()),
        ("usage dirty", crate::billing::usage_dirty_key(hash)),
        ("usage month final", crate::billing::usage_month_final_key(hash, 2026, 10)),
    ]
}

/// The survey behind the tag choice: no family uses `'i'` after a stream
/// hash, and none uses `'R'` after the 0xFF×16 sentinel.
#[test]
fn the_index_and_refcount_tags_are_free_in_todays_keyspace() {
    let hash = [0x42; 16];
    let mut stream_tags = std::collections::BTreeSet::new();
    let mut sentinel_tags = std::collections::BTreeSet::new();
    for (name, key) in todays_rows(&hash) {
        if key[..16] == [0xFF; 16] {
            sentinel_tags.insert(key[16]);
        } else {
            assert_eq!(key[..16], hash, "{name}");
            stream_tags.insert(key[16]);
        }
    }
    assert_eq!(stream_tags, b"BCFGclpqstx".iter().copied().collect());
    assert_eq!(sentinel_tags, b"DMUV".iter().copied().collect());
    assert!(!stream_tags.contains(&INDEX_TAG));
    assert!(!sentinel_tags.contains(&REFCOUNT_TAG));
    let r = refcount_key(ObjectId { epoch: 1, seq: 2 });
    assert_eq!((&r[..16], r[16], r.len()), (&[0xFF; 16][..], b'R', 33));
}

/// A prefix scan of one stream's index rows returns exactly its rows, with
/// stream hashes at the edges of the tag bytes (`0x4F`/`0x50`, the old
/// `'O'`/`'P'` leading tags) and at the sentinel itself, among every other
/// row family and refcount rows.
#[test]
fn index_prefix_scans_are_isolated_at_adversarial_hashes() {
    let mut hashes: Vec<[u8; 16]> = vec![[0x4F; 16], [0x50; 16], [0xFF; 16], [0x69; 16], [0x68; 16]];
    let mut h = [0x50u8; 16];
    h[15] = 0x4F;
    hashes.push(h);
    let mut h = [0xFFu8; 16];
    h[15] = 0xFE;
    hashes.push(h);
    let mut db: BTreeMap<Vec<u8>, &'static str> = BTreeMap::new();
    for hash in &hashes {
        for (name, key) in todays_rows(hash) {
            db.insert(key, name);
        }
        for last in [0u64, 1, 255, 256, u64::MAX] {
            db.insert(index_key(hash, last), "index");
        }
    }
    for seq in 0..5 {
        db.insert(refcount_key(ObjectId { epoch: 0xFF, seq }), "refcount");
    }
    for hash in &hashes {
        let prefix = index_prefix(hash);
        let rows: Vec<(&Vec<u8>, &&str)> = db.range(prefix.to_vec()..).take_while(|(k, _)| k.starts_with(&prefix)).collect();
        let lasts: Vec<u64> = rows.iter().map(|(k, name)| {
            assert_eq!(**name, "index", "{hash:02x?}: a {name} row in the index scan");
            let (h, last) = parse_index_key(k).unwrap();
            assert_eq!(&h, hash);
            last
        }).collect();
        assert_eq!(lasts, vec![0, 1, 255, 256, u64::MAX], "in offset order");
    }
    for (key, name) in &db {
        assert_eq!(parse_index_key(key).is_some(), *name == "index", "{name}");
    }
}

#[test]
fn index_rows_round_trip_and_refuse_malformed_values() {
    let row = IndexRow {
        object: ObjectId { epoch: 7, seq: 99 },
        chunk_off: 123_456,
        chunk_len: 1_000_000,
        first: 1_000,
        last: 2_999,
        page_count: 100,
        stored: 999_000,
        max_ts: -5,
        anchors: vec![
            super::object::Anchor { page: 27, offset: 1_189, byte: 270_640 },
            super::object::Anchor { page: 53, offset: 1_371, byte: 530_640 },
        ],
    };
    let raw = row.encode().unwrap();
    assert_eq!(raw.len(), INDEX_FIXED_LEN + 24);
    assert_eq!(IndexRow::decode(&raw).unwrap(), row);
    assert_eq!(row.start_for(1_000), None);
    assert_eq!(row.start_for(1_200).map(|a| a.page), Some(27));
    assert_eq!(row.start_for(2_999).map(|a| a.page), Some(53));
    let mut bad = raw.clone();
    bad[0] = 2;
    assert_eq!(IndexRow::decode(&bad), Err(IndexError::Version));
    assert_eq!(IndexRow::decode(&raw[..raw.len() - 1]), Err(IndexError::Length));
    assert_eq!(IndexRow::decode(&raw[..INDEX_FIXED_LEN - 1]), Err(IndexError::Length));
    let mut swapped = row.clone();
    swapped.anchors.reverse();
    assert_eq!(IndexRow::decode(&swapped.encode().unwrap()), Err(IndexError::Anchors));
    let mut outside = row.clone();
    outside.anchors[1].offset = 3_000;
    assert_eq!(IndexRow::decode(&outside.encode().unwrap()), Err(IndexError::Anchors));
    let mut backwards = row.clone();
    backwards.anchors[0].offset = 999;
    assert_eq!(backwards.encode(), Err(IndexError::Anchors));
    let rc = RefcountRow { live_bytes: 6 << 20, live_chunks: 200, created_ms: 1_760_000_000_000 };
    assert_eq!(RefcountRow::decode(&rc.encode()), Ok(rc));
    assert_eq!(RefcountRow::decode(&rc.encode()[1..]), Err(IndexError::Length));
}

/// One simulated stream: its pages by last offset, its next offset and its
/// absorbed boundary.
#[derive(Default)]
struct Stream {
    pages: BTreeMap<u64, Bytes>,
    next: u64,
    absorbed: u64,
}

/// Seal every stream with `absorbed < next`: one object, one chunk per
/// stream of its pages `[absorbed, next)`, one index row per chunk into
/// `db`; the object into `objects`. Returns the chunks sealed.
fn seal(
    streams: &mut BTreeMap<[u8; 16], Stream>,
    id: ObjectId,
    db: &mut BTreeMap<Vec<u8>, Vec<u8>>,
    objects: &mut HashMap<ObjectId, Bytes>,
) -> usize {
    let chunks: Vec<ChunkInput> = streams
        .iter()
        .filter(|(_, s)| s.absorbed < s.next)
        .map(|(hash, s)| ChunkInput {
            hash: *hash,
            first: s.absorbed,
            max_ts: s.next as i64,
            pages: s.pages.range(s.absorbed..).map(|(l, p)| (*l, p.clone())).collect(),
        })
        .collect();
    if chunks.is_empty() {
        return 0;
    }
    let enc = encode(&chunks).unwrap();
    for c in &enc.chunks {
        let row = IndexRow::for_chunk(id, c);
        assert!(db.insert(index_key(&c.entry.hash, c.entry.last), row.encode().unwrap()).is_none());
        streams.get_mut(&c.entry.hash).unwrap().absorbed = c.entry.last + 1;
    }
    objects.insert(id, enc.to_bytes());
    enc.chunks.len()
}

/// **Index rows of random consecutive seals tile `[0, next)` exactly.**
///
/// Random streams append real layout 5 pages; seals fire at random points
/// and take every stream's `[absorbed, next)`. Per stream, the index rows in
/// key order cover `[0, next)` with no gap and no overlap; each row's chunk,
/// fetched by its offset and length alone, decodes to exactly the pages
/// appended for `[first, last]`, each admitted at its offsets; the row's
/// page count, stored bytes and anchors match; and a seek to any offset
/// lands on the row holding it.
#[test]
fn index_rows_of_random_consecutive_seals_tile_the_stream() {
    for seed in 0..12u64 {
        let mut rng = StdRng::seed_from_u64(0x711E ^ seed);
        let mut streams: BTreeMap<[u8; 16], Stream> = BTreeMap::new();
        for _ in 0..rng.random_range(1..=10) {
            let mut h = [0u8; 16];
            rng.fill(&mut h[..]);
            if rng.random_bool(0.2) {
                h[..15].fill(*[0x4F, 0x50, 0xFF].get(rng.random_range(0..3)).unwrap());
            }
            streams.insert(h, Stream::default());
        }
        let hashes: Vec<[u8; 16]> = streams.keys().copied().collect();
        let mut db = BTreeMap::new();
        let mut objects = HashMap::new();
        let (mut seq, mut seals) = (0u64, 0usize);
        for _ in 0..rng.random_range(50..250) {
            let hash = hashes[rng.random_range(0..hashes.len())];
            let s = streams.get_mut(&hash).unwrap();
            let n = rng.random_range(1..=3);
            for (last, page) in sealed_pages(&mut rng, &hash, s.next, n, 30, 120) {
                s.pages.insert(last, page);
                s.next = last + 1;
            }
            if rng.random_bool(0.15) {
                seq += 1;
                seals += usize::from(seal(&mut streams, ObjectId { epoch: 1, seq }, &mut db, &mut objects) > 0);
            }
        }
        seq += 1;
        seal(&mut streams, ObjectId { epoch: 1, seq }, &mut db, &mut objects);
        assert!(seals > 0 || seq == 1, "seed {seed}: no seal before the last");
        for (hash, s) in &streams {
            let prefix = index_prefix(hash);
            let rows: Vec<(u64, IndexRow)> = db
                .range(prefix.to_vec()..)
                .take_while(|(k, _)| k.starts_with(&prefix))
                .map(|(k, v)| (parse_index_key(k).unwrap().1, IndexRow::decode(v).unwrap()))
                .collect();
            let mut expect = 0u64;
            for (key_last, row) in &rows {
                assert_eq!(row.first, expect, "seed {seed}: gap or overlap");
                assert_eq!(*key_last, row.last);
                let body = &objects[&row.object];
                let at = row.chunk_off as usize;
                let chunk = decode_chunk(&body.slice(at..at + row.chunk_len as usize)).unwrap();
                assert_eq!((chunk.hash, chunk.first, chunk.last), (*hash, row.first, row.last));
                let want: Vec<(u64, Bytes)> =
                    s.pages.range(row.first..=row.last).map(|(l, p)| (*l, p.clone())).collect();
                assert_eq!(chunk.pages, want, "seed {seed}: the chunk holds the appended pages");
                assert_eq!(chunk.pages.len(), row.page_count as usize);
                assert_eq!(chunk.pages.iter().map(|(_, p)| p.len() as u64).sum::<u64>(), row.stored);
                assert_eq!(chunk.anchors(), row.anchors);
                let mut next = row.first;
                for (last, raw) in &chunk.pages {
                    let page = CheckedPage::admit(raw.clone(), *last).unwrap();
                    assert_eq!(page.first(), next);
                    next = last + 1;
                }
                expect = row.last + 1;
            }
            assert_eq!(expect, s.next, "seed {seed}: the rows end at next");
            for _ in 0..50 {
                if s.next == 0 {
                    break;
                }
                let x = rng.random_range(0..s.next);
                let (k, v) = db.range(index_key(hash, x)..).next().unwrap();
                assert!(k.starts_with(&prefix));
                let row = IndexRow::decode(v).unwrap();
                assert!(row.first <= x && x <= row.last, "seed {seed}: seek {x}");
            }
        }
    }
}
