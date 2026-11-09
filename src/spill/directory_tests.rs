//! The seal directory's rows: round trips with every key-summary shape, the
//! pinned bytes, and refusal of every malformed row.
#![cfg(test)]
use bytes::Bytes;

use super::directory::{
    DirEntry, Directory, KeySummary, MAX_SUMMARY_KEYS, decode_row, directory_key, encode_row,
    fingerprint, parse_directory_key,
};
use super::object::{ChunkInput, ObjectId, encode};
use super::test_pages::keyed_page;

const OBJECT: ObjectId = ObjectId {
    home: 2,
    epoch: 3,
    seq: 9,
};

/// A chunk of `hash` from `first` with one record per routing key in `keys`.
fn chunk(hash: [u8; 16], first: u64, keys: &[String]) -> ChunkInput {
    let pages: Vec<(u64, Bytes)> = keys
        .iter()
        .enumerate()
        .map(|(i, key)| keyed_page(&hash, key, first + i as u64, &[b"r".to_vec()], 7))
        .collect();
    ChunkInput {
        hash,
        first,
        max_ts: 7,
        pages,
    }
}

fn keys(n: usize) -> Vec<String> {
    (0..n).map(|i| format!("k{}", i % 20)).collect()
}

#[test]
fn a_row_round_trips_every_key_summary_shape() {
    let inputs = [
        chunk([1; 16], 0, &keys(1)),
        chunk([2; 16], 40, &keys(MAX_SUMMARY_KEYS)),
        chunk([3; 16], 7, &keys(MAX_SUMMARY_KEYS + 1)),
        chunk([4; 16], 0, &[String::new(), "a".into(), String::new()]),
    ];
    let object = encode(&inputs).unwrap();
    let summaries: Vec<KeySummary> = inputs.iter().map(|c| KeySummary::of(c).unwrap()).collect();
    assert!(matches!(&summaries[0], KeySummary::Keys(k) if k.len() == 1));
    assert!(matches!(&summaries[1], KeySummary::Keys(k) if k.len() == MAX_SUMMARY_KEYS));
    assert!(matches!(&summaries[2], KeySummary::Bloom(b) if b.len() == (17 * 10usize).div_ceil(8)));
    assert!(matches!(&summaries[3], KeySummary::Keys(k) if k.len() == 2));
    let pairs: Vec<_> = object.chunks.iter().zip(&summaries).collect();
    let row = encode_row(&pairs);
    let decoded = decode_row(OBJECT, &row).unwrap();
    for ((hash, entry), ((chunk, keys), input)) in decoded.iter().zip(pairs.iter().zip(&inputs)) {
        assert_eq!(*hash, input.hash);
        assert_eq!(*entry, DirEntry::of(OBJECT, chunk, (*keys).clone()));
    }
    assert!(summaries[3].may_hold("") && summaries[3].may_hold("a"));
    assert!(!summaries[3].may_hold("b"), "a key the chunk does not hold");
    for key in keys(MAX_SUMMARY_KEYS + 1) {
        assert!(
            summaries[2].may_hold(&key),
            "a bloom never refuses a key it holds: {key}"
        );
    }
    let mut directory = Directory::default();
    for (hash, entry) in decoded {
        directory.insert(hash, entry);
    }
    assert_eq!((directory.entries(), directory.streams()), (4, 4));
    assert_eq!(directory.from(&[2; 16], 45, 8).len(), 1);
    assert!(
        directory.from(&[2; 16], 56, 8).is_empty(),
        "past the chunk's last"
    );
}

#[test]
fn the_row_and_key_bytes_are_pinned() {
    assert_eq!(
        crate::crypto::hex(&directory_key(OBJECT)),
        concat!(
            "ffffffffffffffffffffffffffffffff",
            "4b",
            "00000002",
            "0000000000000003",
            "0000000000000009"
        )
    );
    assert_eq!(parse_directory_key(&directory_key(OBJECT)), Some(OBJECT));
    assert_eq!(
        fingerprint(""),
        u64::from_be_bytes(
            crate::crypto::RoutingKeyHash::of("").0[..8]
                .try_into()
                .unwrap()
        )
    );
    let input = chunk([1; 16], 5, &["lane".into()]);
    let object = encode(std::slice::from_ref(&input)).unwrap();
    let summary = KeySummary::of(&input).unwrap();
    let row = encode_row(&[(&object.chunks[0], &summary)]);
    let len = object.chunks[0].entry.len;
    let mut expected = vec![1, 1];
    expected.extend_from_slice(&[1; 16]);
    expected.extend_from_slice(&[5, 0]);
    crate::crypto_page::put_varint(&mut expected, u64::from(len));
    expected.push(1);
    crate::crypto_page::put_varint(&mut expected, u64::try_from(input.max_ts).unwrap());
    expected.push(1);
    expected.extend_from_slice(&fingerprint("lane").to_be_bytes());
    expected.push(0);
    assert_eq!(
        row, expected,
        "ver, 1 chunk, hash, first, span, len, pages, max_ts, 1 key, no anchor"
    );
}

#[test]
fn every_malformed_row_is_refused() {
    let input = chunk([1; 16], 0, &keys(3));
    let object = encode(std::slice::from_ref(&input)).unwrap();
    let row = encode_row(&[(&object.chunks[0], &KeySummary::of(&input).unwrap())]);
    for end in 0..row.len() {
        assert!(
            decode_row(OBJECT, &row[..end]).is_none(),
            "a {end}-byte prefix"
        );
    }
    let mut trailing = row.clone();
    trailing.push(0);
    assert!(decode_row(OBJECT, &trailing).is_none());
    let mut version = row.clone();
    version[0] = 2;
    assert!(decode_row(OBJECT, &version).is_none());
    // The key count sits after hash (16), first, span, len (2 B), pages and max_ts (1 B).
    let at = 2 + 16 + 1 + 1 + 2 + 1 + 1;
    let mut too_many = row.clone();
    too_many[at] = u8::try_from(MAX_SUMMARY_KEYS + 1).unwrap();
    assert!(
        decode_row(OBJECT, &too_many).is_none(),
        "more keys than a summary names"
    );
    let mut unsorted = row;
    let (a, b) = (at + 1, at + 9);
    let first: [u8; 8] = unsorted[a..a + 8].try_into().unwrap();
    unsorted.copy_within(b..b + 8, a);
    unsorted[b..b + 8].copy_from_slice(&first);
    assert!(decode_row(OBJECT, &unsorted).is_none(), "unsorted keys");
}

#[test]
fn a_bloom_is_capped_and_its_false_positive_rate_is_about_one_percent() {
    let many: Vec<String> = (0..5_000).map(|i| format!("key-{i}")).collect();
    let pages: Vec<(u64, Bytes)> = many
        .iter()
        .enumerate()
        .map(|(i, key)| keyed_page(&[9; 16], key, i as u64, &[b"r".to_vec()], 7))
        .collect();
    let input = ChunkInput {
        hash: [9; 16],
        first: 0,
        max_ts: 7,
        pages,
    };
    let KeySummary::Bloom(bloom) = KeySummary::of(&input).unwrap() else {
        panic!("a bloom above the cap");
    };
    assert_eq!(bloom.len(), super::directory::MAX_BLOOM_BYTES, "capped");
    let summary = KeySummary::Bloom(bloom);
    assert!(
        many.iter().all(|key| summary.may_hold(key)),
        "no false negative"
    );
    let some: Vec<String> = (0..20).map(|i| format!("key-{i}")).collect();
    let pages: Vec<(u64, Bytes)> = some
        .iter()
        .enumerate()
        .map(|(i, key)| keyed_page(&[9; 16], key, i as u64, &[b"r".to_vec()], 7))
        .collect();
    let summary = KeySummary::of(&ChunkInput {
        hash: [9; 16],
        first: 0,
        max_ts: 7,
        pages,
    })
    .unwrap();
    let false_positives = (0..10_000)
        .filter(|i| summary.may_hold(&format!("other-{i}")))
        .count();
    assert!(
        false_positives < 300,
        "about 1% at 10 bits per key: {false_positives} of 10,000"
    );
}

/// The directory's memory gauge is what its streams hold through inserts,
/// a re-insert of the same chunk, a removal and a removal that names another
/// object (which removes nothing); a bloom entry counts its bloom.
#[test]
fn the_memory_gauge_is_the_sum_of_the_entries_through_inserts_and_removals() {
    let inputs = [
        chunk([1; 16], 0, &keys(1)),
        chunk([2; 16], 0, &keys(MAX_SUMMARY_KEYS + 1)),
    ];
    let object = encode(&inputs).unwrap();
    let entries: Vec<DirEntry> = object
        .chunks
        .iter()
        .zip(&inputs)
        .map(|(c, input)| DirEntry::of(OBJECT, c, KeySummary::of(input).unwrap()))
        .collect();
    let mut directory = Directory::default();
    let sum = |d: &Directory| d.recounted_bytes();
    directory.insert([1; 16], entries[0].clone());
    let one = directory.bytes();
    assert!(one > 0);
    directory.insert([2; 16], entries[1].clone());
    assert_eq!(directory.bytes(), sum(&directory));
    assert!(
        super::directory::process_bytes() >= directory.bytes(),
        "the process gauge counts this directory"
    );
    assert!(
        directory.bytes() - one > one,
        "the bloom entry counts its bloom"
    );
    directory.insert([2; 16], entries[1].clone());
    assert_eq!(
        (directory.entries(), directory.bytes()),
        (2, sum(&directory)),
        "a re-insert replaces"
    );
    directory.remove(
        &[2; 16],
        entries[1].last,
        ObjectId {
            home: 0,
            epoch: 3,
            seq: 10,
        },
    );
    assert_eq!(
        directory.entries(),
        2,
        "a removal naming another object removes nothing"
    );
    directory.remove(&[2; 16], entries[1].last, OBJECT);
    assert_eq!((directory.entries(), directory.bytes()), (1, one));
    directory.remove(&[1; 16], entries[0].last, OBJECT);
    assert_eq!((directory.entries(), directory.bytes()), (0, 0));
}

/// One stream's memory is its slot and capacity, eight bytes per summary key
/// or one per bloom byte, and its anchors.
#[test]
fn an_entrys_memory_counts_its_summary_and_anchors_exactly() {
    use super::object::Anchor;
    let entry = |keys, anchors| DirEntry {
        object: OBJECT,
        chunk_off: 0,
        chunk_len: 0,
        first: 0,
        last: 9,
        page_count: 1,
        max_ts_ms: 0,
        keys,
        anchors: vec![
            Anchor {
                page: 1,
                offset: 1,
                byte: 1,
            };
            anchors
        ],
    };
    let base = usize::try_from(super::directory::stream_charge(1)).unwrap();
    let memory = |e: DirEntry| {
        let mut d = Directory::default();
        d.insert([7; 16], e);
        usize::try_from(d.bytes()).unwrap()
    };
    assert_eq!(
        memory(entry(KeySummary::Keys(vec![1, 2, 3]), 2)),
        base + 3 * 8 + 2 * size_of::<Anchor>()
    );
    assert_eq!(memory(entry(KeySummary::Bloom(vec![0; 64]), 0)), base + 64);
}

/// One tiny entry, as a seal of one record makes.
fn tiny(last: u64) -> DirEntry {
    DirEntry {
        object: OBJECT,
        chunk_off: 0,
        chunk_len: 0,
        first: last,
        last,
        page_count: 1,
        max_ts_ms: 0,
        keys: KeySummary::Keys(vec![1]),
        anchors: Vec::new(),
    }
}

/// A stream is charged what it allocates: its map slot, its entries' capacity
/// and each entry's own heap, exactly, through a capacity's growth and until
/// its last entry leaves.
#[test]
fn a_stream_is_charged_its_slot_its_capacity_and_its_entries_heap() {
    use super::directory::{entry_own_heap, stream_charge};
    // A slot is its 16-byte key, its 24-byte `Vec` header and a control byte.
    let entry = size_of::<DirEntry>() as u64;
    assert_eq!(
        [stream_charge(0), stream_charge(1), stream_charge(4)],
        [41, 41 + entry, 41 + 4 * entry]
    );
    let mut d = Directory::default();
    let own = entry_own_heap(&tiny(0));
    assert_eq!(own, 8, "one summary key");
    d.insert([1; 16], tiny(0));
    assert_eq!(d.bytes(), stream_charge(1) + own, "one entry");
    d.insert([1; 16], tiny(1));
    assert_eq!(d.bytes(), stream_charge(2) + 2 * own, "two entries");
    d.insert([1; 16], tiny(2));
    assert_eq!(d.bytes(), stream_charge(4) + 3 * own, "three, capacity 4");
    d.insert([1; 16], tiny(1));
    assert_eq!(
        d.bytes(),
        stream_charge(4) + 3 * own,
        "a re-insert replaces"
    );
    d.remove(&[1; 16], 1, OBJECT);
    assert_eq!(d.bytes(), stream_charge(4) + 2 * own, "capacity is kept");
    d.remove(&[1; 16], 0, OBJECT);
    d.remove(&[1; 16], 2, OBJECT);
    assert_eq!((d.entries(), d.streams(), d.bytes()), (0, 0, 0));
}

/// A stream's chunks read in order of (last offset, object) wherever they
/// were inserted, from an offset, at most `max`.
#[test]
fn a_streams_entries_read_by_last_offset_and_object() {
    let mut d = Directory::default();
    let hash = [2; 16];
    for last in [5, 1, 9, 3] {
        d.insert(hash, tiny(last));
    }
    let mut other = tiny(5);
    other.object = ObjectId {
        home: 0,
        epoch: 3,
        seq: 8,
    };
    d.insert(hash, other);
    let lasts = |d: &Directory, from, max| -> Vec<(u64, u64)> {
        d.from(&hash, from, max)
            .iter()
            .map(|e| (e.last, e.object.seq))
            .collect()
    };
    assert_eq!(lasts(&d, 0, 9), [(1, 9), (3, 9), (5, 8), (5, 9), (9, 9)]);
    assert_eq!(
        lasts(&d, 3, 2),
        [(3, 9), (5, 8)],
        "from includes an entry ending there"
    );
    assert_eq!(lasts(&d, 4, 9), [(5, 8), (5, 9), (9, 9)]);
    assert_eq!(lasts(&d, 10, 9), []);
    d.remove(&hash, 5, OBJECT);
    assert_eq!(
        lasts(&d, 5, 9),
        [(5, 8), (9, 9)],
        "only that object's entry left"
    );
    assert!(d.from(&[9; 16], 0, 9).is_empty(), "an unknown stream");
}

/// 10,000 single-chunk streams, the shape a population of tiny streams seals:
/// charged under 200 B each, and the charge within 1.5x of what the allocator
/// holds (a BTreeMap per stream was a 1.5 KiB leaf node charged 116 B).
#[test]
fn single_chunk_streams_are_charged_what_they_allocate() {
    let mut d = Directory::default();
    let before = crate::test_alloc::live_bytes();
    for n in 0..10_000u64 {
        let mut hash = [0u8; 16];
        hash[..8].copy_from_slice(&n.to_le_bytes());
        d.insert(hash, tiny(0));
    }
    let held = u64::try_from(crate::test_alloc::live_bytes() - before).unwrap();
    assert!(d.bytes() <= 10_000 * 200, "charged {} B", d.bytes());
    assert!(
        held * 2 <= d.bytes() * 3,
        "the allocator holds {held} B against a charge of {} B",
        d.bytes()
    );
}

/// A row a split copied keeps each kept chunk's offset (K-SPLIT-DECISION-2026-11-09): of a row of
/// two chunks, the second stream's entry read into its own range alone starts where the first
/// chunk ends, not at 0 as a row re-encoded without the first would say.
#[test]
fn a_row_decoded_into_a_range_keeps_each_kept_chunks_offset() {
    use crate::keyspace::range::Range;
    use crate::spill::directory::split::decode_row_in_range;
    let (low, high) = ([0x10; 16], [0x90; 16]);
    let inputs = [chunk(low, 0, &["x".into()]), chunk(high, 0, &["y".into()])];
    let object = encode(&inputs).unwrap();
    let summaries: Vec<_> = inputs.iter().map(|c| KeySummary::of(c).unwrap()).collect();
    let row = encode_row(&[
        (&object.chunks[0], &summaries[0]),
        (&object.chunks[1], &summaries[1]),
    ]);
    let kept = decode_row_in_range(OBJECT, &row, Range::of("1")).unwrap();
    let first_len = u64::from(object.chunks[0].entry.len);
    let got: Vec<_> = kept
        .iter()
        .map(|(h, e)| (*h, e.chunk_off, e.chunk_len))
        .collect();
    assert_eq!(got, [(high, first_len, object.chunks[1].entry.len)]);
    assert_eq!(
        decode_row_in_range(OBJECT, &row, Range::ALL).unwrap().len(),
        2
    );
    assert!(decode_row_in_range(OBJECT, &row[..row.len() - 1], Range::ALL).is_none());
}
