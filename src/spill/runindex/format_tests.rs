//! The run-index object and pointer rows: round trips, the pinned bytes
//! (golden vectors: the format is final under layout 6), every single-bit
//! flip refused, and refusal of every malformed object the writer cannot
//! produce.
use super::format::{
    DENSE, FormatError, MAX_RUN_BLOOM_BYTES, ObjectWriter, RUN_BLOOM_BITS_PER_KEY, TRAILER_LEN,
    decode_object, decode_run, encode_run, run_bloom,
};
use super::{
    ORPHAN_TAG, Orphans, POINTER_TAG, Pointer, decode_orphans, decode_pointer, encode_orphans,
    encode_pointer, orphan_key, parse_sentinel_key, pointer_key, run_index_path,
};
use crate::spill::directory::{DirEntry, KeySummary};
use crate::spill::object::{Anchor, ObjectId};

fn entry(seq: u64, first: u64, last: u64, keys: KeySummary) -> DirEntry {
    DirEntry {
        object: ObjectId { epoch: 2, seq },
        chunk_off: seq * 100,
        chunk_len: 90,
        first,
        last,
        page_count: u32::try_from(last - first + 1).unwrap(),
        keys,
        anchors: Vec::new(),
    }
}

fn fps(keys: &[u64]) -> KeySummary {
    KeySummary::Keys(keys.to_vec())
}

/// Two runs: a dense one of two chunks, and one with a gap whose second
/// chunk carries an anchor and a bloom summary.
type Runs = Vec<([u8; 16], Vec<DirEntry>)>;

fn sample() -> (Vec<u8>, Runs) {
    let mut gapped = entry(7, 40, 600, KeySummary::Bloom(vec![0xA5; 3]));
    gapped.anchors = vec![Anchor {
        page: 3,
        offset: 300,
        byte: 70,
    }];
    let runs = vec![
        (
            [1; 16],
            vec![entry(1, 0, 9, fps(&[5])), entry(2, 10, 12, fps(&[5, 9]))],
        ),
        ([9; 16], vec![entry(3, 0, 3, fps(&[1])), gapped]),
    ];
    let mut w = ObjectWriter::new();
    for (hash, entries) in &runs {
        w.push(*hash, entries).unwrap();
    }
    let (bytes, _) = w.finish(1).unwrap().unwrap();
    (bytes, runs)
}

#[test]
fn an_object_round_trips_its_runs_and_footer() {
    let (bytes, runs) = sample();
    let (trailer, decoded) = decode_object(&bytes).unwrap();
    assert_eq!(trailer.level, 1);
    assert_eq!(trailer.runs, 2);
    assert_eq!(trailer.entries, 4);
    assert_eq!(decoded.len(), 2);
    for ((footer, entries), (hash, want)) in decoded.iter().zip(&runs) {
        assert_eq!(footer.hash, *hash);
        assert_eq!(entries, want);
        assert_eq!(footer.first, want[0].first);
        assert_eq!(footer.last, want[want.len() - 1].last);
    }
    assert!(decoded[0].0.dense() && !decoded[1].0.dense());
    assert!(decoded[0].0.may_hold(5) && decoded[0].0.may_hold(9));
    assert!(
        decoded[1].0.bloom.is_empty(),
        "a bloom-summarised chunk leaves the run's keys unknown"
    );
    assert!(decoded[1].0.may_hold(12345));
}

/// The pinned bytes of `sample()`: any change to the format changes them,
/// and the format is final under one `LAYOUT_VERSION`.
const GOLDEN: &str = concat!(
    "020201645a00090a010000000000000005000202c8015a0a02030200000000000000050000000000",
    "000009008b95ff9d020203ac025a000304010000000000000001000207bc055a28b004b1040003a5",
    "a5a50103840246c9b151000101010101010101010101010101010100000000000000003000000000",
    "000000000000000c00000000000000020000000103003acfe1090909090909090909090909090909",
    "0930000000000000002b00000000000000000000005802000000000000020000000000005b000000",
    "000000006900000002000000040000000000000001000100fd9c21af2de099924c367269",
);

#[test]
fn an_object_is_pinned_byte_for_byte() {
    let (bytes, _) = sample();
    let hex: String = bytes.iter().map(|b| format!("{b:02x}")).collect();
    assert_eq!(hex, GOLDEN, "the run-index object's bytes changed");
}

#[test]
fn every_single_bit_flip_is_refused() {
    let (bytes, _) = sample();
    for i in 0..bytes.len() * 8 {
        let mut flipped = bytes.clone();
        flipped[i / 8] ^= 1 << (i % 8);
        assert!(
            decode_object(&flipped).is_none(),
            "bit {i} flipped and still decoded"
        );
    }
}

#[test]
fn a_truncated_or_extended_object_is_refused() {
    let (bytes, _) = sample();
    for cut in 0..bytes.len() {
        assert!(decode_object(&bytes[..cut]).is_none(), "cut at {cut}");
    }
    let mut longer = vec![0u8];
    longer.extend_from_slice(&bytes);
    assert!(decode_object(&longer).is_none());
}

#[test]
fn the_writer_refuses_what_the_format_cannot_hold() {
    let mut w = ObjectWriter::new();
    w.push([5; 16], &[entry(1, 0, 3, fps(&[1]))]).unwrap();
    assert_eq!(
        w.push([5; 16], &[entry(1, 4, 5, fps(&[1]))]),
        Err(FormatError::HashOrder)
    );
    assert_eq!(
        w.push([4; 16], &[entry(1, 4, 5, fps(&[1]))]),
        Err(FormatError::HashOrder)
    );
    assert_eq!(w.push([6; 16], &[]), Err(FormatError::RunOrder));
    let contained = [entry(1, 0, 9, fps(&[1])), entry(2, 2, 9, fps(&[1]))];
    assert_eq!(w.push([6; 16], &contained), Err(FormatError::RunOrder));
    let same_first = [entry(1, 0, 3, fps(&[1])), entry(2, 0, 9, fps(&[1]))];
    assert_eq!(encode_run(&same_first), Err(FormatError::RunOrder));
    assert!(matches!(w.finish(3), Err(FormatError::Level)));
    assert!(
        ObjectWriter::new().finish(0).unwrap().is_none(),
        "no run, no object"
    );
}

#[test]
fn overlapping_and_gapped_entries_form_a_run() {
    let run = [
        entry(1, 0, 9, fps(&[1])),
        entry(2, 5, 19, fps(&[2])),
        entry(3, 40, 41, fps(&[3])),
    ];
    let raw = encode_run(&run).unwrap();
    assert_eq!(decode_run(&raw).unwrap(), run);
}

#[test]
fn a_run_bloom_never_excludes_a_key_its_run_holds() {
    let keys: Vec<u64> = (0..600u64)
        .map(|i| i.wrapping_mul(0x9e37_79b9_7f4a_7c15))
        .collect();
    let entries: Vec<DirEntry> = keys
        .chunks(16)
        .enumerate()
        .map(|(i, k)| {
            let mut k = k.to_vec();
            k.sort_unstable();
            entry(
                i as u64,
                i as u64 * 10,
                i as u64 * 10 + 9,
                KeySummary::Keys(k),
            )
        })
        .collect();
    let bloom = run_bloom(&entries);
    assert_eq!(bloom.len(), (600 * RUN_BLOOM_BITS_PER_KEY).div_ceil(8));
    let mut w = ObjectWriter::new();
    w.push([1; 16], &entries).unwrap();
    let (bytes, _) = w.finish(0).unwrap().unwrap();
    let (_, runs) = decode_object(&bytes).unwrap();
    let footer = &runs[0].0;
    assert_eq!(footer.flags, DENSE);
    for k in &keys {
        assert!(footer.may_hold(*k), "a held key {k:x} excluded");
    }
    let false_positives = (0..20_000u64)
        .filter(|i| footer.may_hold(i ^ 0x5555))
        .count();
    assert!(
        false_positives < 400,
        "{false_positives} false positives in 20,000 at 10 bits per key"
    );
}

#[test]
fn a_run_of_too_many_keys_carries_no_bloom() {
    let n = MAX_RUN_BLOOM_BYTES * 8 / RUN_BLOOM_BITS_PER_KEY + 1;
    let entries: Vec<DirEntry> = (0..n as u64).map(|i| entry(i, i, i, fps(&[i]))).collect();
    assert!(run_bloom(&entries).is_empty());
    assert!(!run_bloom(&entries[..n - 1]).is_empty());
}

#[test]
fn the_trailer_is_forty_bytes_with_its_magic_last() {
    let (bytes, _) = sample();
    assert_eq!(&bytes[bytes.len() - 4..], b"L6ri");
    assert_eq!(TRAILER_LEN, 40);
}

fn pointer() -> Pointer {
    Pointer {
        id: ObjectId { epoch: 4, seq: 11 },
        level: 2,
        len: 1000,
        footer_off: 900,
        footer_len: 60,
        runs: 2,
        entries: 7,
        seals: 30,
        orphans: Orphans {
            chunks: 3,
            bytes: 4096,
        },
        created_ms: 1_761_000_000_000,
    }
}

#[test]
fn a_pointer_row_round_trips_and_is_pinned() {
    let p = pointer();
    let raw = encode_pointer(&p);
    assert_eq!(decode_pointer(p.id, &raw), Some(p.clone()));
    let hex: String = raw.iter().map(|b| format!("{b:02x}")).collect();
    assert_eq!(hex, "0102e80784073c02071e03802080949e9ea033");
    for cut in 0..raw.len() {
        assert!(decode_pointer(p.id, &raw[..cut]).is_none());
    }
    let mut bad = p.clone();
    bad.len += 1;
    assert!(
        decode_pointer(p.id, &encode_pointer(&bad)).is_none(),
        "a footer that does not end before the trailer"
    );
    bad = p.clone();
    bad.level = 3;
    assert!(decode_pointer(p.id, &encode_pointer(&bad)).is_none());
    let key = pointer_key(p.id);
    assert_eq!(key.len(), 34);
    assert_eq!(parse_sentinel_key(POINTER_TAG, &key), Some(p.id));
    assert_eq!(parse_sentinel_key(ORPHAN_TAG, &key), None);
    assert_eq!(
        run_index_path("db", p.id),
        "db/ridx/0000000000000004/000000000000000b"
    );
}

#[test]
fn an_orphan_row_round_trips() {
    let o = Orphans {
        chunks: 9,
        bytes: 1 << 20,
    };
    let raw = encode_orphans(o);
    assert_eq!(decode_orphans(&raw), Some(o));
    assert!(decode_orphans(&raw[..raw.len() - 1]).is_none());
    let mut trailing = raw.clone();
    trailing.push(0);
    assert!(decode_orphans(&trailing).is_none(), "a trailing byte");
    let mut version = raw;
    version[0] += 1;
    assert!(decode_orphans(&version).is_none(), "another version");
    assert_eq!(
        o.add(Orphans {
            chunks: 2,
            bytes: 5
        })
        .add(Orphans {
            chunks: u64::MAX,
            bytes: 0
        }),
        Orphans {
            chunks: u64::MAX,
            bytes: (1 << 20) + 5
        },
        "counters add, saturating"
    );
    assert_eq!(
        parse_sentinel_key(ORPHAN_TAG, &orphan_key(ObjectId { epoch: 1, seq: 2 })),
        Some(ObjectId { epoch: 1, seq: 2 })
    );
}

/// Every seal-index row tag in the sentinel range sorts below the usage
/// month-finals' `'V'`: the drain scans `'V' ‖ hash` for each billed segment
/// many times a second, SlateDB scans do not fill the block cache, and an SST
/// whose keys reach past `'V'` overlaps every such scan, which then pays a
/// GET (the S10 rig's SST GET climb, package 5 round 1: about 25 scans a
/// second, each a data-block GET, once the first pointer row existed).
#[test]
fn every_index_tag_sorts_below_the_usage_finals() {
    use crate::spill::directory::{DIRECTORY_TAG, REFCOUNT_TAG};
    let finals = crate::billing::usage_month_final_key(&[0; 16], 2026, 10)[17];
    assert_eq!(finals, b'V');
    for tag in [DIRECTORY_TAG, REFCOUNT_TAG, POINTER_TAG, ORPHAN_TAG] {
        assert!(
            tag < finals,
            "tag {:?} sorts at or after the usage finals' 'V'",
            tag as char
        );
    }
}

// ---- refusal of every malformed layer (mutation coverage, package 5) ----

use super::format::{bloom_holds, decode_trailer};
use crate::spill::crc32c::crc32c;

/// An object's parts: its runs, its footer and its trailer fields.
struct Parts {
    runs: Vec<u8>,
    footer: Vec<u8>,
    t: [u8; 32],
}

/// The parts of `sample()`'s object.
fn parts() -> Parts {
    let (bytes, _) = sample();
    let n = bytes.len();
    let (_, crc) = decode_trailer(&bytes[n - TRAILER_LEN..]).unwrap();
    let footer_off = usize::try_from(u64::from_le_bytes(
        bytes[n - TRAILER_LEN..n - TRAILER_LEN + 8]
            .try_into()
            .unwrap(),
    ))
    .unwrap();
    assert_eq!(crc32c(&bytes[footer_off..n - TRAILER_LEN]), crc);
    Parts {
        runs: bytes[..footer_off].to_vec(),
        footer: bytes[footer_off..n - TRAILER_LEN].to_vec(),
        t: bytes[n - TRAILER_LEN..n - 8].try_into().unwrap(),
    }
}

impl Parts {
    /// The object again, the footer's length, CRC and the trailer CRC
    /// recomputed unless `keep_crc`.
    fn object(&self, keep_crc: bool) -> Vec<u8> {
        let mut t = self.t;
        t[8..12].copy_from_slice(&u32::try_from(self.footer.len()).unwrap().to_le_bytes());
        if !keep_crc {
            t[28..32].copy_from_slice(&crc32c(&self.footer).to_le_bytes());
        }
        let mut out = self.runs.clone();
        out.extend_from_slice(&self.footer);
        out.extend_from_slice(&t);
        out.extend_from_slice(&crc32c(&t).to_le_bytes());
        out.extend_from_slice(b"L6ri");
        out
    }

    /// Footer entry `i`'s start (both sample blooms are 2 B and 0 B long).
    fn entry(&self, i: usize) -> usize {
        let first_len =
            51 + usize::from(u16::from_le_bytes(self.footer[49..51].try_into().unwrap()));
        i * first_len
    }
}

#[test]
fn the_crafted_object_decodes_as_written() {
    assert!(decode_object(&parts().object(false)).is_some());
}

#[test]
fn every_inconsistent_footer_entry_is_refused() {
    type Change = fn(&mut Parts);
    let cases: [(&str, Change); 7] = [
        ("a run hash repeated", |p| {
            let (a, b) = (p.entry(0), p.entry(1));
            let h: [u8; 16] = p.footer[a..a + 16].try_into().unwrap();
            p.footer[b..b + 16].copy_from_slice(&h);
        }),
        ("runs out of hash order", |p| {
            let (a, b) = (p.entry(0), p.entry(1));
            let h0: [u8; 16] = p.footer[a..a + 16].try_into().unwrap();
            let h1: [u8; 16] = p.footer[b..b + 16].try_into().unwrap();
            p.footer[a..a + 16].copy_from_slice(&h1);
            p.footer[b..b + 16].copy_from_slice(&h0);
        }),
        ("a run of no entry", |p| {
            p.footer[44..48].copy_from_slice(&0u32.to_le_bytes())
        }),
        ("a run off its offset", |p| {
            let b = p.entry(1);
            let off = u64::from_le_bytes(p.footer[b + 16..b + 24].try_into().unwrap()) + 1;
            p.footer[b + 16..b + 24].copy_from_slice(&off.to_le_bytes());
        }),
        ("a span whose first passes its last", |p| {
            p.footer[28..36].copy_from_slice(&u64::MAX.to_le_bytes())
        }),
        ("an unknown flag", |p| p.footer[48] |= 2),
        ("a trailing byte in the footer", |p| p.footer.push(0)),
    ];
    for (what, change) in cases {
        let mut p = parts();
        change(&mut p);
        assert!(decode_object(&p.object(false)).is_none(), "{what} decoded");
    }
}

#[test]
fn a_footer_must_match_its_trailer() {
    let p = parts();
    let mut bad = p.object(false);
    let at = p.runs.len();
    bad[at + 20] ^= 1;
    assert!(
        decode_object(&bad).is_none(),
        "a footer byte flipped under an intact trailer CRC"
    );
    let mut q = parts();
    q.footer[0] ^= 1;
    assert!(
        decode_object(&q.object(true)).is_none(),
        "a footer whose CRC the trailer does not store"
    );
    let mut short = parts();
    short.t[16..24].copy_from_slice(&5u64.to_le_bytes());
    assert!(
        decode_object(&short.object(false)).is_none(),
        "entries that do not sum to the trailer's"
    );
}

#[test]
fn every_trailer_field_out_of_range_is_refused() {
    type Change = fn(&mut [u8; 32]);
    let cases: [(&str, Change); 4] = [
        ("version 2", |t| {
            t[24..26].copy_from_slice(&2u16.to_le_bytes())
        }),
        ("a level past daily", |t| t[26] = 3),
        ("a flag", |t| t[27] = 1),
        ("no run", |t| t[12..16].copy_from_slice(&0u32.to_le_bytes())),
    ];
    for (what, change) in cases {
        let mut p = parts();
        change(&mut p.t);
        assert!(decode_object(&p.object(false)).is_none(), "{what} decoded");
    }
}

#[test]
fn every_level_up_to_daily_is_written_and_none_past_it() {
    for level in 0..=2u8 {
        let mut w = ObjectWriter::new();
        w.push([1; 16], &[entry(1, 0, 3, fps(&[1]))]).unwrap();
        let (bytes, trailer) = w.finish(level).unwrap().unwrap();
        assert_eq!(trailer.level, level);
        assert_eq!(decode_object(&bytes).unwrap().0.level, level);
    }
    let mut w = ObjectWriter::new();
    w.push([1; 16], &[entry(1, 0, 3, fps(&[1]))]).unwrap();
    assert!(matches!(w.finish(3), Err(FormatError::Level)));
}

#[test]
fn a_summary_naming_a_fingerprint_twice_is_refused() {
    let raw = encode_run(&[entry(1, 0, 3, KeySummary::Keys(vec![5, 5]))]).unwrap();
    assert!(decode_run(&raw).is_none());
}

#[test]
fn a_run_bloom_at_its_byte_cap_decodes() {
    let n = (MAX_RUN_BLOOM_BYTES * 8 / RUN_BLOOM_BITS_PER_KEY) as u64;
    let entries: Vec<DirEntry> = (0..n).map(|i| entry(i, i, i, fps(&[i]))).collect();
    assert_eq!(run_bloom(&entries).len(), MAX_RUN_BLOOM_BYTES);
    let mut w = ObjectWriter::new();
    w.push([1; 16], &entries).unwrap();
    let (bytes, _) = w.finish(0).unwrap().unwrap();
    let (_, runs) = decode_object(&bytes).expect("a bloom of exactly the cap decodes");
    assert_eq!(runs[0].0.bloom.len(), MAX_RUN_BLOOM_BYTES);
}

/// The bloom's probe positions are part of the format: these bytes are
/// pinned for fingerprints 1 to 40.
#[test]
fn a_run_bloom_is_pinned_byte_for_byte() {
    let entries: Vec<DirEntry> = (1..=40u64).map(|i| entry(i, i, i, fps(&[i]))).collect();
    let bloom = run_bloom(&entries);
    let hex: String = bloom.iter().map(|b| format!("{b:02x}")).collect();
    assert_eq!(hex, PINNED_BLOOM);
    assert!((1..=40u64).all(|fp| bloom_holds(&bloom, fp)));
}

const PINNED_BLOOM: &str = "ffffffffff9d0c3116abb1f14670ab38806f8b1e268c724a9ac5c1d5a5507d35050d6f12c0709dc823a8ea0ef2152eb3359c";
