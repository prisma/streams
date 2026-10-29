#![no_main]
//! The run-index object's codec (`spill::runindex::format`), which every
//! cold sealed read parses back from the store: a footer by the range its
//! pointer row names, then one stream's run by the range its footer entry
//! names, and a merge reads whole objects. The first input byte picks a
//! mode:
//!
//! - object: the rest is an object as the store returned it; the trailer,
//!   the footer it locates and every run the footer locates must all check
//!   before anything is trusted, whatever decodes satisfies the format, and
//!   writing its runs again reproduces exactly the same bytes (the codec is
//!   canonical, footer flags and blooms included);
//! - run: the rest is one run as a ranged GET returned it; whatever decodes
//!   satisfies the run order and encodes back to exactly the same bytes;
//! - encode: the rest describes up to six runs of up to eight entries
//!   within the writer's domain; their object decodes back to exactly those
//!   runs, every key a run's entries name passes its bloom, and a truncated
//!   or extended object is refused.
//!
//! Decoding never panics, and its allocations are bounded whatever the
//! lengths claim: every reservation is capped at 4,096 items, and
//! everything else grows only from bytes that are present.
//!
//! Compiles the actual private production owners by path: no copied codec
//! or verification-only public API. The tiling rules over decoded runs are
//! KANI-105's; the pointer row codec stays in `spill::runindex`.
#[path = "../../src/spill/crc32c.rs"]
mod crc32c;
#[allow(
    dead_code,
    reason = "run-index fuzz target; compile the actual run-index codec; the writer's run counter and the pointer helpers serve the fold"
)]
#[path = "../../src/spill/runindex/format.rs"]
mod format;
#[allow(
    dead_code,
    reason = "run-index fuzz target; compile the actual segment object owner for its id and anchor types; its chunk codec has its own target"
)]
#[path = "../../src/spill/object.rs"]
mod object;
#[allow(
    dead_code,
    reason = "run-index fuzz target; compile the actual directory row codec for its entry and summary types; the row codec has its own target"
)]
#[path = "../../src/spill/directory/row.rs"]
mod row;
#[path = "../../src/crypto_page/varint.rs"]
mod varint;

/// The service crate's paths the included owners import through.
mod crypto_page {
    pub(crate) use super::varint::{get_varint, put_varint};
}
mod spill {
    pub(crate) use super::{crc32c, object};
    pub(crate) mod directory {
        pub(crate) use super::super::row::{
            DirEntry, KeySummary, MAX_BLOOM_BYTES, MAX_SUMMARY_KEYS,
        };
    }
}

use format::{
    FooterEntry, ObjectWriter, TRAILER_LEN, checked_footer, decode_run, decode_trailer, encode_run,
};
use object::{Anchor, ObjectId};
use row::{DirEntry, KeySummary, MAX_BLOOM_BYTES, MAX_SUMMARY_KEYS};

libfuzzer_sys::fuzz_target!(|raw: &[u8]| {
    let Some((&mode, rest)) = raw.split_first() else {
        return;
    };
    match mode % 3 {
        0 => check_object(rest),
        1 => check_run(rest),
        _ => check_encode(rest),
    }
});

/// Every layer of `raw`, each checked before the next is located; `None`
/// as soon as one fails.
type Decoded = (format::Trailer, Vec<(FooterEntry, Vec<DirEntry>)>);

fn decode(raw: &[u8]) -> Option<Decoded> {
    let split = raw.len().checked_sub(TRAILER_LEN)?;
    let (trailer, crc) = decode_trailer(raw.get(split..)?)?;
    let start = usize::try_from(trailer.footer_off).ok()?;
    let footer = checked_footer(raw.get(start..split)?, &trailer, crc)?;
    let mut out = Vec::new();
    for f in footer {
        let at = usize::try_from(f.run_off).ok()?;
        let entries = decode_run(raw.get(at..at.checked_add(f.run_len as usize)?)?)?;
        out.push((f, entries));
    }
    Some((trailer, out))
}

/// Decode `raw` as an object; whatever decodes satisfies the format and is
/// written back to exactly `raw` when its footer agrees with its runs.
fn check_object(raw: &[u8]) {
    let Some((trailer, runs)) = decode(raw) else {
        return;
    };
    let mut writer = ObjectWriter::new();
    let mut agrees = true;
    let mut at = 0u64;
    for (f, entries) in &runs {
        assert_eq!(f.run_off, at, "the runs tile the object from byte 0");
        at += u64::from(f.run_len);
        assert!(format::run_ordered(entries), "a decoded run is in order");
        agrees &= f.entries as usize == entries.len()
            && entries.first().map(|e| e.first) == Some(f.first)
            && entries.last().map(|e| e.last) == Some(f.last)
            && f.flags == format::run_flags(entries)
            && f.bloom == format::run_bloom(entries);
        assert!(
            writer.push(f.hash, entries).is_ok(),
            "decoded runs in hash order write again"
        );
    }
    assert_eq!(
        at, trailer.footer_off,
        "the runs end where the footer starts"
    );
    if agrees {
        let finished = writer.finish(trailer.level);
        assert!(
            matches!(finished, Ok(Some(_))),
            "a decoded object's runs and level write again"
        );
        let Ok(Some((bytes, written))) = finished else {
            return;
        };
        assert_eq!(written, trailer, "the trailer is written again");
        assert_eq!(bytes, raw, "the codec is canonical");
    }
}

/// Decode `raw` as one run; whatever decodes encodes back to exactly it.
fn check_run(raw: &[u8]) {
    let Some(entries) = decode_run(raw) else {
        return;
    };
    assert!(format::run_ordered(&entries));
    for e in &entries {
        assert!(e.first <= e.last, "an entry covers first..=last");
        assert!(
            e.anchors.iter().all(|a| a.offset >= e.first),
            "anchors lie at or after first"
        );
        match &e.keys {
            KeySummary::Keys(k) => {
                assert!((1..=MAX_SUMMARY_KEYS).contains(&k.len()) && k.is_sorted_by(|a, b| a < b))
            }
            KeySummary::Bloom(b) => assert!((1..=MAX_BLOOM_BYTES).contains(&b.len())),
        }
    }
    assert_eq!(
        encode_run(&entries).as_deref(),
        Ok(raw),
        "the run codec is canonical"
    );
}

/// Write the runs the input describes; the object decodes back to exactly
/// them, every key passes its run's bloom, and no prefix or extension of
/// the object decodes.
fn check_encode(raw: &[u8]) {
    let mut input = Input(raw);
    let mut runs: Vec<([u8; 16], Vec<DirEntry>)> = Vec::new();
    let mut hash = [0u8; 16];
    for _ in 0..input.byte().map_or(0, |b| b % 7) {
        let step = input.byte().unwrap_or(1).max(1);
        let Some(next) = u128::from_be_bytes(hash).checked_add(u128::from(step)) else {
            break;
        };
        hash = next.to_be_bytes();
        let entries = input.run();
        if entries.is_empty() {
            break;
        }
        runs.push((hash, entries));
    }
    let mut writer = ObjectWriter::new();
    for (h, entries) in &runs {
        assert!(
            writer.push(*h, entries).is_ok(),
            "a described run is in the writer's domain"
        );
    }
    let level = input.byte().map_or(0, |b| b % 3);
    let finished = writer.finish(level);
    assert!(finished.is_ok(), "a level below 3 is valid");
    let Ok(Some((bytes, _))) = finished else {
        assert!(runs.is_empty(), "only an object of no run is not written");
        return;
    };
    let decoded = decode(&bytes);
    assert!(decoded.is_some(), "a written object decodes");
    let Some((trailer, decoded)) = decoded else {
        return;
    };
    assert_eq!(trailer.level, level);
    assert_eq!(decoded.len(), runs.len());
    for ((f, entries), (h, want)) in decoded.iter().zip(&runs) {
        assert_eq!(
            (&f.hash, entries),
            (h, want),
            "each run decodes back to itself"
        );
        for e in want {
            if let KeySummary::Keys(keys) = &e.keys {
                assert!(
                    keys.iter().all(|k| f.may_hold(*k)),
                    "a run bloom admits every key its run names"
                );
            }
        }
    }
    for cut in [1, 2, bytes.len() / 2] {
        if let Some(prefix) = bytes.len().checked_sub(cut).and_then(|n| bytes.get(..n)) {
            assert!(decode(prefix).is_none(), "a truncated object is refused");
        }
    }
    let mut extended = bytes;
    extended.insert(0, input.byte().unwrap_or_default());
    assert!(decode(&extended).is_none(), "a shifted object is refused");
}

/// The encode-mode description: a little-endian reader that yields nothing
/// once the input runs out.
struct Input<'a>(&'a [u8]);

impl Input<'_> {
    fn bytes<const N: usize>(&mut self) -> Option<[u8; N]> {
        let (head, rest) = self.0.split_first_chunk::<N>()?;
        self.0 = rest;
        Some(*head)
    }

    fn byte(&mut self) -> Option<u8> {
        self.bytes::<1>().map(|[b]| b)
    }

    fn u32(&mut self) -> Option<u32> {
        self.bytes().map(u32::from_le_bytes)
    }

    /// Up to eight entries with first and last both strictly increasing.
    fn run(&mut self) -> Vec<DirEntry> {
        let mut out: Vec<DirEntry> = Vec::new();
        for _ in 0..self.byte().map_or(0, |b| b % 9) {
            let Some(e) = self.entry(out.last()) else {
                break;
            };
            out.push(e);
        }
        out
    }

    fn entry(&mut self, prev: Option<&DirEntry>) -> Option<DirEntry> {
        let first = prev
            .map_or(Some(0), |p| p.first.checked_add(1))?
            .checked_add(u64::from(self.byte()?))?;
        let floor = prev.map_or(first, |p| p.last.saturating_add(1).max(first));
        let last = floor.checked_add(u64::from(self.byte()?))?;
        let keys = if self.byte()? % 4 == 0 {
            KeySummary::Bloom(vec![
                self.byte()?;
                usize::from(self.byte()?).clamp(1, MAX_BLOOM_BYTES)
            ])
        } else {
            let mut k: Vec<u64> = (0..self.byte()? % 4 + 1)
                .map(|i| u64::from(i) * 0x9e37_79b9)
                .collect();
            k.dedup();
            KeySummary::Keys(k)
        };
        let anchors = match self.byte()? % 3 {
            0 => vec![Anchor {
                page: self.u32()?,
                offset: first + u64::from(self.byte()?) % (last - first + 1),
                byte: self.u32()?,
            }],
            _ => Vec::new(),
        };
        Some(DirEntry {
            object: ObjectId {
                epoch: u64::from(self.byte()?),
                seq: u64::from(self.u32()?),
            },
            chunk_off: u64::from(self.u32()?),
            chunk_len: self.u32()?,
            first,
            last,
            page_count: self.u32()?,
            keys,
            anchors,
        })
    }
}
