#![no_main]
//! The sealed segment object's chunk parser (`spill::object::decode_chunk`),
//! which verifies every chunk a sealed-range read fetches by ranged GET.
//! The first input byte picks a mode:
//!
//! - raw: the rest is a chunk as the store returned it;
//! - sealed: the rest is a chunk body, and its CRC-32C is appended with the
//!   production `crc32c`, so the parser's layout checks run past the CRC;
//! - round trip: the rest describes up to four chunks, which the production
//!   encoder writes into one object; every chunk it accepts must decode back
//!   to its input, and any single-bit flip must be refused.
//!
//! Whatever decodes must satisfy the format: the CRC matched, the pages
//! tile `first..=last` and the body after the directory exactly, and the
//! encoder writes the same bytes back from the decoded fields. Decoding
//! never panics, and it allocates at most a directory's worth of entries for
//! the bytes it was given.
//!
//! Compiles the actual private production owners by path: no copied decoder
//! or verification-only public API. The whole-object `decode` is compiled
//! for tests only until garbage collection reads whole objects, so it is
//! outside this target.
#[path = "../../src/spill/crc32c.rs"]
mod crc32c;
#[allow(
    dead_code,
    reason = "segment chunk fuzz target; compile the actual segment object owner; object naming is outside this decoder target"
)]
#[path = "../../src/spill/object.rs"]
mod object;

use bytes::{Bytes, BytesMut};
use crc32c::crc32c;
use object::{
    CHUNK_CRC_LEN, CHUNK_HEADER_LEN, ChunkInput, DIR_ENTRY_LEN, DecodedChunk, decode_chunk, encode,
};

/// The most records one directory entry describes.
const DIR_MAX_RECORDS: u64 = 1 << 16;

libfuzzer_sys::fuzz_target!(|raw: &[u8]| {
    let Some((&mode, rest)) = raw.split_first() else {
        return;
    };
    match mode % 3 {
        0 => check_chunk(&Bytes::copy_from_slice(rest)),
        1 => {
            let mut sealed = BytesMut::from(rest);
            sealed.extend_from_slice(&crc32c(rest).to_le_bytes());
            check_chunk(&sealed.freeze());
        }
        _ => check_round_trip(rest),
    }
});

/// Decode `chunk`; whatever decodes satisfies the format.
fn check_chunk(chunk: &Bytes) {
    if let Ok(decoded) = decode_chunk(chunk) {
        check_decoded(chunk, &decoded);
    }
}

/// The format's invariants over a chunk `decode_chunk` accepted.
fn check_decoded(chunk: &Bytes, decoded: &DecodedChunk) {
    let body_len = chunk.len().checked_sub(CHUNK_CRC_LEN);
    assert!(
        body_len.is_some_and(|n| n >= CHUNK_HEADER_LEN),
        "a decoded chunk holds a header and a CRC"
    );
    let body_len = body_len.unwrap_or_default();
    assert_eq!(
        crc32c(&chunk[..body_len]).to_le_bytes()[..],
        chunk[body_len..],
        "a decoded chunk's CRC matched"
    );
    assert!(!decoded.pages.is_empty(), "a decoded chunk has pages");
    assert_eq!(decoded.pages.len(), decoded.counts.len());
    let mut at = CHUNK_HEADER_LEN + DIR_ENTRY_LEN * decoded.pages.len();
    let mut next = Some(decoded.first);
    let mut stored = 0usize;
    for ((last, page), count) in decoded.pages.iter().zip(&decoded.counts) {
        assert!(
            (1..=DIR_MAX_RECORDS).contains(count),
            "a page holds 1 to 65,536 records"
        );
        assert!(next.is_some(), "only the last page may end at u64::MAX");
        let start = next.unwrap_or_default();
        assert_eq!(
            start.checked_add(count - 1),
            Some(*last),
            "the pages tile first..=last"
        );
        assert_eq!(
            chunk.get(at..at + page.len()),
            Some(&page[..]),
            "the pages tile the body"
        );
        at += page.len();
        stored += page.len();
        next = last.checked_add(1);
    }
    assert_eq!(
        next.map(|n| n - 1),
        Some(decoded.last),
        "the last page ends the chunk"
    );
    assert_eq!(at, body_len, "nothing follows the last page");
    assert_eq!(u32::try_from(stored).ok(), Some(decoded.stored));
    let reencoded = encode(&[ChunkInput {
        hash: decoded.hash,
        first: decoded.first,
        max_ts: decoded.max_ts,
        pages: decoded.pages.clone(),
    }]);
    assert!(
        reencoded.is_ok(),
        "the encoder accepts a chunk the decoder accepted"
    );
    let Ok(object) = reencoded else {
        return;
    };
    let body = concat(&object.parts);
    assert_eq!(
        body.get(..chunk.len()),
        Some(&chunk[..]),
        "the encoder writes the decoded chunk back"
    );
}

/// Encode the chunks the input describes; each one the encoder accepts
/// decodes back to its input, and every single-bit flip is refused.
fn check_round_trip(raw: &[u8]) {
    let mut input = Input(raw);
    let chunks = input.chunks();
    let Ok(object) = encode(&chunks) else {
        return;
    };
    let body = concat(&object.parts);
    assert_eq!(body.len(), object.len, "the parts hold the object's length");
    let mut expect_at = 0u64;
    for (chunk, encoded) in chunks.iter().zip(&object.chunks) {
        assert_eq!(
            encoded.entry.offset, expect_at,
            "the chunks tile the object"
        );
        let len = u64::from(encoded.entry.len);
        expect_at += len;
        let start = usize::try_from(encoded.entry.offset).unwrap_or(usize::MAX);
        let span = usize::try_from(len)
            .ok()
            .and_then(|len| start.checked_add(len))
            .and_then(|end| body.get(start..end));
        assert!(span.is_some(), "a chunk entry points inside its object");
        let bytes = Bytes::copy_from_slice(span.unwrap_or_default());
        let decoded = decode_chunk(&bytes);
        assert!(decoded.is_ok(), "an encoded chunk decodes");
        let Ok(decoded) = decoded else {
            return;
        };
        check_decoded(&bytes, &decoded);
        assert_eq!(decoded.hash, chunk.hash);
        assert_eq!(decoded.first, chunk.first);
        assert_eq!(decoded.max_ts, chunk.max_ts);
        assert_eq!(
            decoded.pages, chunk.pages,
            "a chunk decodes back to its pages"
        );
        assert_eq!(decoded.last, encoded.entry.last);
        assert_eq!(decoded.stored, encoded.entry.stored);
        if let Some(bit) = input.flip(bytes.len()) {
            let mut flipped = BytesMut::from(&bytes[..]);
            flipped[bit / 8] ^= 1 << (bit % 8);
            assert!(
                decode_chunk(&flipped.freeze()).is_err(),
                "a single-bit flip is refused"
            );
        }
    }
}

/// The object body as one buffer.
fn concat(parts: &[Bytes]) -> Vec<u8> {
    let mut body = Vec::with_capacity(parts.iter().map(Bytes::len).sum());
    for part in parts {
        body.extend_from_slice(part);
    }
    body
}

/// The round-trip description: a little-endian reader that yields nothing
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

    /// Up to four chunks of up to eight pages of up to 255 bytes each: small
    /// enough to keep the target fast, so no chunk reaches the 256 KiB of its
    /// first anchor (the anchors are pinned by `spill::object_tests`).
    fn chunks(&mut self) -> Vec<ChunkInput> {
        let count = self.byte().map_or(0, |b| b % 5);
        (0..count).map_while(|_| self.chunk()).collect()
    }

    fn chunk(&mut self) -> Option<ChunkInput> {
        let hash = [self.byte()?; 16];
        let first = u64::from_le_bytes(self.bytes()?);
        let max_ts = i64::from_le_bytes(self.bytes()?);
        let pages = self.byte()? % 9;
        let mut next = first;
        let mut out = Vec::with_capacity(usize::from(pages));
        for _ in 0..pages {
            let records = u64::from(u16::from_le_bytes(self.bytes()?));
            let len = usize::from(self.byte()?);
            let fill = self.byte()?;
            // A record count of 0 names a page that ends before it starts,
            // which the encoder must refuse.
            let last = next.wrapping_add(records).wrapping_sub(1);
            out.push((last, Bytes::from(vec![fill; len])));
            next = last.wrapping_add(1);
        }
        Some(ChunkInput {
            hash,
            first,
            max_ts,
            pages: out,
        })
    }

    /// A bit of a `len`-byte chunk to flip, if the input names one.
    fn flip(&mut self, len: usize) -> Option<usize> {
        let at = usize::from(u16::from_le_bytes(self.bytes()?));
        len.checked_mul(8)
            .filter(|bits| *bits > 0)
            .map(|bits| at % bits)
    }
}
