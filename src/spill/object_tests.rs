//! The segment object: round trips from 1 to 10,000 chunks, real layout 5
//! pages byte for byte, the zero-copy body, the encoder's refusals, the
//! anchors, and every single-bit flip in a 6 MiB object detected.
#![cfg(test)]
use bytes::Bytes;
use rand::{Rng, SeedableRng, rngs::StdRng};

use super::crc32c::{crc32c, single_bit, zero_byte};
use super::object::{
    ANCHOR_BYTES, CHUNK_CRC_LEN, CHUNK_HEADER_LEN, ChunkInput, DIR_ENTRY_LEN, ObjectError,
    ObjectId, TABLE_ENTRY_LEN, TRAILER_LEN, decode, decode_chunk, encode, object_path,
};
use super::test_pages::{opaque_object, opaque_pages, pool, sealed_pages};
use crate::crypto_page::CheckedPage;

/// The decoded object holds exactly the input: every chunk's stream, first
/// and last offsets and max timestamp, and every page byte for byte under
/// its last offset; each chunk also decodes alone from its own byte range.
fn assert_round_trip(chunks: &[ChunkInput]) -> Bytes {
    let enc = encode(chunks).expect("encode");
    assert_eq!(enc.len, enc.parts.iter().map(Bytes::len).sum::<usize>());
    let body = enc.to_bytes();
    assert_eq!(body.len(), enc.len);
    let decoded = decode(&body).expect("decode");
    assert_eq!(decoded.len(), chunks.len());
    for ((entry, chunk), (input, encoded)) in decoded.iter().zip(chunks.iter().zip(&enc.chunks)) {
        assert_eq!(*entry, encoded.entry);
        assert_eq!(chunk.hash, input.hash);
        assert_eq!(chunk.first, input.first);
        assert_eq!(chunk.last, input.pages.last().unwrap().0);
        assert_eq!(chunk.max_ts, input.max_ts);
        assert_eq!(chunk.pages, input.pages, "pages byte for byte");
        assert_eq!(chunk.anchors().unwrap(), encoded.anchors);
        let at = |x: u64| usize::try_from(x).unwrap();
        let (s, e) = (at(entry.offset), at(entry.offset + u64::from(entry.len)));
        assert_eq!(
            &decode_chunk(&body.slice(s..e)).expect("ranged chunk"),
            chunk
        );
    }
    body
}

fn random_chunks(
    rng: &mut StdRng,
    pool: &Bytes,
    n: usize,
    max_pages: usize,
    max_len: usize,
) -> Vec<ChunkInput> {
    let mut at = 0usize;
    (0..n)
        .map(|_| {
            let mut hash = [0u8; 16];
            rng.fill(&mut hash[..]);
            let first = rng.random_range(0..u64::MAX / 2);
            let pages: Vec<(u64, usize)> = (0..rng.random_range(1..=max_pages))
                .map(|_| (rng.random_range(1..=4096u64), rng.random_range(1..=max_len)))
                .collect();
            ChunkInput {
                hash,
                first,
                max_ts: rng.random::<i64>(),
                pages: opaque_pages(pool, &mut at, first, &pages),
            }
        })
        .collect()
}

#[test]
fn objects_round_trip_from_1_to_10000_chunks() {
    let mut rng = StdRng::seed_from_u64(0x0B1);
    let pool = pool(&mut rng, 4 << 20);
    for n in [1usize, 2, 3, 17, 200, 1_000, 10_000] {
        let max_pages = if n >= 1_000 { 3 } else { 12 };
        let chunks = random_chunks(&mut rng, &pool, n, max_pages, 3_000);
        let body = assert_round_trip(&chunks);
        let overhead = body.len()
            - chunks
                .iter()
                .flat_map(|c| &c.pages)
                .map(|(_, p)| p.len())
                .sum::<usize>();
        let pages: usize = chunks.iter().map(|c| c.pages.len()).sum();
        assert_eq!(
            overhead,
            n * (CHUNK_HEADER_LEN + CHUNK_CRC_LEN + TABLE_ENTRY_LEN)
                + pages * DIR_ENTRY_LEN
                + TRAILER_LEN,
            "{n} chunks: 104 B per chunk, 6 B per page, 28 B per object"
        );
    }
}

/// Real pages sealed by the shard log's cipher: each decoded page is
/// admitted against the last offset the directory derives for it.
#[test]
fn real_layout5_pages_round_trip_and_admit_at_their_offsets() {
    let mut rng = StdRng::seed_from_u64(0x5EA1);
    let chunks: Vec<ChunkInput> = (0..40u8)
        .map(|i| {
            let hash = [i.wrapping_mul(37); 16];
            let first = rng.random_range(0..1_000_000u64);
            let n = rng.random_range(1..8);
            let pages = sealed_pages(&mut rng, &hash, first, n, (50, 400));
            ChunkInput {
                hash,
                first,
                max_ts: 7,
                pages,
            }
        })
        .collect();
    let body = assert_round_trip(&chunks);
    for (_, chunk) in decode(&body).unwrap() {
        let mut next = chunk.first;
        for ((last, raw), count) in chunk.pages.iter().zip(&chunk.counts) {
            let page = CheckedPage::admit(raw.clone(), *last).expect("admit");
            assert_eq!((page.first(), page.count() as u64), (next, *count));
            next = last + 1;
        }
    }
}

/// The body is the pages' own allocations plus one metadata buffer: no
/// page byte is copied into the PUT payload.
#[test]
fn the_body_slices_the_pages_without_copying() {
    let mut rng = StdRng::seed_from_u64(0xC0B1);
    let pool = pool(&mut rng, 1 << 20);
    let chunks = random_chunks(&mut rng, &pool, 50, 6, 4_000);
    let enc = encode(&chunks).unwrap();
    let page_ptrs: std::collections::HashSet<*const u8> = chunks
        .iter()
        .flat_map(|c| &c.pages)
        .map(|(_, p)| p.as_ptr())
        .collect();
    let in_parts = enc
        .parts
        .iter()
        .filter(|p| page_ptrs.contains(&p.as_ptr()))
        .count();
    assert_eq!(
        in_parts,
        page_ptrs.len(),
        "every page is a part, by pointer"
    );
    // The rest are slices of ONE metadata allocation, merged where adjacent:
    // one before the first chunk's pages, one between chunks, one after.
    assert_eq!(enc.parts.len(), page_ptrs.len() + chunks.len() + 1);
    let payload: object_store::PutPayload = enc.parts.iter().cloned().collect();
    assert_eq!(payload.content_length(), enc.len);
}

#[test]
fn the_encoder_refuses_what_a_directory_cannot_describe() {
    let page = Bytes::from_static(b"page");
    let chunk = |first, pages: Vec<(u64, Bytes)>| ChunkInput {
        hash: [1; 16],
        first,
        max_ts: 0,
        pages,
    };
    assert_eq!(
        encode(&[chunk(5, vec![])]).err(),
        Some(ObjectError::EmptyChunk)
    );
    assert_eq!(
        encode(&[chunk(5, vec![(4, page.clone())])]).err(),
        Some(ObjectError::PageOffsets)
    );
    assert_eq!(
        encode(&[chunk(5, vec![(5, page.clone()), (5, page.clone())])]).err(),
        Some(ObjectError::PageOffsets),
        "a page with no record"
    );
    assert_eq!(
        encode(&[chunk(0, vec![(1 << 16, page.clone())])]).err(),
        Some(ObjectError::PageOffsets),
        "65,537 records do not fit a u16 count"
    );
    assert!(encode(&[chunk(0, vec![((1 << 16) - 1, page)])]).is_ok());
    assert!(encode(&[]).is_ok(), "an empty object is a footer");
}

/// An anchor marks the first page at or past each 256 KiB of page bytes,
/// with its first offset and its byte position in the chunk.
#[test]
fn anchors_mark_every_256_kib_of_pages() {
    let mut rng = StdRng::seed_from_u64(0xA7C);
    let pool = pool(&mut rng, 4 << 20);
    let mut at = 0;
    // 100 pages of 10,000 B and 7 records: 1,000,000 page bytes.
    let pages = opaque_pages(&pool, &mut at, 1_000, &[(7, 10_000); 100]);
    let enc = encode(&[ChunkInput {
        hash: [9; 16],
        first: 1_000,
        max_ts: 0,
        pages,
    }])
    .unwrap();
    let anchors = &enc.chunks[0].anchors;
    let dir = 100 * DIR_ENTRY_LEN;
    // Pages start at multiples of 10,000; the first at or past 262,144 is
    // page 27, past 524,288 page 53, past 786,432 page 79.
    let want: Vec<(u32, u64, u32)> = [27u32, 53, 79]
        .iter()
        .map(|&p| {
            (
                p,
                1_000 + 7 * u64::from(p),
                u32::try_from(CHUNK_HEADER_LEN + dir).unwrap() + p * 10_000,
            )
        })
        .collect();
    let got: Vec<(u32, u64, u32)> = anchors.iter().map(|a| (a.page, a.offset, a.byte)).collect();
    assert_eq!(got, want);
    assert!(u64::from(want[0].0) * 10_000 >= ANCHOR_BYTES);
    // One page larger than several marks yields one anchor after it.
    let mut at = 0;
    let pages = opaque_pages(&pool, &mut at, 0, &[(1, 100), (1, 600_000), (1, 100)]);
    let enc = encode(&[ChunkInput {
        hash: [9; 16],
        first: 0,
        max_ts: 0,
        pages,
    }])
    .unwrap();
    let got: Vec<u32> = enc.chunks[0].anchors.iter().map(|a| a.page).collect();
    assert_eq!(got, vec![2]);
}

#[test]
fn object_names_sit_under_the_shard_db_path() {
    let id = ObjectId {
        epoch: 3,
        seq: 0x2A,
    };
    assert_eq!(
        object_path("shards/0", id),
        "shards/0/hseg/0000000000000003/000000000000002a"
    );
}

/// Every prefix of an object and random garbage fail to decode; nothing
/// panics.
#[test]
fn truncations_and_garbage_are_refused() {
    let mut rng = StdRng::seed_from_u64(0x6A6);
    let pool = pool(&mut rng, 1 << 16);
    let chunks = random_chunks(&mut rng, &pool, 4, 3, 300);
    let body = encode(&chunks).unwrap().to_bytes();
    for len in 0..body.len() {
        assert!(decode(&body.slice(..len)).is_err(), "prefix {len}");
    }
    for _ in 0..2_000 {
        let mut junk = vec![0u8; rng.random_range(0..400)];
        rng.fill(&mut junk[..]);
        if junk.len() >= 4 && rng.random_bool(0.5) {
            let n = junk.len();
            junk[n - 4..].copy_from_slice(b"L6sg");
        }
        assert!(decode(&Bytes::from(junk.clone())).is_err());
        assert!(decode_chunk(&Bytes::from(junk)).is_err());
    }
}

/// A check region of the object: `[start, end)` is CRC'd and the CRC is
/// stored at `[end, end + 4)`.
struct Region {
    start: usize,
    end: usize,
}

/// The object's check regions, as the decoder verifies them: each chunk
/// (from the table), the table (from the trailer), and the trailer's first
/// 20 bytes; the last 4 bytes are the magic.
fn regions(body: &Bytes, enc: &super::object::EncodedObject) -> Vec<Region> {
    let n = body.len();
    let mut out: Vec<Region> = enc
        .chunks
        .iter()
        .map(|c| Region {
            start: usize::try_from(c.entry.offset).unwrap(),
            end: usize::try_from(c.entry.offset + u64::from(c.entry.len)).unwrap() - CHUNK_CRC_LEN,
        })
        .collect();
    let table_at = n - TRAILER_LEN - TABLE_ENTRY_LEN * enc.chunks.len();
    // The table's CRC sits inside the trailer (its 16..20), so the table
    // region is followed by the trailer region rather than by its CRC.
    out.push(Region {
        start: table_at,
        end: n - TRAILER_LEN,
    });
    out.push(Region {
        start: n - TRAILER_LEN,
        end: n - 8,
    });
    out
}

/// **Every single-bit flip in a 6 MiB object is detected.**
///
/// 1. The check regions, their CRC fields and the magic tile the object
///    exactly, so every bit lies in a region, a stored CRC or the magic.
/// 2. Exhaustive over all 8 × 6 MiB positions: a one-bit error at distance
///    k from a region's end changes that region's CRC by Z^k(single_bit),
///    and that is never 0 for any k up to the object's length (iterated
///    with the implementation's own table). A flip in a stored CRC or in
///    the magic changes the stored value itself.
/// 3. The linearity in (2) holds on this object's regions for sampled
///    flips, through the slicing-by-16 path.
/// 4. End to end: the decoder refuses the flipped object for every bit of
///    the trailer, both ends of the table, the first chunk's header and
///    directory and its CRC, and 160 random positions.
#[test]
fn every_single_bit_flip_in_a_6_mib_object_is_detected() {
    let mut rng = StdRng::seed_from_u64(0xF11B);
    let pool = pool(&mut rng, 7 << 20);
    let chunks = opaque_object(&mut rng, &pool, 200, 6 << 20, (10 * 1024, 10));
    let enc = encode(&chunks).unwrap();
    let body = enc.to_bytes();
    let n = body.len();
    assert!(n >= 6 << 20, "{n}");
    assert!(decode(&body).is_ok());

    // (1) regions + CRC fields + magic tile [0, n).
    let regions = regions(&body, &enc);
    let mut covered = vec![0u8; n];
    for (i, r) in regions.iter().enumerate() {
        for c in &mut covered[r.start..r.end] {
            *c += 1;
        }
        // Chunk CRCs follow their chunk; the table's CRC is inside the
        // trailer region; the trailer's CRC is at n-8..n-4.
        let crc_at = if i < enc.chunks.len() {
            Some(r.end)
        } else if i == regions.len() - 1 {
            Some(n - 8)
        } else {
            None
        };
        if let Some(at) = crc_at {
            for c in &mut covered[at..at + 4] {
                *c += 1;
            }
        }
    }
    for c in &mut covered[n - 4..] {
        *c += 1;
    }
    assert!(
        covered.iter().all(|c| *c == 1),
        "every byte under exactly one check"
    );

    // (2) exhaustive: no one-bit error at any distance below n is invisible.
    for bit in 0..8 {
        let mut w = single_bit(bit);
        for k in 0..n {
            assert_ne!(w, 0, "bit {bit} at distance {k}");
            w = zero_byte(w);
        }
    }

    // (3) the stored CRCs match, and sampled flips move them by the syndrome.
    for r in &regions {
        let data = &body[r.start..r.end];
        let stored_at = if r.end == n - TRAILER_LEN {
            n - 12
        } else {
            r.end
        };
        let stored = u32::from_le_bytes(body[stored_at..stored_at + 4].try_into().unwrap());
        assert_eq!(crc32c(data), stored);
    }
    for _ in 0..24 {
        let r = &regions[rng.random_range(0..enc.chunks.len())];
        let (j, b) = (rng.random_range(r.start..r.end), rng.random_range(0..8u32));
        let mut flipped = body[r.start..r.end].to_vec();
        flipped[j - r.start] ^= 1 << b;
        let mut syndrome = single_bit(b);
        for _ in 0..r.end - 1 - j {
            syndrome = zero_byte(syndrome);
        }
        assert_eq!(crc32c(&body[r.start..r.end]) ^ crc32c(&flipped), syndrome);
    }

    // (4) the decoder itself.
    let first = &enc.chunks[0].entry;
    let head_end = CHUNK_HEADER_LEN + DIR_ENTRY_LEN * first.page_count as usize;
    let crc0 = usize::try_from(first.offset + u64::from(first.len)).unwrap() - CHUNK_CRC_LEN;
    let table_at = n - TRAILER_LEN - TABLE_ENTRY_LEN * enc.chunks.len();
    let mut positions: Vec<usize> = (n - TRAILER_LEN..n).collect();
    positions.extend(table_at..table_at + TABLE_ENTRY_LEN);
    positions.extend(n - TRAILER_LEN - TABLE_ENTRY_LEN..n - TRAILER_LEN);
    positions.extend(0..head_end);
    positions.extend(crc0..crc0 + CHUNK_CRC_LEN);
    let mut flips: Vec<(usize, u32)> = positions
        .iter()
        .flat_map(|&j| (0..8).map(move |b| (j, b)))
        .collect();
    flips.extend((0..160).map(|_| (rng.random_range(0..n), rng.random_range(0..8u32))));
    let mut buf = body.to_vec();
    for &(j, b) in &flips {
        // Flip in place and hand the decoder the same allocation back and forth.
        buf[j] ^= 1 << b;
        let flipped = Bytes::from(std::mem::take(&mut buf));
        assert!(
            decode(&flipped).is_err(),
            "flip at {j} bit {b} went unnoticed"
        );
        buf = Vec::from(flipped);
        buf[j] ^= 1 << b;
    }
    assert!(
        flips.len() > 8 * (TRAILER_LEN + 2 * TABLE_ENTRY_LEN),
        "{}",
        flips.len()
    );
    assert_eq!(Bytes::from(buf), body, "every flip was undone");
}
