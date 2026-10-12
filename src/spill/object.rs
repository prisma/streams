//! The segment object (design B §1.1, §1.2 step 6): one immutable object per
//! seal, holding every sealed stream's pages as one chunk each.
//!
//! Layout (all integers little-endian):
//!
//! ```text
//! object  := chunk* table trailer
//! chunk   := header dir pages crc
//!   header  (40 B) := hash[16] first:u64 page_count:u32 pages_len:u32 max_ts:i64
//!   dir     (6 B per page) := len:u32 (records - 1):u16
//!   pages   := the layout 5 pages, byte for byte, as the shard log holds them
//!   crc     (4 B) := CRC-32C of header ‖ dir ‖ pages
//! table   := entry*            (60 B each)
//!   entry := hash[16] first:u64 last:u64 offset:u64 len:u32 page_count:u32
//!            stored:u32 max_ts:i64
//! trailer (28 B) := table_off:u64 chunk_count:u32 version:u16 flags:u16
//!                   table_crc:u32 trailer_crc:u32 magic[4] = "L6sg"
//! ```
//!
//! Every byte is under exactly one check, and each check's region is fixed
//! by a layer verified before it: the magic is compared; the trailer CRC
//! covers the trailer's first 20 bytes at a fixed place; the table CRC covers
//! the table the verified trailer locates; each chunk CRC covers the chunk
//! the verified table locates. So every single-bit flip anywhere in an
//! object is detected (`spill::object_tests` proves it exhaustively through
//! CRC linearity). A chunk is self-verifying on its own, so a ranged GET of
//! one chunk (index row: offset and length) checks it without the footer.
//!
//! The body is built as a list of `Bytes` slices: the pages are the ring's
//! own allocations, and the headers, directories, CRCs and footer share one
//! metadata buffer. Nothing is copied.

use bytes::{BufMut, Bytes, BytesMut};

use super::crc32c::{Crc32c, crc32c};

pub(crate) const OBJECT_MAGIC: [u8; 4] = *b"L6sg";
pub(crate) const OBJECT_VERSION: u16 = 1;
pub(crate) const CHUNK_HEADER_LEN: usize = 40;
pub(crate) const DIR_ENTRY_LEN: usize = 6;
pub(crate) const CHUNK_CRC_LEN: usize = 4;
pub(crate) const TABLE_ENTRY_LEN: usize = 60;
pub(crate) const TRAILER_LEN: usize = 28;
/// A byte anchor marks the first page starting at or after each multiple of
/// this many page bytes in a chunk (DESIGN §3.2 item 8).
pub(crate) const ANCHOR_BYTES: u64 = 256 << 10;
/// The most records one directory entry can describe (a u16 of count − 1).
const DIR_MAX_RECORDS: u64 = 1 << 16;

/// A segment object's name within its shard DB's path: the writer epoch of
/// the shard DB opening that sealed it, and its seal sequence.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct ObjectId {
    pub(crate) epoch: u64,
    pub(crate) seq: u64,
}

/// `<shard_db_path>/hseg/<epoch:016x>/<seq:016x>`.
pub(crate) fn object_path(shard_db_path: &str, id: ObjectId) -> String {
    format!("{shard_db_path}/hseg/{:016x}/{:016x}", id.epoch, id.seq)
}

/// One stream's contiguous pages to seal: the first offset of its first
/// page, the largest record timestamp among them, and each page as the ring
/// holds it (last offset, stored bytes).
#[derive(Clone, Debug)]
pub(crate) struct ChunkInput {
    pub(crate) hash: [u8; 16],
    pub(crate) first: u64,
    pub(crate) max_ts: i64,
    pub(crate) pages: Vec<(u64, Bytes)>,
}

/// One chunk table entry: where a chunk sits in its object and what it holds.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct ChunkEntry {
    pub(crate) hash: [u8; 16],
    pub(crate) first: u64,
    pub(crate) last: u64,
    /// Byte offset of the chunk's header in the object.
    pub(crate) offset: u64,
    /// Header, directory, pages and CRC.
    pub(crate) len: u32,
    pub(crate) page_count: u32,
    /// The pages' stored bytes (the G10 gauge's unit).
    pub(crate) stored: u32,
    pub(crate) max_ts: i64,
}

/// A read's starting point inside a chunk: page `page` holds offsets from
/// `offset` on and starts `byte` bytes after the chunk's header begins.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Anchor {
    pub(crate) page: u32,
    pub(crate) offset: u64,
    pub(crate) byte: u32,
}

pub(crate) struct EncodedChunk {
    pub(crate) entry: ChunkEntry,
    /// Anchors after page 0, every `ANCHOR_BYTES` of page bytes.
    pub(crate) anchors: Vec<Anchor>,
}

/// An object ready to PUT: its body as slices in order, and its chunks.
pub(crate) struct EncodedObject {
    pub(crate) parts: Vec<Bytes>,
    pub(crate) len: usize,
    pub(crate) chunks: Vec<EncodedChunk>,
}

impl EncodedObject {
    /// The body as one PUT payload, still without a copy.
    pub(crate) fn payload(&self) -> object_store::PutPayload {
        self.parts.iter().cloned().collect()
    }

    /// The body as one buffer (a copy: tests and the in-memory store).
    pub(crate) fn to_bytes(&self) -> Bytes {
        let mut out = BytesMut::with_capacity(self.len);
        for part in &self.parts {
            out.extend_from_slice(part);
        }
        out.freeze()
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ObjectError {
    /// A chunk with no pages.
    EmptyChunk,
    /// A page's last offset does not follow the chunk's previous page, or
    /// it spans more records than a directory entry holds.
    PageOffsets,
    /// A chunk or an object larger than its length fields hold.
    TooLarge,
    Truncated,
    Magic,
    TrailerCrc,
    Version,
    TableCrc,
    /// The chunks do not tile the bytes before the table, or a chunk's
    /// length disagrees with its directory.
    Layout,
    ChunkCrc,
    /// A chunk's header disagrees with its table entry.
    Mismatch,
}

/// A chunk's pages, each as (records in it, stored bytes), with offsets from
/// `first`: the anchors after page 0.
fn anchors_of(first: u64, pages: impl Iterator<Item = (u64, usize)>, dir_len: usize) -> Vec<Anchor> {
    let mut anchors = Vec::new();
    let (mut offset, mut at, mut next_mark) = (first, 0u64, ANCHOR_BYTES);
    for (page, (records, len)) in pages.enumerate() {
        if at >= next_mark {
            anchors.push(Anchor {
                page: page as u32,
                offset,
                byte: (CHUNK_HEADER_LEN + dir_len + at as usize) as u32,
            });
            next_mark = (at / ANCHOR_BYTES + 1) * ANCHOR_BYTES;
        }
        offset += records;
        at += len as u64;
    }
    anchors
}

/// Each page's record count, from the chunk's first offset and the pages'
/// last offsets.
fn page_counts(chunk: &ChunkInput) -> Result<Vec<u64>, ObjectError> {
    if chunk.pages.is_empty() {
        return Err(ObjectError::EmptyChunk);
    }
    let mut next = chunk.first;
    let mut counts = Vec::with_capacity(chunk.pages.len());
    for (last, _) in &chunk.pages {
        let records = last
            .checked_sub(next)
            .and_then(|d| d.checked_add(1))
            .filter(|n| *n <= DIR_MAX_RECORDS)
            .ok_or(ObjectError::PageOffsets)?;
        counts.push(records);
        next = last.checked_add(1).ok_or(ObjectError::PageOffsets)?;
    }
    Ok(counts)
}

/// A piece of the body: a range of the metadata buffer, or chunk `n`'s pages.
enum Span {
    Meta(usize, usize),
    Pages(usize),
}

/// Encode one object from `chunks`, in order.
pub(crate) fn encode(chunks: &[ChunkInput]) -> Result<EncodedObject, ObjectError> {
    let mut counts = Vec::with_capacity(chunks.len());
    let mut meta_len = TRAILER_LEN + TABLE_ENTRY_LEN * chunks.len();
    for chunk in chunks {
        let c = page_counts(chunk)?;
        meta_len += CHUNK_HEADER_LEN + DIR_ENTRY_LEN * c.len() + CHUNK_CRC_LEN;
        counts.push(c);
    }
    let mut meta = BytesMut::with_capacity(meta_len);
    let mut spans = Vec::with_capacity(2 * chunks.len() + 1);
    let mut encoded = Vec::with_capacity(chunks.len());
    let mut offset = 0u64;
    for (index, (chunk, counts)) in chunks.iter().zip(&counts).enumerate() {
        let pages_len: usize = chunk.pages.iter().map(|(_, p)| p.len()).sum();
        let stored = u32::try_from(pages_len).map_err(|_| ObjectError::TooLarge)?;
        let page_count = u32::try_from(counts.len()).map_err(|_| ObjectError::TooLarge)?;
        let dir_len = DIR_ENTRY_LEN * counts.len();
        let len = u32::try_from(CHUNK_HEADER_LEN + dir_len + pages_len + CHUNK_CRC_LEN)
            .map_err(|_| ObjectError::TooLarge)?;
        let start = meta.len();
        meta.put_slice(&chunk.hash);
        meta.put_u64_le(chunk.first);
        meta.put_u32_le(page_count);
        meta.put_u32_le(stored);
        meta.put_i64_le(chunk.max_ts);
        for ((_, page), records) in chunk.pages.iter().zip(counts) {
            meta.put_u32_le(u32::try_from(page.len()).map_err(|_| ObjectError::TooLarge)?);
            meta.put_u16_le((records - 1) as u16);
        }
        let mut crc = Crc32c::new();
        crc.update(&meta[start..]);
        for (_, page) in &chunk.pages {
            crc.update(page);
        }
        spans.push(Span::Meta(start, meta.len()));
        spans.push(Span::Pages(index));
        let crc_at = meta.len();
        meta.put_u32_le(crc.finish());
        spans.push(Span::Meta(crc_at, meta.len()));
        let last = chunk.pages.last().map_or(chunk.first, |(last, _)| *last);
        let anchors = anchors_of(
            chunk.first,
            counts.iter().zip(&chunk.pages).map(|(n, (_, p))| (*n, p.len())),
            dir_len,
        );
        encoded.push(EncodedChunk {
            entry: ChunkEntry {
                hash: chunk.hash,
                first: chunk.first,
                last,
                offset,
                len,
                page_count,
                stored,
                max_ts: chunk.max_ts,
            },
            anchors,
        });
        offset += u64::from(len);
    }
    let table_at = meta.len();
    for chunk in &encoded {
        put_entry(&mut meta, &chunk.entry);
    }
    let table_crc = crc32c(&meta[table_at..]);
    let trailer_at = meta.len();
    meta.put_u64_le(offset);
    meta.put_u32_le(u32::try_from(encoded.len()).map_err(|_| ObjectError::TooLarge)?);
    meta.put_u16_le(OBJECT_VERSION);
    meta.put_u16_le(0);
    meta.put_u32_le(table_crc);
    let trailer_crc = crc32c(&meta[trailer_at..]);
    meta.put_u32_le(trailer_crc);
    meta.put_slice(&OBJECT_MAGIC);
    spans.push(Span::Meta(table_at, meta.len()));
    debug_assert_eq!(meta.len(), meta_len);
    let meta = meta.freeze();
    let mut parts: Vec<Bytes> = Vec::with_capacity(spans.len());
    let mut pending: Option<(usize, usize)> = None;
    for span in spans {
        match span {
            Span::Meta(s, e) => {
                pending = match pending {
                    Some((ps, pe)) if pe == s => Some((ps, e)),
                    Some((ps, pe)) => {
                        parts.push(meta.slice(ps..pe));
                        Some((s, e))
                    }
                    None => Some((s, e)),
                };
            }
            Span::Pages(index) => {
                if let Some((ps, pe)) = pending.take() {
                    parts.push(meta.slice(ps..pe));
                }
                parts.extend(chunks[index].pages.iter().map(|(_, p)| p.clone()));
            }
        }
    }
    if let Some((ps, pe)) = pending {
        parts.push(meta.slice(ps..pe));
    }
    // The chunks end at the table; the table and trailer follow.
    let total = usize::try_from(offset).map_err(|_| ObjectError::TooLarge)?
        + TABLE_ENTRY_LEN * encoded.len()
        + TRAILER_LEN;
    debug_assert_eq!(total, parts.iter().map(Bytes::len).sum::<usize>());
    Ok(EncodedObject {
        parts,
        len: total,
        chunks: encoded,
    })
}

fn put_entry(out: &mut BytesMut, e: &ChunkEntry) {
    out.put_slice(&e.hash);
    out.put_u64_le(e.first);
    out.put_u64_le(e.last);
    out.put_u64_le(e.offset);
    out.put_u32_le(e.len);
    out.put_u32_le(e.page_count);
    out.put_u32_le(e.stored);
    out.put_i64_le(e.max_ts);
}

/// Little-endian field reader over a verified region.
struct Fields<'a>(&'a [u8]);

impl Fields<'_> {
    fn take<const N: usize>(&mut self) -> Result<[u8; N], ObjectError> {
        let (head, rest) = self
            .0
            .split_first_chunk::<N>()
            .ok_or(ObjectError::Truncated)?;
        self.0 = rest;
        Ok(*head)
    }
    fn u16(&mut self) -> Result<u16, ObjectError> {
        Ok(u16::from_le_bytes(self.take()?))
    }
    fn u32(&mut self) -> Result<u32, ObjectError> {
        Ok(u32::from_le_bytes(self.take()?))
    }
    fn u64(&mut self) -> Result<u64, ObjectError> {
        Ok(u64::from_le_bytes(self.take()?))
    }
    fn i64(&mut self) -> Result<i64, ObjectError> {
        Ok(i64::from_le_bytes(self.take()?))
    }
}

/// One verified chunk: its header fields and its pages as the ring holds
/// them (last offset, stored bytes), each a slice of the input.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct DecodedChunk {
    pub(crate) hash: [u8; 16],
    pub(crate) first: u64,
    pub(crate) last: u64,
    pub(crate) max_ts: i64,
    pub(crate) stored: u32,
    pub(crate) pages: Vec<(u64, Bytes)>,
    /// Each page's record count, from the directory.
    pub(crate) counts: Vec<u64>,
}

impl DecodedChunk {
    /// The anchors the encoder derived for this chunk, derived again from
    /// the directory.
    pub(crate) fn anchors(&self) -> Vec<Anchor> {
        anchors_of(
            self.first,
            self.counts.iter().zip(&self.pages).map(|(n, (_, p))| (*n, p.len())),
            DIR_ENTRY_LEN * self.pages.len(),
        )
    }
}

/// Verify and decode one chunk, as a ranged GET of `[offset, offset + len)`
/// returns it.
pub(crate) fn decode_chunk(chunk: &Bytes) -> Result<DecodedChunk, ObjectError> {
    let body_len = chunk
        .len()
        .checked_sub(CHUNK_CRC_LEN)
        .filter(|n| *n >= CHUNK_HEADER_LEN)
        .ok_or(ObjectError::Truncated)?;
    let stored_crc = u32::from_le_bytes(
        chunk[body_len..]
            .try_into()
            .map_err(|_| ObjectError::Truncated)?,
    );
    if crc32c(&chunk[..body_len]) != stored_crc {
        return Err(ObjectError::ChunkCrc);
    }
    let mut f = Fields(&chunk[..CHUNK_HEADER_LEN]);
    let hash = f.take::<16>()?;
    let first = f.u64()?;
    let page_count = f.u32()? as usize;
    let stored = f.u32()?;
    let max_ts = f.i64()?;
    let dir_end = page_count
        .checked_mul(DIR_ENTRY_LEN)
        .and_then(|d| d.checked_add(CHUNK_HEADER_LEN))
        .filter(|end| *end <= body_len)
        .ok_or(ObjectError::Layout)?;
    if page_count == 0 || dir_end + stored as usize != body_len {
        return Err(ObjectError::Layout);
    }
    let mut dir = Fields(&chunk[CHUNK_HEADER_LEN..dir_end]);
    let mut pages = Vec::with_capacity(page_count);
    let mut counts = Vec::with_capacity(page_count);
    let (mut at, mut next) = (dir_end, first);
    for _ in 0..page_count {
        let len = dir.u32()? as usize;
        let records = u64::from(dir.u16()?) + 1;
        let end = at.checked_add(len).filter(|e| *e <= body_len).ok_or(ObjectError::Layout)?;
        let last = next.checked_add(records - 1).ok_or(ObjectError::PageOffsets)?;
        pages.push((last, chunk.slice(at..end)));
        counts.push(records);
        at = end;
        next = last.checked_add(1).ok_or(ObjectError::PageOffsets)?;
    }
    if at != body_len {
        return Err(ObjectError::Layout);
    }
    let last = next - 1;
    Ok(DecodedChunk {
        hash,
        first,
        last,
        max_ts,
        stored,
        pages,
        counts,
    })
}

/// Verify and decode a whole object: magic, trailer, table, then every
/// chunk, each check over a region a verified layer fixed.
pub(crate) fn decode(object: &Bytes) -> Result<Vec<(ChunkEntry, DecodedChunk)>, ObjectError> {
    let n = object.len();
    let trailer_at = n.checked_sub(TRAILER_LEN).ok_or(ObjectError::Truncated)?;
    let trailer = &object[trailer_at..];
    if trailer[24..28] != OBJECT_MAGIC {
        return Err(ObjectError::Magic);
    }
    let stored_trailer_crc = u32::from_le_bytes([trailer[20], trailer[21], trailer[22], trailer[23]]);
    if crc32c(&trailer[..20]) != stored_trailer_crc {
        return Err(ObjectError::TrailerCrc);
    }
    let mut f = Fields(&trailer[..20]);
    let table_off = f.u64()?;
    let chunk_count = f.u32()? as usize;
    let (version, flags) = (f.u16()?, f.u16()?);
    let table_crc = f.u32()?;
    if version != OBJECT_VERSION || flags != 0 {
        return Err(ObjectError::Version);
    }
    let table_off = usize::try_from(table_off).map_err(|_| ObjectError::Layout)?;
    let table_end = chunk_count
        .checked_mul(TABLE_ENTRY_LEN)
        .and_then(|t| t.checked_add(table_off))
        .ok_or(ObjectError::Layout)?;
    if table_end != trailer_at {
        return Err(ObjectError::Layout);
    }
    let table = &object[table_off..table_end];
    if crc32c(table) != table_crc {
        return Err(ObjectError::TableCrc);
    }
    let mut t = Fields(table);
    let mut out = Vec::with_capacity(chunk_count);
    let mut expect_at = 0u64;
    for _ in 0..chunk_count {
        let entry = ChunkEntry {
            hash: t.take::<16>()?,
            first: t.u64()?,
            last: t.u64()?,
            offset: t.u64()?,
            len: t.u32()?,
            page_count: t.u32()?,
            stored: t.u32()?,
            max_ts: t.i64()?,
        };
        if entry.offset != expect_at {
            return Err(ObjectError::Layout);
        }
        expect_at = entry.offset + u64::from(entry.len);
        let (s, e) = (entry.offset as usize, expect_at as usize);
        if e > table_off {
            return Err(ObjectError::Layout);
        }
        let chunk = decode_chunk(&object.slice(s..e))?;
        if chunk.hash != entry.hash
            || chunk.first != entry.first
            || chunk.last != entry.last
            || chunk.pages.len() != entry.page_count as usize
            || chunk.stored != entry.stored
            || chunk.max_ts != entry.max_ts
        {
            return Err(ObjectError::Mismatch);
        }
        out.push((entry, chunk));
    }
    if expect_at as usize != table_off {
        return Err(ObjectError::Layout);
    }
    Ok(out)
}
