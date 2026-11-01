//! A segment object's chunk table, read from its tail (the segment
//! cleaner, `shard::seal::gc::clean`): the trailer names the table's
//! offset, the table lists every chunk (stream, offsets, place). The same
//! checks as the whole-object decoder (`object::decode`, tests only): the
//! magic, the trailer's CRC, the version, the table's place and CRC, and the
//! chunks laid end to end up to the table. One ranged GET of the last
//! `TAIL_BYTES` covers any seal's table (a seal holds at most 1,024 streams).

use bytes::Bytes;

use super::crc32c::crc32c;
use super::object::{ChunkEntry, OBJECT_MAGIC, OBJECT_VERSION, TABLE_ENTRY_LEN, TRAILER_LEN};

/// The tail one GET fetches: the trailer and a table of 1,024 chunks.
pub(crate) const TAIL_BYTES: u64 = (TRAILER_LEN + TABLE_ENTRY_LEN * 1024) as u64;

/// What a tail decode found.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Table {
    /// Every chunk of the object.
    Chunks(Vec<ChunkEntry>),
    /// The table starts at `table_off`, before the tail: fetch from there.
    StartsAt(u64),
}

fn le<const N: usize>(b: &[u8], at: usize) -> Result<[u8; N], String> {
    b.get(at..at + N)
        .and_then(|s| s.try_into().ok())
        .ok_or_else(|| "a truncated table".to_owned())
}

/// Decode the table of an object of `object_len` bytes from `tail`, its
/// bytes from `object_len - tail.len()` to its end.
pub(crate) fn decode_table(tail: &Bytes, object_len: u64) -> Result<Table, String> {
    let n = tail.len();
    let start = object_len
        .checked_sub(n as u64)
        .ok_or("a tail longer than its object")?;
    let trailer_at = n.checked_sub(TRAILER_LEN).ok_or("no trailer")?;
    let trailer = &tail[trailer_at..];
    if le::<4>(trailer, 24)? != OBJECT_MAGIC {
        return Err("not a segment object (magic)".to_owned());
    }
    if crc32c(&trailer[..20]) != u32::from_le_bytes(le(trailer, 20)?) {
        return Err("the trailer's CRC".to_owned());
    }
    let table_off = u64::from_le_bytes(le(trailer, 0)?);
    let count = u32::from_le_bytes(le(trailer, 8)?) as usize;
    let (version, flags) = (
        u16::from_le_bytes(le(trailer, 12)?),
        u16::from_le_bytes(le(trailer, 14)?),
    );
    let table_crc = u32::from_le_bytes(le(trailer, 16)?);
    if version != OBJECT_VERSION || flags != 0 {
        return Err(format!("object version {version}, flags {flags}"));
    }
    let table_len = count
        .checked_mul(TABLE_ENTRY_LEN)
        .ok_or("a table too large")?;
    if table_off.checked_add(table_len as u64) != Some(start + trailer_at as u64) {
        return Err("the table does not end at the trailer".to_owned());
    }
    if table_off < start {
        return Ok(Table::StartsAt(table_off));
    }
    let at = usize::try_from(table_off - start).map_err(|e| e.to_string())?;
    let table = &tail[at..trailer_at];
    if crc32c(table) != table_crc {
        return Err("the table's CRC".to_owned());
    }
    let mut out = Vec::with_capacity(count);
    let mut expect_at = 0u64;
    for row in table.as_chunks::<TABLE_ENTRY_LEN>().0 {
        let entry = ChunkEntry {
            hash: le(row, 0)?,
            first: u64::from_le_bytes(le(row, 16)?),
            last: u64::from_le_bytes(le(row, 24)?),
            offset: u64::from_le_bytes(le(row, 32)?),
            len: u32::from_le_bytes(le(row, 40)?),
            page_count: u32::from_le_bytes(le(row, 44)?),
            stored: u32::from_le_bytes(le(row, 48)?),
            max_ts: i64::from_le_bytes(le(row, 52)?),
        };
        if entry.offset != expect_at || entry.first > entry.last {
            return Err(format!("chunk {} is out of place", out.len()));
        }
        expect_at = entry.offset + u64::from(entry.len);
        out.push(entry);
    }
    if expect_at != table_off {
        return Err("the chunks do not end at the table".to_owned());
    }
    Ok(Table::Chunks(out))
}
