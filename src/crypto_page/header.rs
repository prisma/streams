//! The clear header and admission. A stored page is admitted from its bytes
//! and its row key alone, without any key: the version, the record count,
//! the routing key, the exact length and the key's last offset are checked
//! here, and everything the header says is authenticated later as AAD.

use bytes::Bytes;

use super::body::{get_varint, put_varint};
use super::{
    NONCE_LEN, PAGE_MAX_RECORDS, PAGE_VER, PAGE_VER_Z, PageCorruption, PageLane, SealError,
    TAG_LEN, body_cap, last_offset, page_tag,
};

/// Everything the clear header of a page being sealed says.
pub(super) struct HeaderFields<'a> {
    pub(super) ver: u8,
    pub(super) first: u64,
    pub(super) count: usize,
    /// The first record's timestamp.
    pub(super) ts_ms: i64,
    pub(super) lane: &'a PageLane<'a>,
    pub(super) nonce: [u8; NONCE_LEN],
}

/// The header bytes from ver through nonce: the AAD after the segment.
pub(super) fn encode_header(fields: &HeaderFields<'_>) -> Result<Vec<u8>, SealError> {
    let routing_key = fields.lane.routing_key.as_bytes();
    let rk_len = u16::try_from(routing_key.len()).map_err(|_| SealError::RoutingKeyTooLong)?;
    let mut header = Vec::with_capacity(routing_key.len().saturating_add(37));
    header.push(fields.ver);
    header.extend_from_slice(&fields.first.to_be_bytes());
    put_varint(&mut header, fields.count as u64);
    header.extend_from_slice(&fields.ts_ms.to_be_bytes());
    header.extend_from_slice(&fields.lane.key_version.to_be_bytes());
    header.extend_from_slice(&rk_len.to_be_bytes());
    header.extend_from_slice(routing_key);
    header.extend_from_slice(&fields.nonce);
    Ok(header)
}

/// A stored page's clear fields, borrowed from its bytes.
struct ClearPage<'a> {
    ver: u8,
    first: u64,
    count: usize,
    ts_ms: i64,
    key_version: u32,
    routing_key: &'a str,
    nonce: [u8; NONCE_LEN],
    /// The header bytes, ver through nonce, are the row's first
    /// `header_len` bytes; the ciphertext and tag are its last `ct_len`.
    header_len: usize,
    ct_len: usize,
}

/// Advance past the next complete fixed-width field.
fn field<const N: usize>(input: &mut &[u8]) -> Result<[u8; N], PageCorruption> {
    let (value, rest) = input
        .split_first_chunk::<N>()
        .ok_or(PageCorruption::Truncated)?;
    *input = rest;
    Ok(*value)
}

/// Parse a stored page's clear header and bound its ciphertext. The record
/// count is checked before any field after it is read.
fn parse(raw: &[u8]) -> Result<ClearPage<'_>, PageCorruption> {
    let mut input = raw;
    let [ver] = field::<1>(&mut input)?;
    if !matches!(ver, PAGE_VER | PAGE_VER_Z) {
        return Err(PageCorruption::Version);
    }
    let first = u64::from_be_bytes(field(&mut input)?);
    let count = get_varint(&mut input)
        .and_then(|count| usize::try_from(count).ok())
        .filter(|count| (1..=PAGE_MAX_RECORDS).contains(count))
        .ok_or(PageCorruption::Count)?;
    let ts_ms = i64::from_be_bytes(field(&mut input)?);
    let key_version = u32::from_be_bytes(field(&mut input)?);
    let rk_len = u16::from_be_bytes(field(&mut input)?);
    let (routing_key, rest) = input
        .split_at_checked(usize::from(rk_len))
        .ok_or(PageCorruption::Truncated)?;
    input = rest;
    let routing_key = std::str::from_utf8(routing_key).map_err(|_| PageCorruption::RoutingKey)?;
    let nonce = field::<NONCE_LEN>(&mut input)?;
    let header_len = raw.len().saturating_sub(input.len());
    let ct_len = usize::try_from(u32::from_be_bytes(field(&mut input)?))
        .map_err(|_| PageCorruption::Truncated)?;
    if input.len() < ct_len {
        return Err(PageCorruption::Truncated);
    }
    if input.len() > ct_len {
        return Err(PageCorruption::Trailing);
    }
    let body = ct_len.checked_sub(TAG_LEN).ok_or(PageCorruption::Tag)?;
    if body > body_cap(count) {
        return Err(PageCorruption::Oversized);
    }
    Ok(ClearPage {
        ver,
        first,
        count,
        ts_ms,
        key_version,
        routing_key,
        nonce,
        header_len,
        ct_len,
    })
}

/// The last offset a row key names under `prefix`, the canonical page-key
/// prefix of the selected segment: its namespace and its keyspace's page tag
/// (`'p'` in the shard log, `'g'` in history). The key must be exactly that
/// prefix and eight bytes.
fn row_last(key: &[u8], prefix: &[u8]) -> Result<u64, PageCorruption> {
    if page_tag(prefix.len()).is_none_or(|tag| prefix.last() != Some(&tag)) {
        return Err(PageCorruption::RowTag);
    }
    let (namespace, last) = key
        .split_last_chunk::<8>()
        .ok_or(PageCorruption::KeyWidth)?;
    if namespace.len() != prefix.len() {
        return Err(PageCorruption::KeyWidth);
    }
    if namespace != prefix {
        return Err(PageCorruption::Namespace);
    }
    Ok(u64::from_be_bytes(*last))
}

/// A row's bytes parsed as one whole page whose first offset and record
/// count end at `last`, the offset its key names.
fn page_at(raw: &[u8], last: u64) -> Result<ClearPage<'_>, PageCorruption> {
    let page = parse(raw)?;
    let derived = last_offset(page.first, page.count).ok_or(PageCorruption::OffsetOverflow)?;
    if derived != last {
        return Err(PageCorruption::Offset {
            key: last,
            page: derived,
        });
    }
    Ok(page)
}

/// A stored page admitted against its row key without decrypting. It
/// retains the exact immutable bytes it was admitted from; its fields are
/// private so no caller can pair them with other bytes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct CheckedPage {
    raw: Bytes,
    /// `raw[..header_len]` is the header, ver through nonce (the AAD after
    /// the segment); `raw[sealed_from..]` the ciphertext and tag.
    header_len: usize,
    sealed_from: usize,
    nonce: [u8; NONCE_LEN],
    ver: u8,
    first: u64,
    last: u64,
    count: usize,
    ts_ms: i64,
    key_version: u32,
    routing_key: Box<str>,
}

impl CheckedPage {
    /// Admit a stored row. `prefix` is the canonical page-key prefix of the
    /// selected segment: its namespace and its keyspace's page tag (`'p'` in
    /// the shard log, `'g'` in history); the row key is that prefix and the
    /// page's last offset.
    pub(crate) fn from_row(key: &[u8], prefix: &[u8], raw: Bytes) -> Result<Self, PageCorruption> {
        Self::admit(raw, row_last(key, prefix)?)
    }

    /// Admit page bytes held under `last`, as the tail ring holds them.
    pub(crate) fn admit(raw: Bytes, last: u64) -> Result<Self, PageCorruption> {
        let page = page_at(&raw, last)?;
        let (header_len, sealed_from) = (page.header_len, raw.len().saturating_sub(page.ct_len));
        let routing_key = Box::from(page.routing_key);
        let (ver, first, count) = (page.ver, page.first, page.count);
        let (ts_ms, key_version, nonce) = (page.ts_ms, page.key_version, page.nonce);
        Ok(Self {
            raw,
            header_len,
            sealed_from,
            nonce,
            ver,
            first,
            last,
            count,
            ts_ms,
            key_version,
            routing_key,
        })
    }

    pub(crate) fn first(&self) -> u64 {
        self.first
    }

    pub(crate) fn last(&self) -> u64 {
        self.last
    }

    pub(crate) fn count(&self) -> usize {
        self.count
    }

    pub(crate) fn ts_ms(&self) -> i64 {
        self.ts_ms
    }

    pub(crate) fn key_version(&self) -> u32 {
        self.key_version
    }

    /// The page's lane, readable without decrypting so keyed reads skip
    /// other keys' pages.
    pub(crate) fn routing_key(&self) -> &str {
        &self.routing_key
    }

    /// Whether the body is stored zstd-compressed (version 7).
    pub(crate) fn is_compressed(&self) -> bool {
        self.ver == PAGE_VER_Z
    }

    /// The stored bytes: what is retained, copied and billed.
    pub(crate) fn raw(&self) -> &Bytes {
        &self.raw
    }

    /// The header bytes, ver through nonce. Admission proved the range;
    /// an empty slice, which no admitted page yields, would only fail
    /// authentication.
    pub(super) fn header_bytes(&self) -> &[u8] {
        self.raw.get(..self.header_len).unwrap_or_default()
    }

    /// The ciphertext and its tag. Admission proved the range; an empty
    /// slice, which no admitted page yields, would only fail authentication.
    pub(super) fn sealed_bytes(&self) -> &[u8] {
        self.raw.get(self.sealed_from..).unwrap_or_default()
    }

    pub(super) fn nonce(&self) -> &[u8; NONCE_LEN] {
        &self.nonce
    }
}

#[cfg(kani)]
mod proofs;
