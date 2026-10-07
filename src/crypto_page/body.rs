//! The page body: every record's length, then every record's timestamp
//! delta, then the payloads. The parser reads exactly that and nothing else:
//! each table entry is a minimal varint, a delta keeps the record's
//! timestamp within i64, and the lengths cover the remaining bytes exactly.
//! It is a pure function over a byte slice so the Kani harnesses in
//! `proofs.rs` can check it over symbolic bodies.

use super::BodyError;

/// One record's place in a parsed body: its payload length and timestamp.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct RecordSpan {
    pub(super) len: usize,
    pub(super) ts_ms: i64,
}

/// A parsed body: where its payloads start and each record's span, in
/// offset order. The spans' lengths sum to the bytes from `payload_start`.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct PageTable {
    pub(super) payload_start: usize,
    pub(super) records: Vec<RecordSpan>,
}

/// Append `value` as a minimal LEB128 varint.
pub(super) fn put_varint(out: &mut Vec<u8>, mut value: u64) {
    loop {
        let low = (value & 0x7f) as u8;
        value >>= 7;
        if value == 0 {
            out.push(low);
            return;
        }
        out.push(low | 0x80);
    }
}

/// Read one minimal LEB128 varint and consume exactly its bytes. An
/// unterminated run, a value past u64 and a redundant zero last group (an
/// overlong encoding) are refused.
pub(super) fn get_varint(input: &mut &[u8]) -> Option<u64> {
    let mut value = 0u64;
    for shift in (0..64).step_by(7) {
        let (&byte, rest) = input.split_first()?;
        *input = rest;
        let group = u64::from(byte & 0x7f);
        if shift == 63 && group > 1 {
            return None;
        }
        value |= group << shift;
        if byte & 0x80 == 0 {
            return (byte != 0 || shift == 0).then_some(value);
        }
    }
    None
}

/// Bytes the minimal varint of `value` takes: one per started seven bits.
pub(super) fn varint_len(value: u64) -> usize {
    let bits = u64::BITS.saturating_sub(value.leading_zeros()).max(1);
    bits.div_ceil(7) as usize
}

/// Body bytes one record of `len` payload bytes adds to a page: its length
/// entry, its zero timestamp delta and its payload.
pub(super) fn record_body_bytes(len: usize) -> usize {
    varint_len(len as u64).saturating_add(1).saturating_add(len)
}

/// The body of a page of `records`, every timestamp delta 0. The caller
/// bounds the records; the body is not compressed here.
pub(super) fn build_body<R: AsRef<[u8]>>(records: &[R]) -> Vec<u8> {
    let size = records.iter().fold(0usize, |size, record| {
        size.saturating_add(record_body_bytes(record.as_ref().len()))
    });
    let mut body = Vec::with_capacity(size);
    for record in records {
        put_varint(&mut body, record.as_ref().len() as u64);
    }
    for _ in records {
        put_varint(&mut body, 0);
    }
    for record in records {
        body.extend_from_slice(record.as_ref());
    }
    body
}

/// Parse the body of a page of `count` records whose header timestamp is
/// `ts_ms`. Ok only when both tables hold exactly `count` minimal varints,
/// every timestamp fits an i64, and the lengths sum to the remaining bytes.
pub(crate) fn parse_body(body: &[u8], count: usize, ts_ms: i64) -> Result<PageTable, BodyError> {
    let mut input = body;
    // Each record takes at least two table bytes, so a claimed count can
    // never reserve more than the body could describe.
    let mut records = Vec::with_capacity(count.min(body.len()));
    for _ in 0..count {
        let len = get_varint(&mut input)
            .and_then(|len| usize::try_from(len).ok())
            .ok_or(BodyError::LengthTable)?;
        records.push(RecordSpan { len, ts_ms });
    }
    for record in &mut records {
        let delta = get_varint(&mut input).ok_or(BodyError::DeltaTable)?;
        record.ts_ms = ts_ms
            .checked_add_unsigned(delta)
            .ok_or(BodyError::Timestamp)?;
    }
    let payload_start = body.len().saturating_sub(input.len());
    for record in &records {
        input = input.get(record.len..).ok_or(BodyError::PayloadShort)?;
    }
    if !input.is_empty() {
        return Err(BodyError::Trailing);
    }
    Ok(PageTable {
        payload_start,
        records,
    })
}
