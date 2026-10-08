//! The page body: every record's length, then every record's timestamp
//! delta to the record before it (0 for the first, whose timestamp is the
//! header's), then the payloads. The parser reads exactly that and nothing
//! else: each table entry is a minimal varint, the first delta is 0, every
//! timestamp stays within i64, and the lengths cover the remaining bytes
//! exactly.
//! It is a pure function over a byte slice so the Kani harnesses in
//! `proofs.rs` can check it over symbolic bodies.

use super::{BodyError, MAX_RECORD_PLAINTEXT, SealRecord};

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

/// Body bytes one record of `len` payload bytes adds to a page whose
/// previous record is `delta` milliseconds older: its length entry, its
/// delta entry and its payload.
pub(super) fn record_body_bytes(len: usize, delta: u64) -> usize {
    varint_len(len as u64)
        .saturating_add(varint_len(delta))
        .saturating_add(len)
}

/// The delta a record at `ts_ms` stores after a record at `previous`: 0 for
/// a page's first record, None when it would go back in time.
pub(super) fn timestamp_delta(previous: Option<i64>, ts_ms: i64) -> Option<u64> {
    match previous {
        None => Some(0),
        Some(previous) => (ts_ms >= previous).then(|| ts_ms.abs_diff(previous)),
    }
}

/// The body of a page of `records`, each timestamp a delta to the record
/// before it; None when a timestamp goes back. The caller bounds the
/// records; the body is not compressed here.
pub(super) fn build_body(records: &[SealRecord<'_>]) -> Option<Vec<u8>> {
    let mut previous = None;
    let mut size = 0usize;
    for record in records {
        let delta = timestamp_delta(previous, record.ts_ms)?;
        size = size.saturating_add(record_body_bytes(record.payload.len(), delta));
        previous = Some(record.ts_ms);
    }
    let mut body = Vec::with_capacity(size);
    for record in records {
        put_varint(&mut body, record.payload.len() as u64);
    }
    // In order, as checked above: the first record's delta is 0.
    let mut previous = records.first().map_or(0, |record| record.ts_ms);
    for record in records {
        put_varint(&mut body, record.ts_ms.abs_diff(previous));
        previous = record.ts_ms;
    }
    for record in records {
        body.extend_from_slice(record.payload);
    }
    Some(body)
}

/// Parse the body of a page of `count` records whose header timestamp is
/// `ts_ms`. Ok only when both tables hold exactly `count` minimal varints,
/// no length passes the record cap, the first delta is 0, every timestamp
/// (the previous record's plus its delta) fits an i64, and the lengths sum
/// to the remaining bytes. Every loop is bounded by `count` itself, which
/// keeps the parser's loops finite for the verifier (KANI-097).
pub(crate) fn parse_body(body: &[u8], count: usize, ts_ms: i64) -> Result<PageTable, BodyError> {
    let mut input = body;
    // Admission bounds the count to 4,096, so this reserves at most 64 KiB,
    // and only for an authenticated page.
    let mut records = Vec::with_capacity(count);
    let mut payloads = 0usize;
    for _ in 0..count {
        let len = get_varint(&mut input)
            .and_then(|len| usize::try_from(len).ok())
            .ok_or(BodyError::LengthTable)?;
        if len > MAX_RECORD_PLAINTEXT {
            return Err(BodyError::RecordTooLarge);
        }
        // At most 4,096 records of at most 32 MiB: the sum never saturates.
        payloads = payloads.saturating_add(len);
        records.push(RecordSpan { len, ts_ms });
    }
    let mut previous = None;
    for record in records.iter_mut().take(count) {
        let delta = get_varint(&mut input).ok_or(BodyError::DeltaTable)?;
        record.ts_ms = match previous {
            None if delta != 0 => return Err(BodyError::FirstDelta),
            None => ts_ms,
            Some(previous) => {
                i64::checked_add_unsigned(previous, delta).ok_or(BodyError::Timestamp)?
            }
        };
        previous = Some(record.ts_ms);
    }
    let payload_start = body.len().saturating_sub(input.len());
    // The payloads follow the tables back to back: they tile the rest of
    // the body exactly when their lengths sum to it.
    if payloads > input.len() {
        return Err(BodyError::PayloadShort);
    }
    if payloads < input.len() {
        return Err(BodyError::Trailing);
    }
    Ok(PageTable {
        payload_start,
        records,
    })
}
