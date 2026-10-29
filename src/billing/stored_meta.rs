//! The stored billing meta's two halves (layout 6, DESIGN-6A f3).
//!
//! A segment's billing meta (`SegmentBillingMetaV1`) is stored in two
//! places, so a commit group writes no billing row of its own:
//!
//! - its **static row** (`billing_meta_key`, this module's `encode_static`):
//!   the identity, the month that holds the storage clock and the retention
//!   flag. It changes only at the first billed append of an incarnation, at a
//!   month rollover and at a retention flag change, and is written only then;
//! - its **usage counters** (`UsageCounters`): the version, the lifetime and
//!   month totals, the gauge and the storage clock. They ride the stream's
//!   tail row (`shard::rows`), which every billed group writes anyway.
//!
//! Every counter is absolute, never a delta (REVIEW-6A F4), so the newest
//! tail always carries the whole state, and the usage drain meters a stream
//! by its version: the rollup keeps the largest version and ignores any
//! snapshot at or below it, so emitting one version twice is harmless.

use super::SegmentBillingMetaV1;
use crate::crypto_page::{get_varint, put_varint};

/// The static row's format version.
const STATIC_VERSION: u8 = 1;
/// The tag that opens a tail's usage counters after its known suffix.
pub(crate) const COUNTERS_TAG: u8 = b'F';

/// A segment's cumulative usage counters, as its tail carries them.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct UsageCounters {
    pub(crate) usage_version: u64,
    pub(crate) ingest_payload_bytes_total: u64,
    pub(crate) ingest_records_total: u64,
    pub(crate) owned_frame_bytes_current: u64,
    pub(crate) storage_accounted_through_ms: i64,
    pub(crate) month_ingest_payload_bytes: u64,
    pub(crate) month_ingest_records: u64,
    /// The month's byte-time; `None` while the meta holds no figure (the
    /// empty string a fresh meta carries, distinct from "0").
    pub(crate) month_storage_byte_ms: Option<u128>,
}

impl UsageCounters {
    /// The counters of `meta`; `None` when its byte-time is not a number,
    /// which `load` refuses as corruption before any meta reaches here.
    pub(crate) fn of(meta: &SegmentBillingMetaV1) -> Option<Self> {
        let month_storage_byte_ms = if meta.month_storage_byte_ms.is_empty() {
            None
        } else {
            Some(meta.month_storage_byte_ms.parse().ok()?)
        };
        Some(Self {
            usage_version: meta.usage_version,
            ingest_payload_bytes_total: meta.ingest_payload_bytes_total,
            ingest_records_total: meta.ingest_records_total,
            owned_frame_bytes_current: meta.owned_frame_bytes_current,
            storage_accounted_through_ms: meta.storage_accounted_through_ms,
            month_ingest_payload_bytes: meta.month_ingest_payload_bytes,
            month_ingest_records: meta.month_ingest_records,
            month_storage_byte_ms,
        })
    }

    /// `meta` with these counters.
    pub(crate) fn apply(&self, meta: &mut SegmentBillingMetaV1) {
        meta.usage_version = self.usage_version;
        meta.ingest_payload_bytes_total = self.ingest_payload_bytes_total;
        meta.ingest_records_total = self.ingest_records_total;
        meta.owned_frame_bytes_current = self.owned_frame_bytes_current;
        meta.storage_accounted_through_ms = self.storage_accounted_through_ms;
        meta.month_ingest_payload_bytes = self.month_ingest_payload_bytes;
        meta.month_ingest_records = self.month_ingest_records;
        meta.month_storage_byte_ms = self
            .month_storage_byte_ms
            .map_or_else(String::new, |v| v.to_string());
    }

    /// `'F'`, then minimal LEB128 varints: version, ingest bytes, ingest
    /// records, gauge, the storage clock (zigzag), month bytes, month
    /// records, and the month byte-time as 0 for none or 1 then its low and
    /// high 64 bits.
    pub(crate) fn encode_into(&self, out: &mut Vec<u8>) {
        out.push(COUNTERS_TAG);
        for v in [
            self.usage_version,
            self.ingest_payload_bytes_total,
            self.ingest_records_total,
            self.owned_frame_bytes_current,
            zigzag(self.storage_accounted_through_ms),
            self.month_ingest_payload_bytes,
            self.month_ingest_records,
        ] {
            put_varint(out, v);
        }
        match self.month_storage_byte_ms {
            None => put_varint(out, 0),
            Some(byte_ms) => {
                put_varint(out, 1);
                let bytes = byte_ms.to_le_bytes();
                let (low, high) = bytes.split_at(8);
                for half in [low, high] {
                    put_varint(out, u64::from_le_bytes(half.try_into().unwrap_or_default()));
                }
            }
        }
    }

    /// The counters `encode_into` wrote, consuming exactly `input`; any
    /// other byte sequence is refused.
    pub(crate) fn decode(input: &[u8]) -> Option<Self> {
        let (&tag, mut rest) = input.split_first()?;
        if tag != COUNTERS_TAG {
            return None;
        }
        let mut next = || get_varint(&mut rest);
        let mut counters = Self {
            usage_version: next()?,
            ingest_payload_bytes_total: next()?,
            ingest_records_total: next()?,
            owned_frame_bytes_current: next()?,
            storage_accounted_through_ms: unzigzag(next()?),
            month_ingest_payload_bytes: next()?,
            month_ingest_records: next()?,
            month_storage_byte_ms: None,
        };
        counters.month_storage_byte_ms = match next()? {
            0 => None,
            1 => Some(u128::from(next()?) | (u128::from(next()?) << 64)),
            _ => return None,
        };
        rest.is_empty().then_some(counters)
    }
}

/// The static row of `meta`: `[1][v u16][segment u32][month_year i32]`,
/// little-endian, then minimal LEB128 varints: the month, the flags (bit 0:
/// retained by forks), and the account, project, stream id and stream name,
/// each as its length and its UTF-8 bytes.
pub(crate) fn encode_static(meta: &SegmentBillingMetaV1) -> Vec<u8> {
    let names = [
        &meta.account_id,
        &meta.project_id,
        &meta.stream_id,
        &meta.stream_name,
    ];
    let mut out = Vec::with_capacity(15 + names.iter().map(|s| 2 + s.len()).sum::<usize>());
    out.push(STATIC_VERSION);
    out.extend_from_slice(&meta.v.to_le_bytes());
    out.extend_from_slice(&meta.segment_id.to_le_bytes());
    out.extend_from_slice(&meta.month_year.to_le_bytes());
    put_varint(&mut out, u64::from(meta.month_month));
    put_varint(&mut out, u64::from(meta.retained_by_forks));
    for name in names {
        put_varint(&mut out, name.len() as u64);
        out.extend_from_slice(name.as_bytes());
    }
    out
}

/// The meta a static row describes, with zero counters; `None` for any
/// byte sequence `encode_static` does not write.
pub(crate) fn decode_static(raw: &[u8]) -> Option<SegmentBillingMetaV1> {
    let (fixed, mut rest) = raw.split_first_chunk::<11>()?;
    let [version, v0, v1, s0, s1, s2, s3, y0, y1, y2, y3] = *fixed;
    if version != STATIC_VERSION {
        return None;
    }
    let month_month = u32::try_from(get_varint(&mut rest)?).ok()?;
    let retained_by_forks = match get_varint(&mut rest)? {
        0 => false,
        1 => true,
        _ => return None,
    };
    let mut name = || -> Option<String> {
        let len = usize::try_from(get_varint(&mut rest)?).ok()?;
        let (bytes, tail) = rest.split_at_checked(len)?;
        rest = tail;
        String::from_utf8(bytes.to_vec()).ok()
    };
    let (account_id, project_id, stream_id, stream_name) = (name()?, name()?, name()?, name()?);
    rest.is_empty().then_some(SegmentBillingMetaV1 {
        v: u16::from_le_bytes([v0, v1]),
        account_id,
        project_id,
        stream_id,
        stream_name,
        segment_id: u32::from_le_bytes([s0, s1, s2, s3]),
        month_year: i32::from_le_bytes([y0, y1, y2, y3]),
        month_month,
        retained_by_forks,
        ..Default::default()
    })
}

fn zigzag(v: i64) -> u64 {
    ((v << 1) ^ (v >> 63)).cast_unsigned()
}

fn unzigzag(v: u64) -> i64 {
    (v >> 1).cast_signed() ^ -(v & 1).cast_signed()
}

#[cfg(test)]
mod tests;
