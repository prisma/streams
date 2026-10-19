//! A read batch is one record of the drain's `_usage` append: the body is
//! one JSON array, one record per envelope, and an append with one record
//! over the per-record ceiling (`MAX_RECORD_PAYLOAD_BYTES`, 131,072 bytes by
//! default and in the release profile) is refused whole (G4), so a single
//! oversized batch stopped every later usage drain of the cell. This module
//! owns the bound that keeps a batch inside one record: the accumulator
//! seals under it, and a spooled batch over the ceiling (sealed by a binary
//! that sealed at an estimated 1 MiB) drains as pieces.

use super::{BillingIdentity, ReadBatch, ReadRow, UsageEnvelope, UsagePayload};

/// The most encoded row bytes a sealed read batch carries: half the default
/// record ceiling, which leaves its envelope (source, cell, event id, under
/// 1 KiB) far inside one record.
pub(crate) const READ_BATCH_ROW_BYTES: usize = 64 << 10;

/// The most encoded read-batch bytes one drain round appends, at least one
/// batch: with the round's 1 MB of segment rows, a body far under the
/// smallest request-body limit a profile sets (8 MiB, shared-cell).
pub(super) const READ_ROUND_BYTES: usize = 4 << 20;

/// The encoded size of a read row whose identity strings are empty and whose
/// five counters are all `u64::MAX`, plus the comma that separates it from
/// the next row.
const ROW_FIXED_MAX: usize = 262;

impl BillingIdentity {
    /// An upper bound on the encoded size of this identity's read row,
    /// whatever its counters: JSON escapes a quote, a backslash or a control
    /// byte in at most six bytes.
    pub(crate) fn read_row_bound(&self) -> usize {
        let strings = [
            &self.account_id,
            &self.project_id,
            &self.stream_id,
            &self.stream_name,
        ];
        ROW_FIXED_MAX + strings.iter().map(|s| escaped_bound(s)).sum::<usize>()
    }
}

fn escaped_bound(text: &str) -> usize {
    let escaped = text.bytes().filter(|b| matches!(b, b'"' | b'\\' | 0..0x20));
    text.len() + 5 * escaped.count()
}

/// Consecutive runs of `rows`, in order, whose row bounds sum to at most
/// `READ_BATCH_ROW_BYTES` (a run holds at least one row).
pub(crate) fn split_rows(rows: Vec<ReadRow>) -> Vec<Vec<ReadRow>> {
    let (mut runs, mut run, mut bytes) = (Vec::new(), Vec::new(), 0);
    for row in rows {
        let bound = row.identity.read_row_bound();
        if !run.is_empty() && bytes + bound > READ_BATCH_ROW_BYTES {
            runs.push(std::mem::take(&mut run));
            bytes = 0;
        }
        bytes += bound;
        run.push(row);
    }
    if !run.is_empty() {
        runs.push(run);
    }
    runs
}

/// How one drain round turns read batches into `_usage` envelopes.
pub(super) struct ReadEmission<'a> {
    pub(super) cell: &'a str,
    /// The per-record ceiling appends enforce (0 = unlimited).
    pub(super) ceiling: usize,
}

impl ReadEmission<'_> {
    /// Adds the batch's envelopes to the round unless the round already
    /// carries envelopes and they would take its body past `budget`; a
    /// spooled batch left out stays in the spool for the next round.
    pub(super) fn admit(
        &self,
        rb: &ReadBatch,
        envelopes: &mut Vec<UsageEnvelope>,
        body_bytes: &mut usize,
        budget: usize,
    ) -> bool {
        let batch = self.envelopes(rb);
        let bytes = batch.iter().map(super::encoded_size).sum::<usize>();
        if !envelopes.is_empty() && *body_bytes + bytes > budget {
            return false;
        }
        *body_bytes += bytes;
        envelopes.extend(batch);
        true
    }

    /// One envelope identified by the batch's source boot and sequence, or,
    /// for a batch whose record would exceed the ceiling, one per run of
    /// `split_rows`: run i carries source boot `{boot}/{seq}` and sequence
    /// i, so the rollup's per-boot sequence floor deduplicates a second
    /// emission run by run. The runs depend only on the batch's row order,
    /// which the spool keeps, so every emission of a batch is the same.
    fn envelopes(&self, rb: &ReadBatch) -> Vec<UsageEnvelope> {
        let whole = self.envelope(rb.clone());
        let record = serde_json::to_vec(&whole).map_or(usize::MAX, |v| v.len());
        if self.ceiling == 0 || record <= self.ceiling {
            return vec![whole];
        }
        let mut source = rb.source.clone();
        source.boot = format!("{}/{}", rb.source.boot, rb.seq);
        let runs = split_rows(rb.rows.clone()).into_iter().zip(0u64..);
        runs.map(|(rows, seq)| {
            self.envelope(ReadBatch {
                source: source.clone(),
                seq,
                from_ms: rb.from_ms,
                to_ms: rb.to_ms,
                rows,
            })
        })
        .collect()
    }

    fn envelope(&self, rb: ReadBatch) -> UsageEnvelope {
        let event_id = format!("read/{}/{}", rb.source.boot, rb.seq);
        let event_time_ms = rb.to_ms;
        super::envelope(
            self.cell,
            UsagePayload::ReadBatch(rb),
            event_time_ms,
            event_id,
        )
    }
}

#[cfg(test)]
mod tests;
