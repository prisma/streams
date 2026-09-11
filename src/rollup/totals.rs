//! The invoice meter view shared by customer responses and reconciliation.
//! Frozen invoice bases remain authoritative when late snapshots advance
//! the live segment floors. Storage is handled separately: only stream rows
//! extrapolate it provisionally.

use super::{AggRow, MonthRow, eff_u64, eff_u128};

#[derive(Debug, Default, PartialEq)]
pub(super) struct InvoiceMeters {
    pub(super) ingest_bytes: u64,
    pub(super) ingest_records: u64,
    pub(super) read_payload_bytes: u64,
    pub(super) read_records: u64,
    pub(super) read_operations: u64,
    pub(super) queue_operations: u64,
    pub(super) append_requests: u64,
}

impl InvoiceMeters {
    pub(super) fn add(&mut self, other: &Self) {
        self.ingest_bytes += other.ingest_bytes;
        self.ingest_records += other.ingest_records;
        self.read_payload_bytes += other.read_payload_bytes;
        self.read_records += other.read_records;
        self.read_operations += other.read_operations;
        self.queue_operations += other.queue_operations;
        self.append_requests += other.append_requests;
    }
}

impl MonthRow {
    pub(super) fn invoice_meters(&self) -> InvoiceMeters {
        let (ib, irec, rpb, rrec, rop, qop, areq) = match &self.frozen {
            Some(f) => (
                f.ingest_bytes,
                f.ingest_records,
                f.read_payload_bytes,
                f.read_records,
                f.read_operations,
                f.queue_operations,
                f.append_requests,
            ),
            None => (
                self.ingest_bytes(),
                self.ingest_records(),
                self.read_payload_bytes,
                self.read_records,
                self.read_operations,
                self.queue_operations,
                self.append_requests,
            ),
        };
        InvoiceMeters {
            ingest_bytes: eff_u64(ib, self.corr.ingest_payload_bytes_delta),
            ingest_records: eff_u64(irec, self.corr.ingest_records_delta),
            read_payload_bytes: eff_u64(rpb, self.corr.read_payload_bytes_delta),
            read_records: eff_u64(rrec, self.corr.read_records_delta),
            read_operations: eff_u64(rop, self.corr.read_operations_delta),
            queue_operations: eff_u64(qop, self.corr.queue_operations_delta),
            append_requests: eff_u64(areq, self.corr.append_requests_delta),
        }
    }

    /// Effective invoice totals: frozen base when finalized, live counters
    /// otherwise, plus each materialized correction exactly once.
    pub(crate) fn effective(&self) -> serde_json::Value {
        let meters = self.invoice_meters();
        let storage = match &self.frozen {
            Some(f) => f.storage_byte_ms.parse::<u128>().unwrap_or(0),
            None => self.storage_byte_ms(),
        };
        serde_json::json!({
            "ingestPayloadBytes": meters.ingest_bytes,
            "ingestRecords": meters.ingest_records,
            "readPayloadBytes": meters.read_payload_bytes,
            "readRecords": meters.read_records,
            "readOperations": meters.read_operations,
            "queueOperations": meters.queue_operations,
            "appendRequests": meters.append_requests,
            "storageByteSeconds": (eff_u128(storage, &self.corr.storage_byte_ms_delta) / 1000).to_string(),
            "correctionCount": self.corr.count,
        })
    }
}

impl AggRow {
    pub(super) fn invoice_meters(&self) -> InvoiceMeters {
        InvoiceMeters {
            ingest_bytes: eff_u64(self.ingest_bytes, self.corr.ingest_payload_bytes_delta),
            ingest_records: eff_u64(self.ingest_records, self.corr.ingest_records_delta),
            read_payload_bytes: eff_u64(
                self.read_payload_bytes,
                self.corr.read_payload_bytes_delta,
            ),
            read_records: eff_u64(self.read_records, self.corr.read_records_delta),
            read_operations: eff_u64(self.read_operations, self.corr.read_operations_delta),
            queue_operations: eff_u64(self.queue_operations, self.corr.queue_operations_delta),
            append_requests: eff_u64(self.append_requests, self.corr.append_requests_delta),
        }
    }
}
