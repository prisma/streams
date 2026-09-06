//! One returned-page contract for local execution, stitched scans and peers.
//! Payload is charged in plaintext bytes. A first record may exceed the
//! requested page size (so small requests cannot wedge a cursor), up to the
//! record limit. Record count and conservatively escaped JSON metadata have
//! independent caps. Storage work is chunked separately from returned data.
use crate::crypto::MAX_RECORD_PLAINTEXT;

pub(crate) const MAX_PAGE_PLAINTEXT: usize = 8 << 20;
pub(crate) const MAX_PAGE_RECORDS: usize = 4096;
pub(crate) const MAX_PAGE_METADATA: usize = 1 << 20;
pub(crate) const SCAN_WINDOW: u64 = 4096;
pub(crate) const MAX_SCAN_BATCH_BYTES: usize = MAX_PAGE_PLAINTEXT;

#[derive(Clone)]
pub(crate) struct PageBudget {
    requested: usize,
    plaintext: usize,
    metadata: usize,
    records: usize,
}
impl PageBudget {
    pub(crate) fn new(requested: usize) -> Self {
        Self {
            requested: requested.clamp(1, MAX_PAGE_PLAINTEXT),
            plaintext: 0,
            metadata: 0,
            records: 0,
        }
    }
    pub(crate) fn remaining(&self) -> usize {
        self.requested.saturating_sub(self.plaintext)
    }
    pub(crate) fn decode_limit(&self) -> usize {
        if self.records == 0 {
            MAX_RECORD_PLAINTEXT
        } else {
            self.remaining()
        }
    }
    pub(crate) fn metadata_fits(&self, key: &str) -> bool {
        key.len() <= u16::MAX as usize
            && self.records < MAX_PAGE_RECORDS
            && self.metadata + metadata_charge(key) <= MAX_PAGE_METADATA
    }
    pub(crate) fn admit(&mut self, plaintext: usize, key: &str) -> bool {
        if self.full() || plaintext > self.decode_limit() || !self.metadata_fits(key) {
            return false;
        }
        self.plaintext += plaintext;
        self.metadata += metadata_charge(key);
        self.records += 1;
        true
    }
    pub(crate) fn full(&self) -> bool {
        self.remaining() == 0
            || self.records == MAX_PAGE_RECORDS
            || self.metadata + 128 > MAX_PAGE_METADATA
    }
}
fn metadata_charge(key: &str) -> usize {
    // JSON can escape each key byte as six bytes. 128 covers the offset,
    // object fields/punctuation and base64 padding for each record.
    key.len() * 6 + 128
}
pub(crate) fn max_wire_bytes() -> usize {
    MAX_RECORD_PLAINTEXT.div_ceil(3) * 4 + MAX_PAGE_METADATA + 2048
}
