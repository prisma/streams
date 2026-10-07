//! Opening: authenticate the whole page, decompress it within its cap, parse
//! its tables exactly, and only then hand out records with their offsets and
//! timestamps.

use std::io::Read;

use aes_gcm_siv::Nonce;
use aes_gcm_siv::aead::{Aead, Payload};

use super::body::{PageTable, RecordSpan, parse_body};
use super::{CheckedPage, OpenError, PageCipher, body_cap};

/// zstd window limit: the 32 MiB record cap's, as stored frames use.
const WINDOW_LOG_MAX: u32 = 25;

/// Decompress at most `cap` body bytes. Reading stops at cap + 1, so an
/// oversized body is refused without being materialised.
fn decompress(compressed: &[u8], cap: usize) -> Result<Vec<u8>, OpenError> {
    let mut decoder =
        zstd::stream::read::Decoder::new(compressed).map_err(|_| OpenError::Decompression)?;
    decoder
        .window_log_max(WINDOW_LOG_MAX)
        .map_err(|_| OpenError::Decompression)?;
    let mut body = Vec::new();
    decoder
        .take((cap as u64).saturating_add(1))
        .read_to_end(&mut body)
        .map_err(|_| OpenError::Decompression)?;
    if body.len() > cap {
        return Err(OpenError::BodyTooLarge);
    }
    Ok(body)
}

impl PageCipher {
    /// Open an admitted page of this cipher's lane. Every record is returned
    /// or none: authentication, decompression and the exact body parse all
    /// succeed before any record is visible.
    pub(crate) fn open(&self, page: &CheckedPage) -> Result<OpenedPage, OpenError> {
        let payload = Payload {
            msg: page.sealed_bytes(),
            aad: &self.aad(page.header_bytes()),
        };
        let plain = self
            .cipher
            .decrypt(Nonce::from_slice(page.nonce()), payload)
            .map_err(|_| OpenError::Authentication)?;
        // A raw body is bounded by admission: its ciphertext is at most the
        // cap plus the tag.
        let body = if page.is_compressed() {
            decompress(&plain, body_cap(page.count()))?
        } else {
            plain
        };
        let table = parse_body(&body, page.count(), page.ts_ms()).map_err(OpenError::Body)?;
        Ok(OpenedPage {
            first: page.first(),
            last: page.last(),
            body,
            table,
        })
    }
}

/// An authenticated page whose tables parsed exactly.
#[derive(Debug)]
pub(crate) struct OpenedPage {
    first: u64,
    last: u64,
    body: Vec<u8>,
    table: PageTable,
}

impl OpenedPage {
    pub(crate) fn first(&self) -> u64 {
        self.first
    }

    pub(crate) fn last(&self) -> u64 {
        self.last
    }

    /// The plaintext body bytes, before any compression: tables and payloads.
    pub(crate) fn body_len(&self) -> usize {
        self.body.len()
    }

    /// The records in offset order, from `first` through `last`.
    pub(crate) fn records(&self) -> PageRecords<'_> {
        let (_, payloads) = self.body.split_at(self.table.payload_start);
        PageRecords {
            offsets: self.first..=self.last,
            spans: self.table.records.iter(),
            payloads,
        }
    }
}

/// One record of an opened page.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct PageRecord<'a> {
    pub(crate) offset: u64,
    pub(crate) ts_ms: i64,
    pub(crate) payload: &'a [u8],
}

/// The records of an opened page. The parse proved one span per offset and
/// that the spans tile the payload bytes, so every split below is in range.
pub(crate) struct PageRecords<'a> {
    offsets: std::ops::RangeInclusive<u64>,
    spans: std::slice::Iter<'a, RecordSpan>,
    payloads: &'a [u8],
}

impl<'a> Iterator for PageRecords<'a> {
    type Item = PageRecord<'a>;

    fn next(&mut self) -> Option<PageRecord<'a>> {
        let span = self.spans.next()?;
        let offset = self.offsets.next()?;
        let (payload, rest) = self.payloads.split_at(span.len);
        self.payloads = rest;
        Some(PageRecord {
            offset,
            ts_ms: span.ts_ms,
            payload,
        })
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.spans.size_hint()
    }
}
