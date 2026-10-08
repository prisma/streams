//! Opening: authenticate the whole page, decompress it within its cap, parse
//! its tables exactly, and only then hand out records with their offsets and
//! timestamps.
//!
//! A compressed body is decoded in one pass into a buffer of exactly the
//! size its zstd frame declares, which must be at most the page's cap: no
//! window or streaming buffer is allocated, and a frame cannot inflate past
//! its buffer. Only the frame the writer makes opens: one zstd frame with a
//! content size, no checksum and no dictionary, of a body the writer would
//! have compressed (at least PAGE_COMPRESS_MIN_BYTES, and shrunk).

use aes_gcm_siv::Nonce;
use aes_gcm_siv::aead::{Aead, Payload};
use zstd::zstd_safe;

use super::body::{PageTable, RecordSpan, parse_body};
use super::{
    CheckedPage, OpenError, PAGE_COMPRESS_MIN_BYTES, PageCipher, SINGLE_RECORD_TABLE_MAX, TAG_LEN,
    body_cap,
};

/// The frame header descriptor bits the writer never sets: a dictionary id
/// (bits 0 and 1), the content checksum (bit 2) and the reserved bit 3.
const UNWRITTEN_DESCRIPTOR_BITS: u8 = 0b0000_1111;

/// What opening pages reuses: the zstd context of the compressed pages one
/// read opens, made at the first of them.
#[derive(Default)]
pub(crate) struct PageDecoder {
    zstd: Option<zstd::bulk::Decompressor<'static>>,
}

impl PageDecoder {
    /// Decompress the authenticated message of a compressed page whose body
    /// holds at most `cap` bytes.
    fn decompress(&mut self, message: &[u8], cap: usize) -> Result<Vec<u8>, OpenError> {
        let size = declared_size(message)?;
        if size > cap {
            return Err(OpenError::BodyTooLarge);
        }
        if size < PAGE_COMPRESS_MIN_BYTES || message.len() >= size {
            return Err(OpenError::StoredRaw);
        }
        let zstd = match &mut self.zstd {
            Some(zstd) => zstd,
            None => self
                .zstd
                .insert(zstd::bulk::Decompressor::new().map_err(|_| OpenError::Decompression)?),
        };
        let mut body = Vec::with_capacity(size);
        let written = zstd
            .decompress_to_buffer(message, &mut body)
            .map_err(|_| OpenError::Decompression)?;
        if written != size {
            return Err(OpenError::Decompression);
        }
        Ok(body)
    }
}

/// The body size the message's one zstd frame declares. The message must be
/// exactly one standard frame (no skippable frame, nothing after it) whose
/// descriptor carries a content size and nothing the writer never writes.
fn declared_size(message: &[u8]) -> Result<usize, OpenError> {
    let [0x28, 0xb5, 0x2f, 0xfd, descriptor, ..] = *message else {
        return Err(OpenError::Decompression);
    };
    if descriptor & UNWRITTEN_DESCRIPTOR_BITS != 0
        || zstd_safe::find_frame_compressed_size(message) != Ok(message.len())
    {
        return Err(OpenError::Decompression);
    }
    match zstd_safe::get_frame_content_size(message) {
        Ok(Some(size)) => usize::try_from(size).map_err(|_| OpenError::BodyTooLarge),
        _ => Err(OpenError::Decompression),
    }
}

/// Whether a page of `count` records whose body is `body` bytes holds one
/// record that is surely longer than `limit`: its table takes at most
/// SINGLE_RECORD_TABLE_MAX of the body.
fn surely_over(count: usize, body: usize, limit: usize) -> bool {
    count == 1 && body.saturating_sub(SINGLE_RECORD_TABLE_MAX) > limit
}

impl PageCipher {
    /// Open an admitted page of this cipher's lane with a decoder of its
    /// own and no limit. Every record is returned or none.
    #[cfg(test)]
    pub(crate) fn open(&self, page: &CheckedPage) -> Result<OpenedPage, OpenError> {
        self.open_within(page, &mut PageDecoder::default(), usize::MAX)?
            .ok_or(OpenError::BodyTooLarge)
    }

    /// Open an admitted page of this cipher's lane, decompressing with
    /// `decoder`. Every record is returned or none: authentication,
    /// decompression and the exact body parse all succeed before any record
    /// is visible. None when the page holds one record that is surely
    /// longer than `limit` (a read's remaining budget): a raw page is then
    /// not decrypted and a compressed one not inflated.
    pub(crate) fn open_within(
        &self,
        page: &CheckedPage,
        decoder: &mut PageDecoder,
        limit: usize,
    ) -> Result<Option<OpenedPage>, OpenError> {
        let sealed = page.sealed_bytes();
        let raw_body = sealed.len().saturating_sub(TAG_LEN);
        if !page.is_compressed() && surely_over(page.count(), raw_body, limit) {
            return Ok(None);
        }
        let payload = Payload {
            msg: sealed,
            aad: &self.aad(page.header_bytes()),
        };
        let plain = self
            .cipher
            .decrypt(Nonce::from_slice(page.nonce()), payload)
            .map_err(|_| OpenError::Authentication)?;
        // A raw body is bounded by admission: its ciphertext is at most the
        // cap plus the tag.
        let body = if page.is_compressed() {
            if surely_over(page.count(), declared_size(&plain)?, limit) {
                return Ok(None);
            }
            decoder.decompress(&plain, body_cap(page.count()))?
        } else {
            plain
        };
        let table = parse_body(&body, page.count(), page.ts_ms()).map_err(OpenError::Body)?;
        Ok(Some(OpenedPage {
            first: page.first(),
            last: page.last(),
            body,
            table,
        }))
    }
}

/// An authenticated page whose tables parsed exactly. It holds decrypted
/// plaintext, so it has no Debug form (nor has `PageRecord`).
pub(crate) struct OpenedPage {
    first: u64,
    last: u64,
    body: Vec<u8>,
    table: PageTable,
}

impl OpenedPage {
    #[cfg(test)]
    pub(crate) fn first(&self) -> u64 {
        self.first
    }

    #[cfg(test)]
    pub(crate) fn last(&self) -> u64 {
        self.last
    }

    /// The plaintext body bytes, before any compression: tables and payloads.
    #[cfg(test)]
    pub(crate) fn body_len(&self) -> usize {
        self.body.len()
    }

    /// The bytes the body's buffer holds.
    #[cfg(test)]
    pub(crate) fn body_capacity(&self) -> usize {
        self.body.capacity()
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
#[derive(Clone, Copy, PartialEq, Eq)]
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
