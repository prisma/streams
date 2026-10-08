//! Sealing: cut a request into pages, build each body, compress it when that
//! pays, and encrypt it once under a fresh random nonce.

use aes_gcm_siv::Nonce;
use aes_gcm_siv::aead::{Aead, OsRng, Payload, rand_core::RngCore};

use super::body::{build_body, record_body_bytes, timestamp_delta};
use super::header::{HeaderFields, encode_header};
use super::{
    MAX_RECORD_PLAINTEXT, NONCE_LEN, PAGE_COMPRESS_MIN_BYTES, PAGE_MAX_RECORDS,
    PAGE_TARGET_PLAINTEXT, PAGE_VER, PAGE_VER_Z, PageCipher, PageLane, SealError, SealRecord,
    SealedPage, body_cap, last_offset,
};

/// A request's records cut into pages, in order. A page takes records while
/// its body, each record's length and timestamp delta counted at its real
/// varint width, stays within PAGE_TARGET_PLAINTEXT and it holds at most
/// PAGE_MAX_RECORDS; a record whose body alone is larger is a page by itself.
pub(crate) fn split_pages<'r, 'a>(
    records: &'r [SealRecord<'a>],
) -> Result<Vec<&'r [SealRecord<'a>]>, SealError> {
    let mut pages = Vec::new();
    let mut rest = records;
    while !rest.is_empty() {
        let (page, tail) = rest.split_at(page_len(rest)?);
        pages.push(page);
        rest = tail;
    }
    Ok(pages)
}

/// How many of the leading `records` the next page takes; at least one.
fn page_len(records: &[SealRecord<'_>]) -> Result<usize, SealError> {
    let (mut body, mut taken, mut previous) = (0usize, 0usize, None);
    for record in records.iter().take(PAGE_MAX_RECORDS) {
        let len = record.payload.len();
        if len > MAX_RECORD_PLAINTEXT {
            return Err(SealError::RecordTooLarge);
        }
        let delta = timestamp_delta(previous, record.ts_ms).ok_or(SealError::TimestampOrder)?;
        let grown = body.saturating_add(record_body_bytes(len, delta));
        if taken > 0 && grown > PAGE_TARGET_PLAINTEXT {
            break;
        }
        body = grown;
        taken = taken.saturating_add(1);
        previous = Some(record.ts_ms);
    }
    Ok(taken)
}

/// The message a page encrypts: its body zstd level 1 when the body is at
/// least PAGE_COMPRESS_MIN_BYTES and compression shrinks it, raw otherwise.
/// A compressor error only means compression did not pay.
fn compress(body: Vec<u8>) -> (u8, Vec<u8>) {
    if body.len() >= PAGE_COMPRESS_MIN_BYTES
        && let Ok(compressed) = zstd::bulk::compress(&body, 1)
        && compressed.len() < body.len()
    {
        return (PAGE_VER_Z, compressed);
    }
    (PAGE_VER, body)
}

impl PageCipher {
    /// Seal a whole request starting at offset `first`: one page per
    /// `split_pages` group, all or none. Each page is returned with the last
    /// offset its row is keyed by. Timestamps never go back, across pages
    /// too.
    pub(crate) fn seal_request(
        &self,
        lane: &PageLane<'_>,
        first: u64,
        records: &[SealRecord<'_>],
    ) -> Result<Vec<SealedPage>, SealError> {
        if records.windows(2).any(|pair| match pair {
            [before, after] => after.ts_ms < before.ts_ms,
            _ => false,
        }) {
            return Err(SealError::TimestampOrder);
        }
        let mut pages = Vec::new();
        let mut next = Some(first);
        for page in split_pages(records)? {
            let sealed = self.seal(lane, next.ok_or(SealError::OffsetOverflow)?, page)?;
            next = sealed.last.checked_add(1);
            pages.push(sealed);
        }
        Ok(pages)
    }

    /// Seal one page of `records` starting at offset `first`.
    pub(crate) fn seal(
        &self,
        lane: &PageLane<'_>,
        first: u64,
        records: &[SealRecord<'_>],
    ) -> Result<SealedPage, SealError> {
        let mut nonce = [0; NONCE_LEN];
        OsRng.fill_bytes(&mut nonce);
        self.seal_with_nonce(lane, first, records, nonce)
    }

    // Private: production reaches it only with a fresh random nonce. Fixed
    // nonces exist for the golden vectors.
    pub(super) fn seal_with_nonce(
        &self,
        lane: &PageLane<'_>,
        first: u64,
        records: &[SealRecord<'_>],
        nonce: [u8; NONCE_LEN],
    ) -> Result<SealedPage, SealError> {
        let count = records.len();
        let Some(head) = records.first() else {
            return Err(SealError::Empty);
        };
        if count > PAGE_MAX_RECORDS {
            return Err(SealError::TooManyRecords);
        }
        let last = last_offset(first, count).ok_or(SealError::OffsetOverflow)?;
        if records
            .iter()
            .any(|record| record.payload.len() > MAX_RECORD_PLAINTEXT)
        {
            return Err(SealError::RecordTooLarge);
        }
        let body = build_body(records).ok_or(SealError::TimestampOrder)?;
        if body.len() > body_cap(count) {
            return Err(SealError::PageTooLarge);
        }
        let (ver, message) = compress(body);
        let fields = HeaderFields {
            ver,
            first,
            count,
            ts_ms: head.ts_ms,
            lane,
            nonce,
        };
        let bytes = self.seal_message(&fields, &message)?;
        Ok(SealedPage { last, bytes })
    }

    /// Encrypt an already built (and possibly compressed) message under the
    /// header `fields`. The message is not checked against them.
    pub(super) fn seal_message(
        &self,
        fields: &HeaderFields<'_>,
        message: &[u8],
    ) -> Result<Vec<u8>, SealError> {
        let mut page = encode_header(fields)?;
        let payload = Payload {
            msg: message,
            aad: &self.aad(&page),
        };
        let sealed = self
            .cipher
            .encrypt(Nonce::from_slice(&fields.nonce), payload)
            .map_err(|_| SealError::Cipher)?;
        let ct_len = u32::try_from(sealed.len()).map_err(|_| SealError::PageTooLarge)?;
        page.extend_from_slice(&ct_len.to_be_bytes());
        page.extend_from_slice(&sealed);
        Ok(page)
    }
}
