//! The decoder of compressed pages: one pass into a buffer of exactly the
//! body size the authenticated zstd frame declares, at most the page's cap,
//! and only the one frame shape the writer makes. Anything else is refused
//! before or instead of inflating it.
#![cfg(test)]

use zstd::zstd_safe::CParameter;

use super::tamper_tests::authentic;
use super::tests::{TS, cipher, noise};
use super::{OpenError, PAGE_TARGET_PLAINTEXT, PAGE_VER_Z, body_cap};

/// The body of two records of `len` bytes of `fill` each, zero deltas.
fn two_records(len: usize, fill: u8) -> Vec<u8> {
    let mut body = Vec::new();
    for _ in 0..2 {
        super::body::put_varint(&mut body, len as u64);
    }
    body.extend_from_slice(&[0, 0]);
    body.extend(std::iter::repeat_n(fill, len.checked_mul(2).unwrap()));
    body
}

/// `body` compressed at level 1 with zstd's `parameters` set.
fn compressed(body: &[u8], parameters: &[CParameter]) -> Vec<u8> {
    let mut zstd = zstd::bulk::Compressor::new(1).unwrap();
    for parameter in parameters {
        zstd.set_parameter(*parameter).unwrap();
    }
    zstd.compress(body).unwrap()
}

/// Open a two-record version 7 page whose message is `message`: the
/// opened body's length and capacity, or the refusal.
fn open_two(message: &[u8]) -> Result<(usize, usize), OpenError> {
    let page = authentic(PAGE_VER_Z, 2, TS, message);
    cipher()
        .open(&page)
        .map(|opened| (opened.body_len(), opened.body_capacity()))
}

#[test]
fn a_compressed_body_is_held_in_exactly_its_size() {
    let at_cap = two_records(32_764, 0);
    assert_eq!(at_cap.len(), PAGE_TARGET_PLAINTEXT);
    let message = compressed(&at_cap, &[]);
    assert_eq!(
        open_two(&message),
        Ok((PAGE_TARGET_PLAINTEXT, PAGE_TARGET_PLAINTEXT))
    );
}

#[test]
fn a_frame_must_declare_its_content_size_and_nothing_else() {
    let body = two_records(300, b'a');
    assert_eq!(open_two(&compressed(&body, &[])), Ok((606, 606)));
    let sizeless = compressed(&body, &[CParameter::ContentSizeFlag(false)]);
    assert_eq!(open_two(&sizeless), Err(OpenError::Decompression));
    let checksummed = compressed(&body, &[CParameter::ChecksumFlag(true)]);
    assert_eq!(open_two(&checksummed), Err(OpenError::Decompression));
}

#[test]
fn only_one_frame_opens() {
    let body = two_records(300, b'a');
    let (head, tail) = body.split_at(303);
    let two_frames = [compressed(head, &[]), compressed(tail, &[])].concat();
    assert_eq!(open_two(&two_frames), Err(OpenError::Decompression));
    // A skippable frame (magic 0x184D2A50, four bytes of user data) before
    // the writer's frame.
    let skippable = [
        &[0x50, 0x2a, 0x4d, 0x18, 4, 0, 0, 0, 1, 2, 3, 4][..],
        &compressed(&body, &[]),
    ]
    .concat();
    assert_eq!(open_two(&skippable), Err(OpenError::Decompression));
    let trailing = [compressed(&body, &[]), vec![0]].concat();
    assert_eq!(open_two(&trailing), Err(OpenError::Decompression));
}

#[test]
fn a_body_the_writer_stores_raw_does_not_open_compressed() {
    // Under 256 bytes: the writer never tries to compress it.
    let small = two_records(100, b'a');
    assert_eq!(small.len(), 204);
    assert_eq!(
        open_two(&compressed(&small, &[])),
        Err(OpenError::StoredRaw)
    );
    // zstd cannot shrink it: the writer keeps it raw.
    let mut noisy = two_records(300, 0);
    noisy.truncate(6);
    noisy.extend_from_slice(&noise(9, 600));
    let message = compressed(&noisy, &[]);
    assert!(message.len() >= noisy.len());
    assert_eq!(open_two(&message), Err(OpenError::StoredRaw));
}

#[test]
fn a_declared_size_past_the_cap_is_refused_without_inflating_it() {
    let past = vec![0; PAGE_TARGET_PLAINTEXT + 1];
    assert_eq!(
        open_two(&compressed(&past, &[])),
        Err(OpenError::BodyTooLarge)
    );
    // A frame declaring 1 GiB in 22 bytes: refused from its header.
    let mut frame = vec![0x28, 0xb5, 0x2f, 0xfd, 0xe0];
    frame.extend_from_slice(&(1u64 << 30).to_le_bytes());
    frame.extend_from_slice(&[0x02 | 1, 0x00, 0x10, 0x00]);
    assert_eq!(open_two(&frame), Err(OpenError::BodyTooLarge));
    let one = authentic(PAGE_VER_Z, 1, TS, &frame);
    assert!(body_cap(1) < 1 << 30);
    assert_eq!(cipher().open(&one).err(), Some(OpenError::BodyTooLarge));
}

/// A single-record page whose record is surely longer than the caller's
/// limit is not opened: a raw one is not even decrypted, a compressed one
/// not inflated. The pages below would fail if they were (a forged
/// ciphertext; a frame that declares 1,000 bytes and holds 2,000).
#[test]
fn a_single_record_past_the_limit_is_neither_decrypted_nor_inflated() {
    use super::{CheckedPage, PAGE_VER, PageDecoder};
    use bytes::Bytes;
    let mut decoder = PageDecoder::default();
    let forged = super::admission_tests::forged(PAGE_VER, 0, 1, 1_000 + 16);
    let raw = CheckedPage::admit(Bytes::from(forged), 0).unwrap();
    let open = |page: &CheckedPage, limit, decoder: &mut PageDecoder| {
        cipher()
            .open_within(page, decoder, limit)
            .map(|opened| opened.map(|opened| opened.body_len()))
    };
    // The body holds at least 995 payload bytes.
    assert_eq!(open(&raw, 994, &mut decoder), Ok(None));
    assert_eq!(
        open(&raw, 995, &mut decoder),
        Err(OpenError::Authentication)
    );
    let mut frame = vec![0x28, 0xb5, 0x2f, 0xfd, 0x60];
    frame.extend_from_slice(&(1_000u16 - 256).to_le_bytes());
    // One RLE block of 2,000 zero bytes: last, type 1, size 2,000 << 3.
    let block = 1 | (1 << 1) | (2_000 << 3);
    frame.extend_from_slice(u32::to_le_bytes(block).get(..3).unwrap());
    frame.push(0);
    let compressed = authentic(PAGE_VER_Z, 1, TS, &frame);
    assert_eq!(open(&compressed, 994, &mut decoder), Ok(None));
    assert_eq!(
        open(&compressed, 995, &mut decoder),
        Err(OpenError::Decompression)
    );
}
