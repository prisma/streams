//! Authentication covers the whole clear header, the segment identity and
//! the lane's key; an authenticated body still parses exactly and
//! decompresses only up to its cap. No failure returns any record.
#![cfg(test)]

use aes_gcm_siv::Aes256GcmSiv;
use aes_gcm_siv::aead::KeyInit;
use bytes::Bytes;
use hkdf::Hkdf;
use sha2::Sha256;

use super::header::HeaderFields;
use super::tests::{FIRST, NONCE, SEGMENT, TS, cipher, flip, lane, reference, subkey};
use super::{
    BodyError, CheckedPage, OpenError, PAGE_TARGET_PLAINTEXT, PAGE_VER, PAGE_VER_Z, PageCipher,
    PageLane, body_cap, page_key,
};

fn opened(cipher: &PageCipher, raw: Vec<u8>, last: u64) -> Result<Vec<(u64, i64)>, OpenError> {
    let page = CheckedPage::admit(Bytes::from(raw), last).unwrap();
    let records = cipher.open(&page)?;
    Ok(records
        .records()
        .map(|record| (record.offset, record.ts_ms))
        .collect())
}

/// An authentic page of `count` records from offset 0 whose message (the
/// body, compressed when `ver` is 7) is exactly `message`.
fn authentic(ver: u8, count: usize, ts_ms: i64, message: &[u8]) -> CheckedPage {
    let lane = PageLane { ts_ms, ..lane() };
    let fields = HeaderFields {
        ver,
        first: 0,
        count,
        lane: &lane,
        nonce: NONCE,
    };
    let raw = cipher().seal_message(&fields, message).unwrap();
    let last = u64::try_from(count).unwrap().checked_sub(1).unwrap();
    CheckedPage::admit(Bytes::from(raw), last).unwrap()
}

/// A zstd frame of `blocks` RLE blocks of 128 KiB zeros each: four input
/// bytes per block, with no content size, so only the reader's cap stops it.
fn rle_bomb(blocks: usize) -> Vec<u8> {
    // Magic, a descriptor with no content size, and a 128 KiB window.
    let mut frame = vec![0x28, 0xb5, 0x2f, 0xfd, 0x00, 0x38];
    for block in 0..blocks {
        let last = u8::from(block.checked_add(1) == Some(blocks));
        // Block header (LE): last bit, type 1 (RLE), size 131072 << 3.
        frame.extend_from_slice(&[0x02 | last, 0x00, 0x10, 0x00]);
    }
    frame
}

#[test]
fn the_reference_page_opens_to_its_records() {
    let records = opened(&cipher(), reference(), 42).unwrap();
    assert_eq!(records, [(FIRST, TS), (41, TS), (42, TS)]);
}

#[test]
fn every_clear_header_byte_is_authenticated() {
    // first, count, ts_ms, key version, routing key and nonce bytes.
    let positions = (1..10).chain(10..22).chain(24..38);
    for at in positions {
        let mut raw = reference();
        flip(&mut raw, at);
        let page = CheckedPage::admit(Bytes::from(raw.clone()), 0)
            .err()
            .and_then(|refusal| match refusal {
                super::PageCorruption::Offset { page, .. } => Some(page),
                _ => None,
            })
            .unwrap_or_else(|| panic!("byte {at} keeps the page admissible"));
        assert_eq!(
            opened(&cipher(), raw, page),
            Err(OpenError::Authentication),
            "byte {at}"
        );
    }
    let mut compressed = reference();
    *compressed.first_mut().unwrap() = PAGE_VER_Z;
    assert_eq!(
        opened(&cipher(), compressed, 42),
        Err(OpenError::Authentication)
    );
}

#[test]
fn every_ciphertext_and_tag_byte_is_authenticated() {
    for at in 42..78 {
        let mut raw = reference();
        flip(&mut raw, at);
        assert_eq!(
            opened(&cipher(), raw, 42),
            Err(OpenError::Authentication),
            "byte {at}"
        );
    }
}

#[test]
fn the_segment_identity_is_bound_by_the_key_and_by_the_aad() {
    let other = [0x23; 16];
    let moved = PageCipher::new(&subkey("rk", 1), &other);
    assert_eq!(
        opened(&moved, reference(), 42),
        Err(OpenError::Authentication)
    );
    let other_aad = PageCipher {
        cipher: Aes256GcmSiv::new((&page_key(&subkey("rk", 1), &SEGMENT)).into()),
        segment: other,
    };
    assert_eq!(
        opened(&other_aad, reference(), 42),
        Err(OpenError::Authentication)
    );
    let other_key = PageCipher {
        cipher: Aes256GcmSiv::new((&page_key(&subkey("rk", 1), &other)).into()),
        segment: SEGMENT,
    };
    assert_eq!(
        opened(&other_key, reference(), 42),
        Err(OpenError::Authentication)
    );
}

#[test]
fn only_the_lanes_routing_key_and_key_version_open_it() {
    for (routing_key, key_version) in [("rk", 0), ("rk", 2), ("rl", 1), ("", 1)] {
        let wrong = PageCipher::new(&subkey(routing_key, key_version), &SEGMENT);
        assert_eq!(
            opened(&wrong, reference(), 42),
            Err(OpenError::Authentication),
            "{routing_key} v{key_version}"
        );
    }
}

#[test]
fn a_frame_key_of_the_same_subkey_and_segment_cannot_open_a_page() {
    let mut frame_key = [0; 32];
    Hkdf::<Sha256>::new(Some(&SEGMENT), &subkey("rk", 1))
        .expand(b"prisma-streams/frame/v4/aes-256-gcm-siv", &mut frame_key)
        .unwrap();
    assert_ne!(frame_key, page_key(&subkey("rk", 1), &SEGMENT));
    let frame = PageCipher {
        cipher: Aes256GcmSiv::new((&frame_key).into()),
        segment: SEGMENT,
    };
    assert_eq!(
        opened(&frame, reference(), 42),
        Err(OpenError::Authentication)
    );
}

#[test]
fn an_authenticated_body_must_parse_exactly() {
    let refusal = |count: usize, ts_ms: i64, message: &[u8]| {
        let page = authentic(PAGE_VER, count, ts_ms, message);
        cipher()
            .open(&page)
            .map(|records| records.records().count())
    };
    let body = |error| Err(OpenError::Body(error));
    assert_eq!(
        refusal(2, TS, &[2, 2, 0, 0, 1, 2, 3]),
        body(BodyError::PayloadShort)
    );
    assert_eq!(
        refusal(2, TS, &[2, 2, 0, 0, 1, 2, 3, 4, 5]),
        body(BodyError::Trailing)
    );
    assert_eq!(refusal(2, TS, &[2, 2, 0, 0, 1, 2, 3, 4]), Ok(2));
    assert_eq!(refusal(1, TS, &[0x80]), body(BodyError::LengthTable));
    assert_eq!(
        refusal(1, TS, &[0x81, 0x00, 0, 7]),
        body(BodyError::LengthTable)
    );
    assert_eq!(refusal(1, TS, &[1]), body(BodyError::DeltaTable));
    assert_eq!(
        refusal(1, TS, &[1, 0x80, 0x00, 7]),
        body(BodyError::DeltaTable)
    );
    assert_eq!(refusal(1, i64::MAX, &[1, 1, 7]), body(BodyError::Timestamp));
    assert_eq!(refusal(1, i64::MAX, &[1, 0, 7]), Ok(1));
}

#[test]
fn a_timestamp_delta_is_added_to_the_page_timestamp() {
    let page = authentic(PAGE_VER, 2, -5, &[1, 1, 0, 0x83, 0x01, b'a', b'b']);
    let records = cipher().open(&page).unwrap();
    let seen: Vec<(u64, i64, &[u8])> = records
        .records()
        .map(|record| (record.offset, record.ts_ms, record.payload))
        .collect();
    assert_eq!(seen, [(0, -5, &b"a"[..]), (1, 126, &b"b"[..])]);
}

#[test]
fn a_compressed_body_must_be_zstd() {
    let page = authentic(PAGE_VER_Z, 1, TS, b"not a zstd frame");
    assert_eq!(cipher().open(&page).err(), Some(OpenError::Decompression));
}

#[test]
fn a_compressed_body_opens_at_exactly_its_cap_and_not_one_byte_past() {
    let at_cap = vec![vec![0; 32_764], vec![0; 32_764]];
    let sealed = cipher().seal_request(&lane(), 0, &at_cap).unwrap();
    let [page] = sealed.as_slice() else {
        panic!("one page")
    };
    let page = CheckedPage::admit(Bytes::from(page.bytes.clone()), 1).unwrap();
    assert!(page.is_compressed());
    assert_eq!(
        cipher().open(&page).unwrap().body_len(),
        PAGE_TARGET_PLAINTEXT
    );

    let past = zstd::bulk::compress(&[0; PAGE_TARGET_PLAINTEXT + 1], 1).unwrap();
    let page = authentic(PAGE_VER_Z, 2, TS, &past);
    assert_eq!(cipher().open(&page).err(), Some(OpenError::BodyTooLarge));
}

#[test]
fn a_decompression_bomb_is_cut_at_the_cap_without_inflating_it() {
    let two_blocks = zstd::stream::decode_all(rle_bomb(2).as_slice()).unwrap();
    assert_eq!(
        two_blocks,
        vec![0; 256 << 10],
        "the bomb is a valid zstd frame"
    );
    // 1 GiB of zeros in 32 KiB: admitted, and refused after cap + 1 bytes.
    let gib = rle_bomb(8192);
    assert_eq!(gib.len(), 6 + 8192 * 4);
    let page = authentic(PAGE_VER_Z, 2, TS, &gib);
    assert_eq!(cipher().open(&page).err(), Some(OpenError::BodyTooLarge));
    // A single-record page stops at the record cap's body, not 64 MiB.
    let page = authentic(PAGE_VER_Z, 1, TS, &rle_bomb(512));
    assert_eq!(body_cap(1), (32 << 20) + 14);
    assert_eq!(cipher().open(&page).err(), Some(OpenError::BodyTooLarge));
}

/// Decrypted plaintext has no `{:?}` form: neither an opened page nor one
/// of its records implements Debug, so no log field, failed assertion or
/// `unwrap_err` can print a customer's payload. The probe resolves to the
/// inherent `DEBUG` only for a type that is Debug; `u8` shows it does.
#[test]
fn opened_plaintext_has_no_debug_form() {
    trait NotDebug {
        const DEBUG: bool = false;
    }
    impl<T: ?Sized> NotDebug for T {}
    struct Probe<T: ?Sized>(std::marker::PhantomData<T>);
    impl<T: ?Sized + std::fmt::Debug> Probe<T> {
        const DEBUG: bool = true;
    }
    let printable = [
        Probe::<u8>::DEBUG,
        Probe::<super::OpenedPage>::DEBUG,
        Probe::<super::open::PageRecord<'static>>::DEBUG,
    ];
    assert_eq!(
        printable,
        [true, false, false],
        "[u8, OpenedPage, PageRecord] have a Debug form"
    );
}
