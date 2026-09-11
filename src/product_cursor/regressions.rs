//! Falsifiable shape, authentication and allocation-admission controls.
use super::{
    CatalogCursor, KIND_CATALOG_V1, KIND_KEY_V2, KIND_LEASE_V2, KIND_MSG_V2, KIND_SCAN_V2,
    KeyCursor, LeaseToken, MessageId, ScanCursor, StreamKey, b64, mac_key, mac16, unb64,
};
use crate::tenant::ProjectId;
use proptest::{prop_assert, prop_assert_eq};

fn project() -> ProjectId {
    ProjectId::new("cursor-regression").unwrap()
}
fn key() -> StreamKey {
    StreamKey([7; 32])
}

fn sign(mut payload: Vec<u8>) -> String {
    let tag = mac16(&mac_key(&project(), &key(), &[1; 16]), &payload);
    payload.extend_from_slice(&tag);
    b64(&payload)
}

fn scan_shape(count: u32, rows: &[u8], tail: &[u8]) -> String {
    let mut p = vec![KIND_SCAN_V2];
    p.extend_from_slice(&[1; 16]);
    p.extend_from_slice(&3u64.to_le_bytes());
    p.extend_from_slice(&count.to_le_bytes());
    p.extend_from_slice(rows);
    p.extend_from_slice(tail);
    sign(p)
}

#[test]
fn scan_count_is_admitted_against_the_complete_wire_rows() {
    let row = [0; 12];
    let tail = [0; 20];
    for (count, rows, tail) in [
        (1, &[][..], tail.as_slice()),
        (0, row.as_slice(), tail.as_slice()),
        (1, &[0; 11][..], tail.as_slice()),
        (1, row.as_slice(), &[0; 19][..]),
        (u32::MAX, &[][..], tail.as_slice()),
    ] {
        assert_eq!(
            ScanCursor::decode(
                &scan_shape(count, rows, tail),
                &project(),
                &key(),
                &[1; 16],
                0
            ),
            Err("invalid_cursor")
        );
    }
    let empty =
        ScanCursor::decode(&scan_shape(0, &[], &tail), &project(), &key(), &[1; 16], 0).unwrap();
    assert!(empty.segments.is_empty());
    assert_eq!(empty.map_version, 3);
    assert_eq!(empty.current_index, 0);
    assert_eq!(empty.current_offset, 0);
    assert_eq!(empty.expires_at_ms, 0);
    assert_eq!(
        ScanCursor::decode(&scan_shape(0, &[], &tail), &project(), &key(), &[1; 16], 1),
        Err("scan_expired")
    );
}

#[test]
fn scan_accepts_the_last_complete_wire_size_and_rejects_the_next() {
    let mut cursor = ScanCursor {
        epoch: [1; 16],
        map_version: 17,
        segments: vec![(7, 19); 1360],
        current_index: 11,
        current_offset: 13,
        expires_at_ms: 23,
    };
    let token = cursor.encode(&project(), &key());
    assert_eq!(
        ScanCursor::decode(&token, &project(), &key(), &[1; 16], 23).unwrap(),
        cursor
    );
    cursor.segments.push((29, 31));
    assert_eq!(
        ScanCursor::decode(
            &cursor.encode(&project(), &key()),
            &project(),
            &key(),
            &[1; 16],
            23
        ),
        Err("invalid_cursor")
    );
}

#[test]
fn fixed_tokens_reject_each_truncation_and_trailing_byte() {
    let message = MessageId {
        epoch: [1; 16],
        key_hash: [2; 16],
        seg_id: 3,
        offset: 5,
    };
    let key_cursor = KeyCursor {
        epoch: message.epoch,
        key_hash: message.key_hash,
        seg_id: message.seg_id,
        offset: message.offset,
    };
    let lease = LeaseToken {
        msg: message.clone(),
        lease_gen: 7,
        consumer_gen: 11,
        deadline_ms: 13,
    };
    for token in [
        key_cursor.encode(&project(), &key()),
        message.encode(&project(), &key()),
        lease.encode(&project(), &key()),
    ] {
        let raw = unb64(&token).unwrap();
        for end in 0..raw.len() {
            let short = b64(raw.get(..end).unwrap());
            assert!(KeyCursor::decode(&short, &project(), &key(), &[1; 16], &[2; 16]).is_err());
            assert!(MessageId::decode(&short, &project(), &key(), &[1; 16]).is_err());
            assert!(LeaseToken::decode(&short, &project(), &key(), &[1; 16]).is_err());
        }
        let mut extra = raw;
        extra.push(0);
        let extra = b64(&extra);
        assert!(KeyCursor::decode(&extra, &project(), &key(), &[1; 16], &[2; 16]).is_err());
        assert!(MessageId::decode(&extra, &project(), &key(), &[1; 16]).is_err());
        assert!(LeaseToken::decode(&extra, &project(), &key(), &[1; 16]).is_err());
    }
    assert_eq!(
        KeyCursor::decode(
            &b64(&[KIND_SCAN_V2]),
            &project(),
            &key(),
            &[1; 16],
            &[2; 16]
        ),
        Err("wrong_cursor_kind")
    );
    assert_eq!(
        ScanCursor::decode(&b64(&[KIND_KEY_V2]), &project(), &key(), &[1; 16], 0),
        Err("wrong_cursor_kind")
    );
    assert_eq!(
        MessageId::decode(&b64(&[KIND_LEASE_V2]), &project(), &key(), &[1; 16]),
        Err("wrong_token_kind")
    );
    assert_eq!(
        LeaseToken::decode(&b64(&[KIND_MSG_V2]), &project(), &key(), &[1; 16]),
        Err("wrong_token_kind")
    );
    assert_eq!(
        KeyCursor::decode("!", &project(), &key(), &[1; 16], &[2; 16]),
        Err("invalid_cursor")
    );
    assert_eq!(
        ScanCursor::decode("!", &project(), &key(), &[1; 16], 0),
        Err("invalid_cursor")
    );
    assert_eq!(
        MessageId::decode("!", &project(), &key(), &[1; 16]),
        Err("invalid_message_id")
    );
    assert_eq!(
        LeaseToken::decode("!", &project(), &key(), &[1; 16]),
        Err("invalid_lease_token")
    );
}

#[test]
fn every_authenticator_byte_is_required() {
    let cursor = ScanCursor {
        epoch: [1; 16],
        map_version: 3,
        segments: vec![(7, 11)],
        current_index: 0,
        current_offset: 5,
        expires_at_ms: 13,
    };
    let original = unb64(&cursor.encode(&project(), &key())).unwrap();
    for index in 0..original.len() {
        let mut corrupted = original.clone();
        *corrupted.get_mut(index).unwrap() ^= 1;
        assert!(ScanCursor::decode(&b64(&corrupted), &project(), &key(), &[1; 16], 0).is_err());
    }
    let token = cursor.encode(&project(), &key());
    let stranger = ProjectId::new("stranger").unwrap();
    assert_eq!(
        ScanCursor::decode(&token, &stranger, &key(), &[1; 16], 0),
        Err("invalid_cursor")
    );
    assert_eq!(
        ScanCursor::decode(&token, &project(), &key(), &[2; 16], 99),
        Err("invalid_cursor"),
        "identity errors precede expiry"
    );
}

#[test]
fn paired_mac_corruption_and_resigned_extra_fields_are_rejected() {
    let cursor = KeyCursor {
        epoch: [1; 16],
        key_hash: [2; 16],
        seg_id: 3,
        offset: 5,
    };
    let mut raw = unb64(&cursor.encode(&project(), &key())).unwrap();
    let (_, tag) = raw.split_last_chunk_mut::<16>().unwrap();
    for byte in tag.iter_mut().take(2) {
        *byte ^= 1;
    }
    assert_eq!(
        KeyCursor::decode(&b64(&raw), &project(), &key(), &[1; 16], &[2; 16]),
        Err("invalid_cursor")
    );
    let message = MessageId {
        epoch: cursor.epoch,
        key_hash: cursor.key_hash,
        seg_id: cursor.seg_id,
        offset: cursor.offset,
    };
    let lease = LeaseToken {
        msg: message.clone(),
        lease_gen: 7,
        consumer_gen: 11,
        deadline_ms: 13,
    };
    for token in [
        cursor.encode(&project(), &key()),
        message.encode(&project(), &key()),
        lease.encode(&project(), &key()),
    ] {
        let raw = unb64(&token).unwrap();
        let (payload, _) = raw.split_last_chunk::<16>().unwrap();
        let mut extended = payload.to_vec();
        extended.push(0);
        let token = sign(extended);
        assert!(KeyCursor::decode(&token, &project(), &key(), &[1; 16], &[2; 16]).is_err());
        assert!(MessageId::decode(&token, &project(), &key(), &[1; 16]).is_err());
        assert!(LeaseToken::decode(&token, &project(), &key(), &[1; 16]).is_err());
    }
}

#[test]
fn catalog_rejects_missing_fields_wrong_kind_and_non_utf8() {
    for raw in [
        vec![],
        vec![KIND_CATALOG_V1],
        vec![KIND_CATALOG_V1, 1],
        vec![KIND_CATALOG_V1, 1, 0],
        vec![KIND_CATALOG_V1, 1, 0, 255],
        vec![KIND_KEY_V2, 0, 0],
    ] {
        assert!(CatalogCursor::decode(&b64(&raw), &project(), None).is_none());
        assert!(CatalogCursor::decode(&b64(&raw), &project(), Some(&[7; 32])).is_none());
    }
    let cursor = CatalogCursor {
        project: project(),
        last_name: "ok".into(),
    };
    let mut raw = unb64(&cursor.encode(None)).unwrap();
    *raw.last_mut().unwrap() = 255;
    assert!(CatalogCursor::decode(&b64(&raw), &project(), None).is_none());
    assert!(CatalogCursor::decode("!", &project(), None).is_none());
}

proptest::proptest! {
    #[test]
    fn quality_product_cursor_fields_roundtrip_without_truncation(
        seg in proptest::num::u32::ANY,
        offset in proptest::num::u64::ANY,
        lease_gen in proptest::num::u32::ANY,
        generation in proptest::num::u64::ANY,
        deadline in proptest::num::i64::ANY,
    ) {
        let message = MessageId { epoch: [1; 16], key_hash: [2; 16], seg_id: seg, offset };
        let lease = LeaseToken { msg: message.clone(), lease_gen, consumer_gen: generation, deadline_ms: deadline };
        prop_assert_eq!(MessageId::decode(&message.encode(&project(), &key()), &project(), &key(), &[1; 16]).unwrap(), message);
        let token = lease.encode(&project(), &key());
        prop_assert_eq!(LeaseToken::decode(&token, &project(), &key(), &[1; 16]).unwrap(), lease);
        prop_assert!(LeaseToken::decode(&token, &ProjectId::new("other").unwrap(), &key(), &[1; 16]).is_err());
        let scan = ScanCursor { epoch: [1; 16], map_version: generation, segments: vec![(seg, offset)], current_index: seg, current_offset: offset, expires_at_ms: deadline };
        prop_assert_eq!(ScanCursor::decode(&scan.encode(&project(), &key()), &project(), &key(), &[1; 16], deadline).unwrap(), scan);
    }
}

#[test]
fn encoded_size_boundary_preserves_kind_error_priority() {
    let mut raw = vec![0; 16_386];
    *raw.first_mut().unwrap() = KIND_KEY_V2;
    let token = b64(&raw);
    assert_eq!(token.len(), 21_848);
    assert_eq!(
        ScanCursor::decode(&token, &project(), &key(), &[1; 16], 0),
        Err("wrong_cursor_kind")
    );
    assert!(
        unb64(&"A".repeat(21_849)).is_none(),
        "one trailing base64 sextet cannot encode a byte"
    );
    raw.push(0);
    let token = b64(&raw);
    assert_eq!(token.len(), 21_850);
    assert_eq!(
        ScanCursor::decode(&token, &project(), &key(), &[1; 16], 0),
        Err("invalid_cursor")
    );
}
