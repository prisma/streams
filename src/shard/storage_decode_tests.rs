//! R12: corrupt storage refuses to open without overwriting records.
#![cfg(test)]
use super::*;

#[test]
fn r12_supported_tail_versions_and_extensions() {
    let tail = TailFields {
        next: 9,
        absorbed: 4,
        trimmed: 2,
        trim_safe_to: 3,
        seq: Some("lane".into()),
        ..Default::default()
    };
    let full = encode_tail(&tail);
    let base = 44 + 4;
    for extension in [0, 16, 24, 32] {
        let decoded = stored_tail(&full[..base + extension]).unwrap();
        assert_eq!(decoded.next, 9);
        assert_eq!(decoded.seq.as_deref(), Some("lane"));
    }
    let mut v2 = full.clone();
    v2[0] = 2;
    v2.remove(41); // v2 has no flags
    assert_eq!(stored_tail(&v2[..43 + 4]).unwrap().next, 9);
    for len in 0..full.len() {
        if ![base, base + 16, base + 24].contains(&len) {
            assert!(stored_tail(&full[..len]).is_err(), "len={len}");
        }
    }
    let mut invalid = full.clone();
    invalid[0] = 99;
    assert!(stored_tail(&invalid).is_err());
    let mut invalid = full.clone();
    invalid[41] = 4;
    assert!(stored_tail(&invalid).is_err());
    let mut invalid = full;
    invalid[25..33].copy_from_slice(&10u64.to_le_bytes());
    assert!(stored_tail(&invalid).is_err());
}

#[test]
fn r12_cursor_requires_exact_width() {
    for len in 0..=9 {
        let result = decode_cursor(&vec![0; len]);
        assert_eq!(result.is_ok(), len == 8, "len={len}");
    }
    assert_eq!(decode_cursor(&123u64.to_le_bytes()).unwrap(), 123);
}

#[test]
fn r12_byte_compatibility_does_not_bypass_stored_state_validation() {
    let tail = TailFields {
        next: 9,
        absorbed: 4,
        trimmed: 2,
        trim_safe_to: 3,
        ..Default::default()
    };
    for version in [2, 3] {
        let mut encoded = encode_tail(&tail);
        if version == 2 {
            encoded[0] = 2;
            encoded.remove(41);
        }
        encoded.extend_from_slice(&[0xee; 7]);
        assert_eq!(stored_tail(&encoded).unwrap().next, tail.next);
        for (at, value) in [(25, 10u64), (33, 5), (encoded.len() - 23, 5)] {
            let mut inconsistent = encoded.clone();
            inconsistent[at..at + 8].copy_from_slice(&value.to_le_bytes());
            assert!(decode_tail(&inconsistent).is_some(), "byte layout is valid");
            assert!(stored_tail(&inconsistent).is_err(), "state is invalid");
        }
    }
}

#[expect(
    clippy::let_underscore_must_use,
    reason = "r12_corrupt_tail_refuses_open_without_overwriting_records; the fixture ignores a delivery or join result whose only failure is the shutdown it stages itself; treating it as fallible would add branches the pinned sequence never takes"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn r12_corrupt_tail_refuses_open_without_overwriting_records() {
    let store: Arc<dyn object_store::ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let db = Arc::new(Db::builder("r12", store.clone()).build().await.unwrap());
    let hash = [12; 16];
    let mut wb = WriteBatch::new();
    wb.put(record_key(&hash, 0), b"retained ciphertext");
    wb.put(tail_key(&hash), b"broken tail");
    db.write(wb).await.unwrap().await_durable().await.unwrap();
    let (tx, _rx) = mpsc::channel(1);
    let engine = ShardEngine::start(
        "r12".into(),
        db.clone(),
        store,
        ShardConfig::default(),
        tx,
        None,
        ShardMaintenance::default(),
    );
    assert!(engine.stream_handle(hash).await.is_err());
    assert!(engine.tail_fields(&hash).await.is_err());
    assert!(engine.durable_absorbed(&hash).await.is_err());
    assert!(engine.seed_fork_tail(hash, [1; 16], 0).await.is_err());
    assert_eq!(
        db.get(record_key(&hash, 0))
            .await
            .unwrap()
            .unwrap()
            .as_ref(),
        b"retained ciphertext"
    );
    assert_eq!(
        db.get(tail_key(&hash)).await.unwrap().unwrap().as_ref(),
        b"broken tail"
    );
    assert!(!engine.streams.lock().unwrap().contains_key(&hash));
    engine.begin_close();
    let _ = db.close().await;
}

/// Hand-built producer row, independent of the production encoder.
fn stored_producer_row(epoch: u64, seq: u64, offset: u64, hash: [u8; 16]) -> Vec<u8> {
    let mut row = Vec::with_capacity(40);
    for field in [epoch, seq, offset] {
        row.extend_from_slice(&field.to_le_bytes());
    }
    row.extend_from_slice(&hash);
    row
}

/// An engine over a fresh store, for loading lane rows planted directly.
async fn lane_engine(name: &str) -> (Arc<ShardEngine>, Arc<Db>) {
    let store: Arc<dyn object_store::ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let db = Arc::new(Db::builder(name, store.clone()).build().await.unwrap());
    let (tx, _rx) = mpsc::channel(1);
    let engine = ShardEngine::start(
        name.into(),
        db.clone(),
        store,
        ShardConfig::default(),
        tx,
        None,
        ShardMaintenance::default(),
    );
    (engine, db)
}

/// Review item 55: a producer row that exists but does not decode is
/// corruption. Read as missing it reopened the lane at producer seq 0, or
/// served an older predecessor's state in its place.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn r12_undecodable_producer_rows_are_corruption_not_absence() {
    let (engine, db) = lane_engine("r12-producer-rows").await;
    let (own, parent, key) = ([12; 16], [13; 16], [14; 16]);
    let mut wb = WriteBatch::new();
    wb.put(
        producer_key(&parent, &key, "p"),
        stored_producer_row(7, 3, 11, [9; 16]),
    );
    db.write(wb).await.unwrap();
    let producer = engine.load_producer_chain(&own, &[parent], &key, "p").await;
    assert_eq!(
        producer.unwrap(),
        Some((7, 3, 11, [9; 16])),
        "a missing own row defers to its predecessor"
    );
    let alone = engine.load_producer_chain(&own, &[], &key, "p").await;
    assert_eq!(
        alone.unwrap(),
        None,
        "no row anywhere is a lane that never committed"
    );
    let mut accepted = Vec::new();
    for width in [15, 17, 23, 25, 39, 41] {
        let mut wb = WriteBatch::new();
        wb.put(producer_key(&own, &key, "p"), vec![1u8; width]);
        db.write(wb).await.unwrap();
        let loaded = engine.load_producer_chain(&own, &[parent], &key, "p").await;
        if loaded.is_ok() {
            accepted.push(width);
        }
    }
    assert!(
        accepted.is_empty(),
        "undecodable producer rows were accepted at widths {accepted:?}"
    );
    engine.begin_close();
    engine
        .await_terminated(std::time::Duration::from_secs(5))
        .await
        .unwrap();
}

/// A Stream-Seq row that is not UTF-8 is corruption, never an unset
/// sequence that would accept any value for the routing key.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn r12_non_utf8_stream_seq_row_is_corruption_not_absence() {
    let (engine, db) = lane_engine("r12-seq-rows").await;
    let (own, parent, key) = ([12; 16], [13; 16], [14; 16]);
    let mut wb = WriteBatch::new();
    wb.put(seq_key(&parent, &key), b"parent-seq");
    db.write(wb).await.unwrap();
    let seq = engine.load_seq_chain(&own, &[parent], &key).await.unwrap();
    assert_eq!(
        seq.as_deref(),
        Some("parent-seq"),
        "a missing own row defers to its predecessor"
    );
    assert_eq!(engine.load_seq_chain(&own, &[], &key).await.unwrap(), None);
    let mut wb = WriteBatch::new();
    wb.put(seq_key(&own, &key), [0xffu8]);
    db.write(wb).await.unwrap();
    let loaded = engine.load_seq_chain(&own, &[parent], &key).await;
    assert!(
        loaded.is_err(),
        "a non-UTF-8 Stream-Seq row must fail closed, got {loaded:?}"
    );
    engine.begin_close();
    engine
        .await_terminated(std::time::Duration::from_secs(5))
        .await
        .unwrap();
}

/// Review item 55: the decoder accepts exactly the widths a commit ever
/// wrote (16 and 24 legacy, 40 current) and refuses every other length.
#[test]
fn r12_producer_row_requires_exact_supported_width() {
    let mut row = stored_producer_row(7, 3, 11, [9; 16]);
    assert_eq!(
        encode_producer_row((7, 3, 11, [9; 16])),
        row,
        "new commits write the full width"
    );
    row.push(0);
    for len in 0..=row.len() {
        let expected = match len {
            16 => Some((7, 3, u64::MAX, [0; 16])),
            24 => Some((7, 3, 11, [0; 16])),
            40 => Some((7, 3, 11, [9; 16])),
            _ => None,
        };
        assert_eq!(decode_producer_row(&row[..len]).ok(), expected, "len={len}");
    }
}

#[test]
fn r12_stream_seq_row_requires_utf8() {
    assert_eq!(decode_seq_row(b"alpha").unwrap(), "alpha");
    assert_eq!(decode_seq_row(b"").unwrap(), "");
    for raw in [&[0xffu8][..], &[b'a', 0xc3][..], &[0xed, 0xa0, 0x80][..]] {
        assert!(decode_seq_row(raw).is_err(), "raw={raw:?}");
    }
}

proptest::proptest! {
    #![proptest_config(proptest::prelude::ProptestConfig { cases: 1024, ..proptest::prelude::ProptestConfig::default() })]

    /// Every committed producer state reads back exactly; arbitrary stored
    /// bytes decode only at a supported width, into the fields a hand-built
    /// row carries there.
    #[test]
    fn quality_producer_rows_decode_only_at_a_supported_width(
        epoch in proptest::num::u64::ANY,
        seq in proptest::num::u64::ANY,
        offset in proptest::num::u64::ANY,
        hash in proptest::array::uniform16(proptest::num::u8::ANY),
        raw in proptest::collection::vec(proptest::num::u8::ANY, 0..=48),
    ) {
        let row = (epoch, seq, offset, hash);
        proptest::prop_assert_eq!(decode_producer_row(&encode_producer_row(row)).ok(), Some(row));
        let field = |at: usize| u64::from_le_bytes(raw[at..at + 8].try_into().unwrap());
        let expected = match raw.len() {
            16 => Some((field(0), field(8), u64::MAX, [0; 16])),
            24 => Some((field(0), field(8), field(16), [0; 16])),
            40 => Some((field(0), field(8), field(16), raw[24..].try_into().unwrap())),
            _ => None,
        };
        proptest::prop_assert_eq!(decode_producer_row(&raw).ok(), expected);
    }

    /// Stream-Seq rows decode exactly the text they hold and nothing else.
    #[test]
    fn quality_stream_seq_rows_decode_exactly_their_utf8(
        text in "\\PC{0,24}",
        raw in proptest::collection::vec(proptest::num::u8::ANY, 0..=16),
    ) {
        let expected = Some(text.clone());
        proptest::prop_assert_eq!(decode_seq_row(text.as_bytes()).ok(), expected);
        proptest::prop_assert_eq!(decode_seq_row(&raw).ok(), std::str::from_utf8(&raw).ok().map(str::to_owned));
    }
}

/// Review item 53: the tail row copies the lane's Stream-Seq only while its
/// length fits the row's u16; a longer one is not copied, and the row still
/// decodes every field (its lane row holds the sequence).
#[test]
fn r53_the_tail_copies_a_stream_seq_only_while_its_length_fits() {
    let tail = |seq: String| TailFields {
        next: 9,
        absorbed: 4,
        trimmed: 2,
        trim_safe_to: 3,
        route: [0xA5; 16],
        unabsorbed_bytes: 77,
        seq: Some(seq),
        ..Default::default()
    };
    let fits = "s".repeat(usize::from(u16::MAX));
    let decoded = stored_tail(&encode_tail(&tail(fits.clone()))).unwrap();
    assert_eq!(decoded.seq.as_deref(), Some(fits.as_str()));
    let past = stored_tail(&encode_tail(&tail("s".repeat(usize::from(u16::MAX) + 1)))).unwrap();
    assert_eq!(past.seq, None, "a copy past the u16 length is not written");
    assert_eq!(
        (
            past.next,
            past.absorbed,
            past.trim_safe_to,
            past.route,
            past.unabsorbed_bytes
        ),
        (9, 4, 3, [0xA5; 16], 77),
        "every field decodes where it was written"
    );
}
