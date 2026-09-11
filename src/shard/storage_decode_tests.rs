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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[expect(
    clippy::let_underscore_must_use,
    reason = "r12_corrupt_tail_refuses_open_without_overwriting_records; the fixture ignores a delivery or join result whose only failure is the shutdown it stages itself; treating it as fallible would add branches the pinned sequence never takes"
)]
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
