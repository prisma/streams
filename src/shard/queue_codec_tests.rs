use super::*;
use crate::queue::{self, QueueOp};

#[expect(
    clippy::let_underscore_must_use,
    reason = "r08_corrupt_queue_rows_refuse_without_replacing_persisted_state; the fixture closes the database on the way out; a failed close leaves nothing the assertions depend on"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn r08_corrupt_queue_rows_refuse_without_replacing_persisted_state() {
    for case in 0..6 {
        let store: Arc<dyn object_store::ObjectStore> =
            Arc::new(object_store::memory::InMemory::new());
        let db = Arc::new(
            Db::builder(format!("r08-queue-{case}"), store.clone())
                .build()
                .await
                .unwrap(),
        );
        let (tx, _rx) = mpsc::channel(1);
        let engine = ShardEngine::start(
            format!("r08-queue-{case}"),
            db.clone(),
            store,
            ShardConfig::default(),
            tx,
            None,
            Default::default(),
        );
        let hash = [72; 16];
        let (key, value) = match case {
            0 => (queue::config_key(&hash, "c"), b"{".to_vec()),
            1 => (queue::cursor_key(&hash, "c", 1), vec![1]),
            2 => (queue::lease_key(&hash, "c", 1, 0), vec![0; 17]),
            3 | 4 => (queue::fence_key(&hash, "c"), vec![1]),
            _ => (queue::state_prefix(&hash, b'x', "c"), vec![]),
        };
        let mut wb = WriteBatch::new();
        wb.put(&key[..], &value[..]);
        db.write(wb).await.unwrap().await_durable().await.unwrap();
        let op = match case {
            3 => QueueOp::Receive {
                consumer: "c".into(),
                cgen: 1,
                max: 1,
                visibility_ms: 10,
                max_deliveries: 2,
                keys: HashMap::new(),
                covered_to: 0,
            },
            4 => QueueOp::ConfigDeleteStep {
                consumer: "c".into(),
                fence_below: 2,
                max_rows: 10,
                max_bytes: 1000,
            },
            _ => QueueOp::ConfigPut {
                consumer: "c".into(),
                cfg: Default::default(),
            },
        };
        let (resp, received) = oneshot::channel();
        engine
            .commit_group(
                vec![CommitOp::Queue { hash, op, resp }],
                &ShardConfig::default(),
            )
            .await;
        assert!(
            tokio::time::timeout(std::time::Duration::from_secs(2), received)
                .await
                .unwrap()
                .unwrap()
                .is_err(),
            "case {case}"
        );
        assert_eq!(
            db.get(&key[..]).await.unwrap().unwrap().as_ref(),
            value.as_slice(),
            "case {case}"
        );
        if case != 0 {
            assert!(
                db.get(queue::config_key(&hash, "c"))
                    .await
                    .unwrap()
                    .is_none()
            );
        }
        if matches!(case, 1 | 2 | 5) {
            assert!(
                !engine
                    .stream_handle(hash)
                    .await
                    .unwrap()
                    .state
                    .lock()
                    .unwrap()
                    .queue
                    .loaded
            );
        }
        engine.begin_close();
        let _ = db.close().await;
    }
}

#[test]
fn r08_queue_codecs_reject_partial_extensions_and_keep_key_bytes() {
    let hash = [3; 16];
    let key = queue::lease_key(&hash, "a", 0x0102, 0x0304);
    let mut golden = vec![3; 16];
    golden.extend_from_slice(b"la\0\0\0\0\0\0\0\x01\x02\0\0\0\0\0\0\x03\x04");
    assert_eq!(key, golden);
    assert_eq!(
        queue::decode_state_key(&hash, b'l', &key).unwrap(),
        ("a", 0x0102, Some(0x0304))
    );
    for width in 0..40 {
        assert_eq!(
            queue::decode_lease(&vec![0; width]).is_some(),
            matches!(width, 16 | 32)
        );
    }
    for width in 0..12 {
        assert_eq!(queue::decode_counter(&vec![0; width]).is_ok(), width == 8);
    }
    for width in 0..key.len() {
        assert!(queue::decode_state_key(&hash, b'l', &key[..width]).is_err());
    }
    let mut extended = key.clone();
    extended.push(0);
    assert!(queue::decode_state_key(&hash, b'l', &extended).is_err());
    let mut invalid_name = key.clone();
    invalid_name[17] = 255;
    assert!(queue::decode_state_key(&hash, b'l', &invalid_name).is_err());
    assert!(queue::decode_state_key(&[4; 16], b'l', &key).is_err());
}
