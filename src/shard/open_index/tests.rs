#![cfg(test)]
//! The shard log's identity row (S3a-DESIGN §2.6): written on a new log, checked on every open
//! through the production builder, and a log of another layout or home refused.
use std::sync::Arc;

use object_store::ObjectStore;

/// Opens the log at `path` over `store` through the production builder.
async fn open(store: &Arc<dyn ObjectStore>, path: &str) -> Result<slatedb::Db, slatedb::Error> {
    crate::native_queue::compaction_guard::shard_log_builder(
        path,
        store.clone(),
        Default::default(),
    )
    .build()
    .await
}

/// A log at `path` over `store` holding `rows`, written without the identity check.
async fn seed(store: &Arc<dyn ObjectStore>, path: &str, rows: &[(Vec<u8>, Vec<u8>)]) {
    let db = crate::keyspace::db_builder(path, store.clone())
        .build()
        .await
        .unwrap();
    for (key, value) in rows {
        db.put(key, value).await.unwrap();
    }
    db.close().await.unwrap();
}

fn identity(layout: u32, home: u32) -> (Vec<u8>, Vec<u8>) {
    let value = [layout.to_be_bytes(), home.to_be_bytes()].concat();
    (super::identity_key(), value)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2, crate = "crate::test_rt")]
async fn a_new_log_records_its_layout_and_home_and_reopens() {
    let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let path = crate::sharddir::shard_db_path("01");
    let db = open(&store, &path).await.unwrap();
    let stored = db.get(&super::identity_key()).await.unwrap().unwrap();
    assert_eq!(crate::crypto::hex(&stored), "0000000800000001");
    assert_eq!(
        super::identity_key(),
        [[0xFF; 16].as_slice(), b"Y"].concat()
    );
    db.close().await.unwrap();
    let again = open(&store, &path).await.unwrap();
    assert_eq!(
        again.get(&super::identity_key()).await.unwrap(),
        Some(stored)
    );
    again.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2, crate = "crate::test_rt")]
async fn an_open_refuses_a_log_of_another_layout_or_home() {
    let tail = (
        crate::keyspace::stream_row(&[0x11; 16], 0),
        b"layout 7".to_vec(),
    );
    let cases = [
        ("a layout 7 log: rows and no identity", vec![tail]),
        ("a layout 7 identity", vec![identity(7, 2)]),
        ("another home's identity", vec![identity(8, 3)]),
    ];
    for (case, rows) in cases {
        let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let path = crate::sharddir::shard_db_path("10");
        seed(&store, &path, &rows).await;
        let refusal = open(&store, &path)
            .await
            .err()
            .unwrap_or_else(|| panic!("{case}: opened"));
        assert_eq!(refusal.kind(), slatedb::ErrorKind::Data, "{case}");
        let message = refusal.to_string();
        assert!(
            message.contains("another layout or home"),
            "{case}: {message}"
        );
        assert!(
            message.contains("this binary's 0000000800000002"),
            "{case}: {message}"
        );
        // The refusal wrote nothing: the log holds exactly what it held.
        let db = crate::keyspace::db_builder(path.as_str(), store.clone())
            .build()
            .await
            .unwrap();
        let mut held = Vec::new();
        let mut scan = db.scan(..).await.unwrap();
        while let Some(row) = scan.next().await.unwrap() {
            held.push((row.key.to_vec(), row.value.to_vec()));
        }
        assert_eq!(held, rows, "{case}");
        db.close().await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2, crate = "crate::test_rt")]
async fn a_log_whose_path_names_no_shard_is_refused() {
    let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let refusal = open(&store, "shards/a-label").await.err().unwrap();
    assert!(
        refusal.to_string().contains("names no shard prefix"),
        "{refusal}"
    );
}

#[test]
fn a_shards_home_is_its_prefix_read_as_a_number() {
    use crate::keyspace::home_of;
    let homes: Vec<_> = ["", "0", "1", "00", "01", "10", "11"].map(home_of).into();
    assert_eq!(homes, [0, 0, 1, 0, 1, 2, 3].map(Some));
    assert_eq!(home_of(&"1".repeat(24)), Some(0xFF_FFFF));
    for refused in ["02", "a-label", "+1", &"0".repeat(25)] {
        assert_eq!(home_of(refused), None, "{refused}");
    }
    for prefix in ["", "0", "011", "t/explicit"] {
        let path = crate::sharddir::shard_db_path(prefix);
        assert_eq!(crate::sharddir::prefix_of_db_path(&path), prefix);
    }
}

/// An engine reads its home from its log's `'Y'` row and names every object it claims with it,
/// under its own path; another home's root is that home's prefix of the shard's width.
#[tokio::test(flavor = "multi_thread", worker_threads = 2, crate = "crate::test_rt")]
async fn an_engine_names_its_objects_with_its_logs_home() {
    let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let db = open(&store, &crate::sharddir::shard_db_path("10"))
        .await
        .unwrap();
    let engine = crate::shard::ShardEngine::start(
        "10".to_string(),
        Arc::new(db),
        store,
        crate::shard::ShardConfig::default(),
        None,
        Default::default(),
    );
    engine.seal_ready().await.unwrap();
    assert_eq!(engine.home(), 2);
    let id = engine.claim_object().id;
    assert_eq!((id.home, id.epoch), (2, engine.writer_epoch));
    let path = engine.object_path_of(crate::spill::gc::ObjectKind::Segment, id);
    assert!(path.starts_with("shards/10/hseg/"), "{path}");
    assert_eq!(engine.object_root(3), "shards/11");
    engine.begin_close();
}
