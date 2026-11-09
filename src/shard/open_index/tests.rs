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

/// **A log at an operation-born home's path takes the home its path names** (S4 route A): a new log
/// at `shards/10-7` records home 7 and reopens; a log there that records another home is refused;
/// a path of neither form names no home and is refused.
#[tokio::test(flavor = "multi_thread", worker_threads = 2, crate = "crate::test_rt")]
async fn a_log_at_an_operation_born_home_takes_the_home_its_path_names() {
    let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let db = open(&store, "shards/10-7").await.unwrap();
    assert_eq!(super::recorded_home(&db).await.unwrap(), Some(7));
    db.close().await.unwrap();
    open(&store, "shards/10-7")
        .await
        .unwrap()
        .close()
        .await
        .unwrap();
    let other: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    seed(&other, "shards/10-7", &[identity(8, 2)]).await;
    let refusal = open(&other, "shards/10-7").await.err().unwrap();
    assert!(
        refusal
            .to_string()
            .contains("this binary's 0000000800000007"),
        "{refusal}"
    );
    for path in [
        "shards/10-x",
        "shards/1a-3",
        "shards/-3",
        "shards/10-",
        "other/10-3",
        "shards/10~3",
    ] {
        let refusal = open(&store, path)
            .await
            .err()
            .unwrap_or_else(|| panic!("{path}: opened"));
        assert!(
            refusal.to_string().contains("names no shard prefix"),
            "{path}: {refusal}"
        );
    }
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

/// The index triggers (S3a §4.1, KANI-107): `'W'` from the base and staged tails alone, over the
/// four combinations; `'M'` cleared only by an acknowledgement covering the durable version.
#[test]
fn the_index_triggers_follow_base_and_staged_tails() {
    use super::IndexOp::{Delete, Put};
    use super::{dirty_cleared, unsealed_op};
    assert_eq!(unsealed_op((3, 3), (3, 4)), Some(Put), "gains a page");
    assert_eq!(unsealed_op((3, 4), (4, 4)), Some(Delete), "loses its last");
    assert_eq!(unsealed_op((3, 4), (3, 5)), None, "stays unsealed");
    assert_eq!(unsealed_op((3, 3), (5, 5)), None, "stays sealed");
    assert!(!dirty_cleared(7, Some(8)));
    assert!(dirty_cleared(8, Some(8)) && dirty_cleared(9, Some(8)) && dirty_cleared(0, None));
}

/// The rows of a seeded log for the repair tests: `unsealed` tails holding `bytes` unsealed bytes,
/// a billed tail at `version`, and the index rows given.
async fn seeded(path: &str, rows: &[(Vec<u8>, Vec<u8>)]) -> slatedb::Db {
    let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    seed(&store, path, rows).await;
    crate::keyspace::db_builder(path, store)
        .build()
        .await
        .unwrap()
}

fn tail(absorbed: u64, next: u64, bytes: u64, version: Option<u64>) -> Vec<u8> {
    crate::shard::encode_tail(&crate::shard::TailFields {
        absorbed,
        next,
        unabsorbed_bytes: bytes,
        usage: version.map(|v| crate::billing::stored_meta::UsageCounters {
            usage_version: v,
            ..Default::default()
        }),
        ..Default::default()
    })
}

/// An open of a log carrying the repair marker recomputes both indexes from every tail (S3a §4.1):
/// the unsealed stream gains its 'W' row, the billed one its 'M' row at its version, a dirty row
/// with no tail goes, and so does the marker. Without the marker the same log's open reads only
/// the index: the missing rows stay missing and the ledger has no bytes.
#[tokio::test(flavor = "multi_thread", worker_threads = 2, crate = "crate::test_rt")]
async fn an_open_repairs_the_indexes_the_marker_asks_for() {
    let (unsealed, billed, gone) = ([1; 16], [2; 16], [3; 16]);
    let mut rows = vec![
        (crate::shard::tail_key(&unsealed), tail(4, 10, 300, None)),
        (crate::shard::tail_key(&billed), tail(5, 5, 0, Some(5))),
        (super::dirty_key(&gone), 9u64.to_be_bytes().to_vec()),
    ];
    let plain = seeded("repair-plain", &rows).await;
    let ledger = super::load_or_rebuild_maintenance(&plain).await.unwrap();
    assert_eq!(
        ledger.unabsorbed_frame_bytes, 0,
        "an ordinary open scans no tail"
    );
    assert!(super::unsealed_members(&plain).await.unwrap().is_empty());
    rows.push((super::repair_key(), Vec::new()));
    let marked = seeded("repair-marked", &rows).await;
    let ledger = super::load_or_rebuild_maintenance(&marked).await.unwrap();
    assert_eq!(ledger.unabsorbed_frame_bytes, 300);
    assert_eq!(super::unsealed_members(&marked).await.unwrap(), [unsealed]);
    assert_eq!(super::dirty_members(&marked).await.unwrap(), [(billed, 5)]);
    assert_eq!(
        marked.get(&super::repair_key()).await.unwrap(),
        None,
        "the marker goes"
    );
}

/// An open that finds an unsealed-index member whose tail is sealed or gone repairs both
/// indexes before it serves: the stale rows go and the unindexed unsealed stream gains its row.
#[tokio::test(flavor = "multi_thread", worker_threads = 2, crate = "crate::test_rt")]
async fn an_open_repairs_a_stale_unsealed_member() {
    let (sealed, gone, missing) = ([1; 16], [2; 16], [3; 16]);
    let rows = [
        (crate::shard::tail_key(&sealed), tail(10, 10, 0, None)),
        (super::unsealed_key(&sealed), Vec::new()),
        (super::unsealed_key(&gone), Vec::new()),
        (crate::shard::tail_key(&missing), tail(0, 3, 70, None)),
    ];
    let db = seeded("repair-stale", &rows).await;
    let ledger = super::load_or_rebuild_maintenance(&db).await.unwrap();
    assert_eq!(super::unsealed_members(&db).await.unwrap(), [missing]);
    assert_eq!(ledger.unabsorbed_frame_bytes, 70);
}

/// The open indexes' keys and the repair marker's, byte for byte (S3a §2.4): the sentinel, the
/// tag, then the stream's hash ('W', 'M') or nothing ('X').
#[test]
fn the_index_and_marker_keys_are_pinned() {
    let hash = [0x07; 16];
    let sentinel = [0xFF; 16];
    let key = |tag: &[u8], rest: &[u8]| [sentinel.as_slice(), tag, rest].concat();
    assert_eq!(super::unsealed_key(&hash), key(b"W", &hash));
    assert_eq!(super::dirty_key(&hash), key(b"M", &hash));
    assert_eq!(super::repair_key(), key(b"X", &[]));
}

/// A repair reports exactly what it changed, per index and direction (its `warn` line is what an
/// operator reads after calling the route): two streams gain 'W', three billed tails gain 'M', a
/// 'W' row and two 'M' rows that name no tail go, and rows already right are left alone.
#[tokio::test(flavor = "multi_thread", worker_threads = 2, crate = "crate::test_rt")]
async fn a_repair_counts_each_row_it_adds_and_removes() {
    let mut rows = Vec::new();
    for hash in [[1; 16], [2; 16]] {
        rows.push((crate::shard::tail_key(&hash), tail(4, 10, 300, None)));
    }
    for hash in [[3; 16], [4; 16], [5; 16]] {
        rows.push((crate::shard::tail_key(&hash), tail(5, 5, 0, Some(5))));
    }
    rows.push((super::unsealed_key(&[6; 16]), Vec::new()));
    for hash in [[7; 16], [8; 16]] {
        rows.push((super::dirty_key(&hash), 9u64.to_be_bytes().to_vec()));
    }
    let settled = [9; 16];
    rows.push((crate::shard::tail_key(&settled), tail(4, 10, 300, Some(2))));
    rows.push((super::unsealed_key(&settled), Vec::new()));
    rows.push((super::dirty_key(&settled), 2u64.to_be_bytes().to_vec()));
    let db = seeded("repair-counts", &rows).await;
    let repair = super::repair_indexes(&db).await.unwrap();
    let expected = super::Repair {
        unsealed_added: 2,
        unsealed_removed: 1,
        dirty_set: 3,
        dirty_removed: 2,
    };
    assert_eq!(repair, expected);
}

/// An open whose one stale unsealed-index member is a sealed tail (absorbed == next, its tail
/// still there) repairs: the row goes, though no member is gone.
#[tokio::test(flavor = "multi_thread", worker_threads = 2, crate = "crate::test_rt")]
async fn an_open_repairs_an_index_row_whose_tail_is_sealed() {
    let sealed = [4; 16];
    let rows = vec![
        (crate::shard::tail_key(&sealed), tail(10, 10, 0, None)),
        (super::unsealed_key(&sealed), Vec::new()),
    ];
    let db = seeded("repair-sealed-only", &rows).await;
    super::load_or_rebuild_maintenance(&db).await.unwrap();
    let members = super::unsealed_members(&db).await.unwrap();
    assert_eq!(
        members,
        Vec::<[u8; 16]>::new(),
        "the sealed member's row goes"
    );
}
