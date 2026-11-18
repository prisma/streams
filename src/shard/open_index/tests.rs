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

/// **A log's identity row is the sentinel's `'Y'` key holding this binary's layout and the home**,
/// as a split seeds each child with it (`split::seed_rows`) and a merge its merged leaf: the
/// layout version then the home, each four bytes big-endian.
#[test]
fn the_identity_row_is_the_sentinels_y_holding_the_layout_and_the_home() {
    let (key, value) = super::identity_row(0x0102_0304);
    assert_eq!(key, [[0xFF; 16].as_slice(), b"Y"].concat());
    let layout = crate::registry::LAYOUT_VERSION.to_be_bytes();
    assert_eq!(value, [layout.as_slice(), &[1, 2, 3, 4]].concat());
    assert_eq!(super::identity_row(7).1[4..], [0, 0, 0, 7]);
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

/// **A log at an operation-born home's path opens only with its own identity row** (S4 route A;
/// addendum 6's second ruling): its clone seeds the `'Y'` row, so a log there that has none is
/// missing. Over an empty store, the open of `shards/10-7` is refused `log_missing` and leaves no
/// `'Y'` behind, again on a retry; a log there seeded with home 7's row opens and records home 7; a
/// log there that records another home is refused; a path of neither form names no home and is
/// refused.
#[tokio::test(flavor = "multi_thread", worker_threads = 2, crate = "crate::test_rt")]
async fn a_log_at_an_operation_born_home_opens_only_with_its_identity_row() {
    let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    for attempt in ["first", "retry"] {
        let refusal = open(&store, "shards/10-7").await.err().unwrap();
        assert_eq!(
            refusal.to_string(),
            "Data error: log_missing: shard log shards/10-7 (prefix 10, home 7) has no identity row; \
             an operation-born home's log is created only by its clone (RUNBOOK)",
            "{attempt}"
        );
        let raw = crate::keyspace::db_builder("shards/10-7", store.clone())
            .build()
            .await
            .unwrap();
        assert_eq!(
            raw.get(&super::identity_key()).await.unwrap(),
            None,
            "{attempt}"
        );
        raw.close().await.unwrap();
    }
    let seeded: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let layout = crate::registry::LAYOUT_VERSION;
    seed(&seeded, "shards/10-7", &[identity(layout, 7)]).await;
    let db = open(&seeded, "shards/10-7").await.unwrap();
    assert_eq!(super::recorded_home(&db).await.unwrap(), Some(7));
    db.close().await.unwrap();
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

/// A born home's path names its home only in decimal digits: `shards/<prefix>-<home>` with a sign
/// (`+3`, which `u32::from_str` takes) or any other byte in the home names no home, as an empty
/// home does; a well-formed one names it, the root's included.
#[test]
fn a_born_homes_path_names_its_home_only_in_digits() {
    use crate::sharddir::home_of_db_path;
    assert_eq!(home_of_db_path("shards/10-7"), Some(7));
    assert_eq!(home_of_db_path("shards/root-12"), Some(12));
    for path in ["shards/10-+3", "shards/10-3a", "shards/10-", "shards/10- 3"] {
        assert_eq!(home_of_db_path(path), None, "{path}");
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

/// A queue's tail is its own key (`hash ‖ 'Q' ‖ 't'`, S3b-DESIGN §5.1) and its unsealed pages
/// are named in 'W' like a stream's: an open reads a member's tail by either key, so an indexed
/// queue keeps its row and its bytes in the ledger with no repair, and a repair finds queue tails
/// as it finds stream tails.
#[tokio::test(flavor = "multi_thread", worker_threads = 2, crate = "crate::test_rt")]
async fn an_open_and_a_repair_read_a_queues_tail_as_they_read_a_streams() {
    let (indexed, unindexed) = ([4; 16], [5; 16]);
    let mut rows = vec![
        (crate::shard::queue_tail_key(&indexed), tail(0, 3, 90, None)),
        (super::unsealed_key(&indexed), Vec::new()),
    ];
    let plain = seeded("queue-open", &rows).await;
    let ledger = super::load_or_rebuild_maintenance(&plain).await.unwrap();
    assert_eq!(
        (
            ledger.unabsorbed_frame_bytes,
            super::unsealed_members(&plain).await.unwrap()
        ),
        (90, vec![indexed]),
        "the queue's row stays and its bytes are counted"
    );
    rows.push((
        crate::shard::queue_tail_key(&unindexed),
        tail(2, 6, 40, None),
    ));
    let marked = seeded("queue-repair", &rows).await;
    let repair = super::repair_indexes(&marked).await.unwrap();
    assert_eq!(
        repair,
        super::Repair {
            unsealed_added: 1,
            ..Default::default()
        }
    );
    assert_eq!(
        super::unsealed_members(&marked).await.unwrap(),
        [indexed, unindexed]
    );
    let ledger = super::load_or_rebuild_maintenance(&marked).await.unwrap();
    assert_eq!(ledger.unabsorbed_frame_bytes, 130);
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

/// `open` without the log's compactor: four flushed tables are SlateDB's `min_compaction_sources`,
/// so a compactor would compact them while the test writes or measures (on Linux CI it did, mid
/// load), and the layout under measurement would depend on its timing.
async fn open_uncompacted(store: &Arc<dyn ObjectStore>, path: &str) -> slatedb::Db {
    let settings = slatedb::config::Settings {
        compactor_options: None,
        ..Default::default()
    };
    crate::native_queue::compaction_guard::shard_log_builder(path, store.clone(), settings)
        .build()
        .await
        .unwrap()
}

/// A log at `path` over `store` whose unsealed index was put for `streams` streams, then deleted
/// for all but the first `kept` as their tails sealed (when any were), twice, each batch flushed to
/// its own table:
/// four tables of index versions and tombstones, as a busy shard's prefix holds them until
/// compaction. The kept members' tails each hold 70 unsealed bytes.
async fn churned(store: &Arc<dyn ObjectStore>, path: &str, streams: u32, kept: u32) {
    let db = open_uncompacted(store, path).await;
    let hash = |n: u32| {
        let mut hash = [0u8; 16];
        hash[..4].copy_from_slice(&n.to_be_bytes());
        hash
    };
    for _ in 0..2 {
        let mut put = slatedb::WriteBatch::new();
        for n in 0..streams {
            put.put(super::unsealed_key(&hash(n)), b"");
            put.put(crate::shard::tail_key(&hash(n)), tail(0, 3, 70, None));
        }
        flushed(&db, put).await;
        let mut deleted = slatedb::WriteBatch::new();
        for n in kept..streams {
            deleted.delete(super::unsealed_key(&hash(n)));
            deleted.put(crate::shard::tail_key(&hash(n)), tail(3, 3, 0, None));
        }
        // Every stream kept: nothing sealed, so no batch of deletions (SlateDB refuses an empty one).
        if kept < streams {
            flushed(&db, deleted).await;
        }
    }
    db.close().await.unwrap();
}

/// Writes `batch` to `db` and flushes the memtable to a table.
async fn flushed(db: &slatedb::Db, batch: slatedb::WriteBatch) {
    db.write(batch).await.unwrap();
    let memtable = slatedb::config::FlushOptions {
        flush_type: slatedb::config::FlushType::MemTable,
    };
    db.flush_with_options(memtable).await.unwrap();
}

/// The store GETs of an open's index load of a churned log of `streams` streams at `path`,
/// reopened without its compactor so no block is cached and the four tables stay as written; the
/// load finds the ten kept members' bytes.
async fn index_load_gets(path: &str, streams: u32) -> u64 {
    load_gets(path, streams, 10).await
}

/// `index_load_gets` with `kept` members left: the load finds their bytes.
async fn load_gets(path: &str, streams: u32, kept: u32) -> u64 {
    let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    churned(&store, path, streams, kept).await;
    let db = open_uncompacted(&store, path).await;
    let metrics = crate::spill::metrics::for_log(path);
    // Every GET of the database: whole objects (`db.get`) and blocks read by range
    // (`db.get_range`, `db.get_ranges`).
    let reads = || -> u64 {
        let requests = metrics.requests();
        requests
            .iter()
            .filter(|(kind, _)| kind.starts_with("db.get"))
            .map(|(_, n)| n)
            .sum()
    };
    let before = reads();
    let ledger = super::load_or_rebuild_maintenance(&db).await.unwrap();
    let gets = reads().saturating_sub(before);
    assert_eq!(
        ledger.unabsorbed_frame_bytes,
        u64::from(kept) * 70,
        "the kept members"
    );
    db.close().await.unwrap();
    gets
}

/// **A flushed shard's open reads its indexes in bounded round trips**
/// (GATE-DECISION-open-cost-2026-11-17, fix 1). A log whose unsealed index was put and deleted for
/// 1,000 and then 4,000 streams over four flushed tables, ten members left, reopened cold: the
/// open's index load makes at most 32 store GETs, the same bound at both sizes. Red before fix 1:
/// the index scan fetches one block per GET, so the count grows with the streams.
#[tokio::test(flavor = "multi_thread", worker_threads = 2, crate = "crate::test_rt")]
async fn a_flushed_shards_open_reads_its_indexes_in_bounded_round_trips() {
    let one = index_load_gets("shards/0110110", 1_000).await;
    let four = index_load_gets("shards/0110111", 4_000).await;
    println!("INDEX_GETS 1000={one} 4000={four}");
    assert!(
        one <= 32 && four <= 32,
        "GETs of the open's index load: {one} at 1,000 streams, {four} at 4,000"
    );
}

/// **The sentinel's scans read 1 MiB ahead, sixteen fetches at once, cached, at Memory
/// durability** (GATE-DECISION-open-cost-2026-11-17): the four values pinned, Memory being F1b's
/// condition for the split's reads at one sequence.
#[test]
fn the_sentinel_scan_options_are_pinned() {
    let options = super::sentinel_scan_opts();
    assert_eq!(
        options.durability_filter,
        slatedb::config::DurabilityLevel::Memory
    );
    assert_eq!(options.read_ahead_bytes, 1_048_576);
    assert_eq!(options.max_fetch_tasks, 16);
    assert!(options.cache_blocks);
}

/// **A flushed shard's open reads its members' tails in bounded round trips**
/// (GATE-DECISION-f5-pause-log-reads-2026-11-17, fix 2). Every one of 1,000 and then 4,000
/// streams a member of the unsealed index, over four flushed tables, reopened cold: the open's
/// index load makes at most 64 store requests (`get`, `get_range`, `get_ranges`), the same bound at
/// both sizes, and 32 members among 4,000 streams stay within it too. Red before fix 2: one point
/// get per member's tail, so the count grows with the members.
#[tokio::test(flavor = "multi_thread", worker_threads = 2, crate = "crate::test_rt")]
async fn a_flushed_shards_open_reads_its_members_tails_in_bounded_round_trips() {
    let one = load_gets("shards/0111000", 1_000, 1_000).await;
    let four = load_gets("shards/0111001", 4_000, 4_000).await;
    let sparse = load_gets("shards/0111010", 4_000, 32).await;
    println!("TAIL_GETS 1000={one} 4000={four} sparse={sparse}");
    assert!(
        one <= 64 && four <= 64 && sparse <= 64,
        "requests of the open's index load: {one} at 1,000 members, {four} at 4,000, {sparse} at 32 of 4,000"
    );
}

/// **The ranged tail reads find what the point gets find** (fix 2): on one log of stream
/// members, queue members, a member whose tail is sealed, one whose tail is gone and one whose
/// tail cannot be decoded, among streams that are not members, the scans of `run_tails` and the
/// gets of `member_tail` return the same tail for every member.
#[tokio::test(flavor = "multi_thread", worker_threads = 2, crate = "crate::test_rt")]
async fn the_ranged_tail_reads_find_what_the_point_gets_find() {
    let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let db = open_uncompacted(&store, "shards/0111011").await;
    let hash = |n: u32| {
        let mut hash = [0u8; 16];
        hash[..4].copy_from_slice(&n.to_be_bytes());
        hash
    };
    let mut batch = slatedb::WriteBatch::new();
    for n in 0..200u32 {
        let key = match n % 5 {
            0 => crate::shard::queue_tail_key(&hash(n)),
            _ => crate::shard::tail_key(&hash(n)),
        };
        let value = match n % 7 {
            0 => tail(3, 3, 0, None),
            1 => b"not a tail".to_vec(),
            _ => tail(0, 3, u64::from(n), None),
        };
        if n % 11 != 0 {
            batch.put(key, value);
        }
    }
    flushed(&db, batch).await;
    let members: Vec<[u8; 16]> = (0..200u32).filter(|n| n % 3 != 0).map(hash).collect();
    let gets = super::tails_by_gets(&db, &members, false).await.unwrap();
    let scans = super::run_tails(&db, &members).await.unwrap();
    assert_eq!(scans, gets, "the same tail for every member");
    assert!(
        gets.iter().any(Option::is_none),
        "a member whose tail is gone"
    );
    db.close().await.unwrap();
}

/// The hash `n` stands for in the `scanned` tests: its first four bytes.
fn member(n: u32) -> [u8; 16] {
    let mut hash = [0u8; 16];
    hash[..4].copy_from_slice(&n.to_be_bytes());
    hash
}

/// A key that sorts after `member(n)`'s and before `member(n + 1)`'s: `member(n)` and `i` after it.
fn between(n: u32, i: u32) -> Vec<u8> {
    [member(n).as_slice(), &i.to_be_bytes()].concat()
}

/// `scanned` over a log holding exactly `rows`, each member's row at its hash itself.
async fn scanned_over(
    path: &str,
    rows: &[(Vec<u8>, Vec<u8>)],
    hashes: &[[u8; 16]],
) -> (Vec<Option<bytes::Bytes>>, usize) {
    let db = seeded(path, rows).await;
    let found = super::scanned(&db, hashes, |hash| hash.to_vec())
        .await
        .unwrap();
    db.close().await.unwrap();
    found
}

/// The tails `scanned` returns for `values`, `None` where a member was not read.
fn tails(values: &[Option<&[u8]>]) -> Vec<Option<bytes::Bytes>> {
    values
        .iter()
        .map(|value| value.map(bytes::Bytes::copy_from_slice))
        .collect()
}

/// **A tail scan reads each member's row among absent members and neighbours' rows** (fix 2,
/// #163's survivors): members 1 to 5 with 1 and 4 absent, rows before the first, between members
/// and after the last, and none of them read as a member's. Each member present returns its own
/// row, and the scan reached all five; no members scan nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 2, crate = "crate::test_rt")]
async fn a_tail_scan_reads_each_members_row_among_absent_members_and_neighbours() {
    let hashes: Vec<[u8; 16]> = (1..=5).map(member).collect();
    let rows = vec![
        (between(0, 7), b"before".to_vec()),
        (member(2).to_vec(), b"two".to_vec()),
        (between(2, 1), b"after two".to_vec()),
        (between(3, 0), b"before three's".to_vec()),
        (member(3).to_vec(), b"three".to_vec()),
        (between(3, 1), b"after three".to_vec()),
        (between(4, 1), b"in four's place".to_vec()),
        (member(5).to_vec(), b"five".to_vec()),
        (between(5, 1), b"past the last".to_vec()),
    ];
    let found = scanned_over("shards/0111100", &rows, &hashes).await;
    let expected = tails(&[None, Some(b"two"), Some(b"three"), None, Some(b"five")]);
    assert_eq!(
        found,
        (expected, 5),
        "each member's own row, all five reached"
    );
    let none = scanned_over("shards/0111101", &rows, &[]).await;
    assert_eq!(none, (Vec::new(), 0), "no members");
}

/// The rows of the row-budget tests: member 1's row, `neighbours` rows after it, then member 2's.
fn crowded(neighbours: u32) -> Vec<(Vec<u8>, Vec<u8>)> {
    let mut rows = vec![(member(1).to_vec(), b"one".to_vec())];
    rows.extend((0..neighbours).map(|i| (between(1, i), b"x".to_vec())));
    rows.push((member(2).to_vec(), b"two".to_vec()));
    rows
}

/// **A tail scan stops after sixteen rows per member it reached, plus one** (fix 2, #163's
/// survivors), the budget written as literals so a mutated constant moves only the code. With
/// member 1 reached the budget is 32 rows: 31 neighbours after its row reach member 2, 32 stop the
/// scan at member 1. With member 1 absent and member 2 reached on the 33rd row the budget is 48, so
/// member 3 after it is reached too.
#[tokio::test(flavor = "multi_thread", worker_threads = 2, crate = "crate::test_rt")]
async fn a_tail_scan_stops_after_sixteen_rows_per_member_reached() {
    let pair = [member(1), member(2)];
    let within = scanned_over("shards/0111110", &crowded(31), &pair).await;
    assert_eq!(within, (tails(&[Some(b"one"), Some(b"two")]), 2), "32 rows");
    let over = scanned_over("shards/0111111", &crowded(32), &pair).await;
    assert_eq!(over, (tails(&[Some(b"one"), None]), 1), "33 rows");
    let mut rows: Vec<(Vec<u8>, Vec<u8>)> =
        (0..32).map(|i| (between(1, i), b"x".to_vec())).collect();
    rows.push((member(2).to_vec(), b"two".to_vec()));
    rows.push((member(3).to_vec(), b"three".to_vec()));
    let three = [member(1), member(2), member(3)];
    let reached = scanned_over("shards/1000000", &rows, &three).await;
    let expected = tails(&[None, Some(b"two"), Some(b"three")]);
    assert_eq!(reached, (expected, 3), "member 2 on the 33rd row");
}

/// **A tail scan stops once it passed more than 1 MiB of keys and values** (fix 2, #163's
/// survivors), written as the literal 1,048,576: member 1's row and a neighbour's of exactly
/// 1,048,576 bytes together reach member 2; one byte more stops the scan at member 1.
#[tokio::test(flavor = "multi_thread", worker_threads = 2, crate = "crate::test_rt")]
async fn a_tail_scan_stops_after_one_mib_of_keys_and_values() {
    let pair = [member(1), member(2)];
    let neighbour = between(1, 0);
    // Member 1's 16-byte key and 3-byte value, then the neighbour's 20-byte key and its value.
    let fill = 1_048_576 - 16 - 3 - neighbour.len();
    let rows = |value: usize| {
        vec![
            (member(1).to_vec(), b"one".to_vec()),
            (neighbour.clone(), vec![0u8; value]),
            (member(2).to_vec(), b"two".to_vec()),
        ]
    };
    let exact = scanned_over("shards/1000001", &rows(fill), &pair).await;
    assert_eq!(exact, (tails(&[Some(b"one"), Some(b"two")]), 2), "1 MiB");
    let over = scanned_over("shards/1000010", &rows(fill + 1), &pair).await;
    assert_eq!(over, (tails(&[Some(b"one"), None]), 1), "1 MiB and a byte");
}
