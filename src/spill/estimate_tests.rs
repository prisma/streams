//! Calibration of the sealer's memtable trigger against SlateDB's own freeze (review
//! B2): the row at which `spill::estimate::l0_estimate` reaches
//! `l0_sst_size_bytes` must be the row at which SlateDB froze the memtable.
//!
//! Each mix writes rows with keys ordered by row number into a DB with no
//! compactor, single-row batches near the threshold, then forces a flush.
//! The oldest L0 SST is the memtable SlateDB froze on its own; its last key
//! names the row that froze it. The inputs are checked too: the estimator is
//! fed SlateDB's own `write_ops` and `memtable_write_bytes` deltas.

#![cfg(test)]

use std::sync::Arc;

use bytes::Bytes;
use rand::{Rng, SeedableRng, rngs::StdRng};
use slatedb::config::{FlushOptions, FlushType, Settings, WriteOptions};
use slatedb::db_stats;

use super::estimate::l0_estimate;

const L0_BYTES: u64 = 8 << 20;

#[derive(Clone, Copy, Debug)]
enum Row {
    /// A page: 25 B key, 300 B to 64 KiB value.
    Page,
    /// A lease or ack row: 35 B key, 60 to 80 B value.
    Lease,
    /// A page tombstone: 25 B key.
    Tombstone,
}

fn key(i: u64, len: usize) -> Vec<u8> {
    let mut k = i.to_be_bytes().to_vec();
    k.resize(len, 0x5A);
    k
}

fn row_of(rng: &mut StdRng, mix: &[Row]) -> Row {
    mix[rng.random_range(0..mix.len())]
}

/// Add row `i` of kind `row` to `wb`: its raw key + value bytes.
fn put_row(wb: &mut slatedb::WriteBatch, row: Row, i: u64, rng: &mut StdRng, pool: &Bytes) -> u64 {
    let (key_len, value_len) = match row {
        Row::Page => (25, Some(rng.random_range(300..=(64 << 10)))),
        Row::Lease => (35, Some(rng.random_range(60..=80))),
        Row::Tombstone => (25, None),
    };
    let k = key(i, key_len);
    let raw = k.len() + value_len.unwrap_or(0);
    match value_len {
        Some(len) => wb.put_bytes(Bytes::from(k), pool.slice(..len)),
        None => wb.delete(k),
    }
    u64::try_from(raw).unwrap()
}

/// Write rows of `mix` into `db` until 64 rows past the row the estimate
/// says freezes the memtable: (that row, rows written, raw key+value bytes).
async fn write_past_the_freeze(db: &slatedb::Db, rng: &mut StdRng, mix: &[Row]) -> (u64, u64, u64) {
    // A write returns once it is in the memtable (its handle awaits
    // durability; nothing here waits on it).
    let opts = WriteOptions::default();
    let pool: Bytes = {
        let mut v = vec![0u8; 64 << 10];
        rng.fill(&mut v[..]);
        Bytes::from(v)
    };
    let (mut n, mut kv) = (0u64, 0u64);
    let mut predicted = None;
    let mut i = 0u64;
    // Up to 256 rows a batch while far below the threshold, one row a batch
    // near it, and 64 rows past the predicted freeze.
    let max_row: u64 = if mix.iter().any(|r| matches!(r, Row::Page)) {
        (64 << 10) + 128
    } else {
        256
    };
    loop {
        let near = l0_estimate(n, kv) + 256 * max_row >= L0_BYTES;
        let rows = if near { 1 } else { 256 };
        let mut wb = slatedb::WriteBatch::new();
        for _ in 0..rows {
            kv += put_row(&mut wb, row_of(rng, mix), i, rng, &pool);
            n += 1;
            if predicted.is_none() && l0_estimate(n, kv) >= L0_BYTES {
                predicted = Some(i);
            }
            i += 1;
        }
        db.write_with_options(wb, &opts).await.unwrap();
        if predicted.is_some_and(|p| i > p + 64) {
            break;
        }
    }
    (predicted.unwrap(), n, kv)
}

/// Write rows of `mix` until SlateDB freezes; returns (predicted freeze row,
/// actual freeze row, rows, raw key+value bytes, estimate at the freeze).
async fn freeze_point(seed: u64, mix: &[Row]) -> (u64, u64, u64, u64) {
    let mut rng = StdRng::seed_from_u64(seed);
    let store = Arc::new(object_store::memory::InMemory::new());
    let path = format!("spill-estimate-{seed}-{}", mix.len());
    let metrics = crate::spill::metrics::for_log(&path);
    let settings = Settings {
        l0_sst_size_bytes: usize::try_from(L0_BYTES).unwrap(),
        max_unflushed_bytes: 512 << 20,
        compactor_options: None,
        ..Default::default()
    };
    let db = slatedb::Db::builder(path.as_str(), store)
        .with_settings(settings)
        .with_metrics_recorder(metrics.recorder())
        .build()
        .await
        .unwrap();
    let ops0 = metrics.counter(db_stats::WRITE_OPS);
    let bytes0 = metrics.counter(db_stats::MEMTABLE_WRITE_BYTES);
    let (predicted, n, kv) = write_past_the_freeze(&db, &mut rng, mix).await;
    // The estimator's inputs are SlateDB's own counters.
    assert_eq!(metrics.counter(db_stats::WRITE_OPS) - ops0, n, "write_ops");
    assert_eq!(
        metrics.counter(db_stats::MEMTABLE_WRITE_BYTES) - bytes0,
        kv,
        "memtable_write_bytes"
    );
    db.flush_with_options(FlushOptions {
        flush_type: FlushType::MemTable,
    })
    .await
    .unwrap();
    let manifest = db.manifest();
    let firsts: Vec<(u64, u64)> = manifest
        .l0()
        .iter()
        .map(|v| {
            let first = v.sst.info.first_entry.as_ref().unwrap();
            let last = v.sst.info.last_entry.as_ref().unwrap();
            let idx = |b: &Bytes| u64::from_be_bytes(b[..8].try_into().unwrap());
            (idx(first), idx(last))
        })
        .collect();
    assert_eq!(
        firsts.len(),
        2,
        "one own freeze and the forced flush: {firsts:?}"
    );
    let oldest = firsts.iter().min().unwrap();
    assert_eq!(oldest.0, 0);
    db.close().await.unwrap();
    (predicted, oldest.1, n, kv)
}

/// The estimator names the row SlateDB froze at, for pages, lease rows,
/// tombstones and a mix of all three.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_memtable_estimate_matches_slatedbs_own_freeze() {
    let mixes: [(&str, &[Row]); 4] = [
        ("pages", &[Row::Page]),
        ("lease rows", &[Row::Lease]),
        ("tombstones", &[Row::Tombstone]),
        (
            "mixed",
            &[
                Row::Page,
                Row::Lease,
                Row::Lease,
                Row::Lease,
                Row::Tombstone,
            ],
        ),
    ];
    for (seed, (name, mix)) in mixes.iter().enumerate() {
        let (predicted, actual, rows, kv) = freeze_point(seed as u64 + 1, mix).await;
        eprintln!(
            "memtable estimate calibration {name}: froze at row {actual}, predicted {predicted} \
             (rows written {rows}, raw key+value {kv} B)"
        );
        assert_eq!(
            actual, predicted,
            "{name}: SlateDB froze at {actual}, the estimate says {predicted}"
        );
    }
}

/// The SlateDB units of each row kind against its raw key + value bytes
/// : pages are within a few percent, lease
/// rows a quarter or more above.
#[test]
fn slatedb_units_per_raw_byte() {
    let ratio = |n: u64, kv: u64| l0_estimate(n, kv) as f64 / kv as f64;
    let page = ratio(1_000, 1_000 * (25 + 10_240));
    let lease = ratio(60_000, 60_000 * (35 + 70));
    let tomb = ratio(150_000, 150_000 * 25);
    eprintln!(
        "SlateDB units per raw byte: 10 KiB pages {page:.4}, lease rows {lease:.4}, tombstones {tomb:.4}"
    );
    assert!(page < 1.01, "{page}");
    assert!((1.25..1.35).contains(&lease), "{lease}");
    assert!(tomb > 2.0, "{tomb}");
}
