#![cfg(test)]
//! Sealed reads through a shard's run-index merge and its cleaner on a
//! multi-shard cell (the multi-shard read 500 of 2026-11-08). A shard owns a
//! stream by the prefix of its route hash (`RouteHash::for_stream`, minted by
//! the registry from the project and name), while the stream's rows, footers
//! and chunk tables carry its storage hash (`StreamDesc::storage_hash`, the
//! engine identity), which shares nothing with that prefix. A merge that
//! kept only the footers whose storage hash had the shard's prefix dropped
//! most of the shard's own streams from the merged run-index object (their
//! records stored but unreachable, their reads 500), and a clean pass that
//! counted only such chunks left the shard's other live chunks out of its
//! copies, so the originals stayed pinned and their dead bytes unreclaimed.

use super::fixture_billing::DrainWait;
use super::fixture_cell::{Cell, CellSpec, open_cell, project};
use super::fixture_http::engine_shutdown;
use std::sync::atomic::Ordering;

/// The shards' bit prefixes: the registry's longest-prefix code.
const PREFIXES: [&str; 4] = ["00", "01", "10", "11"];
/// Ledger streams: they stay live and must read back whole.
const LEDGERS: usize = 16;
/// Append-seal-fold rounds: the default fan-in of a level-0 merge.
const ROUNDS: usize = 4;

fn ledger(n: usize) -> String {
    format!("ledger-{n:02}")
}

fn bulk(n: usize) -> String {
    format!("bulk-{n:02}")
}

/// Ledger `n`'s record of `round`, as its read returns the value.
fn ledger_value(round: usize, n: usize) -> serde_json::Value {
    serde_json::json!({ "round": round, "n": n })
}

/// A cell of four bit-prefix shards, its ledgers created.
async fn cell() -> Cell {
    let mut spec = CellSpec::open(1);
    // No tail ring: the reads go to the sealed chunks.
    spec.cli = |c| c.tail_ring_bytes = 0;
    spec.prefixes = PREFIXES.iter().map(|p| (*p).to_string()).collect();
    let cell = open_cell(spec).await;
    hold_seals(&cell).await;
    for n in 0..LEDGERS {
        create(&cell, &ledger(n)).await;
    }
    cell
}

/// Opens every shard and turns its own sealer off: only the test seals,
/// so a seal object holds every stream's chunk of its round.
async fn hold_seals(cell: &Cell) {
    for prefix in PREFIXES {
        let wait = std::time::Duration::from_secs(10);
        match cell.state.shards.open_or_wait(prefix, wait).await {
            crate::sharddir::OpenOutcome::Ready(engine) => {
                engine.configure_seal(|s| s.auto = false);
            }
            _ => panic!("shard {prefix} did not open"),
        }
    }
}

async fn create(cell: &Cell, name: &str) {
    let path = format!("/v1/streams/{name}");
    let created = cell
        .call(0, "PUT", &path, br#"{"format":{"kind":"json"}}"#)
        .await;
    assert_eq!(created.0, 201, "{name}");
}

async fn append(cell: &Cell, name: &str, value: &serde_json::Value) {
    let path = format!("/v1/streams/{name}/records:batch");
    let body = serde_json::to_vec(&[value]).unwrap();
    assert_eq!(cell.call(0, "POST", &path, &body).await.0, 200, "{name}");
}

/// Seal every shard (twice: a requested seal returns once it committed,
/// and the second takes anything the first's snapshot missed), then fold.
async fn seal_and_fold(cell: &Cell) {
    for engine in cell.state.shards.engines() {
        for _ in 0..2 {
            engine
                .seal_once(crate::shard::seal::Trigger::Requested)
                .await
                .expect("seal");
        }
        engine.fold_once().await.expect("fold");
    }
}

/// The ledgers whose reads differ from their `ROUNDS` records:
/// (ledger, status, values read).
async fn short_ledgers(cell: &Cell) -> Vec<(usize, u16, Vec<serde_json::Value>)> {
    let mut short = Vec::new();
    for n in 0..LEDGERS {
        let path = format!("/v1/streams/{}/records", ledger(n));
        let (status, _, body) = cell.call(0, "GET", &path, b"").await;
        let records: Vec<serde_json::Value> = serde_json::from_slice(&body).unwrap_or_default();
        let values = records;
        let want: Vec<serde_json::Value> = (0..ROUNDS).map(|r| ledger_value(r, n)).collect();
        if status != 200 || values != want {
            short.push((n, status, values));
        }
    }
    short
}

/// Ledgers whose storage hash's leading bits are not their shard's prefix,
/// which a filter on the storage hash would wrongly treat as foreign.
async fn misprefixed(cell: &Cell) -> usize {
    let prefixes: Vec<String> = PREFIXES.iter().map(|p| (*p).to_string()).collect();
    let mut count = 0;
    for n in 0..LEDGERS {
        let sref = crate::tenant::TenantStreamRef::new(
            crate::tenant::ProjectId::new(&project(0)).unwrap(),
            crate::tenant::CanonicalStreamName::new(&ledger(n)).unwrap(),
        );
        let route = crate::crypto::RouteHash::for_stream(&sref).0;
        let owner = crate::registry::shard_for_hash(&prefixes, &route);
        let storage = cell
            .state
            .registry
            .get(&sref)
            .await
            .unwrap()
            .expect("live")
            .storage_hash();
        if crate::registry::shard_for_hash(&prefixes, &storage) != owner {
            count += 1;
        }
    }
    count
}

/// **A run-index merge keeps every stream its shard serves.** 16 ledgers
/// take a record per round over four rounds of seal and fold on each of
/// four shards, then each shard runs one merge: every ledger still reads
/// its four records. Before: about three quarters of them answered 500
/// (`stored record corruption: Missing`), their index entries dropped.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_merge_keeps_every_stream_its_shard_serves() {
    let cell = cell().await;
    assert!(
        misprefixed(&cell).await > 0,
        "a storage hash off its prefix"
    );
    for round in 0..ROUNDS {
        for n in 0..LEDGERS {
            append(&cell, &ledger(n), &ledger_value(round, n)).await;
        }
        seal_and_fold(&cell).await;
    }
    cell.state.registry.sweep_cache();
    assert_eq!(short_ledgers(&cell).await, Vec::new(), "before the merges");
    let engines = cell.state.shards.engines();
    assert_eq!(engines.len(), PREFIXES.len(), "every shard holds a stream");
    for engine in &engines {
        engine
            .merge_once()
            .await
            .expect("a merge is due")
            .expect("merge");
    }
    assert_eq!(short_ledgers(&cell).await, Vec::new(), "after the merges");
    engine_shutdown(&cell.state).await;
}

/// Runs the production sweep, a drain round (a purge waits until the
/// close reached the ledger) and every shard's collector pass until no
/// shard holds a purge that is asked for and not yet done.
async fn purge_all(cell: &Cell) {
    let mut drain = DrainWait::default();
    for _ in 0..40 {
        crate::billing::sweep_owned_outboxes(&cell.state).await;
        drain.round(&cell.state).await;
        let mut pending = 0;
        for engine in cell.state.shards.engines() {
            engine.gc_pass().await;
            pending += engine.purge_due().await.expect("purge");
        }
        if pending == 0 {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    panic!("the bulk streams were never purged");
}

/// 8 KiB of letters that do not compress, seeded by `seed`: a bulk record
/// outweighs a ledger's in its object.
fn pad(seed: usize) -> String {
    let mut x = (seed as u64 + 1).wrapping_mul(0x9E37_79B9_7F4A_7C15);
    (0..8 * 1024)
        .map(|_| {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            char::from(b'a' + u8::try_from(x % 26).unwrap_or(0))
        })
        .collect()
}

/// The test cleaner's rule: rewrite anything less than half live.
fn half_dead(object_len: u64, live_bytes: u64, live_chunks: u64) -> bool {
    live_chunks > 0 && 2 * live_bytes < object_len
}

/// What every shard's cleaner released: objects it rewrote whole.
fn cleaned(cell: &Cell) -> u64 {
    let engines = cell.state.shards.engines();
    let of = |e: &std::sync::Arc<crate::shard::ShardEngine>| {
        e.gc_state().counters.cleaned.load(Ordering::Relaxed)
    };
    engines.iter().map(of).sum()
}

/// **A clean pass copies every live chunk its shard serves.** Beside the 16
/// ledgers, 16 bulk streams take a large record per round in the same
/// seals; the bulk streams are deleted and purged, so each shard's four seal
/// objects are mostly dead. After the purge (whose collector passes run the
/// cleaner), a merge, a clean pass and the collector, the cleaner has copied
/// every ledger chunk and released all 16 originals, and every ledger reads
/// its four records exactly. Before: the purge reclaimed nothing of the bulk
/// streams the merges had dropped from the index, so no object was a
/// candidate; and the cleaner alone copied only the chunks whose storage
/// hash had its shard's prefix, so the originals stayed pinned by the rest
/// and their dead bytes were never reclaimed.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_clean_pass_copies_every_live_chunk_its_shard_serves() {
    let cell = cell().await;
    assert!(
        misprefixed(&cell).await > 0,
        "a storage hash off its prefix"
    );
    for n in 0..LEDGERS {
        create(&cell, &bulk(n)).await;
    }
    for round in 0..ROUNDS {
        for n in 0..LEDGERS {
            append(&cell, &ledger(n), &ledger_value(round, n)).await;
            let value = serde_json::json!({ "pad": pad(round * LEDGERS + n), "round": round });
            append(&cell, &bulk(n), &value).await;
        }
        seal_and_fold(&cell).await;
    }
    for engine in cell.state.shards.engines() {
        engine.gc_state().configure(|s| {
            s.grace = std::time::Duration::ZERO;
            s.purge_delay = std::time::Duration::ZERO;
            s.clean_pays = half_dead;
        });
    }
    for n in 0..LEDGERS {
        let path = format!("/v1/streams/{}", bulk(n));
        let deleted = cell.call(0, "DELETE", &path, b"").await.0;
        assert!(matches!(deleted, 200 | 204), "{path}: {deleted}");
    }
    purge_all(&cell).await;
    for engine in cell.state.shards.engines() {
        engine.collect().await.expect("collect");
        if let Some(merged) = engine.merge_once().await {
            merged.expect("merge");
        }
        engine.clean_pass().await.expect("clean");
        engine.collect().await.expect("collect");
    }
    let seals = (ROUNDS * PREFIXES.len()) as u64;
    assert_eq!(cleaned(&cell), seals, "every seal object rewritten whole");
    cell.state.registry.sweep_cache();
    assert_eq!(
        short_ledgers(&cell).await,
        Vec::new(),
        "after the clean pass"
    );
    engine_shutdown(&cell.state).await;
}
