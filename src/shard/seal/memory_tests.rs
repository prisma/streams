#![cfg(test)]
//! HIS-027, what layout 6 bounds in one process today: several shard
//! engines on one store, each with 100 active streams over a space of a
//! million routing keys, written far past the unsealed trigger. Per shard
//! the unsealed pages stay near the trigger; the in-memory seal directory
//! grows by at most one entry per active stream per seal, and every entry
//! stays under a fixed cap. Package 5's fold bounds the directory itself
//! at one fold period (DESIGN-6A §2.3); its assertion joins this test then.

use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use futures_util::future::join_all;
use rand::{Rng, SeedableRng, rngs::StdRng};

use super::tests::{append_lane, hashes, manual, open, random_bytes, unique};
use crate::shard::ShardEngine;

const ENGINES: usize = 6;
const ACTIVE: usize = 100;
const ROUNDS: usize = 30;
const RECORD: usize = 2 << 10;
const TRIGGER: u64 = 256 << 10;
/// Unsealed bytes a shard may hold at a sample: four triggers, a sixth of
/// what each shard is written.
const UNSEALED_MAX: u64 = 4 * TRIGGER;
/// One directory entry's memory, its key summary and anchors included.
const ENTRY_MAX: u64 = 512;

/// One round: one append of one record under a random key of the million
/// to each active stream of `engine`, all in flight at once.
async fn round(engine: &Arc<ShardEngine>, streams: &[[u8; 16]], rng: &mut StdRng) {
    let appends: Vec<(String, Bytes)> = streams
        .iter()
        .map(|_| {
            (
                format!("k{}", rng.random_range(0..1_000_000u32)),
                random_bytes(rng, RECORD),
            )
        })
        .collect();
    join_all(streams.iter().zip(appends).map(|(hash, (key, record))| {
        let engine = engine.clone();
        async move { append_lane(&engine, *hash, &key, vec![record]).await }
    }))
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn many_engines_hold_unsealed_pages_near_the_trigger_and_bounded_directory_entries() {
    let mut settings = manual();
    settings.auto = true;
    settings.unsealed_bytes = TRIGGER;
    settings.age_seal = Duration::from_millis(200);
    settings.tick = Duration::from_millis(10);
    let store: Arc<dyn object_store::ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let mut engines = Vec::new();
    for i in 0..ENGINES {
        let prefix = unique(&format!("memory-{i}"));
        engines.push(
            open(
                store.clone(),
                &prefix,
                settings,
                1 << 20,
                Duration::from_millis(2),
            )
            .await,
        );
    }
    let mut rng = StdRng::seed_from_u64(0x27);
    let streams: Vec<Vec<[u8; 16]>> = (0..ENGINES).map(|_| hashes(&mut rng, ACTIVE)).collect();
    let mut peak = vec![0u64; ENGINES];
    for _ in 0..ROUNDS {
        let seeds: Vec<u64> = (0..ENGINES).map(|_| rng.random()).collect();
        join_all(engines.iter().zip(&streams).zip(seeds).map(
            |((engine, streams), seed)| async move {
                round(engine, streams, &mut StdRng::seed_from_u64(seed)).await;
            },
        ))
        .await;
        tokio::time::sleep(Duration::from_millis(50)).await;
        for (i, engine) in engines.iter().enumerate() {
            let unsealed = engine.maintenance_snapshot().unabsorbed_frame_bytes;
            peak[i] = peak[i].max(unsealed);
            assert!(
                engine.layout.unsealed().len() <= ACTIVE,
                "engine {i}: more streams held than active"
            );
        }
    }
    let written = (ROUNDS * ACTIVE * RECORD) as u64;
    eprintln!(
        "HIS-027: {ENGINES} engines, {written} B written each; peak unsealed per engine {peak:?}"
    );
    for (i, p) in peak.iter().enumerate() {
        assert!(
            *p <= UNSEALED_MAX,
            "engine {i}: {p} B unsealed (max {UNSEALED_MAX}, written {written})"
        );
    }
    check_directories(&engines).await;
}

/// After the last seal: every engine's directory holds at most one entry
/// per active stream per seal, and every entry is under `ENTRY_MAX`.
async fn check_directories(engines: &[Arc<ShardEngine>]) {
    let t0 = Instant::now();
    while engines
        .iter()
        .any(|e| e.maintenance_snapshot().unabsorbed_frame_bytes > 0)
    {
        assert!(
            t0.elapsed() < Duration::from_secs(10),
            "the age trigger never sealed the rest"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let mut total = 0u64;
    for (i, engine) in engines.iter().enumerate() {
        // No seal in flight: a seal publishes its entries before its commit
        // and counts itself after 7a, so a running one would be half-counted.
        engine.layout.seal.configure(|s| s.auto = false);
        let _no_seal = engine.layout.seal.serial.lock().await;
        let seals = engine
            .layout
            .seal
            .counters
            .seals
            .load(std::sync::atomic::Ordering::Relaxed);
        let directory = engine.layout.seal.directory.read().unwrap();
        let entries = directory.entries();
        let sizes: Vec<u64> = directory.entry_bytes().collect();
        let largest = sizes.iter().copied().max().unwrap_or(0);
        let bytes: u64 = sizes.iter().sum();
        assert_eq!(
            directory.bytes(),
            bytes,
            "engine {i}: the gauge is the entries' sum"
        );
        total += bytes;
        eprintln!(
            "HIS-027 engine {i}: {seals} seals, {entries} entries, {bytes} B, largest {largest} B"
        );
        assert!(entries > 0 && seals > 1, "engine {i}: nothing sealed");
        assert!(
            entries <= seals * ACTIVE as u64,
            "engine {i}: {entries} entries from {seals} seals"
        );
        assert!(largest <= ENTRY_MAX, "engine {i}: an entry of {largest} B");
    }
    eprintln!(
        "HIS-027: directories hold {total} B across {} engines",
        engines.len()
    );
    for engine in engines {
        engine.begin_close();
    }
}
