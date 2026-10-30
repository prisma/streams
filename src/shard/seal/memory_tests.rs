#![cfg(test)]
//! HIS-027, what layout 6 bounds in one process: several shard engines on
//! one store, each with 100 active streams over a space of a million
//! routing keys, written far past the unsealed trigger. Per shard the
//! unsealed pages stay near the trigger; the in-memory seal directory
//! grows by at most one entry per active stream per seal, every entry
//! stays under a fixed cap, and the fold keeps the directory to the chunks
//! sealed within one fold period (DESIGN-6A §2.3): at every sample it holds
//! no more than the shard sealed in the last two periods, while the shard
//! seals many times that over the run.

use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::atomic::Ordering;
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
/// The fold period the engines run with.
const FOLD_PERIOD: Duration = Duration::from_millis(300);
/// The window the directory's bound is checked against: two periods (a
/// fold's scan lags its period by at most one tick and its own run).
const FOLD_WINDOW: Duration = Duration::from_millis(600);

/// Each engine's chunks sealed so far, by sample time: the folded entries
/// plus the directory's (no consolidation runs: nothing reads).
struct Sealed {
    samples: VecDeque<(Instant, u64)>,
    peak_directory: u64,
    peak_excess: i64,
}

impl Sealed {
    fn new() -> Self {
        Self {
            samples: VecDeque::new(),
            peak_directory: 0,
            peak_excess: i64::MIN,
        }
    }

    /// Sample `engine`: its directory must hold at most what it sealed
    /// within `FOLD_WINDOW`.
    fn sample(&mut self, i: usize, engine: &ShardEngine) {
        let (entries, largest) = {
            let d = engine.layout.seal.directory.read().unwrap();
            (d.entries(), d.entry_bytes().max().unwrap_or(0))
        };
        assert!(largest <= ENTRY_MAX, "engine {i}: an entry of {largest} B");
        let folded = engine
            .layout
            .seal
            .fold
            .counters
            .entries_folded
            .load(Ordering::Relaxed);
        let now = Instant::now();
        let total = folded + entries;
        self.samples.push_back((now, total));
        while self.samples.len() > 1 && self.samples[1].0 <= now - FOLD_WINDOW {
            self.samples.pop_front();
        }
        let base = match self.samples.front() {
            Some((t, n)) if *t <= now - FOLD_WINDOW => *n,
            _ => 0,
        };
        let window = total - base;
        self.peak_directory = self.peak_directory.max(entries);
        self.peak_excess = self.peak_excess.max(entries as i64 - window as i64);
        assert!(
            entries <= window,
            "engine {i}: the directory holds {entries} entries, more than the {window} sealed in the last two fold periods"
        );
    }
}

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

#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
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
        let engine = open(
            store.clone(),
            &prefix,
            settings,
            1 << 20,
            Duration::from_millis(2),
        )
        .await;
        engine.layout.seal.fold.configure(|f| {
            f.period = FOLD_PERIOD;
            f.tick = Duration::from_millis(20);
        });
        engines.push(engine);
    }
    let mut sealed: Vec<Sealed> = (0..ENGINES).map(|_| Sealed::new()).collect();
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
            sealed[i].sample(i, engine);
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
    check_folds(&engines, &sealed);
    check_directories(&engines).await;
}

/// The fold ran and kept the directory to a small part of what was sealed.
fn check_folds(engines: &[Arc<ShardEngine>], sealed: &[Sealed]) {
    for (i, (engine, s)) in engines.iter().zip(sealed).enumerate() {
        let fold = &engine.layout.seal.fold.counters;
        let folds = fold.folds.load(Ordering::Relaxed);
        let folded = fold.entries_folded.load(Ordering::Relaxed);
        let total = s.samples.back().map_or(0, |(_, n)| *n);
        eprintln!(
            "HIS-027 engine {i}: {folds} folds, {folded} of {total} entries folded, peak directory {} (peak excess over the window {})",
            s.peak_directory, s.peak_excess
        );
        assert!(folds >= 3, "engine {i}: the fold ran {folds} times");
        assert!(
            s.peak_directory * 2 <= total,
            "engine {i}: the directory peaked at {} of {total} entries",
            s.peak_directory
        );
    }
}

/// After the last seal: every engine's directory holds at most one entry
/// per active stream per seal (seals since the last fold), and every entry
/// is under `ENTRY_MAX`.
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
        assert!(seals > 1, "engine {i}: nothing sealed");
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
