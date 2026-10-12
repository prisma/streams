//! The sealer's triggers and deadlines (SPIKE-PLAN M3, review B1/B2).
//!
//! Every dimension has a seal trigger and, above it, a deadline (B1): the
//! trigger starts a seal; the deadline flushes the memtable whatever a seal
//! is doing (degraded mode, values into the L0, as today). The memtable
//! dimension's deadline is SlateDB's own freeze at `l0_sst_size_bytes`
//! (8 MiB on shard DBs), so the spike adds none there.

use std::time::Duration;

/// What the memtable dimension's trigger counts.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum TriggerMode {
    /// The trigger fix: SlateDB's L0 estimate of every row version and
    /// tombstone written since the last forced memtable flush.
    Memtable,
    /// The control (`L6_SPILL_TRIGGER=pages`): the design's original
    /// trigger, unabsorbed page bytes on the shard.
    Pages,
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct SpillSettings {
    pub(crate) mode: TriggerMode,
    /// Seal when the memtable dimension reaches this (6 MiB; SlateDB units
    /// in `Memtable` mode, page bytes in `Pages` mode).
    pub(crate) memtable_bytes: u64,
    /// Seal at this many WAL objects since the last memtable flush (48).
    pub(crate) wal_objects_seal: u64,
    /// Flush the memtable at this many, seal or no seal (64, the G6 bound).
    pub(crate) wal_objects_deadline: u64,
    /// Seal when the oldest unabsorbed page is this old (45 s).
    pub(crate) age_seal: Duration,
    /// Flush the memtable when it has held an unabsorbed page this long
    /// (60 s), seal or no seal.
    pub(crate) age_deadline: Duration,
    /// Seal when unabsorbed page bytes reach this share of the ring share
    /// (percent), or when the ring refused a batch since the last seal.
    pub(crate) ring_pct: u64,
    /// How often the triggers are evaluated (besides after each durable
    /// group).
    pub(crate) tick: Duration,
    /// Chunks hold at most this many pages, so a seal stages at most this
    /// many page tombstones per stream per group (item 13).
    pub(crate) max_pages_per_chunk: usize,
    /// A commit group carries at most this many streams' advances.
    pub(crate) max_streams_per_group: usize,
    /// Whether the engine's spill task seals and flushes by itself. Tests
    /// that drive `spill_once` directly turn it off.
    pub(crate) auto: bool,
}

impl Default for SpillSettings {
    fn default() -> Self {
        *from_env()
    }
}

fn from_env() -> &'static SpillSettings {
    static S: std::sync::OnceLock<SpillSettings> = std::sync::OnceLock::new();
    S.get_or_init(|| {
        let num = |name: &str, default: u64| {
            std::env::var(name)
                .ok()
                .and_then(|v| v.parse::<u64>().ok())
                .unwrap_or(default)
        };
        let mode = match std::env::var("L6_SPILL_TRIGGER").as_deref() {
            Ok("pages") => TriggerMode::Pages,
            _ => TriggerMode::Memtable,
        };
        let wal_deadline = num("L6_SPILL_WAL_OBJECTS", 64);
        let age_deadline = num("L6_SPILL_AGE_MS", 60_000);
        SpillSettings {
            mode,
            memtable_bytes: num("L6_SPILL_MEMTABLE_BYTES", 6 << 20),
            wal_objects_seal: num("L6_SPILL_WAL_OBJECTS_SEAL", wal_deadline * 3 / 4),
            wal_objects_deadline: wal_deadline,
            age_seal: Duration::from_millis(num("L6_SPILL_AGE_SEAL_MS", age_deadline * 3 / 4)),
            age_deadline: Duration::from_millis(age_deadline),
            ring_pct: num("L6_SPILL_RING_PCT", 75),
            tick: Duration::from_millis(num("L6_SPILL_TICK_MS", 250)),
            max_pages_per_chunk: usize::try_from(num("L6_SPILL_MAX_PAGES", 8192)).unwrap_or(8192),
            max_streams_per_group: usize::try_from(num("L6_SPILL_MAX_STREAMS", 1024))
                .unwrap_or(1024),
            auto: true,
        }
    })
}

/// Why a seal started.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Trigger {
    Memtable,
    Pages,
    WalObjects,
    Age,
    Ring,
    /// Called directly (tests).
    Manual,
}

impl Trigger {
    pub(crate) const ALL: [Trigger; 6] = [
        Trigger::Memtable,
        Trigger::Pages,
        Trigger::WalObjects,
        Trigger::Age,
        Trigger::Ring,
        Trigger::Manual,
    ];

    pub(crate) fn index(self) -> usize {
        match self {
            Trigger::Memtable => 0,
            Trigger::Pages => 1,
            Trigger::WalObjects => 2,
            Trigger::Age => 3,
            Trigger::Ring => 4,
            Trigger::Manual => 5,
        }
    }

    pub(crate) fn name(self) -> &'static str {
        match self {
            Trigger::Memtable => "memtable",
            Trigger::Pages => "pages",
            Trigger::WalObjects => "wal_objects",
            Trigger::Age => "age",
            Trigger::Ring => "ring",
            Trigger::Manual => "manual",
        }
    }
}

/// Why the memtable was flushed without a seal.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Deadline {
    WalObjects,
    Age,
    /// The last flush of a measurement (tests and shutdown), so a frozen,
    /// not yet flushed memtable is counted (review A1).
    Final,
}
