//! L6 seal spike: design B's spill path ("seal" in the design; `seal_*`
//! already names segment sealing here, DESIGN §3.2 item 23).
//!
//! Throwaway code on the never-merged `l6-spike` branch
//! (`~/.streams-k2/analysis/public-beta/l6/SPIKE-PLAN.md`). M2 builds the
//! pure building blocks: the segment object (`object`), its CRC-32C
//! (`crc32c`) and the index and refcount rows (`index`). The ring's
//! process-wide budget lives with the ring (`shard::tail_ring`). M3 adds the
//! sealer's settings and triggers (`settings`), the memtable trigger in
//! SlateDB's units (`estimate`) and its counters (`stats`); the sealer
//! itself needs the engine's internals and lives in `shard::spill`.
//!
//! `L6_SPILL=1` is the spike's one switch (SPIKE-PLAN §2.1): it exists only
//! so both arms come from one build; production B would have one path.
#![allow(dead_code, reason = "L6 spike: building blocks M4 (reads) and M8 (refcounts) use")]

pub(crate) mod crc32c;
pub(crate) mod estimate;
pub(crate) mod index;
pub(crate) mod object;
pub(crate) mod settings;
pub(crate) mod stats;

#[cfg(test)]
mod bench_tests;
#[cfg(test)]
mod crc32c_tests;
#[cfg(test)]
mod estimate_tests;
#[cfg(test)]
mod index_tests;
#[cfg(test)]
mod object_tests;
#[cfg(test)]
pub(crate) mod test_pages;

pub(crate) use crate::shard::spill::read::read_range;

/// L6 spike M4: a keyed history read on the spill arm, refused loudly and
/// counted (per shard DB and process-wide); the error a read returns.
pub(crate) fn keyed_unsupported(engine: &crate::shard::ShardEngine) -> String {
    use std::sync::atomic::Ordering;
    stats::KEYED_UNSUPPORTED_TOTAL.fetch_add(1, Ordering::Relaxed);
    if let Some(sp) = &engine.spill {
        sp.stats.reads.keyed_unsupported.fetch_add(1, Ordering::Relaxed);
    }
    tracing::error!(shard = %engine.prefix, "l6 spike: keyed history read unsupported");
    #[cfg(test)]
    eprintln!("L6 keyed history read refused on shard {}", engine.prefix);
    "l6 spike: keyed history read unsupported".to_string()
}

/// L6 spike M4: a default-lane read of sealed data (`Some("")`), counted.
pub(crate) fn note_lane_read(engine: &crate::shard::ShardEngine) {
    if let Some(sp) = &engine.spill {
        sp.stats
            .reads
            .lane_reads
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }
}

/// Whether this process runs the spill arm (`L6_SPILL=1`), read once.
pub(crate) fn enabled() -> bool {
    static ON: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ON.get_or_init(|| std::env::var("L6_SPILL").is_ok_and(|v| v == "1"))
}

/// The ring budget of a spill-arm process (review B5), from
/// `L6_RING_PROCESS_BYTES` (64 MiB), `L6_RING_SHARD_BYTES` (8 MiB, the cap)
/// and `L6_RING_FLOOR_BYTES` (2 MiB, the minimum under contention).
pub(crate) fn ring_settings_from_env() -> crate::shard::RingSettings {
    let read = |name: &str, default: u64| {
        std::env::var(name)
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(default)
    };
    crate::shard::RingSettings {
        process_bytes: read("L6_RING_PROCESS_BYTES", 64 << 20),
        shard_cap_bytes: read("L6_RING_SHARD_BYTES", 8 << 20),
        shard_floor_bytes: read("L6_RING_FLOOR_BYTES", 2 << 20),
    }
}
