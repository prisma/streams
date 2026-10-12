//! The sealer's counters, per shard DB, for `/v1/debug/store` (`l6_spill`).

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

use super::settings::{Deadline, Trigger};

/// Upper bounds of the chunk-size histogram's first three buckets (the
/// fourth is everything larger): < 32 KiB, < 256 KiB, < 1 MiB, ≥ 1 MiB.
const CHUNK_BOUNDS: [u64; 3] = [32 << 10, 256 << 10, 1 << 20];
/// Upper bounds (ms, inclusive) of the duration histograms; the last bucket
/// is everything slower.
const MS_BOUNDS: [u64; 7] = [10, 50, 100, 250, 500, 1000, 2000];

pub(crate) struct Histogram<const N: usize> {
    buckets: [AtomicU64; N],
    count: AtomicU64,
    sum: AtomicU64,
    max: AtomicU64,
}

impl<const N: usize> Default for Histogram<N> {
    fn default() -> Self {
        Self {
            buckets: std::array::from_fn(|_| AtomicU64::new(0)),
            count: AtomicU64::new(0),
            sum: AtomicU64::new(0),
            max: AtomicU64::new(0),
        }
    }
}

impl<const N: usize> Histogram<N> {
    fn record(&self, bounds: &[u64], value: u64) {
        let i = bounds.iter().position(|b| value <= *b).unwrap_or(bounds.len());
        self.buckets[i.min(N - 1)].fetch_add(1, Ordering::Relaxed);
        self.count.fetch_add(1, Ordering::Relaxed);
        self.sum.fetch_add(value, Ordering::Relaxed);
        self.max.fetch_max(value, Ordering::Relaxed);
    }

    fn json(&self, bounds: &[u64], unit: &str) -> serde_json::Value {
        let mut buckets = serde_json::Map::new();
        for (i, b) in self.buckets.iter().enumerate() {
            let label = match bounds.get(i) {
                Some(bound) => format!("le_{bound}{unit}"),
                None => "more".into(),
            };
            buckets.insert(label, b.load(Ordering::Relaxed).into());
        }
        serde_json::json!({
            "count": self.count.load(Ordering::Relaxed),
            "sum": self.sum.load(Ordering::Relaxed),
            "max": self.max.load(Ordering::Relaxed),
            "buckets": buckets,
        })
    }
}

/// Every counter the plan lists for M3 (SPIKE-PLAN §3 M3 "Wiring").
#[derive(Default)]
pub(crate) struct SpillStats {
    pub(crate) seals_started: [AtomicU64; 6],
    pub(crate) seals_completed: AtomicU64,
    /// Seals with nothing to seal (only the memtable flush ran).
    pub(crate) seals_empty: AtomicU64,
    pub(crate) seal_failures: AtomicU64,
    /// Seals stopped at a test's abort point.
    pub(crate) seal_aborts: AtomicU64,
    pub(crate) objects_put: AtomicU64,
    pub(crate) object_bytes: AtomicU64,
    pub(crate) chunks: AtomicU64,
    pub(crate) pages_sealed: AtomicU64,
    pub(crate) page_bytes_sealed: AtomicU64,
    pub(crate) index_rows: AtomicU64,
    pub(crate) page_tombstones: AtomicU64,
    pub(crate) groups: AtomicU64,
    pub(crate) streams_per_group_max: AtomicU64,
    pub(crate) tombstones_per_group_max: AtomicU64,
    /// Advances by outcome in the committer.
    pub(crate) advances_exact: AtomicU64,
    pub(crate) advances_detached: AtomicU64,
    pub(crate) advances_diverged: AtomicU64,
    /// Dropped before retirement: a duplicate or a refused lane.
    pub(crate) advances_stale: AtomicU64,
    /// An Exact advance with no descriptor in the side table.
    pub(crate) side_table_missing: AtomicU64,
    /// Step 1 gave up waiting for durable dispatch to catch up (1 s).
    pub(crate) catch_up_timeouts: AtomicU64,
    /// The ring let go of a batch with a page at or above its stream's
    /// absorbed boundary (mirrors the ring share's counter; 0 by design).
    pub(crate) ring_evicted_unabsorbed: AtomicU64,
    /// Streams whose window the ring did not cover (per-stream seek).
    pub(crate) ring_fallbacks: AtomicU64,
    pub(crate) ring_fallback_bytes: AtomicU64,
    /// Forced memtable flushes the seals issued, and SlateDB's
    /// `immutable_memtable_flushes` delta around them, capped at one per
    /// flush; the excess is an own freeze a forced flush carried out.
    pub(crate) spill_flushes: AtomicU64,
    pub(crate) spill_flush_attributed: AtomicU64,
    pub(crate) spill_flush_errors: AtomicU64,
    pub(crate) deadline_flushes_wal: AtomicU64,
    pub(crate) deadline_flushes_age: AtomicU64,
    pub(crate) deadline_flushes_final: AtomicU64,
    pub(crate) deadline_flush_attributed: AtomicU64,
    pub(crate) deadline_flush_errors: AtomicU64,
    /// A deadline flushed the memtable while a seal was running.
    pub(crate) deadline_preempted_seal: AtomicU64,
    /// Forced flushes whose delta exceeded one: an own freeze was pending.
    pub(crate) forced_flush_excess: AtomicU64,
    /// Σ unabsorbed page bytes on the shard at each seal's memtable flush:
    /// values committed after the snapshot, which reach the L0.
    pub(crate) leak_bytes: AtomicU64,
    /// The memtable dimension (SlateDB units) and WAL objects at each seal
    /// trigger and at each forced flush.
    pub(crate) mem_estimate_at_flush_max: AtomicU64,
    pub(crate) wal_objects_at_flush_max: AtomicU64,
    pub(crate) mem_estimate_last: AtomicU64,
    pub(crate) wal_objects_last: AtomicU64,
    /// L0 bytes SlateDB reported (`l0_flush_bytes`) around the seals' flushes.
    pub(crate) spill_l0_bytes: AtomicU64,
    pub(crate) chunk_sizes: Histogram<4>,
    /// M4: DST fixtures that put an absorber's cadence on this sealer.
    pub(crate) absorber_cadences: AtomicU64,
    /// M4: failed attempts of the sealer's startup dirty-index seed.
    pub(crate) seed_failures: AtomicU64,
    /// M4: the read path's counters.
    pub(crate) reads: ReadStats,
    pub(crate) seal_ms: Histogram<8>,
    pub(crate) put_ms: Histogram<8>,
    pub(crate) commit_ms: Histogram<8>,
    pub(crate) flush_ms: Histogram<8>,
}

impl SpillStats {
    pub(crate) fn started(&self, trigger: Trigger) {
        self.seals_started[trigger.index()].fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn deadline(&self, kind: Deadline) {
        let c = match kind {
            Deadline::WalObjects => &self.deadline_flushes_wal,
            Deadline::Age => &self.deadline_flushes_age,
            Deadline::Final => &self.deadline_flushes_final,
        };
        c.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn chunk(&self, bytes: u64) {
        self.chunk_sizes.record(&CHUNK_BOUNDS, bytes);
    }

    pub(crate) fn seal_ms(&self, ms: u64) {
        self.seal_ms.record(&MS_BOUNDS, ms);
    }

    pub(crate) fn put_ms(&self, ms: u64) {
        self.put_ms.record(&MS_BOUNDS, ms);
    }

    pub(crate) fn commit_ms(&self, ms: u64) {
        self.commit_ms.record(&MS_BOUNDS, ms);
    }

    pub(crate) fn flush_ms(&self, ms: u64) {
        self.flush_ms.record(&MS_BOUNDS, ms);
    }

    pub(crate) fn get(c: &AtomicU64) -> u64 {
        c.load(Ordering::Relaxed)
    }

    pub(crate) fn json(&self) -> serde_json::Value {
        let g = Self::get;
        let mut started = serde_json::Map::new();
        for t in Trigger::ALL {
            started.insert(t.name().into(), g(&self.seals_started[t.index()]).into());
        }
        serde_json::json!({
            "seals_started": started,
            "seals_completed": g(&self.seals_completed),
            "seals_empty": g(&self.seals_empty),
            "seal_failures": g(&self.seal_failures),
            "seal_aborts": g(&self.seal_aborts),
            "objects_put": g(&self.objects_put),
            "object_bytes": g(&self.object_bytes),
            "chunks": g(&self.chunks),
            "pages_sealed": g(&self.pages_sealed),
            "page_bytes_sealed": g(&self.page_bytes_sealed),
            "index_rows": g(&self.index_rows),
            "page_tombstones": g(&self.page_tombstones),
            "groups": g(&self.groups),
            "streams_per_group_max": g(&self.streams_per_group_max),
            "tombstones_per_group_max": g(&self.tombstones_per_group_max),
            "advances": {
                "exact": g(&self.advances_exact),
                "detached": g(&self.advances_detached),
                "diverged": g(&self.advances_diverged),
                "stale": g(&self.advances_stale),
                "side_table_missing": g(&self.side_table_missing),
            },
            "catch_up_timeouts": g(&self.catch_up_timeouts),
            "ring_evicted_unabsorbed": g(&self.ring_evicted_unabsorbed),
            "ring_fallbacks": g(&self.ring_fallbacks),
            "ring_fallback_bytes": g(&self.ring_fallback_bytes),
            "spill_flushes": g(&self.spill_flushes),
            "spill_flush_attributed": g(&self.spill_flush_attributed),
            "spill_flush_errors": g(&self.spill_flush_errors),
            "deadline_flushes": {
                "wal_objects": g(&self.deadline_flushes_wal),
                "age": g(&self.deadline_flushes_age),
                "final": g(&self.deadline_flushes_final),
            },
            "deadline_flush_attributed": g(&self.deadline_flush_attributed),
            "deadline_flush_errors": g(&self.deadline_flush_errors),
            "deadline_preempted_seal": g(&self.deadline_preempted_seal),
            "forced_flush_excess": g(&self.forced_flush_excess),
            "leak_bytes": g(&self.leak_bytes),
            "mem_estimate_at_flush_max": g(&self.mem_estimate_at_flush_max),
            "wal_objects_at_flush_max": g(&self.wal_objects_at_flush_max),
            "mem_estimate_last": g(&self.mem_estimate_last),
            "wal_objects_last": g(&self.wal_objects_last),
            "spill_l0_bytes": g(&self.spill_l0_bytes),
            "chunk_sizes": self.chunk_sizes.json(&CHUNK_BOUNDS, "B_lt"),
            "seal_ms": self.seal_ms.json(&MS_BOUNDS, "ms"),
            "put_ms": self.put_ms.json(&MS_BOUNDS, "ms"),
            "commit_ms": self.commit_ms.json(&MS_BOUNDS, "ms"),
            "flush_ms": self.flush_ms.json(&MS_BOUNDS, "ms"),
            "absorber_cadences": g(&self.absorber_cadences),
            "seed_failures": g(&self.seed_failures),
            "reads": self.reads.json(),
        })
    }
}

/// L6 spike M4: what `spill::read_range` did on one shard DB.
#[derive(Default)]
pub(crate) struct ReadStats {
    /// Calls (unkeyed history ranges below the absorbed boundary).
    pub(crate) calls: AtomicU64,
    /// Calls that served their whole window.
    pub(crate) completed: AtomicU64,
    /// Index rows read, and the chunks a read planned from them.
    pub(crate) rows: AtomicU64,
    /// Ranged GETs issued (retries included), and their body bytes.
    pub(crate) gets: AtomicU64,
    pub(crate) get_bytes: AtomicU64,
    /// GETs of a whole chunk (CRC verified) and of part of one (from or to
    /// a byte anchor; each page admitted and later authenticated).
    pub(crate) whole_chunks: AtomicU64,
    pub(crate) partial_chunks: AtomicU64,
    /// Pages admitted, and the page bytes served after clipping.
    pub(crate) pages: AtomicU64,
    /// Calls that stopped at their byte budget.
    pub(crate) budget_partials: AtomicU64,
    /// Calls that found no index row for an offset below the boundary they
    /// were given: an honest partial (DESIGN §3.2 item 6).
    pub(crate) missing_row_partials: AtomicU64,
    /// GETs answered 404 while their index row was live: a retryable error.
    pub(crate) not_found_retryable: AtomicU64,
    /// Transient GET errors retried (at most 3 attempts per GET).
    pub(crate) get_retries: AtomicU64,
    /// GETs that still failed after their retries.
    pub(crate) get_errors: AtomicU64,
    /// The most GETs one read had in flight at once (cap 8).
    pub(crate) in_flight_max: AtomicU64,
    /// Keyed history reads refused (unsupported in the spike; a run that
    /// counts one is void).
    pub(crate) keyed_unsupported: AtomicU64,
    /// Default-lane reads (`Some("")`, every product-surface read of an
    /// unkeyed stream), and the pages of other lanes they skipped.
    pub(crate) lane_reads: AtomicU64,
    pub(crate) lane_skipped_pages: AtomicU64,
    /// Merged reads whose tail scan met a hole a newer boundary explained
    /// (`absorption_race` adopted it and the read re-served the gap from the
    /// sealed range): readers that raced a seal's advance and trims.
    pub(crate) race_adoptions: AtomicU64,
    pub(crate) read_ms: Histogram<8>,
}

impl ReadStats {
    pub(crate) fn read_ms(&self, ms: u64) {
        self.read_ms.record(&MS_BOUNDS, ms);
    }

    pub(crate) fn json(&self) -> serde_json::Value {
        let g = SpillStats::get;
        serde_json::json!({
            "calls": g(&self.calls),
            "completed": g(&self.completed),
            "rows": g(&self.rows),
            "gets": g(&self.gets),
            "get_bytes": g(&self.get_bytes),
            "whole_chunks": g(&self.whole_chunks),
            "partial_chunks": g(&self.partial_chunks),
            "pages": g(&self.pages),
            "budget_partials": g(&self.budget_partials),
            "missing_row_partials": g(&self.missing_row_partials),
            "not_found_retryable": g(&self.not_found_retryable),
            "get_retries": g(&self.get_retries),
            "get_errors": g(&self.get_errors),
            "in_flight_max": g(&self.in_flight_max),
            "keyed_unsupported": g(&self.keyed_unsupported),
            "lane_reads": g(&self.lane_reads),
            "lane_skipped_pages": g(&self.lane_skipped_pages),
            "race_adoptions": g(&self.race_adoptions),
            "read_ms": self.read_ms.json(&MS_BOUNDS, "ms"),
        })
    }
}

/// L6 spike M4: keyed history reads refused in this process (all shards).
pub(crate) static KEYED_UNSUPPORTED_TOTAL: AtomicU64 = AtomicU64::new(0);

fn registry() -> &'static Mutex<BTreeMap<String, Arc<SpillStats>>> {
    // mt-lint: allow(name-keyed-map): keyed by shard DB path (L6 spike
    // instrumentation), never by a stream or tenant identity.
    static REGISTRY: OnceLock<Mutex<BTreeMap<String, Arc<SpillStats>>>> = OnceLock::new();
    REGISTRY.get_or_init(|| Mutex::new(BTreeMap::new()))
}

/// The sealer counters of the shard DB at `path`, created on first use and
/// kept across reopens of the same path in this process.
#[expect(clippy::unwrap_used, reason = "L6 spike: throwaway instrumentation")]
pub(crate) fn for_db(path: &str) -> Arc<SpillStats> {
    registry()
        .lock()
        .unwrap()
        .entry(path.to_string())
        .or_default()
        .clone()
}

/// `l6_spill` in `/v1/debug/store`.
#[expect(clippy::unwrap_used, reason = "L6 spike: throwaway instrumentation")]
pub(crate) fn snapshot_json() -> serde_json::Value {
    let reg = registry().lock().unwrap();
    let shards: serde_json::Map<String, serde_json::Value> =
        reg.iter().map(|(p, s)| (p.clone(), s.json())).collect();
    serde_json::json!({
        "shards": shards,
        "keyed_history_reads_unsupported": KEYED_UNSUPPORTED_TOTAL.load(Ordering::Relaxed),
    })
}
