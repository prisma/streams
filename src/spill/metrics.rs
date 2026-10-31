//! Each SlateDB's metrics, by path: the sealer's memtable trigger counts in
//! SlateDB's own units (`spill::estimate`), from the counters SlateDB
//! records while it writes, and every database's request attribution
//! (`attribution`, served under each shard's `seal` in
//! `GET /v1/debug/timings`). The shard log builder (`keyspace::db_builder`)
//! and the telemetry databases (the usage rollup, the read spool) each get
//! one recorder, kept here by the database's path for the life of the
//! process, so a reopen of the same path records into the same recorder and
//! its counters keep counting (the sealer reads them as deltas). A path no
//! builder built has none.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, OnceLock};

use slatedb_common::metrics::{DefaultMetricsRecorder, MetricValue, MetricsRecorder};

/// The gauge holding the shard log's memtable freeze size.
const FREEZE_BYTES: &str = "prisma_streams.shard_log.freeze_bytes";

/// One shard log's recorder.
pub(crate) struct ShardLogMetrics {
    recorder: Arc<DefaultMetricsRecorder>,
}

impl ShardLogMetrics {
    /// The recorder the shard log's builder takes.
    pub(crate) fn recorder(&self) -> Arc<dyn MetricsRecorder> {
        self.recorder.clone()
    }

    /// Record the size at which this shard log's SlateDB freezes its
    /// memtable by itself (`L0_SST_SIZE_BYTES`), for the sealer's memtable
    /// trigger (`seal::memtable_trigger`); the shard log builder records it.
    pub(crate) fn note_freeze_bytes(&self, bytes: usize) {
        let gauge =
            self.recorder
                .register_gauge(FREEZE_BYTES, "the memtable size SlateDB freezes at", &[]);
        gauge.set(i64::try_from(bytes).unwrap_or(i64::MAX));
    }

    /// The freeze size the builder recorded, if it did.
    pub(crate) fn freeze_bytes(&self) -> Option<usize> {
        self.recorder
            .snapshot()
            .by_name(FREEZE_BYTES)
            .iter()
            .find_map(|m| match m.value {
                MetricValue::Gauge(v) => usize::try_from(v).ok(),
                _ => None,
            })
    }

    /// Attribute this database's reads until the returned guard drops to
    /// `site` (S-gap step 0: telemetry SST GETs per call site).
    pub(crate) fn call_site(self: &Arc<Self>, site: &'static str) -> CallSite {
        self.recorder
            .register_up_down_counter(CALL_SITES_ACTIVE, "call sites running", &[])
            .increment(1);
        self.recorder
            .register_counter(CALL_SITES_STARTED, "call sites started", &[])
            .increment(1);
        CallSite {
            metrics: self.clone(),
            site,
            gets: self.reader_gets(),
            started: self.total(CALL_SITES_STARTED),
            alone: self.total(CALL_SITES_ACTIVE) == 1,
        }
    }

    /// The reader's GETs of the database's main store: SlateDB's object-store
    /// request counter with `component=db`, `store_type=main`, `api=get`
    /// (SST blocks, indexes and filters, read by range, and the manifest a
    /// read may refresh; HEADs and LISTs are not reads of data).
    pub(crate) fn reader_gets(&self) -> u64 {
        let wanted = [
            ("component", "db"),
            ("store_type", "main"),
            ("op", "get"),
            ("api", "get"),
        ];
        self.recorder
            .snapshot()
            .by_name("slatedb.object_store.request_count")
            .iter()
            .filter(|m| {
                wanted
                    .iter()
                    .all(|(k, v)| m.labels.iter().any(|(lk, lv)| lk == k && lv == v))
            })
            .map(|m| match m.value {
                MetricValue::Counter(v) => v,
                _ => 0,
            })
            .sum()
    }

    /// Tests: a call site's (calls, reader GETs) so far.
    #[cfg(test)]
    pub(crate) fn call_site_counts(&self, site: &str) -> (u64, u64) {
        let of = |metric: &str| {
            self.recorder
                .snapshot()
                .by_name(metric)
                .iter()
                .filter(|m| m.labels.iter().any(|(k, v)| k == "site" && v == site))
                .map(|m| match m.value {
                    MetricValue::Counter(v) => v,
                    _ => 0,
                })
                .sum()
        };
        (of(CALL_SITE_CALLS), of(CALL_SITE_GETS))
    }

    /// A counter's or up-down counter's value now, summed over its labels.
    fn total(&self, metric: &str) -> u64 {
        self.recorder
            .snapshot()
            .by_name(metric)
            .iter()
            .map(|m| match m.value {
                MetricValue::Counter(v) => v,
                MetricValue::UpDownCounter(v) => v.unsigned_abs(),
                _ => 0,
            })
            .sum()
    }

    /// A counter's value now (0 before SlateDB registers it).
    pub(crate) fn counter(&self, metric: &str) -> u64 {
        self.recorder
            .snapshot()
            .by_name(metric)
            .iter()
            .map(|m| match m.value {
                MetricValue::Counter(v) => v,
                _ => 0,
            })
            .sum()
    }
}

/// The counters `attribution` reports per database: its object-store
/// requests by component and API, its reads by kind, its block cache.
const ATTRIBUTED: [&str; 6] = [
    "slatedb.object_store.request_count",
    "slatedb.db.request_count",
    "slatedb.db_cache.access_count",
    "slatedb.db.sst_count",
    CALL_SITE_GETS,
    CALL_SITE_CALLS,
];

/// A call site's share of its database's reader GETs, by `site` (`overlap`
/// for calls that ran beside another call site of the same database).
const CALL_SITE_GETS: &str = "prisma_streams.call_site.sst_gets";
/// A call site's guarded calls, by `site` as `CALL_SITE_GETS`: a rollup
/// step counts its cursor read and its page apply, the outbox each of its
/// two pending scans.
const CALL_SITE_CALLS: &str = "prisma_streams.call_site.calls";
/// Call sites of the database running now, and started since boot.
const CALL_SITES_ACTIVE: &str = "prisma_streams.call_site.active";
const CALL_SITES_STARTED: &str = "prisma_streams.call_site.started";

/// One call of a database's attributed call site (`ShardLogMetrics::call_site`):
/// when it drops, the reader GETs of the database's main store since it
/// began go to its site, or to `overlap` when another call site of the same
/// database ran meanwhile. The compactor's and the collector's GETs carry
/// their own component and never count.
pub(crate) struct CallSite {
    metrics: Arc<ShardLogMetrics>,
    site: &'static str,
    gets: u64,
    started: u64,
    alone: bool,
}

impl Drop for CallSite {
    fn drop(&mut self) {
        let m = &self.metrics;
        let gets = m.reader_gets().saturating_sub(self.gets);
        let alone = self.alone && m.total(CALL_SITES_STARTED) == self.started;
        let site = if alone { self.site } else { "overlap" };
        let labels = [("site", site)];
        let described = "a call site's reader GETs of its database's main store";
        m.recorder
            .register_counter(CALL_SITE_GETS, described, &labels)
            .increment(gets);
        m.recorder
            .register_counter(CALL_SITE_CALLS, "a call site's calls", &labels)
            .increment(1);
        m.recorder
            .register_up_down_counter(CALL_SITES_ACTIVE, "call sites running", &[])
            .increment(-1);
    }
}

/// Every database's request attribution, by path: each attributed counter
/// as `name{labels}` -> value, zeros left out (step 0 of the S-gap plan).
pub(crate) fn attribution() -> serde_json::Value {
    let logs: Vec<(String, Arc<ShardLogMetrics>)> = registry()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .iter()
        .map(|(path, m)| (path.clone(), m.clone()))
        .collect();
    let mut out = serde_json::Map::new();
    for (path, m) in logs {
        let mut counters = serde_json::Map::new();
        for metric in m.recorder.snapshot().all() {
            if !ATTRIBUTED.contains(&metric.name.as_str()) {
                continue;
            }
            let value = match metric.value {
                MetricValue::Counter(v) => v,
                MetricValue::Gauge(v) | MetricValue::UpDownCounter(v) => v.unsigned_abs(),
                _ => continue,
            };
            if value == 0 {
                continue;
            }
            let labels: Vec<String> = metric
                .labels
                .iter()
                .map(|(k, v)| format!("{k}={v}"))
                .collect();
            counters.insert(
                format!("{}{{{}}}", metric.name, labels.join(",")),
                value.into(),
            );
        }
        out.insert(path, serde_json::Value::Object(counters));
    }
    serde_json::Value::Object(out)
}

// mt-lint: allow(name-keyed-map): keyed by shard log path, never by a stream or tenant identity
type Registry = Mutex<BTreeMap<String, Arc<ShardLogMetrics>>>;

fn registry() -> &'static Registry {
    static REGISTRY: OnceLock<Registry> = OnceLock::new();
    REGISTRY.get_or_init(Registry::default)
}

/// The metrics of the shard log at `path`, made on its first build.
#[expect(
    clippy::unwrap_used,
    reason = "spill::metrics::for_log; a poisoned registry may hold a half-inserted recorder; recovering it could give two builds of one path two recorders and the sealer counters that never move"
)]
pub(crate) fn for_log(path: &str) -> Arc<ShardLogMetrics> {
    registry()
        .lock()
        .unwrap()
        .entry(path.to_string())
        .or_insert_with(|| {
            Arc::new(ShardLogMetrics {
                recorder: Arc::new(DefaultMetricsRecorder::new()),
            })
        })
        .clone()
}

/// The metrics of the shard log at `path`, if a shard log builder built it.
#[expect(
    clippy::unwrap_used,
    reason = "spill::metrics::of_log; a poisoned registry may hold a half-inserted recorder; recovering it could give two builds of one path two recorders and the sealer counters that never move"
)]
pub(crate) fn of_log(path: &str) -> Option<Arc<ShardLogMetrics>> {
    registry().lock().unwrap().get(path).cloned()
}
