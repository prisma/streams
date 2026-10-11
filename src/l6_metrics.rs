//! L6 seal spike, milestone M1: read-only SlateDB metrics per shard DB.
//!
//! Throwaway instrumentation on the never-merged `l6-spike` branch
//! (`~/.streams-k2/analysis/public-beta/l6/SPIKE-PLAN.md` §3 M1). Each shard
//! DB the bootstrap opener builds gets one `DefaultMetricsRecorder`, kept
//! here by the DB's path for the life of the process (a reopen of the same
//! path re-registers into the same recorder, so its counters keep counting),
//! plus the spike's own counters around the 5 s memtable ticker. `GET
//! /v1/debug/store` reports them as `l6_slatedb`.
//!
//! Nothing here changes behaviour: SlateDB records into a no-op recorder
//! without it.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

use slatedb::db_stats;
use slatedb_common::metrics::{DefaultMetricsRecorder, MetricValue, MetricsRecorder};

/// One shard DB's recorder and the spike's counters beside it.
pub(crate) struct ShardDbMetrics {
    recorder: Arc<DefaultMetricsRecorder>,
    /// Opens of this path in this process.
    opens: AtomicU64,
    /// Forced memtable flushes the 5 s ticker issued (calls).
    ticker_flushes: AtomicU64,
    /// ...of which returned an error.
    ticker_flush_errors: AtomicU64,
    /// Sum over ticker calls of the `immutable_memtable_flushes` delta read
    /// immediately before and after the call (review A1: attribution by
    /// delta, never by subtraction).
    ticker_flush_attributed: AtomicU64,
}

/// The five metrics M1 names, reported at the top of each shard's entry.
const NAMED: [&str; 5] = [
    db_stats::IMMUTABLE_MEMTABLE_FLUSHES,
    db_stats::L0_FLUSH_BYTES,
    db_stats::MEMTABLE_WRITE_BYTES,
    db_stats::TOTAL_MEM_SIZE_BYTES,
    db_stats::BACKPRESSURE_COUNT,
];

fn registry() -> &'static Mutex<BTreeMap<String, Arc<ShardDbMetrics>>> {
    static REGISTRY: OnceLock<Mutex<BTreeMap<String, Arc<ShardDbMetrics>>>> = OnceLock::new();
    REGISTRY.get_or_init(|| Mutex::new(BTreeMap::new()))
}

/// The metrics of the shard DB at `path`, created on its first open.
#[expect(clippy::unwrap_used, reason = "L6 spike: throwaway instrumentation")]
pub(crate) fn for_db(path: &str) -> Arc<ShardDbMetrics> {
    let mut reg = registry().lock().unwrap();
    let entry = reg
        .entry(path.to_string())
        .or_insert_with(|| {
            Arc::new(ShardDbMetrics {
                recorder: Arc::new(DefaultMetricsRecorder::new()),
                opens: AtomicU64::new(0),
                ticker_flushes: AtomicU64::new(0),
                ticker_flush_errors: AtomicU64::new(0),
                ticker_flush_attributed: AtomicU64::new(0),
            })
        })
        .clone();
    entry.opens.fetch_add(1, Ordering::Relaxed);
    entry
}

/// The metrics of the shard DB at `path`, if the bootstrap opener built it
/// (engines a test builds directly have none).
#[expect(clippy::unwrap_used, reason = "L6 spike: throwaway instrumentation")]
pub(crate) fn get(path: &str) -> Option<Arc<ShardDbMetrics>> {
    registry().lock().unwrap().get(path).cloned()
}

impl ShardDbMetrics {
    /// The recorder to hand `DbBuilder::with_metrics_recorder`.
    pub(crate) fn recorder(&self) -> Arc<dyn MetricsRecorder> {
        self.recorder.clone()
    }

    /// A counter's current value (0 when not registered yet).
    pub(crate) fn counter(&self, name: &str) -> u64 {
        self.recorder
            .snapshot()
            .by_name(name)
            .iter()
            .map(|m| match m.value {
                MetricValue::Counter(v) => v,
                _ => 0,
            })
            .sum()
    }

    /// Book one ticker flush: `before` and `after` are
    /// `immutable_memtable_flushes` read around the call.
    pub(crate) fn ticker_flush(&self, before: u64, after: u64, failed: bool) {
        self.ticker_flushes.fetch_add(1, Ordering::Relaxed);
        if failed {
            self.ticker_flush_errors.fetch_add(1, Ordering::Relaxed);
        }
        self.ticker_flush_attributed
            .fetch_add(after.saturating_sub(before), Ordering::Relaxed);
    }

    fn json(&self) -> serde_json::Value {
        let snap = self.recorder.snapshot();
        let mut all = serde_json::Map::new();
        for m in snap.all() {
            let mut key = m.name.clone();
            if !m.labels.is_empty() {
                let labels: Vec<String> =
                    m.labels.iter().map(|(k, v)| format!("{k}={v}")).collect();
                key = format!("{key}{{{}}}", labels.join(","));
            }
            let value = match &m.value {
                MetricValue::Counter(v) => serde_json::json!(v),
                MetricValue::Gauge(v) | MetricValue::UpDownCounter(v) => serde_json::json!(v),
                MetricValue::Histogram {
                    count, sum, max, ..
                } => serde_json::json!({"count": count, "sum": sum, "max": max}),
            };
            all.insert(key, value);
        }
        let mut out = serde_json::Map::new();
        for name in NAMED {
            let short = name.trim_start_matches("slatedb.db.");
            let v: i64 = snap
                .by_name(name)
                .iter()
                .map(|m| match m.value {
                    MetricValue::Counter(v) => v as i64,
                    MetricValue::Gauge(v) | MetricValue::UpDownCounter(v) => v,
                    MetricValue::Histogram { .. } => 0,
                })
                .sum();
            out.insert(short.to_string(), serde_json::json!(v));
        }
        out.insert(
            "opens".into(),
            self.opens.load(Ordering::Relaxed).into(),
        );
        out.insert(
            "ticker_flushes".into(),
            self.ticker_flushes.load(Ordering::Relaxed).into(),
        );
        out.insert(
            "ticker_flush_errors".into(),
            self.ticker_flush_errors.load(Ordering::Relaxed).into(),
        );
        out.insert(
            "ticker_flush_attributed".into(),
            self.ticker_flush_attributed.load(Ordering::Relaxed).into(),
        );
        out.insert("all".into(), all.into());
        out.into()
    }
}

/// `l6_slatedb` in `/v1/debug/store`: every shard DB opened in this process.
#[expect(clippy::unwrap_used, reason = "L6 spike: throwaway instrumentation")]
pub(crate) fn snapshot_json() -> serde_json::Value {
    let reg = registry().lock().unwrap();
    let shards: serde_json::Map<String, serde_json::Value> =
        reg.iter().map(|(path, m)| (path.clone(), m.json())).collect();
    serde_json::json!({ "shards": shards })
}

#[cfg(test)]
mod tests {
    use super::*;
    use slatedb::config::{FlushOptions, FlushType};

    /// The recorder handed to the builder sees the memtable flush the
    /// ticker forces: one immutable memtable, its L0 bytes, the write bytes.
    #[tokio::test]
    async fn a_forced_memtable_flush_is_recorded_per_db_path() {
        let store: Arc<dyn object_store::ObjectStore> =
            Arc::new(object_store::memory::InMemory::new());
        let path = "l6-metrics-test/shards/00";
        let m = for_db(path);
        let db = slatedb::Db::builder(path, store)
            .with_metrics_recorder(m.recorder())
            .build()
            .await
            .unwrap();
        db.put(b"k1", b"value-one").await.unwrap();
        let before = m.counter(db_stats::IMMUTABLE_MEMTABLE_FLUSHES);
        db.flush_with_options(FlushOptions {
            flush_type: FlushType::MemTable,
        })
        .await
        .unwrap();
        let after = m.counter(db_stats::IMMUTABLE_MEMTABLE_FLUSHES);
        m.ticker_flush(before, after, false);
        assert_eq!(after - before, 1);
        let snap = snapshot_json();
        let entry = &snap["shards"][path];
        assert_eq!(entry["immutable_memtable_flushes"], serde_json::json!(1));
        assert_eq!(entry["ticker_flushes"], serde_json::json!(1));
        assert_eq!(entry["ticker_flush_attributed"], serde_json::json!(1));
        assert!(entry["l0_flush_bytes"].as_i64().unwrap() > 0, "{entry}");
        assert!(entry["memtable_write_bytes"].as_i64().unwrap() > 0, "{entry}");
        assert!(get(path).is_some());
        db.close().await.unwrap();
    }
}
