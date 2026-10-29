//! Each shard log's SlateDB metrics (layout 6): the sealer's memtable
//! trigger counts in SlateDB's own units (`spill::estimate`), from the
//! counters SlateDB records while it writes. The shard log builder
//! (`keyspace::db_builder`) gives every shard log one recorder, kept here
//! by the log's path for the life of the process, so a reopen of the same
//! path records into the same recorder and its counters keep counting
//! (the sealer reads them as deltas). A path no builder built has none.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, OnceLock};

use slatedb_common::metrics::{DefaultMetricsRecorder, MetricValue, MetricsRecorder};

/// One shard log's recorder.
pub(crate) struct ShardLogMetrics {
    recorder: Arc<DefaultMetricsRecorder>,
}

impl ShardLogMetrics {
    /// The recorder the shard log's builder takes.
    pub(crate) fn recorder(&self) -> Arc<dyn MetricsRecorder> {
        self.recorder.clone()
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
