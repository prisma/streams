//! Bounded interval counters emitted through the internal metrics stream.
//! Customer identity is explicit: two tenants may use the same stream name
//! without colliding in billing or noisy-neighbor telemetry.

use std::collections::HashMap;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

#[derive(Default, Clone, serde::Serialize)]
pub struct PerStream {
    pub appends: u64,
    pub append_bytes: u64,
    pub reads: u64,
    pub read_bytes: u64,
    pub queue_ops: u64,
    pub requests: u64,
    pub client_errors: u64,
    pub throttled: u64,
    pub server_errors: u64,
    pub request_duration_us: u64,
    /// Non-cumulative bins: <=50 ms, <=250 ms, <=1 s, <=10 s, >10 s.
    pub request_duration_ms_buckets: [u64; 5],
}

#[derive(Clone, Eq, Hash, PartialEq)]
struct MeterKey {
    customer_id: String,
    stream: String,
}

#[derive(serde::Serialize)]
pub struct MeteredStream {
    pub customer_id: String,
    pub stream: String,
    #[serde(flatten)]
    pub counters: PerStream,
}

pub struct MetricsDrain {
    pub streams: Vec<MeteredStream>,
    /// A non-zero value is an alarm: the process stayed memory-bounded, but
    /// billing cardinality exceeded its configured safety envelope.
    pub dropped_series: u64,
}

impl MetricsDrain {
    pub fn is_empty(&self) -> bool {
        self.streams.is_empty() && self.dropped_series == 0
    }
}

struct MetricsInner {
    counters: HashMap<MeterKey, PerStream>,
    dropped_series: u64,
}

pub struct Metrics {
    inner: Mutex<MetricsInner>,
    capacity: usize,
    export_configured: AtomicBool,
    export_healthy: AtomicBool,
    export_failures: AtomicU64,
    dropped_series_total: AtomicU64,
}

const DEFAULT_METRICS_CAPACITY: usize = 100_000;

impl Default for Metrics {
    fn default() -> Self {
        Self::with_capacity(DEFAULT_METRICS_CAPACITY)
    }
}

impl Metrics {
    fn with_capacity(capacity: usize) -> Self {
        Self {
            inner: Mutex::new(MetricsInner {
                counters: HashMap::new(),
                dropped_series: 0,
            }),
            capacity: capacity.max(1),
            export_configured: AtomicBool::new(false),
            export_healthy: AtomicBool::new(true),
            export_failures: AtomicU64::new(0),
            dropped_series_total: AtomicU64::new(0),
        }
    }

    fn record(&self, customer_id: &str, stream: &str, apply: impl FnOnce(&mut PerStream)) {
        let mut inner = self.inner.lock().unwrap();
        let key = MeterKey {
            customer_id: customer_id.to_string(),
            stream: stream.to_string(),
        };
        if let Some(counters) = inner.counters.get_mut(&key) {
            apply(counters);
            return;
        }
        if inner.counters.len() >= self.capacity {
            inner.dropped_series = inner.dropped_series.saturating_add(1);
            self.dropped_series_total.fetch_add(1, Ordering::Relaxed);
            return;
        }
        let mut counters = PerStream::default();
        apply(&mut counters);
        inner.counters.insert(key, counters);
    }

    pub fn append(&self, customer_id: &str, stream: &str, bytes: u64) {
        self.record(customer_id, stream, |counters| {
            counters.appends = counters.appends.saturating_add(1);
            counters.append_bytes = counters.append_bytes.saturating_add(bytes);
        });
    }

    pub fn read(&self, customer_id: &str, stream: &str, bytes: u64) {
        self.record(customer_id, stream, |counters| {
            counters.reads = counters.reads.saturating_add(1);
            counters.read_bytes = counters.read_bytes.saturating_add(bytes);
        });
    }

    pub fn queue(&self, customer_id: &str, stream: &str) {
        self.record(customer_id, stream, |counters| {
            counters.queue_ops = counters.queue_ops.saturating_add(1);
        });
    }

    pub fn request(
        &self,
        customer_id: &str,
        stream: &str,
        status: axum::http::StatusCode,
        elapsed: std::time::Duration,
    ) {
        self.record(customer_id, stream, |counters| {
            counters.requests = counters.requests.saturating_add(1);
            if status == axum::http::StatusCode::TOO_MANY_REQUESTS {
                counters.throttled = counters.throttled.saturating_add(1);
            } else if status.is_client_error() {
                counters.client_errors = counters.client_errors.saturating_add(1);
            } else if status.is_server_error() {
                counters.server_errors = counters.server_errors.saturating_add(1);
            }
            counters.request_duration_us = counters
                .request_duration_us
                .saturating_add(elapsed.as_micros().min(u64::MAX as u128) as u64);
            let millis = elapsed.as_millis().min(u64::MAX as u128) as u64;
            let bucket = [50, 250, 1_000, 10_000]
                .iter()
                .position(|limit| millis <= *limit)
                .unwrap_or(4);
            counters.request_duration_ms_buckets[bucket] =
                counters.request_duration_ms_buckets[bucket].saturating_add(1);
        });
    }

    pub fn drain(&self) -> MetricsDrain {
        let mut inner = self.inner.lock().unwrap();
        let counters = std::mem::take(&mut inner.counters);
        let dropped_series = std::mem::take(&mut inner.dropped_series);
        let streams = counters
            .into_iter()
            .map(|(key, counters)| MeteredStream {
                customer_id: key.customer_id,
                stream: key.stream,
                counters,
            })
            .collect();
        MetricsDrain {
            streams,
            dropped_series,
        }
    }

    pub fn configure_export(&self) {
        self.export_configured.store(true, Ordering::Release);
    }

    pub fn record_export_result(&self, success: bool) {
        self.export_healthy.store(success, Ordering::Release);
        if !success {
            self.export_failures.fetch_add(1, Ordering::Relaxed);
        }
    }

    pub fn export_configured(&self) -> bool {
        self.export_configured.load(Ordering::Acquire)
    }

    pub fn export_healthy(&self) -> bool {
        self.export_healthy.load(Ordering::Acquire)
    }

    pub fn export_failures(&self) -> u64 {
        self.export_failures.load(Ordering::Relaxed)
    }

    pub fn dropped_series_total(&self) -> u64 {
        self.dropped_series_total.load(Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tenant_names_do_not_collide_and_cardinality_is_bounded() {
        let metrics = Metrics::with_capacity(2);
        metrics.append("a", "orders", 10);
        metrics.append("b", "orders", 20);
        metrics.append("c", "orders", 30);
        let drained = metrics.drain();
        assert_eq!(drained.streams.len(), 2);
        assert_eq!(drained.dropped_series, 1);
        assert!(
            drained
                .streams
                .iter()
                .any(|series| series.customer_id == "a" && series.counters.append_bytes == 10)
        );
        assert!(
            drained
                .streams
                .iter()
                .any(|series| series.customer_id == "b" && series.counters.append_bytes == 20)
        );
    }

    #[test]
    fn tenant_red_metrics_distinguish_throttling_from_service_errors() {
        let metrics = Metrics::with_capacity(2);
        metrics.request(
            "a",
            "orders",
            axum::http::StatusCode::TOO_MANY_REQUESTS,
            std::time::Duration::from_millis(60),
        );
        metrics.request(
            "a",
            "orders",
            axum::http::StatusCode::SERVICE_UNAVAILABLE,
            std::time::Duration::from_millis(300),
        );
        let drained = metrics.drain();
        let counters = &drained.streams[0].counters;
        assert_eq!(counters.requests, 2);
        assert_eq!(counters.throttled, 1);
        assert_eq!(counters.server_errors, 1);
        assert_eq!(counters.request_duration_ms_buckets, [0, 1, 1, 0, 0]);
    }
}
