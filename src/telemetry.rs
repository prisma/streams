//! Bounded process-local RED telemetry exported as OpenMetrics.
//!
//! Labels are deliberately finite: operation and status class are enums, and
//! the only dynamic series is the already-bounded set of open shard prefixes.
//! Customer/stream billing remains in the encrypted internal metrics stream;
//! putting arbitrary tenant names into a scrape target would make a hostile
//! tenant a monitoring-cardinality attack.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use axum::http::{Method, StatusCode, Uri};

const OPERATIONS: usize = 5;
const STATUS_CLASSES: usize = 5;
const LATENCY_BUCKETS_MS: [u64; 11] = [5, 10, 25, 50, 100, 250, 500, 1_000, 2_500, 5_000, 10_000];
const TAIL_FRESHNESS_BUCKETS_MS: [u64; 9] = [10, 25, 50, 100, 250, 500, 1_000, 2_500, 5_000];
const FENCE_KINDS: usize = 2;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FenceKind {
    Writer,
    Reconfiguration,
}

impl FenceKind {
    fn index(self) -> usize {
        match self {
            Self::Writer => 0,
            Self::Reconfiguration => 1,
        }
    }

    fn labels() -> [&'static str; FENCE_KINDS] {
        ["writer", "reconfiguration"]
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Operation {
    Append,
    Read,
    Control,
    Queue,
    Other,
}

impl Operation {
    fn index(self) -> usize {
        match self {
            Self::Append => 0,
            Self::Read => 1,
            Self::Control => 2,
            Self::Queue => 3,
            Self::Other => 4,
        }
    }

    fn label(self) -> &'static str {
        match self {
            Self::Append => "append",
            Self::Read => "read",
            Self::Control => "control",
            Self::Queue => "queue",
            Self::Other => "other",
        }
    }

    fn all() -> [Self; OPERATIONS] {
        [
            Self::Append,
            Self::Read,
            Self::Control,
            Self::Queue,
            Self::Other,
        ]
    }
}

#[derive(Clone, Copy)]
enum StatusClass {
    Success,
    Redirect,
    ClientError,
    Throttled,
    ServerError,
}

impl StatusClass {
    fn from_status(status: StatusCode) -> Self {
        if status == StatusCode::TOO_MANY_REQUESTS {
            Self::Throttled
        } else if status.is_success() {
            Self::Success
        } else if status.is_redirection() {
            Self::Redirect
        } else if status.is_client_error() {
            Self::ClientError
        } else {
            Self::ServerError
        }
    }

    fn index(self) -> usize {
        match self {
            Self::Success => 0,
            Self::Redirect => 1,
            Self::ClientError => 2,
            Self::Throttled => 3,
            Self::ServerError => 4,
        }
    }

    fn labels() -> [&'static str; STATUS_CLASSES] {
        ["2xx", "3xx", "4xx", "429", "5xx"]
    }
}

struct OperationMetrics {
    responses: [AtomicU64; STATUS_CLASSES],
    latency_buckets: [AtomicU64; LATENCY_BUCKETS_MS.len() + 1],
    duration_us: AtomicU64,
}

impl OperationMetrics {
    fn new() -> Self {
        Self {
            responses: std::array::from_fn(|_| AtomicU64::new(0)),
            latency_buckets: std::array::from_fn(|_| AtomicU64::new(0)),
            duration_us: AtomicU64::new(0),
        }
    }
}

pub struct Telemetry {
    operations: [OperationMetrics; OPERATIONS],
    tail_freshness_buckets: [AtomicU64; TAIL_FRESHNESS_BUCKETS_MS.len() + 1],
    tail_freshness_duration_us: AtomicU64,
    absorber_pending_bytes: AtomicU64,
    absorber_healthy: AtomicBool,
    fence_events: [AtomicU64; FENCE_KINDS],
}

impl Default for Telemetry {
    fn default() -> Self {
        Self {
            operations: std::array::from_fn(|_| OperationMetrics::new()),
            tail_freshness_buckets: std::array::from_fn(|_| AtomicU64::new(0)),
            tail_freshness_duration_us: AtomicU64::new(0),
            absorber_pending_bytes: AtomicU64::new(0),
            absorber_healthy: AtomicBool::new(true),
            fence_events: std::array::from_fn(|_| AtomicU64::new(0)),
        }
    }
}

impl Telemetry {
    pub fn classify(method: &Method, uri: &Uri) -> Operation {
        let path = uri.path();
        if !path.starts_with("/v1/stream/") {
            return Operation::Other;
        }
        if path.contains("/queue/") {
            return Operation::Queue;
        }
        match *method {
            Method::GET | Method::HEAD => Operation::Read,
            Method::PUT | Method::DELETE => Operation::Control,
            Method::POST => Operation::Append,
            _ => Operation::Other,
        }
    }

    pub fn record(&self, operation: Operation, status: StatusCode, elapsed: Duration) {
        let metrics = &self.operations[operation.index()];
        metrics.responses[StatusClass::from_status(status).index()].fetch_add(1, Ordering::Relaxed);
        let millis = elapsed.as_millis().min(u64::MAX as u128) as u64;
        let bucket = LATENCY_BUCKETS_MS
            .iter()
            .position(|limit| millis <= *limit)
            .unwrap_or(LATENCY_BUCKETS_MS.len());
        metrics.latency_buckets[bucket].fetch_add(1, Ordering::Relaxed);
        metrics.duration_us.fetch_add(
            elapsed.as_micros().min(u64::MAX as u128) as u64,
            Ordering::Relaxed,
        );
    }

    pub fn record_tail_freshness(&self, elapsed: Duration) {
        let millis = elapsed.as_millis().min(u64::MAX as u128) as u64;
        let bucket = TAIL_FRESHNESS_BUCKETS_MS
            .iter()
            .position(|limit| millis <= *limit)
            .unwrap_or(TAIL_FRESHNESS_BUCKETS_MS.len());
        self.tail_freshness_buckets[bucket].fetch_add(1, Ordering::Relaxed);
        self.tail_freshness_duration_us.fetch_add(
            elapsed.as_micros().min(u64::MAX as u128) as u64,
            Ordering::Relaxed,
        );
    }

    pub fn add_absorber_pending_bytes(&self, bytes: u64) {
        let _ = self.absorber_pending_bytes.fetch_update(
            Ordering::Relaxed,
            Ordering::Relaxed,
            |current| Some(current.saturating_add(bytes)),
        );
    }

    pub fn remove_absorber_pending_bytes(&self, bytes: u64) {
        let _ = self.absorber_pending_bytes.fetch_update(
            Ordering::Relaxed,
            Ordering::Relaxed,
            |current| Some(current.saturating_sub(bytes)),
        );
    }

    pub fn absorber_healthy(&self) -> bool {
        self.absorber_healthy.load(Ordering::Acquire)
    }

    pub fn mark_absorber_unhealthy(&self) {
        self.absorber_healthy.store(false, Ordering::Release);
    }

    pub fn record_fence(&self, kind: FenceKind) {
        self.fence_events[kind.index()].fetch_add(1, Ordering::Relaxed);
    }

    pub fn render_openmetrics(&self, out: &mut String) {
        out.push_str("# HELP streams_http_requests_total HTTP responses by bounded operation and status class.\n");
        out.push_str("# TYPE streams_http_requests_total counter\n");
        for operation in Operation::all() {
            let metrics = &self.operations[operation.index()];
            for (index, status) in StatusClass::labels().iter().enumerate() {
                out.push_str(&format!(
                    "streams_http_requests_total{{operation=\"{}\",status_class=\"{}\"}} {}\n",
                    operation.label(),
                    status,
                    metrics.responses[index].load(Ordering::Relaxed)
                ));
            }
        }
        out.push_str(
            "# HELP streams_http_request_duration_seconds End-to-end HTTP response latency.\n",
        );
        out.push_str("# TYPE streams_http_request_duration_seconds histogram\n");
        for operation in Operation::all() {
            let metrics = &self.operations[operation.index()];
            let mut cumulative = 0u64;
            for (index, limit_ms) in LATENCY_BUCKETS_MS.iter().enumerate() {
                cumulative = cumulative
                    .saturating_add(metrics.latency_buckets[index].load(Ordering::Relaxed));
                out.push_str(&format!(
                    "streams_http_request_duration_seconds_bucket{{operation=\"{}\",le=\"{}\"}} {}\n",
                    operation.label(),
                    *limit_ms as f64 / 1_000.0,
                    cumulative
                ));
            }
            cumulative = cumulative.saturating_add(
                metrics.latency_buckets[LATENCY_BUCKETS_MS.len()].load(Ordering::Relaxed),
            );
            out.push_str(&format!(
                "streams_http_request_duration_seconds_bucket{{operation=\"{}\",le=\"+Inf\"}} {}\n",
                operation.label(),
                cumulative
            ));
            out.push_str(&format!(
                "streams_http_request_duration_seconds_sum{{operation=\"{}\"}} {:.6}\n",
                operation.label(),
                metrics.duration_us.load(Ordering::Relaxed) as f64 / 1_000_000.0
            ));
            out.push_str(&format!(
                "streams_http_request_duration_seconds_count{{operation=\"{}\"}} {}\n",
                operation.label(),
                cumulative
            ));
        }
        out.push_str("# HELP streams_tail_freshness_seconds Durable append visibility to an active tail response.\n");
        out.push_str("# TYPE streams_tail_freshness_seconds histogram\n");
        let mut cumulative = 0u64;
        for (index, limit_ms) in TAIL_FRESHNESS_BUCKETS_MS.iter().enumerate() {
            cumulative = cumulative
                .saturating_add(self.tail_freshness_buckets[index].load(Ordering::Relaxed));
            out.push_str(&format!(
                "streams_tail_freshness_seconds_bucket{{le=\"{}\"}} {}\n",
                *limit_ms as f64 / 1_000.0,
                cumulative
            ));
        }
        cumulative = cumulative.saturating_add(
            self.tail_freshness_buckets[TAIL_FRESHNESS_BUCKETS_MS.len()].load(Ordering::Relaxed),
        );
        out.push_str(&format!(
            "streams_tail_freshness_seconds_bucket{{le=\"+Inf\"}} {cumulative}\n"
        ));
        out.push_str(&format!(
            "streams_tail_freshness_seconds_sum {:.6}\n",
            self.tail_freshness_duration_us.load(Ordering::Relaxed) as f64 / 1_000_000.0
        ));
        out.push_str(&format!(
            "streams_tail_freshness_seconds_count {cumulative}\n"
        ));
        out.push_str("# HELP streams_absorber_pending_bytes Plaintext payload bytes awaiting durable history absorption.\n");
        out.push_str("# TYPE streams_absorber_pending_bytes gauge\n");
        out.push_str(&format!(
            "streams_absorber_pending_bytes {}\n",
            self.absorber_pending_bytes.load(Ordering::Relaxed)
        ));
        out.push_str("# HELP streams_fence_events_total Shard ownership or reconfiguration fences observed by this process.\n");
        out.push_str("# TYPE streams_fence_events_total counter\n");
        for (index, kind) in FenceKind::labels().iter().enumerate() {
            out.push_str(&format!(
                "streams_fence_events_total{{kind=\"{kind}\"}} {}\n",
                self.fence_events[index].load(Ordering::Relaxed)
            ));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn operation_and_status_labels_are_finite_and_throttling_is_distinct() {
        let telemetry = Telemetry::default();
        let append =
            Telemetry::classify(&Method::POST, &"/v1/stream/orders".parse::<Uri>().unwrap());
        assert_eq!(append, Operation::Append);
        telemetry.record(
            append,
            StatusCode::TOO_MANY_REQUESTS,
            Duration::from_millis(7),
        );
        telemetry.record(
            append,
            StatusCode::SERVICE_UNAVAILABLE,
            Duration::from_millis(12),
        );

        let mut rendered = String::new();
        telemetry.render_openmetrics(&mut rendered);
        assert!(
            rendered.contains(
                "streams_http_requests_total{operation=\"append\",status_class=\"429\"} 1"
            )
        );
        assert!(
            rendered.contains(
                "streams_http_requests_total{operation=\"append\",status_class=\"5xx\"} 1"
            )
        );
        assert!(rendered.contains(
            "streams_http_request_duration_seconds_bucket{operation=\"append\",le=\"0.01\"} 1"
        ));
        assert!(
            rendered
                .contains("streams_http_request_duration_seconds_count{operation=\"append\"} 2")
        );
        telemetry.record_tail_freshness(Duration::from_millis(60));
        telemetry.add_absorber_pending_bytes(10);
        telemetry.remove_absorber_pending_bytes(3);
        telemetry.record_fence(FenceKind::Writer);
        assert!(telemetry.absorber_healthy());
        telemetry.mark_absorber_unhealthy();
        assert!(!telemetry.absorber_healthy());
        let mut rendered = String::new();
        telemetry.render_openmetrics(&mut rendered);
        assert!(rendered.contains("streams_tail_freshness_seconds_count 1"));
        assert!(rendered.contains("streams_absorber_pending_bytes 7"));
        assert!(rendered.contains("streams_fence_events_total{kind=\"writer\"} 1"));
    }
}
