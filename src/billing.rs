//! Billing, usage, and telemetry core (docs/OBSERVABILITY-BILLING.md).
//!
//! Four planes, each with a single job:
//!   1. data-plane billing state  — exact per-segment ingest/storage
//!      accumulators updated in the SAME shard WriteBatch as the
//!      records they describe (shard.rs committer);
//!   2. the `_usage` ledger       — an internal total-order stream of
//!      idempotent usage observations;
//!   3. the usage rollup          — one SlateDB materialization; the
//!      customer dashboard is a point read, never a ledger scan;
//!   4. operational telemetry     — `_ops_events` (typed, durable,
//!      deterministic IDs) and `_ops_metrics` (mergeable series).
//!
//! This module owns the shared vocabulary: identities, month math,
//! reserved names, schema types, and the source/boot identity. The
//! moving parts live next to what they meter (committer hooks in
//! shard.rs, the read meter at the public response coordinator, the
//! rollup in rollup.rs).
//!
//! Design rule this module enforces everywhere: **a stream name is
//! display metadata, not the billing identity** — billing keys are
//! (account, project, stream incarnation).

use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------
// Reserved system streams
// ---------------------------------------------------------------------

pub const USAGE_STREAM: &str = "_usage";
pub const OPS_METRICS_STREAM: &str = "_ops_metrics";
pub const OPS_EVENTS_STREAM: &str = "_ops_events";

/// The reserved internal namespaces. Reserved streams are invisible to
/// the customer catalog, refused on every public surface (raw and
/// product), excluded from customer usage and limits, and reachable
/// only through the fleet-internal telemetry path. Reserving the whole
/// `_`-prefix (not just the three current names) keeps the namespace
/// available for future system streams without a migration.
pub fn is_reserved_stream(name: &str) -> bool {
    name.starts_with('_')
}

// ---------------------------------------------------------------------
// Identity
// ---------------------------------------------------------------------

/// Who gets the invoice line. `stream_id` is the immutable incarnation
/// (the descriptor's `stream_epoch` hex) — deleting and recreating a
/// name yields a NEW identity, and a stale observation for the old one
/// can never mutate the new stream's rollup.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct BillingIdentity {
    pub account_id: String,
    pub project_id: String,
    /// Immutable resource incarnation: hex stream_epoch.
    pub stream_id: String,
    /// Display and lookup metadata only.
    pub stream_name: String,
}

/// Which process observed it. `boot` changes on every process start, so
/// (source, seq) batches from a restarted process can never collide
/// with — or re-bill — batches from the previous life.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MeterSource {
    pub cell: String,
    pub instance: String,
    pub boot: String,
}

/// This process's boot id: 16 random bytes, hex, minted once.
pub fn boot_id() -> &'static str {
    static B: std::sync::OnceLock<String> = std::sync::OnceLock::new();
    B.get_or_init(|| crate::crypto::hex(&crate::http::rand_epoch()))
}

// ---------------------------------------------------------------------
// UTC month math (no chrono dependency; Hinnant civil-date algorithm)
// ---------------------------------------------------------------------

/// (year, month 1..=12) of a UTC millisecond timestamp.
pub fn utc_year_month(ms: i64) -> (i32, u32) {
    let days = ms.div_euclid(86_400_000);
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    (y as i32, m as u32)
}

/// First instant of (year, month), UTC, in ms.
pub fn month_start_ms(year: i32, month: u32) -> i64 {
    let y = i64::from(if month <= 2 { year - 1 } else { year });
    let m = i64::from(month);
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let mp = if m > 2 { m - 3 } else { m + 9 };
    let doy = (153 * mp + 2) / 5;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    (era * 146_097 + doe - 719_468) * 86_400_000
}

pub fn next_month(year: i32, month: u32) -> (i32, u32) {
    if month == 12 {
        (year + 1, 1)
    } else {
        (year, month + 1)
    }
}

/// Display/key form: "2026-08".
pub fn month_str(year: i32, month: u32) -> String {
    format!("{year:04}-{month:02}")
}

/// Parse "YYYY-MM". Strict: exactly 7 chars, month 1..=12.
pub fn parse_month(s: &str) -> Option<(i32, u32)> {
    let (y, m) = s.split_once('-')?;
    if y.len() != 4 || m.len() != 2 {
        return None;
    }
    let year: i32 = y.parse().ok()?;
    let month: u32 = m.parse().ok()?;
    (1..=12).contains(&month).then_some((year, month))
}

// ---------------------------------------------------------------------
// Data-plane billing state (the row the committer maintains)
// ---------------------------------------------------------------------

/// Exact, durable, per-segment cumulative billing state. Lives in the
/// shard DB under `<seg-hash>'B'`, written in the SAME WriteBatch as
/// the records it describes — a committed append and its usage are one
/// atomic fact. Restart- and ownership-move-safe by construction: the
/// row moves with the shard.
///
/// JSON-encoded with defaults: the row is written once per commit
/// GROUP (not per record), so codec cost is noise next to the WAL
/// write, and `#[serde(default)]` gives forward evolution without a
/// hand-rolled binary version ladder.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct SegmentBillingMetaV1 {
    #[serde(default)]
    pub v: u16,
    // Identity, captured at first append and immutable for the life of
    // the incarnation (a recreation is a different segment identity).
    #[serde(default)]
    pub account_id: String,
    #[serde(default)]
    pub project_id: String,
    #[serde(default)]
    pub stream_id: String,
    #[serde(default)]
    pub stream_name: String,
    #[serde(default)]
    pub segment_id: u32,

    /// Monotone version: bumps on every mutation; the rollup keeps the
    /// largest and never double-adds a snapshot it has already seen.
    #[serde(default)]
    pub usage_version: u64,

    // Lifetime totals (never reset).
    #[serde(default)]
    pub ingest_payload_bytes_total: u64,
    #[serde(default)]
    pub ingest_records_total: u64,

    /// The billable storage gauge: canonical encrypted frame bytes this
    /// segment owns. Unchanged by absorption/trim/compaction; zeroed by
    /// hard deletion.
    #[serde(default)]
    pub owned_frame_bytes_current: u64,
    /// Storage byte-time is integrated up to here.
    #[serde(default)]
    pub storage_accounted_through_ms: i64,

    // Current-month exact accumulators. Rollover splits elapsed storage
    // time at the UTC boundary; the closed month's final numbers are
    // staged into the outbox in the same WriteBatch.
    #[serde(default)]
    pub month_year: i32,
    #[serde(default)]
    pub month_month: u32,
    #[serde(default)]
    pub month_ingest_payload_bytes: u64,
    #[serde(default)]
    pub month_ingest_records: u64,
    /// u128 as string (JSON numbers cap at 2^53 usefully; byte-ms for a
    /// large stream-month exceeds it).
    #[serde(default)]
    pub month_storage_byte_ms: String,

    #[serde(default)]
    pub retained_by_forks: bool,
}

impl SegmentBillingMetaV1 {
    pub fn month_byte_ms(&self) -> u128 {
        self.month_storage_byte_ms.parse().unwrap_or(0)
    }

    /// Advance the storage integral to `now_ms`, splitting at UTC month
    /// boundaries. Each closed month's final accumulators are handed to
    /// `on_month_close` BEFORE the month fields reset — the committer
    /// stages them into the usage outbox in the same WriteBatch.
    pub fn advance_storage_clock(
        &mut self,
        now_ms: i64,
        mut on_month_close: impl FnMut(&SegmentBillingMetaV1),
    ) {
        if self.storage_accounted_through_ms == 0 {
            // First observation: start the clock, open the month.
            let (y, m) = utc_year_month(now_ms);
            self.storage_accounted_through_ms = now_ms;
            self.month_year = y;
            self.month_month = m;
            return;
        }
        if now_ms <= self.storage_accounted_through_ms {
            return;
        }
        loop {
            let (ny, nm) = next_month(self.month_year, self.month_month);
            let boundary = month_start_ms(ny, nm);
            let upto = now_ms.min(boundary);
            let dt = (upto - self.storage_accounted_through_ms).max(0) as u128;
            let add = dt * self.owned_frame_bytes_current as u128;
            let cur = self.month_byte_ms();
            self.month_storage_byte_ms = (cur + add).to_string();
            self.storage_accounted_through_ms = upto;
            if upto < boundary {
                return;
            }
            // Month closed: report final numbers, then open the next.
            on_month_close(self);
            self.month_year = ny;
            self.month_month = nm;
            self.month_ingest_payload_bytes = 0;
            self.month_ingest_records = 0;
            self.month_storage_byte_ms = "0".to_string();
        }
    }
}

/// What an append request carries so the committer can attribute its
/// durable billing state: the identity plus which logical segment the
/// submit hash addresses. One Arc per request, built where the
/// descriptor is already in hand.
#[derive(Clone, Debug)]
pub struct BillingRef {
    pub identity: BillingIdentity,
    pub segment_id: u32,
}

impl SegmentBillingMetaV1 {
    /// The `_usage` view of this row (live or closed-month final).
    pub fn to_snapshot(&self, month_final: bool) -> SegmentSnapshot {
        SegmentSnapshot {
            identity: BillingIdentity {
                account_id: self.account_id.clone(),
                project_id: self.project_id.clone(),
                stream_id: self.stream_id.clone(),
                stream_name: self.stream_name.clone(),
            },
            segment_id: self.segment_id,
            usage_version: self.usage_version,
            month: month_str(self.month_year, self.month_month),
            month_final,
            ingest_payload_bytes_month: self.month_ingest_payload_bytes,
            ingest_records_month: self.month_ingest_records,
            owned_frame_bytes_current: self.owned_frame_bytes_current,
            storage_byte_ms_month: self.month_storage_byte_ms.clone(),
            storage_accounted_through_ms: self.storage_accounted_through_ms,
            retained_by_forks: self.retained_by_forks,
        }
    }
}

// ---------------------------------------------------------------------
// `_usage` ledger schema (JSON records)
// ---------------------------------------------------------------------

/// Envelope common to every `_usage` record. `event_id` is
/// deterministic for snapshots and lifecycle observations (replays
/// deduplicate downstream); read batches identify by (source, seq).
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct UsageEnvelope {
    pub v: u16,
    pub event_id: String,
    pub event_time_ms: i64,
    pub emitted_ms: i64,
    pub cell: String,
    #[serde(flatten)]
    pub payload: UsagePayload,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum UsagePayload {
    SegmentSnapshot(SegmentSnapshot),
    ReadBatch(ReadBatch),
    StreamLifecycle(StreamLifecycle),
    UsageCorrection(UsageCorrection),
}

/// Exact durable ingest/storage state for one segment, either the live
/// row (`month_final: false`) or a closed month's final numbers.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SegmentSnapshot {
    #[serde(flatten)]
    pub identity: BillingIdentity,
    pub segment_id: u32,
    pub usage_version: u64,
    pub month: String,
    pub month_final: bool,
    pub ingest_payload_bytes_month: u64,
    pub ingest_records_month: u64,
    pub owned_frame_bytes_current: u64,
    /// u128 as string.
    pub storage_byte_ms_month: String,
    pub storage_accounted_through_ms: i64,
    pub retained_by_forks: bool,
}

impl SegmentSnapshot {
    /// Deterministic id: same (segment incarnation, month, version) →
    /// same id, so a re-emitted snapshot deduplicates.
    pub fn deterministic_event_id(&self) -> String {
        format!(
            "snap/{}/{}/{}/{}{}",
            self.identity.stream_id,
            self.segment_id,
            self.month,
            self.usage_version,
            if self.month_final { "/final" } else { "" }
        )
    }
}

/// Externally delivered read usage, as deltas over [from_ms, to_ms).
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ReadBatch {
    pub source: MeterSource,
    pub seq: u64,
    pub from_ms: i64,
    pub to_ms: i64,
    pub rows: Vec<ReadRow>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ReadRow {
    #[serde(flatten)]
    pub identity: BillingIdentity,
    #[serde(default)]
    pub read_payload_bytes: u64,
    #[serde(default)]
    pub read_records: u64,
    #[serde(default)]
    pub read_operations: u64,
    #[serde(default)]
    pub queue_operations: u64,
    #[serde(default)]
    pub append_requests: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StreamLifecycle {
    #[serde(flatten)]
    pub identity: BillingIdentity,
    /// created | soft_deleted | hard_deleted | expired | fork_retained
    pub transition: String,
    pub at_ms: i64,
}

impl StreamLifecycle {
    pub fn deterministic_event_id(&self) -> String {
        format!("life/{}/{}", self.identity.stream_id, self.transition)
    }
}

/// Explicit post-close correction — finalized months are never silently
/// rewritten.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct UsageCorrection {
    #[serde(flatten)]
    pub identity: BillingIdentity,
    pub month: String,
    pub reason: String,
    #[serde(default)]
    pub ingest_payload_bytes_delta: i64,
    #[serde(default)]
    pub read_payload_bytes_delta: i64,
    /// i128 as string.
    #[serde(default)]
    pub storage_byte_ms_delta: String,
}

// ---------------------------------------------------------------------
// Shard-DB key layout for billing rows
// ---------------------------------------------------------------------

/// `<seg-hash>'B'` → SegmentBillingMetaV1 (JSON). Tag byte `B` is
/// unused by any other row family in the shard keyspace.
pub fn billing_meta_key(hash: &[u8; 16]) -> Vec<u8> {
    let mut k = Vec::with_capacity(17);
    k.extend_from_slice(hash);
    k.push(b'B');
    k
}

/// Usage-dirty index: `<0xFF sentinel>'U'<seg-hash>` → LE usage_version
/// not yet acknowledged by `_usage`. Same sentinel discipline as the
/// absorber's dirty index (shard.rs): sorts outside every stream's key
/// range, scanned once by the drainer, deleted through the committer so
/// acknowledgment serializes with concurrent appends.
pub const USAGE_DIRTY_SENTINEL: [u8; 16] = [0xFF; 16];

pub fn usage_dirty_key(hash: &[u8; 16]) -> Vec<u8> {
    let mut k = Vec::with_capacity(33);
    k.extend_from_slice(&USAGE_DIRTY_SENTINEL);
    k.push(b'U');
    k.extend_from_slice(hash);
    k
}

/// Closed-month final snapshots awaiting emission:
/// `<0xFF sentinel>'V'<seg-hash><"YYYY-MM">` → SegmentSnapshot (JSON,
/// self-contained). Rare — one per segment-month — and deleted after
/// the ledger acknowledges.
pub fn usage_month_final_key(hash: &[u8; 16], year: i32, month: u32) -> Vec<u8> {
    let mut k = Vec::with_capacity(40);
    k.extend_from_slice(&USAGE_DIRTY_SENTINEL);
    k.push(b'V');
    k.extend_from_slice(hash);
    k.extend_from_slice(month_str(year, month).as_bytes());
    k
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn month_math_round_trips() {
        // Epoch is 1970-01.
        assert_eq!(utc_year_month(0), (1970, 1));
        assert_eq!(month_start_ms(1970, 1), 0);
        // A known instant: 2026-08-06 ≈ 1786-billion ms.
        let (y, m) = utc_year_month(1_786_000_000_000);
        assert_eq!((y, m), (2026, 8));
        // Boundary consistency: the first ms of every month maps back,
        // and the ms before it maps to the previous month.
        let mut cur = (2025, 12u32);
        for _ in 0..15 {
            let start = month_start_ms(cur.0, cur.1);
            assert_eq!(utc_year_month(start), cur, "start of {cur:?}");
            let (py, pm) = utc_year_month(start - 1);
            assert_eq!(next_month(py, pm), cur, "instant before {cur:?}");
            cur = next_month(cur.0, cur.1);
        }
        assert_eq!(parse_month("2026-08"), Some((2026, 8)));
        assert_eq!(parse_month("2026-13"), None);
        assert_eq!(parse_month("junk"), None);
    }

    #[test]
    fn storage_clock_splits_at_month_boundaries() {
        let mut m = SegmentBillingMetaV1 {
            owned_frame_bytes_current: 1000,
            ..Default::default()
        };
        // Clock starts mid-July 2026.
        let jul = month_start_ms(2026, 7) + 86_400_000;
        m.advance_storage_clock(jul, |_| panic!("no close on start"));
        assert_eq!((m.month_year, m.month_month), (2026, 7));
        // Advance into August: July closes with exactly the byte-time
        // up to the boundary.
        let aug_start = month_start_ms(2026, 8);
        let into_aug = aug_start + 3_600_000;
        let mut closed = Vec::new();
        m.advance_storage_clock(into_aug, |c| closed.push(c.clone()));
        assert_eq!(closed.len(), 1);
        let jul_final = &closed[0];
        assert_eq!((jul_final.month_year, jul_final.month_month), (2026, 7));
        let expect_jul = (aug_start - jul) as u128 * 1000;
        assert_eq!(jul_final.month_byte_ms(), expect_jul);
        // The live row is now August with exactly one hour integrated.
        assert_eq!((m.month_year, m.month_month), (2026, 8));
        assert_eq!(m.month_byte_ms(), 3_600_000u128 * 1000);
        // Idle multi-month jump closes every intervening month.
        let mut closes = Vec::new();
        m.advance_storage_clock(month_start_ms(2026, 11) + 5, |c| {
            closes.push(month_str(c.month_year, c.month_month))
        });
        assert_eq!(closes, vec!["2026-08", "2026-09", "2026-10"]);
    }

    #[test]
    fn reserved_namespace_is_the_underscore_prefix() {
        assert!(is_reserved_stream(USAGE_STREAM));
        assert!(is_reserved_stream(OPS_METRICS_STREAM));
        assert!(is_reserved_stream(OPS_EVENTS_STREAM));
        assert!(is_reserved_stream("_future_system_thing"));
        assert!(!is_reserved_stream("orders"));
        assert!(!is_reserved_stream("customers/_acme")); // only the leading segment
    }

    #[test]
    fn snapshot_event_ids_are_deterministic() {
        let id = BillingIdentity {
            account_id: "a".into(),
            project_id: "p".into(),
            stream_id: "11".repeat(8),
            stream_name: "orders".into(),
        };
        let s = SegmentSnapshot {
            identity: id,
            segment_id: 3,
            usage_version: 42,
            month: "2026-08".into(),
            month_final: false,
            ingest_payload_bytes_month: 0,
            ingest_records_month: 0,
            owned_frame_bytes_current: 0,
            storage_byte_ms_month: "0".into(),
            storage_accounted_through_ms: 0,
            retained_by_forks: false,
        };
        assert_eq!(s.deterministic_event_id(), s.deterministic_event_id());
        let mut f = s.clone();
        f.month_final = true;
        assert_ne!(s.deterministic_event_id(), f.deterministic_event_id());
    }
}

// ---------------------------------------------------------------------
// Read-delivery meter (§7): ONE accumulator at the public response
// coordinator
// ---------------------------------------------------------------------

/// Flush thresholds (§7.2). The active map seals into a batch on any of
/// these; a sealed batch is what the drainer appends to `_usage`.
pub const READ_FLUSH_INTERVAL_MS: i64 = 10_000;
pub const READ_FLUSH_MAX_ENTRIES: usize = 10_000;
pub const READ_FLUSH_MAX_EST_BYTES: usize = 1 << 20;
/// Sealed batches waiting for the ledger. When the ledger is down long
/// enough to fill this, sealing PAUSES and deltas keep merging into the
/// active map — attribution is never discarded (§14.1), memory stays
/// bounded by stream cardinality, and the lag is visible.
pub const READ_SEALED_MAX_BATCHES: usize = 64;

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RowDelta {
    pub read_payload_bytes: u64,
    pub read_records: u64,
    pub read_operations: u64,
    pub queue_operations: u64,
    pub append_requests: u64,
}

struct ActiveMap {
    rows: std::collections::HashMap<BillingIdentity, RowDelta>,
    opened_ms: i64,
    /// Rough encoded-size estimate (identity strings + numbers), used
    /// only against READ_FLUSH_MAX_EST_BYTES.
    est_bytes: usize,
}

pub struct ReadUsageAccumulator {
    active: std::sync::Mutex<ActiveMap>,
    sealed: std::sync::Mutex<std::collections::VecDeque<ReadBatch>>,
    seq: std::sync::atomic::AtomicU64,
    source: MeterSource,
    /// Batches that could not seal because the sealed queue was full —
    /// a telemetry-lag signal, not data loss (rows kept merging).
    pub seal_deferrals: std::sync::atomic::AtomicU64,
}

impl ReadUsageAccumulator {
    pub fn new(source: MeterSource) -> Self {
        ReadUsageAccumulator {
            active: std::sync::Mutex::new(ActiveMap {
                rows: std::collections::HashMap::new(),
                opened_ms: 0,
                est_bytes: 0,
            }),
            sealed: std::sync::Mutex::new(std::collections::VecDeque::new()),
            seq: std::sync::atomic::AtomicU64::new(0),
            source,
            seal_deferrals: std::sync::atomic::AtomicU64::new(0),
        }
    }

    /// Add one observation. Reserved system streams are never metered
    /// (self-metering exclusion, §8.4).
    pub fn meter(&self, id: &BillingIdentity, d: RowDelta) {
        if is_reserved_stream(&id.stream_name) {
            return;
        }
        let mut a = self.active.lock().unwrap();
        if a.rows.is_empty() {
            a.opened_ms = crate::shard::now_ms();
        }
        match a.rows.get_mut(id) {
            Some(row) => {
                row.read_payload_bytes += d.read_payload_bytes;
                row.read_records += d.read_records;
                row.read_operations += d.read_operations;
                row.queue_operations += d.queue_operations;
                row.append_requests += d.append_requests;
                a.est_bytes += 8;
            }
            None => {
                a.est_bytes += 120
                    + id.account_id.len()
                    + id.project_id.len()
                    + id.stream_id.len()
                    + id.stream_name.len();
                a.rows.insert(id.clone(), d);
            }
        }
        if a.rows.len() >= READ_FLUSH_MAX_ENTRIES || a.est_bytes >= READ_FLUSH_MAX_EST_BYTES {
            self.seal_locked(&mut a);
        }
    }

    fn seal_locked(&self, a: &mut ActiveMap) {
        if a.rows.is_empty() {
            return;
        }
        let mut sealed = self.sealed.lock().unwrap();
        if sealed.len() >= READ_SEALED_MAX_BATCHES {
            // Ledger outage: keep merging instead of sealing. Rotation
            // resumes as soon as the drainer catches up.
            self.seal_deferrals
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            return;
        }
        let rows = std::mem::take(&mut a.rows)
            .into_iter()
            .map(|(identity, d)| ReadRow {
                identity,
                read_payload_bytes: d.read_payload_bytes,
                read_records: d.read_records,
                read_operations: d.read_operations,
                queue_operations: d.queue_operations,
                append_requests: d.append_requests,
            })
            .collect();
        let now = crate::shard::now_ms();
        sealed.push_back(ReadBatch {
            source: self.source.clone(),
            seq: self.seq.fetch_add(1, std::sync::atomic::Ordering::SeqCst),
            from_ms: a.opened_ms,
            to_ms: now,
            rows,
        });
        a.est_bytes = 0;
        a.opened_ms = now;
    }

    /// Timer/shutdown entry: seal if the active interval is at least
    /// `max_age_ms` old (0 = unconditionally).
    pub fn seal_if_aged(&self, max_age_ms: i64) {
        let mut a = self.active.lock().unwrap();
        if a.rows.is_empty() {
            return;
        }
        if max_age_ms == 0 || crate::shard::now_ms() - a.opened_ms >= max_age_ms {
            self.seal_locked(&mut a);
        }
    }

    /// Hand up to `max` sealed batches to the drainer. The drainer
    /// requeues on emission failure — a batch leaves this process only
    /// after `_usage` acknowledged it.
    pub fn drain_sealed(&self, max: usize) -> Vec<ReadBatch> {
        let mut sealed = self.sealed.lock().unwrap();
        let n = sealed.len().min(max);
        sealed.drain(..n).collect()
    }

    /// Failed emission: put the batches back at the FRONT, original
    /// order, so sequence numbers stay as monotone as delivery allows.
    pub fn requeue(&self, batches: Vec<ReadBatch>) {
        let mut sealed = self.sealed.lock().unwrap();
        for b in batches.into_iter().rev() {
            sealed.push_front(b);
        }
    }

    /// (active rows, active est bytes, sealed batches) — the §14.2 lag
    /// gauges, and the "maximum possible loss" numerator.
    pub fn unflushed(&self) -> (usize, usize, usize) {
        let a = self.active.lock().unwrap();
        let s = self.sealed.lock().unwrap();
        (a.rows.len(), a.est_bytes, s.len())
    }

    /// Test/operator view of the live (unsealed) rows.
    pub fn snapshot_active(&self) -> Vec<(BillingIdentity, RowDelta)> {
        let a = self.active.lock().unwrap();
        a.rows.iter().map(|(k, v)| (k.clone(), v.clone())).collect()
    }
}

/// The BillingIdentity for a descriptor, with deployment defaults for
/// descriptors created before the cutover.
pub fn identity_of(
    state: &crate::http::AppState,
    desc: &crate::registry::StreamDesc,
) -> BillingIdentity {
    BillingIdentity {
        account_id: desc
            .account_id
            .clone()
            .unwrap_or_else(|| state.account_id.clone()),
        project_id: desc
            .project_id
            .clone()
            .unwrap_or_else(|| state.project_id.clone()),
        stream_id: desc.stream_epoch.clone(),
        stream_name: desc.name.clone(),
    }
}

/// Meter one externally delivered read page (op + payload bytes).
pub fn meter_read(
    state: &crate::http::AppState,
    desc: &crate::registry::StreamDesc,
    bytes: u64,
    records: u64,
) {
    state.billing_reads.meter(
        &identity_of(state, desc),
        RowDelta {
            read_payload_bytes: bytes,
            read_records: records,
            read_operations: 1,
            ..Default::default()
        },
    );
}

/// Meter SSE/subscription payload chunks (no extra operation count).
pub fn meter_read_chunk(
    acc: &ReadUsageAccumulator,
    id: &BillingIdentity,
    bytes: u64,
    records: u64,
) {
    acc.meter(
        id,
        RowDelta {
            read_payload_bytes: bytes,
            read_records: records,
            ..Default::default()
        },
    );
}

/// Meter a queue operation with delivered payload (pull; redelivery
/// meters again by design §4.2).
pub fn meter_pull(
    state: &crate::http::AppState,
    desc: &crate::registry::StreamDesc,
    bytes: u64,
    records: u64,
) {
    state.billing_reads.meter(
        &identity_of(state, desc),
        RowDelta {
            read_payload_bytes: bytes,
            read_records: records,
            queue_operations: 1,
            ..Default::default()
        },
    );
}

/// Meter a zero-data queue operation (settle/extend/config).
pub fn meter_queue_op(state: &crate::http::AppState, desc: &crate::registry::StreamDesc) {
    state.billing_reads.meter(
        &identity_of(state, desc),
        RowDelta {
            queue_operations: 1,
            ..Default::default()
        },
    );
}

/// Count an accepted append request (informational dimension §4.5; the
/// BILLED ingest bytes come from the committer, not this counter).
pub fn meter_append_request(state: &crate::http::AppState, desc: &crate::registry::StreamDesc) {
    state.billing_reads.meter(
        &identity_of(state, desc),
        RowDelta {
            append_requests: 1,
            ..Default::default()
        },
    );
}
