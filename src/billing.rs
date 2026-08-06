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
