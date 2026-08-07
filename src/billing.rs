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

/// TRUSTED billing time (round-21 blocker 1). Month selection, storage
/// integration, lifecycle closure and usage event time use ONLY this —
/// never `Stream-Timestamp`, which is customer-controlled record
/// METADATA. A client shifting its record clock can relabel frames; it
/// can no longer move ingest between invoice months or park the
/// storage clock in the future to dodge accrual. Tests inject months
/// here instead of abusing the public header.
pub fn billing_now_ms() -> i64 {
    #[cfg(test)]
    {
        let v = BILLING_CLOCK_OVERRIDE.load(std::sync::atomic::Ordering::Relaxed);
        if v != 0 {
            return v;
        }
    }
    crate::shard::now_ms()
}

#[cfg(test)]
pub static BILLING_CLOCK_OVERRIDE: std::sync::atomic::AtomicI64 =
    std::sync::atomic::AtomicI64::new(0);

/// Serializes month-sensitive tests: the clock-injecting test takes
/// write; tests asserting real-now months take read.
#[cfg(test)]
pub fn billing_clock_lock() -> &'static tokio::sync::RwLock<()> {
    static L: std::sync::OnceLock<tokio::sync::RwLock<()>> = std::sync::OnceLock::new();
    L.get_or_init(|| tokio::sync::RwLock::new(()))
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
    /// Round-22 item 8: corrections are auditable financial records —
    /// deterministic id (dedupe key + artifact name), schema version,
    /// the source event that produced the delta, and when it applied.
    #[serde(default)]
    pub correction_id: String,
    #[serde(default)]
    pub correction_version: u32,
    #[serde(default)]
    pub source_event_id: String,
    #[serde(default)]
    pub created_at_ms: i64,
    #[serde(default)]
    pub ingest_payload_bytes_delta: i64,
    #[serde(default)]
    pub ingest_records_delta: i64,
    #[serde(default)]
    pub read_payload_bytes_delta: i64,
    #[serde(default)]
    pub read_records_delta: i64,
    #[serde(default)]
    pub read_operations_delta: i64,
    #[serde(default)]
    pub queue_operations_delta: i64,
    #[serde(default)]
    pub append_requests_delta: i64,
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

    fn test_batch(seq: u64) -> ReadBatch {
        ReadBatch {
            source: MeterSource {
                cell: "c".into(),
                instance: "i".into(),
                boot: "b".into(),
            },
            seq,
            from_ms: 0,
            to_ms: 1000,
            rows: vec![ReadRow {
                identity: BillingIdentity {
                    account_id: "a".into(),
                    project_id: "p".into(),
                    stream_id: "22".repeat(8),
                    stream_name: "orders".into(),
                },
                read_payload_bytes: seq + 1,
                read_records: 1,
                read_operations: 1,
                queue_operations: 0,
                append_requests: 0,
            }],
        }
    }

    /// Round-22 item 2a: a store fault after batch k of n leaves
    /// 1..k in the spool and k..n back in memory — the remainder is
    /// never dropped.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn spool_fault_requeues_the_remainder() {
        let store: std::sync::Arc<dyn object_store::ObjectStore> =
            std::sync::Arc::new(object_store::memory::InMemory::new());
        let spool = ReadSpool::open(store, "", "t-remainder").await.unwrap();
        let acc = ReadUsageAccumulator::new(MeterSource {
            cell: "c".into(),
            instance: "i".into(),
            boot: "b".into(),
        });
        acc.requeue(vec![test_batch(0), test_batch(1), test_batch(2)]);
        // Allow exactly one persist, then fault.
        spool.fail_after.store(1, std::sync::atomic::Ordering::SeqCst);
        let err = spool_sealed(&acc, &spool, 10).await.unwrap_err();
        assert!(err.contains("read spool persist"), "{err}");
        // Batch 0 is durable; batches 1 and 2 are back in memory in
        // original order — nothing was dropped.
        let spooled = spool.pending(10).await.unwrap();
        assert_eq!(spooled.len(), 1);
        assert_eq!(spooled[0].1.seq, 0);
        let back = acc.drain_sealed(10);
        assert_eq!(
            back.iter().map(|b| b.seq).collect::<Vec<_>>(),
            vec![1, 2],
            "the failed batch AND the unpersisted tail requeued"
        );
    }

    /// Round-22 item 2c: a corrupt spool row is quarantined (moved,
    /// preserved, counted — the count survives reopen), never
    /// silently skipped, and never blocks the healthy rows.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn spool_quarantines_corrupt_rows() {
        let store: std::sync::Arc<dyn object_store::ObjectStore> =
            std::sync::Arc::new(object_store::memory::InMemory::new());
        let spool = ReadSpool::open(store.clone(), "", "t-quarantine")
            .await
            .unwrap();
        spool.persist(&test_batch(7)).await.unwrap();
        spool.put_raw(b"rb/\x00garbage", b"{not json").await.unwrap();
        let ok = spool.pending(10).await.unwrap();
        assert_eq!(ok.len(), 1, "healthy row still drains");
        assert_eq!(ok[0].1.seq, 7);
        assert_eq!(spool.quarantined_count(), 1);
        assert_eq!(spool.quarantine_rows().await.len(), 1, "row preserved");
        // Idempotent: the moved row is not re-quarantined.
        spool.pending(10).await.unwrap();
        assert_eq!(spool.quarantined_count(), 1);
        // The alert condition survives a restart.
        drop(spool);
        let reopened = ReadSpool::open(store, "", "t-quarantine").await.unwrap();
        assert_eq!(reopened.quarantined_count(), 1);
        assert_eq!(reopened.pending(10).await.unwrap().len(), 1);
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

// ---------------------------------------------------------------------
// The telemetry drainer (§6.3/§7) and the `_usage` transport (§8)
// ---------------------------------------------------------------------

/// Envelope for one emission batch: read batches + segment snapshots +
/// closed-month finals, appended to `_usage` as one JSON-array record
/// batch (one envelope per record).
fn envelope(
    cell: &str,
    payload: UsagePayload,
    event_time_ms: i64,
    event_id: String,
) -> UsageEnvelope {
    UsageEnvelope {
        v: 1,
        event_id,
        event_time_ms,
        emitted_ms: crate::shard::now_ms(),
        cell: cell.to_string(),
        payload,
    }
}

/// Encoded size of one envelope, for the drain's hard byte budget.
fn encoded_size(e: &UsageEnvelope) -> usize {
    serde_json::to_vec(e).map(|v| v.len()).unwrap_or(4096) + 1
}

/// BILLING_MODE=required: production billing — volatile fallbacks are
/// refused and billing infrastructure failures are fatal at startup.
pub fn billing_required() -> bool {
    std::env::var("BILLING_MODE").map(|v| v == "required").unwrap_or(false)
}

/// Drain step 1: move sealed batches into the durable spool. On a
/// mid-loop store fault the failed batch AND the not-yet-persisted
/// remainder requeue at the accumulator head (round-22 item 2a) — a
/// fault after batch k of n must leave batches k..n in memory and
/// 1..k in the spool, nothing dropped.
pub async fn spool_sealed(
    acc: &ReadUsageAccumulator,
    spool: &ReadSpool,
    max: usize,
) -> Result<(), String> {
    let sealed = acc.drain_sealed(max);
    let mut it = sealed.into_iter();
    while let Some(rb) = it.next() {
        if let Err(e) = spool.persist(&rb).await {
            let mut back = vec![rb];
            back.extend(it);
            acc.requeue(back);
            return Err(format!("read spool persist: {e}"));
        }
    }
    Ok(())
}

/// Ensure `_usage` exists and append one JSON-array body with the
/// system key, through the normal in-process raw path (ownership,
/// group commit and durability all apply). Ok(()) only after the
/// append acknowledged durably.
async fn usage_ledger_append(
    state: &std::sync::Arc<crate::http::AppState>,
    key: &str,
    body: Vec<u8>,
) -> Result<(), String> {
    system_append(state, USAGE_STREAM, key, body).await
}

/// One drain round (§6.3 steps 1-5): emit sealed read batches and every
/// dirty segment snapshot (+ closed-month finals) to `_usage`, then
/// acknowledge exactly what was emitted. Idempotent under every crash
/// interleaving: a lost emission leaves dirty state; a lost ack
/// re-emits an identical snapshot the rollup deduplicates.
pub async fn drain_once(state: &std::sync::Arc<crate::http::AppState>) -> Result<usize, String> {
    let Some(key) = state.usage_key.clone() else {
        return Ok(0);
    };
    let cell = state.cell_id.clone();
    let mut envelopes: Vec<UsageEnvelope> = Vec::new();

    // 1. Read batches: seal, then SPOOL DURABLY before anything can
    // forget them (round-21 blocker 3). The ledger is fed from the
    // spool, and a spooled batch is deleted only after `_usage`
    // acknowledged — a ledger outage accumulates on disk while the
    // in-memory accumulator keeps rotating normally.
    state.billing_reads.seal_if_aged(READ_FLUSH_INTERVAL_MS);
    let mut spooled_keys: Vec<Vec<u8>> = Vec::new();
    let mut memless: Vec<ReadBatch> = Vec::new();
    let mut body_bytes = 0usize;
    if let Some(spool) = state.read_spool.get() {
        spool_sealed(&state.billing_reads, spool, 64).await?;
        for (key, rb) in spool.pending(64).await.map_err(|e| e.to_string())? {
            let env = envelope(
                &cell,
                UsagePayload::ReadBatch(rb.clone()),
                rb.to_ms,
                format!("read/{}/{}", rb.source.boot, rb.seq),
            );
            body_bytes += encoded_size(&env);
            envelopes.push(env);
            spooled_keys.push(key);
        }
    } else if billing_required() {
        // Round-22 item 2b: required mode has NO memory-only window.
        // Until the spool is open, drains fail (the meter keeps
        // accumulating; nothing is emitted from volatile state).
        return Err("read spool not open (BILLING_MODE=required refuses the memory-only path)".into());
    } else {
        // No spool configured (bare test rigs): the pre-spool path.
        memless = state.billing_reads.drain_sealed(16);
        for rb in &memless {
            let env = envelope(
                &cell,
                UsagePayload::ReadBatch(rb.clone()),
                rb.to_ms,
                format!("read/{}/{}", rb.source.boot, rb.seq),
            );
            body_bytes += encoded_size(&env);
            envelopes.push(env);
        }
    }

    // 2. Dirty segment snapshots + month finals from every open engine.
    let engines: Vec<std::sync::Arc<crate::shard::ShardEngine>> = {
        let map = state.shards.read().unwrap();
        map.values().cloned().collect()
    };
    let mut acks: Vec<(
        std::sync::Arc<crate::shard::ShardEngine>,
        [u8; 16],
        u64,
        Vec<Vec<u8>>,
    )> = Vec::new();
    // HARD-bounded round (round-21 blocker 9, tightened by round-22
    // item 9): at most DRAIN_MAX_ENVELOPES and DRAIN_MAX_BYTES of
    // encoded payload per ledger append — month finals and lifecycle
    // events count against the budget like everything else. A row's
    // contribution (finals + live snapshot + lifecycle) is admitted
    // atomically: over budget, the whole row waits for the next round
    // (2 s later) with its dirty marker intact.
    const DRAIN_MAX_ENVELOPES: usize = 1000;
    const DRAIN_MAX_BYTES: usize = 1_000_000;
    const LIFECYCLE_EST: usize = 512;
    'engines: for engine in engines {
        // Round-22 item 6: financial scans fail CLOSED. A scan error
        // skips this engine's contribution entirely — no emit and no
        // ack, so every dirty row stays dirty and the next round
        // retries — instead of treating "scan failed" as "nothing
        // exists" and acking finals away against an empty list.
        let dirty = match engine.usage_dirty_scan().await {
            Ok(d) => d,
            Err(e) => {
                tracing::warn!("usage dirty scan failed (engine drain deferred): {e}");
                continue;
            }
        };
        if dirty.is_empty() {
            continue;
        }
        let finals = match engine.usage_month_finals().await {
            Ok(f) => f,
            Err(e) => {
                tracing::warn!("month-final scan failed (engine drain deferred, fail closed): {e}");
                continue;
            }
        };
        for (hash, version) in dirty {
            let Some(mut meta) = engine.billing_meta(hash).await else {
                continue;
            };
            // Defense in depth for §8.4: a reserved stream's row (none
            // should exist) is acked away, never emitted.
            if is_reserved_stream(&meta.stream_name) {
                engine.submit_usage_ack(hash, meta.usage_version, Vec::new());
                continue;
            }
            // Admit the row's WHOLE contribution against the budget
            // before any side effect: its month finals, its live
            // snapshot, and headroom for a possible lifecycle event.
            // (A first row larger than the whole budget still ships
            // alone — the bound is per-append, not a wedge.)
            let row_finals: Vec<(&Vec<u8>, &SegmentSnapshot)> = finals
                .iter()
                .filter(|(k, _)| k.len() >= 33 && k[17..33] == hash)
                .map(|(k, f)| (k, f))
                .collect();
            let snap_probe = meta.to_snapshot(false);
            let mut row_bytes = LIFECYCLE_EST + encoded_size(&envelope(
                &cell,
                UsagePayload::SegmentSnapshot(snap_probe),
                meta.storage_accounted_through_ms,
                String::new(),
            ));
            for (_, fs) in &row_finals {
                row_bytes += encoded_size(&envelope(
                    &cell,
                    UsagePayload::SegmentSnapshot((*fs).clone()),
                    fs.storage_accounted_through_ms,
                    String::new(),
                ));
            }
            let row_envs = row_finals.len() + 2;
            if !envelopes.is_empty()
                && (envelopes.len() + row_envs > DRAIN_MAX_ENVELOPES
                    || body_bytes + row_bytes > DRAIN_MAX_BYTES)
            {
                break 'engines;
            }
            // TERMINAL-CLOSURE RECONCILER (round-21 blocker 6): a
            // nonzero gauge whose descriptor is gone, expired, or
            // recreated under a new epoch means the closure was lost
            // (full committer queue, crash, foreign owner at delete
            // time) — resubmit it, traffic-independent, until the
            // gauge zeroes. Deletion, cascade and EXPIRY all land here
            // because they all leave the descriptor not-alive. A fork-
            // retained source (soft delete with live children) is NOT
            // closure: it flags retained_by_forks and keeps accruing —
            // the fork billing contract.
            match state.registry.get(&meta.stream_name).await {
                Ok(Some(d)) if d.stream_epoch == meta.stream_id => {
                    if d.soft_deleted && !d.deleted {
                        let was_retained = meta.retained_by_forks;
                        meta.retained_by_forks = true;
                        // Round-22 item 7: the fork billing contract is
                        // persisted ON the row, not only in emitted
                        // snapshots — it must survive restarts and
                        // ownership moves.
                        if !was_retained {
                            if let Err(e) = engine.submit_billing_retained(hash, true).await {
                                tracing::warn!(
                                    "retained-by-forks persist failed for {}: {e}",
                                    meta.stream_name
                                );
                            }
                        }
                    } else if !crate::http::desc_alive(&d) && meta.owned_frame_bytes_current > 0 {
                        // Round-22 item 7: the close accounts to the
                        // PERSISTED logical time — the tombstone's
                        // stamp for deletes, the configured expiry
                        // instant for expirations — never to "whenever
                        // the closure finally ran". The persisted time
                        // also makes the lifecycle event id identical
                        // across retries, so the ledger dedupes them.
                        let close_ms = if d.deleted {
                            d.logical_close_ms.unwrap_or_else(billing_now_ms)
                        } else {
                            d.expires_at_ms.unwrap_or_else(billing_now_ms)
                        };
                        if let Err(e) = engine.submit_billing_close(hash, close_ms).await {
                            tracing::warn!(
                                "billing close submit failed for {} (row stays dirty): {e}",
                                meta.stream_name
                            );
                            continue;
                        }
                        let lc = StreamLifecycle {
                            identity: BillingIdentity {
                                account_id: meta.account_id.clone(),
                                project_id: meta.project_id.clone(),
                                stream_id: meta.stream_id.clone(),
                                stream_name: meta.stream_name.clone(),
                            },
                            transition: if d.deleted {
                                "hard_deleted".to_string()
                            } else {
                                "expired".to_string()
                            },
                            at_ms: close_ms,
                        };
                        let id = lc.deterministic_event_id();
                        let env =
                            envelope(&cell, UsagePayload::StreamLifecycle(lc), close_ms, id);
                        body_bytes += encoded_size(&env);
                        envelopes.push(env);
                    }
                }
                Ok(_) if meta.owned_frame_bytes_current > 0 => {
                    // Name gone entirely, or recreated under a new
                    // epoch: this incarnation is terminal either way.
                    // No persisted stamp survives the replacement —
                    // account to now (residual documented at the
                    // tombstone walk).
                    if let Err(e) = engine.submit_billing_close(hash, billing_now_ms()).await {
                        tracing::warn!(
                            "billing close submit failed for {} (row stays dirty): {e}",
                            meta.stream_name
                        );
                        continue;
                    }
                }
                Err(e) => {
                    // Round-22 item 7 (fail closed): a registry read
                    // fault must not default to "emit and ack as if
                    // alive" — the row stays dirty and retries.
                    tracing::warn!(
                        "registry read failed for {} (usage row deferred): {e}",
                        meta.stream_name
                    );
                    continue;
                }
                Ok(_) => {}
            }
            // Emit the CURRENT row at ITS version (>= the marker's) —
            // acking the emitted version keeps anything newer dirty.
            let ver = meta.usage_version;
            let snap = meta.to_snapshot(false);
            let mut final_keys = Vec::new();
            for (k, fs) in row_finals {
                let env = envelope(
                    &cell,
                    UsagePayload::SegmentSnapshot(fs.clone()),
                    fs.storage_accounted_through_ms,
                    fs.deterministic_event_id(),
                );
                body_bytes += encoded_size(&env);
                envelopes.push(env);
                final_keys.push(k.clone());
            }
            let env = envelope(
                &cell,
                UsagePayload::SegmentSnapshot(snap.clone()),
                meta.storage_accounted_through_ms,
                snap.deterministic_event_id(),
            );
            body_bytes += encoded_size(&env);
            envelopes.push(env);
            let _ = version;
            acks.push((engine.clone(), hash, ver, final_keys));
        }
    }

    if envelopes.is_empty() {
        return Ok(0);
    }
    let body = serde_json::to_vec(&envelopes).map_err(|e| e.to_string())?;
    match usage_ledger_append(state, &key, body).await {
        Ok(()) => {
            // Spooled batches leave the spool ONLY now, after the
            // ledger acknowledged durably.
            if let Some(spool) = state.read_spool.get() {
                spool
                    .remove(&spooled_keys)
                    .await
                    .map_err(|e| e.to_string())?;
            }
            for (engine, hash, ver, finals) in acks {
                engine.submit_usage_ack(hash, ver, finals);
            }
            Ok(envelopes.len())
        }
        Err(e) => {
            // Spooled batches stay durable in the spool; memless ones
            // requeue at the accumulator's front.
            state.billing_reads.requeue(memless);
            Err(e)
        }
    }
}

/// Open the durable read spool (verified readable — `ReadSpool::open`
/// scans pending rows before returning) and register it on the state.
/// Required mode calls this SYNCHRONOUSLY before the instance serves
/// (round-22 items 2b/10): there is no memory-only window in which a
/// crash could lose metered reads.
pub async fn open_read_spool(
    state: &std::sync::Arc<crate::http::AppState>,
) -> anyhow::Result<()> {
    if state.read_spool.get().is_some() {
        return Ok(());
    }
    let sp = ReadSpool::open(
        state.data_store.clone(),
        &std::env::var("PATH_PREFIX").unwrap_or_default(),
        &state.instance_name,
    )
    .await?;
    let _ = state.read_spool.set(std::sync::Arc::new(sp));
    Ok(())
}

/// The drainer task: every TELEMETRY_DRAIN_SECS (default 2), one drain
/// round. Errors log and retry — the durable outbox holds the truth.
pub fn spawn_telemetry(state: std::sync::Arc<crate::http::AppState>) {
    if state.usage_key.is_none() {
        tracing::info!("telemetry pipeline off (USAGE_STREAM_KEY unset)");
        return;
    }
    // Open the durable read spool before the first drain (required
    // mode already opened it synchronously at startup); ownership
    // sweep runs at start and every OUTBOX_SWEEP_SECS.
    {
        let st = state.clone();
        tokio::spawn(async move {
            if let Err(e) = open_read_spool(&st).await {
                tracing::error!("read spool open failed: {e}");
            }
            sweep_owned_outboxes(&st).await;
            let sweep_secs: u64 = std::env::var("OUTBOX_SWEEP_SECS")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(300);
            loop {
                tokio::time::sleep(std::time::Duration::from_secs(sweep_secs)).await;
                sweep_owned_outboxes(&st).await;
            }
        });
    }
    let secs: u64 = std::env::var("TELEMETRY_DRAIN_SECS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(2);
    let metrics_secs: u64 = std::env::var("METRICS_INTERVAL_SECS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(15);
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(std::time::Duration::from_secs(secs.max(1)));
        let mut last_metrics = 0i64;
        loop {
            tick.tick().await;
            if let Err(e) = drain_once(&state).await {
                tracing::warn!("usage drain: {e}");
            }
            if let Err(e) = crate::ops::drain_ops_once(&state).await {
                tracing::warn!("ops drain: {e}");
            }
            if let Err(e) = crate::fleet::drain_fleet_events(&state).await {
                tracing::warn!("fleet event drain: {e}");
            }
            let now = crate::shard::now_ms();
            if now - last_metrics >= (metrics_secs as i64) * 1000 {
                last_metrics = now;
                if let Err(e) = crate::ops::emit_metrics_once(&state).await {
                    tracing::warn!("ops metrics emit: {e}");
                }
            }
        }
    });
}

// ---------------------------------------------------------------------
// The rollup consumer (§9.3) and month closer (§9.4-§9.6)
// ---------------------------------------------------------------------

/// One rollup step: read the next `_usage` page from the stored cursor
/// through the normal in-process read path, apply it transactionally,
/// advance. Returns envelopes applied (0 = caught up).
pub async fn rollup_step(state: &std::sync::Arc<crate::http::AppState>) -> Result<usize, String> {
    let Some(rollup) = state.rollup.get() else {
        return Ok(0);
    };
    let Some(key) = state.usage_key.clone() else {
        return Ok(0);
    };
    use axum::http::{HeaderMap, HeaderValue};
    let mut hdrs = HeaderMap::new();
    hdrs.insert(
        "stream-encryption-key",
        HeaderValue::from_str(&key).map_err(|_| "bad usage key".to_string())?,
    );
    let cursor = rollup.cursor().await.filter(|c| !c.is_empty());
    let Some((body, next)) = system_read(state, USAGE_STREAM, &key, cursor).await? else {
        return Ok(0); // ledger not created yet
    };
    let envelopes: Vec<UsageEnvelope> = if body.is_empty() {
        Vec::new()
    } else {
        serde_json::from_slice(&body).map_err(|e| format!("ledger decode: {e}"))?
    };
    if envelopes.is_empty() {
        return Ok(0);
    }
    rollup
        .apply_page(&envelopes, &next)
        .await
        .map_err(|e| e.to_string())?;
    Ok(envelopes.len())
}

/// One ops-metrics rollup step (§13.1): consume `_ops_metrics` from
/// its own cursor into raw + m1 tiers.
pub async fn ops_rollup_step(
    state: &std::sync::Arc<crate::http::AppState>,
) -> Result<usize, String> {
    let Some(rollup) = state.rollup.get() else {
        return Ok(0);
    };
    let Some(key) = state.usage_key.clone() else {
        return Ok(0);
    };
    use axum::http::{HeaderMap, HeaderValue};
    let mut hdrs = HeaderMap::new();
    hdrs.insert(
        "stream-encryption-key",
        HeaderValue::from_str(&key).map_err(|_| "bad usage key".to_string())?,
    );
    let cursor = rollup.ops_cursor().await.filter(|c| !c.is_empty());
    let Some((body, next)) = system_read(state, OPS_METRICS_STREAM, &key, cursor).await? else {
        return Ok(0);
    };
    let snaps: Vec<crate::ops::OpsSnapshot> = if body.is_empty() {
        Vec::new()
    } else {
        serde_json::from_slice(&body).map_err(|e| format!("ops decode: {e}"))?
    };
    if snaps.is_empty() {
        return Ok(0);
    }
    rollup
        .apply_ops_page(&snaps, &next)
        .await
        .map_err(|e| e.to_string())?;
    Ok(snaps.len())
}

/// Monthly-artifact content mismatches observed at publication (an
/// AlreadyExists object whose bytes differ from the frozen row). Any
/// nonzero value is a standing operator alert: an immutable invoice
/// path holds content we did not stage.
pub static ARTIFACT_MISMATCHES: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

/// Publish pending monthly + correction artifacts (blocker 7 phase 2,
/// hardened by round-22 item 8): create-only PUT of each staged body
/// to its immutable path. AlreadyExists is trusted ONLY after reading
/// the object back and verifying its bytes equal what we staged — an
/// equal body is an earlier successful attempt; a different body is a
/// financial-integrity alarm and the row stays pending for an
/// operator, never silently marked published. Anything else stays
/// pending and retries next tick and after restart.
pub async fn publish_artifacts(
    rollup: &crate::rollup::UsageRollup,
    store: &std::sync::Arc<dyn object_store::ObjectStore>,
    prefix: &str,
) -> Result<usize, String> {
    use object_store::{ObjectStoreExt, PutMode, PutOptions, PutPayload};
    async fn create_verified(
        store: &std::sync::Arc<dyn object_store::ObjectStore>,
        path: &str,
        body: &[u8],
    ) -> bool {
        let opath = object_store::path::Path::from(path.to_string());
        match store
            .put_opts(
                &opath,
                PutPayload::from(body.to_vec()),
                PutOptions::from(PutMode::Create),
            )
            .await
        {
            Ok(_) => true,
            Err(object_store::Error::AlreadyExists { .. }) => {
                match store.get(&opath).await {
                    Ok(r) => match r.bytes().await {
                        Ok(existing) if existing.as_ref() == body => true,
                        Ok(existing) => {
                            ARTIFACT_MISMATCHES
                                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                            tracing::error!(
                                "artifact CONTENT MISMATCH at {path}: existing {}B != staged {}B — \
                                 refusing to mark published; operator must reconcile",
                                existing.len(),
                                body.len()
                            );
                            false
                        }
                        Err(e) => {
                            tracing::warn!("artifact verify read {path}: {e}");
                            false
                        }
                    },
                    Err(e) => {
                        tracing::warn!("artifact verify get {path}: {e}");
                        false
                    }
                }
            }
            Err(e) => {
                tracing::warn!("artifact PUT {path}: {e}");
                false
            }
        }
    }
    let pending = rollup
        .pending_artifacts(64)
        .await
        .map_err(|e| e.to_string())?;
    let mut published = 0usize;
    for (pkey, month, project, stream_id, row) in pending {
        // `project` here is "{account}/{project}" — the artifact path
        // carries the account dimension (doc item D2).
        let path = if prefix.is_empty() {
            format!("telemetry/usage-monthly/{project}/{stream_id}/{month}.json")
        } else {
            format!("{prefix}/telemetry/usage-monthly/{project}/{stream_id}/{month}.json")
        };
        let body = serde_json::to_vec(&row).map_err(|e| e.to_string())?;
        if create_verified(store, &path, &body).await {
            rollup
                .mark_artifact_published(&pkey, &path)
                .await
                .map_err(|e| e.to_string())?;
            published += 1;
        }
    }
    // Correction artifacts (round-22 item 8): each correction applied
    // to a finalized month is its own immutable object next to the
    // monthly artifact.
    let corr = rollup
        .pending_correction_artifacts(64)
        .await
        .map_err(|e| e.to_string())?;
    for (pkey, month, acct_proj, stream_id, cid, body) in corr {
        let safe = cid.replace('/', "~");
        let path = if prefix.is_empty() {
            format!("telemetry/usage-monthly/{acct_proj}/{stream_id}/{month}.corrections/{safe}.json")
        } else {
            format!(
                "{prefix}/telemetry/usage-monthly/{acct_proj}/{stream_id}/{month}.corrections/{safe}.json"
            )
        };
        if create_verified(store, &path, &body).await {
            rollup
                .mark_correction_published(&pkey, &path)
                .await
                .map_err(|e| e.to_string())?;
            published += 1;
        }
    }
    Ok(published)
}

/// The rollup task: consume continuously; close the PREVIOUS month
/// after the grace period, writing one immutable artifact per stream
/// (§9.6) under telemetry/usage-monthly/.
pub fn spawn_rollup(state: std::sync::Arc<crate::http::AppState>, prefix: String) {
    use object_store::ObjectStoreExt;
    tokio::spawn(async move {
        let store = state.data_store.clone();
        let rollup = match crate::rollup::UsageRollup::open(store.clone(), &prefix).await {
            Ok(r) => std::sync::Arc::new(r),
            Err(e) => {
                tracing::error!("usage rollup open failed: {e}");
                return;
            }
        };
        let _ = state.rollup.set(rollup);
        tracing::info!("usage rollup running");
        let grace_ms: i64 = std::env::var("MONTH_CLOSE_GRACE_MS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(24 * 3_600_000);
        let mut last_close = 0i64;
        loop {
            let usage_n = match rollup_step(&state).await {
                Ok(n) => n,
                Err(e) => {
                    tracing::warn!("rollup step: {e}");
                    0
                }
            };
            let ops_n = match ops_rollup_step(&state).await {
                Ok(n) => n,
                Err(e) => {
                    tracing::warn!("ops rollup step: {e}");
                    0
                }
            };
            if usage_n == 0 && ops_n == 0 {
                tokio::time::sleep(std::time::Duration::from_secs(2)).await;
            }
            let now = crate::shard::now_ms();
            if now - last_close > 3_600_000 {
                last_close = now;
                let rollup2 = state.rollup.get().unwrap().clone();
                let store2 = state.data_store.clone();
                let pfx = prefix.clone();
                if let Ok(n) = rollup2.sweep_ops_raw(now, 10_000).await {
                    if n > 0 {
                        tracing::info!("ops raw retention: {n} points expired");
                    }
                }
                // Round-22 item 8: missed months catch up IN ORDER from
                // the persisted oldest-unfinalized marker — a rollup
                // that was down over one or more boundaries closes
                // every overdue month, oldest first, before touching
                // the newest.
                match rollup2.close_months_due(grace_ms).await {
                    Ok(closed) => {
                        for (mstr, n) in closed {
                            if n > 0 {
                                tracing::info!("month {mstr} closed: {n} streams");
                            }
                        }
                    }
                    Err(e) => tracing::warn!("month close: {e}"),
                }
                // Two-phase artifact publication (round-21 blocker 7):
                // PutMode::Create against the immutable path; an
                // AlreadyExists is an earlier successful PUT. Pending
                // rows survive crash and retry here every tick.
                if let Err(e) = publish_artifacts(&rollup2, &store2, &pfx).await {
                    tracing::warn!("artifact publication: {e}");
                }
            }
        }
    });
}

// ---------------------------------------------------------------------
// Durable read spool (round-21 blocker 3)
// ---------------------------------------------------------------------

/// Per-instance durable spool for sealed read batches. A sealed batch
/// is written HERE before anything else forgets it; it leaves only
/// after `_usage` acknowledged. This makes the design's loss bound
/// true: a hard crash loses at most the active interval plus one drain
/// cadence of sealed-but-unspooled batches — never the sealed backlog,
/// and a LEDGER OUTAGE accumulates on disk, not in process memory.
pub struct ReadSpool {
    db: std::sync::Arc<slatedb::Db>,
    next: std::sync::atomic::AtomicU64,
    /// Corrupt rows moved to `quarantine/` since open (plus any found
    /// at open) — nonzero means reads may be under-billed and an
    /// operator must inspect; the rows themselves are preserved.
    quarantined: std::sync::atomic::AtomicU64,
    /// Test fault injection: -1 = never fail; N >= 0 = allow N more
    /// successful persists, then fail every one after.
    #[cfg(test)]
    pub fail_after: std::sync::atomic::AtomicI64,
}

impl ReadSpool {
    pub async fn open(
        store: std::sync::Arc<dyn object_store::ObjectStore>,
        prefix: &str,
        instance: &str,
    ) -> anyhow::Result<Self> {
        let inst = if instance.is_empty() {
            "solo"
        } else {
            instance
        };
        let path = if prefix.is_empty() {
            format!("telemetry/read-spool/{inst}")
        } else {
            format!("{prefix}/telemetry/read-spool/{inst}")
        };
        let db = crate::on_slatedb_rt(async move {
            slatedb::Db::builder(path.as_str(), store).build().await
        })
        .await?;
        let next = match db.get(&b"meta/next-seq"[..]).await? {
            Some(v) => u64::from_le_bytes(v[..8].try_into().unwrap_or([0; 8])),
            None => 0,
        };
        // Rows quarantined by earlier boots stay on the books: the
        // corruption alert survives restarts until an operator clears
        // the quarantine explicitly.
        let mut prior = 0u64;
        let mut it = db.scan_prefix(&b"quarantine/"[..], ..).await?;
        while (it.next().await?).is_some() {
            prior += 1;
        }
        let sp = ReadSpool {
            db: std::sync::Arc::new(db),
            next: std::sync::atomic::AtomicU64::new(next),
            quarantined: std::sync::atomic::AtomicU64::new(prior),
            #[cfg(test)]
            fail_after: std::sync::atomic::AtomicI64::new(-1),
        };
        // Round-22 item 2b: an openable spool whose PENDING rows cannot
        // be scanned is not "ready" — prove readability before use.
        sp.pending(16).await?;
        Ok(sp)
    }

    fn key(seq: u64) -> Vec<u8> {
        let mut k = b"rb/".to_vec();
        k.extend_from_slice(&seq.to_be_bytes());
        k
    }

    /// Durably persist one sealed batch. Returns its spool key.
    pub async fn persist(&self, b: &ReadBatch) -> anyhow::Result<Vec<u8>> {
        #[cfg(test)]
        {
            let left = self.fail_after.load(std::sync::atomic::Ordering::SeqCst);
            if left == 0 {
                anyhow::bail!("injected spool fault");
            }
            if left > 0 {
                self.fail_after
                    .fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
            }
        }
        let seq = self.next.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let key = Self::key(seq);
        let mut wb = slatedb::WriteBatch::new();
        wb.put(key.clone(), serde_json::to_vec(b)?);
        wb.put(&b"meta/next-seq"[..], &(seq + 1).to_le_bytes()[..]);
        self.db.write(wb).await?;
        // The build runs with wal_disable (the engines own their WAL
        // cadence), so an explicit flush is what makes "spooled" mean
        // OBJECT-STORE durable — the entire point of this spool.
        self.db.flush().await?;
        Ok(key)
    }

    /// Oldest pending batches (recovered across restarts). A row that
    /// does not decode is QUARANTINED (round-22 item 2c): moved to
    /// `quarantine/<key>` in one batch — preserved for forensics,
    /// never re-parsed, never silently skipped — counted, logged at
    /// error level, and raised as an alert by the ops evaluator.
    /// Scan/store faults still propagate as errors (fail closed);
    /// only decode failures quarantine.
    pub async fn pending(&self, max: usize) -> anyhow::Result<Vec<(Vec<u8>, ReadBatch)>> {
        let mut out = Vec::new();
        let mut corrupt: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
        let mut iter = self.db.scan_prefix(&b"rb/"[..], ..).await?;
        while let Some(kv) = iter.next().await? {
            match serde_json::from_slice(&kv.value) {
                Ok(b) => {
                    out.push((kv.key.to_vec(), b));
                    if out.len() >= max {
                        break;
                    }
                }
                Err(e) => {
                    tracing::error!(
                        "read spool row corrupt ({} bytes at {}): {e} — quarantining; \
                         reads in this batch are NOT billed until an operator recovers it",
                        kv.value.len(),
                        String::from_utf8_lossy(&kv.key),
                    );
                    corrupt.push((kv.key.to_vec(), kv.value.to_vec()));
                }
            }
        }
        if !corrupt.is_empty() {
            let mut wb = slatedb::WriteBatch::new();
            for (k, v) in &corrupt {
                let mut qk = b"quarantine/".to_vec();
                qk.extend_from_slice(k);
                wb.put(qk, v.clone());
                wb.delete(k.clone());
            }
            self.db.write(wb).await?;
            self.db.flush().await?;
            self.quarantined
                .fetch_add(corrupt.len() as u64, std::sync::atomic::Ordering::Relaxed);
        }
        Ok(out)
    }

    /// Quarantined-row count (this boot + found at open). Nonzero is
    /// an open alert until the quarantine is cleared by an operator.
    pub fn quarantined_count(&self) -> u64 {
        self.quarantined.load(std::sync::atomic::Ordering::Relaxed)
    }

    #[cfg(test)]
    pub async fn put_raw(&self, key: &[u8], val: &[u8]) -> anyhow::Result<()> {
        let mut wb = slatedb::WriteBatch::new();
        wb.put(key.to_vec(), val.to_vec());
        self.db.write(wb).await?;
        Ok(())
    }

    #[cfg(test)]
    pub async fn quarantine_rows(&self) -> Vec<Vec<u8>> {
        let mut out = Vec::new();
        let mut it = self.db.scan_prefix(&b"quarantine/"[..], ..).await.unwrap();
        while let Some(kv) = it.next().await.unwrap() {
            out.push(kv.key.to_vec());
        }
        out
    }

    /// Remove batches the ledger has durably acknowledged.
    pub async fn remove(&self, keys: &[Vec<u8>]) -> anyhow::Result<()> {
        if keys.is_empty() {
            return Ok(());
        }
        let mut wb = slatedb::WriteBatch::new();
        for k in keys {
            wb.delete(k.clone());
        }
        self.db.write(wb).await?;
        Ok(())
    }

    pub async fn depth(&self) -> usize {
        self.pending(usize::MAX).await.map(|v| v.len()).unwrap_or(0)
    }
}

/// Open every shard THIS instance owns so its billing outbox becomes
/// drainable (round-21 blocker 4): the drainer must not depend on
/// customer traffic to rediscover dirty usage after a crash or an
/// ownership move. Bounded by the fleet's shard count.
pub async fn sweep_owned_outboxes(state: &std::sync::Arc<crate::http::AppState>) {
    let prefixes: Vec<String> = state.shard_prefixes.clone();
    for prefix in prefixes {
        if let Some(owner) = state.effective_owner(&prefix) {
            if owner != state.instance_name {
                continue;
            }
        }
        let already = state.shards.read().unwrap().contains_key(&prefix);
        if already {
            continue;
        }
        // Opening registers the engine in state.shards; the normal
        // drain covers it from then on.
        match state
            .gate
            .get_or_open(&prefix, std::time::Duration::from_secs(20))
            .await
        {
            crate::sharddir::OpenOutcome::Ready(_) => {
                tracing::info!("outbox sweep opened shard {prefix}");
            }
            _ => {
                tracing::debug!("outbox sweep could not open {prefix} (moving/contended)");
            }
        }
    }
    tombstone_walk(state).await;
}

/// Round-22 item 7: the tombstone walk. Dirty-index reconciliation
/// only sees rows that are DIRTY; a closure lost while the row sat
/// clean (crash between the registry tombstone and the committer op,
/// ownership move mid-delete) leaves a nonzero gauge nothing ever
/// revisits. Page the registry RAW — tombstones, expirations and
/// fork-retention included — and for every terminal descriptor whose
/// segments this instance owns, resubmit the close against the
/// PERSISTED logical time. Idempotent: a zero gauge no-ops, and the
/// persisted stamp makes every retry account to the same instant.
/// Fork-retained sources get their durable flag here too.
///
/// Residual (accepted): a closure lost to a crash while the row was
/// clean AND the name recreated under a new epoch before the next
/// sweep replaces the tombstone this walk needs; that incarnation's
/// gauge is then reachable only through the dirty-path reconciler.
pub async fn tombstone_walk(state: &std::sync::Arc<crate::http::AppState>) {
    if state.usage_key.is_none() {
        return;
    }
    let mut after: Option<String> = None;
    loop {
        let page = match state.registry.list_page_raw(after.as_deref(), 256).await {
            Ok(p) => p,
            Err(e) => {
                tracing::warn!("tombstone walk paused (registry list): {e}");
                return;
            }
        };
        for d in &page.streams {
            let expired = d
                .expires_at_ms
                .is_some_and(|e| billing_now_ms() >= e);
            let retained = d.soft_deleted && !d.deleted;
            let terminal = d.deleted || expired;
            if !terminal && !retained {
                continue;
            }
            let seg_ids: Vec<u32> = d
                .segments
                .as_ref()
                .map(|m| m.segments.iter().map(|sg| sg.seg_id).collect())
                .unwrap_or_else(|| vec![0]);
            for sid in seg_ids {
                let route = d.segment_route_by_id(sid);
                // Foreign routes are skipped — every instance walks the
                // same registry and closes what IT owns.
                let Ok(engine) = state.engine_for(&route).await else {
                    continue;
                };
                let hash = d.dynamic_segment_identity(sid);
                let Some(meta) = engine.billing_meta(hash).await else {
                    continue;
                };
                if meta.stream_id != d.stream_epoch {
                    continue;
                }
                if terminal && meta.owned_frame_bytes_current > 0 {
                    let close_ms = if d.deleted {
                        d.logical_close_ms.unwrap_or_else(billing_now_ms)
                    } else {
                        d.expires_at_ms.unwrap_or_else(billing_now_ms)
                    };
                    tracing::info!(
                        "tombstone walk: closing {}#{sid} ({} B) at persisted {}",
                        d.name,
                        meta.owned_frame_bytes_current,
                        close_ms
                    );
                    if let Err(e) = engine.submit_billing_close(hash, close_ms).await {
                        tracing::warn!("tombstone-walk close failed for {}: {e}", d.name);
                    }
                } else if retained && !meta.retained_by_forks {
                    if let Err(e) = engine.submit_billing_retained(hash, true).await {
                        tracing::warn!("tombstone-walk retain failed for {}: {e}", d.name);
                    }
                }
            }
        }
        if page.exhausted || page.next_after.is_none() {
            return;
        }
        after = page.next_after;
    }
}

// ---------------------------------------------------------------------
// Fleet-safe system-stream client (round-21 blocker 5)
// ---------------------------------------------------------------------

/// Append to a reserved system stream from ANY fleet member: local
/// first; on an ownership 409 the body relays ONCE to the owner's
/// fleet-internal telemetry endpoint, authenticated with
/// FLEET_INTERNAL_TOKEN and carrying the system key. Ambiguity is
/// safe end to end because every record downstream deduplicates by
/// deterministic id / source sequence.
pub async fn system_append(
    state: &std::sync::Arc<crate::http::AppState>,
    stream: &str,
    key: &str,
    body: Vec<u8>,
) -> Result<(), String> {
    use axum::http::{HeaderMap, HeaderValue};
    debug_assert!(is_reserved_stream(stream));
    let mut hdrs = HeaderMap::new();
    hdrs.insert(
        "stream-encryption-key",
        HeaderValue::from_str(key).map_err(|_| "bad system key".to_string())?,
    );
    hdrs.insert("content-type", HeaderValue::from_static("application/json"));
    // Local attempt (create lazily on 404).
    let mut r = crate::http::append(
        state.clone(),
        stream.to_string(),
        hdrs.clone(),
        axum::body::Body::from(body.clone()),
        None,
        None,
        None,
    )
    .await;
    if r.status() == axum::http::StatusCode::NOT_FOUND {
        let c = crate::http::create_stream(
            state.clone(),
            stream.to_string(),
            hdrs.clone(),
            bytes::Bytes::new(),
        )
        .await;
        let cst = c.status().as_u16();
        if !(cst == 200 || cst == 201 || cst == 409) {
            // A create refused by ownership relays below with the body.
            if crate::http::replay_peer_url(state, &c).is_none() {
                return Err(format!("system create {stream}: {cst}"));
            }
            r = c;
        } else {
            r = crate::http::append(
                state.clone(),
                stream.to_string(),
                hdrs.clone(),
                axum::body::Body::from(body.clone()),
                None,
                None,
                None,
            )
            .await;
        }
    }
    if r.status().is_success() {
        return Ok(());
    }
    // Ownership bounce: relay once to the owner.
    if let Some((_, base)) = crate::http::replay_peer_url(state, &r) {
        let mut req = crate::http::peer_client()
            .post(format!(
                "{base}/v1/internal/telemetry-append/{}",
                crate::http::encode_stream_name_path(stream)
            ))
            .timeout(std::time::Duration::from_secs(20))
            .header("stream-encryption-key", key)
            .header("content-type", "application/json")
            .body(body);
        if let Some(t) = &state.fleet_internal_token {
            req = req.header("authorization", format!("Bearer {t}"));
        }
        match req.send().await {
            Ok(resp) if resp.status().is_success() => return Ok(()),
            Ok(resp) => return Err(format!("telemetry relay {stream}: {}", resp.status())),
            Err(e) => return Err(format!("telemetry relay {stream}: {e}")),
        }
    }
    Err(format!("system append {stream}: {}", r.status()))
}

/// Read a page of a reserved system stream from ANY fleet member:
/// local read; on an ownership 409, one relay hop through the
/// incarnation-bound internal segment read. Returns (json body, next
/// cursor).
pub async fn system_read(
    state: &std::sync::Arc<crate::http::AppState>,
    stream: &str,
    key: &str,
    offset: Option<String>,
) -> Result<Option<(bytes::Bytes, String)>, String> {
    use axum::http::{HeaderMap, HeaderValue};
    let mut hdrs = HeaderMap::new();
    hdrs.insert(
        "stream-encryption-key",
        HeaderValue::from_str(key).map_err(|_| "bad system key".to_string())?,
    );
    let params = crate::http::ReadParams {
        offset: offset.clone(),
        ..Default::default()
    };
    let resp = crate::http::read_inner(
        state.clone(),
        stream.to_string(),
        params,
        hdrs,
        false,
        true,
        crate::http::SseSurface::Raw,
    )
    .await;
    if resp.status() == axum::http::StatusCode::NOT_FOUND {
        return Ok(None);
    }
    if resp.status().is_success() {
        let next = resp
            .headers()
            .get("Stream-Next-Offset")
            .and_then(|v| v.to_str().ok())
            .unwrap_or_default()
            .to_string();
        let body = axum::body::to_bytes(resp.into_body(), 64 << 20)
            .await
            .map_err(|e| e.to_string())?;
        return Ok(Some((body, next)));
    }
    let Some((_, base)) = crate::http::replay_peer_url(state, &resp) else {
        return Err(format!("system read {stream}: {}", resp.status()));
    };
    // Relay through the incarnation-bound internal read.
    let desc = state
        .registry
        .get(stream)
        .await
        .map_err(|e| e.to_string())?
        .ok_or_else(|| "system stream descriptor missing".to_string())?;
    let Some(target) = crate::product::InternalTarget::of(&desc, 0) else {
        return Err("system stream has no epoch".into());
    };
    let q = offset
        .map(|o| format!("?offset={}", urlencode(&o)))
        .unwrap_or_default();
    let mut req = crate::http::peer_client()
        .get(format!(
            "{base}/v1/internal/segment-read/{}{q}",
            crate::http::encode_stream_name_path(stream)
        ))
        .timeout(std::time::Duration::from_secs(20))
        .header("stream-encryption-key", key);
    for (k, v) in target.headers() {
        req = req.header(k, v);
    }
    if let Some(t) = &state.fleet_internal_token {
        req = req.header("authorization", format!("Bearer {t}"));
    }
    match req.send().await {
        Ok(resp) if resp.status().is_success() => {
            let next = resp
                .headers()
                .get("stream-next-offset")
                .and_then(|v| v.to_str().ok())
                .unwrap_or_default()
                .to_string();
            let body = resp.bytes().await.map_err(|e| e.to_string())?;
            Ok(Some((body, next)))
        }
        Ok(resp) => Err(format!("system read relay: {}", resp.status())),
        Err(e) => Err(format!("system read relay: {e}")),
    }
}

fn urlencode(s: &str) -> String {
    let mut out = String::new();
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' => {
                out.push(b as char)
            }
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}
