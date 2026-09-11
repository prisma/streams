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

mod read_accumulator;
pub(crate) use read_accumulator::{
    READ_FLUSH_INTERVAL_MS, READ_SEALED_MAX_BATCHES, ReadUsageAccumulator, RowDelta,
};

mod read_spool;
pub(crate) use read_spool::ReadSpool;

// ---------------------------------------------------------------------
// Reserved system streams
// ---------------------------------------------------------------------

pub(crate) const USAGE_STREAM: &str = "_usage";
pub(crate) const OPS_METRICS_STREAM: &str = "_ops_metrics";
pub(crate) const OPS_EVENTS_STREAM: &str = "_ops_events";
pub(crate) const AUDIT_EVENTS_STREAM: &str = "_audit_events";

/// The reserved internal namespaces. Reserved streams are invisible to
/// the customer catalog, refused on every public surface (raw and
/// product), excluded from customer usage and limits, and reachable
/// only through the fleet-internal telemetry path. Reserving the whole
/// `_`-prefix (not just the three current names) keeps the namespace
/// available for future system streams without a migration.
// mt-lint: allow(name-param-shared-core): name-SHAPE predicate (reserved prefix), no identity derived
pub(crate) fn is_reserved_stream(name: &str) -> bool {
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
pub(crate) struct BillingIdentity {
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
pub(crate) struct MeterSource {
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
pub(crate) fn billing_now_ms() -> i64 {
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
pub(crate) static BILLING_CLOCK_OVERRIDE: std::sync::atomic::AtomicI64 =
    std::sync::atomic::AtomicI64::new(0);

/// Serializes month-sensitive tests: the clock-injecting test takes
/// write; tests asserting real-now months take read.
#[cfg(test)]
pub(crate) fn billing_clock_lock() -> &'static tokio::sync::RwLock<()> {
    static L: std::sync::OnceLock<tokio::sync::RwLock<()>> = std::sync::OnceLock::new();
    L.get_or_init(|| tokio::sync::RwLock::new(()))
}

// ---------------------------------------------------------------------
// UTC month math (no chrono dependency; Hinnant civil-date algorithm)
// ---------------------------------------------------------------------

/// (year, month 1..=12) of a UTC millisecond timestamp.
#[expect(
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    reason = "utc_year_month; the civil-date algorithm yields a month in 1..=12 and a year far inside i32 for every i64 millisecond count; checked conversions would add failure paths no input reaches"
)]
pub(crate) fn utc_year_month(ms: i64) -> (i32, u32) {
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
pub(crate) fn month_start_ms(year: i32, month: u32) -> i64 {
    let y = i64::from(if month <= 2 { year - 1 } else { year });
    let m = i64::from(month);
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let mp = if m > 2 { m - 3 } else { m + 9 };
    let doy = (153 * mp + 2) / 5;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    (era * 146_097 + doe - 719_468) * 86_400_000
}

pub(crate) fn next_month(year: i32, month: u32) -> (i32, u32) {
    if month == 12 {
        (year + 1, 1)
    } else {
        (year, month + 1)
    }
}

/// Display/key form: "2026-08".
pub(crate) fn month_str(year: i32, month: u32) -> String {
    format!("{year:04}-{month:02}")
}

/// Parse "YYYY-MM". Strict: exactly 7 chars, month 1..=12.
pub(crate) fn parse_month(s: &str) -> Option<(i32, u32)> {
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
pub(crate) struct SegmentBillingMetaV1 {
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
    pub(crate) fn month_byte_ms(&self) -> u128 {
        self.month_storage_byte_ms.parse().unwrap_or(0)
    }

    /// Advance the storage integral to `now_ms`, splitting at UTC month
    /// boundaries. Each closed month's final accumulators are handed to
    /// `on_month_close` BEFORE the month fields reset — the committer
    /// stages them into the usage outbox in the same WriteBatch.
    pub(crate) fn advance_storage_clock(
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
            let dt = u128::try_from((upto - self.storage_accounted_through_ms).max(0)).unwrap_or(0);
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
pub(crate) struct BillingRef {
    pub identity: BillingIdentity,
    pub segment_id: u32,
}

impl SegmentBillingMetaV1 {
    /// The `_usage` view of this row (live or closed-month final).
    pub(crate) fn to_snapshot(&self, month_final: bool) -> SegmentSnapshot {
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
pub(crate) struct UsageEnvelope {
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
pub(crate) enum UsagePayload {
    SegmentSnapshot(SegmentSnapshot),
    ReadBatch(ReadBatch),
    StreamLifecycle(StreamLifecycle),
    UsageCorrection(UsageCorrection),
}

/// Exact durable ingest/storage state for one segment, either the live
/// row (`month_final: false`) or a closed month's final numbers.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct SegmentSnapshot {
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
    pub(crate) fn deterministic_event_id(&self) -> String {
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
pub(crate) struct ReadBatch {
    pub source: MeterSource,
    pub seq: u64,
    pub from_ms: i64,
    pub to_ms: i64,
    pub rows: Vec<ReadRow>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct ReadRow {
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
pub(crate) struct StreamLifecycle {
    #[serde(flatten)]
    pub identity: BillingIdentity,
    /// created | soft_deleted | hard_deleted | expired | fork_retained
    pub transition: String,
    pub at_ms: i64,
}

impl StreamLifecycle {
    pub(crate) fn deterministic_event_id(&self) -> String {
        format!("life/{}/{}", self.identity.stream_id, self.transition)
    }
}

/// Explicit post-close correction — finalized months are never silently
/// rewritten.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct UsageCorrection {
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
pub(crate) fn billing_meta_key(hash: &[u8; 16]) -> Vec<u8> {
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
pub(crate) const USAGE_DIRTY_SENTINEL: [u8; 16] = [0xFF; 16];

pub(crate) fn usage_dirty_key(hash: &[u8; 16]) -> Vec<u8> {
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
pub(crate) fn usage_month_final_key(hash: &[u8; 16], year: i32, month: u32) -> Vec<u8> {
    let mut k = Vec::with_capacity(40);
    k.extend_from_slice(&USAGE_DIRTY_SENTINEL);
    k.push(b'V');
    k.extend_from_slice(hash);
    k.extend_from_slice(month_str(year, month).as_bytes());
    k
}

#[cfg(test)]
mod tests;

// ---------------------------------------------------------------------
// Read-delivery meter (§7): ONE accumulator at the public response
// coordinator
// ---------------------------------------------------------------------

/// The BillingIdentity for a descriptor, with deployment defaults for
/// descriptors created before the cutover. Counts feed misses — use
/// on METERING paths only.
pub(crate) fn identity_of(
    state: &crate::http::AppState,
    desc: &crate::registry::StreamDesc,
) -> BillingIdentity {
    identity_inner(state, desc, true)
}

/// Same resolution WITHOUT the miss counter — for read-only query
/// paths (usage GETs), so dashboard polling of a feed-lagged project
/// cannot inflate a counter named "meter events".
pub(crate) fn identity_of_query(
    state: &crate::http::AppState,
    desc: &crate::registry::StreamDesc,
) -> BillingIdentity {
    identity_inner(state, desc, false)
}

fn identity_inner(
    state: &crate::http::AppState,
    desc: &crate::registry::StreamDesc,
    count_miss: bool,
) -> BillingIdentity {
    identity_with_capabilities(&state.auth, &state.deployment, desc, count_miss)
}

pub(crate) fn identity_with_capabilities(
    auth: &crate::auth::AuthService,
    deployment: &crate::deployment::DeploymentIdentity,
    desc: &crate::registry::StreamDesc,
    count_miss: bool,
) -> BillingIdentity {
    // Stage 7 (workspace-at-event): under shadow/enforce the billable
    // owner is the WORKSPACE the policy snapshot names for the
    // descriptor's project AT METERING TIME — invoices attach to the
    // owner-at-event, which is what makes transfer splits possible.
    // A project absent from the snapshot (feed lag, removal mid-sweep)
    // falls to the deployment account and is COUNTED under enforce so
    // reconciliation sees it (shadow's deployment project is absent
    // from the platform feed by construction — counting there would
    // drown the signal in noise); in Off mode the deployment account
    // is the single-tenant truth.
    let account_id = if auth.mode != crate::auth::AuthMode::Off {
        match auth.workspace_for(&desc.project_id) {
            Some(ws) => ws.as_str().to_string(),
            None => {
                if count_miss && auth.mode == crate::auth::AuthMode::Enforce {
                    UNOWNED_METER_EVENTS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                }
                deployment.account_id().to_string()
            }
        }
    } else {
        desc.account_id
            .clone()
            .unwrap_or_else(|| deployment.account_id().to_string())
    };
    BillingIdentity {
        account_id,
        // Layout 4: the descriptor's project is MANDATORY — the
        // deployment-global fallback is gone (MULTITENANCY §20).
        project_id: desc.project_id.as_str().to_string(),
        stream_id: desc.stream_epoch.clone(),
        stream_name: desc.name.clone(),
    }
}

/// Metering events whose project had NO owner in the policy snapshot
/// (attributed to the deployment account) — exported in the ops
/// snapshot as unowned_meter_events_total; a nonzero steady rate means
/// the feed is lying or lagging.
pub(crate) static UNOWNED_METER_EVENTS: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

/// Segment rows whose STORED workspace disagrees with the current
/// policy resolution at emission time (feed-lag first-append capture,
/// or a transfer awaiting the §12.1 split). Detection only — mutating
/// the stored identity mid-month would double-count (rollup deltas are
/// absolute per account row); the committer heals at the next month
/// boundary. Exported as segment_identity_drift_total.
pub(crate) static SEGMENT_IDENTITY_DRIFT: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

/// Meter one externally delivered read page (op + payload bytes).
pub(crate) fn meter_read(
    state: &crate::http::AppState,
    desc: &crate::registry::StreamDesc,
    bytes: u64,
    records: u64,
) {
    state.billing.meter_read(
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
pub(crate) fn meter_read_chunk(
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
pub(crate) fn meter_pull(
    state: &crate::http::AppState,
    desc: &crate::registry::StreamDesc,
    bytes: u64,
    records: u64,
) {
    state.billing.meter_read(
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
pub(crate) fn meter_queue_op(state: &crate::http::AppState, desc: &crate::registry::StreamDesc) {
    state.billing.meter_read(
        &identity_of(state, desc),
        RowDelta {
            queue_operations: 1,
            ..Default::default()
        },
    );
}

/// Count an accepted append request (informational dimension §4.5; the
/// BILLED ingest bytes come from the committer, not this counter).
pub(crate) fn meter_append_request(
    state: &crate::http::AppState,
    desc: &crate::registry::StreamDesc,
) {
    state.billing.meter_read(
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
pub(crate) fn billing_required(cfg: &crate::config::BillingConfig) -> bool {
    cfg.mode_env.as_deref() == Some("required")
}

/// Drain step 1: move sealed batches into the durable spool. On a
/// mid-loop store fault the failed batch AND the not-yet-persisted
/// remainder requeue at the accumulator head (round-22 item 2a) — a
/// fault after batch k of n must leave batches k..n in memory and
/// 1..k in the spool, nothing dropped.
pub(crate) async fn spool_sealed(
    acc: &ReadUsageAccumulator,
    spool: &ReadSpool,
    max: usize,
) -> Result<(), String> {
    let mut sealed = crate::billing_service::ReadDrain::new(acc, max);
    if sealed.batches.is_empty() {
        return Ok(());
    }
    // One WriteBatch + one flush for the whole round (OOM review item
    // 5). persist_all is all-or-nothing: on error nothing became
    // durable, so the WHOLE drained set — not just a suffix — requeues
    // (round-22 item 2a's no-loss guarantee, now trivially whole-set).
    spool
        .persist_all(&sealed.batches)
        .await
        .map_err(|e| format!("read spool persist: {e}"))?;
    // Until durable acceptance, Drop returns the whole set even when the
    // future is cancelled at a storage await. An ambiguous accepted write
    // may replay; the unchanged source/sequence identity deduplicates it.
    sealed.accepted();
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

/// One engine's acknowledgement of a drained row: the engine, the stream,
/// the scope acknowledged and the spooled keys it covers.
type UsageAck = (
    std::sync::Arc<crate::shard::ShardEngine>,
    [u8; 16],
    crate::shard::UsageAckScope,
    Vec<Vec<u8>>,
);

/// One drain round (§6.3 steps 1-5): emit sealed read batches and every
/// dirty segment snapshot (+ closed-month finals) to `_usage`, then
/// acknowledge exactly what was emitted. Idempotent under every crash
/// interleaving: a lost emission leaves dirty state; a lost ack
/// re-emits an identical snapshot the rollup deduplicates.
#[expect(
    clippy::too_many_lines,
    reason = "drain_once; one drain round is one ledger append whose admission, emission and acknowledgement must stay in a single visible sequence; splitting it would hide which side effect each crash interleaving reaches"
)]
pub(crate) async fn drain_once(
    state: &std::sync::Arc<crate::http::AppState>,
) -> Result<usize, String> {
    let Some(key) = state.billing.usage_key() else {
        return Ok(0);
    };
    let cell = state.deployment.cell_id().as_str().to_string();
    let mut envelopes: Vec<UsageEnvelope> = Vec::new();

    // 1. Read batches: seal, then SPOOL DURABLY before anything can
    // forget them (round-21 blocker 3). The ledger is fed from the
    // spool, and a spooled batch is deleted only after `_usage`
    // acknowledged — a ledger outage accumulates on disk while the
    // in-memory accumulator keeps rotating normally.
    state.billing.seal_aged_reads(READ_FLUSH_INTERVAL_MS);
    let mut spooled_keys: Vec<Vec<u8>> = Vec::new();
    let mut memless = state.billing.read_drain(0);
    let mut body_bytes = 0usize;
    if state.billing.read_spool_open() {
        state.billing.spool_sealed_reads(64).await?;
        for (key, rb) in state.billing.pending_spooled(64).await? {
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
    } else if billing_required(&state.config.billing) {
        // Round-22 item 2b: required mode has NO memory-only window.
        // Until the spool is open, drains fail (the meter keeps
        // accumulating; nothing is emitted from volatile state).
        return Err(
            "read spool not open (BILLING_MODE=required refuses the memory-only path)".into(),
        );
    } else {
        // No spool configured (bare test rigs): the pre-spool path.
        memless = state.billing.read_drain(16);
        for rb in &memless.batches {
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
    let engines = state.billing.drain_engines(state.shards.engines(), 4);
    let mut acks: Vec<UsageAck> = Vec::new();
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
        state.billing.begin_drain_engine(&engine.prefix);
        // Round-22 item 6: financial scans fail CLOSED. A scan error
        // skips this engine's contribution entirely — no emit and no
        // ack, so every dirty row stays dirty and the next round
        // retries — instead of treating "scan failed" as "nothing
        // exists" and acking finals away against an empty list.
        let (dirty, more_dirty) = match engine
            .usage_dirty_page(state.billing.drain_row_cursor(&engine.prefix), 64)
            .await
        {
            Ok(page) => page,
            Err(e) => {
                tracing::warn!("usage dirty scan failed (engine drain deferred): {e}");
                continue;
            }
        };
        if dirty.is_empty() {
            state.billing.set_drain_row_cursor(&engine.prefix, None);
            continue;
        };
        for (hash, version) in dirty {
            state
                .billing
                .set_drain_row_cursor(&engine.prefix, Some(hash));
            let (row_finals, more_finals) = match engine.usage_month_finals_page(hash, 32).await {
                Ok(page) => page,
                Err(error) => {
                    tracing::error!("month-final read failed (dirty row deferred): {error}");
                    continue;
                }
            };
            let mut meta = match engine.load_billing_meta(hash).await {
                Ok(Some(meta)) => meta,
                Ok(None) => continue,
                Err(error) => {
                    tracing::error!("billing drain metadata read failed: {error}");
                    continue;
                }
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
            let snap_probe = meta.to_snapshot(false);
            let mut row_bytes = LIFECYCLE_EST
                + encoded_size(&envelope(
                    &cell,
                    UsagePayload::SegmentSnapshot(snap_probe),
                    meta.storage_accounted_through_ms,
                    String::new(),
                ));
            for (_, fs) in &row_finals {
                row_bytes += encoded_size(&envelope(
                    &cell,
                    UsagePayload::SegmentSnapshot(fs.clone()),
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
            // Stage 7 review fix (P0): resolve under the row's OWN
            // project — the row carries it. Resolving under the
            // deployment sref made every live enforce-mode project
            // stream look "gone" and spuriously billing-closed it,
            // zeroing its storage gauge each drain. Rows without a
            // project predate layout 4 and were deployment-owned by
            // definition.
            let row_ref = if meta.project_id.is_empty() {
                // mt-lint: allow(raw-adapter-sref): pre-Stage-7 rows carry no
                // project and are deployment-owned by definition (reviewed
                // Stage 7 P0); a layout-4 GA cell boots with no such rows.
                state.deployment.raw_adapter_sref(&meta.stream_name)
            } else {
                match crate::tenant::ProjectId::new(&meta.project_id) {
                    // mt-lint: allow(stream-ref-construction): the ref is rebuilt from the row's OWN persisted project — durable attribution, not a request name (Stage 7 P0)
                    Ok(p) => p.stream_ref(&meta.stream_name),
                    Err(_) => {
                        // Fail closed: defer, never close on a row we
                        // cannot even attribute.
                        tracing::warn!(
                            "unparseable project {:?} on usage row for {} (deferred)",
                            meta.project_id,
                            meta.stream_name
                        );
                        continue;
                    }
                }
            };
            match state.registry.get(&row_ref).await {
                Ok(Some(d)) if d.stream_epoch == meta.stream_id => {
                    if d.soft_deleted && !d.deleted {
                        let was_retained = meta.retained_by_forks;
                        meta.retained_by_forks = true;
                        // Round-22 item 7: the fork billing contract is
                        // persisted ON the row, not only in emitted
                        // snapshots — it must survive restarts and
                        // ownership moves.
                        if !was_retained
                            && let Err(e) = engine.submit_billing_retained(hash, true).await
                        {
                            tracing::warn!(
                                "retained-by-forks persist failed for {}: {e}",
                                meta.stream_name
                            );
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
                        let env = envelope(&cell, UsagePayload::StreamLifecycle(lc), close_ms, id);
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
            // Stage 7 review: DETECT drift between the row's stored
            // workspace and the current policy resolution. No mutation
            // here — that would double-count the month; the committer
            // re-stamps at the next month boundary, and this counter
            // makes the interim visible to reconciliation.
            if state.auth.mode == crate::auth::AuthMode::Enforce
                && !meta.project_id.is_empty()
                && !is_reserved_stream(&meta.stream_name)
                && let Ok(p) = crate::tenant::ProjectId::new(&meta.project_id)
                && let Some(ws) = state.auth.workspace_for(&p)
                && ws.as_str() != meta.account_id
            {
                SEGMENT_IDENTITY_DRIFT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }
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
            // Partial final pages retain discovery independently of the
            // metadata version, including a legacy version-zero row.
            acks.push((
                engine.clone(),
                hash,
                if more_finals {
                    crate::shard::UsageAckScope::FinalRowsOnly
                } else {
                    crate::shard::UsageAckScope::ThroughVersion(ver)
                },
                final_keys,
            ));
        }
        if !more_dirty {
            state.billing.set_drain_row_cursor(&engine.prefix, None);
        }
    }

    if envelopes.is_empty() {
        return Ok(0);
    }
    let body = serde_json::to_vec(&envelopes).map_err(|e| e.to_string())?;
    match usage_ledger_append(state, &key, body).await {
        Ok(()) => {
            memless.accepted();
            // Spooled batches leave the spool ONLY now, after the
            // ledger acknowledged durably.
            state.billing.remove_spooled(&spooled_keys).await?;
            for (engine, hash, scope, finals) in acks {
                match scope {
                    crate::shard::UsageAckScope::ThroughVersion(version) => {
                        engine.submit_usage_ack(hash, version, finals)
                    }
                    crate::shard::UsageAckScope::FinalRowsOnly => {
                        engine.submit_usage_final_ack(hash, finals)
                    }
                }
            }
            Ok(envelopes.len())
        }
        Err(e) => {
            // Spooled batches stay durable in the spool; memless ones
            // requeue at the accumulator's front.
            // ReadDrain's Drop requeues optional-mode volatile batches,
            // including cancellation before this match is reached.
            Err(e)
        }
    }
}

/// Open the durable read spool (verified readable — `ReadSpool::open`
/// scans pending rows before returning) and register it on the state.
/// Required mode calls this SYNCHRONOUSLY before the instance serves
/// (round-22 items 2b/10): there is no memory-only window in which a
/// crash could lose metered reads.
pub(crate) async fn open_read_spool(
    state: &std::sync::Arc<crate::http::AppState>,
) -> anyhow::Result<()> {
    if state.billing.read_spool_open() {
        return Ok(());
    }
    let prefix = state
        .config
        .billing
        .path_prefix_env
        .clone()
        .unwrap_or_default();
    let sp = ReadSpool::open_with_cache(
        state.data_store.clone(),
        &prefix,
        state.ownership.instance(),
        &state.config,
        state.runtime.telemetry.cache.clone(),
    )
    .await?;
    if state
        .billing
        .install_read_spool(std::sync::Arc::new(sp))
        .is_err()
    {
        tracing::warn!("read spool already installed; keeping the first");
    }
    Ok(())
}

/// The drainer task: every TELEMETRY_DRAIN_SECS (default 2), one drain
/// round. Errors log and retry — the durable outbox holds the truth.
pub(crate) fn spawn_telemetry(
    state: std::sync::Arc<crate::http::AppState>,
    tasks: &crate::tasks::TaskSupervisor,
) {
    if state.billing.usage_key().is_none() {
        tracing::info!("telemetry pipeline off (USAGE_STREAM_KEY unset)");
        return;
    }
    // Open the durable read spool before the first drain (required
    // mode already opened it synchronously at startup); ownership
    // sweep runs at start and every OUTBOX_SWEEP_SECS.
    {
        let st = state.clone();
        if let Err(rejected) = tasks.spawn(
            "telemetry-outbox-sweep",
            crate::tasks::Policy::Critical,
            move |cancel| async move {
                tokio::select! {
                    biased;
                    _ = cancel.cancelled() => return crate::tasks::TaskResult::Done,
                    _ = async {
                        if let Err(e) = open_read_spool(&st).await {
                            tracing::error!("read spool open failed: {e}");
                        }
                        sweep_owned_outboxes(&st).await;
                    } => {}
                }
                let sweep_secs: u64 = st.config.billing.outbox_sweep_secs;
                loop {
                    tokio::select! {
                        _ = cancel.cancelled() => return crate::tasks::TaskResult::Done,
                        _ = tokio::time::sleep(std::time::Duration::from_secs(sweep_secs)) => {}
                    }
                    tokio::select! {
                        biased;
                        _ = cancel.cancelled() => return crate::tasks::TaskResult::Done,
                        _ = sweep_owned_outboxes(&st) => {}
                    }
                }
            },
        ) {
            tracing::warn!("telemetry-outbox-sweep not spawned: {rejected:?}");
        }
    }
    let secs: u64 = state.config.billing.telemetry_drain_secs;
    let metrics_secs: u64 = state.config.billing.metrics_interval_secs;
    if let Err(rejected) = tasks.spawn(
        "telemetry-drain",
        crate::tasks::Policy::Critical,
        move |cancel| async move {
            let mut tick = tokio::time::interval(std::time::Duration::from_secs(secs.max(1)));
            let mut last_metrics = None;
            loop {
                tokio::select! {
                    _ = cancel.cancelled() => return crate::tasks::TaskResult::Done,
                    _ = tick.tick() => {}
                }
                tokio::select! {
                    biased;
                    _ = cancel.cancelled() => return crate::tasks::TaskResult::Done,
                    _ = async {
                        match drain_once(&state).await {
                            Ok(_) => {
                                state.runtime.telemetry.drain_succeeded(state.runtime.clock.now());
                            }
                            Err(e) => tracing::warn!("usage drain: {e}"),
                        }
                        if let Err(e) = crate::ops::drain_ops_once(&state).await {
                            tracing::warn!("ops drain: {e}");
                        }
                        if let Err(e) = crate::audit::drain_audit_once(&state).await {
                            tracing::warn!("audit drain: {e}");
                        }
                        if let Err(e) = crate::fleet::drain_fleet_events(&state).await {
                            tracing::warn!("fleet event drain: {e}");
                        }
                        let now = state.runtime.clock.monotonic();
                        if last_metrics.is_none_or(|previous| {
                            now.since(previous) >= std::time::Duration::from_secs(metrics_secs)
                        }) {
                            last_metrics = Some(now);
                            if let Err(e) = crate::ops::emit_metrics_once(&state).await {
                                tracing::warn!("ops metrics emit: {e}");
                            }
                        }
                    } => {}
                }
            }
        },
    ) {
        tracing::warn!("telemetry-drain not spawned: {rejected:?}");
    }
}

// ---------------------------------------------------------------------
// The rollup consumer (§9.3) and month closer (§9.4-§9.6)
// ---------------------------------------------------------------------

/// One rollup step: read the next `_usage` page from the stored cursor
/// through the normal in-process read path, apply it transactionally,
/// advance. Returns envelopes applied (0 = caught up).
pub(crate) async fn rollup_step(
    state: &std::sync::Arc<crate::http::AppState>,
) -> Result<usize, String> {
    let Some(rollup) = state.rollup.get() else {
        return Ok(0);
    };
    let Some(key) = state.billing.usage_key() else {
        return Ok(0);
    };
    use axum::http::{HeaderMap, HeaderValue};
    let mut hdrs = HeaderMap::new();
    hdrs.insert(
        "stream-encryption-key",
        HeaderValue::from_str(&key).map_err(|_| "bad usage key".to_string())?,
    );
    let cursor = rollup
        .cursor()
        .await
        .map_err(|e| e.to_string())?
        .filter(|c| !c.is_empty());
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
    let t_apply = std::time::Instant::now();
    rollup
        .apply_page(&envelopes, &next)
        .await
        .map_err(|e| e.to_string())?;
    state.runtime.telemetry.rollup_applied(
        state.runtime.clock.now(),
        u64::try_from(t_apply.elapsed().as_millis()).unwrap_or(u64::MAX),
    );
    Ok(envelopes.len())
}

/// One ops-metrics rollup step (§13.1): consume `_ops_metrics` from
/// its own cursor into raw + m1 tiers.
pub(crate) async fn ops_rollup_step(
    state: &std::sync::Arc<crate::http::AppState>,
) -> Result<usize, String> {
    let Some(rollup) = state.rollup.get() else {
        return Ok(0);
    };
    let Some(key) = state.billing.usage_key() else {
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

/// Process-wide lifetime count of tombstone closure submissions.
pub(crate) static WALK_CLOSE_SUBMITS: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

/// Monthly-artifact content mismatches observed at publication (an
/// AlreadyExists object whose bytes differ from the frozen row). Any
/// nonzero value is a standing operator alert: an immutable invoice
/// path holds content we did not stage.
pub(crate) static ARTIFACT_MISMATCHES: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

/// Publish pending monthly + correction artifacts (blocker 7 phase 2,
/// hardened by round-22 item 8): create-only PUT of each staged body
/// to its immutable path. AlreadyExists is trusted ONLY after reading
/// the object back and verifying its bytes equal what we staged — an
/// equal body is an earlier successful attempt; a different body is a
/// financial-integrity alarm and the row stays pending for an
/// operator, never silently marked published. Anything else stays
/// pending and retries next tick and after restart.
pub(crate) async fn publish_artifacts(
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
            Err(object_store::Error::AlreadyExists { .. }) => match store.get(&opath).await {
                Ok(r) => match r.bytes().await {
                    Ok(existing) if existing.as_ref() == body => true,
                    Ok(existing) => {
                        ARTIFACT_MISMATCHES.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
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
            },
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
            format!(
                "telemetry/usage-monthly/{acct_proj}/{stream_id}/{month}.corrections/{safe}.json"
            )
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
/// Open the rollup DB and register it on the state. Required mode
/// calls this synchronously BEFORE serving (round-22 item 10): a
/// rollup instance that cannot open its database is not ready.
pub(crate) async fn open_rollup(
    state: &std::sync::Arc<crate::http::AppState>,
    prefix: &str,
) -> anyhow::Result<()> {
    if state.rollup.get().is_some() {
        return Ok(());
    }
    let r = crate::rollup::UsageRollup::open_with_cache(
        state.data_store.clone(),
        prefix,
        &state.config,
        state.runtime.telemetry.cache.clone(),
    )
    .await?;
    if state.rollup.install(std::sync::Arc::new(r)).is_err() {
        tracing::warn!("usage rollup already installed; keeping the first");
    }
    Ok(())
}

#[expect(
    clippy::unwrap_used,
    reason = "spawn_rollup; open_rollup installs the rollup before this task is spawned, so the slot is populated for the task's whole life; a fallible read would turn a startup-order invariant into a runtime branch"
)]
pub(crate) fn spawn_rollup(
    state: std::sync::Arc<crate::http::AppState>,
    prefix: String,
    tasks: &crate::tasks::TaskSupervisor,
) {
    if let Err(rejected) = tasks.spawn(
        "usage-rollup",
        crate::tasks::Policy::Critical,
        move |cancel| async move {
            tokio::select! {
                biased;
                _ = cancel.cancelled() => return crate::tasks::TaskResult::Done,
                opened = open_rollup(&state, &prefix) => if let Err(e) = opened {
                    tracing::error!("usage rollup open failed: {e}");
                    return crate::tasks::TaskResult::Failed(format!("usage rollup open failed: {e}"));
                },
            }
            tracing::info!("usage rollup running");
            let grace_ms: i64 = state.config.billing.month_close_grace_ms;
            let mut last_close = None;
            loop {
                // Every application page commits its cursor with its rows;
                // pending artifacts remain durable until verified publication.
                // Dropping any active phase therefore leaves replayable debt.
                tokio::select! {
                    biased;
                    _ = cancel.cancelled() => return crate::tasks::TaskResult::Done,
                    _ = async {
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
                        let monotonic = state.runtime.clock.monotonic();
                        if last_close.is_none_or(|previous| {
                            monotonic.since(previous) > std::time::Duration::from_secs(3600)
                        }) {
                            last_close = Some(monotonic);
                            let now = state.runtime.clock.now().ms();
                            let rollup2 = state.rollup.get().unwrap().clone();
                            let store2 = state.data_store.clone();
                            let pfx = prefix.clone();
                            if let Ok(n) = rollup2.sweep_ops_raw(now, 10_000).await
                                && n > 0
                            {
                                tracing::info!("ops raw retention: {n} points expired");
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
                    } => {}
                }
            }
        },
    ) {
        tracing::warn!("usage-rollup not spawned: {rejected:?}");
    }
}

// ---------------------------------------------------------------------
// Durable read spool (round-21 blocker 3)
// ---------------------------------------------------------------------

/// Telemetry databases use a shared, runtime-owned cache supplied by their
/// caller. Their settings remain bounded separately from shard/history DBs.
/// Telemetry DBs are tiny and quiet next to shard/history DBs: small
/// memtables, small L0 targets, long metadata polls, and the same slow
/// GC cadence as history. Never SlateDB defaults.
pub(crate) fn telemetry_settings(
    _cfg: &crate::config::BillingConfig,
    compactor: &slatedb::config::CompactorOptions,
) -> slatedb::config::Settings {
    let mut gc = slatedb::config::Settings::default()
        .garbage_collector_options
        .unwrap_or_default();
    for slot in [
        &mut gc.wal_options,
        &mut gc.manifest_options,
        &mut gc.compacted_options,
        &mut gc.compactions_options,
    ] {
        *slot = Some(slatedb::config::GarbageCollectorDirectoryOptions {
            interval: Some(std::time::Duration::from_secs(600)),
            ..slot.unwrap_or_default()
        });
    }
    // R28 review: `..Default::default()` silently gave the telemetry
    // DBs the UPSTREAM compaction worker (concurrency 4, 4
    // subcompactions, 4x2 MiB read-ahead, 256 MiB rolls) beside the
    // bounded shard DBs — the exact cross-DB working-set overlap the
    // R27-4 posture exists to prevent. Every DB shares the one resolved
    // compactor profile; only the poll cadence stays telemetry-slow
    // (compactor polls read the manifest, and telemetry is billed to
    // stay cheap).
    let mut co = compactor.clone();
    co.poll_interval = std::time::Duration::from_secs(5);
    slatedb::config::Settings {
        wal_enabled: false,
        flush_interval: Some(std::time::Duration::from_millis(200)),
        manifest_poll_interval: std::time::Duration::from_secs(300),
        garbage_collector_options: Some(gc),
        compression_codec: Some(slatedb::config::CompressionCodec::Zstd),
        max_unflushed_bytes: 8 * 1024 * 1024,
        l0_sst_size_bytes: 2 * 1024 * 1024,
        l0_max_ssts: 32,
        l0_max_ssts_per_key: 32,
        compactor_options: Some(co),
        ..Default::default()
    }
}

/// Open every shard THIS instance owns so its billing outbox becomes
/// drainable (round-21 blocker 4): the drainer must not depend on
/// customer traffic to rediscover dirty usage after a crash or an
/// ownership move. Bounded by the fleet's shard count.
/// Prefixes the SWEEP (or the tombstone walk) opened solely for
/// telemetry-debt discovery. Customer-opened engines never enter this
/// set and are never closed here.
/// R29 test-isolation fix: scheduler bookkeeping lives on the
/// AppState, not in process statics — parallel DST rigs each hold
/// their own budgeted residents, and a process-global gauge summed
/// them into false bound violations.
#[derive(Default)]
pub(crate) struct SweepSched {
    /// prefix -> custody value for engines this scheduler holds.
    // mt-lint: allow(name-keyed-map): shard prefix (sweep custody), not stream identity
    pub(super) opened: std::sync::Mutex<std::collections::HashMap<String, u64>>,
    /// prefix -> sweeps spent resident (quantum accounting).
    // mt-lint: allow(name-keyed-map): shard prefix (sweep quantum accounting)
    pub(super) cycles: std::sync::Mutex<std::collections::HashMap<String, usize>>,
    /// Peak concurrently scheduler-held engines (per state).
    pub(super) peak: std::sync::atomic::AtomicUsize,
    /// Sweep cycle counter (rotation).
    pub(super) cycle: std::sync::atomic::AtomicUsize,
    /// R30: tombstone-walk continuation — the registry page token to
    /// RESUME from next sweep. Set when the walk stops on a budget
    /// deferral (resume at the deferred descriptor's page), cleared on
    /// exhaustion (wrap to the beginning). Without it every sweep
    /// restarted at the first descriptor, and two early routes with
    /// persistent debt could starve every later terminal stream's
    /// billing closure indefinitely. In-memory: a restart resumes from
    /// the beginning, which costs one extra circle, never correctness
    /// (the walk is idempotent).
    pub(super) walk_cursor: std::sync::Mutex<Option<String>>,
}

/// Sweep-resident engine count (ops gauge): how many engines exist
/// ONLY because debt discovery opened them.
pub(crate) fn sweep_resident_engines(state: &std::sync::Arc<crate::http::AppState>) -> u64 {
    state.billing.sweep_resident_engines() as u64
}

/// Engines whose custody the scheduler currently holds, DERIVED from
/// the serving map (no counter to maintain or leak).
pub(crate) fn scheduler_held(state: &std::sync::Arc<crate::http::AppState>) -> usize {
    state
        .shards
        .engines()
        .iter()
        .filter(|e| e.sweep_custody.load(std::sync::atomic::Ordering::Relaxed) != 0)
        .count()
}

/// OOM review item 6: discovery must not mean PERMANENT residency. A
/// 16-shard topology swept at boot kept 16 full engines (memtables,
/// absorbers, history partitions) resident on a nominally idle
/// instance. Engines this sweep opens are MARKED; once their billing
/// debt is drained (no dirty rows, no month finals) a later pass
/// deliberately closes them again — the same remove+begin_close dance
/// the ownership-yield path uses. Debt probes fail toward KEEPING the
/// engine (discovery must not lose to a transient scan fault), and a
/// customer engine that raced into the mark window costs one benign
/// re-open on its next request.
pub(crate) async fn sweep_owned_outboxes(state: &std::sync::Arc<crate::http::AppState>) {
    let budget = sweep_resident_budget(&state.config.billing);
    let cycle = state.billing.next_sweep_cycle();

    // Phase 1 — audit the scheduler's existing residents. An engine we
    // opened on an earlier cycle is ours to close ONLY while the
    // customer has not adopted it: any external_touches movement since
    // our baseline revokes the scheduler's custody (the engine then
    // belongs to normal traffic + the ownership-yield path). Debt-free
    // residents close; indebted residents occupy budget slots in
    // rotation order.
    let mut retained = 0usize;
    // Shards evicted THIS sweep (quantum expiry / over budget) are
    // skipped by discovery below: without the cooldown the rotation
    // can re-admit a just-evicted resident ahead of shards that have
    // never had a turn (fairness hole found by the rotation gate).
    let mut evicted_now: std::collections::HashSet<String> = std::collections::HashSet::new();
    let mut marked: Vec<String> = state.billing.sweep_custody_prefixes();
    marked.sort();
    let n = marked.len();
    if n > 0 {
        marked.rotate_left(cycle % n);
    }
    for prefix in marked {
        if state.ownership.foreign_owner(&prefix).is_some() {
            unmark(state, &prefix);
            continue;
        }
        let Some(engine) = state.shards.open(&prefix) else {
            unmark(state, &prefix);
            continue;
        };
        if !custody_intact(state, &prefix, &engine) {
            // Customer traffic adopted it: no longer scheduler-owned.
            unmark(state, &prefix);
            continue;
        }
        let debt = probe_debt(&engine).await;
        let cycles = state.billing.note_sweep_cycle(&prefix);
        if debt.any() && retained < budget && cycles <= residence_quantum(&state.config.billing) {
            retained += 1;
            continue;
        }
        if debt.any() {
            evicted_now.insert(prefix.clone());
            tracing::info!(
                unabsorbed_frame_bytes = debt.maintenance,
                trim_debt_streams = debt.trim,
                billing = debt.billing,
                "sweep closes {prefix} over residency budget {budget}; \
                 debt is durable and rotation returns to it"
            );
        }
        close_scheduler_engine(state, &prefix);
    }

    // Phase 2 — bounded discovery, STRICTLY one open at a time and only
    // while budget headroom remains. The R27-2 version opened every
    // owned shard to look for debt and only then closed the surplus —
    // reproducing exactly the cross-DB residency overlap the bound
    // exists to prevent (R28 review). Peak scheduler-held engines is
    // now retained + the single probe in flight, which never exceeds
    // the budget because discovery stops at the budget line.
    let discovery_cap: usize = state.config.billing.sweep_discovery_max;
    let mut discovered = 0usize;
    let mut owned: Vec<String> = state.shards.prefixes().to_vec();
    let n = owned.len();
    if n > 0 {
        owned.rotate_left(cycle % n);
    }
    for prefix in owned {
        if retained >= budget || discovered >= discovery_cap {
            break;
        }
        if evicted_now.contains(&prefix) {
            continue;
        }
        if state.ownership.foreign_owner(&prefix).is_some() {
            unmark(state, &prefix);
            continue;
        }
        if state.shards.is_open(&prefix) {
            // Already resident (customer traffic or an earlier mark):
            // the normal drain covers it; phase 1 audits marks.
            continue;
        }
        match state
            .shards
            .open_or_wait(&prefix, std::time::Duration::from_secs(20))
            .await
        {
            crate::sharddir::OpenOutcome::Ready(_) => {}
            _ => {
                tracing::debug!("sweep discovery could not open {prefix} (moving/contended)");
                continue;
            }
        }
        discovered += 1;
        let Some(engine) = state.shards.open(&prefix) else {
            continue;
        };
        if !mark(state, &prefix, &engine) {
            // Custody declined: a customer resolved this engine already
            // (possibly by coalescing into the open we just started).
            // It is theirs; the normal drain covers any debt.
            continue;
        }
        // Custody is installed before the first awaited probe. Cancellation
        // leaves a tracked resident for the next sweep or runtime shutdown.
        let debt = probe_debt(&engine).await;
        if debt.any() {
            tracing::info!(
                unabsorbed_frame_bytes = debt.maintenance,
                trim_debt_streams = debt.trim,
                billing = debt.billing,
                "sweep retains {prefix} (debt; drain runs while resident)"
            );
            retained += 1;
        } else {
            close_scheduler_engine(state, &prefix);
        }
    }

    // The tombstone walk budgets its own opens (walk_engine_budgeted):
    // over-budget descriptors defer to the next sweep's re-page.
    tombstone_walk(state).await;
}

/// One residency budget for EVERY debt class the scheduler can retain
/// an engine for — billing rows, month finals, maintenance backlog,
/// physical-trim debt (R28 review: an unconditional billing-debt
/// retention let a `_usage` outage keep every owned shard resident,
/// bypassing the bound entirely). Validated at startup: zero would
/// silently starve all cold-debt drain, so it is rejected there.
pub(crate) fn sweep_resident_budget(cfg: &crate::config::BillingConfig) -> usize {
    cfg.sweep_maint_resident.max(1)
}

#[derive(Debug, Clone, Copy)]
struct Debt {
    billing: bool,
    maintenance: u64,
    trim: u64,
}

impl Debt {
    fn any(&self) -> bool {
        self.billing || self.maintenance > 0 || self.trim > 0
    }
}

/// Probe failures count as debt: discovery must not lose to a
/// transient scan fault.
async fn probe_debt(engine: &std::sync::Arc<crate::shard::ShardEngine>) -> Debt {
    let billing = engine.has_billing_debt().await.unwrap_or(true);
    Debt {
        billing,
        maintenance: engine.maintenance_snapshot().unabsorbed_frame_bytes,
        trim: engine.trim_stats().0 as u64,
    }
}

/// R29 custody core. One global adoption sequence orders every
/// external resolution against every custody install; the invariants:
///
///   * custody installs ONLY onto an engine with zero external
///     history — a customer who resolved the engine before the sweep
///     probed it (including one who coalesced into the sweep's own
///     in-flight open) makes the install DECLINE, closing the
///     pre-mark window the R28 baseline model left open;
///   * an external resolution atomically revokes custody
///     (stamp_external, called inside the request path's map guard);
///   * internal paths (tombstone walk, scaler) never stamp, so
///     maintenance cannot leak an engine out of the rotation;
///   * a close succeeds only via compare_exchange on the installer's
///     exact custody value — custody still present implies no
///     external stamp since install.
static ADOPTION_SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// Called from every EXTERNAL engine resolution (http engine_for fast
/// path and its gate Ready path). Held-count is DERIVED from custody
/// flags in the serving map, so revocation is just the swap.
pub(crate) fn stamp_external(engine: &std::sync::Arc<crate::shard::ShardEngine>) {
    let seq = ADOPTION_SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed) + 1;
    engine
        .last_external_seq
        .store(seq, std::sync::atomic::Ordering::Relaxed);
    engine
        .sweep_custody
        .swap(0, std::sync::atomic::Ordering::Relaxed);
}

/// Install scheduler custody. Returns the custody value on success;
/// None means the engine has external history (or gained it during
/// the install race) and the scheduler must treat it as
/// customer-resident.
fn install_custody(engine: &std::sync::Arc<crate::shard::ShardEngine>) -> Option<u64> {
    use std::sync::atomic::Ordering;
    if engine.last_external_seq.load(Ordering::Relaxed) != 0 {
        return None;
    }
    let seq = ADOPTION_SEQ.fetch_add(1, Ordering::Relaxed) + 1;
    engine.sweep_custody.store(seq, Ordering::Relaxed);
    // Re-check: a stamp that landed between the first read and the
    // store has either already revoked (swap saw our value) or carries
    // a newer last_external_seq; both mean decline.
    if engine.last_external_seq.load(Ordering::Relaxed) != 0 {
        if engine
            .sweep_custody
            .compare_exchange(seq, 0, Ordering::Relaxed, Ordering::Relaxed)
            .is_ok()
        {
            // we still held it; release cleanly (no SCHED_HELD yet)
        }
        return None;
    }
    Some(seq)
}

fn residence_quantum(cfg: &crate::config::BillingConfig) -> usize {
    cfg.sweep_resident_quantum.max(1)
}

/// Take scheduler custody of an engine and register it for rotation.
/// False = external history; the engine belongs to customer traffic.
fn mark(
    state: &std::sync::Arc<crate::http::AppState>,
    prefix: &str,
    engine: &std::sync::Arc<crate::shard::ShardEngine>,
) -> bool {
    match install_custody(engine) {
        Some(seq) => {
            // `scheduler_held` derives from the engines' custody stamps
            // (already installed above), so the peak is exact here.
            state
                .billing
                .claim_sweep_custody(prefix, seq, scheduler_held(state));
            true
        }
        None => false,
    }
}

fn unmark(state: &std::sync::Arc<crate::http::AppState>, prefix: &str) {
    state.billing.release_sweep_custody(prefix);
}

/// Still under the custody value we installed? Anything else —
/// revoked by an external stamp, or no record — means adopted.
fn custody_intact(
    state: &std::sync::Arc<crate::http::AppState>,
    prefix: &str,
    engine: &std::sync::Arc<crate::shard::ShardEngine>,
) -> bool {
    let rec = state.billing.sweep_custody_seq(prefix);
    match rec {
        Some(seq) => {
            engine
                .sweep_custody
                .load(std::sync::atomic::Ordering::Relaxed)
                == seq
        }
        None => false,
    }
}

/// Peak concurrently scheduler-held engines, for the DST bound gate.
pub(crate) fn sweep_open_peak(state: &std::sync::Arc<crate::http::AppState>) -> usize {
    state.billing.sweep_peak()
}
#[cfg(test)]
pub(crate) fn sweep_open_peak_reset(state: &std::sync::Arc<crate::http::AppState>) {
    state.billing.reset_sweep_peak();
}
/// Close an engine the scheduler owns. Removal from the map happens
/// FIRST (no new engine_for resolution can hand it out), then the
/// touch counter is re-checked: engine_for increments it INSIDE the
/// map read guard, so any resolution that predates our write lock is
/// visible here — an adopted engine is reinstated instead of closed.
/// The residual is a request that resolved before the MARK-time
/// baseline and is still in flight a whole sweep cycle later; that
/// window is the ownership-move window, which clients already survive
/// by replay contract.
fn close_scheduler_engine(state: &std::sync::Arc<crate::http::AppState>, prefix: &str) {
    use std::sync::atomic::Ordering;
    let Some(seq) = state.billing.sweep_custody_seq(prefix) else {
        return;
    };
    // R30: ONE write guard held through remove -> custody CAS ->
    // possible reinsertion. The previous version released the guard
    // between removal and the CAS, so a request arriving in that gap
    // observed an empty slot and could start a SECOND open while the
    // first engine was about to be reinstated — a replacement engine,
    // spurious fencing, and avoidable in-flight failures. With the
    // guard held, external resolution (which stamps under the READ
    // guard) is strictly ordered against this decision: whatever
    // stamped before we acquired the write lock is visible to the
    // CAS, and nothing can resolve or re-open the prefix until the
    // slot's fate is settled. Only begin_close() runs after release.
    // PR 6-A: the directory holds that one guard through remove ->
    // decide -> reinstate; the custody CAS is the decision.
    // PR 6.1.1-B: ONE retirement — the custody CAS is the decision, and
    // the directory removes, arms the holdoff and closes as one step.
    match state.shards.retire(
        prefix,
        crate::shard_directory::RetirementReason::SweepEviction,
        |engine, _incarnation| {
            engine
                .sweep_custody
                .compare_exchange(seq, 0, Ordering::Relaxed, Ordering::Relaxed)
                .is_ok()
        },
    ) {
        crate::shard_directory::RetireOutcome::Retired(_) => {
            unmark(state, prefix);
            tracing::info!("sweep closed shard {prefix}");
        }
        // Adopted (reinstated under the same guard), or already gone.
        crate::shard_directory::RetireOutcome::Kept
        | crate::shard_directory::RetireOutcome::Absent => unmark(state, prefix),
    }
}

/// R29: the walk shares the scheduler budget BEFORE opening. An
/// engine already resident is used quietly (no adoption stamp — an
/// internal touch must not leak it out of the rotation). A cold route
/// opens only while scheduler-held engines are under budget, takes
/// custody like any discovery open, and is closed (or retained as an
/// indebted resident) right after its closures are submitted. Over
/// budget -> the descriptor is DEFERRED: the walk re-pages every
/// sweep, which is the continuation.
async fn walk_engine_budgeted(
    state: &std::sync::Arc<crate::http::AppState>,
    route: &[u8; 16],
    budget: usize,
) -> Option<(std::sync::Arc<crate::shard::ShardEngine>, bool)> {
    let prefix = state.shards.prefix_for(route);
    if let Some(e) = state.shards.open(&prefix) {
        return Some((e, false)); // resident: quiet use, not ours to close
    }
    if scheduler_held(state) >= budget {
        WALK_DEFERRED.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        return None;
    }
    let engine = state.engine_for_quiet(route).await.ok()?;
    if mark(state, &prefix, &engine) {
        Some((engine, true)) // scheduler-held: caller settles custody
    } else {
        // Custody declined (customer raced in): quiet use.
        Some((engine, false))
    }
}

/// After the walk finishes with a scheduler-opened engine: keep it as
/// a budgeted resident if it carries debt, close it otherwise.
async fn walk_settle(state: &std::sync::Arc<crate::http::AppState>, prefix: &str) {
    let Some(engine) = state.shards.open(prefix) else {
        unmark(state, prefix);
        return;
    };
    if !custody_intact(state, prefix, &engine) {
        unmark(state, prefix);
        return;
    }
    if !probe_debt(&engine).await.any() {
        close_scheduler_engine(state, prefix);
    }
    // Indebted: stays marked; phase 1 rotates it like any resident.
}

/// Descriptors skipped this sweep because the budget was full; the
/// next sweep's walk retries them (ops gauge).
pub(crate) static WALK_DEFERRED: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

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
pub(crate) async fn tombstone_walk(state: &std::sync::Arc<crate::http::AppState>) {
    if state.billing.usage_key().is_none() {
        return;
    }
    let after: Option<String> = state.billing.sweep_walk_cursor();
    let page = match state
        .registry
        // mt-lint: allow(state-tenant-read): deployment-tenant catalog sweep — terminal-closure reconciliation walks the raw surface's own rows
        .list_page_raw(state.deployment.deployment_tenant(), after.as_deref(), 256)
        .await
    {
        Ok(p) => p,
        Err(e) => {
            tracing::warn!("tombstone walk paused (registry list): {e}");
            return;
        }
    };
    for d in &page.streams {
        let expired = d.expires_at_ms.is_some_and(|e| billing_now_ms() >= e);
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
            let Some(route) = d.segment_route_by_id(sid) else {
                tracing::error!(
                    segment = sid,
                    "billing sweep encountered missing validated segment"
                );
                state.billing.set_sweep_walk_cursor(after.clone());
                return;
            };
            // Foreign routes are skipped — every instance walks the
            // same registry and closes what IT owns.
            let budget = sweep_resident_budget(&state.config.billing);
            let Some((engine, ours)) = walk_engine_budgeted(state, &route, budget).await else {
                // Deferred (budget full) or open-contended: STOP and
                // resume AT THIS PAGE next sweep — the continuation
                // is what makes deferral fair instead of starving
                // later terminal descriptors (R30).
                state.billing.set_sweep_walk_cursor(after.clone());
                return;
            };
            let hash = d.dynamic_segment_identity(sid);
            let meta = match engine.load_billing_meta(hash).await {
                Ok(Some(meta)) => meta,
                Ok(None) => continue,
                Err(error) => {
                    tracing::error!("billing sweep metadata read failed: {error}");
                    state.billing.set_sweep_walk_cursor(after.clone());
                    return;
                }
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
                } else {
                    WALK_CLOSE_SUBMITS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                }
            } else if retained
                && !meta.retained_by_forks
                && let Err(e) = engine.submit_billing_retained(hash, true).await
            {
                tracing::warn!("tombstone-walk retain failed for {}: {e}", d.name);
            }
            if ours {
                // Scheduler-opened for this descriptor: close it or
                // keep it as an indebted budgeted resident NOW —
                // never accumulate walk opens across the page.
                let prefix = state.shards.prefix_for(&route);
                walk_settle(state, &prefix).await;
            }
        }
    }
    if page.exhausted || page.next_after.is_none() {
        // Full circle from wherever we started: wrap.
        state.billing.set_sweep_walk_cursor(None);
        return;
    }
    // One provider page is the pass budget. Only completed pages advance;
    // cancellation or a deferred segment replays the current page.
    state.billing.set_sweep_walk_cursor(page.next_after);
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
// mt-lint: allow(name-param-shared-core): system ledger under the system project; names are crate constants (_usage, _ops_*), never customer input
pub(crate) async fn system_append(
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
        // mt-lint: allow(stream-ref-construction): system ledger under the system project; names are crate constants
        crate::tenant::system_project().stream_ref(stream),
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
            crate::tenant::system_project(),
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
                // mt-lint: allow(stream-ref-construction): system ledger under the system project; names are crate constants
                crate::tenant::system_project().stream_ref(stream),
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
        let mk = |bearer: Option<&str>| {
            let mut req = crate::http::peer_client()
                .post(format!(
                    "{base}/v1/internal/telemetry-append/{}",
                    crate::http::encode_stream_name_path(stream)
                ))
                .timeout(std::time::Duration::from_secs(20))
                .header("stream-encryption-key", key)
                .header("content-type", "application/json")
                .body(body.clone());
            if let Some(t) = bearer {
                req = req.header("authorization", format!("Bearer {t}"));
            }
            req
        };
        match state.peer.send(mk).await {
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
// mt-lint: allow(name-param-shared-core): system ledger under the system project; names are crate constants, never customer input
pub(crate) async fn system_read(
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
        // mt-lint: allow(stream-ref-construction): system ledger under the system project; names are crate constants
        crate::tenant::system_project().stream_ref(stream),
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
        // mt-lint: allow(stream-ref-construction): system ledger under the system project; names are crate constants
        .get(&crate::tenant::system_project().stream_ref(stream))
        .await
        .map_err(|e| e.to_string())?
        .ok_or_else(|| "system stream descriptor missing".to_string())?;
    let Some(target) = crate::product::InternalTarget::of(&desc, 0) else {
        return Err("system stream has no epoch".into());
    };
    let q = offset
        .map(|o| format!("?offset={}", urlencode(&o)))
        .unwrap_or_default();
    let mk = |bearer: Option<&str>| {
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
        if let Some(t) = bearer {
            req = req.header("authorization", format!("Bearer {t}"));
        }
        req
    };
    match state.peer.send(mk).await {
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
