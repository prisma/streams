//! Usage rollup (docs/OBSERVABILITY-BILLING.md §9): ONE partitioned
//! SlateDB materialization of the `_usage` ledger. Dashboard lookups
//! are point reads; the ledger is never scanned to answer a customer.
//!
//! Processing contract (§9.3): each ledger page becomes ONE WriteBatch
//! covering source-dedupe state, segment state, month/name/project
//! rows, and the consumed cursor — durable before the cursor advances,
//! so a crash replays the page and every apply is idempotent:
//!   - read batches dedupe by (source boot, seq);
//!   - segment snapshots dedupe by usage_version, and month/aggregate
//!     rows absorb them as DELTAS against the previously applied
//!     absolute values (never added twice);
//!   - corrections append to an explicit list; finalized months are
//!     never silently rewritten (§9.5).

use crate::billing::{UsageCorrection, UsageEnvelope, month_start_ms, next_month, parse_month};
mod allocation;
mod close;
mod page;
mod storage;

use serde::{Deserialize, Serialize};
use slatedb::{Db, WriteBatch};
use std::sync::Arc;

// v2: every key carries the ACCOUNT (round-21 multi-tenant identity —
// project ids are not assumed globally unique). Fresh namespace; the
// preview's v1 rollup data is disposable pre-launch.
pub(crate) const ROLLUP_PATH: &str = "telemetry/usage-rollup/v2/p0";

// ---------------------------------------------------------------------
// Keyspace (§9.2)
// ---------------------------------------------------------------------

fn k_source(boot: &str) -> Vec<u8> {
    format!("source/{boot}").into_bytes()
}
fn k_segment(account: &str, project: &str, stream_id: &str, seg: u32) -> Vec<u8> {
    format!("segment/{account}/{project}/{stream_id}/{seg}").into_bytes()
}
fn k_month(month: &str, account: &str, project: &str, stream_id: &str) -> Vec<u8> {
    format!("month/{month}/{account}/{project}/{stream_id}").into_bytes()
}
fn k_month_prefix(month: &str) -> Vec<u8> {
    format!("month/{month}/").into_bytes()
}
// mt-lint: allow(name-param-shared-core): rollup row-key builder; the project is an explicit sibling parameter
fn k_name(month: &str, account: &str, project: &str, name: &str) -> Vec<u8> {
    // Bare (non-tenant-qualified) hash is safe here: the key path already
    // embeds account+project, so equal names in different projects land in
    // disjoint rows. The hash only canonicalizes the name segment.
    format!(
        "name/{month}/{account}/{project}/{}",
        crate::crypto::hex(&crate::crypto::stream_hash(name))
    )
    .into_bytes()
}
fn k_project(month: &str, account: &str, project: &str) -> Vec<u8> {
    format!("project/{month}/{account}/{project}").into_bytes()
}
const K_CURSOR: &[u8] = b"meta/usage-cursor";

// ---------------------------------------------------------------------
// Row types
// ---------------------------------------------------------------------

/// Latest absolute state per segment: the dedupe floor for snapshots
/// and the source of storage extrapolation for provisional months.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub(crate) struct SegmentState {
    #[serde(default)]
    pub usage_version: u64,
    #[serde(default)]
    pub owned_frame_bytes_current: u64,
    #[serde(default)]
    pub storage_accounted_through_ms: i64,
    #[serde(default)]
    pub stream_name: String,
    #[serde(default)]
    pub account_id: String,
}

/// Per-segment contribution recorded inside a month row: the ABSOLUTE
/// per-(segment, month) values last applied, so an updated snapshot
/// applies as a delta and a replayed one applies as zero.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub(crate) struct SegMonth {
    #[serde(default)]
    pub usage_version: u64,
    #[serde(default)]
    pub ingest_bytes: u64,
    #[serde(default)]
    pub ingest_records: u64,
    /// u128 as string.
    #[serde(default)]
    pub storage_byte_ms: String,
    #[serde(default)]
    pub gauge_bytes: u64,
    #[serde(default)]
    pub accounted_through_ms: i64,
    /// True once a month-final snapshot landed — extrapolation stops.
    #[serde(default)]
    pub final_seen: bool,
}

/// One (month, project, stream incarnation): the invoice row.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub(crate) struct MonthRow {
    #[serde(default)]
    pub account_id: String,
    #[serde(default)]
    pub stream_name: String,
    /// Absolute per-segment ingest/storage state for delta application.
    #[serde(default)]
    pub segments: std::collections::HashMap<u32, SegMonth>,
    // Read-side accumulators (deltas, added once per deduped batch).
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
    #[serde(default)]
    pub finalized_at_ms: Option<i64>,
    /// Totals FROZEN at finalization (§9.5 / round-21 blocker 8): the
    /// invoice base. Late data appends to `corrections`; it never
    /// mutates these.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub frozen: Option<FrozenTotals>,
    #[serde(default)]
    pub corrections: Vec<UsageCorrection>,
    /// Materialized sums over `corrections` (round-22 item 8).
    #[serde(default)]
    pub corr: CorrTotals,
    #[serde(default)]
    pub updated_ms: i64,
}

/// Materialized correction sums (round-22 item 8): effective totals
/// = base + these, served by the usage API without walking the
/// corrections list. Kept on month rows AND on name/project
/// aggregates so aggregate answers stay invoice-consistent.
#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
pub(crate) struct CorrTotals {
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
    #[serde(default)]
    pub count: u32,
}

impl CorrTotals {
    pub(crate) fn absorb(&mut self, c: &UsageCorrection) {
        self.ingest_payload_bytes_delta += c.ingest_payload_bytes_delta;
        self.ingest_records_delta += c.ingest_records_delta;
        self.read_payload_bytes_delta += c.read_payload_bytes_delta;
        self.read_records_delta += c.read_records_delta;
        self.read_operations_delta += c.read_operations_delta;
        self.queue_operations_delta += c.queue_operations_delta;
        self.append_requests_delta += c.append_requests_delta;
        let cur: i128 = self.storage_byte_ms_delta.parse().unwrap_or(0);
        let d: i128 = c.storage_byte_ms_delta.parse().unwrap_or(0);
        self.storage_byte_ms_delta = (cur + d).to_string();
        self.count += 1;
    }
}

/// base + signed delta, floored at zero (a correction can subtract).
pub(crate) fn eff_u64(base: u64, delta: i64) -> u64 {
    if delta >= 0 {
        base.saturating_add(delta as u64)
    } else {
        base.saturating_sub(delta.unsigned_abs())
    }
}

pub(crate) fn eff_u128(base: u128, delta_str: &str) -> u128 {
    let d: i128 = delta_str.parse().unwrap_or(0);
    if d >= 0 {
        base.saturating_add(d as u128)
    } else {
        base.saturating_sub(d.unsigned_abs())
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub(crate) struct FrozenTotals {
    #[serde(default)]
    pub ingest_bytes: u64,
    #[serde(default)]
    pub ingest_records: u64,
    #[serde(default)]
    pub storage_byte_ms: String,
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

impl MonthRow {
    pub(crate) fn ingest_bytes(&self) -> u64 {
        self.segments.values().map(|s| s.ingest_bytes).sum()
    }
    pub(crate) fn ingest_records(&self) -> u64 {
        self.segments.values().map(|s| s.ingest_records).sum()
    }
    pub(crate) fn storage_byte_ms(&self) -> u128 {
        self.segments
            .values()
            .map(|s| s.storage_byte_ms.parse::<u128>().unwrap_or(0))
            .sum()
    }
    /// Provisional storage byte-time: the recorded integral plus each
    /// non-final segment gauge extrapolated to `now`, clamped to the
    /// month's end (§9.4).
    pub(crate) fn storage_byte_ms_provisional(&self, month: &str, now_ms: i64) -> u128 {
        let end = parse_month(month)
            .map(|(y, m)| {
                let (ny, nm) = next_month(y, m);
                month_start_ms(ny, nm)
            })
            .unwrap_or(i64::MAX);
        let upto = now_ms.min(end);
        self.segments
            .values()
            .map(|s| {
                let base = s.storage_byte_ms.parse::<u128>().unwrap_or(0);
                if s.final_seen || s.accounted_through_ms >= upto {
                    base
                } else {
                    base + storage::byte_ms(s.gauge_bytes, s.accounted_through_ms, upto)
                }
            })
            .sum()
    }
    pub(crate) fn owned_bytes_now(&self) -> u64 {
        self.segments.values().map(|s| s.gauge_bytes).sum()
    }
    /// Effective (invoice) totals: the frozen base when finalized,
    /// live accumulators otherwise, plus materialized corrections.
    pub(crate) fn effective(&self) -> serde_json::Value {
        let (ib, irec, sbm, rpb, rrec, rop, qop, areq) = match &self.frozen {
            Some(f) => (
                f.ingest_bytes,
                f.ingest_records,
                f.storage_byte_ms.parse::<u128>().unwrap_or(0),
                f.read_payload_bytes,
                f.read_records,
                f.read_operations,
                f.queue_operations,
                f.append_requests,
            ),
            None => (
                self.ingest_bytes(),
                self.ingest_records(),
                self.storage_byte_ms(),
                self.read_payload_bytes,
                self.read_records,
                self.read_operations,
                self.queue_operations,
                self.append_requests,
            ),
        };
        serde_json::json!({
            "ingestPayloadBytes": eff_u64(ib, self.corr.ingest_payload_bytes_delta),
            "ingestRecords": eff_u64(irec, self.corr.ingest_records_delta),
            "readPayloadBytes": eff_u64(rpb, self.corr.read_payload_bytes_delta),
            "readRecords": eff_u64(rrec, self.corr.read_records_delta),
            "readOperations": eff_u64(rop, self.corr.read_operations_delta),
            "queueOperations": eff_u64(qop, self.corr.queue_operations_delta),
            "appendRequests": eff_u64(areq, self.corr.append_requests_delta),
            "storageByteSeconds": (eff_u128(sbm, &self.corr.storage_byte_ms_delta) / 1000).to_string(),
            "correctionCount": self.corr.count,
        })
    }
}

/// Aggregate across a project's streams for one month (also the shape
/// of the per-name aggregate, which additionally lists incarnations).
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub(crate) struct AggRow {
    #[serde(default)]
    pub ingest_bytes: u64,
    #[serde(default)]
    pub ingest_records: u64,
    /// i128-safe accumulator as string (deltas are signed only via
    /// corrections; stored non-negative).
    #[serde(default)]
    pub storage_byte_ms: String,
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
    /// Name rows: the incarnations that contributed this month.
    #[serde(default)]
    pub incarnations: Vec<String>,
    /// Correction sums carried at the aggregate level too (round-22
    /// item 8): project/name answers include late-data effects.
    #[serde(default)]
    pub corr: CorrTotals,
}

impl AggRow {
    fn add_storage(&mut self, delta: u128) {
        let cur: u128 = self.storage_byte_ms.parse().unwrap_or(0);
        self.storage_byte_ms = (cur + delta).to_string();
    }
}

/// Every (month, duration-ms) span of [from, to): walks each UTC month
/// boundary the interval crosses.
fn month_spans(from_ms: i64, to_ms: i64) -> Vec<(String, i64)> {
    let to = to_ms.max(from_ms + 1);
    let mut out = Vec::new();
    let mut cur = from_ms;
    while cur < to {
        let (y, m) = crate::billing::utc_year_month(cur);
        let (ny, nm) = next_month(y, m);
        let boundary = month_start_ms(ny, nm).min(to);
        out.push((crate::billing::month_str(y, m), boundary - cur));
        cur = boundary;
    }
    out
}

/// The invoice-reconciliation verdict for one month (Stage 7): counts
/// walked and every disagreement, empty = the books balance.
#[derive(Clone, Debug, Default, Serialize)]
pub(crate) struct ReconcileReport {
    pub month: String,
    pub stream_rows: usize,
    pub projects: usize,
    pub mismatches: Vec<String>,
    pub ok: bool,
}

// ---------------------------------------------------------------------
// The rollup database
// ---------------------------------------------------------------------

/// PR 6.1-C: the rollup instance's DATABASE, installed once at startup
/// on the instance that runs the consumer (`ROLLUP=1`) and read by the
/// usage-lookup and operator surfaces. It is a store, not a decision:
/// the honest owner is a named install-once slot, not a getter on a
/// "billing service" that would then own nothing about it.
#[derive(Clone, Default)]
pub(crate) struct RollupSlot {
    inner: std::sync::Arc<std::sync::OnceLock<std::sync::Arc<UsageRollup>>>,
}

impl RollupSlot {
    /// Install the database. `Err` means another install won: this
    /// instance already has one.
    pub(crate) fn install(
        &self,
        rollup: std::sync::Arc<UsageRollup>,
    ) -> Result<(), std::sync::Arc<UsageRollup>> {
        self.inner.set(rollup)
    }

    /// The database, if this instance is a rollup consumer.
    pub(crate) fn get(&self) -> Option<&std::sync::Arc<UsageRollup>> {
        self.inner.get()
    }

    /// Whether this instance runs the rollup consumer.
    pub(crate) fn installed(&self) -> bool {
        self.inner.get().is_some()
    }
}

pub(crate) struct UsageRollup {
    pub db: Arc<Db>,
    close_rows_visited: std::sync::atomic::AtomicU64,
}

/// SlateDB scan_prefix subranges are SUFFIX-relative. Exclude the exact
/// persisted cursor and let SlateDB enforce the prefix's upper bound.
type CloseScanRange = (std::ops::Bound<Vec<u8>>, std::ops::Bound<Vec<u8>>);

fn close_scan_range(prefix: &[u8], after: Option<&[u8]>) -> anyhow::Result<CloseScanRange> {
    use std::ops::Bound;
    let lower = match after {
        None => Bound::Unbounded,
        Some(cursor) => Bound::Excluded(
            cursor
                .strip_prefix(prefix)
                .ok_or_else(|| anyhow::anyhow!("month-close cursor outside expected prefix"))?
                .to_vec(),
        ),
    };
    Ok((lower, Bound::Unbounded))
}

/// Missing rows alone initialize state; corrupt/unavailable rows block progress.
async fn get_json<T: for<'a> Deserialize<'a> + Default>(db: &Db, key: &[u8]) -> anyhow::Result<T> {
    Ok(read_json(db, key).await?.unwrap_or_default())
}

async fn read_bytes(db: &Db, key: &[u8]) -> anyhow::Result<Option<bytes::Bytes>> {
    #[cfg(test)]
    if read_faults()
        .lock()
        .unwrap()
        .remove(&(db as *const Db as usize, key.to_vec()))
    {
        anyhow::bail!("injected rollup repository read failure");
    }
    Ok(db.get(key).await?)
}

async fn read_json<T: for<'a> Deserialize<'a>>(db: &Db, key: &[u8]) -> anyhow::Result<Option<T>> {
    read_bytes(db, key)
        .await?
        .map(|raw| decode_json(&raw))
        .transpose()
}

fn decode_json<T: for<'a> Deserialize<'a>>(raw: &[u8]) -> anyhow::Result<T> {
    // Persisted decimal fields intentionally accept the historical empty
    // representation of zero. Nonempty malformed values never become zero.
    fn validate_decimal(key: &str, value: &serde_json::Value) -> anyhow::Result<()> {
        if !key.contains("storage_byte_ms") {
            return Ok(());
        }
        let number = value
            .as_str()
            .ok_or_else(|| anyhow::anyhow!("invalid decimal accounting field"))?;
        if number.is_empty() {
            return Ok(());
        }
        if key.ends_with("delta") {
            number.parse::<i128>()?;
        } else {
            number.parse::<u128>()?;
        }
        Ok(())
    }

    fn validate(v: &serde_json::Value) -> anyhow::Result<()> {
        match v {
            serde_json::Value::Object(fields) => {
                for (key, value) in fields {
                    validate_decimal(key, value)?;
                    validate(value)?;
                }
            }
            serde_json::Value::Array(values) => {
                for value in values {
                    validate(value)?;
                }
            }
            _ => {}
        }
        Ok(())
    }
    validate(&serde_json::from_slice::<serde_json::Value>(raw)?)?;
    Ok(serde_json::from_slice(raw)?)
}

#[cfg(test)]
type ReadFaults = std::sync::Mutex<std::collections::HashSet<(usize, Vec<u8>)>>;

#[cfg(test)]
fn read_faults() -> &'static ReadFaults {
    static FAULTS: std::sync::OnceLock<ReadFaults> = std::sync::OnceLock::new();
    FAULTS.get_or_init(Default::default)
}

impl UsageRollup {
    /// Test-only convenience: independent tests construct independent caches.
    #[cfg(test)]
    pub(crate) async fn open(
        store: Arc<dyn object_store::ObjectStore>,
        prefix: &str,
        cfg: &crate::config::ServerConfig,
    ) -> anyhow::Result<Self> {
        Self::open_with_cache(
            store,
            prefix,
            cfg,
            crate::runtime::TelemetryResources::new(cfg.billing.telemetry_cache_bytes).cache,
        )
        .await
    }

    /// The runtime shares one telemetry cache across spool and rollup DBs.
    pub(crate) async fn open_with_cache(
        store: Arc<dyn object_store::ObjectStore>,
        prefix: &str,
        cfg: &crate::config::ServerConfig,
        cache: Arc<slatedb::db_cache::foyer::FoyerCache>,
    ) -> anyhow::Result<Self> {
        let path = if prefix.is_empty() {
            ROLLUP_PATH.to_string()
        } else {
            format!("{prefix}/{ROLLUP_PATH}")
        };
        let settings =
            crate::billing::telemetry_settings(&cfg.billing, &cfg.engine.compactor_options());
        let db = crate::bootstrap::on_slatedb_rt(async move {
            Db::builder(path.as_str(), store)
                .with_settings(settings)
                .with_db_cache(cache)
                .build()
                .await
        })
        .await?;
        Ok(UsageRollup {
            db: Arc::new(db),
            close_rows_visited: Default::default(),
        })
    }

    pub(crate) async fn cursor(&self) -> anyhow::Result<Option<String>> {
        read_bytes(&self.db, K_CURSOR)
            .await?
            .map(|v| String::from_utf8(v.to_vec()).map_err(Into::into))
            .transpose()
    }

    /// Apply one ledger page and its cursor in one durable transaction.
    pub(crate) async fn apply_page(
        &self,
        envelopes: &[UsageEnvelope],
        next_cursor: &str,
    ) -> anyhow::Result<()> {
        page::apply(&self.db, envelopes, next_cursor).await
    }

    /// L0 posture of the rollup DB (same in-memory manifest probe as
    /// the history partitions and the read spool).
    pub(crate) fn l0_stats(&self) -> (u64, u64, u64, u64) {
        crate::history::history_l0_stats(&self.db)
    }

    // ---- point reads (the customer API) ------------------------------

    pub(crate) async fn month_row(
        &self,
        month: &str,
        account: &str,
        project: &str,
        stream_id: &str,
    ) -> anyhow::Result<Option<MonthRow>> {
        read_json(&self.db, &k_month(month, account, project, stream_id)).await
    }

    // mt-lint: allow(name-param-shared-core): rollup lookup; project is an explicit sibling parameter
    pub(crate) async fn name_row(
        &self,
        month: &str,
        account: &str,
        project: &str,
        name: &str,
    ) -> anyhow::Result<Option<AggRow>> {
        read_json(&self.db, &k_name(month, account, project, name)).await
    }

    pub(crate) async fn project_row(
        &self,
        month: &str,
        account: &str,
        project: &str,
    ) -> anyhow::Result<Option<AggRow>> {
        read_json(&self.db, &k_project(month, account, project)).await
    }

    /// Invoice reconciliation (MULTITENANCY Stage 7): recompute every
    /// (account, project) total for one month from the STREAM month
    /// rows and compare against the served project aggregates. A
    /// mid-month ownership transfer legitimately splits a project
    /// across two accounts (workspace-at-event), so the invariant is
    /// per-(account, project) agreement — never one-account-per-
    /// project. Storage byte-time is excluded: month rows extrapolate
    /// provisionally while aggregates accumulate applied integrals, so
    /// they only agree at month close.
    pub async fn reconcile_month(&self, month: &str) -> anyhow::Result<ReconcileReport> {
        #[derive(Default, Clone, PartialEq, Debug)]
        struct Tot {
            ingest_bytes: u64,
            ingest_records: u64,
            read_payload_bytes: u64,
            read_records: u64,
            read_operations: u64,
            queue_operations: u64,
            append_requests: u64,
        }
        let mut report = ReconcileReport {
            month: month.to_string(),
            ..Default::default()
        };
        // 1. Recompute per-(account, project) effective totals from the
        //    stream month rows (base + materialized corrections).
        let mut computed: std::collections::BTreeMap<(String, String), Tot> =
            std::collections::BTreeMap::new();
        let pfx = k_month_prefix(month);
        let mut iter = self.db.scan_prefix(&pfx[..], ..).await?;
        while let Some(kv) = iter.next().await? {
            let k = std::str::from_utf8(&kv.key).unwrap_or("");
            // month/{month}/{account}/{project}/{stream_id}
            let parts: Vec<&str> = k.splitn(5, '/').collect();
            if parts.len() != 5 {
                report
                    .mismatches
                    .push(format!("unparseable month key: {k}"));
                continue;
            }
            let (account, project) = (parts[2], parts[3]);
            let Ok(row) = serde_json::from_slice::<MonthRow>(&kv.value) else {
                report
                    .mismatches
                    .push(format!("undecodable month row: {k}"));
                continue;
            };
            report.stream_rows += 1;
            if !row.account_id.is_empty() && row.account_id != account {
                report.mismatches.push(format!(
                    "row account drift at {k}: key={account} row={}",
                    row.account_id
                ));
            }
            let t = computed
                .entry((account.to_string(), project.to_string()))
                .or_default();
            t.ingest_bytes += eff_u64(row.ingest_bytes(), row.corr.ingest_payload_bytes_delta);
            t.ingest_records += eff_u64(row.ingest_records(), row.corr.ingest_records_delta);
            t.read_payload_bytes +=
                eff_u64(row.read_payload_bytes, row.corr.read_payload_bytes_delta);
            t.read_records += eff_u64(row.read_records, row.corr.read_records_delta);
            t.read_operations += eff_u64(row.read_operations, row.corr.read_operations_delta);
            t.queue_operations += eff_u64(row.queue_operations, row.corr.queue_operations_delta);
            t.append_requests += eff_u64(row.append_requests, row.corr.append_requests_delta);
        }
        // 2. Walk the served project aggregates and compare.
        let apfx = format!("project/{month}/").into_bytes();
        let mut seen: std::collections::BTreeSet<(String, String)> = Default::default();
        let mut iter = self.db.scan_prefix(&apfx[..], ..).await?;
        while let Some(kv) = iter.next().await? {
            let k = std::str::from_utf8(&kv.key).unwrap_or("");
            // project/{month}/{account}/{project}
            let parts: Vec<&str> = k.splitn(4, '/').collect();
            if parts.len() != 4 {
                report
                    .mismatches
                    .push(format!("unparseable project key: {k}"));
                continue;
            }
            let (account, project) = (parts[2].to_string(), parts[3].to_string());
            let Ok(agg) = serde_json::from_slice::<AggRow>(&kv.value) else {
                report
                    .mismatches
                    .push(format!("undecodable project aggregate: {k}"));
                continue;
            };
            report.projects += 1;
            let served = Tot {
                ingest_bytes: eff_u64(agg.ingest_bytes, agg.corr.ingest_payload_bytes_delta),
                ingest_records: eff_u64(agg.ingest_records, agg.corr.ingest_records_delta),
                read_payload_bytes: eff_u64(
                    agg.read_payload_bytes,
                    agg.corr.read_payload_bytes_delta,
                ),
                read_records: eff_u64(agg.read_records, agg.corr.read_records_delta),
                read_operations: eff_u64(agg.read_operations, agg.corr.read_operations_delta),
                queue_operations: eff_u64(agg.queue_operations, agg.corr.queue_operations_delta),
                append_requests: eff_u64(agg.append_requests, agg.corr.append_requests_delta),
            };
            match computed.get(&(account.clone(), project.clone())) {
                None => report.mismatches.push(format!(
                    "aggregate without stream rows: {account}/{project}"
                )),
                Some(c) if *c != served => report.mismatches.push(format!(
                    "totals disagree for {account}/{project}: streams={c:?} aggregate={served:?}"
                )),
                Some(_) => {}
            }
            seen.insert((account, project));
        }
        for (account, project) in computed.keys() {
            if !seen.contains(&(account.clone(), project.clone())) {
                report.mismatches.push(format!(
                    "stream rows without an aggregate: {account}/{project}"
                ));
            }
        }
        report.ok = report.mismatches.is_empty();
        Ok(report)
    }

    /// All persistent segment states for one stream (bounded by its
    /// segment count) — the current-month fallback when no month row
    /// exists yet (round-21 blocker 2).
    pub(crate) async fn stream_segment_states(
        &self,
        account: &str,
        project: &str,
        stream_id: &str,
    ) -> anyhow::Result<Vec<SegmentState>> {
        let pfx = format!("segment/{account}/{project}/{stream_id}/").into_bytes();
        let mut out = Vec::new();
        let mut iter = self.db.scan_prefix(&pfx[..], ..).await?;
        while let Some(kv) = iter.next().await? {
            out.push(decode_json(&kv.value)?);
        }
        Ok(out)
    }

    /// Pending monthly artifacts (blocker 7): (pending key, month,
    /// project, stream-id, row).
    pub(crate) async fn pending_artifacts(
        &self,
        max: usize,
    ) -> anyhow::Result<Vec<(Vec<u8>, String, String, String, MonthRow)>> {
        let mut out = Vec::new();
        let mut iter = self.db.scan_prefix(&b"artifact-pending/"[..], ..).await?;
        while let Some(kv) = iter.next().await? {
            let k = std::str::from_utf8(&kv.key).unwrap_or("").to_string();
            let parts: Vec<&str> = k.splitn(5, '/').collect();
            if parts.len() == 5
                && let Ok(row) = serde_json::from_slice::<MonthRow>(&kv.value)
            {
                out.push((
                    kv.key.to_vec(),
                    parts[1].to_string(),
                    format!("{}/{}", parts[2], parts[3]),
                    parts[4].to_string(),
                    row,
                ));
            }
            if out.len() >= max {
                break;
            }
        }
        Ok(out)
    }

    /// Phase 2 of publication: the object is verifiably in the store —
    /// retire the pending row and record the done marker.
    pub(crate) async fn mark_artifact_published(
        &self,
        pending_key: &[u8],
        object_path: &str,
    ) -> anyhow::Result<()> {
        let mut wb = WriteBatch::new();
        wb.delete(pending_key);
        let done_key = {
            let mut k = b"artifact-done/".to_vec();
            k.extend_from_slice(&pending_key[b"artifact-pending/".len()..]);
            k
        };
        wb.put(
            done_key,
            serde_json::to_vec(&serde_json::json!({
                "path": object_path,
                "published_ms": crate::billing::billing_now_ms(),
            }))?,
        );
        self.db.write(wb).await?;
        Ok(())
    }

    /// Pending correction artifacts (round-22 item 8): (pending key,
    /// month, "account/project", stream-id, correction id, body).
    pub(crate) async fn pending_correction_artifacts(
        &self,
        max: usize,
    ) -> anyhow::Result<Vec<(Vec<u8>, String, String, String, String, Vec<u8>)>> {
        let mut out = Vec::new();
        let mut iter = self.db.scan_prefix(&b"corr-pending/"[..], ..).await?;
        while let Some(kv) = iter.next().await? {
            let k = std::str::from_utf8(&kv.key).unwrap_or("").to_string();
            let parts: Vec<&str> = k.splitn(6, '/').collect();
            if parts.len() == 6 {
                let cid = serde_json::from_slice::<UsageCorrection>(&kv.value)
                    .map(|c| c.correction_id)
                    .unwrap_or_else(|_| parts[5].to_string());
                out.push((
                    kv.key.to_vec(),
                    parts[1].to_string(),
                    format!("{}/{}", parts[2], parts[3]),
                    parts[4].to_string(),
                    cid,
                    kv.value.to_vec(),
                ));
            }
            if out.len() >= max {
                break;
            }
        }
        Ok(out)
    }

    pub(crate) async fn mark_correction_published(
        &self,
        pending_key: &[u8],
        object_path: &str,
    ) -> anyhow::Result<()> {
        let mut wb = WriteBatch::new();
        wb.delete(pending_key);
        let done_key = {
            let mut k = b"corr-done/".to_vec();
            k.extend_from_slice(&pending_key[b"corr-pending/".len()..]);
            k
        };
        wb.put(
            done_key,
            serde_json::to_vec(&serde_json::json!({
                "path": object_path,
                "published_ms": crate::billing::billing_now_ms(),
            }))?,
        );
        self.db.write(wb).await?;
        Ok(())
    }

    // ---- month close (§9.4/§9.5/§9.6) --------------------------------

    /// Physical rows visited by both close phases, including resumed runs.
    #[cfg(test)]
    pub(crate) fn close_rows_visited(&self) -> u64 {
        self.close_rows_visited
            .load(std::sync::atomic::Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests;

// ---------------------------------------------------------------------
// Operational metrics rollup (§13.1): raw 15 s points + 1-minute
// aggregates in the SAME materialization, with a raw-tier retention
// sweep. Longer tiers (5 m/1 h) are the same mechanism applied again.
// ---------------------------------------------------------------------

const K_OPS_CURSOR: &[u8] = b"meta/ops-cursor";
pub(crate) const OPS_RAW_RETENTION_MS: i64 = 7 * 86_400_000;

fn k_ops_raw(instance: &str, ts_ms: i64) -> Vec<u8> {
    format!("ops/raw/{instance}/{ts_ms:020}").into_bytes()
}
fn k_ops_m1(instance: &str, minute_ms: i64) -> Vec<u8> {
    format!("ops/m1/{instance}/{minute_ms:020}").into_bytes()
}

/// One-minute aggregate: last cumulative counters + max gauges seen.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub(crate) struct OpsM1 {
    #[serde(default)]
    // mt-lint: allow(name-keyed-map): metric name, not stream identity
    pub counters: std::collections::BTreeMap<String, u64>,
    #[serde(default)]
    // mt-lint: allow(name-keyed-map): metric name, not stream identity
    pub gauges_max: std::collections::BTreeMap<String, u64>,
    #[serde(default)]
    pub samples: u32,
}

impl UsageRollup {
    pub(crate) async fn ops_cursor(&self) -> Option<String> {
        self.db
            .get(K_OPS_CURSOR)
            .await
            .ok()
            .flatten()
            .and_then(|v| String::from_utf8(v.to_vec()).ok())
    }

    /// Ingest one `_ops_metrics` page: raw point + m1 merge + cursor in
    /// one WriteBatch (same §9.3 discipline as usage pages).
    pub(crate) async fn apply_ops_page(
        &self,
        snaps: &[crate::ops::OpsSnapshot],
        next_cursor: &str,
    ) -> anyhow::Result<()> {
        let mut wb = WriteBatch::new();
        let mut m1s: std::collections::HashMap<Vec<u8>, OpsM1> = Default::default();
        for s in snaps {
            wb.put(k_ops_raw(&s.instance, s.ts_ms), serde_json::to_vec(s)?);
            let minute = s.ts_ms - s.ts_ms.rem_euclid(60_000);
            let key = k_ops_m1(&s.instance, minute);
            let mut agg: OpsM1 = match m1s.get(&key) {
                Some(a) => a.clone(),
                None => get_json(&self.db, &key).await?,
            };
            for (k, v) in &s.counters {
                agg.counters.insert(k.clone(), *v); // cumulative: last wins
            }
            for (k, v) in &s.gauges {
                let e = agg.gauges_max.entry(k.clone()).or_insert(0);
                *e = (*e).max(*v);
            }
            agg.samples += 1;
            m1s.insert(key, agg);
        }
        for (k, agg) in &m1s {
            wb.put(k.clone(), serde_json::to_vec(agg)?);
        }
        wb.put(K_OPS_CURSOR, next_cursor.as_bytes());
        self.db.write(wb).await?;
        Ok(())
    }

    #[cfg(test)]
    pub(crate) async fn ops_m1(&self, instance: &str, minute_ms: i64) -> Option<OpsM1> {
        self.db
            .get(&k_ops_m1(instance, minute_ms)[..])
            .await
            .ok()
            .flatten()
            .and_then(|v| serde_json::from_slice(&v).ok())
    }

    /// Retention sweep for the raw tier (§13.1): delete points older
    /// than the cutoff, bounded per call.
    pub(crate) async fn sweep_ops_raw(
        &self,
        now_ms: i64,
        max_deletes: usize,
    ) -> anyhow::Result<usize> {
        let cutoff = now_ms - OPS_RAW_RETENTION_MS;
        let mut wb = WriteBatch::new();
        let mut n = 0usize;
        let mut iter = self.db.scan_prefix(&b"ops/raw/"[..], ..).await?;
        while let Some(kv) = iter.next().await? {
            if n >= max_deletes {
                break;
            }
            let key = std::str::from_utf8(&kv.key).unwrap_or("");
            let Some(ts) = key.rsplit('/').next().and_then(|t| t.parse::<i64>().ok()) else {
                continue;
            };
            if ts < cutoff {
                wb.delete(&kv.key);
                n += 1;
            }
        }
        if n > 0 {
            self.db.write(wb).await?;
        }
        Ok(n)
    }
}

#[cfg(test)]
mod accounting_failure_tests;

#[cfg(test)]
mod close_seek_tests;
