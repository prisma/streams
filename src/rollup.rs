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

use crate::billing::{
    ReadBatch, ReadRow, SegmentSnapshot, UsageCorrection, UsageEnvelope, UsagePayload,
    month_start_ms, next_month, parse_month,
};
use serde::{Deserialize, Serialize};
use slatedb::{Db, WriteBatch};
use std::sync::Arc;

// v2: every key carries the ACCOUNT (round-21 multi-tenant identity —
// project ids are not assumed globally unique). Fresh namespace; the
// preview's v1 rollup data is disposable pre-launch.
pub const ROLLUP_PATH: &str = "telemetry/usage-rollup/v2/p0";

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
fn k_name(month: &str, account: &str, project: &str, name: &str) -> Vec<u8> {
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
pub struct SegmentState {
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
pub struct SegMonth {
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
pub struct MonthRow {
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
pub struct CorrTotals {
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
    pub fn absorb(&mut self, c: &UsageCorrection) {
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
pub fn eff_u64(base: u64, delta: i64) -> u64 {
    if delta >= 0 {
        base.saturating_add(delta as u64)
    } else {
        base.saturating_sub(delta.unsigned_abs())
    }
}

pub fn eff_u128(base: u128, delta_str: &str) -> u128 {
    let d: i128 = delta_str.parse().unwrap_or(0);
    if d >= 0 {
        base.saturating_add(d as u128)
    } else {
        base.saturating_sub(d.unsigned_abs())
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct FrozenTotals {
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
    pub fn ingest_bytes(&self) -> u64 {
        self.segments.values().map(|s| s.ingest_bytes).sum()
    }
    pub fn ingest_records(&self) -> u64 {
        self.segments.values().map(|s| s.ingest_records).sum()
    }
    pub fn storage_byte_ms(&self) -> u128 {
        self.segments
            .values()
            .map(|s| s.storage_byte_ms.parse::<u128>().unwrap_or(0))
            .sum()
    }
    /// Provisional storage byte-time: the recorded integral plus each
    /// non-final segment gauge extrapolated to `now`, clamped to the
    /// month's end (§9.4).
    pub fn storage_byte_ms_provisional(&self, month: &str, now_ms: i64) -> u128 {
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
                    base + (upto - s.accounted_through_ms) as u128 * s.gauge_bytes as u128
                }
            })
            .sum()
    }
    pub fn owned_bytes_now(&self) -> u64 {
        self.segments.values().map(|s| s.gauge_bytes).sum()
    }
    /// Effective (invoice) totals: the frozen base when finalized,
    /// live accumulators otherwise, plus materialized corrections.
    pub fn effective(&self) -> serde_json::Value {
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
pub struct AggRow {
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

// ---------------------------------------------------------------------
// The rollup database
// ---------------------------------------------------------------------

pub struct UsageRollup {
    pub db: Arc<Db>,
}

async fn get_json<T: for<'a> Deserialize<'a> + Default>(db: &Db, key: &[u8]) -> T {
    match db.get(key).await {
        Ok(Some(v)) => serde_json::from_slice(&v).unwrap_or_default(),
        _ => T::default(),
    }
}

impl UsageRollup {
    pub async fn open(
        store: Arc<dyn object_store::ObjectStore>,
        prefix: &str,
    ) -> anyhow::Result<Self> {
        let path = if prefix.is_empty() {
            ROLLUP_PATH.to_string()
        } else {
            format!("{prefix}/{ROLLUP_PATH}")
        };
        let db = crate::on_slatedb_rt(async move {
            Db::builder(path.as_str(), store)
                .with_settings(crate::billing::telemetry_settings())
                .with_db_cache(crate::billing::telemetry_cache())
                .build()
                .await
        })
        .await?;
        Ok(UsageRollup { db: Arc::new(db) })
    }

    pub async fn cursor(&self) -> Option<String> {
        self.db
            .get(K_CURSOR)
            .await
            .ok()
            .flatten()
            .and_then(|v| String::from_utf8(v.to_vec()).ok())
    }

    /// Apply one ledger page transactionally (§9.3). `next_cursor` is
    /// stored in the SAME batch; the whole page is durable before the
    /// consumer advances. Idempotent under replay.
    pub async fn apply_page(
        &self,
        envelopes: &[UsageEnvelope],
        next_cursor: &str,
    ) -> anyhow::Result<()> {
        let mut wb = WriteBatch::new();
        // Row caches so several envelopes touching the same row in one
        // page see each other's updates before the batch lands.
        let mut sources: std::collections::HashMap<String, u64> = Default::default();
        let mut months: std::collections::HashMap<Vec<u8>, MonthRow> = Default::default();
        let mut segs: std::collections::HashMap<Vec<u8>, SegmentState> = Default::default();
        let mut names: std::collections::HashMap<Vec<u8>, AggRow> = Default::default();
        let mut projects: std::collections::HashMap<Vec<u8>, AggRow> = Default::default();
        let mut extra: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
        let now = crate::shard::now_ms();

        for env in envelopes {
            match &env.payload {
                UsagePayload::ReadBatch(rb) => {
                    self.apply_read_batch(
                        rb,
                        &mut sources,
                        &mut months,
                        &mut names,
                        &mut projects,
                        &mut extra,
                    )
                    .await;
                }
                UsagePayload::SegmentSnapshot(snap) => {
                    self.apply_snapshot(
                        snap,
                        &mut months,
                        &mut segs,
                        &mut names,
                        &mut projects,
                        &mut extra,
                    )
                    .await;
                }
                UsagePayload::StreamLifecycle(_) => {
                    // Informational in v1: recreation isolation is
                    // already structural (stream_id keying).
                }
                UsagePayload::UsageCorrection(c) => {
                    let key = k_month(
                        &c.month,
                        &c.identity.account_id,
                        &c.identity.project_id,
                        &c.identity.stream_id,
                    );
                    let mut row: MonthRow = match months.get(&key) {
                        Some(r) => r.clone(),
                        None => get_json(&self.db, &key).await,
                    };
                    // Round-22 item 8: fill provenance for envelope-
                    // supplied corrections from the envelope itself.
                    let mut c = c.clone();
                    if c.correction_id.is_empty() {
                        c.correction_id = env.event_id.clone();
                    }
                    if c.correction_version == 0 {
                        c.correction_version = 1;
                    }
                    if c.source_event_id.is_empty() {
                        c.source_event_id = env.event_id.clone();
                    }
                    if c.created_at_ms == 0 {
                        c.created_at_ms = env.emitted_ms;
                    }
                    self.push_correction(&mut row, c, &mut names, &mut projects, &mut extra)
                        .await;
                    row.updated_ms = now;
                    months.insert(key, row);
                }
            }
        }
        for (boot, seq) in sources {
            wb.put(k_source(&boot), &seq.to_le_bytes()[..]);
        }
        for (k, row) in &mut months {
            row.updated_ms = now;
            wb.put(k.clone(), serde_json::to_vec(row)?);
        }
        for (k, st) in &segs {
            wb.put(k.clone(), serde_json::to_vec(st)?);
        }
        for (k, agg) in &names {
            wb.put(k.clone(), serde_json::to_vec(agg)?);
        }
        for (k, agg) in &projects {
            wb.put(k.clone(), serde_json::to_vec(agg)?);
        }
        for (k, v) in &extra {
            wb.put(k.clone(), v.clone());
        }
        wb.put(K_CURSOR, next_cursor.as_bytes());
        self.db.write(wb).await?;
        Ok(())
    }

    /// Append ONE correction to a month row: dedupe by correction_id
    /// (a replayed correction applies exactly once), materialize its
    /// sums on the row AND on both aggregates (round-22 item 8f), and
    /// stage its immutable artifact when the month is finalized (8e).
    async fn push_correction(
        &self,
        mr: &mut MonthRow,
        c: UsageCorrection,
        names: &mut std::collections::HashMap<Vec<u8>, AggRow>,
        projects: &mut std::collections::HashMap<Vec<u8>, AggRow>,
        extra: &mut Vec<(Vec<u8>, Vec<u8>)>,
    ) {
        if !c.correction_id.is_empty()
            && mr
                .corrections
                .iter()
                .any(|x| x.correction_id == c.correction_id)
        {
            return;
        }
        mr.corr.absorb(&c);
        for (key, is_name) in [
            (
                k_name(
                    &c.month,
                    &c.identity.account_id,
                    &c.identity.project_id,
                    &c.identity.stream_name,
                ),
                true,
            ),
            (
                k_project(&c.month, &c.identity.account_id, &c.identity.project_id),
                false,
            ),
        ] {
            let mut a: AggRow = match names.get(&key).or_else(|| projects.get(&key)) {
                Some(r) => r.clone(),
                None => get_json(&self.db, &key).await,
            };
            a.corr.absorb(&c);
            if is_name {
                names.insert(key, a);
            } else {
                projects.insert(key, a);
            }
        }
        if mr.finalized_at_ms.is_some() {
            let pk = format!(
                "corr-pending/{}/{}/{}/{}/{}",
                c.month,
                c.identity.account_id,
                c.identity.project_id,
                c.identity.stream_id,
                c.correction_id.replace('/', "~"),
            );
            if let Ok(v) = serde_json::to_vec(&c) {
                extra.push((pk.into_bytes(), v));
            }
        }
        mr.corrections.push(c);
    }

    async fn apply_read_batch(
        &self,
        rb: &ReadBatch,
        sources: &mut std::collections::HashMap<String, u64>,
        months: &mut std::collections::HashMap<Vec<u8>, MonthRow>,
        names: &mut std::collections::HashMap<Vec<u8>, AggRow>,
        projects: &mut std::collections::HashMap<Vec<u8>, AggRow>,
        extra: &mut Vec<(Vec<u8>, Vec<u8>)>,
    ) {
        let boot = rb.source.boot.clone();
        let floor = match sources.get(&boot) {
            Some(v) => *v,
            None => {
                let stored: u64 = self
                    .db
                    .get(&k_source(&boot)[..])
                    .await
                    .ok()
                    .flatten()
                    .map(|v| u64::from_le_bytes(v[..8].try_into().unwrap_or([0; 8])))
                    // The sentinel for "nothing processed" is MAX so
                    // seq 0 (a fresh boot's first batch) still applies.
                    .unwrap_or(u64::MAX);
                stored
            }
        };
        if floor != u64::MAX && rb.seq <= floor {
            return; // duplicate delivery of an applied batch
        }
        sources.insert(boot, rb.seq);
        // Round-22 item 5: EXACT integer allocation across every UTC
        // month boundary the batch intersects. Each dimension of each
        // row is split with integer proportions and the remainder goes
        // to the final span, so sum(allocations) == original for every
        // metered dimension — one byte can never bill twice, and a
        // batch spanning many months (a long outage) covers the
        // intermediate months too.
        let spans = month_spans(rb.from_ms, rb.to_ms);
        if spans.len() == 1 {
            self.apply_read_rows(rb, &spans[0].0, months, names, projects, extra)
                .await;
        } else {
            let total: i64 = spans.iter().map(|(_, d)| *d).sum::<i64>().max(1);
            let mut allocated: Vec<Vec<[u64; 5]>> = vec![vec![[0; 5]; rb.rows.len()]; spans.len()];
            for (ri, row) in rb.rows.iter().enumerate() {
                let dims = [
                    row.read_payload_bytes,
                    row.read_records,
                    row.read_operations,
                    row.queue_operations,
                    row.append_requests,
                ];
                for (di, v) in dims.iter().enumerate() {
                    let mut given = 0u64;
                    for (si, (_, dur)) in spans.iter().enumerate() {
                        let a = if si + 1 == spans.len() {
                            *v - given // remainder-to-last: exact by construction
                        } else {
                            ((*v as u128 * *dur as u128) / total as u128) as u64
                        };
                        allocated[si][ri][di] = a;
                        given += a;
                    }
                }
            }
            for (si, (month, _)) in spans.iter().enumerate() {
                let scaled = ReadBatch {
                    source: rb.source.clone(),
                    seq: rb.seq,
                    from_ms: rb.from_ms,
                    to_ms: rb.to_ms,
                    rows: rb
                        .rows
                        .iter()
                        .enumerate()
                        .map(|(ri, row)| ReadRow {
                            identity: row.identity.clone(),
                            read_payload_bytes: allocated[si][ri][0],
                            read_records: allocated[si][ri][1],
                            read_operations: allocated[si][ri][2],
                            queue_operations: allocated[si][ri][3],
                            append_requests: allocated[si][ri][4],
                        })
                        .collect(),
                };
                self.apply_read_rows(&scaled, month, months, names, projects, extra)
                    .await;
            }
        }
    }

    async fn apply_read_rows(
        &self,
        rb: &ReadBatch,
        month: &str,
        months: &mut std::collections::HashMap<Vec<u8>, MonthRow>,
        names: &mut std::collections::HashMap<Vec<u8>, AggRow>,
        projects: &mut std::collections::HashMap<Vec<u8>, AggRow>,
        extra: &mut Vec<(Vec<u8>, Vec<u8>)>,
    ) {
        let scale = |v: u64| -> u64 { v };
        for row in &rb.rows {
            let mkey = k_month(
                month,
                &row.identity.account_id,
                &row.identity.project_id,
                &row.identity.stream_id,
            );
            let mut mr: MonthRow = match months.get(&mkey) {
                Some(r) => r.clone(),
                None => get_json(&self.db, &mkey).await,
            };
            if mr.finalized_at_ms.is_some() {
                // Late reads after finalization: explicit correction
                // carrying EVERY read dimension (round-22 item 8),
                // frozen base untouched (round-21 blocker 8).
                let c = UsageCorrection {
                    identity: row.identity.clone(),
                    month: month.to_string(),
                    reason: format!(
                        "late read batch {}#{} after finalization",
                        rb.source.boot, rb.seq
                    ),
                    correction_id: format!(
                        "corr/read/{}/{}/{}/{}",
                        rb.source.boot, rb.seq, month, row.identity.stream_id
                    ),
                    correction_version: 1,
                    source_event_id: format!("read/{}/{}", rb.source.boot, rb.seq),
                    created_at_ms: crate::billing::billing_now_ms(),
                    ingest_payload_bytes_delta: 0,
                    ingest_records_delta: 0,
                    read_payload_bytes_delta: scale(row.read_payload_bytes) as i64,
                    read_records_delta: scale(row.read_records) as i64,
                    read_operations_delta: scale(row.read_operations) as i64,
                    queue_operations_delta: scale(row.queue_operations) as i64,
                    append_requests_delta: scale(row.append_requests) as i64,
                    storage_byte_ms_delta: "0".into(),
                };
                self.push_correction(&mut mr, c, names, projects, extra)
                    .await;
                months.insert(mkey, mr);
                continue;
            }
            mr.account_id = row.identity.account_id.clone();
            mr.stream_name = row.identity.stream_name.clone();
            mr.read_payload_bytes += scale(row.read_payload_bytes);
            mr.read_records += scale(row.read_records);
            mr.read_operations += scale(row.read_operations);
            mr.queue_operations += scale(row.queue_operations);
            mr.append_requests += scale(row.append_requests);
            months.insert(mkey, mr);
            for (key, agg) in [
                (
                    k_name(
                        month,
                        &row.identity.account_id,
                        &row.identity.project_id,
                        &row.identity.stream_name,
                    ),
                    true,
                ),
                (
                    k_project(month, &row.identity.account_id, &row.identity.project_id),
                    false,
                ),
            ] {
                let mut a: AggRow = match names.get(&key).or_else(|| projects.get(&key)) {
                    Some(r) => r.clone(),
                    None => get_json(&self.db, &key).await,
                };
                a.read_payload_bytes += scale(row.read_payload_bytes);
                a.read_records += scale(row.read_records);
                a.read_operations += scale(row.read_operations);
                a.queue_operations += scale(row.queue_operations);
                a.append_requests += scale(row.append_requests);
                if agg {
                    if !a.incarnations.contains(&row.identity.stream_id) {
                        a.incarnations.push(row.identity.stream_id.clone());
                    }
                    names.insert(key, a);
                } else {
                    projects.insert(key, a);
                }
            }
        }
    }

    async fn apply_snapshot(
        &self,
        snap: &SegmentSnapshot,
        months: &mut std::collections::HashMap<Vec<u8>, MonthRow>,
        segs: &mut std::collections::HashMap<Vec<u8>, SegmentState>,
        names: &mut std::collections::HashMap<Vec<u8>, AggRow>,
        projects: &mut std::collections::HashMap<Vec<u8>, AggRow>,
        extra: &mut Vec<(Vec<u8>, Vec<u8>)>,
    ) {
        let id = &snap.identity;
        let skey = k_segment(
            &id.account_id,
            &id.project_id,
            &id.stream_id,
            snap.segment_id,
        );
        let mut st: SegmentState = match segs.get(&skey) {
            Some(r) => r.clone(),
            None => get_json(&self.db, &skey).await,
        };
        let mkey = k_month(&snap.month, &id.account_id, &id.project_id, &id.stream_id);
        let mut mr: MonthRow = match months.get(&mkey) {
            Some(r) => r.clone(),
            None => get_json(&self.db, &mkey).await,
        };
        let finalized = mr.finalized_at_ms.is_some();
        let sm = mr.segments.entry(snap.segment_id).or_default();
        // Version fence PER (segment, month) target: the live row and a
        // month-final can carry the same version family; a strictly
        // older snapshot applies as nothing.
        if snap.usage_version <= sm.usage_version && !snap.month_final {
            return;
        }
        if snap.month_final && sm.final_seen && !finalized {
            return; // replayed final
        }
        if finalized {
            if snap.month_final && sm.final_seen && snap.usage_version <= sm.usage_version {
                return; // replayed final against a closed month
            }
            // LATE data into a FINALIZED month (round-21 blocker 8):
            // the frozen base never mutates — the delta becomes an
            // explicit, versioned correction, and the floors advance so
            // a replay corrects exactly once.
            let d_bytes = snap
                .ingest_payload_bytes_month
                .saturating_sub(sm.ingest_bytes);
            let d_recs = snap.ingest_records_month.saturating_sub(sm.ingest_records);
            let new_ms: u128 = snap.storage_byte_ms_month.parse().unwrap_or(0);
            let old_ms: u128 = sm.storage_byte_ms.parse().unwrap_or(0);
            let d_ms = new_ms.saturating_sub(old_ms);
            // Advance the floors FIRST (ends the segment borrow), so a
            // replay corrects exactly once; the deltas are already in
            // locals.
            sm.usage_version = snap.usage_version.max(sm.usage_version);
            sm.ingest_bytes = snap.ingest_payload_bytes_month.max(sm.ingest_bytes);
            sm.ingest_records = snap.ingest_records_month.max(sm.ingest_records);
            sm.storage_byte_ms = new_ms.max(old_ms).to_string();
            sm.final_seen = true;
            if d_bytes > 0 || d_ms > 0 || d_recs > 0 {
                let c = UsageCorrection {
                    identity: id.clone(),
                    month: snap.month.clone(),
                    reason: format!(
                        "late segment snapshot v{} after finalization",
                        snap.usage_version
                    ),
                    correction_id: format!(
                        "corr/snap/{}/{}",
                        snap.deterministic_event_id(),
                        snap.month
                    ),
                    correction_version: 1,
                    source_event_id: snap.deterministic_event_id(),
                    created_at_ms: crate::billing::billing_now_ms(),
                    ingest_payload_bytes_delta: d_bytes as i64,
                    ingest_records_delta: d_recs as i64,
                    read_payload_bytes_delta: 0,
                    read_records_delta: 0,
                    read_operations_delta: 0,
                    queue_operations_delta: 0,
                    append_requests_delta: 0,
                    storage_byte_ms_delta: d_ms.to_string(),
                };
                self.push_correction(&mut mr, c, names, projects, extra)
                    .await;
            }
            months.insert(mkey, mr);
            return;
        }
        // Deltas against the last applied absolutes.
        let d_bytes = snap
            .ingest_payload_bytes_month
            .saturating_sub(sm.ingest_bytes);
        let d_recs = snap.ingest_records_month.saturating_sub(sm.ingest_records);
        let new_ms: u128 = snap.storage_byte_ms_month.parse().unwrap_or(0);
        let old_ms: u128 = sm.storage_byte_ms.parse().unwrap_or(0);
        let d_ms = new_ms.saturating_sub(old_ms);
        sm.usage_version = snap.usage_version;
        sm.ingest_bytes = snap.ingest_payload_bytes_month;
        sm.ingest_records = snap.ingest_records_month;
        sm.storage_byte_ms = snap.storage_byte_ms_month.clone();
        sm.gauge_bytes = snap.owned_frame_bytes_current;
        sm.accounted_through_ms = snap.storage_accounted_through_ms;
        sm.final_seen |= snap.month_final;
        mr.account_id = id.account_id.clone();
        mr.stream_name = id.stream_name.clone();
        months.insert(mkey, mr);
        // Round-22 item 4: a rollover emits a month-FINAL and a live
        // snapshot under the SAME usage_version. Whichever arrives
        // second must still be able to advance the global state, or the
        // next month's carry starts from the stale gauge/boundary — so
        // same-version ties break on the storage clock.
        let advances = snap.usage_version > st.usage_version
            || (snap.usage_version == st.usage_version
                && snap.storage_accounted_through_ms > st.storage_accounted_through_ms);
        if advances {
            st.usage_version = snap.usage_version;
            st.owned_frame_bytes_current = snap.owned_frame_bytes_current;
            st.storage_accounted_through_ms = snap.storage_accounted_through_ms;
            st.stream_name = id.stream_name.clone();
            st.account_id = id.account_id.clone();
            segs.insert(skey, st);
        }
        for (key, is_name) in [
            (
                k_name(&snap.month, &id.account_id, &id.project_id, &id.stream_name),
                true,
            ),
            (
                k_project(&snap.month, &id.account_id, &id.project_id),
                false,
            ),
        ] {
            let mut a: AggRow = match names.get(&key).or_else(|| projects.get(&key)) {
                Some(r) => r.clone(),
                None => get_json(&self.db, &key).await,
            };
            a.ingest_bytes += d_bytes;
            a.ingest_records += d_recs;
            a.add_storage(d_ms);
            if is_name {
                if !a.incarnations.contains(&id.stream_id) {
                    a.incarnations.push(id.stream_id.clone());
                }
                names.insert(key, a);
            } else {
                projects.insert(key, a);
            }
        }
    }

    /// L0 posture of the rollup DB (same in-memory manifest probe as
    /// the history partitions and the read spool).
    pub fn l0_stats(&self) -> (u64, u64, u64, u64) {
        crate::history::history_l0_stats(&self.db)
    }

    // ---- point reads (the customer API) ------------------------------

    pub async fn month_row(
        &self,
        month: &str,
        account: &str,
        project: &str,
        stream_id: &str,
    ) -> Option<MonthRow> {
        self.db
            .get(&k_month(month, account, project, stream_id)[..])
            .await
            .ok()
            .flatten()
            .and_then(|v| serde_json::from_slice(&v).ok())
    }

    pub async fn name_row(
        &self,
        month: &str,
        account: &str,
        project: &str,
        name: &str,
    ) -> Option<AggRow> {
        self.db
            .get(&k_name(month, account, project, name)[..])
            .await
            .ok()
            .flatten()
            .and_then(|v| serde_json::from_slice(&v).ok())
    }

    pub async fn project_row(&self, month: &str, account: &str, project: &str) -> Option<AggRow> {
        self.db
            .get(&k_project(month, account, project)[..])
            .await
            .ok()
            .flatten()
            .and_then(|v| serde_json::from_slice(&v).ok())
    }

    /// All persistent segment states for one stream (bounded by its
    /// segment count) — the current-month fallback when no month row
    /// exists yet (round-21 blocker 2).
    pub async fn stream_segment_states(
        &self,
        account: &str,
        project: &str,
        stream_id: &str,
    ) -> Vec<SegmentState> {
        let pfx = format!("segment/{account}/{project}/{stream_id}/").into_bytes();
        let mut out = Vec::new();
        if let Ok(mut iter) = self.db.scan_prefix(&pfx[..], ..).await {
            while let Ok(Some(kv)) = iter.next().await {
                if let Ok(st) = serde_json::from_slice::<SegmentState>(&kv.value) {
                    out.push(st);
                }
            }
        }
        out
    }

    /// Pending monthly artifacts (blocker 7): (pending key, month,
    /// project, stream-id, row).
    pub async fn pending_artifacts(
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
    pub async fn mark_artifact_published(
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
    pub async fn pending_correction_artifacts(
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

    pub async fn mark_correction_published(
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

    /// Round-22 item 8: close every overdue month IN ORDER from the
    /// persisted oldest-unclosed marker. A rollup that was down across
    /// one or more boundaries catches up oldest-first; the marker
    /// advances only after that month's close completed, so a crash
    /// resumes at the same month. Returns (month, streams closed).
    pub async fn close_months_due(&self, grace_ms: i64) -> anyhow::Result<Vec<(String, usize)>> {
        const MARKER: &[u8] = b"meta/oldest-unclosed-month";
        fn prev_month(y: i32, m: u32) -> (i32, u32) {
            if m == 1 { (y - 1, 12) } else { (y, m - 1) }
        }
        let now = crate::billing::billing_now_ms();
        let (cy, cm) = crate::billing::utc_year_month(now);
        let (mut y, mut m) = match self.db.get(MARKER).await? {
            Some(v) => {
                let s = String::from_utf8_lossy(&v).to_string();
                match parse_month(&s) {
                    Some(x) => x,
                    None => prev_month(cy, cm),
                }
            }
            None => {
                // First run: start at the OLDEST month with data — a
                // fresh marker must not skip a backlog that predates
                // it. `month/` keys sort by month string, so the first
                // key names the oldest.
                let mut it = self.db.scan_prefix(&b"month/"[..], ..).await?;
                match it.next().await? {
                    Some(kv) => String::from_utf8_lossy(&kv.key)
                        .split('/')
                        .nth(1)
                        .and_then(parse_month)
                        .unwrap_or_else(|| prev_month(cy, cm)),
                    None => prev_month(cy, cm),
                }
            }
        };
        let mut out = Vec::new();
        // Safety cap far above any real backlog; the loop also stops
        // at the current (never-closeable) month.
        for _ in 0..600 {
            if (y, m) >= (cy, cm) {
                break;
            }
            let (ny, nm) = next_month(y, m);
            if now < month_start_ms(ny, nm) + grace_ms {
                break; // grace not yet met; younger months even less so
            }
            let n = self.close_month(y, m, grace_ms).await?;
            out.push((crate::billing::month_str(y, m), n));
            let mut wb = WriteBatch::new();
            wb.put(MARKER, crate::billing::month_str(ny, nm).as_bytes());
            self.db.write(wb).await?;
            (y, m) = (ny, nm);
        }
        Ok(out)
    }

    // ---- month close (§9.4/§9.5/§9.6) --------------------------------

    /// Close (year, month) — round-21 blockers 2 and 9.
    ///
    /// PASS A (carry): page the persistent `segment/` index and, for
    /// every segment whose storage clock lags the month boundary,
    /// synthesize the missing byte-time up to the boundary INTO the
    /// closing month's row — an idle retained stream accrues every
    /// month with no stream write and no data-plane traffic — then
    /// advance the segment's accounting boundary.
    ///
    /// PASS B (finalize): page the month's rows, extrapolate any
    /// remaining non-final segment to the exact boundary, stamp
    /// `finalized_at`, and hand each closed row to `artifact`.
    ///
    /// Both passes run in bounded chunks (`CLOSE_CHUNK` rows), each
    /// chunk one durable WriteBatch behind a persisted cursor — a crash
    /// resumes mid-month with no lost or repeated accrual (the carry is
    /// guarded by per-segment `final_seen`/boundary checks, so a replay
    /// applies zero).
    pub async fn close_month(&self, year: i32, month: u32, grace_ms: i64) -> anyhow::Result<usize> {
        // Round-22 item 9: chunks are bounded by ROWS AND BYTES — a
        // month of few-but-huge rows (a stream with thousands of
        // segments) must not build an unbounded WriteBatch.
        const CLOSE_CHUNK: usize = 1000;
        const CLOSE_CHUNK_BYTES: usize = 1_000_000;
        let mstr = crate::billing::month_str(year, month);
        let (ny, nm) = next_month(year, month);
        let boundary = month_start_ms(ny, nm);
        let start = month_start_ms(year, month);
        let now = crate::billing::billing_now_ms();
        if now < boundary + grace_ms {
            return Ok(0); // not yet closeable
        }
        // ---- pass A: carry idle gauges into the closing month ----
        let seg_cursor_key = format!("meta/close-seg-cursor/{mstr}").into_bytes();
        let mut after: Option<Vec<u8>> = self
            .db
            .get(&seg_cursor_key[..])
            .await
            .ok()
            .flatten()
            .map(|v| v.to_vec());
        loop {
            let mut wb = WriteBatch::new();
            let mut page: Vec<(Vec<u8>, SegmentState)> = Vec::new();
            let mut page_bytes = 0usize;
            {
                let mut iter = self.db.scan_prefix(&b"segment/"[..], ..).await?;
                while let Some(kv) = iter.next().await? {
                    if let Some(a) = &after
                        && kv.key.as_ref() <= &a[..]
                    {
                        continue;
                    }
                    page_bytes += kv.value.len();
                    if let Ok(st) = serde_json::from_slice::<SegmentState>(&kv.value) {
                        page.push((kv.key.to_vec(), st));
                    }
                    // Byte-bound only once something is in the page —
                    // an empty page must mean "pass done", never
                    // "chunk full of undecodable rows".
                    if page.len() >= CLOSE_CHUNK
                        || (page_bytes >= CLOSE_CHUNK_BYTES && !page.is_empty())
                    {
                        break;
                    }
                }
            }
            if page.is_empty() {
                break;
            }
            let last_key = page.last().unwrap().0.clone();
            // Round-22 item 3: page-LOCAL row caches, exactly like
            // apply_page. Two segments of one stream in one page must
            // MERGE into a single month-row put — independent
            // read-modify-writes into the same WriteBatch let the last
            // put win and silently dropped a segment's byte-time, with
            // both SegmentStates already advanced (unrecoverable).
            let mut mrows: std::collections::HashMap<Vec<u8>, MonthRow> = Default::default();
            let mut arows: std::collections::HashMap<Vec<u8>, AggRow> = Default::default();
            for (key, mut st) in page {
                if st.storage_accounted_through_ms >= boundary {
                    continue;
                }
                // key = segment/<account>/<project>/<stream-id>/<seg>
                let parts: Vec<&str> = std::str::from_utf8(&key)
                    .unwrap_or("")
                    .splitn(5, '/')
                    .collect();
                if parts.len() != 5 {
                    continue;
                }
                let (account, project, stream_id) = (parts[1], parts[2], parts[3]);
                let seg_id: u32 = parts[4].parse().unwrap_or(0);
                let mkey = k_month(&mstr, account, project, stream_id);
                let mut row: MonthRow = match mrows.get(&mkey) {
                    Some(rw) => rw.clone(),
                    None => get_json(&self.db, &mkey).await,
                };
                let sm = row.segments.entry(seg_id).or_default();
                if !sm.final_seen {
                    let span_start = st.storage_accounted_through_ms.max(start);
                    if span_start < boundary && st.owned_frame_bytes_current > 0 {
                        let add =
                            (boundary - span_start) as u128 * st.owned_frame_bytes_current as u128;
                        let cur: u128 = sm.storage_byte_ms.parse().unwrap_or(0);
                        sm.storage_byte_ms = (cur + add).to_string();
                        // Aggregates absorb the same delta.
                        for (akey, is_name) in [
                            (k_name(&mstr, account, project, &st.stream_name), true),
                            (k_project(&mstr, account, project), false),
                        ] {
                            let mut a: AggRow = match arows.get(&akey) {
                                Some(x) => x.clone(),
                                None => get_json(&self.db, &akey).await,
                            };
                            a.add_storage(add);
                            if is_name && !a.incarnations.contains(&stream_id.to_string()) {
                                a.incarnations.push(stream_id.to_string());
                            }
                            arows.insert(akey, a);
                        }
                    }
                    sm.gauge_bytes = st.owned_frame_bytes_current;
                    sm.accounted_through_ms = boundary;
                    sm.final_seen = true;
                    row.account_id = st.account_id.clone();
                    if row.stream_name.is_empty() {
                        row.stream_name = st.stream_name.clone();
                    }
                    row.updated_ms = now;
                    mrows.insert(mkey, row);
                }
                st.storage_accounted_through_ms = boundary;
                wb.put(key, serde_json::to_vec(&st)?);
            }
            for (k, row) in &mrows {
                wb.put(k.clone(), serde_json::to_vec(row)?);
            }
            for (k, a) in &arows {
                wb.put(k.clone(), serde_json::to_vec(a)?);
            }
            wb.put(seg_cursor_key.clone(), last_key.clone());
            self.db.write(wb).await?;
            after = Some(last_key);
        }
        // ---- pass B: finalize the month's rows, chunked ----
        let fin_cursor_key = format!("meta/close-fin-cursor/{mstr}").into_bytes();
        let mut fin_after: Option<Vec<u8>> = self
            .db
            .get(&fin_cursor_key[..])
            .await
            .ok()
            .flatten()
            .map(|v| v.to_vec());
        let pfx = k_month_prefix(&mstr);
        let mut closed = 0usize;
        loop {
            let mut wb = WriteBatch::new();
            let mut page: Vec<(Vec<u8>, MonthRow)> = Vec::new();
            let mut page_bytes = 0usize;
            {
                let mut iter = self.db.scan_prefix(&pfx[..], ..).await?;
                while let Some(kv) = iter.next().await? {
                    if let Some(a) = &fin_after
                        && kv.key.as_ref() <= &a[..]
                    {
                        continue;
                    }
                    page_bytes += kv.value.len();
                    if let Ok(row) = serde_json::from_slice::<MonthRow>(&kv.value) {
                        page.push((kv.key.to_vec(), row));
                    }
                    if page.len() >= CLOSE_CHUNK
                        || (page_bytes >= CLOSE_CHUNK_BYTES && !page.is_empty())
                    {
                        break;
                    }
                }
            }
            if page.is_empty() {
                break;
            }
            let last_key = page.last().unwrap().0.clone();
            for (key, mut row) in page {
                if row.finalized_at_ms.is_some() {
                    continue;
                }
                for sm in row.segments.values_mut() {
                    if !sm.final_seen && sm.accounted_through_ms < boundary {
                        let cur: u128 = sm.storage_byte_ms.parse().unwrap_or(0);
                        let from = sm.accounted_through_ms.max(start);
                        let add = (boundary - from).max(0) as u128 * sm.gauge_bytes as u128;
                        sm.storage_byte_ms = (cur + add).to_string();
                        sm.accounted_through_ms = boundary;
                        sm.final_seen = true;
                    }
                }
                row.finalized_at_ms = Some(now);
                // Freeze the invoice base (blocker 8) and stage the
                // monthly artifact as a PENDING row in the SAME batch
                // (blocker 7): publication is two-phase — a failed or
                // crashed PUT retries from this durable outbox, and a
                // finalized row is never re-derived.
                row.frozen = Some(FrozenTotals {
                    ingest_bytes: row.ingest_bytes(),
                    ingest_records: row.ingest_records(),
                    storage_byte_ms: row.storage_byte_ms().to_string(),
                    read_payload_bytes: row.read_payload_bytes,
                    read_records: row.read_records,
                    read_operations: row.read_operations,
                    queue_operations: row.queue_operations,
                    append_requests: row.append_requests,
                });
                let parts: Vec<&str> = std::str::from_utf8(&key)
                    .unwrap_or("")
                    .splitn(4, '/')
                    .collect();
                if parts.len() == 4 {
                    let pkey = format!("artifact-pending/{mstr}/{}/{}", parts[2], parts[3]);
                    wb.put(pkey.into_bytes(), serde_json::to_vec(&row)?);
                }
                wb.put(key, serde_json::to_vec(&row)?);
                closed += 1;
            }
            wb.put(fin_cursor_key.clone(), last_key.clone());
            self.db.write(wb).await?;
            fin_after = Some(last_key);
        }
        // Cursors are month-scoped; clear them once the month is done.
        let mut wb = WriteBatch::new();
        wb.delete(seg_cursor_key);
        wb.delete(fin_cursor_key);
        self.db.write(wb).await?;
        Ok(closed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::billing::{BillingIdentity, MeterSource, ReadRow, UsageEnvelope, UsagePayload};

    fn mem_store() -> Arc<dyn object_store::ObjectStore> {
        Arc::new(object_store::memory::InMemory::new())
    }

    fn id() -> BillingIdentity {
        BillingIdentity {
            account_id: "acct".into(),
            project_id: "proj".into(),
            stream_id: "aa".repeat(8),
            stream_name: "orders".into(),
        }
    }

    fn snap(
        version: u64,
        month: &str,
        bytes: u64,
        byte_ms: u128,
        gauge: u64,
        final_: bool,
    ) -> UsageEnvelope {
        let s = SegmentSnapshot {
            identity: id(),
            segment_id: 0,
            usage_version: version,
            month: month.into(),
            month_final: final_,
            ingest_payload_bytes_month: bytes,
            ingest_records_month: bytes / 10,
            owned_frame_bytes_current: gauge,
            storage_byte_ms_month: byte_ms.to_string(),
            storage_accounted_through_ms: crate::billing::month_start_ms(2026, 7) + 1_000_000,
            retained_by_forks: false,
        };
        UsageEnvelope {
            v: 1,
            event_id: s.deterministic_event_id(),
            event_time_ms: 0,
            emitted_ms: 0,
            cell: "c".into(),
            payload: UsagePayload::SegmentSnapshot(s),
        }
    }

    /// Round-22 item 3: THREE segments of one stream, idle months —
    /// every month bills the SUM of all three (nothing last-put-wins),
    /// and name/project aggregates match.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn multi_segment_idle_carry_sums_all_segments() {
        let _xw = crate::billing::billing_clock_lock().write().await;
        struct Reset;
        impl Drop for Reset {
            fn drop(&mut self) {
                crate::billing::BILLING_CLOCK_OVERRIDE
                    .store(0, std::sync::atomic::Ordering::Relaxed);
            }
        }
        let _r = Reset;
        crate::billing::BILLING_CLOCK_OVERRIDE.store(
            crate::billing::month_start_ms(2026, 12),
            std::sync::atomic::Ordering::Relaxed,
        );
        let r = UsageRollup::open(mem_store(), "mseg").await.unwrap();
        let jul15 = crate::billing::month_start_ms(2026, 7) + 14 * 86_400_000;
        // Three segments, gauges 100/200/300, all accounted through Jul 15.
        for (seg, gauge) in [(0u32, 100u64), (1, 200), (2, 300)] {
            let s = SegmentSnapshot {
                identity: id(),
                segment_id: seg,
                usage_version: 1,
                month: "2026-07".into(),
                month_final: false,
                ingest_payload_bytes_month: 10,
                ingest_records_month: 1,
                owned_frame_bytes_current: gauge,
                storage_byte_ms_month: "0".into(),
                storage_accounted_through_ms: jul15,
                retained_by_forks: false,
            };
            let env = UsageEnvelope {
                v: 1,
                event_id: s.deterministic_event_id(),
                event_time_ms: jul15,
                emitted_ms: jul15,
                cell: "c".into(),
                payload: UsagePayload::SegmentSnapshot(s),
            };
            r.apply_page(&[env], &format!("c{seg}")).await.unwrap();
        }
        let day = 86_400_000u128;
        let total_gauge = 600u128;
        // July: 17 idle days x 600 B across the three segments.
        assert_eq!(r.close_month(2026, 7, 0).await.unwrap(), 1);
        let jul = r
            .month_row("2026-07", "acct", "proj", &id().stream_id)
            .await
            .unwrap();
        assert_eq!(jul.storage_byte_ms(), 17 * day * total_gauge);
        // August, fully idle: 31 days x 600 — the multi-segment carry
        // must SUM, not last-put-win.
        assert_eq!(r.close_month(2026, 8, 0).await.unwrap(), 1);
        let aug = r
            .month_row("2026-08", "acct", "proj", &id().stream_id)
            .await
            .unwrap();
        assert_eq!(aug.storage_byte_ms(), 31 * day * total_gauge);
        assert_eq!(aug.segments.len(), 3, "every segment carried");
        let agg = r.project_row("2026-08", "acct", "proj").await.unwrap();
        assert_eq!(agg.storage_byte_ms, (31 * day * total_gauge).to_string());
        let name = r
            .name_row("2026-08", "acct", "proj", "orders")
            .await
            .unwrap();
        assert_eq!(name.storage_byte_ms, (31 * day * total_gauge).to_string());
    }

    /// Round-22 item 4: a rollover's month-final and live snapshots
    /// share a usage version; whichever applies second must still
    /// advance the global segment state, and the NEXT month's carry
    /// uses the NEW gauge exactly.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn same_version_live_snapshot_advances_segment_state() {
        let _xw = crate::billing::billing_clock_lock().write().await;
        struct Reset;
        impl Drop for Reset {
            fn drop(&mut self) {
                crate::billing::BILLING_CLOCK_OVERRIDE
                    .store(0, std::sync::atomic::Ordering::Relaxed);
            }
        }
        let _r = Reset;
        crate::billing::BILLING_CLOCK_OVERRIDE.store(
            crate::billing::month_start_ms(2026, 11),
            std::sync::atomic::Ordering::Relaxed,
        );
        let r = UsageRollup::open(mem_store(), "svtie").await.unwrap();
        let aug1 = crate::billing::month_start_ms(2026, 8);
        // Month-FINAL for July at version 5 (accounted through Aug 1,
        // OLD gauge 100), then the LIVE August snapshot at the SAME
        // version with the NEW gauge 400 accounted through Aug 2.
        let mk = |month: &str, final_: bool, gauge: u64, through: i64, byte_ms: u128| {
            let s = SegmentSnapshot {
                identity: id(),
                segment_id: 0,
                usage_version: 5,
                month: month.into(),
                month_final: final_,
                ingest_payload_bytes_month: 0,
                ingest_records_month: 0,
                owned_frame_bytes_current: gauge,
                storage_byte_ms_month: byte_ms.to_string(),
                storage_accounted_through_ms: through,
                retained_by_forks: false,
            };
            UsageEnvelope {
                v: 1,
                event_id: s.deterministic_event_id(),
                event_time_ms: through,
                emitted_ms: through,
                cell: "c".into(),
                payload: UsagePayload::SegmentSnapshot(s),
            }
        };
        r.apply_page(
            &[
                mk("2026-07", true, 100, aug1, 999),
                mk("2026-08", false, 400, aug1 + 86_400_000, 34_560_000_000),
            ],
            "c1",
        )
        .await
        .unwrap();
        let st = &r
            .stream_segment_states("acct", "proj", &id().stream_id)
            .await[0];
        assert_eq!(
            st.owned_frame_bytes_current, 400,
            "the live snapshot must win the same-version tie"
        );
        assert_eq!(st.storage_accounted_through_ms, aug1 + 86_400_000);
        // Close August: gauge 400 from Aug 2 to Sep 1 (30 more days).
        assert_eq!(r.close_month(2026, 8, 0).await.unwrap(), 1);
        let aug = r
            .month_row("2026-08", "acct", "proj", &id().stream_id)
            .await
            .unwrap();
        let day = 86_400_000u128;
        assert_eq!(
            aug.storage_byte_ms(),
            34_560_000_000 + 30 * day * 400,
            "the month close must extrapolate from the NEW gauge"
        );
    }

    /// Round-22 item 5: integer month allocation is exact — the sum of
    /// per-month allocations equals the original for every dimension,
    /// across two AND four month spans.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn read_batch_month_split_is_exact() {
        let r = UsageRollup::open(mem_store(), "split").await.unwrap();
        let aug1 = crate::billing::month_start_ms(2026, 8);
        // One byte, one record, one op split dead-center on a boundary:
        // must land as 0+1 or 1+0, never 1+1.
        let rb = ReadBatch {
            source: MeterSource {
                cell: "c".into(),
                instance: "i".into(),
                boot: "bx".into(),
            },
            seq: 0,
            from_ms: aug1 - 5_000,
            to_ms: aug1 + 5_000,
            rows: vec![ReadRow {
                identity: id(),
                read_payload_bytes: 1,
                read_records: 1,
                read_operations: 1,
                queue_operations: 0,
                append_requests: 0,
            }],
        };
        let env = UsageEnvelope {
            v: 1,
            event_id: "read/bx/0".into(),
            event_time_ms: 0,
            emitted_ms: 0,
            cell: "c".into(),
            payload: UsagePayload::ReadBatch(rb),
        };
        r.apply_page(&[env], "c1").await.unwrap();
        let jul = r
            .month_row("2026-07", "acct", "proj", &id().stream_id)
            .await
            .unwrap_or_default();
        let aug = r
            .month_row("2026-08", "acct", "proj", &id().stream_id)
            .await
            .unwrap_or_default();
        assert_eq!(
            jul.read_payload_bytes + aug.read_payload_bytes,
            1,
            "one byte bills exactly once"
        );
        assert_eq!(jul.read_records + aug.read_records, 1);
        // A four-month batch (long outage) covers the INTERIOR months.
        let rb2 = ReadBatch {
            source: MeterSource {
                cell: "c".into(),
                instance: "i".into(),
                boot: "by".into(),
            },
            seq: 0,
            from_ms: crate::billing::month_start_ms(2026, 1) + 10,
            to_ms: crate::billing::month_start_ms(2026, 4) + 10,
            rows: vec![ReadRow {
                identity: id(),
                read_payload_bytes: 1_000_003,
                read_records: 77,
                read_operations: 13,
                queue_operations: 5,
                append_requests: 9,
            }],
        };
        let env2 = UsageEnvelope {
            v: 1,
            event_id: "read/by/0".into(),
            event_time_ms: 0,
            emitted_ms: 0,
            cell: "c".into(),
            payload: UsagePayload::ReadBatch(rb2),
        };
        r.apply_page(&[env2], "c2").await.unwrap();
        let mut sums = [0u64; 5];
        for m in ["2026-01", "2026-02", "2026-03", "2026-04"] {
            let row = r
                .month_row(m, "acct", "proj", &id().stream_id)
                .await
                .unwrap_or_default();
            sums[0] += row.read_payload_bytes;
            sums[1] += row.read_records;
            sums[2] += row.read_operations;
            sums[3] += row.queue_operations;
            sums[4] += row.append_requests;
            if m == "2026-02" || m == "2026-03" {
                assert!(row.read_payload_bytes > 0, "interior month {m} covered");
            }
        }
        assert_eq!(
            sums,
            [1_000_003, 77, 13, 5, 9],
            "sum preserved per dimension"
        );
    }

    /// Round-22 item 8: a rollup that was down across several
    /// boundaries closes every overdue month IN ORDER from the
    /// persisted oldest-unclosed marker, and the marker survives so
    /// the next tick starts where this one stopped.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn missed_months_catch_up_in_order() {
        let _xw = crate::billing::billing_clock_lock().write().await;
        struct Reset;
        impl Drop for Reset {
            fn drop(&mut self) {
                crate::billing::BILLING_CLOCK_OVERRIDE
                    .store(0, std::sync::atomic::Ordering::Relaxed);
            }
        }
        let _r = Reset;
        crate::billing::BILLING_CLOCK_OVERRIDE.store(
            crate::billing::month_start_ms(2026, 12),
            std::sync::atomic::Ordering::Relaxed,
        );
        let r = UsageRollup::open(mem_store(), "catchup").await.unwrap();
        let jul15 = crate::billing::month_start_ms(2026, 7) + 14 * 86_400_000;
        let s = SegmentSnapshot {
            identity: id(),
            segment_id: 0,
            usage_version: 1,
            month: "2026-07".into(),
            month_final: false,
            ingest_payload_bytes_month: 10,
            ingest_records_month: 1,
            owned_frame_bytes_current: 100,
            storage_byte_ms_month: "0".into(),
            storage_accounted_through_ms: jul15,
            retained_by_forks: false,
        };
        let env = UsageEnvelope {
            v: 1,
            event_id: s.deterministic_event_id(),
            event_time_ms: jul15,
            emitted_ms: jul15,
            cell: "c".into(),
            payload: UsagePayload::SegmentSnapshot(s),
        };
        r.apply_page(&[env], "c1").await.unwrap();
        // The rollup "was down" July..November: one catch-up call at
        // Dec 1 closes Jul, Aug, Sep, Oct, Nov — in that order.
        let closed = r.close_months_due(0).await.unwrap();
        let months: Vec<&str> = closed.iter().map(|(m, _)| m.as_str()).collect();
        assert_eq!(
            months,
            vec!["2026-07", "2026-08", "2026-09", "2026-10", "2026-11"],
            "oldest first, no gaps"
        );
        assert!(
            closed.iter().all(|(_, n)| *n == 1),
            "every month closed the stream"
        );
        let day = 86_400_000u128;
        let nov = r
            .month_row("2026-11", "acct", "proj", &id().stream_id)
            .await
            .unwrap();
        assert_eq!(
            nov.storage_byte_ms(),
            30 * day * 100,
            "idle November billed"
        );
        assert_eq!(
            r.pending_artifacts(64).await.unwrap().len(),
            5,
            "one artifact per closed month"
        );
        // Marker advanced: nothing further due.
        assert!(r.close_months_due(0).await.unwrap().is_empty());
    }

    /// Round-21 blocker 2, the reviewer's exact scenario: one July
    /// write, then TOTAL silence. July, August and September must each
    /// accrue the correct storage byte-time, each produce one artifact,
    /// and the gauge must never read as zero. Re-closing any month is a
    /// no-op.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn idle_retained_months_accrue_storage() {
        // Months close only after their boundary on the TRUSTED clock —
        // inject December so July-September are all closeable.
        let _xw = crate::billing::billing_clock_lock().write().await;
        struct Reset;
        impl Drop for Reset {
            fn drop(&mut self) {
                crate::billing::BILLING_CLOCK_OVERRIDE
                    .store(0, std::sync::atomic::Ordering::Relaxed);
            }
        }
        let _reset = Reset;
        crate::billing::BILLING_CLOCK_OVERRIDE.store(
            crate::billing::month_start_ms(2026, 12),
            std::sync::atomic::Ordering::Relaxed,
        );
        let r = UsageRollup::open(mem_store(), "idle").await.unwrap();
        let jul15 = crate::billing::month_start_ms(2026, 7) + 14 * 86_400_000;
        // Live snapshot: gauge 100 B, accounted through Jul 15.
        let mut s0 = match snap(1, "2026-07", 500, 0, 100, false).payload.clone() {
            UsagePayload::SegmentSnapshot(x) => x,
            _ => unreachable!(),
        };
        s0.storage_accounted_through_ms = jul15;
        let env = UsageEnvelope {
            v: 1,
            event_id: s0.deterministic_event_id(),
            event_time_ms: jul15,
            emitted_ms: jul15,
            cell: "c".into(),
            payload: UsagePayload::SegmentSnapshot(s0),
        };
        r.apply_page(&[env], "c1").await.unwrap();

        let day = 86_400_000u128;
        let art_for = |arts: Vec<(Vec<u8>, String, String, String, MonthRow)>, m: &str| {
            arts.into_iter()
                .find(|(_, month, ..)| month == m)
                .map(|(.., row)| row.storage_byte_ms())
        };
        // Close July: Jul 15 -> Aug 1 = 17 idle days at 100 B.
        assert_eq!(r.close_month(2026, 7, 0).await.unwrap(), 1);
        assert_eq!(
            art_for(r.pending_artifacts(64).await.unwrap(), "2026-07"),
            Some(17 * day * 100)
        );
        // Close August: a FULLY idle month — no row existed until the
        // carry pass synthesized it. 31 days at 100 B.
        assert_eq!(
            r.close_month(2026, 8, 0).await.unwrap(),
            1,
            "an idle month must still close its stream"
        );
        assert_eq!(
            art_for(r.pending_artifacts(64).await.unwrap(), "2026-08"),
            Some(31 * day * 100)
        );
        // September: 30 days.
        assert_eq!(r.close_month(2026, 9, 0).await.unwrap(), 1);
        assert_eq!(
            art_for(r.pending_artifacts(64).await.unwrap(), "2026-09"),
            Some(30 * day * 100)
        );
        // The durable segment state still knows the gauge.
        let states = r
            .stream_segment_states("acct", "proj", &id().stream_id)
            .await;
        assert_eq!(states.len(), 1);
        assert_eq!(states[0].owned_frame_bytes_current, 100);
        // Replays are no-ops.
        assert_eq!(r.close_month(2026, 8, 0).await.unwrap(), 0);
        // Aggregates carried the idle storage too.
        let aug_proj = r.project_row("2026-08", "acct", "proj").await.unwrap();
        assert_eq!(aug_proj.storage_byte_ms, (31 * day * 100).to_string());
    }

    /// Snapshots are ABSOLUTE; aggregates absorb them as deltas, and a
    /// replay applies as zero. Month close extrapolates idle gauges to
    /// the boundary, finalizes, and emits exactly one artifact per
    /// stream; a second close is a no-op.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn rollup_applies_deltas_and_closes_months() {
        let r = UsageRollup::open(mem_store(), "t1").await.unwrap();
        // Version 1: 100 bytes, byte-ms 5000. Version 2: 250 bytes,
        // byte-ms 9000 (absolute). Replay of version 2.
        r.apply_page(&[snap(1, "2026-07", 100, 5000, 40, false)], "c1")
            .await
            .unwrap();
        r.apply_page(&[snap(2, "2026-07", 250, 9000, 40, false)], "c2")
            .await
            .unwrap();
        r.apply_page(&[snap(2, "2026-07", 250, 9000, 40, false)], "c3")
            .await
            .unwrap();
        let row = r
            .month_row("2026-07", "acct", "proj", &id().stream_id)
            .await
            .unwrap();
        assert_eq!(row.ingest_bytes(), 250, "absolute, not summed");
        assert_eq!(row.storage_byte_ms(), 9000);
        let proj = r.project_row("2026-07", "acct", "proj").await.unwrap();
        assert_eq!(proj.ingest_bytes, 250, "aggregate absorbed deltas once");
        assert_eq!(proj.storage_byte_ms, "9000");
        let name = r
            .name_row("2026-07", "acct", "proj", "orders")
            .await
            .unwrap();
        assert_eq!(name.incarnations, vec![id().stream_id]);

        // A read batch, then its duplicate: applied once.
        let rb = ReadBatch {
            source: MeterSource {
                cell: "c".into(),
                instance: "i".into(),
                boot: "b1".into(),
            },
            seq: 0,
            from_ms: crate::billing::month_start_ms(2026, 7),
            to_ms: crate::billing::month_start_ms(2026, 7) + 5,
            rows: vec![ReadRow {
                identity: id(),
                read_payload_bytes: 77,
                read_records: 3,
                read_operations: 2,
                queue_operations: 1,
                append_requests: 4,
            }],
        };
        let env = UsageEnvelope {
            v: 1,
            event_id: "read/b1/0".into(),
            event_time_ms: 0,
            emitted_ms: 0,
            cell: "c".into(),
            payload: UsagePayload::ReadBatch(rb),
        };
        r.apply_page(std::slice::from_ref(&env), "c4")
            .await
            .unwrap();
        r.apply_page(std::slice::from_ref(&env), "c5")
            .await
            .unwrap();
        let row = r
            .month_row("2026-07", "acct", "proj", &id().stream_id)
            .await
            .unwrap();
        assert_eq!(row.read_payload_bytes, 77, "source-seq dedupe");
        assert_eq!(row.append_requests, 4);

        // Close July (grace 0): the idle gauge extrapolates from
        // accounted_through to the boundary, the row finalizes, one
        // artifact per stream.
        let n = r.close_month(2026, 7, 0).await.unwrap();
        assert_eq!(n, 1);
        let artifacts = r.pending_artifacts(64).await.unwrap();
        assert_eq!(artifacts.len(), 1);
        let boundary = month_start_ms(2026, 8);
        let through = crate::billing::month_start_ms(2026, 7) + 1_000_000;
        let expect = 9000u128 + (boundary - through) as u128 * 40;
        assert_eq!(
            artifacts[0].4.storage_byte_ms(),
            expect,
            "idle extrapolation to the boundary"
        );
        let closed = r
            .month_row("2026-07", "acct", "proj", &id().stream_id)
            .await
            .unwrap();
        assert!(closed.finalized_at_ms.is_some());
        // Second close: nothing left to do.
        assert_eq!(r.close_month(2026, 7, 0).await.unwrap(), 0);

        // A correction after close appends explicitly; the base row is
        // never silently rewritten (§9.5).
        let corr = UsageEnvelope {
            v: 1,
            event_id: "corr/1".into(),
            event_time_ms: 0,
            emitted_ms: 0,
            cell: "c".into(),
            payload: UsagePayload::UsageCorrection(crate::billing::UsageCorrection {
                identity: id(),
                month: "2026-07".into(),
                reason: "late read batch".into(),
                correction_id: String::new(), // filled from the envelope
                correction_version: 0,
                source_event_id: String::new(),
                created_at_ms: 0,
                ingest_payload_bytes_delta: 0,
                ingest_records_delta: 0,
                read_payload_bytes_delta: 12,
                read_records_delta: 0,
                read_operations_delta: 0,
                queue_operations_delta: 0,
                append_requests_delta: 0,
                storage_byte_ms_delta: "0".into(),
            }),
        };
        r.apply_page(&[corr], "c6").await.unwrap();
        let corrected = r
            .month_row("2026-07", "acct", "proj", &id().stream_id)
            .await
            .unwrap();
        assert_eq!(corrected.corrections.len(), 1);
        assert_eq!(
            corrected.read_payload_bytes, 77,
            "the base number is untouched; the correction is explicit"
        );

        // Round-21 blocker 8: LATE DATA after finalization converts to
        // corrections automatically — no manual envelope needed.
        r.apply_page(&[snap(3, "2026-07", 300, 10_000, 40, false)], "c7")
            .await
            .unwrap();
        let after_snap = r
            .month_row("2026-07", "acct", "proj", &id().stream_id)
            .await
            .unwrap();
        let frozen = after_snap.frozen.clone().expect("frozen at finalization");
        assert_eq!(
            frozen.ingest_bytes, 250,
            "the frozen invoice base never moves"
        );
        assert_eq!(after_snap.corrections.len(), 2);
        let c = &after_snap.corrections[1];
        assert_eq!(c.ingest_payload_bytes_delta, 50);
        // Storage corrects ZERO here: finalization already extrapolated
        // this segment's byte-time past the late snapshot's absolute —
        // the late value is not additive news (and negative corrections
        // are deliberately not synthesized).
        assert_eq!(c.storage_byte_ms_delta, "0");
        // Replaying the same late snapshot corrects ZERO more times.
        r.apply_page(&[snap(3, "2026-07", 300, 10_000, 40, false)], "c8")
            .await
            .unwrap();
        let replay = r
            .month_row("2026-07", "acct", "proj", &id().stream_id)
            .await
            .unwrap();
        assert_eq!(replay.corrections.len(), 2, "late replay corrected twice");

        // ...and a late READ batch corrects too.
        let late_rb = ReadBatch {
            source: MeterSource {
                cell: "c".into(),
                instance: "i".into(),
                boot: "b2".into(),
            },
            seq: 0,
            from_ms: crate::billing::month_start_ms(2026, 7) + 10,
            to_ms: crate::billing::month_start_ms(2026, 7) + 20,
            rows: vec![ReadRow {
                identity: id(),
                read_payload_bytes: 9,
                read_records: 1,
                read_operations: 1,
                queue_operations: 0,
                append_requests: 0,
            }],
        };
        let late_env = UsageEnvelope {
            v: 1,
            event_id: "read/b2/0".into(),
            event_time_ms: 0,
            emitted_ms: 0,
            cell: "c".into(),
            payload: UsagePayload::ReadBatch(late_rb),
        };
        r.apply_page(&[late_env], "c9").await.unwrap();
        let after_read = r
            .month_row("2026-07", "acct", "proj", &id().stream_id)
            .await
            .unwrap();
        assert_eq!(after_read.read_payload_bytes, 77, "base reads untouched");
        assert_eq!(after_read.corrections.len(), 3);
        assert_eq!(after_read.corrections[2].read_payload_bytes_delta, 9);

        // Two-phase publication (blocker 7): pending rows survive until
        // published with Create, the retry is idempotent, and the
        // pending queue empties.
        let store = mem_store();
        let n = crate::billing::publish_artifacts(&r, &store, "t1")
            .await
            .unwrap();
        assert_eq!(n, 4, "1 monthly + 3 correction artifacts");
        assert!(r.pending_artifacts(64).await.unwrap().is_empty());
        assert!(
            r.pending_correction_artifacts(64).await.unwrap().is_empty(),
            "correction artifacts drained"
        );
        assert_eq!(
            crate::billing::publish_artifacts(&r, &store, "t1")
                .await
                .unwrap(),
            0,
            "re-publication finds nothing pending"
        );
        use object_store::ObjectStoreExt;
        let path = object_store::path::Path::from(format!(
            "t1/telemetry/usage-monthly/acct/proj/{}/2026-07.json",
            id().stream_id
        ));
        let got = store.get(&path).await.expect("artifact object exists");
        let body = got.bytes().await.unwrap();
        let art: MonthRow = serde_json::from_slice(&body).unwrap();
        assert!(art.frozen.is_some(), "the artifact is the frozen row");

        // Round-22 item 8: corrections carry full provenance, the
        // materialized sums cover every dimension, aggregates carry
        // the same sums, effective = frozen + corrections, and each
        // correction is its own immutable object.
        let row = r
            .month_row("2026-07", "acct", "proj", &id().stream_id)
            .await
            .unwrap();
        for c in &row.corrections {
            assert!(!c.correction_id.is_empty(), "correction_id set");
            assert_eq!(c.correction_version, 1);
            assert!(!c.source_event_id.is_empty(), "source event recorded");
        }
        assert_eq!(row.corr.read_payload_bytes_delta, 12 + 9);
        assert_eq!(row.corr.ingest_payload_bytes_delta, 50);
        assert_eq!(row.corr.count, 3);
        let eff = row.effective();
        let frozen_b = row.frozen.as_ref().unwrap().read_payload_bytes;
        assert_eq!(
            eff["readPayloadBytes"].as_u64().unwrap(),
            frozen_b + 12 + 9,
            "effective = frozen base + corrections"
        );
        let agg = r.project_row("2026-07", "acct", "proj").await.unwrap();
        assert_eq!(
            agg.corr.read_payload_bytes_delta,
            12 + 9,
            "project aggregate carries correction sums"
        );
        let cpath = object_store::path::Path::from(format!(
            "t1/telemetry/usage-monthly/acct/proj/{}/2026-07.corrections/corr~1.json",
            id().stream_id
        ));
        let cbody = store
            .get(&cpath)
            .await
            .expect("correction artifact")
            .bytes()
            .await
            .unwrap();
        let cart: crate::billing::UsageCorrection = serde_json::from_slice(&cbody).unwrap();
        assert_eq!(cart.read_payload_bytes_delta, 12);

        // Content verification (round-22 item 8a): an AlreadyExists
        // whose bytes DIFFER is a mismatch — never marked published.
        let clash =
            object_store::path::Path::from("t2/telemetry/usage-monthly/acct/proj/x/2026-01.json");
        store
            .put(&clash, object_store::PutPayload::from(b"tampered".to_vec()))
            .await
            .unwrap();
        // Manufacture a pending row aimed at that path.
        {
            let mut wb = WriteBatch::new();
            wb.put(
                b"artifact-pending/2026-01/acct/proj/x".to_vec(),
                serde_json::to_vec(&MonthRow::default()).unwrap(),
            );
            r.db.write(wb).await.unwrap();
        }
        let before = crate::billing::ARTIFACT_MISMATCHES.load(std::sync::atomic::Ordering::Relaxed);
        let n2 = crate::billing::publish_artifacts(&r, &store, "t2")
            .await
            .unwrap();
        assert_eq!(n2, 0, "mismatched artifact must NOT publish");
        assert_eq!(
            crate::billing::ARTIFACT_MISMATCHES.load(std::sync::atomic::Ordering::Relaxed),
            before + 1,
            "mismatch counted for the alert"
        );
        assert_eq!(
            r.pending_artifacts(64).await.unwrap().len(),
            1,
            "row stays pending for the operator"
        );
    }
}

// ---------------------------------------------------------------------
// Operational metrics rollup (§13.1): raw 15 s points + 1-minute
// aggregates in the SAME materialization, with a raw-tier retention
// sweep. Longer tiers (5 m/1 h) are the same mechanism applied again.
// ---------------------------------------------------------------------

const K_OPS_CURSOR: &[u8] = b"meta/ops-cursor";
pub const OPS_RAW_RETENTION_MS: i64 = 7 * 86_400_000;

fn k_ops_raw(instance: &str, ts_ms: i64) -> Vec<u8> {
    format!("ops/raw/{instance}/{ts_ms:020}").into_bytes()
}
fn k_ops_m1(instance: &str, minute_ms: i64) -> Vec<u8> {
    format!("ops/m1/{instance}/{minute_ms:020}").into_bytes()
}

/// One-minute aggregate: last cumulative counters + max gauges seen.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct OpsM1 {
    #[serde(default)]
    pub counters: std::collections::BTreeMap<String, u64>,
    #[serde(default)]
    pub gauges_max: std::collections::BTreeMap<String, u64>,
    #[serde(default)]
    pub samples: u32,
}

impl UsageRollup {
    pub async fn ops_cursor(&self) -> Option<String> {
        self.db
            .get(K_OPS_CURSOR)
            .await
            .ok()
            .flatten()
            .and_then(|v| String::from_utf8(v.to_vec()).ok())
    }

    /// Ingest one `_ops_metrics` page: raw point + m1 merge + cursor in
    /// one WriteBatch (same §9.3 discipline as usage pages).
    pub async fn apply_ops_page(
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
                None => get_json(&self.db, &key).await,
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

    pub async fn ops_m1(&self, instance: &str, minute_ms: i64) -> Option<OpsM1> {
        self.db
            .get(&k_ops_m1(instance, minute_ms)[..])
            .await
            .ok()
            .flatten()
            .and_then(|v| serde_json::from_slice(&v).ok())
    }

    /// Retention sweep for the raw tier (§13.1): delete points older
    /// than the cutoff, bounded per call.
    pub async fn sweep_ops_raw(&self, now_ms: i64, max_deletes: usize) -> anyhow::Result<usize> {
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
