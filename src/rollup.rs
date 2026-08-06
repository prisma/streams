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
    ReadBatch, SegmentSnapshot, UsageCorrection, UsageEnvelope, UsagePayload, month_start_ms,
    next_month, parse_month,
};
use serde::{Deserialize, Serialize};
use slatedb::{Db, WriteBatch};
use std::sync::Arc;

pub const ROLLUP_PATH: &str = "telemetry/usage-rollup/v1/p0";

// ---------------------------------------------------------------------
// Keyspace (§9.2)
// ---------------------------------------------------------------------

fn k_source(boot: &str) -> Vec<u8> {
    format!("source/{boot}").into_bytes()
}
fn k_segment(project: &str, stream_id: &str, seg: u32) -> Vec<u8> {
    format!("segment/{project}/{stream_id}/{seg}").into_bytes()
}
fn k_month(month: &str, project: &str, stream_id: &str) -> Vec<u8> {
    format!("month/{month}/{project}/{stream_id}").into_bytes()
}
fn k_month_prefix(month: &str) -> Vec<u8> {
    format!("month/{month}/").into_bytes()
}
fn k_name(month: &str, project: &str, name: &str) -> Vec<u8> {
    format!(
        "name/{month}/{project}/{}",
        crate::crypto::hex(&crate::crypto::stream_hash(name))
    )
    .into_bytes()
}
fn k_project(month: &str, project: &str) -> Vec<u8> {
    format!("project/{month}/{project}").into_bytes()
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
    #[serde(default)]
    pub corrections: Vec<UsageCorrection>,
    #[serde(default)]
    pub updated_ms: i64,
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
}

impl AggRow {
    fn add_storage(&mut self, delta: u128) {
        let cur: u128 = self.storage_byte_ms.parse().unwrap_or(0);
        self.storage_byte_ms = (cur + delta).to_string();
    }
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
        let db =
            crate::on_slatedb_rt(async move { Db::builder(path.as_str(), store).build().await })
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
        let now = crate::shard::now_ms();

        for env in envelopes {
            match &env.payload {
                UsagePayload::ReadBatch(rb) => {
                    self.apply_read_batch(rb, &mut sources, &mut months, &mut names, &mut projects)
                        .await;
                }
                UsagePayload::SegmentSnapshot(snap) => {
                    self.apply_snapshot(snap, &mut months, &mut segs, &mut names, &mut projects)
                        .await;
                }
                UsagePayload::StreamLifecycle(_) => {
                    // Informational in v1: recreation isolation is
                    // already structural (stream_id keying).
                }
                UsagePayload::UsageCorrection(c) => {
                    let key = k_month(&c.month, &c.identity.project_id, &c.identity.stream_id);
                    let mut row: MonthRow = match months.get(&key) {
                        Some(r) => r.clone(),
                        None => get_json(&self.db, &key).await,
                    };
                    row.corrections.push(c.clone());
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
        wb.put(K_CURSOR, next_cursor.as_bytes());
        self.db.write(wb).await?;
        Ok(())
    }

    async fn apply_read_batch(
        &self,
        rb: &ReadBatch,
        sources: &mut std::collections::HashMap<String, u64>,
        months: &mut std::collections::HashMap<Vec<u8>, MonthRow>,
        names: &mut std::collections::HashMap<Vec<u8>, AggRow>,
        projects: &mut std::collections::HashMap<Vec<u8>, AggRow>,
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
        let (y, m) = crate::billing::utc_year_month(rb.to_ms);
        let month = crate::billing::month_str(y, m);
        for row in &rb.rows {
            let mkey = k_month(&month, &row.identity.project_id, &row.identity.stream_id);
            let mut mr: MonthRow = match months.get(&mkey) {
                Some(r) => r.clone(),
                None => get_json(&self.db, &mkey).await,
            };
            mr.account_id = row.identity.account_id.clone();
            mr.stream_name = row.identity.stream_name.clone();
            mr.read_payload_bytes += row.read_payload_bytes;
            mr.read_records += row.read_records;
            mr.read_operations += row.read_operations;
            mr.queue_operations += row.queue_operations;
            mr.append_requests += row.append_requests;
            months.insert(mkey, mr);
            for (key, agg) in [
                (
                    k_name(&month, &row.identity.project_id, &row.identity.stream_name),
                    true,
                ),
                (k_project(&month, &row.identity.project_id), false),
            ] {
                let mut a: AggRow = match names.get(&key).or_else(|| projects.get(&key)) {
                    Some(r) => r.clone(),
                    None => get_json(&self.db, &key).await,
                };
                a.read_payload_bytes += row.read_payload_bytes;
                a.read_records += row.read_records;
                a.read_operations += row.read_operations;
                a.queue_operations += row.queue_operations;
                a.append_requests += row.append_requests;
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
    ) {
        let id = &snap.identity;
        let skey = k_segment(&id.project_id, &id.stream_id, snap.segment_id);
        let mut st: SegmentState = match segs.get(&skey) {
            Some(r) => r.clone(),
            None => get_json(&self.db, &skey).await,
        };
        let mkey = k_month(&snap.month, &id.project_id, &id.stream_id);
        let mut mr: MonthRow = match months.get(&mkey) {
            Some(r) => r.clone(),
            None => get_json(&self.db, &mkey).await,
        };
        let sm = mr.segments.entry(snap.segment_id).or_default();
        // Version fence PER (segment, month) target: the live row and a
        // month-final can carry the same version family; a strictly
        // older snapshot applies as nothing.
        if snap.usage_version <= sm.usage_version && !snap.month_final {
            return;
        }
        if snap.month_final && sm.final_seen {
            return; // replayed final
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
        if snap.usage_version > st.usage_version {
            st.usage_version = snap.usage_version;
            st.owned_frame_bytes_current = snap.owned_frame_bytes_current;
            st.storage_accounted_through_ms = snap.storage_accounted_through_ms;
            st.stream_name = id.stream_name.clone();
            st.account_id = id.account_id.clone();
            segs.insert(skey, st);
        }
        for (key, is_name) in [
            (k_name(&snap.month, &id.project_id, &id.stream_name), true),
            (k_project(&snap.month, &id.project_id), false),
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

    // ---- point reads (the customer API) ------------------------------

    pub async fn month_row(&self, month: &str, project: &str, stream_id: &str) -> Option<MonthRow> {
        self.db
            .get(&k_month(month, project, stream_id)[..])
            .await
            .ok()
            .flatten()
            .and_then(|v| serde_json::from_slice(&v).ok())
    }

    pub async fn name_row(&self, month: &str, project: &str, name: &str) -> Option<AggRow> {
        self.db
            .get(&k_name(month, project, name)[..])
            .await
            .ok()
            .flatten()
            .and_then(|v| serde_json::from_slice(&v).ok())
    }

    pub async fn project_row(&self, month: &str, project: &str) -> Option<AggRow> {
        self.db
            .get(&k_project(month, project)[..])
            .await
            .ok()
            .flatten()
            .and_then(|v| serde_json::from_slice(&v).ok())
    }

    // ---- month close (§9.4/§9.5/§9.6) --------------------------------

    /// Close (year, month): advance every non-final segment's storage
    /// integral to the exact UTC boundary (idle-gauge extrapolation —
    /// no stream write required), stamp `finalized_at`, and hand each
    /// closed row to `artifact` for the immutable monthly object. Rows
    /// already finalized are untouched. Returns closed stream count.
    pub async fn close_month(
        &self,
        year: i32,
        month: u32,
        grace_ms: i64,
        mut artifact: impl FnMut(&str, &str, &MonthRow, &str),
    ) -> anyhow::Result<usize> {
        let mstr = crate::billing::month_str(year, month);
        let (ny, nm) = next_month(year, month);
        let boundary = month_start_ms(ny, nm);
        let now = crate::shard::now_ms();
        if now < boundary + grace_ms {
            return Ok(0); // not yet closeable
        }
        let pfx = k_month_prefix(&mstr);
        let mut closed = 0usize;
        let mut wb = WriteBatch::new();
        let mut iter = self.db.scan_prefix(&pfx[..], ..).await?;
        let mut rows: Vec<(Vec<u8>, MonthRow)> = Vec::new();
        while let Some(kv) = iter.next().await? {
            if let Ok(row) = serde_json::from_slice::<MonthRow>(&kv.value) {
                rows.push((kv.key.to_vec(), row));
            }
        }
        for (key, mut row) in rows {
            if row.finalized_at_ms.is_some() {
                continue;
            }
            for sm in row.segments.values_mut() {
                if !sm.final_seen && sm.accounted_through_ms < boundary {
                    let cur: u128 = sm.storage_byte_ms.parse().unwrap_or(0);
                    let add = (boundary - sm.accounted_through_ms) as u128 * sm.gauge_bytes as u128;
                    sm.storage_byte_ms = (cur + add).to_string();
                    sm.accounted_through_ms = boundary;
                    sm.final_seen = true;
                }
            }
            row.finalized_at_ms = Some(now);
            // key = month/<M>/<project>/<stream-id>
            let parts: Vec<&str> = std::str::from_utf8(&key)
                .unwrap_or("")
                .splitn(4, '/')
                .collect();
            if parts.len() == 4 {
                artifact(parts[2], parts[3], &row, &mstr);
            }
            wb.put(key, serde_json::to_vec(&row)?);
            closed += 1;
        }
        if closed > 0 {
            self.db.write(wb).await?;
        }
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
            .month_row("2026-07", "proj", &id().stream_id)
            .await
            .unwrap();
        assert_eq!(row.ingest_bytes(), 250, "absolute, not summed");
        assert_eq!(row.storage_byte_ms(), 9000);
        let proj = r.project_row("2026-07", "proj").await.unwrap();
        assert_eq!(proj.ingest_bytes, 250, "aggregate absorbed deltas once");
        assert_eq!(proj.storage_byte_ms, "9000");
        let name = r.name_row("2026-07", "proj", "orders").await.unwrap();
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
            .month_row("2026-07", "proj", &id().stream_id)
            .await
            .unwrap();
        assert_eq!(row.read_payload_bytes, 77, "source-seq dedupe");
        assert_eq!(row.append_requests, 4);

        // Close July (grace 0): the idle gauge extrapolates from
        // accounted_through to the boundary, the row finalizes, one
        // artifact per stream.
        let mut artifacts = Vec::new();
        let n = r
            .close_month(2026, 7, 0, |proj, sid, row, m| {
                artifacts.push((
                    proj.to_string(),
                    sid.to_string(),
                    m.to_string(),
                    row.storage_byte_ms(),
                ))
            })
            .await
            .unwrap();
        assert_eq!(n, 1);
        assert_eq!(artifacts.len(), 1);
        let boundary = month_start_ms(2026, 8);
        let through = crate::billing::month_start_ms(2026, 7) + 1_000_000;
        let expect = 9000u128 + (boundary - through) as u128 * 40;
        assert_eq!(artifacts[0].3, expect, "idle extrapolation to the boundary");
        let closed = r
            .month_row("2026-07", "proj", &id().stream_id)
            .await
            .unwrap();
        assert!(closed.finalized_at_ms.is_some());
        // Second close: nothing left to do.
        assert_eq!(r.close_month(2026, 7, 0, |_, _, _, _| {}).await.unwrap(), 0);

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
                ingest_payload_bytes_delta: 0,
                read_payload_bytes_delta: 12,
                storage_byte_ms_delta: "0".into(),
            }),
        };
        r.apply_page(&[corr], "c6").await.unwrap();
        let corrected = r
            .month_row("2026-07", "proj", &id().stream_id)
            .await
            .unwrap();
        assert_eq!(corrected.corrections.len(), 1);
        assert_eq!(
            corrected.read_payload_bytes, 77,
            "the base number is untouched; the correction is explicit"
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
                wb.delete(kv.key.to_vec());
                n += 1;
            }
        }
        if n > 0 {
            self.db.write(wb).await?;
        }
        Ok(n)
    }
}
