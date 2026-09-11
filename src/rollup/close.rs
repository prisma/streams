//! Month close has two resumable phases. Each bounded input page is staged
//! completely before its rows and exclusive resume cursor commit together.
//! Carry pages share month and aggregate caches so multiple segments of one
//! stream cannot overwrite each other's accrual.

use super::{
    AggRow, FrozenTotals, MonthRow, SegmentState, UsageRollup, close_scan_range, decode_json,
    get_json, k_month, k_month_prefix, k_name, k_project, month_start_ms, next_month, parse_month,
};
use crate::billing::BillingIdentity;
use serde::de::DeserializeOwned;
use slatedb::WriteBatch;
use std::collections::{HashMap, hash_map::Entry};

struct MonthClose<'a> {
    rollup: &'a UsageRollup,
    month: String,
    start: i64,
    boundary: i64,
    now: i64,
}

impl UsageRollup {
    /// Round-22 item 8: close every overdue month IN ORDER from the
    /// persisted oldest-unclosed marker. A rollup that was down across
    /// one or more boundaries catches up oldest-first; the marker
    /// advances only after that month's close completed, so a crash
    /// resumes at the same month. Returns (month, streams closed).
    pub(crate) async fn close_months_due(
        &self,
        grace_ms: i64,
    ) -> anyhow::Result<Vec<(String, usize)>> {
        const MARKER: &[u8] = b"meta/oldest-unclosed-month";
        fn prev_month(y: i32, m: u32) -> (i32, u32) {
            if m == 1 { (y - 1, 12) } else { (y, m - 1) }
        }
        let now = crate::billing::billing_now_ms();
        let (cy, cm) = crate::billing::utc_year_month(now);
        let (mut y, mut m) = match self.db.get(MARKER).await? {
            Some(v) => {
                let s = std::str::from_utf8(&v)?;
                parse_month(s)
                    .ok_or_else(|| anyhow::anyhow!("invalid oldest-unclosed-month cursor"))?
            }
            None => {
                // First run: start at the OLDEST month with data — a
                // fresh marker must not skip a backlog that predates
                // it. `month/` keys sort by month string, so the first
                // key names the oldest.
                let mut it = self.db.scan_prefix(&b"month/"[..], ..).await?;
                match it.next().await? {
                    Some(kv) => std::str::from_utf8(&kv.key)?
                        .split('/')
                        .nth(1)
                        .and_then(parse_month)
                        .ok_or_else(|| anyhow::anyhow!("invalid month index key"))?,
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

    /// Close both bounded phases in order: carry retained storage gauges,
    /// then freeze invoice rows and stage their publication outbox. Every
    /// page commits its resume cursor with its rows, so a restart resumes
    /// after the last durable page. Month cursors clear only after both phases.
    pub(crate) async fn close_month(
        &self,
        year: i32,
        month: u32,
        grace_ms: i64,
    ) -> anyhow::Result<usize> {
        let (next_year, next_month) = next_month(year, month);
        let close = MonthClose {
            rollup: self,
            month: crate::billing::month_str(year, month),
            start: month_start_ms(year, month),
            boundary: month_start_ms(next_year, next_month),
            now: crate::billing::billing_now_ms(),
        };
        if close.now < close.boundary + grace_ms {
            return Ok(0);
        }
        let segment_cursor = format!("meta/close-seg-cursor/{}", close.month).into_bytes();
        let finalize_cursor = format!("meta/close-fin-cursor/{}", close.month).into_bytes();
        close.carry(&segment_cursor).await?;
        let closed = close.finalize(&finalize_cursor).await?;
        let mut batch = WriteBatch::new();
        batch.delete(segment_cursor);
        batch.delete(finalize_cursor);
        self.db.write(batch).await?;
        Ok(closed)
    }
}

impl MonthClose<'_> {
    async fn read_page<T: DeserializeOwned>(
        &self,
        prefix: &[u8],
        after: Option<&[u8]>,
    ) -> anyhow::Result<Vec<(Vec<u8>, T)>> {
        const CLOSE_CHUNK: usize = 1000;
        const CLOSE_CHUNK_BYTES: usize = 1_000_000;
        let mut rows = Vec::new();
        let mut bytes = 0usize;
        let mut iter = self
            .rollup
            .db
            .scan_prefix(prefix, close_scan_range(prefix, after)?)
            .await?;
        while let Some(row) = iter.next().await? {
            self.rollup
                .close_rows_visited
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            bytes += row.value.len();
            rows.push((row.key.to_vec(), decode_json(&row.value)?));
            // A large row still makes progress; an empty page means the
            // phase is exhausted, never that decoding or a size check failed.
            if rows.len() >= CLOSE_CHUNK || bytes >= CLOSE_CHUNK_BYTES {
                break;
            }
        }
        Ok(rows)
    }

    async fn carry(&self, cursor: &[u8]) -> anyhow::Result<()> {
        let mut after = self.rollup.db.get(cursor).await?.map(|v| v.to_vec());
        loop {
            let rows = self
                .read_page::<SegmentState>(b"segment/", after.as_deref())
                .await?;
            let Some((last_key, _)) = rows.last() else {
                break;
            };
            let last_key = last_key.clone();
            let mut page = CarryPage {
                close: self,
                batch: WriteBatch::new(),
                months: HashMap::new(),
                aggregates: HashMap::new(),
            };
            for (key, state) in rows {
                page.segment(key, state).await?;
            }
            let mut batch = page.into_batch()?;
            batch.put(cursor, last_key.clone());
            self.rollup.db.write(batch).await?;
            #[cfg(test)]
            if super::read_faults().lock().unwrap().remove(&(
                std::sync::Arc::as_ptr(&self.rollup.db) as usize,
                b"stop-after-close-chunk".to_vec(),
            )) {
                anyhow::bail!("test interruption after committed close chunk");
            }
            after = Some(last_key);
        }
        Ok(())
    }

    async fn finalize(&self, cursor: &[u8]) -> anyhow::Result<usize> {
        let mut after = self.rollup.db.get(cursor).await?.map(|v| v.to_vec());
        let prefix = k_month_prefix(&self.month);
        let mut closed = 0;
        loop {
            let rows = self
                .read_page::<MonthRow>(&prefix, after.as_deref())
                .await?;
            let Some((last_key, _)) = rows.last() else {
                break;
            };
            let last_key = last_key.clone();
            let mut batch = WriteBatch::new();
            for (key, row) in rows {
                closed += self.finalize_row(&mut batch, &key, row)?;
            }
            batch.put(cursor, last_key.clone());
            self.rollup.db.write(batch).await?;
            after = Some(last_key);
        }
        Ok(closed)
    }

    fn finalize_row(
        &self,
        batch: &mut WriteBatch,
        key: &[u8],
        mut row: MonthRow,
    ) -> anyhow::Result<usize> {
        let parts: Vec<&str> = std::str::from_utf8(key)?.split('/').collect();
        anyhow::ensure!(
            parts.len() == 5
                && parts[0] == "month"
                && parts[1] == self.month
                && parts.iter().all(|p| !p.is_empty()),
            "invalid month accounting key"
        );
        anyhow::ensure!(
            row.account_id == parts[2],
            "month accounting identity mismatch"
        );
        if row.finalized_at_ms.is_some() {
            return Ok(0);
        }
        for segment in row.segments.values_mut() {
            if segment.final_seen || segment.accounted_through_ms >= self.boundary {
                continue;
            }
            let current: u128 = segment.storage_byte_ms.parse().unwrap_or(0);
            let from = segment.accounted_through_ms.max(self.start);
            let add = super::storage::byte_ms(segment.gauge_bytes, from, self.boundary);
            segment.storage_byte_ms = (current + add).to_string();
            segment.accounted_through_ms = self.boundary;
            segment.final_seen = true;
        }
        row.finalized_at_ms = Some(self.now);
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
        // The immutable invoice and its publication outbox use identical bytes
        // in this page's durable batch. Failed publication retries the outbox.
        let pending = format!(
            "artifact-pending/{}/{}/{}/{}",
            self.month, parts[2], parts[3], parts[4]
        );
        let value = serde_json::to_vec(&row)?;
        batch.put(pending.into_bytes(), value.clone());
        batch.put(key, value);
        Ok(1)
    }
}

struct CarryPage<'a, 'b> {
    close: &'a MonthClose<'b>,
    batch: WriteBatch,
    months: HashMap<Vec<u8>, MonthRow>,
    aggregates: HashMap<Vec<u8>, AggRow>,
}

impl CarryPage<'_, '_> {
    async fn segment(&mut self, key: Vec<u8>, mut state: SegmentState) -> anyhow::Result<()> {
        let parts: Vec<&str> = std::str::from_utf8(&key)?.split('/').collect();
        anyhow::ensure!(
            parts.len() == 5 && parts[0] == "segment" && parts.iter().all(|p| !p.is_empty()),
            "invalid segment accounting key"
        );
        let segment: u32 = parts[4].parse()?;
        anyhow::ensure!(
            state.account_id == parts[1],
            "segment accounting identity mismatch"
        );
        if state.storage_accounted_through_ms >= self.close.boundary {
            return Ok(());
        }
        let identity = BillingIdentity {
            account_id: state.account_id.clone(),
            project_id: parts[2].to_owned(),
            stream_id: parts[3].to_owned(),
            stream_name: state.stream_name.clone(),
        };
        let month_key = k_month(
            &self.close.month,
            &identity.account_id,
            &identity.project_id,
            &identity.stream_id,
        );
        let mut row = match self.months.get(&month_key) {
            Some(row) => row.clone(),
            None => get_json::<MonthRow>(&self.close.rollup.db, &month_key).await?,
        };
        let month_segment = row.segments.entry(segment).or_default();
        if !month_segment.final_seen {
            let from = state.storage_accounted_through_ms.max(self.close.start);
            if from < self.close.boundary && state.owned_frame_bytes_current > 0 {
                let add = super::storage::byte_ms(
                    state.owned_frame_bytes_current,
                    from,
                    self.close.boundary,
                );
                let current: u128 = month_segment.storage_byte_ms.parse().unwrap_or(0);
                month_segment.storage_byte_ms = (current + add).to_string();
                self.add_storage(&identity, add).await?;
            }
            month_segment.gauge_bytes = state.owned_frame_bytes_current;
            month_segment.accounted_through_ms = self.close.boundary;
            month_segment.final_seen = true;
            row.account_id = state.account_id.clone();
            if row.stream_name.is_empty() {
                row.stream_name = state.stream_name.clone();
            }
            row.updated_ms = self.close.now;
            self.months.insert(month_key, row);
        }
        state.storage_accounted_through_ms = self.close.boundary;
        self.batch.put(key, serde_json::to_vec(&state)?);
        Ok(())
    }

    async fn aggregate(&mut self, key: Vec<u8>) -> anyhow::Result<&mut AggRow> {
        Ok(match self.aggregates.entry(key) {
            Entry::Occupied(row) => row.into_mut(),
            Entry::Vacant(entry) => {
                let row = get_json(&self.close.rollup.db, entry.key()).await?;
                entry.insert(row)
            }
        })
    }

    async fn add_storage(
        &mut self,
        identity: &BillingIdentity,
        amount: u128,
    ) -> anyhow::Result<()> {
        let name_key = k_name(
            &self.close.month,
            &identity.account_id,
            &identity.project_id,
            &identity.stream_name,
        );
        let name = self.aggregate(name_key).await?;
        name.add_storage(amount);
        if !name.incarnations.contains(&identity.stream_id) {
            name.incarnations.push(identity.stream_id.clone());
        }
        let project_key = k_project(
            &self.close.month,
            &identity.account_id,
            &identity.project_id,
        );
        self.aggregate(project_key).await?.add_storage(amount);
        Ok(())
    }

    fn into_batch(mut self) -> anyhow::Result<WriteBatch> {
        for (key, row) in self.months {
            self.batch.put(key, serde_json::to_vec(&row)?);
        }
        for (key, row) in self.aggregates {
            self.batch.put(key, serde_json::to_vec(&row)?);
        }
        Ok(self.batch)
    }
}

#[cfg(test)]
mod tests;
