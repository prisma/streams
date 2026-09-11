//! The pending state and durable commit of one usage-ledger page.
//!
//! Every read observes this page's prior updates. Only consuming `commit`
//! publishes rows, correction artifacts, source floors and the cursor together.
//! A failed read or validation drops the entire pending transaction.

use super::{
    AggRow, K_CURSOR, MonthRow, SegmentState, decode_json, get_json, k_month, k_name, k_project,
    k_segment, k_source, month_spans, read_bytes,
};
use crate::billing::{
    ReadBatch, ReadRow, SegmentSnapshot, UsageCorrection, UsageEnvelope, UsagePayload,
};
use slatedb::{Db, WriteBatch};
use std::collections::HashMap;

/// The only entry point: an application failure cannot reach the consuming commit.
pub(super) async fn apply(
    db: &Db,
    envelopes: &[UsageEnvelope],
    next_cursor: &str,
) -> anyhow::Result<()> {
    let mut page = Page::new(db);
    for envelope in envelopes {
        page.apply(envelope).await?;
    }
    page.commit(next_cursor).await
}

struct Page<'a> {
    db: &'a Db,
    // mt-lint: allow(name-keyed-map): source boot identifiers fence ledger batches across projects; these are not stream names
    sources: HashMap<String, u64>,
    months: HashMap<Vec<u8>, MonthRow>,
    segs: HashMap<Vec<u8>, SegmentState>,
    aggregates: HashMap<Vec<u8>, AggRow>,
    writes: WriteBatch,
    now: i64,
}

impl<'a> Page<'a> {
    fn new(db: &'a Db) -> Self {
        Self {
            db,
            sources: HashMap::new(),
            months: HashMap::new(),
            segs: HashMap::new(),
            aggregates: HashMap::new(),
            writes: WriteBatch::new(),
            now: crate::shard::now_ms(),
        }
    }

    async fn apply(&mut self, env: &UsageEnvelope) -> anyhow::Result<()> {
        // Validate the same financial decimal encodings at input and storage boundaries.
        decode_json::<UsageEnvelope>(&serde_json::to_vec(env)?)?;
        match &env.payload {
            UsagePayload::ReadBatch(rb) => self.apply_read_batch(rb).await?,
            UsagePayload::SegmentSnapshot(snap) => self.apply_snapshot(snap).await?,
            UsagePayload::UsageCorrection(c) => self.apply_correction(env, c).await?,
            UsagePayload::StreamLifecycle(_) => {
                // Informational: stream_id keying already isolates reincarnations.
            }
        }
        Ok(())
    }

    async fn apply_correction(
        &mut self,
        env: &UsageEnvelope,
        c: &UsageCorrection,
    ) -> anyhow::Result<()> {
        let key = k_month(
            &c.month,
            &c.identity.account_id,
            &c.identity.project_id,
            &c.identity.stream_id,
        );
        let mut row: MonthRow = match self.months.get(&key) {
            Some(row) => row.clone(),
            None => get_json(self.db, &key).await?,
        };
        // Envelope provenance fills only missing fields; explicit corrections retain theirs.
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
        self.push_correction(&mut row, c).await?;
        self.months.insert(key, row);
        Ok(())
    }

    async fn commit(mut self, next_cursor: &str) -> anyhow::Result<()> {
        for (boot, seq) in self.sources {
            self.writes.put(k_source(&boot), seq.to_le_bytes());
        }
        for (key, mut row) in self.months {
            row.updated_ms = self.now;
            self.writes.put(key, serde_json::to_vec(&row)?);
        }
        for (key, row) in self.segs {
            self.writes.put(key, serde_json::to_vec(&row)?);
        }
        for (key, row) in self.aggregates {
            self.writes.put(key, serde_json::to_vec(&row)?);
        }
        self.writes.put(K_CURSOR, next_cursor.as_bytes());
        self.db.write(self.writes).await?;
        Ok(())
    }

    /// Append ONE correction to a month row: dedupe by correction_id
    /// (a replayed correction applies exactly once), materialize its
    /// sums on the row AND on both aggregates (round-22 item 8f), and
    /// stage its immutable artifact when the month is finalized (8e).
    async fn push_correction(
        &mut self,
        mr: &mut MonthRow,
        c: UsageCorrection,
    ) -> anyhow::Result<()> {
        if !c.correction_id.is_empty()
            && mr
                .corrections
                .iter()
                .any(|x| x.correction_id == c.correction_id)
        {
            return Ok(());
        }
        mr.corr.absorb(&c);
        for key in [
            k_name(
                &c.month,
                &c.identity.account_id,
                &c.identity.project_id,
                &c.identity.stream_name,
            ),
            k_project(&c.month, &c.identity.account_id, &c.identity.project_id),
        ] {
            let mut a: AggRow = match self.aggregates.get(&key) {
                Some(r) => r.clone(),
                None => get_json(self.db, &key).await?,
            };
            a.corr.absorb(&c);
            self.aggregates.insert(key, a);
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
            self.writes.put(pk.into_bytes(), serde_json::to_vec(&c)?);
        }
        mr.corrections.push(c);
        Ok(())
    }

    async fn apply_read_batch(&mut self, rb: &ReadBatch) -> anyhow::Result<()> {
        let boot = rb.source.boot.clone();
        let floor = match self.sources.get(&boot) {
            Some(v) => *v,
            None => read_bytes(self.db, &k_source(&boot))
                .await?
                .map(|v| crate::shard::decode_cursor(&v))
                .transpose()?
                .unwrap_or(u64::MAX),
        };
        if floor != u64::MAX && rb.seq <= floor {
            return Ok(()); // duplicate delivery of an applied batch
        }
        self.sources.insert(boot, rb.seq);
        // Round-22 item 5: EXACT integer allocation across every UTC
        // month boundary the batch intersects. Each dimension of each
        // row is split with integer proportions and the remainder goes
        // to the final span, so sum(allocations) == original for every
        // metered dimension — one byte can never bill twice, and a
        // batch spanning many months (a long outage) covers the
        // intermediate months too.
        let spans = month_spans(rb.from_ms, rb.to_ms);
        if spans.len() == 1 {
            for row in &rb.rows {
                self.apply_read_row(rb, row, &spans[0].0).await?;
            }
            return Ok(());
        }
        let durations = spans
            .iter()
            .map(|(_, duration)| u64::try_from(*duration))
            .collect::<Result<Vec<_>, _>>()?;
        // Retain month-first application order: each span sees all rows from
        // earlier spans before applying its own rows.
        let mut by_month = vec![Vec::with_capacity(rb.rows.len()); spans.len()];
        for row in &rb.rows {
            let dimensions = [
                row.read_payload_bytes,
                row.read_records,
                row.read_operations,
                row.queue_operations,
                row.append_requests,
            ];
            let allocations =
                super::allocation::allocate(dimensions, &durations).map_err(anyhow::Error::msg)?;
            for (rows, values) in by_month.iter_mut().zip(allocations) {
                rows.push(values);
            }
        }
        for ((month, _), values) in spans.iter().zip(by_month) {
            for (row, values) in rb.rows.iter().zip(values) {
                // Keep only meter arrays across months, not cloned identities.
                let scaled = ReadRow {
                    identity: row.identity.clone(),
                    read_payload_bytes: values[0],
                    read_records: values[1],
                    read_operations: values[2],
                    queue_operations: values[3],
                    append_requests: values[4],
                };
                self.apply_read_row(rb, &scaled, month).await?;
            }
        }
        Ok(())
    }

    async fn apply_read_row(
        &mut self,
        rb: &ReadBatch,
        row: &ReadRow,
        month: &str,
    ) -> anyhow::Result<()> {
        let mkey = k_month(
            month,
            &row.identity.account_id,
            &row.identity.project_id,
            &row.identity.stream_id,
        );
        let mut mr: MonthRow = match self.months.get(&mkey) {
            Some(r) => r.clone(),
            None => get_json(self.db, &mkey).await?,
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
                read_payload_bytes_delta: row.read_payload_bytes as i64,
                read_records_delta: row.read_records as i64,
                read_operations_delta: row.read_operations as i64,
                queue_operations_delta: row.queue_operations as i64,
                append_requests_delta: row.append_requests as i64,
                storage_byte_ms_delta: "0".into(),
            };
            self.push_correction(&mut mr, c).await?;
            self.months.insert(mkey, mr);
            return Ok(());
        }
        mr.account_id = row.identity.account_id.clone();
        mr.stream_name = row.identity.stream_name.clone();
        mr.read_payload_bytes += row.read_payload_bytes;
        mr.read_records += row.read_records;
        mr.read_operations += row.read_operations;
        mr.queue_operations += row.queue_operations;
        mr.append_requests += row.append_requests;
        self.months.insert(mkey, mr);
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
            let mut a: AggRow = match self.aggregates.get(&key) {
                Some(r) => r.clone(),
                None => get_json(self.db, &key).await?,
            };
            a.read_payload_bytes += row.read_payload_bytes;
            a.read_records += row.read_records;
            a.read_operations += row.read_operations;
            a.queue_operations += row.queue_operations;
            a.append_requests += row.append_requests;
            if agg && !a.incarnations.contains(&row.identity.stream_id) {
                a.incarnations.push(row.identity.stream_id.clone());
            }
            self.aggregates.insert(key, a);
        }
        Ok(())
    }

    async fn apply_snapshot(&mut self, snap: &SegmentSnapshot) -> anyhow::Result<()> {
        let id = &snap.identity;
        let skey = k_segment(
            &id.account_id,
            &id.project_id,
            &id.stream_id,
            snap.segment_id,
        );
        let mut st: SegmentState = match self.segs.get(&skey) {
            Some(r) => r.clone(),
            None => get_json(self.db, &skey).await?,
        };
        let mkey = k_month(&snap.month, &id.account_id, &id.project_id, &id.stream_id);
        let mut mr: MonthRow = match self.months.get(&mkey) {
            Some(r) => r.clone(),
            None => get_json(self.db, &mkey).await?,
        };
        let finalized = mr.finalized_at_ms.is_some();
        let sm = mr.segments.entry(snap.segment_id).or_default();
        // Version fence PER (segment, month) target: the live row and a
        // month-final can carry the same version family; a strictly
        // older snapshot applies as nothing.
        if snap.usage_version <= sm.usage_version && !snap.month_final {
            return Ok(());
        }
        if snap.month_final && sm.final_seen && !finalized {
            return Ok(()); // replayed final
        }
        if finalized {
            if snap.month_final && sm.final_seen && snap.usage_version <= sm.usage_version {
                return Ok(()); // replayed final against a closed month
            }
            if let Some(correction) = apply_late_snapshot(sm, snap) {
                self.push_correction(&mut mr, correction).await?;
            }
            self.months.insert(mkey, mr);
            return Ok(());
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
        self.months.insert(mkey, mr);
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
            self.segs.insert(skey, st);
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
            let mut a: AggRow = match self.aggregates.get(&key) {
                Some(r) => r.clone(),
                None => get_json(self.db, &key).await?,
            };
            a.ingest_bytes += d_bytes;
            a.ingest_records += d_recs;
            a.add_storage(d_ms);
            if is_name && !a.incarnations.contains(&id.stream_id) {
                a.incarnations.push(id.stream_id.clone());
            }
            self.aggregates.insert(key, a);
        }
        Ok(())
    }
}

/// A closed invoice keeps its frozen base; only dedupe floors and an explicit
/// correction advance. Returning no correction still preserves the new floors.
fn apply_late_snapshot(
    sm: &mut super::SegMonth,
    snap: &SegmentSnapshot,
) -> Option<UsageCorrection> {
    let id = &snap.identity;
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
            correction_id: format!("corr/snap/{}/{}", snap.deterministic_event_id(), snap.month),
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
        Some(c)
    } else {
        None
    }
}

#[cfg(test)]
mod tests;
