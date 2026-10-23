//! A queue's rows within one commit group: the state row, and every row the
//! group staged, readable by the group's later operations before the batch
//! is written (read-your-writes inside the group). Rows not staged are read
//! from the shard log, which already holds every earlier group's write.
use std::collections::BTreeMap;

use crate::native_queue::keys;
use crate::native_queue::rows::StateRow;

pub(in crate::shard::transaction) struct NativeStage {
    pub state: StateRow,
    /// Whether the queue exists (its state row was found or created).
    pub exists: bool,
    pub state_dirty: bool,
    /// Staged rows: `Some` put, `None` delete.
    pub rows: BTreeMap<Vec<u8>, Option<Vec<u8>>>,
}

impl NativeStage {
    pub(in crate::shard::transaction) fn absent() -> Self {
        Self {
            state: StateRow::default(),
            exists: false,
            state_dirty: false,
            rows: BTreeMap::new(),
        }
    }
}

impl super::super::CommitTransaction<'_> {
    pub(super) async fn nq_load(&self, hash: &[u8; 16]) -> Result<NativeStage, String> {
        match self.engine.db.get(keys::state(hash)).await {
            Ok(Some(raw)) => Ok(NativeStage {
                state: StateRow::decode(&raw)?,
                exists: true,
                state_dirty: false,
                rows: BTreeMap::new(),
            }),
            Ok(None) => Ok(NativeStage::absent()),
            Err(e) => Err(e.to_string()),
        }
    }

    pub(super) fn nq_put(&mut self, st: &mut NativeStage, key: Vec<u8>, value: Vec<u8>) {
        self.batch.put(&key, &value);
        st.rows.insert(key, Some(value));
        self.extra_writes = true;
    }

    pub(super) fn nq_delete(&mut self, st: &mut NativeStage, key: Vec<u8>) {
        self.batch.delete(&key);
        st.rows.insert(key, None);
        self.extra_writes = true;
    }

    pub(super) async fn nq_get(
        &self,
        st: &NativeStage,
        key: &[u8],
    ) -> Result<Option<Vec<u8>>, String> {
        if let Some(staged) = st.rows.get(key) {
            return Ok(staged.clone());
        }
        self.engine
            .db
            .get(key)
            .await
            .map(|v| v.map(|b| b.to_vec()))
            .map_err(|e| e.to_string())
    }

    /// Up to `limit` live rows with keys in `[from, to)`, in key order,
    /// merging the group's staged rows over the shard log's.
    pub(super) async fn nq_scan(
        &self,
        st: &NativeStage,
        range: (Vec<u8>, Vec<u8>),
        limit: usize,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, String> {
        let (from, to) = range;
        let mut merged: BTreeMap<Vec<u8>, Option<Vec<u8>>> = BTreeMap::new();
        // Stored rows: read past the limit by the staged deletes in range so
        // deleted rows cannot starve the page.
        let staged_in_range = st.rows.range(from.clone()..to.clone()).count();
        let want = limit.saturating_add(staged_in_range);
        let mut it = self
            .engine
            .db
            .scan_with_options(from.clone()..to.clone(), &queue_scan_options())
            .await
            .map_err(|e| e.to_string())?;
        while merged.len() < want {
            match it.next().await.map_err(|e| e.to_string())? {
                Some(kv) => {
                    merged.insert(kv.key.to_vec(), Some(kv.value.to_vec()));
                }
                None => break,
            }
        }
        for (k, v) in st.rows.range(from..to) {
            merged.insert(k.clone(), v.clone());
        }
        Ok(merged
            .into_iter()
            .filter_map(|(k, v)| v.map(|v| (k, v)))
            .take(limit)
            .collect())
    }

    /// Writes the state row if anything changed it.
    pub(super) fn nq_flush_state(&mut self, hash: &[u8; 16], st: &mut NativeStage) {
        if st.state_dirty {
            let raw = st.state.encode();
            self.batch.put(keys::state(hash), raw);
            self.extra_writes = true;
            st.state_dirty = false;
        }
    }
}

/// Queue scans cache the blocks they read (pricing benchmark, 2026-10-24):
/// a claim and a sweep scan the same small index families again and again,
/// and SlateDB's default scan reads every sorted run's block from the
/// bucket each time (1M extra GETs in a 90-minute 1x run).
pub(super) fn queue_scan_options() -> slatedb::config::ScanOptions {
    slatedb::config::ScanOptions {
        cache_blocks: true,
        ..slatedb::config::ScanOptions::default()
    }
}

/// The key range of one row family.
pub(super) fn family_range(hash: &[u8; 16], fam: u8) -> (Vec<u8>, Vec<u8>) {
    let from = keys::family(hash, fam);
    let mut to = from.clone();
    if let Some(last) = to.last_mut() {
        *last += 1;
    }
    (from, to)
}

/// The key range of a time-ordered family from `(at, offset)` on.
pub(super) fn time_range_from(
    hash: &[u8; 16],
    fam: u8,
    at: i64,
    offset: u64,
) -> (Vec<u8>, Vec<u8>) {
    (
        keys::by_time(hash, fam, at, offset),
        family_range(hash, fam).1,
    )
}
