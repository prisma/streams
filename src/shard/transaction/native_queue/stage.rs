//! A queue's rows within one commit group: the state row, and every row the
//! group staged, readable by the group's later operations before the batch
//! is written (read-your-writes inside the group). Rows not staged are read
//! from the shard log, which already holds every earlier group's write.
use std::collections::BTreeMap;
use std::ops::Bound;

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
    /// Invariant F on insert: a put into a time-ordered family lowers its
    /// floor to the entry's time, and the state row carries it in the same
    /// batch as the entry.
    fn lower_floor(&mut self, key: &[u8]) {
        let (Some(&fam), Some(at)) = (key.get(keys::FAMILY_AT), key_time(key)) else {
            return;
        };
        if !key.starts_with(&crate::keyspace::QUEUE) {
            return;
        }
        if let Some(floor) = self.state.floors.of(fam) {
            let lowered = (*floor).min(at);
            if lowered != *floor {
                *floor = lowered;
                self.state_dirty = true;
            }
        }
    }

    /// Raise a family's floor to the first live entry a scan from the
    /// floor found (`i64::MAX` when it found none). Not a state change on
    /// its own: it rides the batch only if the state row is written.
    pub(in crate::shard::transaction) fn raise_floor(&mut self, fam: u8, first: Option<i64>) {
        if let Some(floor) = self.state.floors.of(fam) {
            *floor = (*floor).max(first.unwrap_or(i64::MAX));
        }
    }

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
        st.lower_floor(&key);
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
            .scan_with_options(
                from.clone()..to.clone(),
                &crate::native_queue::scan_options(),
            )
            .await
            .map_err(|e| e.to_string())?;
        for _ in 0..want {
            let Some(kv) = it.next().await.map_err(|e| e.to_string())? else {
                break;
            };
            merged.insert(kv.key.to_vec(), Some(kv.value.to_vec()));
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

    /// Up to `limit` live entries of a time-ordered family, from its floor
    /// (for `avail`, from the later of the floor and the cursor), raising
    /// the floor to the first entry found.
    pub(super) async fn nq_index_scan(
        &self,
        hash: &[u8; 16],
        st: &mut NativeStage,
        fam: u8,
        limit: usize,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, String> {
        let Some(range) = index_range(hash, fam, st) else {
            return Ok(Vec::new());
        };
        let rows = self.nq_scan(st, range, limit).await?;
        st.raise_floor(fam, rows.first().and_then(|(k, _)| key_time(k)));
        Ok(rows)
    }

    /// For a claim at `t`: the live entries of a time-ordered family, read
    /// from where [`Self::nq_index_scan`] starts through its first live entry
    /// past `t` or at most `limit` stored rows (plus one per row the group
    /// staged in range, so its deletes cannot starve the read), with the
    /// time of the first entry past `t` among them: `i64::MAX` when the
    /// read reached the family's end without one, `None` when it stopped at
    /// the limit first. No stored row past that entry is read, and a staged
    /// row past the last stored row read is left out, so the rows are every
    /// live entry of a prefix of the family. Raises no floor: only the
    /// caller knows which entries it consumed.
    pub(super) async fn nq_due_scan(
        &self,
        hash: &[u8; 16],
        st: &mut NativeStage,
        fam: u8,
        read: (usize, i64),
    ) -> Result<(Vec<(Vec<u8>, Vec<u8>)>, Option<i64>), String> {
        let (limit, t) = read;
        let Some((from, to)) = index_range(hash, fam, st) else {
            return Ok((Vec::new(), Some(i64::MAX)));
        };
        let want = limit.saturating_add(st.rows.range(from.clone()..to.clone()).count());
        let mut it = self
            .engine
            .db
            .scan_with_options(
                from.clone()..to.clone(),
                &crate::native_queue::scan_options(),
            )
            .await
            .map_err(|e| e.to_string())?;
        let mut merged: BTreeMap<Vec<u8>, Option<Vec<u8>>> = BTreeMap::new();
        let mut ended = false;
        for _ in 0..want {
            let Some(kv) = it.next().await.map_err(|e| e.to_string())? else {
                ended = true;
                break;
            };
            let key = kv.key.to_vec();
            // A row the group deleted does not end the read.
            let past = key_time(&key).is_some_and(|at| at > t) && st.rows.get(&key) != Some(&None);
            merged.insert(key, Some(kv.value.to_vec()));
            if past {
                break;
            }
        }
        let end = if ended {
            Bound::Excluded(to)
        } else {
            merged
                .keys()
                .next_back()
                .map_or(Bound::Excluded(from.clone()), |k| {
                    Bound::Included(k.clone())
                })
        };
        for (k, v) in st.rows.range((Bound::Included(from), end)) {
            merged.insert(k.clone(), v.clone());
        }
        let rows: Vec<_> = merged
            .into_iter()
            .filter_map(|(k, v)| v.map(|v| (k, v)))
            .collect();
        let next = rows
            .iter()
            .filter_map(|(k, _)| key_time(k))
            .find(|at| *at > t);
        Ok((rows, next.or(ended.then_some(i64::MAX))))
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

/// The time of a time-ordered index key (the eight bytes after
/// `'Q' ‖ gen ‖ hash ‖ family`).
pub(super) fn key_time(key: &[u8]) -> Option<i64> {
    let at = <[u8; 8]>::try_from(key.get(keys::FAMILY_AT + 1..keys::FAMILY_AT + 9)?).ok()?;
    i64::try_from(u64::from_be_bytes(at)).ok()
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

/// The key range of a time-ordered family from its floor on: every live
/// entry by invariant F. `None` when the floor says the family is empty.
pub(super) fn floor_range(
    hash: &[u8; 16],
    fam: u8,
    st: &mut NativeStage,
) -> Option<(Vec<u8>, Vec<u8>)> {
    let floor = *st.state.floors.of(fam)?;
    if floor == i64::MAX {
        return None;
    }
    let mut from = keys::family(hash, fam);
    from.extend_from_slice(&u64::try_from(floor).unwrap_or(0).to_be_bytes());
    Some((from, family_range(hash, fam).1))
}

/// The key range an index scan reads: [`floor_range`], for `avail` from
/// the later of the floor and the cursor.
fn index_range(hash: &[u8; 16], fam: u8, st: &mut NativeStage) -> Option<(Vec<u8>, Vec<u8>)> {
    let (mut from, to) = floor_range(hash, fam, st)?;
    let s = &st.state;
    if fam == keys::AVAIL && s.cursor_started {
        let cursor = keys::by_time(hash, fam, s.cursor_at, s.cursor_off.saturating_add(1));
        from = from.max(cursor);
    }
    Some((from, to))
}
