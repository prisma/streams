//! Merges of run-index objects (layout 6, DESIGN-6A §2.3): `fan_in[0]`
//! fold objects (level 0) into one of level 1 ("hourly" at the 15-minute
//! period), `fan_in[1]` of those into one of level 2 ("daily"), so a cold
//! read consults a bounded number of footers over days.
//!
//! One merge, under the fold's serial lock and the process's one merge
//! permit: the oldest eligible inputs of one level (each at most a quarter
//! of the merge budget, together at most half; at least two) are read
//! whole and checked, merged stream by stream (entries a consolidated chunk
//! contains dropped), PUT and read back like a fold's object, then one
//! batch puts the new pointer row and deletes the inputs' (WAL-flushed,
//! durable; an error closes the engine). The set gains the new object
//! before it loses the inputs, and the inputs are deleted from the store
//! `retire_grace` later (a read holding the old set may still fetch them;
//! a restart before then leaves them as orphans for package 6's sweep).

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Instant;

use bytes::Bytes;
use object_store::ObjectStoreExt;
use slatedb::WriteBatch;

use super::{FoldError, FoldSettings, finish, kept_entries, lock};
use crate::shard::ShardEngine;
use crate::spill::directory::DirEntry;
use crate::spill::object::ObjectId;
use crate::spill::runindex::format::{self, FooterEntry, ObjectWriter};
use crate::spill::runindex::{self, Orphans, Pointer};

/// One input, read whole and checked.
struct Input {
    pointer: Pointer,
    body: Bytes,
    footer: Vec<FooterEntry>,
}

/// The inputs of the next merge, oldest first, if one is due.
fn merge_inputs(set: &BTreeMap<ObjectId, Pointer>, s: &FoldSettings) -> Option<Vec<Pointer>> {
    let item_cap = s.merge_bytes / 4;
    for level in 0..format::MAX_LEVEL {
        let fan_in = s
            .fan_in
            .get(usize::from(level))
            .copied()
            .unwrap_or(usize::MAX)
            .max(2);
        let eligible: Vec<&Pointer> = set
            .values()
            .filter(|p| p.level == level && p.len <= item_cap)
            .collect();
        if eligible.len() < fan_in {
            continue;
        }
        let mut inputs = Vec::new();
        let mut total = 0u64;
        for p in eligible.into_iter().take(fan_in) {
            if total + p.len > s.merge_bytes / 2 {
                break;
            }
            total += p.len;
            inputs.push(p.clone());
        }
        if inputs.len() >= 2 {
            return Some(inputs);
        }
    }
    None
}

impl ShardEngine {
    pub(super) async fn merge_logged(self: &Arc<Self>) {
        while let Some(result) = self.merge_once().await {
            let Err(e) = result else {
                continue;
            };
            if !self.is_closed() {
                tracing::warn!(shard = %self.prefix, "run-index merge failed: {e}");
            }
            return;
        }
    }

    /// One merge if one is due: `None` when none is, else its outcome.
    pub(crate) async fn merge_once(self: &Arc<Self>) -> Option<Result<Pointer, FoldError>> {
        let fs = self.fold_state();
        let _one = fs.serial.lock().await;
        let inputs = merge_inputs(&fs.set(), &fs.settings())?;
        let Ok(_permit) = crate::spill::budget::merge_permit().acquire().await else {
            return None;
        };
        let result = self.merge_steps(inputs).await;
        let counter = if result.is_ok() {
            &fs.counters.merges
        } else {
            &fs.counters.merge_failures
        };
        counter.fetch_add(1, Ordering::Relaxed);
        Some(result)
    }

    async fn merge_steps(self: &Arc<Self>, inputs: Vec<Pointer>) -> Result<Pointer, FoldError> {
        let fs = self.fold_state();
        let mut read = Vec::with_capacity(inputs.len());
        for p in inputs {
            read.push(self.read_input(p).await?);
        }
        let level = read.iter().map(|i| i.pointer.level).max().unwrap_or(0) + 1;
        let id = self.next_index_id();
        let built = merged(id, level, &read)?;
        self.put_verified(&built.bytes, &built.pointer).await?;
        #[cfg(test)]
        fs.check_abort(super::test_hooks::FoldStep::Verified)?;
        let mut batch = WriteBatch::new();
        batch.put(
            runindex::pointer_key(id),
            runindex::encode_pointer(&built.pointer),
        );
        for i in &read {
            batch.delete(runindex::pointer_key(i.pointer.id));
        }
        self.write_index_batch(batch).await?;
        #[cfg(test)]
        fs.check_abort(super::test_hooks::FoldStep::Committed)?;
        fs.replace_set(|set| {
            set.insert(id, built.pointer.clone());
        });
        fs.replace_set(|set| {
            for i in &read {
                set.remove(&i.pointer.id);
            }
        });
        let now = Instant::now();
        lock(&fs.retired).extend(read.iter().map(|i| (now, i.pointer.id)));
        fs.counters
            .merged_inputs
            .fetch_add(read.len() as u64, Ordering::Relaxed);
        fs.counters
            .entries_dropped
            .fetch_add(built.dropped, Ordering::Relaxed);
        Ok(built.pointer)
    }

    /// An input, read whole and checked against its pointer row.
    async fn read_input(&self, pointer: Pointer) -> Result<Input, FoldError> {
        let path = self.run_index_path(pointer.id);
        let body = self
            .data_store
            .get(&object_store::path::Path::from(path.as_str()))
            .await
            .map_err(|e| FoldError::Store(format!("{path}: {e}")))?
            .bytes()
            .await
            .map_err(|e| FoldError::Store(format!("{path}: {e}")))?;
        let bad = |what: &str| FoldError::Verify(format!("{path}: {what}"));
        if body.len() as u64 != pointer.len {
            return Err(bad("length"));
        }
        let split = body.len().saturating_sub(format::TRAILER_LEN);
        let (trailer, crc) =
            format::decode_trailer(&body[split..]).ok_or_else(|| bad("trailer"))?;
        let start = usize::try_from(trailer.footer_off).map_err(|_| bad("footer offset"))?;
        let footer = body.get(start..split).ok_or_else(|| bad("footer range"))?;
        let footer = format::checked_footer(footer, &trailer, crc).ok_or_else(|| bad("footer"))?;
        Ok(Input {
            pointer,
            body,
            footer,
        })
    }

    /// Delete the merged-away objects whose grace passed.
    pub(crate) async fn delete_retired(&self) {
        let fs = self.fold_state();
        let grace = fs.settings().retire_grace;
        let due: Vec<ObjectId> = {
            let mut retired = lock(&fs.retired);
            let (due, keep): (Vec<_>, Vec<_>) =
                retired.drain(..).partition(|(at, _)| at.elapsed() >= grace);
            *retired = keep;
            due.into_iter().map(|(_, id)| id).collect()
        };
        for id in due {
            let path = object_store::path::Path::from(self.run_index_path(id));
            match self.data_store.delete(&path).await {
                Ok(()) | Err(object_store::Error::NotFound { .. }) => {
                    fs.counters.retired_deleted.fetch_add(1, Ordering::Relaxed);
                }
                Err(e) => {
                    tracing::debug!(shard = %self.prefix, "a retired run-index object stays (an orphan): {e}")
                }
            }
        }
    }
}

/// One input's run of `f`, decoded and checked against its footer entry.
fn input_run(input: &Input, f: &FooterEntry) -> Result<Vec<DirEntry>, FoldError> {
    let start = usize::try_from(f.run_off).unwrap_or(usize::MAX);
    let raw = start
        .checked_add(f.run_len as usize)
        .and_then(|end| input.body.get(start..end))
        .ok_or_else(|| FoldError::Verify(format!("{:?}: run range", input.pointer.id)))?;
    let entries = format::decode_run(raw)
        .ok_or_else(|| FoldError::Verify(format!("{:?}: run", input.pointer.id)))?;
    if entries.len() != f.entries as usize {
        return Err(FoldError::Verify(format!(
            "{:?}: run count",
            input.pointer.id
        )));
    }
    Ok(entries)
}

/// The merged object: every stream of every input, in stream order, one
/// stream's runs decoded at a time.
fn merged(id: ObjectId, level: u8, inputs: &[Input]) -> Result<super::Built, FoldError> {
    let mut streams: BTreeMap<[u8; 16], Vec<(usize, usize)>> = BTreeMap::new();
    for (i, input) in inputs.iter().enumerate() {
        for (j, f) in input.footer.iter().enumerate() {
            streams.entry(f.hash).or_default().push((i, j));
        }
    }
    let mut writer = ObjectWriter::new();
    let mut dropped = 0;
    for (hash, runs) in streams {
        let mut entries = Vec::new();
        for (i, j) in runs {
            let input = inputs
                .get(i)
                .ok_or_else(|| FoldError::Verify("input".into()))?;
            let f = input
                .footer
                .get(j)
                .ok_or_else(|| FoldError::Verify("footer".into()))?;
            entries.extend(input_run(input, f)?);
        }
        let (kept, d) = kept_entries(entries);
        dropped += d;
        writer.push(hash, &kept).map_err(FoldError::Format)?;
    }
    let seals = inputs.iter().map(|i| i.pointer.seals).sum();
    let orphans = inputs
        .iter()
        .fold(Orphans::default(), |a, i| a.add(i.pointer.orphans));
    let built = finish(id, writer, level, seals, orphans)?;
    Ok(super::Built { dropped, ..built })
}
