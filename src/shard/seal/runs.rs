//! Reading run-index objects (layout 6, DESIGN-6A §2.3-§2.4): a footer by
//! the exact range its pointer row names, one stream's run by the range
//! its footer entry names, each decoded, checked and kept in the index
//! cache (`spill::cache`), and the candidates a sealed read tiles from.
//!
//! A 404 for an object the set names is retryable (a merge may have just
//! retired it after the reader took the set: the reader's next attempt
//! takes the new set); bytes that fail their checks are corruption.

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use bytes::Bytes;
use object_store::ObjectStoreExt;

use crate::shard::ShardEngine;
use crate::shard::record::RangeReadError;
use crate::spill::cache::{IndexValue, index_cache};
use crate::spill::directory::DirEntry;
use crate::spill::runindex::Pointer;
use crate::spill::runindex::format::{self, FooterEntry, TRAILER_LEN};

#[cfg(test)]
mod runs_tests;

/// Attempts per GET on a transient store error (not on a 404).
const GET_ATTEMPTS: u32 = 3;

fn corrupt(what: &str) -> RangeReadError {
    RangeReadError::Store(slatedb::Error::data(format!("run index corrupt: {what}")))
}

/// Memory a decoded run takes in the cache, an upper estimate.
fn run_heap(entries: &[DirEntry]) -> u64 {
    let per = size_of::<DirEntry>() as u64 + 64;
    entries.len() as u64 * per
}

impl ShardEngine {
    /// One ranged GET of a run-index object, retried on a transient error.
    async fn get_index_range(
        &self,
        path: &str,
        range: std::ops::Range<u64>,
    ) -> Result<Bytes, RangeReadError> {
        let at = object_store::path::Path::from(path);
        let mut attempt = 0;
        loop {
            attempt += 1;
            match self.data_store.get_range(&at, range.clone()).await {
                Ok(body) if body.len() as u64 == range.end - range.start => return Ok(body),
                Ok(body) => {
                    return Err(corrupt(&format!(
                        "{path}: GET {range:?} returned {} B",
                        body.len()
                    )));
                }
                Err(object_store::Error::NotFound { .. }) => {
                    return Err(RangeReadError::Store(slatedb::Error::unavailable(format!(
                        "run-index object {path} not found"
                    ))));
                }
                Err(e) if attempt < GET_ATTEMPTS => {
                    tracing::debug!("run-index GET {path} {range:?} failed, retrying: {e}");
                    tokio::time::sleep(Duration::from_millis(5 << attempt)).await;
                }
                Err(e) => {
                    return Err(RangeReadError::Store(slatedb::Error::unavailable(format!(
                        "run-index GET {path} {range:?}: {e}"
                    ))));
                }
            }
        }
    }

    /// The object's footer: from the cache, or one GET of the footer and
    /// trailer, checked against each other and the pointer row.
    pub(crate) async fn fetch_footer(
        &self,
        p: &Pointer,
    ) -> Result<Arc<Vec<FooterEntry>>, RangeReadError> {
        let path = self.run_index_path(p.id);
        let key = self.layout.seal.fold.cache_scope.key(&path);
        if let Some(IndexValue::Footer(f)) = index_cache().get(&key, p.footer_off) {
            return Ok(f);
        }
        self.layout
            .seal
            .fold
            .counters
            .footer_gets
            .fetch_add(1, Ordering::Relaxed);
        let body = self.get_index_range(&path, p.footer_off..p.len).await?;
        let split = body.len().saturating_sub(TRAILER_LEN);
        let (footer, tail) = body.split_at(split);
        let (trailer, crc) =
            format::decode_trailer(tail).ok_or_else(|| corrupt(&format!("{path}: trailer")))?;
        let agrees = trailer.footer_off == p.footer_off
            && trailer.footer_len == p.footer_len
            && u64::from(trailer.runs) == p.runs
            && trailer.entries == p.entries
            && trailer.level == p.level;
        if !agrees {
            return Err(corrupt(&format!(
                "{path}: the trailer disagrees with its pointer row"
            )));
        }
        let entries = format::checked_footer(footer, &trailer, crc)
            .ok_or_else(|| corrupt(&format!("{path}: footer")))?;
        let entries = Arc::new(entries);
        index_cache().insert(
            &key,
            p.footer_off,
            IndexValue::Footer(entries.clone()),
            u64::from(p.footer_len),
        );
        Ok(entries)
    }

    /// One stream's run: from the cache, or one GET of the range its footer
    /// entry names, checked against that entry.
    pub(crate) async fn fetch_run(
        &self,
        p: &Pointer,
        f: &FooterEntry,
    ) -> Result<Arc<Vec<DirEntry>>, RangeReadError> {
        let path = self.run_index_path(p.id);
        let key = self.layout.seal.fold.cache_scope.key(&path);
        if let Some(IndexValue::Run(run)) = index_cache().get(&key, f.run_off) {
            return Ok(run);
        }
        self.layout
            .seal
            .fold
            .counters
            .run_gets
            .fetch_add(1, Ordering::Relaxed);
        let body = self
            .get_index_range(&path, f.run_off..f.run_off + u64::from(f.run_len))
            .await?;
        let entries = format::decode_run(&body)
            .ok_or_else(|| corrupt(&format!("{path}: run at {}", f.run_off)))?;
        let agrees = entries.len() == f.entries as usize
            && entries.first().map(|e| e.first) == Some(f.first)
            && entries.last().map(|e| e.last) == Some(f.last);
        if !agrees {
            return Err(corrupt(&format!(
                "{path}: the run at {} disagrees with its footer",
                f.run_off
            )));
        }
        let size = run_heap(&entries);
        let entries = Arc::new(entries);
        index_cache().insert(&key, f.run_off, IndexValue::Run(entries.clone()), size);
        Ok(entries)
    }

    /// Every live object's run of `hash` that ends at or after `from` and
    /// starts before `to`, ordered by first offset.
    pub(crate) async fn runs_of(
        &self,
        hash: &[u8; 16],
        from: u64,
        to: u64,
    ) -> Result<Vec<(Pointer, FooterEntry)>, RangeReadError> {
        let set = self.layout.seal.fold.set();
        let mut out = Vec::new();
        for p in set.values() {
            let footer = self.fetch_footer(p).await?;
            let Ok(i) = footer.binary_search_by(|f| f.hash.cmp(hash)) else {
                continue;
            };
            let Some(f) = footer.get(i) else {
                continue;
            };
            if f.last >= from && f.first < to {
                out.push((p.clone(), f.clone()));
            }
        }
        out.sort_by_key(|(_, f)| f.first);
        Ok(out)
    }
}
