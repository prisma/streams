//! A native queue's sealed pages, read outside the committer (S3b-DESIGN
//! §6.2). Once a seal takes a queue's pages the committer holds none of
//! them, and no store read for a queue body runs in the committer: a
//! claim's handler, a peek, a read by id and the sweeper read the pages
//! they may need through the stream read path (the one production read
//! path, G2) and pass them in their operation. The committer serves those
//! at or past its own frontier, which only moves on, and drops the rest.
use bytes::Bytes;

use super::super::{Deliver, RingScan, ShardEngine};
use crate::native_queue::rows::StateRow;
use crate::native_queue::{PrepareFrom, keys};

impl ShardEngine {
    /// The queue stream's sealed boundary as the committer applies it.
    pub(crate) async fn queue_sealed_boundary(&self, hash: [u8; 16]) -> Result<u64, String> {
        let handle = self.stream_handle(hash).await.map_err(|e| e.to_string())?;
        let st = handle
            .state
            .lock()
            .map_err(|_| "a queue stream's state is poisoned".to_owned())?;
        Ok(st.applied.absorbed)
    }

    /// Tests: make every queue view's check after the committer's answer
    /// see the sealed boundary `by` past where it is (a seal racing it).
    #[cfg(test)]
    pub(crate) fn race_queue_seals_for_tests(&self, by: u64) {
        let race = &self.layout.seal.hooks.queue_seal_race;
        race.store(by, std::sync::atomic::Ordering::Relaxed);
    }

    /// Tests: what a queue view's raced-seal check adds to the boundary.
    #[cfg(test)]
    pub(crate) fn queue_seal_race_for_tests(&self) -> u64 {
        let race = &self.layout.seal.hooks.queue_seal_race;
        race.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// The sealed pages of the queue `hash` from `from` up to its stream's
    /// sealed boundary, at most `max_bytes` of them (the page that reaches
    /// them is kept), as `(offset, page)`, and that boundary. Nothing is
    /// read while the start is unsealed: the committer holds those pages.
    pub(crate) async fn prepare_queue_pages(
        &self,
        hash: [u8; 16],
        from: PrepareFrom,
        max_bytes: usize,
    ) -> Result<(Vec<(u64, Bytes)>, u64), String> {
        let sealed = self.queue_sealed_boundary(hash).await?;
        let Some(start) = self.queue_prepare_start(hash, from).await? else {
            return Ok((Vec::new(), sealed));
        };
        if start >= sealed {
            return Ok((Vec::new(), sealed));
        }
        let handle = self.stream_handle(hash).await.map_err(|e| e.to_string())?;
        let window = RingScan {
            from: start,
            to: sealed,
            max_bytes,
        };
        let read = self
            .read_stream_window(&handle, window, None, Deliver::Durable)
            .await
            .map_err(|e| format!("{e:?}"))?;
        let pages = read
            .frames
            .iter()
            .map(|slice| (slice.first(), slice.page().raw().clone()))
            .collect();
        Ok((pages, sealed))
    }

    /// Where a preparation starts, from the queue's state row as stored:
    /// its frontier only moves on, so an older row starts no later than
    /// the committer's own and the committer drops what it has passed.
    /// `None` when nothing is to be read.
    async fn queue_prepare_start(
        &self,
        hash: [u8; 16],
        from: PrepareFrom,
    ) -> Result<Option<u64>, String> {
        let raw = self
            .db
            .get(keys::state(&hash))
            .await
            .map_err(|e| e.to_string())?;
        let Some(raw) = raw else {
            return Ok(None);
        };
        let state = StateRow::decode(&raw)?;
        let frontier = state.frontier;
        Ok(match from {
            PrepareFrom::Frontier => Some(frontier),
            PrepareFrom::From(offset) => Some(offset.max(frontier)),
            PrepareFrom::At(offset) => (offset >= frontier).then_some(offset),
        })
    }
}
