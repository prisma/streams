//! The sealer's open (layout 6; its reads layout 8's indexes, S3a §4.1): the in-memory directory
//! from the `'K'` rows, every stream the unsealed index `'W'` names held through its handle (its
//! pending row tiled against its tail), and every stream the usage-dirty index `'M'` names marked
//! in the usage dirty set at its version (the drain re-emits each once; the rollup deduplicates by
//! version). No read scans the shard's tails. A stream whose tail or pending row does not decode
//! fails alone: it is counted and logged, and its own reads and appends meet the corruption when
//! they load it; every other stream is served.

use crate::shard::ShardEngine;
use crate::spill::directory::{self, Directory};

/// A load failure that is the store's, not the stream's: the store closed
/// (a fence among them) or is unavailable.
fn store_failed(error: &slatedb::Error) -> bool {
    matches!(
        error.kind(),
        slatedb::ErrorKind::Closed(_) | slatedb::ErrorKind::Unavailable
    )
}

impl ShardEngine {
    /// The open's rebuild. Until it finishes (the sealer's loop marks the
    /// state ready once it returns), sealed reads and the usage drain wait
    /// (`seal_ready`) and no seal starts.
    pub(super) async fn seal_open(&self) -> anyhow::Result<()> {
        #[cfg(test)]
        self.injected_open_fault()?;
        self.gc_state().note_opened();
        let home = crate::shard::open_index::recorded_home(&self.db).await?;
        let home = home.unwrap_or_else(|| crate::keyspace::home_of(&self.prefix).unwrap_or(0));
        self.layout
            .seal
            .home
            .store(home, std::sync::atomic::Ordering::Relaxed);
        self.probe_heads().await?;
        let dir = self.rebuild_directory().await?;
        self.rebuild_run_index().await?;
        let unsealed = self.scan_tails().await?;
        // Each stream the scan found with unsealed pages is held through
        // the handle the engine serves it from (a request's load may have
        // put it there first). A store that closed (a fence: the next owner
        // opened the log) or is unavailable fails the open, which retries,
        // as the scan's store errors do; any other failure is the stream's.
        for hash in unsealed {
            match self.open_load(hash).await {
                Ok(handle) => self.layout.hold_unsealed(&handle),
                Err(e) if store_failed(&e) => return Err(e.into()),
                Err(e) => self.note_corrupt_stream(&hash, &e.to_string()),
            }
        }
        let sp = &self.layout.seal;
        sp.publish_directory_rebuilt(dir);
        if !self.layout.unsealed().is_empty() {
            sp.note_unsealed();
            sp.seal_inherited();
        }
        Ok(())
    }

    /// The directory from the `'K'` rows the fold has not folded yet (at
    /// most one fold period's). A row that does not decode is counted and
    /// skipped: the fold leaves it in place, and every other row is served.
    async fn rebuild_directory(&self) -> anyhow::Result<Directory> {
        let prefix = directory::directory_prefix();
        let mut rows = self.db.scan_prefix(&prefix[..], ..).await?;
        let mut dir = Directory::default();
        while let Some(kv) = rows.next().await? {
            let decoded = directory::parse_directory_key(&kv.key)
                .and_then(|id| directory::decode_row(id, &kv.value));
            let Some(entries) = decoded else {
                self.note_corrupt_index_row("directory", &kv.key);
                continue;
            };
            for (hash, entry) in entries {
                dir.insert(hash, entry);
            }
        }
        Ok(dir)
    }

    /// The streams the unsealed index names (S3a §4.1), and every stream the usage-dirty index
    /// names marked dirty at its version, so the drain re-emits only those. A store error fails the
    /// open (and it retries).
    async fn scan_tails(&self) -> anyhow::Result<Vec<[u8; 16]>> {
        for (hash, version) in crate::shard::open_index::dirty_members(&self.db).await? {
            self.layout.mark_usage(hash, version);
        }
        Ok(crate::shard::open_index::unsealed_members(&self.db).await?)
    }

    /// A stream the open could not load: counted, logged, skipped.
    fn note_corrupt_stream(&self, hash: &[u8; 16], why: &str) {
        self.layout
            .seal
            .counters
            .corrupt_streams
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        tracing::error!(
            shard = %self.prefix,
            stream = %crate::crypto::hex(&hash[..4]),
            "the sealer's open skipped a stream it cannot load: {why}"
        );
    }

    /// One stream's load at the open (tests: a fence injected for the
    /// shard meets it first).
    async fn open_load(
        &self,
        hash: [u8; 16],
    ) -> Result<std::sync::Arc<crate::shard::StreamHandle>, slatedb::Error> {
        #[cfg(test)]
        self.injected_load_fence()?;
        self.stream_handle(hash).await
    }

    /// Tests: this load meets a fence if one is injected for the shard.
    #[cfg(test)]
    fn injected_load_fence(&self) -> Result<(), slatedb::Error> {
        let mut fences = crate::shard::test_support::seal_open_load_fences()
            .lock()
            .unwrap();
        if let Some(n) = fences.get_mut(&self.prefix)
            && *n > 0
        {
            *n -= 1;
            return Err(slatedb::Error::closed(
                "detected newer DB client (test hook)".into(),
                slatedb::CloseReason::Fenced,
            ));
        }
        Ok(())
    }

    /// Tests: fail this open if a fault is injected for the shard.
    #[cfg(test)]
    fn injected_open_fault(&self) -> anyhow::Result<()> {
        let mut faults = crate::shard::seal_open_faults().lock().unwrap();
        if let Some(n) = faults.get_mut(&self.prefix)
            && *n > 0
        {
            *n -= 1;
            anyhow::bail!("injected sealer open fault (test hook)");
        }
        Ok(())
    }

    /// Wait until the sealer's open finished; an error once the engine
    /// closed before it did, or when no sealer task is running to finish
    /// it (an engine whose required tasks were never spawned would
    /// otherwise keep every sealed read waiting).
    pub(crate) async fn seal_ready(&self) -> anyhow::Result<()> {
        let mut ready = self.layout.seal.ready.subscribe();
        let mut closed = self.close_tx.subscribe();
        loop {
            if *ready.borrow_and_update() {
                return Ok(());
            }
            anyhow::ensure!(
                self.tasks.running("sealer"),
                "no sealer task is running to open the sealer"
            );
            anyhow::ensure!(
                !*closed.borrow_and_update() && !self.is_closed(),
                "the engine closed before its sealer opened"
            );
            tokio::select! {
                changed = ready.changed() => changed?,
                changed = closed.changed() => changed?,
            }
        }
    }
}

impl super::SealState {
    #[expect(
        clippy::unwrap_used,
        reason = "SealState::publish_directory_rebuilt; a poisoned directory may hold a half-inserted seal; recovering it could serve a sealed range from chunks no seal finished publishing, whereas panicking ends the sealer, a required engine task, whose exit retires the engine, and the next open rebuilds this state from the store"
    )]
    fn publish_directory_rebuilt(&self, dir: Directory) {
        let mut current = self.directory.write().unwrap();
        // No seal runs before the open: every entry is the rebuild's.
        *current = dir;
    }
}
