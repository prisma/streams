//! The sealer's open (layout 6): the in-memory directory from the `'K'`
//! rows, then one scan of the stream segment's tails that loads every
//! stream holding unsealed pages (its pending row tiled against its tail)
//! and marks every billed stream in the usage dirty set (f3: the drain
//! re-emits each once; the rollup deduplicates by version). A stream whose
//! tail or pending row does not decode fails alone, as it did before the
//! seal: it is counted and logged, and its own reads and appends meet the
//! corruption when they load it; every other stream is served.

use crate::shard::{ShardEngine, stored_tail};
use crate::spill::directory::{self, Directory};

impl ShardEngine {
    /// The open's rebuild. Until it finishes (the sealer's loop marks the
    /// state ready once it returns), sealed reads and the usage drain wait
    /// (`seal_ready`) and no seal starts.
    pub(super) async fn seal_open(&self) -> anyhow::Result<()> {
        #[cfg(test)]
        self.injected_open_fault()?;
        self.gc_state().note_opened();
        self.probe_heads().await?;
        let dir = self.rebuild_directory().await?;
        self.rebuild_run_index().await?;
        let unsealed = self.scan_tails().await?;
        // Each stream the scan found with unsealed pages is held through
        // the handle the engine serves it from (a request's load may have
        // put it there first).
        for hash in unsealed {
            match self.stream_handle(hash).await {
                Ok(handle) => self.layout.hold_unsealed(&handle),
                Err(e) => self.note_corrupt_stream(&hash, &e.to_string()),
            }
        }
        let sp = &self.layout.seal;
        sp.publish_directory_rebuilt(dir);
        if !self.layout.unsealed().is_empty() {
            sp.note_unsealed();
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

    /// One scan of the stream segment's tails: the streams holding unsealed
    /// pages, with every billed stream marked in the usage dirty set on the
    /// way. An undecodable tail is counted and skipped; a store error fails
    /// the scan (and the open retries).
    async fn scan_tails(&self) -> anyhow::Result<Vec<[u8; 16]>> {
        let mut unsealed = Vec::new();
        let mut rows = self
            .db
            .scan_prefix(&[crate::keyspace::STREAM][..], ..)
            .await?;
        while let Some(row) = rows.next().await? {
            let Some((b't', hash)) = row.key.get(1..).and_then(|k| k.split_last()) else {
                continue;
            };
            let Ok(hash) = <[u8; 16]>::try_from(hash) else {
                continue;
            };
            let tail = match stored_tail(&row.value) {
                Ok(tail) => tail,
                Err(e) => {
                    self.note_corrupt_stream(&hash, &e.to_string());
                    continue;
                }
            };
            if tail.absorbed < tail.next {
                unsealed.push(hash);
            }
            if let Some(usage) = tail.usage {
                self.layout.mark_usage(hash, usage.usage_version);
            }
        }
        Ok(unsealed)
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
