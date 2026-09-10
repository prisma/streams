//! Durable custody of sealed read batches and their resident size ledger.
use super::{ReadBatch, telemetry_settings};
use std::collections::HashMap;
use std::sync::{Arc, Mutex, atomic::AtomicU64};

/// Sealed read batches enter durable custody here before the usage ledger;
/// removal follows only a durable ledger acknowledgement. The runtime shares
/// one bounded cache between this database and its usage rollup.
pub(crate) struct ReadSpool {
    db: Arc<slatedb::Db>,
    next: AtomicU64,
    /// Corrupt rows moved to `quarantine/` since open (plus any found
    /// at open) — nonzero means reads may be under-billed and an
    /// operator must inspect; the rows themselves are preserved.
    quarantined: AtomicU64,
    /// Exact resident (pending-key -> encoded len) map, maintained at
    /// open/persist/remove/quarantine — the depth and byte gauges the
    /// OOM review asks for, WITHOUT a scan per metrics tick.
    resident: Mutex<ResidentRows>,
    /// Test fault injection: -1 = never fail; N >= 0 = allow N more
    /// successful persists, then fail every one after.
    #[cfg(test)]
    pub fail_after: std::sync::atomic::AtomicI64,
}

/// Until spool readability is proved, cancellation/errors retain an explicit
/// close owner. The SlateDB runtime keeps that one close alive to completion.
struct SpoolOpenGuard(Option<Arc<slatedb::Db>>);

impl Drop for SpoolOpenGuard {
    #[expect(
        clippy::disallowed_methods,
        reason = "SpoolOpenGuard abandoned-open close; the storage executor owns this database until close completes even if its opener is cancelled; tying the close to that cancelled future would abandon storage resources"
    )]
    fn drop(&mut self) {
        let Some(db) = self.0.take() else {
            return;
        };
        crate::bootstrap::slatedb_runtime().spawn(async move {
            if let Err(error) = db.close().await {
                tracing::warn!("abandoned read spool close failed: {error}");
            }
        });
    }
}

impl ReadSpool {
    /// Test-only convenience: a fresh cache, with no ambient shared state.
    #[cfg(test)]
    pub(crate) async fn open(
        store: Arc<dyn object_store::ObjectStore>,
        prefix: &str,
        instance: &str,
        cfg: &crate::config::ServerConfig,
    ) -> anyhow::Result<Self> {
        Self::open_with_cache(
            store,
            prefix,
            instance,
            cfg,
            crate::runtime::TelemetryResources::new(cfg.billing.telemetry_cache_bytes).cache,
        )
        .await
    }

    /// Production callers supply their runtime's shared telemetry cache.
    pub(crate) async fn open_with_cache(
        store: Arc<dyn object_store::ObjectStore>,
        prefix: &str,
        instance: &str,
        cfg: &crate::config::ServerConfig,
        cache: Arc<slatedb::db_cache::foyer::FoyerCache>,
    ) -> anyhow::Result<Self> {
        let inst = if instance.is_empty() {
            "solo"
        } else {
            instance
        };
        let path = if prefix.is_empty() {
            format!("telemetry/read-spool/{inst}")
        } else {
            format!("{prefix}/telemetry/read-spool/{inst}")
        };
        let settings = telemetry_settings(&cfg.billing, &cfg.engine.compactor_options());
        let db = crate::bootstrap::on_slatedb_rt(async move {
            slatedb::Db::builder(path.as_str(), store)
                .with_settings(settings)
                .with_db_cache(cache)
                .build()
                .await
        })
        .await?;
        let db = Arc::new(db);
        let mut opening = SpoolOpenGuard(Some(db.clone()));
        let next = match db.get(&b"meta/next-seq"[..]).await? {
            Some(v) => crate::shard::decode_cursor(&v)?,
            None => 0,
        };
        // Rows quarantined by earlier boots stay on the books: the
        // corruption alert survives restarts until an operator clears
        // the quarantine explicitly.
        let mut prior = 0u64;
        let mut it = db.scan_prefix(&b"quarantine/"[..], ..).await?;
        while (it.next().await?).is_some() {
            prior += 1;
        }
        let mut sizes = HashMap::new();
        let mut it = db.scan_prefix(&b"rb/"[..], ..).await?;
        while let Some(kv) = it.next().await? {
            sizes.insert(kv.key.to_vec(), kv.value.len() as u64);
        }
        let bytes = sizes.values().sum();
        let sp = ReadSpool {
            db,
            next: AtomicU64::new(next),
            quarantined: AtomicU64::new(prior),
            resident: Mutex::new(ResidentRows { sizes, bytes }),
            #[cfg(test)]
            fail_after: std::sync::atomic::AtomicI64::new(-1),
        };
        // Round-22 item 2b: an openable spool whose PENDING rows cannot
        // be scanned is not "ready" — prove readability before use.
        sp.pending(16).await?;
        opening.0.take();
        Ok(sp)
    }

    fn key(seq: u64) -> Vec<u8> {
        let mut k = b"rb/".to_vec();
        k.extend_from_slice(&seq.to_be_bytes());
        k
    }

    /// Fixture convenience for one batch; production persists whole drain rounds.
    #[cfg(test)]
    pub(crate) async fn persist(&self, b: &ReadBatch) -> anyhow::Result<Vec<u8>> {
        let keys = self.persist_all(std::slice::from_ref(b)).await?;
        Ok(keys.into_iter().next().expect("one key per batch"))
    }

    /// Durably persist a WHOLE drain round in ONE WriteBatch with ONE
    /// flush (OOM review item 5): a flush per sealed batch minted a
    /// tiny L0 per batch and fed compaction churn on the two-thread
    /// SlateDB runtime the history compactor also needs. All-or-
    /// nothing: on error, NOTHING in `batches` is durable and the
    /// caller requeues the whole slice.
    #[expect(
        clippy::unwrap_used,
        reason = "ReadSpool persist_all; poison may reflect a partial accounting update after a durable write; recovering the ledger could report false spool capacity"
    )]
    pub(crate) async fn persist_all(&self, batches: &[ReadBatch]) -> anyhow::Result<Vec<Vec<u8>>> {
        #[cfg(test)]
        {
            let left = self.fail_after.load(std::sync::atomic::Ordering::SeqCst);
            match left {
                0 => anyhow::bail!("injected spool fault"),
                1.. => {
                    self.fail_after
                        .fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
                }
                _ => {} // Negative values disable the fixture fault.
            }
        }
        if batches.is_empty() {
            return Ok(Vec::new());
        }
        let seq0 = self
            .next
            .fetch_add(batches.len() as u64, std::sync::atomic::Ordering::SeqCst);
        let mut wb = slatedb::WriteBatch::new();
        let mut keys = Vec::with_capacity(batches.len());
        let mut encoded = Vec::with_capacity(batches.len());
        for (i, b) in batches.iter().enumerate() {
            let key = Self::key(seq0 + i as u64);
            let v = serde_json::to_vec(b)?;
            encoded.push((key.clone(), v.len() as u64));
            wb.put(key.clone(), v);
            keys.push(key);
        }
        wb.put(
            &b"meta/next-seq"[..],
            &(seq0 + batches.len() as u64).to_le_bytes()[..],
        );
        self.db.write(wb).await?;
        // The build runs with wal_disable (the engines own their WAL
        // cadence), so an explicit flush is what makes "spooled" mean
        // OBJECT-STORE durable — the entire point of this spool.
        self.db.flush().await?;
        let mut resident = self.resident.lock().unwrap();
        for (key, len) in encoded {
            resident.insert(key, len);
        }
        Ok(keys)
    }

    /// Oldest pending batches (recovered across restarts). A row that
    /// does not decode is QUARANTINED (round-22 item 2c): moved to
    /// `quarantine/<key>` in one batch — preserved for forensics,
    /// never re-parsed, never silently skipped — counted, logged at
    /// error level, and raised as an alert by the ops evaluator.
    /// Scan/store faults still propagate as errors (fail closed);
    /// only decode failures quarantine.
    pub(crate) async fn pending(&self, max: usize) -> anyhow::Result<Vec<(Vec<u8>, ReadBatch)>> {
        if max == 0 {
            return Ok(Vec::new());
        }
        let mut out = Vec::new();
        let mut corrupt: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
        let mut iter = self.db.scan_prefix(&b"rb/"[..], ..).await?;
        while let Some(kv) = iter.next().await? {
            match serde_json::from_slice(&kv.value) {
                Ok(b) => {
                    out.push((kv.key.to_vec(), b));
                }
                Err(e) => {
                    tracing::error!(
                        "read spool row corrupt ({} bytes at {}): {e} — quarantining; \
                         reads in this batch are NOT billed until an operator recovers it",
                        kv.value.len(),
                        String::from_utf8_lossy(&kv.key),
                    );
                    corrupt.push((kv.key.to_vec(), kv.value.to_vec()));
                }
            }
            if out.len() >= max {
                break;
            }
        }
        self.quarantine(&corrupt).await?;
        Ok(out)
    }

    /// Persist quarantine before removing its rows from the resident ledger.
    #[expect(
        clippy::unwrap_used,
        reason = "ReadSpool quarantine accounting; poison may follow a partial resident-ledger update after durable quarantine; silent recovery could conceal unbilled rows"
    )]
    async fn quarantine(&self, corrupt: &[(Vec<u8>, Vec<u8>)]) -> anyhow::Result<()> {
        if corrupt.is_empty() {
            return Ok(());
        }
        let mut wb = slatedb::WriteBatch::new();
        for (k, v) in corrupt {
            let mut qk = b"quarantine/".to_vec();
            qk.extend_from_slice(k);
            wb.put(qk, v.clone());
            wb.delete(k.clone());
        }
        self.db.write(wb).await?;
        self.db.flush().await?;
        self.quarantined
            .fetch_add(corrupt.len() as u64, std::sync::atomic::Ordering::Relaxed);
        self.resident
            .lock()
            .unwrap()
            .remove(corrupt.iter().map(|(key, _)| key));
        Ok(())
    }

    /// L0 posture of the spool DB itself (round-trip of the same
    /// in-memory manifest probe the history partitions use): the
    /// bounded-settings claim, observable.
    pub(crate) fn l0_stats(&self) -> (u64, u64, u64, u64) {
        crate::history::history_l0_stats(&self.db)
    }

    /// Quarantined-row count (this boot + found at open). Nonzero is
    /// an open alert until the quarantine is cleared by an operator.
    pub(crate) fn quarantined_count(&self) -> u64 {
        self.quarantined.load(std::sync::atomic::Ordering::Relaxed)
    }

    #[cfg(test)]
    pub(crate) async fn put_raw(&self, key: &[u8], val: &[u8]) -> anyhow::Result<()> {
        let mut wb = slatedb::WriteBatch::new();
        wb.put(key, val);
        self.db.write(wb).await?;
        Ok(())
    }

    #[cfg(test)]
    pub(crate) async fn close_for_tests(&self) {
        self.db.close().await.unwrap();
    }

    #[cfg(test)]
    pub(crate) async fn quarantine_rows(&self) -> Vec<Vec<u8>> {
        let mut out = Vec::new();
        let mut it = self.db.scan_prefix(&b"quarantine/"[..], ..).await.unwrap();
        while let Some(kv) = it.next().await.unwrap() {
            out.push(kv.key.to_vec());
        }
        out
    }

    /// Remove batches the ledger has durably acknowledged.
    #[expect(
        clippy::unwrap_used,
        reason = "ReadSpool remove; poison may reflect a partial acknowledgement update; recovering the ledger could subtract rows twice or conceal backlog"
    )]
    pub(crate) async fn remove(&self, keys: &[Vec<u8>]) -> anyhow::Result<()> {
        if keys.is_empty() {
            return Ok(());
        }
        let mut wb = slatedb::WriteBatch::new();
        for k in keys {
            wb.delete(k.clone());
        }
        self.db.write(wb).await?;
        self.resident.lock().unwrap().remove(keys);

        Ok(())
    }

    /// (rows, encoded bytes) resident in the spool — exact, lock-cheap
    /// gauges for the ops snapshot and the readiness surface.
    #[expect(
        clippy::unwrap_used,
        reason = "ReadSpool resident; poison leaves no trustworthy row and byte snapshot; silently reporting capacity would hide incomplete accounting"
    )]
    pub(crate) fn resident(&self) -> (u64, u64) {
        let resident = self.resident.lock().unwrap();
        (resident.sizes.len() as u64, resident.bytes)
    }

    pub(crate) async fn depth(&self) -> usize {
        // Health reads use the exact counters rebuilt during open and updated
        // on spool puts/removals. They must not materialize the durable backlog
        // or turn a failed scan into a reassuring zero-depth answer.
        self.resident().0.try_into().unwrap_or(usize::MAX)
    }
}

/// Byte totals change under the same lock as their exact key inventory.
struct ResidentRows {
    sizes: HashMap<Vec<u8>, u64>,
    bytes: u64,
}

impl ResidentRows {
    fn insert(&mut self, key: Vec<u8>, len: u64) {
        let previous = self.sizes.insert(key, len).unwrap_or(0);
        self.bytes = self.bytes - previous + len;
    }

    fn remove<'a>(&mut self, keys: impl IntoIterator<Item = &'a Vec<u8>>) {
        for key in keys {
            if let Some(len) = self.sizes.remove(key) {
                self.bytes -= len;
            }
        }
    }
}

#[cfg(test)]
mod tests;
