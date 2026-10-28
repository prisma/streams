use super::*;
impl CommitTransaction<'_> {
    /// A complete checked snapshot belongs to the overlay. Failed scans never
    /// mark either the shared handle or this transaction as loaded.
    #[expect(
        clippy::unwrap_used,
        reason = "CommitTransaction::load_queue; a poisoned stream state may hold a half-loaded queue; recovering it could serve leases never made durable"
    )]
    pub(super) async fn load_queue(
        &self,
        local: &mut StreamOverlay,
        hash: [u8; 16],
    ) -> Result<(), String> {
        if local.queue.state.is_some() {
            return Ok(());
        }
        let shared = local.handle.state.lock().unwrap().queue.clone();
        if shared.loaded {
            local.queue.state = Some(shared);
            return Ok(());
        }
        let mut fresh = QueueState {
            consumers: HashMap::new(),
            loaded: true,
        };
        for tag in *b"clx" {
            self.load_queue_rows(&mut fresh, &hash, tag).await?;
        }
        local.queue.state = Some(fresh);
        Ok(())
    }
    /// Every consumer row of one tag (`'S' ‖ hash ‖ tag`, stream state).
    async fn load_queue_rows(
        &self,
        fresh: &mut QueueState,
        hash: &[u8; 16],
        tag: u8,
    ) -> Result<(), String> {
        let prefix = [&[crate::keyspace::STREAM][..], hash, &[tag]].concat();
        Self::scan_queue_rows(self.engine, fresh, hash, &prefix, tag).await
    }
    /// Every row under `prefix` of one tag, into `fresh`.
    async fn scan_queue_rows(
        engine: &ShardEngine,
        fresh: &mut QueueState,
        hash: &[u8; 16],
        prefix: &[u8],
        tag: u8,
    ) -> Result<(), String> {
        #[cfg(test)]
        crate::shard::test_support::held_queue_read(&engine.prefix).await;
        let mut rows = engine
            .db
            .scan_prefix(prefix, ..)
            .await
            .map_err(|e| e.to_string())?;
        while let Some(row) = rows.next().await.map_err(|e| e.to_string())? {
            Self::load_queue_row(fresh, hash, tag, &row.key, &row.value)?;
        }
        Ok(())
    }
    /// Put a cold stream's queue state, and the records of the consumers
    /// its group names, on its handle before the group stages, as
    /// `load_queue` and `config_record` would one op at a time on the
    /// serial commit path. The store holds exactly what earlier groups
    /// applied: this group has staged nothing yet. A failed or invalid
    /// read leaves the handle as it was; the op reads again and answers it.
    pub(in crate::shard::transaction) async fn warm_queue(
        engine: &ShardEngine,
        handle: &StreamHandle,
        consumers: &std::collections::BTreeSet<String>,
    ) {
        let cold = handle.state.lock().is_ok_and(|state| !state.queue.loaded);
        if cold && let Ok(snapshot) = Self::queue_snapshot(engine, &handle.hash).await {
            Self::keep_queue_snapshot(handle, snapshot);
        }
        for consumer in consumers {
            Self::warm_config(engine, handle, consumer).await;
        }
    }
    /// The stream's checked queue state, every tag's rows.
    async fn queue_snapshot(engine: &ShardEngine, hash: &[u8; 16]) -> Result<QueueState, String> {
        let mut fresh = QueueState {
            consumers: HashMap::new(),
            loaded: true,
        };
        for tag in *b"clx" {
            let prefix = [&[crate::keyspace::STREAM][..], hash, &[tag]].concat();
            Self::scan_queue_rows(engine, &mut fresh, hash, &prefix, tag).await?;
        }
        Ok(fresh)
    }
    /// `snapshot` on the handle, unless a group loaded it meanwhile.
    fn keep_queue_snapshot(handle: &StreamHandle, snapshot: QueueState) {
        if let Ok(mut state) = handle.state.lock()
            && !state.queue.loaded
        {
            state.queue = snapshot;
        }
    }
    /// `consumer`'s stored record on the handle, unless one is held.
    async fn warm_config(engine: &ShardEngine, handle: &StreamHandle, consumer: &str) {
        if handle.hot(|hot| hot.config(consumer).is_some()) != Some(false) {
            return;
        }
        #[cfg(test)]
        crate::shard::test_support::held_config_read(&engine.prefix).await;
        let stored = engine.db.get(config_key(&handle.hash, consumer)).await;
        if let Ok(Some(raw)) = stored
            && let Ok(record) = decode_consumer_record(&raw)
        {
            handle.hot(|hot| hot.keep_config(consumer, &record));
        }
    }
    fn load_queue_row(
        fresh: &mut QueueState,
        hash: &[u8; 16],
        tag: u8,
        key: &[u8],
        value: &[u8],
    ) -> Result<(), String> {
        let (consumer, cgen, offset) = decode_state_key(hash, tag, key).map_err(str::to_owned)?;
        // Even dead-generation rows must validate before the scan is complete.
        let cursor = if tag == b'c' {
            Some(decode_counter(value).map_err(str::to_owned)?)
        } else {
            None
        };
        let lease = if tag == b'l' {
            Some(decode_lease(value).ok_or("invalid queue lease width")?)
        } else {
            None
        };
        let state = fresh.consumers.entry(consumer.to_owned()).or_default();
        if state.cgen > cgen {
            return Ok(());
        }
        if state.cgen < cgen {
            *state = ConsumerState {
                cgen,
                ..Default::default()
            };
        }
        if let Some(cursor) = cursor {
            state.cursor = cursor;
        }
        if let Some(offset) = offset {
            if let Some(lease) = lease {
                state.leases.insert(offset, lease);
            } else {
                state.acked.insert(offset);
            }
        }
        Ok(())
    }
}
