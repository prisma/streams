use super::*;
impl CommitTransaction<'_> {
    /// A complete checked snapshot belongs to the overlay. Failed scans never
    /// mark either the shared handle or this transaction as loaded.
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
            let mut prefix = hash.to_vec();
            prefix.push(tag);
            let mut rows = self
                .engine
                .db
                .scan_prefix(&prefix[..], ..)
                .await
                .map_err(|e| e.to_string())?;
            while let Some(row) = rows.next().await.map_err(|e| e.to_string())? {
                Self::load_queue_row(&mut fresh, &hash, tag, &row.key, &row.value)?;
            }
        }
        local.queue.state = Some(fresh);
        Ok(())
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
