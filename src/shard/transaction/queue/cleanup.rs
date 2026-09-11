use super::*;
impl CommitTransaction<'_> {
    #[expect(
        clippy::too_many_arguments,
        reason = "CommitTransaction::delete_step; the delete step takes the consumer, its queue, the fence and the batch budget separately as the transaction resolved them; a request struct would exist for this single call site"
    )]
    #[expect(
        clippy::too_many_lines,
        reason = "CommitTransaction::delete_step; the delete step fences, scans, deletes and accounts in one transaction; splitting it would hide which rows each fence covers"
    )]
    #[expect(
        clippy::expect_used,
        reason = "CommitTransaction::delete_step; the consumer's queue entry was checked present just above under the same borrow; a fallible read would add a branch no checked step reaches"
    )]
    #[expect(
        clippy::unwrap_used,
        reason = "CommitTransaction::delete_step; a poisoned fence table or handle state may hold a half-applied fence or queue, and the queue state was populated in this step before it is read; recovering or failing either could delete rows a fence still protects"
    )]
    #[expect(
        clippy::excessive_nesting,
        reason = "CommitTransaction::delete_step; the delete step nests the budget checks inside each prefix scan and lease walk; flattening them would separate the checks from the rows they bound"
    )]
    pub(super) async fn delete_step(
        &mut self,
        local: &mut StreamOverlay,
        hash: [u8; 16],
        consumer: String,
        fence_below: u64,
        budget: CleanupBudget,
        resp: QueueReply,
    ) {
        let CleanupBudget {
            rows: max_rows,
            bytes: max_bytes,
        } = budget;
        // Conservatively fence first; failed scans retain retryable deletion
        // debt without giving an old generation another write window.
        {
            let mut f = self.engine.consumer_fences.lock().unwrap();
            let e = f.entry((hash, consumer.clone())).or_insert(0);
            *e = (*e).max(fence_below);
        }
        {
            let fk = crate::queue::fence_key(&hash, &consumer);
            let cur = match self.engine.db.get(&fk[..]).await {
                Ok(Some(v)) => match decode_counter(&v) {
                    Ok(value) => value,
                    Err(e) => {
                        self.effects
                            .queue_acks
                            .push((resp, Err(format!("consumer_fence_unverified: {e}"))));
                        return;
                    }
                },
                Ok(None) => 0,
                Err(e) => {
                    self.effects
                        .queue_acks
                        .push((resp, Err(format!("consumer_fence_unverified: {e}"))));
                    return;
                }
            };
            if fence_below > cur {
                self.batch.put(&fk[..], &fence_below.to_le_bytes()[..]);
                self.extra_writes = true;
            }
        }
        let scan_cap = max_rows.saturating_mul(4).max(1024);
        let mut dead: Vec<Vec<u8>> = Vec::new();
        let mut dead_bytes = 0usize;
        let mut scanned = 0usize;
        let mut more = false;
        let mut scan_err: Option<String> = None;
        #[cfg(test)]
        if self.engine.take_config_scan_failure() {
            scan_err = Some("injected config-scan failure".into());
        }
        if scan_err.is_none() {
            'scans: for tag in *b"clx" {
                let pfx = state_prefix(&hash, tag, &consumer);
                match self.engine.db.scan_prefix(&pfx[..], ..).await {
                    Ok(mut iter) => loop {
                        if dead.len() >= max_rows || dead_bytes >= max_bytes || scanned >= scan_cap
                        {
                            more = true;
                            break 'scans;
                        }
                        match iter.next().await {
                            Ok(Some(kv)) => {
                                scanned += 1;
                                let generation = match decode_state_key(&hash, tag, &kv.key) {
                                    Ok((name, generation, _)) if name == consumer => generation,
                                    _ => {
                                        scan_err = Some("invalid queue cleanup key".into());
                                        break 'scans;
                                    }
                                };
                                if generation < fence_below {
                                    dead_bytes += kv.key.len();
                                    dead.push(kv.key.to_vec());
                                }
                            }
                            Ok(None) => break,
                            Err(e) => {
                                scan_err = Some(e.to_string());
                                break 'scans;
                            }
                        }
                    },
                    Err(e) => {
                        scan_err = Some(e.to_string());
                        break 'scans;
                    }
                }
            }
        }
        if let Some(e) = scan_err {
            self.effects.queue_acks.push((
                resp,
                Err(format!("consumer delete aborted: state scan failed: {e}")),
            ));
            return;
        }
        if local.queue.state.is_none() {
            local.queue.state = Some(local.handle.state.lock().unwrap().queue.clone());
        }
        let local_dead = matches!(
            local.queue.state.as_ref().unwrap().consumers.get(&consumer),
            Some(cs) if cs.cgen < fence_below
        );
        if local_dead {
            let cs = local
                .queue
                .state
                .as_ref()
                .unwrap()
                .consumers
                .get(&consumer)
                .expect("checked above");
            for off in cs.leases.keys() {
                if dead.len() >= max_rows || dead_bytes >= max_bytes {
                    more = true;
                    break;
                }
                let k = lease_key(&hash, &consumer, cs.cgen, *off);
                dead_bytes += k.len();
                dead.push(k);
            }
            for off in cs.acked.iter() {
                if dead.len() >= max_rows || dead_bytes >= max_bytes {
                    more = true;
                    break;
                }
                let k = ack_key(&hash, &consumer, cs.cgen, *off);
                dead_bytes += k.len();
                dead.push(k);
            }
            if dead.len() < max_rows && dead_bytes < max_bytes {
                let k = cursor_key(&hash, &consumer, cs.cgen);
                dead.push(k);
            } else {
                more = true;
            }
            local
                .queue
                .state
                .as_mut()
                .unwrap()
                .consumers
                .remove(&consumer);
        }
        let deleted_rows = dead.len() as u64;
        if !dead.is_empty() {
            for k in dead {
                self.batch.delete(k);
            }
            self.extra_writes = true;
        }
        self.effects.queue_acks.push((
            resp,
            Ok(QueueOut::DeleteStep {
                complete: !more,
                deleted_rows,
            }),
        ));
    }
}
