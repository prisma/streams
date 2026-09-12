use super::*;
use crate::queue::*;
mod cleanup;
mod config;
mod load;
mod receive;
mod settle;
struct CleanupBudget {
    rows: usize,
    bytes: usize,
}
type QueueReply = oneshot::Sender<Result<QueueOut, String>>;
impl CommitTransaction<'_> {
    #[expect(
        clippy::too_many_lines,
        reason = "CommitTransaction::queue; queue dispatch resolves the consumer fence once and routes every queue op through the same fence verdict; splitting it would separate the ops from the fence that admits them"
    )]
    #[expect(
        clippy::excessive_nesting,
        reason = "CommitTransaction::queue; the dispatch nests the durable fence read inside the uncached branch of the fence lookup; flattening it would separate the read from the cache it fills"
    )]
    #[expect(
        clippy::unwrap_used,
        reason = "CommitTransaction::queue; a poisoned consumer-fence cache may hold a half-raised generation; recovering it could admit an op the fence already superseded"
    )]
    pub(super) async fn queue(
        &mut self,
        local: &mut StreamOverlay,
        hash: [u8; 16],
        op: QueueOp,
        resp: QueueReply,
    ) {
        #[cfg(test)]
        self.client_append_hashes.insert(hash);
        if let Err(error) = self.load_queue(local, hash).await {
            self.effects.queue_acks.push((resp, Err(error)));
            return;
        }
        let op = match op {
            QueueOp::ConfigPut { consumer, cfg } => {
                self.config_put(local, hash, consumer, cfg, resp).await;
                return;
            }
            QueueOp::ConfigGet { consumer } => {
                self.config_get(local, hash, consumer, resp).await;
                return;
            }
            QueueOp::ConfigLifecycle {
                consumer,
                expect_gen,
                deleting,
            } => {
                self.config_lifecycle(local, hash, consumer, expect_gen, deleting, resp)
                    .await;
                return;
            }
            QueueOp::ConfigDeleteStep {
                consumer,
                fence_below,
                max_rows,
                max_bytes,
            } => {
                self.delete_step(
                    local,
                    hash,
                    consumer,
                    fence_below,
                    CleanupBudget {
                        rows: max_rows,
                        bytes: max_bytes,
                    },
                    resp,
                )
                .await;
                return;
            }
            op => op,
        };
        if let Some((cname, op_gen)) = match &op {
            QueueOp::Receive { consumer, cgen, .. } | QueueOp::Settle { consumer, cgen, .. } => {
                Some((consumer.clone(), *cgen))
            }
            _ => None,
        } {
            let fenced = {
                let cached = {
                    let f = self.engine.consumer_fences.lock().unwrap();
                    f.get(&(hash, cname.clone())).copied()
                };
                match cached {
                    Some(min_live) => op_gen < min_live,
                    None => {
                        let fk = crate::queue::fence_key(&hash, &cname);
                        let durable = match self.engine.db.get(&fk[..]).await {
                            Ok(Some(v)) => match decode_counter(&v) {
                                Ok(value) => value,
                                Err(e) => {
                                    self.effects.queue_acks.push((
                                        resp,
                                        Err(format!("consumer_fence_unverified: {e}")),
                                    ));
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
                        self.engine
                            .consumer_fences
                            .lock()
                            .unwrap()
                            .insert((hash, cname.clone()), durable);
                        op_gen < durable
                    }
                }
            };
            if fenced {
                self.effects.queue_acks.push((
                    resp,
                    Err(format!(
                        "consumer_generation_fenced: generation {op_gen} was deleted"
                    )),
                ));
                return;
            }
            if let Some(staged) = local.queue.configs.get(&cname)
                && (staged.state != ConsumerLifecycle::Active || staged.generation != op_gen)
            {
                self.effects.queue_acks.push((
                    resp,
                    Err(format!(
                        "consumer_not_found: generation {op_gen} is not the \
                 active record in this commit group"
                    )),
                ));
                return;
            }
        }

        match op {
            QueueOp::Receive { .. } => self.receive(local, hash, op, resp),
            QueueOp::Settle { .. } => self.settle(local, hash, op, resp),
            _ => unreachable!("configuration already handled"),
        }
    }
}
