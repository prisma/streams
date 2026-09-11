use super::*;
impl CommitTransaction<'_> {
    #[expect(
        clippy::too_many_lines,
        reason = "CommitTransaction::settle; settlement applies acks, retries and extends against one loaded lease table; splitting it would separate the outcomes from the leases they settle"
    )]
    #[expect(
        clippy::expect_used,
        reason = "CommitTransaction::settle; the queue state was loaded before dispatch; a second fallible read would add a branch no dispatched op reaches"
    )]
    #[expect(
        clippy::excessive_nesting,
        reason = "CommitTransaction::settle; the settlement nests each token's generation check inside the ack, retry and extend loops; flattening them would separate the stale verdicts from the lease they protect"
    )]
    pub(super) fn settle(
        &mut self,
        local: &mut StreamOverlay,
        hash: [u8; 16],
        op: QueueOp,
        resp: QueueReply,
    ) {
        let QueueOp::Settle {
            consumer,
            cgen,
            acks,
            retries,
            extends,
            max_deliveries,
        } = op
        else {
            unreachable!("typed delivery dispatch")
        };
        let now = now_ms();
        let out = {
            let cs = local
                .queue
                .state
                .as_mut()
                .expect("loaded")
                .consumers
                .entry(consumer.clone())
                .or_default();
            match decide_consumer_generation(cs.cgen, cgen) {
                ConsumerGeneration::Bind => cs.cgen = cgen,
                ConsumerGeneration::Reset => {
                    *cs = ConsumerState {
                        cgen,
                        ..Default::default()
                    }
                }
                ConsumerGeneration::Continue => {}
                ConsumerGeneration::Fenced { current } => {
                    self.effects.queue_acks.push((
                        resp,
                        Err(format!(
                            "consumer_generation_fenced: generation {cgen} superseded by {current}"
                        )),
                    ));
                    return;
                }
            }
            let (mut a, mut r, mut e2, mut dq, mut stale) =
                (0usize, 0usize, 0usize, 0usize, 0usize);
            let mut poisoned: Vec<(u64, u32, u32, [u8; 16])> = Vec::new();
            for (off, tok_gen) in acks {
                if cs.leases.get(&off).map(|l| l.lease_gen) == Some(tok_gen) {
                    cs.leases.remove(&off);
                    self.batch.delete(lease_key(&hash, &consumer, cgen, off));
                    cs.acked.insert(off);
                    self.batch.put(ack_key(&hash, &consumer, cgen, off), b"");
                    self.extra_writes = true;
                    a += 1;
                } else {
                    stale += 1;
                }
            }
            for (off, tok_gen, delay) in retries {
                if let Some(l) = cs.leases.get(&off).copied() {
                    if l.lease_gen != tok_gen {
                        stale += 1;
                        continue;
                    }
                    if l.delivery_count >= max_deliveries {
                        poisoned.push((off, l.lease_gen, l.delivery_count, l.key_hash));
                        dq += 1;
                        continue;
                    } else {
                        let nl = Lease {
                            deadline_ms: now + delay as i64,
                            ..l
                        };
                        cs.leases.insert(off, nl);
                        self.batch
                            .put(lease_key(&hash, &consumer, cgen, off), encode_lease(&nl));
                        r += 1;
                    }
                    self.extra_writes = true;
                } else {
                    stale += 1;
                }
            }
            for (off, tok_gen, vis) in extends {
                if let Some(l) = cs.leases.get(&off).copied() {
                    if l.lease_gen == tok_gen {
                        let nl = Lease {
                            deadline_ms: now + vis as i64,
                            ..l
                        };
                        cs.leases.insert(off, nl);
                        self.batch
                            .put(lease_key(&hash, &consumer, cgen, off), encode_lease(&nl));
                        self.extra_writes = true;
                        e2 += 1;
                    } else {
                        stale += 1;
                    }
                } else {
                    stale += 1;
                }
            }
            while cs.acked.remove(&cs.cursor) {
                self.batch
                    .delete(ack_key(&hash, &consumer, cgen, cs.cursor));
                cs.cursor += 1;
                self.extra_writes = true;
            }
            self.batch
                .put(cursor_key(&hash, &consumer, cgen), cs.cursor.to_le_bytes());
            let backlog = (local.fields.next - cs.cursor).saturating_sub(cs.acked.len() as u64);
            QueueOut::Settled {
                acked: a,
                retried: r,
                extended: e2,
                dlq: dq,
                backlog,
                stale,
                poisoned,
            }
        };
        self.effects.queue_acks.push((resp, Ok(out)));
    }
}
