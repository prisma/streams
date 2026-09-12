use super::*;
impl CommitTransaction<'_> {
    #[expect(
        clippy::too_many_lines,
        reason = "CommitTransaction::receive; the receive walks the cursor once, deciding acked, in-flight, poisoned and blocked offsets in one pass; splitting it would separate the verdicts from the cursor they advance"
    )]
    #[expect(
        clippy::expect_used,
        reason = "CommitTransaction::receive; the queue state was loaded before dispatch; a second fallible read would add a branch no dispatched op reaches"
    )]
    #[expect(
        clippy::excessive_nesting,
        reason = "CommitTransaction::receive; the walk nests each offset's ack, lease and block verdicts inside the cursor loop; flattening them would separate the verdicts from the offset they skip"
    )]
    pub(super) fn receive(
        &mut self,
        local: &mut StreamOverlay,
        hash: [u8; 16],
        op: QueueOp,
        resp: QueueReply,
    ) {
        let QueueOp::Receive {
            consumer,
            cgen,
            max,
            visibility_ms,
            max_deliveries,
            keys,
            covered_to,
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
            let mut leased = Vec::new();
            let mut poisoned: Vec<(u64, u32, u32, [u8; 16])> = Vec::new();
            let mut blocked: std::collections::HashSet<[u8; 16]> = cs
                .leases
                .values()
                .filter(|l| l.deadline_ms > now)
                .map(|l| l.key_hash)
                .collect();
            let mut off = cs.cursor;
            let mut steps = 0usize;
            while off < local.fields.next && leased.len() < max && steps < max * 8 + 4096 {
                steps += 1;
                if off >= covered_to {
                    break;
                }
                let Some(kh) = keys.get(&off).copied() else {
                    break;
                };
                if cs.acked.contains(&off) {
                    off += 1;
                    continue;
                }
                let prev = cs.leases.get(&off).copied();
                if let Some(l) = prev {
                    if l.deadline_ms > now {
                        off += 1;
                        continue; // in flight
                    }
                    if l.delivery_count >= max_deliveries {
                        poisoned.push((off, l.lease_gen, l.delivery_count, kh));
                        blocked.insert(kh);
                        off += 1;
                        continue;
                    }
                }
                if blocked.contains(&kh) {
                    off += 1;
                    continue;
                }
                let lease = Lease {
                    deadline_ms: now + visibility_ms as i64,
                    delivery_count: prev.map(|l| l.delivery_count).unwrap_or(0) + 1,
                    lease_gen: prev.map(|l| l.lease_gen).unwrap_or(0) + 1,
                    key_hash: kh,
                };
                self.batch
                    .put(lease_key(&hash, &consumer, cgen, off), encode_lease(&lease));
                self.extra_writes = true;
                cs.leases.insert(off, lease);
                blocked.insert(kh);
                leased.push((off, lease.lease_gen, lease.delivery_count, kh));
                off += 1;
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
            QueueOut::Received {
                leased,
                backlog,
                poisoned,
            }
        };
        self.effects.queue_acks.push((resp, Ok(out)));
    }
}
