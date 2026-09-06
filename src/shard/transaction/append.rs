use super::*;
impl CommitTransaction<'_> {
    pub(super) async fn append(
        &mut self,
        local: &mut StreamOverlay,
        hash: [u8; 16],
        req: AppendReq,
    ) {
        #[cfg(test)]
        self.client_append_hashes.insert(hash);
        if let Some(pr) = &req.producer {
            let plane = (req.key_hash, pr.id.clone());
            if !local.producer.rows.contains_key(&plane) {
                let shared = {
                    let st = local.handle.state.lock().unwrap();
                    st.producers.get(&plane).copied()
                };
                let loaded = match shared {
                    Some(v) => Some(v),
                    None => match self
                        .engine
                        .load_producer_chain(&hash, &req.producer_lineage, &req.key_hash, &pr.id)
                        .await
                    {
                        Ok(v) => v,
                        Err(e) => {
                            let _ = req.resp.send(Err(AppendErr::Internal(e.to_string())));
                            return;
                        }
                    },
                };
                if let Some(v) = loaded {
                    local.producer.rows.insert(plane, v);
                }
            }
        }
        // Duplicate decisions precede close, body, sequence and seal-fence checks.
        // Their replies depend on this group or a prior applied durability barrier.
        let prod_echo = if let Some(pr) = &req.producer {
            let current = local
                .producer
                .rows
                .get(&(req.key_hash, pr.id.clone()))
                .copied();
            match decide_producer(pr, current, &local.fields, req.sealed_reject_new) {
                ProducerDecision::Accept(echo) => Some(echo),
                ProducerDecision::Reply(reply) => {
                    self.effects.acks.push((req.resp, reply));
                    return;
                }
            }
        } else {
            None
        };
        if local.fields.closed {
            if (req.finish == AppendFinish::Close)
                && req.entries.is_empty()
                && req.producer.is_none()
            {
                self.effects.acks.push((
                    req.resp,
                    Ok(AppendAck {
                        last_offset: local.fields.next.wrapping_sub(1),
                        next_offset: local.fields.next,
                        closed: true,
                        producer: None,
                        duplicate: false,
                    }),
                ));
            } else {
                self.effects.acks.push((
                    req.resp,
                    Err(AppendErr::Closed {
                        next_offset: local.fields.next,
                    }),
                ));
            }
            return;
        }
        if let Some(d) = &req.deferred_error {
            let _ = req.resp.send(Err(match d {
                DeferredErr::CtMismatch => AppendErr::CtMismatch,
                DeferredErr::BadBody(m) => AppendErr::BadBody(m.clone()),
            }));
            return;
        }
        if let Some(seq) = &req.seq {
            if !local.producer.seqs.contains_key(&req.key_hash) {
                let shared = {
                    let st = local.handle.state.lock().unwrap();
                    st.seqs.get(&req.key_hash).cloned()
                };
                let loaded = match shared {
                    Some(v) => Some(v),
                    None => match self
                        .engine
                        .load_seq_chain(&hash, &req.producer_lineage, &req.key_hash)
                        .await
                    {
                        Ok(v) => v,
                        Err(e) => {
                            let _ = req.resp.send(Err(AppendErr::Internal(e.to_string())));
                            return;
                        }
                    },
                };
                if let Some(v) = loaded {
                    local.producer.seqs.insert(req.key_hash, v);
                }
            }
            if let Some(cur) = local.producer.seqs.get(&req.key_hash)
                && seq <= cur
            {
                self.effects.acks.push((
                    req.resp,
                    Err(AppendErr::SeqConflict {
                        current: Some(cur.clone()),
                    }),
                ));
                return;
            }
        }
        if req.seal_gen.is_some() || (req.finish == AppendFinish::Close) {
            let fence = self
                .engine
                .seal_fences
                .lock()
                .unwrap()
                .get(&hash)
                .copied()
                .unwrap_or(0);
            let stale = !seal_authorized(req.seal_gen, req.finish == AppendFinish::Close, fence);
            if stale {
                let _ = req.resp.send(Err(AppendErr::SealSuperseded));
                return;
            }
        }
        self.accept_append(local, hash, req, prod_echo);
    }
    fn accept_append(
        &mut self,
        local: &mut StreamOverlay,
        hash: [u8; 16],
        req: AppendReq,
        prod_echo: Option<(u64, u64)>,
    ) {
        if let Some(pr) = &req.producer {
            let commit_last = if req.entries.is_empty() {
                local.fields.next.wrapping_sub(1)
            } else {
                local.fields.next + req.entries.len() as u64 - 1
            };
            let rhash = pr.request_hash.unwrap_or([0u8; 16]);
            local.producer.rows.insert(
                (req.key_hash, pr.id.clone()),
                (pr.epoch, pr.seq, commit_last, rhash),
            );
            let mut v = Vec::with_capacity(40);
            v.extend_from_slice(&pr.epoch.to_le_bytes());
            v.extend_from_slice(&pr.seq.to_le_bytes());
            v.extend_from_slice(&commit_last.to_le_bytes());
            v.extend_from_slice(&rhash);
            self.batch
                .put(producer_key(&hash, &req.key_hash, &pr.id), v);
        }
        if req.finish == AppendFinish::Close {
            local.fields.closed = true;
        }
        if req.entries.is_empty() {
            self.effects.acks.push((
                req.resp,
                Ok(AppendAck {
                    last_offset: local.fields.next.wrapping_sub(1),
                    next_offset: local.fields.next,
                    closed: local.fields.closed,
                    producer: prod_echo,
                    duplicate: false,
                }),
            ));
            return;
        }
        let ts = req.ts_hint_ms.unwrap_or_else(now_ms).max(local.fields.ts);
        let start = local.fields.next;
        let cipher =
            crate::crypto::FrameCipher::new(&req.subkey, &hash, self.cfg.frame_compression);
        let usage = req.usage.clone();
        let (mut pt_sum, mut frame_sum) = (0u64, 0u64);
        for (i, payload) in req.entries.iter().enumerate() {
            let offset = start + i as u64;
            let frame = cipher.encrypt(
                &hash,
                offset,
                ts,
                req.key_version,
                &req.routing_key,
                payload,
            );
            pt_sum += payload.len() as u64;
            frame_sum += frame.len() as u64;
            let frame = Bytes::from(frame);
            if self.engine.ring_enabled {
                local.frames.ring.push((offset, frame.clone()));
            }
            local.fields.unabsorbed_bytes += frame.len() as u64;
            local.frames.added_bytes += frame.len() as u64;
            self.batch.put(record_key(&hash, offset), frame);
            local.fields.logical += payload.len() as u64;
            local.frames.payload_bytes += payload.len() as u64;
        }
        self.effects.usage.push((usage, pt_sum, frame_sum));
        self.bill_append(local, &req, pt_sum, frame_sum);
        self.stats.records += req.entries.len() as u64;
        local.fields.next = start + req.entries.len() as u64;
        local.fields.ts = ts;
        if local.fields.route == [0u8; 16] && req.route != [0u8; 16] && !local.fields.history_v2 {
            local.fields.route = req.route;
        }
        if let Some(seq) = &req.seq {
            local.fields.seq = Some(seq.clone());
            local.producer.seqs.insert(req.key_hash, seq.clone());
            self.batch
                .put(seq_key(&hash, &req.key_hash), seq.clone().into_bytes());
        }
        if let Some(mut t) = req.touch {
            t.next_offset = local.fields.next;
            self.effects.touches.push(t);
        }
        self.effects.acks.push((
            req.resp,
            Ok(AppendAck {
                last_offset: local.fields.next - 1,
                next_offset: local.fields.next,
                closed: local.fields.closed,
                producer: prod_echo,
                duplicate: false,
            }),
        ));
    }
    fn bill_append(
        &mut self,
        local: &mut StreamOverlay,
        req: &AppendReq,
        pt_sum: u64,
        frame_sum: u64,
    ) {
        if let Some(bref) = &req.billing {
            if local.billing.meta.is_none() {
                let loaded = crate::billing::SegmentBillingMetaV1::default();
                local.billing.meta = Some(loaded);
            }
            let bm = local.billing.meta.as_mut().unwrap();
            if bm.stream_id.is_empty() {
                bm.v = 1;
                bm.account_id = bref.identity.account_id.clone();
                bm.project_id = bref.identity.project_id.clone();
                bm.stream_id = bref.identity.stream_id.clone();
                bm.stream_name = bref.identity.stream_name.clone();
                bm.segment_id = bref.segment_id;
            }
            let finals = &mut local.billing.month_finals;
            let bts = crate::billing::billing_now_ms();
            let finals_before = finals.len();
            bm.advance_storage_clock(bts, |closed| {
                finals.push(closed.to_snapshot(true));
            });
            if finals.len() > finals_before && bm.account_id != bref.identity.account_id {
                bm.account_id = bref.identity.account_id.clone();
            }
            bm.ingest_payload_bytes_total += pt_sum;
            bm.ingest_records_total += req.entries.len() as u64;
            bm.month_ingest_payload_bytes += pt_sum;
            bm.month_ingest_records += req.entries.len() as u64;
            bm.owned_frame_bytes_current += frame_sum;
            bm.usage_version += 1;
            local.billing.dirty = true;
        }
    }
}
