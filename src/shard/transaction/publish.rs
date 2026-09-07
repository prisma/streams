use super::*;
impl CommitTransaction<'_> {
    /// Local write success permits applied visibility. Remote waiters still
    /// observe the same sequence and dispatch barrier through DurableEffects.
    pub(super) fn publish(
        self,
        sequence: u64,
        maintenance_after: Option<ShardMaintenance>,
        encode_us: u32,
        write_us: u32,
    ) {
        let mut handoff = self.engine.in_flight.lock().unwrap();
        let Some(pending) = handoff.publication() else {
            drop(handoff);
            // Storage may have accepted the batch. Its replacement recovers
            // canonical rows; the retired incarnation publishes no new live
            // mirrors, accounting, rings or success. Outcome remains unknown.
            self.effects.reject(AppendErr::Moved);
            return;
        };
        if let Some(m) = maintenance_after {
            self.engine.publish_maintenance(m);
        }
        INGEST_FRAME_BYTES_TOTAL
            .fetch_add(self.maintenance_added, std::sync::atomic::Ordering::Relaxed);
        ABSORBED_FRAME_BYTES_TOTAL.fetch_add(
            self.maintenance_retired,
            std::sync::atomic::Ordering::Relaxed,
        );
        for local in self.streams.values() {
            let mut st = local.handle.state.lock().unwrap();
            st.applied = local.fields.clone();
            if let Some(b) = local.handle.pressure.get() {
                b.frames_added(local.frames.added_bytes);
                b.frames_retired(local.frames.retired_bytes);
            }
            for (plane, v) in &local.producer.rows {
                st.producers.insert(plane.clone(), *v);
            }
            for (kh, v) in &local.producer.seqs {
                st.seqs.insert(*kh, v.clone());
            }
            if let Some(q) = &local.queue.state {
                st.queue = (*q).clone();
            }
        }
        {
            let mut debt = self.engine.trim_debt.lock().unwrap();
            for (hash, local) in &self.streams {
                if local.fields.trimmed < local.fields.trim_safe_to {
                    debt.insert(*hash);
                } else {
                    debt.remove(hash);
                }
            }
        }
        let trim_used = self.cfg.trim_global_budget.saturating_sub(self.trim_budget);
        if trim_used > 0 {
            self.engine
                .trim_deletes_last
                .store(trim_used, Ordering::Relaxed);
            self.engine
                .trim_deletes_max_batch
                .fetch_max(trim_used, Ordering::Relaxed);
            self.engine
                .trim_deletes_total
                .fetch_add(trim_used, Ordering::Relaxed);
        }
        self.engine
            .stats_appended
            .fetch_add(self.stats.records, Ordering::Relaxed);

        crate::history::INGEST_BYTES_TOTAL.fetch_add(self.stats.appended_bytes, Ordering::Relaxed);
        pending.push(InFlightGroup {
            seq: sequence,
            written_at: std::time::Instant::now(),
            queue_wait_us: self.queue_wait_us,
            encode_us,
            write_us,
            reqs: self.reqs,
            records_n: self.stats.records as u32,
            bytes: self.stats.appended_bytes,
            effects: self.effects,
        });
        drop(handoff);
        for local in self.streams.values() {
            local.handle.applied_notify.notify_waiters();
        }
        self.engine.flush_wake.notify_one();
        self.engine.pump_wake.notify_one();
    }
}
