use super::*;
impl CommitTransaction<'_> {
    /// Local write success permits applied visibility. Remote waiters still
    /// observe the same sequence and dispatch barrier through DurableEffects.
    #[expect(
        clippy::unwrap_used,
        reason = "CommitTransaction::publish; a poisoned handoff or stream state may hold a half-published group; recovering it could expose fields, producers or pages the write never covered"
    )]
    #[expect(
        clippy::cast_possible_truncation,
        reason = "CommitTransaction::publish; the record count is bounded by the group's request count, itself a u32; a checked conversion would only restate that bound"
    )]
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
            local.publish_into(&mut local.handle.state.lock().unwrap());
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
            local.notify_applied();
        }
        self.engine.flush_wake.notify_one();
        self.engine.pump_wake.notify_one();
    }
}
