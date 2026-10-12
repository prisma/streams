//! Test-only absorber start: the DST fixtures drive the absorber through a
//! bare task they abort themselves.
#![cfg(test)]
use super::*;

impl Absorber {
    #[expect(
        clippy::disallowed_methods,
        reason = "Absorber::start; the DST fixtures drive this absorber through a bare task they abort themselves; a supervised spawn would tie a fixture's teardown to a supervisor the fixture never builds"
    )]
    pub(crate) fn start(
        shard: Arc<ShardEngine>,
        cfg: AbsorberConfig,
        rx: mpsc::Receiver<AbsorbSignal>,
    ) -> tokio::task::JoinHandle<()> {
        // L6 spike M4: on the spill arm the fixture gets the sealer at this
        // absorber's cadence; aborting the returned task stops it.
        if shard.spill_adopt_absorber_cadence(cfg.threshold_bytes, cfg.threshold_age, cfg.tick)
            && let Some(task) = shard.spill_cadence_task()
        {
            drop(rx);
            return tokio::spawn(task);
        }
        tokio::spawn(Self::new(shard, cfg).run(rx))
    }
}
