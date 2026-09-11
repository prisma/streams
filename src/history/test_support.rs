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
        data_store: Arc<dyn ObjectStore>,
        shard: Arc<ShardEngine>,
        keys: Arc<KeyCache>,
        cfg: AbsorberConfig,
        rx: mpsc::Receiver<AbsorbSignal>,
    ) -> tokio::task::JoinHandle<()> {
        tokio::spawn(Self::new(data_store, shard, keys, cfg).run(rx))
    }
}
