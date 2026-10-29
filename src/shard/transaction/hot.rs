//! The rows a group leaves on its streams' resident handles
//! (`shard::resident`).
use super::CommitTransaction;

impl CommitTransaction<'_> {
    /// Leave every staged stream's consumer records on its handle (its
    /// billing meta rides its applied tail, layout 6 f3). Called once the
    /// group can no longer be refused before its write: a failed write
    /// retires the engine and its handles with it.
    pub(super) fn keep_hot_rows(&self) {
        for local in self.streams.values() {
            let configs = &local.queue.configs;
            local.handle.hot(|hot| hot.keep(configs));
        }
    }
}
