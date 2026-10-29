//! Loading a stream's handle: its durable tail, from whichever segment
//! holds it (layout 6), and the handle built around it.

use super::{Arc, ShardEngine, StreamHandle, TailFields, queue_tail_key, stored_tail, tail_key};
use crate::spill::pending::{PendingPages, pending_key};

/// A stream's stored state as its handle is built from it: its durable
/// tail, whether it is a native queue's (whose tail lives in the queue
/// segment), and its unsealed pages.
pub(super) struct LoadedTail {
    tail: TailFields,
    native_queue: bool,
    pending: PendingPages,
}

impl ShardEngine {
    /// A stream's stored state: its tail from the stream segment, else the
    /// queue segment (the default tail when neither holds one), and, for a
    /// stream with unsealed records, its pending row, whose pages must tile
    /// `[absorbed, next)` exactly or the load fails (layout 6). Both are read
    /// from one snapshot: a seal that commits between two plain reads would
    /// pair the tail from before it with the pending row from after it.
    pub(super) async fn load_tail(&self, hash: &[u8; 16]) -> Result<LoadedTail, slatedb::Error> {
        let snapshot = self.db.snapshot().await?;
        for (key, native_queue) in [(tail_key(hash), false), (queue_tail_key(hash), true)] {
            let Some(raw) = snapshot.get(key).await? else {
                continue;
            };
            let tail = stored_tail(&raw)?;
            #[cfg(test)]
            self.layout.seal.after_tail_load().await;
            let pending = if native_queue || tail.absorbed == tail.next {
                PendingPages::default()
            } else {
                let raw = snapshot.get(pending_key(hash)).await?.unwrap_or_default();
                PendingPages::tiled(&raw, tail.absorbed, tail.next).map_err(slatedb::Error::data)?
            };
            return Ok(LoadedTail {
                tail,
                native_queue,
                pending,
            });
        }
        Ok(LoadedTail {
            tail: TailFields::default(),
            native_queue: false,
            pending: PendingPages::default(),
        })
    }

    /// A new handle around a loaded stream. A load holds nothing for the
    /// sealer: two loads of one stream may race, and only the handle the
    /// resident map keeps may be held. The sealer's open holds the handle it
    /// is served for every stream with unsealed pages, and the committer the
    /// handle it writes pages to, so a held stream is never evicted until a
    /// seal takes its pages.
    pub(super) fn new_handle(&self, hash: [u8; 16], loaded: LoadedTail) -> Arc<StreamHandle> {
        let LoadedTail {
            tail,
            native_queue,
            pending,
        } = loaded;
        self.fresh_handle(hash, tail, native_queue, pending)
    }
}

impl StreamHandle {
    /// Publish a durable group's tail of this stream, and wake its durable
    /// waiters unless the group's only op on it was a seal (`wake` false),
    /// which changes nothing they wait for (layout 6).
    #[expect(
        clippy::unwrap_used,
        reason = "StreamHandle::publish_durable; a poisoned stream state may hold a half-published tail; recovering it could serve a frontier no group made durable, whereas panicking ends durable dispatch, which retires the engine, and the next open reloads the durable tail from the store"
    )]
    pub(super) fn publish_durable(&self, fields: &TailFields, wake: bool) {
        self.state.lock().unwrap().durable = fields.clone();
        if wake {
            self.notify.notify_waiters();
        }
    }
}
