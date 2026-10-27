//! Loading a stream's handle: its durable tail, from whichever segment
//! holds it (layout 6), and the handle built around it.

use super::{
    Arc, HashMap, Mutex, Notify, ShardEngine, StreamHandle, StreamState, TailFields, TailRing,
    now_ms, queue_tail_key, stored_tail, tail_key,
};

impl ShardEngine {
    /// A stream's durable tail and whether it is a native queue's, whose
    /// tail lives in the queue segment: the stream segment is read first,
    /// then the queue segment; a stream with neither has the default tail.
    pub(super) async fn load_tail(
        &self,
        hash: &[u8; 16],
    ) -> Result<(TailFields, bool), slatedb::Error> {
        for (key, native_queue) in [(tail_key(hash), false), (queue_tail_key(hash), true)] {
            if let Some(raw) = self.db.get(key).await? {
                return Ok((stored_tail(&raw)?, native_queue));
            }
        }
        Ok((TailFields::default(), false))
    }

    /// A new handle around a loaded tail. Trim-debt discovery on load: a
    /// stream evicted (or restarted) mid-maintenance re-enters the TrimTick
    /// rotation the moment anything touches it again. The absorber's
    /// startup marker scan covers never-touched streams.
    #[expect(
        clippy::unwrap_used,
        reason = "ShardEngine::new_handle; a poisoned trim-debt set may hold a partially applied maintenance update; recovering it could retire a boundary that was never committed"
    )]
    pub(super) fn new_handle(
        &self,
        hash: [u8; 16],
        (tail, native_queue): (TailFields, bool),
    ) -> Arc<StreamHandle> {
        if tail.trimmed < tail.trim_safe_to {
            self.trim_debt.lock().unwrap().insert(hash);
        }
        Arc::new(StreamHandle {
            owner: Arc::downgrade(&self.db),
            hash,
            state: Mutex::new(StreamState {
                durable: tail.clone(),
                applied: tail,
                producers: HashMap::new(),
                seqs: HashMap::new(),
                queue: crate::queue::QueueState::default(),
            }),
            notify: Notify::new(),
            applied_notify: Notify::new(),
            ring: Mutex::new(TailRing::default()),
            last_touch_ms: now_ms().unsigned_abs().into(),
            pressure: std::sync::OnceLock::new(),
            native_queue: native_queue.into(),
        })
    }
}
