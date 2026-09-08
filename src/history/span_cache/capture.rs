use super::*;
use crate::shard::record::CheckedFrame;

pub(crate) struct CipherSpan {
    frames: Box<[CheckedFrame]>,
    // Also owns the reservation for a completely empty interval.
    _charge: Arc<Lease>,
}
impl CipherSpan {
    pub(crate) fn frames(&self) -> &[CheckedFrame] {
        &self.frames
    }
}
/// A cancellation-safe complete-scan ticket. Pending identity prevents a late
/// completion from publishing across invalidation, retirement or a replacement.
pub(crate) struct Capture {
    inner: Arc<Inner>,
    scope: Arc<Scope>,
    signal: Arc<Signal>,
    from: u64,
    to: u64,
    charge: Arc<Lease>,
    frames: Vec<CheckedFrame>,
    storage: usize,
    last: Option<u64>,
}
impl Capture {
    pub(super) fn new(
        inner: Arc<Inner>,
        scope: Arc<Scope>,
        signal: Arc<Signal>,
        from: u64,
        to: u64,
        charge: Arc<Lease>,
    ) -> Self {
        Self {
            inner,
            scope,
            signal,
            from,
            to,
            charge,
            frames: Vec::new(),
            storage: 0,
            last: None,
        }
    }
    /// Compact each backend owner immediately. Never retain an unknown SST/
    /// object allocation by caching a small Bytes slice. The conservative
    /// transient bound includes old+new Vec growth and final boxing overlap.
    pub(crate) fn push(&mut self, frame: &CheckedFrame) -> bool {
        let off = frame.view().header.offset;
        if off < self.from || off >= self.to || self.last.is_some_and(|p| p >= off) {
            return false;
        }
        let Some(storage) = self.storage.checked_add(frame.len() + 128) else {
            return false;
        };
        let count = self.frames.len() + 1;
        if storage.saturating_mul(2) + count * std::mem::size_of::<CheckedFrame>() * 4 + CONTROL
            > FILL - CONTROL
        {
            return false;
        }
        let bytes = crate::retained_bytes::with_charge(
            frame.as_ref().to_vec().into_boxed_slice(),
            self.charge.clone(),
        );
        self.frames.push(frame.with_compact_owner(bytes));
        self.storage = storage;
        self.last = Some(off);
        true
    }
    /// Called only after canonical EOS, never after withholding or an error.
    pub(crate) fn complete(mut self) {
        let frames = std::mem::take(&mut self.frames).into_boxed_slice();
        self.charge
            .shrink(self.storage + std::mem::size_of_val(frames.as_ref()) + CONTROL);
        let span = Arc::new(CipherSpan {
            frames,
            _charge: self.charge.clone(),
        });
        let mut state = self.inner.state.lock().unwrap_or_else(|e| e.into_inner());
        if self.scope.live()
            && let Some(entry) = self.pending(&mut state)
        {
            entry.value = Value::Ready(span);
            self.signal.done.send_replace(true);
        }
    }
    fn pending<'a>(&self, state: &'a mut State) -> Option<&'a mut Entry> {
        state
            .entries
            .iter_mut()
            .flatten()
            .find(|e| matches!(&e.value, Value::Pending(s) if Arc::ptr_eq(s, &self.signal)))
    }
}
impl Drop for Capture {
    fn drop(&mut self) {
        let mut state = self.inner.state.lock().unwrap_or_else(|e| e.into_inner());
        for slot in state.entries.iter_mut() {
            if slot.as_ref().is_some_and(
                |e| matches!(&e.value, Value::Pending(s) if Arc::ptr_eq(s, &self.signal)),
            ) {
                *slot = None;
                break;
            }
        }
        self.signal.done.send_replace(true);
    }
}
