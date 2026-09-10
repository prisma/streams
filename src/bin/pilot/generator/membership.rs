//! The generator's terminal boundary, compiled unchanged with Loom primitives
//! in the model test. An admission is counted before its task can be spawned.
use super::synchronization::{AtomicBool, AtomicU64, Mutex, Ordering};

pub(super) struct Membership {
    admission: Mutex<()>,
    closed: AtomicBool,
    active: AtomicU64,
}

impl Membership {
    pub(super) fn new() -> Self {
        Self {
            admission: Mutex::new(()),
            closed: AtomicBool::new(false),
            active: AtomicU64::new(0),
        }
    }

    #[allow(
        clippy::unwrap_used,
        reason = "Generator admission owns closure and worker membership; a poisoned admission lock cannot publish a reliable final count; recovering it could admit work after a final drain response (test compilation under Loom makes expectations conditional)"
    )]
    pub(super) fn reserve(&self) -> bool {
        let _guard = self.admission.lock().unwrap();
        if self.is_closed() {
            return false;
        }
        self.active.fetch_add(1, Ordering::Relaxed);
        true
    }

    #[allow(
        clippy::unwrap_used,
        reason = "Generator drain owns the terminal admission boundary; closure shares the same lock as membership publication; bypassing poison could publish closed before a late worker is counted (the same source also compiles under Loom)"
    )]
    pub(super) fn close(&self) {
        let _guard = self.admission.lock().unwrap();
        self.closed.store(true, Ordering::Release);
    }

    pub(super) fn is_closed(&self) -> bool {
        self.closed.load(Ordering::Acquire)
    }

    pub(super) fn release(&self) {
        self.active.fetch_sub(1, Ordering::Release);
    }

    /// Call before reading cumulative counters: closed + zero acquires every
    /// completed worker's accounting, and no subsequent worker can be admitted.
    pub(super) fn snapshot(&self) -> (bool, u64) {
        let closed = self.is_closed();
        (closed, self.active.load(Ordering::Acquire))
    }
}
