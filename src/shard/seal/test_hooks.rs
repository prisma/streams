//! Tests: where a seal stops, and the faults and controls a test injects.
#![cfg(test)]

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use super::{SealError, SealState};

/// Where a test stops a seal: after step 1, after step 4, after step 6
/// (applied, not yet durable), after step 7a (durable and published,
/// before the memtable flush).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum SealStep {
    WalFlushed,
    Put,
    Applied,
    Durable,
}

/// A gate a seal parks at until released.
pub(crate) struct Hold {
    pub(crate) entered: AtomicU64,
    release: tokio::sync::watch::Sender<bool>,
}

impl Hold {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self {
            entered: AtomicU64::new(0),
            release: tokio::sync::watch::Sender::new(false),
        })
    }

    pub(crate) fn release(&self) {
        self.release.send_replace(true);
    }

    pub(crate) async fn park(&self) {
        self.entered.fetch_add(1, Ordering::AcqRel);
        let mut released = self.release.subscribe();
        released
            .wait_for(|r| *r)
            .await
            .expect("the hold owns its release sender");
    }
}

#[derive(Default)]
pub(crate) struct SealHooks {
    pub(crate) abort_at: Mutex<Option<SealStep>>,
    /// Fail every object PUT, or hold it this long first.
    pub(crate) fail_puts: AtomicBool,
    pub(crate) put_delay: Mutex<Duration>,
    /// Park a seal after its PUT, before its commit.
    pub(crate) before_commit: Mutex<Option<Arc<Hold>>>,
    /// Park every sealed-range GET.
    pub(crate) get_hold: Mutex<Option<Arc<Hold>>>,
    /// Park a read after it captured its boundary and unsealed pages.
    pub(crate) window_hold: Mutex<Option<Arc<Hold>>>,
    /// Park a handle load after it read the stream's tail.
    pub(crate) load_hold: Mutex<Option<Arc<Hold>>>,
    /// Park every durable dispatch before it takes the dispatch gate.
    pub(crate) dispatch_hold: Mutex<Option<Arc<Hold>>>,
    /// Skip every durable dispatch (the stall check's control).
    pub(crate) skip_dispatch: AtomicBool,
    /// The age clock's negative control: after a seal, count only durable
    /// unsealed pages as left over (the behaviour before S16c).
    pub(crate) durable_only_leftover: AtomicBool,
    /// REVIEW-6A F1 negative control: the committer puts the remainder a
    /// seal task would have pre-built from the pages its snapshot knew,
    /// instead of the live one.
    pub(crate) prebuilt_remainder: AtomicBool,
    /// The race DST's negative control: publish the directory entries only
    /// after the commit is durable.
    pub(crate) late_directory: AtomicBool,
    /// REVIEW-6A F2 control: skip step 7a.
    pub(crate) skip_7a: AtomicBool,
    /// Fail the next commit group that stages a seal (R25-D).
    pub(crate) fail_next_group: AtomicBool,
    /// Seal commits handed to the committer.
    pub(crate) commits_sent: AtomicU64,
    /// Bytes the sealed-range GETs asked for (HIS-018's amplification bound).
    pub(crate) get_bytes: AtomicU64,
}

impl SealState {
    pub(super) fn check_abort(&self, step: SealStep) -> Result<(), SealError> {
        if *self.hooks.abort_at.lock().unwrap() == Some(step) {
            return Err(SealError::Aborted(step));
        }
        Ok(())
    }

    pub(super) async fn before_put(&self) -> Result<(), SealError> {
        let delay = *self.hooks.put_delay.lock().unwrap();
        if !delay.is_zero() {
            tokio::time::sleep(delay).await;
        }
        if self.hooks.fail_puts.load(Ordering::Relaxed) {
            return Err(SealError::Store("injected PUT failure".into()));
        }
        Ok(())
    }

    pub(super) async fn before_commit(&self) {
        let hold = self.hooks.before_commit.lock().unwrap().clone();
        if let Some(hold) = hold {
            hold.park().await;
        }
    }

    pub(crate) async fn before_get(&self) {
        let hold = self.hooks.get_hold.lock().unwrap().clone();
        if let Some(hold) = hold {
            hold.park().await;
        }
    }

    pub(crate) async fn before_window_read(&self) {
        let hold = self.hooks.window_hold.lock().unwrap().clone();
        if let Some(hold) = hold {
            hold.park().await;
        }
    }

    /// Park or skip a durable dispatch; true when it is skipped.
    pub(crate) async fn before_dispatch(&self) -> bool {
        let hold = self.hooks.dispatch_hold.lock().unwrap().clone();
        if let Some(hold) = hold {
            hold.park().await;
        }
        self.hooks.skip_dispatch.load(Ordering::Relaxed)
    }

    pub(crate) async fn after_tail_load(&self) {
        let hold = self.hooks.load_hold.lock().unwrap().clone();
        if let Some(hold) = hold {
            hold.park().await;
        }
    }

    /// The age clock's negative control: whether `st` counts as left over
    /// when only durable unsealed pages do, if the control is on.
    pub(super) fn durable_only_leftover(&self, st: &crate::shard::StreamState) -> Option<bool> {
        self.hooks
            .durable_only_leftover
            .load(Ordering::Relaxed)
            .then(|| st.durable.absorbed.lt(&st.durable.next))
    }

    pub(super) fn late_directory(&self) -> bool {
        self.hooks.late_directory.load(Ordering::Relaxed)
    }

    /// Whether a flush its caller wants with step 7a (`wanted`) takes it:
    /// the F2 control skips it.
    pub(super) fn takes_7a(&self, wanted: bool) -> bool {
        wanted && !self.hooks.skip_7a.load(Ordering::Relaxed)
    }

    pub(crate) fn prebuilt_remainder(&self) -> bool {
        self.hooks.prebuilt_remainder.load(Ordering::Relaxed)
    }
}
