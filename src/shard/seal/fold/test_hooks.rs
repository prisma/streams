//! Tests: where a fold or merge stops, and the faults a test injects.
#![cfg(test)]

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use super::{FoldError, FoldState};

/// Where a test stops a fold or merge: after its PUT, after the read-back,
/// after its batch is durable (before the set and directory change).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum FoldStep {
    Put,
    Verified,
    Committed,
}

#[derive(Default)]
pub(crate) struct FoldHooks {
    pub(crate) abort_at: Mutex<Option<FoldStep>>,
    pub(crate) fail_puts: AtomicBool,
    /// Fail the next index batch before it reaches the log.
    pub(crate) fail_next_batch: AtomicBool,
    /// Park every index batch before it reaches the log.
    pub(crate) batch_hold: Mutex<Option<Arc<crate::shard::seal::test_hooks::Hold>>>,
}

impl FoldState {
    pub(super) fn check_abort(&self, step: FoldStep) -> Result<(), FoldError> {
        if *self.hooks.abort_at.lock().unwrap() == Some(step) {
            return Err(FoldError::Aborted(step));
        }
        Ok(())
    }

    pub(super) fn before_put(&self) -> Result<(), FoldError> {
        if self.hooks.fail_puts.load(Ordering::Relaxed) {
            return Err(FoldError::Store("injected PUT failure".into()));
        }
        Ok(())
    }

    pub(super) async fn before_batch(&self) -> Result<(), FoldError> {
        let hold = self.hooks.batch_hold.lock().unwrap().clone();
        if let Some(hold) = hold {
            hold.park().await;
        }
        if self.hooks.fail_next_batch.swap(false, Ordering::Relaxed) {
            return Err(FoldError::Store("injected batch failure".into()));
        }
        Ok(())
    }
}
