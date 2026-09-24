//! A supervised future a closing runtime refuses (follow-up F-G).
//!
//! A runtime that is shutting down refuses a new task by dropping its future
//! inside `tokio::spawn` itself. `TaskSupervisor::spawn` calls it under the
//! registration lock, so that the phase check and the insertion stay one
//! step and nothing can register after the drain. A supervised future's
//! destructors may re-enter its supervisor: an engine's required task
//! carries a guard whose drop closes the engine, which launches that
//! supervisor's shutdown and takes the same lock. Dropped inline, that is a
//! same-thread deadlock: the worker never returns, and the runtime drop,
//! which joins every worker with no timeout, never finishes. The deadlock
//! is reproduced deterministically by
//! `a_spawn_refused_by_a_closing_runtime_cannot_deadlock_its_supervisor`; it
//! is a candidate cause of the two F-G CI hangs, not a confirmed one (no
//! thread stacks were captured from either hang). So a future dropped during
//! the spawn call is set aside and handed back, and the supervisor drops it
//! after it has released the lock.

use std::any::Any;
use std::cell::RefCell;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

thread_local! {
    /// Futures dropped during the spawn call on this thread, set aside
    /// until the caller releases its lock; `None` outside a spawn call.
    static SET_ASIDE: RefCell<Option<Vec<Box<dyn Any>>>> = const { RefCell::new(None) };
}

/// What a refused spawn left to drop; the caller drops it once it holds no
/// lock the future's destructors could take.
pub(super) struct Refused {
    _set_aside: Vec<Box<dyn Any>>,
}

/// Spawns `future` so that, if the runtime refuses it, its drop is handed
/// back instead of running inside `tokio::spawn`.
pub(super) fn spawn_set_aside<F>(future: F) -> (tokio::task::JoinHandle<F::Output>, Refused)
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    let slot = OpenSlot::open();
    #[expect(
        clippy::disallowed_methods,
        reason = "TaskSupervisor worker owner; the registration lock retains each handle before shutdown can take the map; spawning through another supervisor would recursively delegate this canonical owner"
    )]
    let handle = tokio::spawn(SetAsideOnRefusal(Some(Box::pin(future))));
    (handle, slot.close())
}

/// This thread's set-aside slot, open for exactly one spawn call. It closes
/// on every exit, unwinding included: a slot left open would park every later
/// supervised drop on this thread instead of dropping it.
struct OpenSlot {
    closed: bool,
}

impl OpenSlot {
    fn open() -> Self {
        let stale = SET_ASIDE.with(|slot| slot.borrow_mut().replace(Vec::new()));
        drop(stale);
        Self { closed: false }
    }

    /// What the spawn call set aside, for the caller to drop once it holds no
    /// lock the futures' destructors could take.
    fn close(mut self) -> Refused {
        self.closed = true;
        Refused {
            _set_aside: take_set_aside(),
        }
    }
}

impl Drop for OpenSlot {
    fn drop(&mut self) {
        if !self.closed {
            #[expect(
                clippy::disallowed_methods,
                reason = "F-G slot unwind; the spawn call unwound while its caller holds the registration lock, so the set-aside futures cannot be handed back, and dropping them here runs destructors that may take that lock (the deadlock above); the unwind poisons that lock, so the supervisor is unusable and the leak is bounded to this one failed spawn"
            )]
            std::mem::forget(take_set_aside());
        }
    }
}

fn take_set_aside() -> Vec<Box<dyn Any>> {
    SET_ASIDE
        .with(|slot| slot.borrow_mut().take())
        .unwrap_or_default()
}

/// The supervised future, whose drop during a spawn call is set aside.
struct SetAsideOnRefusal<F: 'static>(Option<Pin<Box<F>>>);

impl<F: Future + 'static> Future for SetAsideOnRefusal<F> {
    type Output = F::Output;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<F::Output> {
        match self.0.as_mut() {
            Some(future) => future.as_mut().poll(cx),
            None => Poll::Pending,
        }
    }
}

impl<F: 'static> Drop for SetAsideOnRefusal<F> {
    fn drop(&mut self) {
        let Some(future) = self.0.take() else {
            return;
        };
        // Outside a spawn call the future is dropped here, after the slot's
        // borrow ends: its destructors may spawn again.
        let unrefused = SET_ASIDE.with(|slot| match slot.borrow_mut().as_mut() {
            Some(set_aside) => {
                set_aside.push(Box::new(future));
                None
            }
            None => Some(future),
        });
        drop(unrefused);
    }
}
