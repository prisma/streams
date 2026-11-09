//! The actor of a topology operation (S4-DESIGN §4.2, §4.3; a split, and S6's merge): it renews
//! its intent's lease while the operation runs, by CAS every third of [`intent::LEASE`]; it checks
//! at each step that it still owns the intent (no claimant took it, and its own clock has not passed
//! the lease since its last renewal); and it ends the intent once its renewals stopped, so a renewal
//! never wins against the terminal write.
//!
//! The operation races its steps against [`Actor::renew`] and calls [`Actor::end`] after that race
//! returns; before its point of no return it stops at the first [`Actor::owned`] that fails.

use object_store::ObjectStore;

use super::{intent, recover};

/// The actor's hold on its intent while its operation runs (S4-DESIGN §4.2, §4.3): the intent as it
/// last wrote it, its lease on this process's monotonic clock, and whether a claimant took it.
pub(crate) struct Actor<'a> {
    store: &'a dyn ObjectStore,
    clock: &'a dyn crate::runtime::Clock,
    held: std::sync::Mutex<intent::Held>,
    lease: std::sync::Mutex<intent::Lease>,
    lost: std::sync::atomic::AtomicBool,
}

impl<'a> Actor<'a> {
    pub(crate) fn new(
        store: &'a dyn ObjectStore,
        clock: &'a dyn crate::runtime::Clock,
        held: intent::Held,
    ) -> Self {
        Self {
            store,
            clock,
            held: std::sync::Mutex::new(held),
            lease: std::sync::Mutex::new(intent::Lease::renewed_at(clock.monotonic())),
            lost: std::sync::atomic::AtomicBool::new(false),
        }
    }

    /// The clock the actor's lease runs on.
    pub(crate) fn clock(&self) -> &'a dyn crate::runtime::Clock {
        self.clock
    }

    /// The intent as the actor last wrote it.
    pub(crate) fn held(&self) -> intent::Held {
        self.held
            .lock()
            .map(|held| held.clone())
            .unwrap_or_else(|p| p.into_inner().clone())
    }

    /// Whether the actor still owns its intent (`intent::Lease::owned`): no claimant took it and its
    /// own clock has not passed the lease since its last renewal. Else why not, before `step`.
    pub(crate) fn owned(&self, step: &str) -> Result<(), String> {
        let lost = self.lost.load(std::sync::atomic::Ordering::Acquire);
        let lapsed = self
            .lease
            .lock()
            .is_ok_and(|lease| !lease.owned(self.clock.monotonic()));
        if lost || lapsed {
            return Err(format!("the lease was lost before {step}"));
        }
        Ok(())
    }

    /// Renew the lease every third of it, for as long as the split runs (S4-DESIGN §4.2).
    pub(crate) async fn renew(&self) -> std::convert::Infallible {
        loop {
            self.clock.sleep(intent::LEASE / 3).await;
            self.renew_once().await;
        }
    }

    /// One renewal: `lease_until_ms` CASed forward. A store failure is retried at the next one; a CAS
    /// lost to a write of our own (its reply lost) is adopted; one lost to a claimant ends ownership.
    async fn renew_once(&self) {
        let held = self.held();
        let mut next = held.intent.clone();
        let lease_ms = i64::try_from(intent::LEASE.as_millis()).unwrap_or(i64::MAX);
        next.lease_until_ms = self.clock.now().ms().saturating_add(lease_ms);
        let renewed_at = self.clock.monotonic();
        match intent::replace(self.store, &held, next).await {
            Ok(renewed) => {
                self.keep(renewed);
                if let Ok(mut lease) = self.lease.lock() {
                    *lease = intent::Lease::renewed_at(renewed_at);
                }
            }
            Err(intent::IntentError::Lost) => {
                if !self.adopt(&held).await {
                    self.lost.store(true, std::sync::atomic::Ordering::Release);
                }
            }
            Err(_) => {}
        }
    }

    /// After a lost CAS over `held`: adopt the stored intent if it is still ours (a write of ours
    /// whose reply was lost), live, at our epoch and owner. Whether it was.
    async fn adopt(&self, held: &intent::Held) -> bool {
        let Ok(Some(seen)) = intent::read(self.store).await else {
            return false;
        };
        let ours = seen.intent.lease_epoch == held.intent.lease_epoch
            && seen.intent.lease_owner == held.intent.lease_owner
            && seen.intent.op_id == held.intent.op_id
            && !seen.intent.is_terminal();
        if ours {
            self.keep(seen);
        }
        ours
    }

    fn keep(&self, held: intent::Held) {
        if let Ok(mut kept) = self.held.lock() {
            *kept = held;
        }
    }

    /// End the intent in `state` with `result`, once the renewals stopped: a CAS, re-read and
    /// retried over a write of our own; a claimant's intent is left as it is.
    pub(crate) async fn end(&self, state: intent::State, result: &str) {
        for _ in 0..3 {
            let held = self.held();
            match intent::replace(self.store, &held, recover::terminal(&held, state, result)).await
            {
                Ok(_) => return,
                Err(intent::IntentError::Lost) if !self.adopt(&held).await => return,
                Err(_) => {}
            }
        }
    }
}
