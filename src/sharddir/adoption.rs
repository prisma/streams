//! A customer's resolution through the open gate counts as adoption (R29)
//! at the moment the open is published, never after the caller wakes.
//!
//! The billing sweep's discovery open and a customer's request can join one
//! single-flight open, and both wake from the same publication. Stamping
//! after `get_or_open` returned left the sweep free to install custody,
//! probe and retire the engine before the customer's task ran again, which
//! then held a retired engine. So a customer registers its interest in the
//! prefix's gate state for as long as it waits: `PrefixGate::published`
//! stamps any engine published under that interest before a subscriber
//! wakes, and a customer whose open was already published stamps under the
//! serving map's read guard, which the sweep's close orders against, and
//! only while the engine it was handed is still the resident.

use std::sync::Arc;
use std::time::Duration;

use super::{OpenGate, OpenOutcome, closing_outcome};

/// One customer's interest in a prefix's open, counted in the gate state
/// from before it joins the gate until its call ends or is cancelled.
struct ExternalInterest<'a> {
    gate: &'a OpenGate,
    prefix: &'a str,
}

impl Drop for ExternalInterest<'_> {
    fn drop(&mut self) {
        // Never a panic here: this guard drops while a panic under the gate
        // state unwinds, and a second panic would abort the process. A
        // poisoned gate state has already failed the gate (every site that
        // decides with it unwraps it, `publish_open`, the count's only
        // reader, among them), so the interest needs no release then; this
        // recovers nothing and decides nothing.
        if let Ok(mut st) = self.gate.inner.st.lock()
            && let Some(gate) = st.get_mut(self.prefix)
        {
            gate.external_waiters = gate.external_waiters.saturating_sub(1);
        }
    }
}

impl OpenGate {
    /// `get_or_open` for a resolution that is (`external`) or is not a
    /// customer's: an external caller's engine is adopted before the sweep
    /// can take custody of it, and an external caller is never handed an
    /// engine that was retired before it resumed; it gets the retired
    /// prefix's `shard_closing` refusal instead.
    #[expect(
        clippy::unwrap_used,
        reason = "OpenGate::get_or_open_as; a poisoned gate state or serving map may hold a half-recorded open, retirement or holdoff; recovering either could adopt, serve or refuse the wrong incarnation"
    )]
    pub(crate) async fn get_or_open_as(
        &self,
        prefix: &str,
        wait: Duration,
        external: bool,
    ) -> OpenOutcome {
        if !external {
            return self.get_or_open(prefix, wait).await;
        }
        self.inner
            .st
            .lock()
            .unwrap()
            .entry(prefix.to_string())
            .or_default()
            .external_waiters += 1;
        let interest = ExternalInterest { gate: self, prefix };
        let outcome = self.get_or_open(prefix, wait).await;
        // The interest ends with the wait. Its release takes the gate state,
        // so it comes before the serving map is read, never under it.
        drop(interest);
        let OpenOutcome::Ready(engine) = outcome else {
            return outcome;
        };
        let shards = self.inner.shards.read().unwrap();
        if shards
            .get(prefix)
            .is_some_and(|resident| Arc::ptr_eq(&resident.engine, &engine))
        {
            crate::billing::stamp_external(&engine);
            return OpenOutcome::Ready(engine);
        }
        closing_outcome()
    }
}
