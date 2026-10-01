//! The `error` field of every tracing event on this thread, for tests of
//! diagnostics that have no other observer: a retried source read's cause
//! reaches operators only through its log.
#![cfg(test)]
use std::sync::{Arc, Mutex};
use tracing_subscriber::layer::{Context, SubscriberExt};

/// Captures while alive; the thread's previous subscriber returns on drop.
pub(crate) struct ErrorLog {
    causes: Arc<Mutex<Vec<String>>>,
    _guard: tracing::subscriber::DefaultGuard,
    _peer: tracing::Dispatch,
}

impl ErrorLog {
    pub(crate) fn capture() -> Self {
        // tracing caches a callsite's interest when the callsite is first hit.
        // While one dispatcher is registered it asks only the hitting thread's
        // default, so another test's thread, with no subscriber, that first
        // hits a callsite while this capture is the only dispatcher caches
        // `never` for it, and this capture misses that callsite's events. A
        // second registered dispatcher, interested in nothing and alive as long
        // as the capture, makes tracing ask every live dispatcher instead.
        let peer = tracing::Dispatch::new(tracing::subscriber::NoSubscriber::new());
        let causes = Arc::new(Mutex::new(Vec::new()));
        let layer = ErrorFields(Arc::clone(&causes));
        let guard = tracing::subscriber::set_default(tracing_subscriber::registry().with(layer));
        Self {
            causes,
            _guard: guard,
            _peer: peer,
        }
    }

    pub(crate) fn causes(&self) -> Vec<String> {
        self.causes.lock().unwrap().clone()
    }
}

struct ErrorFields(Arc<Mutex<Vec<String>>>);

impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for ErrorFields {
    fn on_event(&self, event: &tracing::Event<'_>, _: Context<'_, S>) {
        event.record(&mut ErrorField(&self.0));
    }
}

struct ErrorField<'a>(&'a Mutex<Vec<String>>);

impl tracing::field::Visit for ErrorField<'_> {
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        if field.name() == "error" {
            self.0.lock().unwrap().push(format!("{value:?}"));
        }
    }
}

mod tests {
    fn emit() {
        tracing::error!(error = "cause", "captured diagnostic");
    }

    /// A callsite whose first hit is on a thread without a subscriber, while
    /// this capture is the only dispatcher, still reaches the capture.
    #[test]
    fn a_callsite_first_hit_on_another_thread_still_reaches_the_capture() {
        let log = super::ErrorLog::capture();
        std::thread::scope(|threads| {
            threads.spawn(emit);
        });
        emit();
        assert_eq!(log.causes(), ["\"cause\""]);
    }
}
