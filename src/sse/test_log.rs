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
}

impl ErrorLog {
    pub(crate) fn capture() -> Self {
        let causes = Arc::new(Mutex::new(Vec::new()));
        let layer = ErrorFields(Arc::clone(&causes));
        let guard = tracing::subscriber::set_default(tracing_subscriber::registry().with(layer));
        Self {
            causes,
            _guard: guard,
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
