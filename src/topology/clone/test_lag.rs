#![cfg(test)]
//! Tests: a seeded clone whose clock runs behind its sources' (see `topology::clone`), by path.

use std::future::Future;
use std::pin::Pin;
use std::sync::Mutex;
use std::time::Duration;

use slatedb_common::clock::{DefaultSystemClock, SystemClock, SystemClockTicker};

/// The lags tests set, each for the clones in one object store (by its address while a [`Lag`]
/// holds it).
static LAGS: Mutex<Vec<(usize, Duration)>> = Mutex::new(Vec::new());

/// `store`'s key in [`LAGS`]: its address.
pub(super) fn key(store: &dyn object_store::ObjectStore) -> usize {
    std::ptr::from_ref(store).cast::<()>().addr()
}

/// The lag set for the store keyed `key`.
pub(super) fn of(key: usize) -> Option<Duration> {
    let lags = LAGS
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    lags.iter().find(|(k, _)| *k == key).map(|(_, lag)| *lag)
}

/// While it lives, the clones in its store build with a clock running its lag behind; dropped,
/// they build with fresh clocks again.
#[must_use = "the lag holds only while the guard lives"]
pub(crate) struct Lag(usize);

impl Lag {
    /// Every seeded clone in `store` builds with a clock `lag` behind a fresh one.
    pub(crate) fn of(store: &dyn object_store::ObjectStore, lag: Duration) -> Self {
        let key = key(store);
        let mut lags = LAGS
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        lags.retain(|(k, _)| *k != key);
        lags.push((key, lag));
        Self(key)
    }
}

impl Drop for Lag {
    fn drop(&mut self) {
        let mut lags = LAGS
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        lags.retain(|(k, _)| *k != self.0);
    }
}

/// A fresh SlateDB clock, read `lag` behind.
#[derive(Debug)]
pub(super) struct Lagging {
    inner: DefaultSystemClock,
    lag: chrono::TimeDelta,
}

impl Lagging {
    pub(super) fn new(lag: Duration) -> Self {
        Self {
            inner: DefaultSystemClock::new(),
            lag: chrono::TimeDelta::from_std(lag).unwrap_or(chrono::TimeDelta::zero()),
        }
    }
}

impl SystemClock for Lagging {
    fn now(&self) -> chrono::DateTime<chrono::Utc> {
        self.inner.now() - self.lag
    }

    fn sleep<'a>(&'a self, duration: Duration) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>> {
        self.inner.sleep(duration)
    }

    fn ticker<'a>(&'a self, duration: Duration) -> SystemClockTicker<'a> {
        SystemClockTicker::new(self, duration)
    }
}
