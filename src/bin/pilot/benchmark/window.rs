//! One measurement ledger per point. Its owner serializes completion and freeze.
#![warn(clippy::wildcard_enum_match_arm)]

use anyhow::Context;
use hdrhistogram::Histogram;
use std::time::Duration;

#[derive(PartialEq, Eq)]
enum Phase {
    Warmup,
    Measuring,
    Frozen,
}

#[derive(Clone, Copy)]
pub(super) enum Outcome {
    Success,
    Error,
    Throttle,
}

pub(super) struct Window {
    phase: Phase,
    reported_errors: u8,
    ok: u64,
    errors: u64,
    throttles: u64,
    bytes: u64,
    hist: Histogram<u64>,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub(super) struct Snapshot {
    pub(super) ok: u64,
    pub(super) errors: u64,
    pub(super) throttles: u64,
    pub(super) bytes: u64,
    pub(super) p50_ms: f64,
    pub(super) p99_ms: f64,
}

impl Window {
    pub(super) fn new() -> anyhow::Result<Self> {
        Ok(Self {
            phase: Phase::Warmup,
            reported_errors: 0,
            ok: 0,
            errors: 0,
            throttles: 0,
            bytes: 0,
            hist: Histogram::new_with_bounds(1, 300_000_000, 3)?,
        })
    }

    // Logging has a lifetime-wide budget, independent of the measured window.
    pub(super) fn report_error(&mut self) -> bool {
        if self.reported_errors >= 3 {
            return false;
        }
        self.reported_errors += 1;
        true
    }

    pub(super) fn start(&mut self) -> anyhow::Result<()> {
        anyhow::ensure!(
            self.phase == Phase::Warmup,
            "measurement window already started"
        );
        self.phase = Phase::Measuring;
        Ok(())
    }

    pub(super) fn record(
        &mut self,
        outcome: Outcome,
        bytes: u64,
        elapsed: Duration,
    ) -> anyhow::Result<()> {
        if self.phase != Phase::Measuring {
            return Ok(());
        }
        match outcome {
            Outcome::Success => {
                let us = u64::try_from(elapsed.as_micros())
                    .context("benchmark latency overflows")?
                    .max(1);
                anyhow::ensure!(
                    us <= self.hist.high(),
                    "benchmark latency exceeds histogram bounds"
                );
                let ok = self
                    .ok
                    .checked_add(1)
                    .context("benchmark completion count overflows")?;
                let total = self
                    .bytes
                    .checked_add(bytes)
                    .context("benchmark payload count overflows")?;
                self.hist.record(us)?;
                self.ok = ok;
                self.bytes = total;
            }
            Outcome::Error => {
                self.errors = self
                    .errors
                    .checked_add(1)
                    .context("benchmark error count overflows")?
            }
            Outcome::Throttle => {
                self.throttles = self
                    .throttles
                    .checked_add(1)
                    .context("benchmark throttle count overflows")?
            }
        }
        Ok(())
    }

    pub(super) fn snapshot(&self) -> Snapshot {
        Snapshot {
            ok: self.ok,
            errors: self.errors,
            throttles: self.throttles,
            bytes: self.bytes,
            p50_ms: self.hist.value_at_quantile(0.5) as f64 / 1000.0,
            p99_ms: self.hist.value_at_quantile(0.99) as f64 / 1000.0,
        }
    }

    pub(super) fn freeze(&mut self) -> Snapshot {
        self.phase = Phase::Frozen;
        self.snapshot()
    }
}

#[cfg(test)]
mod tests {
    use super::{Outcome, Window};
    use std::time::Duration;

    #[test]
    fn measurement_boundaries_preserve_counts_and_reject_partial_success() {
        let mut window = Window::new().unwrap();
        window
            .record(Outcome::Success, 99, Duration::from_secs(1))
            .unwrap();
        assert_eq!(window.snapshot().ok, 0);
        assert!(window.report_error());
        assert!(window.report_error());
        window.start().unwrap();
        assert!(window.report_error());
        assert!(!window.report_error());
        assert!(window.start().is_err());
        window
            .record(Outcome::Success, 17, Duration::from_micros(1000))
            .unwrap();
        window.record(Outcome::Error, 99, Duration::ZERO).unwrap();
        window
            .record(Outcome::Throttle, 99, Duration::ZERO)
            .unwrap();
        let before = window.snapshot();
        assert_eq!(
            (before.ok, before.bytes, before.errors, before.throttles),
            (1, 17, 1, 1)
        );
        assert_eq!(before.p50_ms, 1.0);
        assert_eq!(before.p99_ms, 1.0);
        assert!(
            window
                .record(Outcome::Success, 1, Duration::from_secs(301))
                .is_err()
        );
        assert_eq!(window.snapshot(), before);
        window.bytes = u64::MAX;
        assert!(window.record(Outcome::Success, 1, Duration::ZERO).is_err());
        assert_eq!(window.ok, 1);
        window.ok = u64::MAX;
        assert!(window.record(Outcome::Success, 0, Duration::ZERO).is_err());
        window.errors = u64::MAX;
        assert!(window.record(Outcome::Error, 0, Duration::ZERO).is_err());
        window.throttles = u64::MAX;
        assert!(window.record(Outcome::Throttle, 0, Duration::ZERO).is_err());
        let frozen = window.freeze();
        window.record(Outcome::Success, 0, Duration::ZERO).unwrap();
        window.record(Outcome::Error, 0, Duration::ZERO).unwrap();
        window.record(Outcome::Throttle, 0, Duration::ZERO).unwrap();
        assert_eq!(window.snapshot(), frozen);
        assert!(window.start().is_err());
    }
}
