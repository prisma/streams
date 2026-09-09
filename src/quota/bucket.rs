//! Fractional token balance, refill clock and Retry-After policy.
#![warn(clippy::indexing_slicing, clippy::arithmetic_side_effects)]

pub(super) struct Bucket {
    /// The admission owner checks and debits paired buckets atomically.
    pub(super) level: f64,
    last_ms: i64,
}

impl Bucket {
    /// A fresh project starts with one full second's burst.
    pub(super) fn full(rate: u64, now_ms: i64) -> Self {
        Self {
            level: rate as f64,
            last_ms: now_ms,
        }
    }

    /// Refill without attempting a debit, including while read balance is negative.
    pub(super) fn refill(&mut self, rate: f64, now_ms: i64) {
        let dt_s = (now_ms.saturating_sub(self.last_ms).max(0) as f64) / 1000.0;
        self.level = (self.level + dt_s * rate).min(rate);
        self.last_ms = now_ms;
    }

    /// Round positive wait time up to seconds, at least one, saturating at u64::MAX.
    #[expect(
        clippy::cast_possible_truncation,
        clippy::cast_sign_loss,
        reason = "quota Retry-After; ceil and max preserve the minimum wait and Rust float conversion saturates unrepresentable seconds; truncating before rounding would understate the debt"
    )]
    pub(super) fn retry_after(&self, rate: f64, cost: f64) -> u64 {
        (((cost - self.level) / rate).ceil().max(1.0)) as u64
    }

    /// Refill and debit only when enough tokens exist.
    pub(super) fn take(&mut self, rate: f64, cost: f64, now_ms: i64) -> Result<(), u64> {
        self.refill(rate, now_ms);
        if self.level < cost {
            return Err(self.retry_after(rate, cost));
        }
        self.level -= cost;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::Bucket;
    use proptest::{prop_assert, prop_assert_eq};

    #[test]
    fn refill_preserves_debt_and_fractional_tokens_without_debiting() {
        let mut bucket = Bucket::full(4, 1000);
        assert_eq!(bucket.level, 4.0);
        assert_eq!(bucket.take(4.0, 3.0, 1000), Ok(()));
        bucket.refill(4.0, 1125);
        assert_eq!(bucket.level, 1.5);
        assert_eq!(bucket.take(4.0, 2.0, 1125), Err(1));
        assert_eq!(bucket.level, 1.5);
        bucket.level = -7.0;
        bucket.refill(4.0, 1625);
        assert_eq!(bucket.level, -5.0);
        bucket.refill(4.0, 20_000);
        assert_eq!(bucket.level, 4.0);
        bucket.refill(2.0, 20_000);
        assert_eq!(
            bucket.level, 2.0,
            "a policy reduction caps the existing burst"
        );
    }

    #[test]
    fn clock_reversal_does_not_refill_and_extreme_forward_time_saturates() {
        let mut bucket = Bucket::full(4, 1000);
        assert_eq!(bucket.take(4.0, 4.0, 1000), Ok(()));
        bucket.refill(4.0, 500);
        assert_eq!(bucket.level, 0.0);
        bucket.refill(4.0, 625);
        assert_eq!(bucket.level, 0.5, "the latest sample owns the refill clock");
        bucket.refill(4.0, i64::MIN);
        assert_eq!(bucket.level, 0.5);
        bucket.refill(4.0, i64::MAX);
        assert_eq!(bucket.level, 4.0);
    }

    #[test]
    fn retry_after_rounds_up_and_saturates() {
        let mut bucket = Bucket::full(4, 0);
        for (level, rate, cost, expected) in [
            (4.0, 4.0, 4.0, 1),
            (4.0, 4.0, 1.0, 1),
            (0.5, 2.0, 1.0, 1),
            (-2.5, 2.0, 0.0, 2),
            (-4.0, 2.0, 0.0, 2),
            (-4.5, 2.0, 0.0, 3),
            (0.0, 0.0, 1.0, u64::MAX),
            (-1.0e30, 1.0, 0.0, u64::MAX),
        ] {
            bucket.level = level;
            assert_eq!(bucket.retry_after(rate, cost), expected);
        }
    }

    proptest::proptest! {
        #[test]
        fn quality_quota_a_full_burst_is_spent_once_and_one_second_refills_it(rate in 1u32..1_000_000, start in -1_000_000i64..1_000_000) {
            let rate_float = f64::from(rate);
            let mut bucket = Bucket::full(u64::from(rate), start);
            prop_assert_eq!(bucket.take(rate_float, rate_float, start), Ok(()));
            prop_assert_eq!(bucket.level, 0.0);
            prop_assert_eq!(bucket.take(rate_float, 1.0, start), Err(1));
            prop_assert_eq!(bucket.level, 0.0);
            prop_assert_eq!(bucket.take(rate_float, rate_float, start.checked_add(1000).unwrap()), Ok(()));
            prop_assert_eq!(bucket.level, 0.0);
        }

        #[test]
        fn quality_quota_denied_debits_leave_available_balance_intact(rate in 1u32..1_000_000, extra in 1u32..1_000_000) {
            let mut bucket = Bucket::full(u64::from(rate), 0);
            prop_assert!(bucket.take(f64::from(rate), f64::from(rate) + f64::from(extra), 0).is_err());
            prop_assert_eq!(bucket.level, f64::from(rate));
        }
    }
}
