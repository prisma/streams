//! Exact storage byte-time over a positive signed-clock interval. The full
//! distance between two i64 timestamps fits in u64; multiplying that distance
//! by a u64 gauge therefore fits in u128 without rounding or saturation.

pub(super) fn byte_ms(gauge: u64, from: i64, through: i64) -> u128 {
    if through <= from {
        return 0;
    }
    u128::from(through.abs_diff(from)) * u128::from(gauge)
}

#[cfg(test)]
mod tests {
    use super::byte_ms;
    use proptest::prelude::{any, prop_assert_eq};

    #[test]
    fn empty_reversed_and_full_width_intervals() {
        assert_eq!(byte_ms(7, 10, 10), 0);
        assert_eq!(byte_ms(7, 11, 10), 0);
        assert_eq!(byte_ms(0, i64::MIN, i64::MAX), 0);
        assert_eq!(byte_ms(3, -10, 10), 60);
        assert_eq!(
            byte_ms(u64::MAX, i64::MIN, i64::MAX),
            u128::from(u64::MAX).pow(2)
        );
    }

    proptest::proptest! {
        #[test]
        fn signed_clock_distance_matches_wide_oracle(gauge in any::<u64>(), from in any::<i64>(), through in any::<i64>()) {
            let span = u128::try_from((i128::from(through) - i128::from(from)).max(0)).unwrap();
            prop_assert_eq!(byte_ms(gauge, from, through), span * u128::from(gauge));
        }

        #[test]
        fn splitting_an_interval_preserves_exact_accrual(gauge in any::<u64>(), a in any::<i64>(), b in any::<i64>()) {
            let (from, through) = (a.min(b), a.max(b));
            let middle = i64::try_from((i128::from(from) + i128::from(through)) / 2).unwrap();
            prop_assert_eq!(byte_ms(gauge, from, through), byte_ms(gauge, from, middle) + byte_ms(gauge, middle, through));
        }
    }
}
