//! Exact integer allocation of the five read meters across positive time spans.
//! Earlier spans receive their floored proportion; the last receives the remainder.

pub(super) fn allocate(
    dimensions: [u64; 5],
    durations: &[u64],
) -> Result<Vec<[u64; 5]>, &'static str> {
    let Some((_, preceding)) = durations.split_last() else {
        return Err("read allocation has no month spans");
    };
    if durations.contains(&0) {
        return Err("read allocation has a nonpositive month span");
    }
    let total: u128 = durations.iter().map(|&duration| u128::from(duration)).sum();
    let mut remainder = dimensions;
    let mut allocated = Vec::with_capacity(durations.len());
    for &duration in preceding {
        let mut values = [0; 5];
        for ((value, &original), remaining) in
            values.iter_mut().zip(&dimensions).zip(&mut remainder)
        {
            *value = u64::try_from(u128::from(original) * u128::from(duration) / total)
                .map_err(|_| "read allocation exceeds its original meter")?;
            *remaining = remaining
                .checked_sub(*value)
                .ok_or("read allocation exceeds its remaining meter")?;
        }
        allocated.push(values);
    }
    allocated.push(remainder);
    Ok(allocated)
}

#[cfg(test)]
mod tests {
    use super::allocate;
    use proptest::prelude::any;

    #[test]
    fn rejects_missing_or_zero_spans() {
        assert!(allocate([1; 5], &[]).is_err());
        assert!(allocate([1; 5], &[0]).is_err());
        assert!(allocate([1; 5], &[1, 0]).is_err());
    }

    #[test]
    fn last_month_receives_every_rounding_remainder() {
        assert_eq!(
            allocate([1, 2, 3, 4, 5], &[1, 1, 1]).unwrap(),
            vec![[0, 0, 1, 1, 1], [0, 0, 1, 1, 1], [1, 2, 1, 2, 3]]
        );
        assert_eq!(
            allocate([u64::MAX; 5], &[u64::MAX]).unwrap(),
            vec![[u64::MAX; 5]]
        );
        assert_eq!(
            allocate([u64::MAX; 5], &[u64::MAX, u64::MAX]).unwrap(),
            vec![[u64::MAX / 2; 5], [u64::MAX / 2 + 1; 5]]
        );
    }

    proptest::proptest! {
        #[test]
        fn quality_allocation_conserves_every_meter_and_proportion(
            dimensions in any::<[u64; 5]>(),
            durations in proptest::collection::vec(1u64..=u64::MAX, 1..33),
        ) {
            let values = allocate(dimensions, &durations).unwrap();
            proptest::prop_assert_eq!(values.len(), durations.len());
            let total: u128 = durations.iter().map(|&d| u128::from(d)).sum();
            for (dimension, original) in dimensions.into_iter().enumerate() {
                let sum: u128 = values.iter().map(|v| u128::from(v[dimension])).sum();
                proptest::prop_assert_eq!(sum, u128::from(original));
                for (value, &duration) in values.iter().zip(&durations).take(durations.len() - 1) {
                    proptest::prop_assert_eq!(u128::from(value[dimension]),
                        u128::from(original) * u128::from(duration) / total);
                }
            }
        }
    }
}
