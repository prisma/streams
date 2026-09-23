//! Boot-time proof of the per-stream token-bucket posture.
//!
//! `UsageService` refills `LIMIT_*_PER_SEC × LIMIT_BURST_SECS` tokens into a
//! fresh bucket and charges one request, `n` bytes and `m` records per
//! append. A posture whose enabled bucket cannot hold ONE unit refuses every
//! append forever; a non-finite knob either bricks the write path
//! (`burst = 0`) or silently turns the limiter off (a `NaN` bucket never
//! compares below a request, and so does a capacity that overflowed to
//! infinity): all are configuration facts, so they are refused here, before
//! any service exists, rather than re-decided per append by the runtime
//! owner.

use crate::config::AdmissionConfig;

/// Every problem with the limit posture, each naming its variable. Empty
/// means `UsageService` can admit at least one unit of every enabled resource.
pub(crate) fn validate_admission_limits(a: &AdmissionConfig) -> Vec<String> {
    let mut errors = Vec::new();
    let burst = a.limit_burst_secs;
    let burst_ok = burst.is_finite() && burst > 0.0;
    if !burst_ok {
        errors.push(format!(
            "LIMIT_BURST_SECS={burst} must be a finite number > 0: every per-stream bucket \
             holds rate x burst tokens, so 0 refuses every append and NaN disables every limit"
        ));
    }
    for (name, rate, unit) in [
        ("LIMIT_BYTES_PER_SEC", a.limit_bytes_per_sec, "byte"),
        ("LIMIT_REQS_PER_SEC", a.limit_reqs_per_sec, "request"),
        ("LIMIT_RECS_PER_SEC", a.limit_recs_per_sec, "record"),
    ] {
        if !rate.is_finite() || rate < 0.0 {
            errors.push(format!(
                "{name}={rate} must be a finite number >= 0 (0 disables the bucket)"
            ));
        } else if burst_ok && !(rate * burst).is_finite() {
            errors.push(format!(
                "{name}={rate} x LIMIT_BURST_SECS={burst} overflows the bucket capacity: an \
                 infinite bucket never refuses, so the {unit} limit would be silently off"
            ));
        } else if burst_ok && rate > 0.0 && rate * burst < 1.0 {
            errors.push(format!(
                "{name}={rate} x LIMIT_BURST_SECS={burst} holds {} tokens: a bucket that cannot \
                 hold one {unit} refuses every append forever (raise it, or set 0 to disable)",
                rate * burst
            ));
        }
    }
    errors
}

#[cfg(test)]
mod tests {
    use super::validate_admission_limits;
    use crate::config::AdmissionConfig;

    fn names(errors: &[String], name: &str) -> usize {
        errors.iter().filter(|e| e.contains(name)).count()
    }

    #[test]
    fn defaults_validate_and_zero_disables_a_bucket() {
        assert!(validate_admission_limits(&AdmissionConfig::default()).is_empty());
        let disabled = AdmissionConfig {
            limit_bytes_per_sec: 0.0,
            limit_reqs_per_sec: 0.0,
            limit_recs_per_sec: 0.0,
            ..Default::default()
        };
        assert!(validate_admission_limits(&disabled).is_empty());
    }

    #[test]
    fn non_finite_or_negative_limits_are_refused_by_name() {
        for burst in [0.0, -1.0, f64::NAN, f64::INFINITY] {
            let errors = validate_admission_limits(&AdmissionConfig {
                limit_burst_secs: burst,
                ..Default::default()
            });
            assert_eq!(
                names(&errors, "LIMIT_BURST_SECS"),
                1,
                "burst {burst}: {errors:?}"
            );
            assert_eq!(errors.len(), 1, "a bad burst is one finding: {errors:?}");
        }
        for rate in [-1.0, f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
            let errors = validate_admission_limits(&AdmissionConfig {
                limit_bytes_per_sec: rate,
                limit_reqs_per_sec: rate,
                limit_recs_per_sec: rate,
                ..Default::default()
            });
            for name in [
                "LIMIT_BYTES_PER_SEC",
                "LIMIT_REQS_PER_SEC",
                "LIMIT_RECS_PER_SEC",
            ] {
                assert_eq!(names(&errors, name), 1, "rate {rate}: {errors:?}");
            }
        }
        // Finite knobs whose product overflows: the limit would be silently off.
        let errors = validate_admission_limits(&AdmissionConfig {
            limit_bytes_per_sec: 1e308,
            ..Default::default()
        });
        assert_eq!(names(&errors, "LIMIT_BYTES_PER_SEC"), 1, "{errors:?}");
        assert!(errors[0].contains("overflows"), "{errors:?}");
    }

    #[test]
    fn an_enabled_bucket_must_hold_one_token() {
        for name in [
            "LIMIT_BYTES_PER_SEC",
            "LIMIT_REQS_PER_SEC",
            "LIMIT_RECS_PER_SEC",
        ] {
            let posture = |rate: f64| {
                let mut a = AdmissionConfig {
                    limit_burst_secs: 2.0,
                    ..Default::default()
                };
                match name {
                    "LIMIT_BYTES_PER_SEC" => a.limit_bytes_per_sec = rate,
                    "LIMIT_REQS_PER_SEC" => a.limit_reqs_per_sec = rate,
                    _ => a.limit_recs_per_sec = rate,
                }
                a
            };
            let refused = validate_admission_limits(&posture(0.4));
            assert_eq!(
                names(&refused, name),
                1,
                "0.4 x 2 = 0.8 tokens: {refused:?}"
            );
            assert!(
                refused[0].contains("holds 0.8 tokens"),
                "the finding states the capacity it computed: {refused:?}"
            );
            assert!(
                validate_admission_limits(&posture(0.5)).is_empty(),
                "0.5 x 2 = exactly one token is admissible"
            );
        }
    }
}
