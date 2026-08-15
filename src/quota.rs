//! Server-side project admission backstops (docs/MULTITENANCY.md
//! §17.3, Stage 6).
//!
//! The GATEWAY owns primary quota enforcement (§17.2, platform-side);
//! every instance still maintains its own bounded per-project
//! admission so a project that slips past — or a gateway bug — cannot
//! take the cell down or starve its neighbors. Enforcement is scoped
//! to enforce-mode requests (the only ones with a verified project),
//! and each refusal names ONLY the offending project:
//!
//!   429 project_rate_limit        the project's request-rate bucket
//!   429 project_concurrency_limit the project's inflight ceiling
//!   503 project_tracker_capacity  the tracker itself is full (new
//!                                 projects only; tracked ones are
//!                                 never affected)
//!
//! Projects NEVER share a bucket (§17.3: no overflow coupling) — the
//! tracker map is keyed by project, and a full tracker refuses to
//! track new projects rather than lumping them together.
//!
//! Stage 6a enforces request rate + concurrency. Byte-rate quotas
//! (append/read volume) and live-subscription counting need metering
//! points with streaming lifetimes and land with 6b.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use crate::project_policy::ProjectQuotas;
use crate::tenant::ProjectId;

/// Bounded tracker: beyond this many distinct projects the tracker
/// refuses NEW ones (503) instead of growing without bound. Far above
/// any near-term cell packing; revisit with real shared-cell density.
pub const MAX_TRACKED_PROJECTS: usize = 1024;

struct Bucket {
    /// Fractional tokens currently available.
    level: f64,
    last_ms: i64,
}

pub struct ProjectAdmission {
    bucket: Mutex<Bucket>,
    inflight: AtomicU64,
}

#[derive(Debug)]
pub enum QuotaRefusal {
    /// Seconds until a token is expected (for Retry-After).
    Rate {
        retry_after_secs: u64,
    },
    Concurrency,
    TrackerCapacity,
}

/// Releases the inflight slot on drop — hold it for the handler's
/// lifetime. (Streaming response bodies outlive the handler; their
/// long-lived cost is the 6b live-subscription dimension, not this
/// counter.)
pub struct QuotaGuard {
    admission: Arc<ProjectAdmission>,
}

impl Drop for QuotaGuard {
    fn drop(&mut self) {
        self.admission.inflight.fetch_sub(1, Ordering::Relaxed);
    }
}

#[derive(Default)]
pub struct QuotaRegistry {
    projects: Mutex<HashMap<ProjectId, Arc<ProjectAdmission>>>,
}

impl QuotaRegistry {
    /// Acquire admission for one request of `project` under `quotas`
    /// (from the CURRENT policy snapshot — never token claims, §17.2).
    /// Quota value 0 = not configured at this level (cell safety
    /// limits still apply elsewhere).
    pub fn admit(
        &self,
        project: &ProjectId,
        quotas: &ProjectQuotas,
        now_ms: i64,
    ) -> Result<QuotaGuard, QuotaRefusal> {
        let admission = {
            let mut m = self.projects.lock().unwrap();
            match m.get(project) {
                Some(a) => a.clone(),
                None => {
                    if m.len() >= MAX_TRACKED_PROJECTS {
                        // Refuse to TRACK, never to merge: an untracked
                        // project sharing a bucket with strangers would
                        // couple their fates (§17.3).
                        return Err(QuotaRefusal::TrackerCapacity);
                    }
                    let a = Arc::new(ProjectAdmission {
                        bucket: Mutex::new(Bucket {
                            // A fresh project starts with a full
                            // second's burst.
                            level: quotas.requests_per_sec as f64,
                            last_ms: now_ms,
                        }),
                        inflight: AtomicU64::new(0),
                    });
                    m.insert(project.clone(), a.clone());
                    a
                }
            }
        };

        // Request-rate token bucket: refill rps/sec continuously,
        // capped at one second's worth (burst == rate). The rate is
        // re-read from the CURRENT quotas every admit, so a policy
        // update takes effect on the next request without any
        // republish handshake.
        if quotas.requests_per_sec > 0 {
            let rate = quotas.requests_per_sec as f64;
            let mut b = admission.bucket.lock().unwrap();
            let dt_s = ((now_ms - b.last_ms).max(0) as f64) / 1000.0;
            b.level = (b.level + dt_s * rate).min(rate);
            b.last_ms = now_ms;
            if b.level < 1.0 {
                let retry_after_secs = ((1.0 - b.level) / rate).ceil().max(1.0) as u64;
                return Err(QuotaRefusal::Rate { retry_after_secs });
            }
            b.level -= 1.0;
        }

        if quotas.max_inflight_requests > 0 {
            // Optimistic acquire; back out on overshoot. Relaxed is
            // fine: this is a backstop counter, not a synchronization
            // edge.
            let prev = admission.inflight.fetch_add(1, Ordering::Relaxed);
            if prev >= quotas.max_inflight_requests {
                admission.inflight.fetch_sub(1, Ordering::Relaxed);
                return Err(QuotaRefusal::Concurrency);
            }
        } else {
            admission.inflight.fetch_add(1, Ordering::Relaxed);
        }
        Ok(QuotaGuard { admission })
    }

    /// Operator visibility: (projects tracked, total inflight).
    pub fn stats(&self) -> (usize, u64) {
        let m = self.projects.lock().unwrap();
        let inflight = m.values().map(|a| a.inflight.load(Ordering::Relaxed)).sum();
        (m.len(), inflight)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn q(rps: u64, inflight: u64) -> ProjectQuotas {
        ProjectQuotas {
            requests_per_sec: rps,
            max_inflight_requests: inflight,
            ..Default::default()
        }
    }

    fn pid(s: &str) -> ProjectId {
        ProjectId::new(s).unwrap()
    }

    #[test]
    fn rate_bucket_admits_burst_then_refuses_then_refills() {
        let r = QuotaRegistry::default();
        let p = pid("proj_a");
        let quotas = q(2, 0);
        assert!(r.admit(&p, &quotas, 1_000).is_ok());
        assert!(r.admit(&p, &quotas, 1_000).is_ok());
        match r.admit(&p, &quotas, 1_000) {
            Err(QuotaRefusal::Rate { retry_after_secs }) => assert!(retry_after_secs >= 1),
            _ => panic!("third request in the same second must be rate-refused"),
        }
        // 500ms later: one token refilled (2/sec).
        assert!(r.admit(&p, &quotas, 1_500).is_ok());
        assert!(matches!(
            r.admit(&p, &quotas, 1_500),
            Err(QuotaRefusal::Rate { .. })
        ));
    }

    #[test]
    fn zero_rate_is_unlimited_and_projects_never_share_buckets() {
        let r = QuotaRegistry::default();
        let a = pid("proj_a");
        let b = pid("proj_b");
        for _ in 0..100 {
            assert!(r.admit(&a, &q(0, 0), 1_000).is_ok());
        }
        // Project A being hot must not consume B's tokens.
        let limited = q(1, 0);
        assert!(r.admit(&b, &limited, 1_000).is_ok());
        assert!(matches!(
            r.admit(&b, &limited, 1_000),
            Err(QuotaRefusal::Rate { .. })
        ));
        assert!(r.admit(&a, &q(0, 0), 1_000).is_ok(), "A unaffected by B");
    }

    #[test]
    fn concurrency_releases_with_the_guard() {
        let r = QuotaRegistry::default();
        let p = pid("proj_c");
        let quotas = q(0, 2);
        let g1 = r.admit(&p, &quotas, 1_000).unwrap();
        let _g2 = r.admit(&p, &quotas, 1_000).unwrap();
        assert!(matches!(
            r.admit(&p, &quotas, 1_000),
            Err(QuotaRefusal::Concurrency)
        ));
        drop(g1);
        assert!(r.admit(&p, &quotas, 1_000).is_ok());
    }

    #[test]
    fn tracker_bound_refuses_new_projects_only() {
        let r = QuotaRegistry::default();
        for i in 0..MAX_TRACKED_PROJECTS {
            assert!(r.admit(&pid(&format!("p{i}")), &q(0, 0), 1_000).is_ok());
        }
        assert!(matches!(
            r.admit(&pid("p_new"), &q(0, 0), 1_000),
            Err(QuotaRefusal::TrackerCapacity)
        ));
        // Already-tracked projects are untouched by tracker pressure.
        assert!(r.admit(&pid("p0"), &q(0, 0), 1_000).is_ok());
    }
}
