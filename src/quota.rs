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

impl Bucket {
    /// Refill at `rate`/sec (capped at one second's burst) and try to
    /// take `cost` tokens. On refusal returns seconds until enough
    /// tokens exist (Retry-After).
    fn take(&mut self, rate: f64, cost: f64, now_ms: i64) -> Result<(), u64> {
        let dt_s = ((now_ms - self.last_ms).max(0) as f64) / 1000.0;
        self.level = (self.level + dt_s * rate).min(rate);
        self.last_ms = now_ms;
        if self.level < cost {
            return Err((((cost - self.level) / rate).ceil().max(1.0)) as u64);
        }
        self.level -= cost;
        Ok(())
    }
}

pub struct ProjectAdmission {
    bucket: Mutex<Bucket>,
    /// §17.2 volume backstops: append payload bytes and records.
    append_bytes: Mutex<Bucket>,
    append_records: Mutex<Bucket>,
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
                        append_bytes: Mutex::new(Bucket {
                            level: quotas.append_bytes_per_sec as f64,
                            last_ms: now_ms,
                        }),
                        append_records: Mutex::new(Bucket {
                            level: quotas.append_records_per_sec as f64,
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
        if quotas.requests_per_sec > 0
            && let Err(retry_after_secs) =
                admission
                    .bucket
                    .lock()
                    .unwrap()
                    .take(quotas.requests_per_sec as f64, 1.0, now_ms)
        {
            return Err(QuotaRefusal::Rate { retry_after_secs });
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

    /// §17.2 append-volume backstop, checked at the APPEND site with
    /// the exact buffered payload size — after the request-rate admit,
    /// before the write is dispatched. A single append larger than one
    /// second's budget is still admitted when the bucket is full
    /// (otherwise it could never succeed); it drives the bucket
    /// negative and later appends wait it out.
    pub fn admit_append(
        &self,
        project: &ProjectId,
        quotas: &ProjectQuotas,
        payload_bytes: u64,
        records: u64,
        now_ms: i64,
    ) -> Result<(), QuotaRefusal> {
        if quotas.append_bytes_per_sec == 0 && quotas.append_records_per_sec == 0 {
            return Ok(());
        }
        let admission = {
            let m = self.projects.lock().unwrap();
            match m.get(project) {
                Some(a) => a.clone(),
                // admit() ran first on this request; absence means the
                // tracker refused it there.
                None => return Err(QuotaRefusal::TrackerCapacity),
            }
        };
        let oversize = |cost: f64, rate: f64, b: &mut Bucket| {
            // Full bucket + oversized op: admit once, go negative.
            let full = b.level >= rate - f64::EPSILON;
            if full && cost > rate {
                b.level -= cost;
                b.last_ms = now_ms;
                return true;
            }
            false
        };
        if quotas.append_bytes_per_sec > 0 {
            let rate = quotas.append_bytes_per_sec as f64;
            let mut b = admission.append_bytes.lock().unwrap();
            // Refill before the oversize check so "full" is current.
            let _ = b.take(rate, 0.0, now_ms);
            if !oversize(payload_bytes as f64, rate, &mut b)
                && let Err(retry_after_secs) = b.take(rate, payload_bytes as f64, now_ms)
            {
                return Err(QuotaRefusal::Rate { retry_after_secs });
            }
        }
        if quotas.append_records_per_sec > 0 {
            let rate = quotas.append_records_per_sec as f64;
            let mut b = admission.append_records.lock().unwrap();
            let _ = b.take(rate, 0.0, now_ms);
            if !oversize(records as f64, rate, &mut b)
                && let Err(retry_after_secs) = b.take(rate, records as f64, now_ms)
            {
                return Err(QuotaRefusal::Rate { retry_after_secs });
            }
        }
        Ok(())
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
    fn append_volume_buckets_meter_bytes_and_records() {
        let r = QuotaRegistry::default();
        let p = pid("proj_v");
        let quotas = ProjectQuotas {
            append_bytes_per_sec: 1_000,
            append_records_per_sec: 10,
            ..Default::default()
        };
        // Track the project first (as the request-rate admit does).
        let _g = r.admit(&p, &quotas, 1_000).unwrap();
        assert!(r.admit_append(&p, &quotas, 600, 5, 1_000).is_ok());
        assert!(r.admit_append(&p, &quotas, 400, 5, 1_000).is_ok());
        // Bytes bucket dry (and records bucket dry).
        assert!(matches!(
            r.admit_append(&p, &quotas, 1, 1, 1_000),
            Err(QuotaRefusal::Rate { .. })
        ));
        // Half a second later: 500 bytes / 5 records refilled.
        assert!(r.admit_append(&p, &quotas, 500, 5, 1_500).is_ok());
        assert!(matches!(
            r.admit_append(&p, &quotas, 1, 0, 1_500),
            Err(QuotaRefusal::Rate { .. })
        ));
    }

    #[test]
    fn oversized_single_append_admits_once_then_waits() {
        let r = QuotaRegistry::default();
        let p = pid("proj_o");
        let quotas = ProjectQuotas {
            append_bytes_per_sec: 100,
            ..Default::default()
        };
        let _g = r.admit(&p, &quotas, 1_000).unwrap();
        // 5x one second's budget: admitted from a full bucket (it
        // could otherwise never succeed), driving the bucket negative.
        assert!(r.admit_append(&p, &quotas, 500, 1, 1_000).is_ok());
        // The debt is real: even a tiny append waits it out...
        match r.admit_append(&p, &quotas, 1, 0, 1_000) {
            Err(QuotaRefusal::Rate { retry_after_secs }) => {
                assert!(retry_after_secs >= 4, "debt horizon: {retry_after_secs}")
            }
            _ => panic!("bucket must be in debt"),
        }
        // ...and clears after the debt window.
        assert!(r.admit_append(&p, &quotas, 50, 0, 7_000).is_ok());
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
