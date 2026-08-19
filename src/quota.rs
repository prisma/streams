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
/// refuses NEW ones (503, typed `TrackerCapacity`) instead of growing
/// without bound. SR-6 posture: Stage 8 certifies 1,000 projects per
/// cell and eviction needs `IDLE_EVICT_MS` of quiet, so the old cap
/// of 1,024 left 24 slots — any 25 first-seen projects inside one
/// idle window wedged new-project admission for up to five minutes.
/// 4,096 gives the certified density 4x headroom at ~2 MiB worst-case
/// tracker memory (~500 B/entry). This is a deliberate HARD ceiling:
/// refusing to track (never merging strangers into shared buckets) is
/// the fail-closed choice; the churn test pins evict-idle-first,
/// never-evict-active, and the typed refusal at true saturation.
pub const MAX_TRACKED_PROJECTS: usize = 4096;

/// A tracked project with no admission attempts for this long (and no
/// inflight work) may be evicted under tracker pressure. Its buckets
/// restart full — an idle project lost no accumulated debt worth
/// keeping at this horizon.
pub const IDLE_EVICT_MS: i64 = 300_000;

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
    /// Last admission attempt (ms) — the idle-eviction clock.
    last_seen_ms: std::sync::atomic::AtomicI64,
    bucket: Mutex<Bucket>,
    /// §17.2 volume backstops: append payload bytes and records.
    append_bytes: Mutex<Bucket>,
    append_records: Mutex<Bucket>,
    /// Read volume is debited POST-HOC (the response size is known
    /// only after serving), so this bucket runs negative and reads are
    /// refused while it is in debt.
    read_bytes: Mutex<Bucket>,
    inflight: AtomicU64,
    live_subs: AtomicU64,
    /// SR2-4 max_streams accounting. Seeded LAZILY from the durable
    /// catalog on the first limited create after boot (the count is
    /// process-local; the catalog is the truth it re-derives from).
    /// Soft-deleted fork-retained sources still count — they hold
    /// storage and the name — and release only on their terminal
    /// hard delete.
    streams: Mutex<StreamCount>,
    /// SR2-4 queued_append_bytes: bytes admitted but not yet decided.
    queued_bytes: AtomicU64,
}

#[derive(Default)]
struct StreamCount {
    seeded: bool,
    count: u64,
}

/// Holds one reserved stream slot until the create DECIDES: `commit`
/// keeps the +1 (the caller truly created a new stream), drop rolls
/// it back (replay, refusal, error).
pub struct StreamReservation {
    admission: Arc<ProjectAdmission>,
    committed: bool,
}

impl StreamReservation {
    pub fn commit(mut self) {
        self.committed = true;
    }
}

impl Drop for StreamReservation {
    fn drop(&mut self) {
        if !self.committed {
            let mut st = self.admission.streams.lock().unwrap();
            st.count = st.count.saturating_sub(1);
        }
    }
}

/// Releases the queued-byte charge when the append is DECIDED (the
/// handler's await returns, success or failure).
pub struct QueuedBytesGuard {
    admission: Arc<ProjectAdmission>,
    bytes: u64,
}

impl Drop for QueuedBytesGuard {
    fn drop(&mut self) {
        self.admission
            .queued_bytes
            .fetch_sub(self.bytes, Ordering::Relaxed);
    }
}

#[derive(Debug)]
pub enum QuotaRefusal {
    /// Seconds until a token is expected (for Retry-After).
    Rate {
        retry_after_secs: u64,
    },
    Concurrency,
    TrackerCapacity,
    /// SR2-4: the project holds `max_streams` live streams.
    StreamLimit,
    /// SR2-4: the project's committer-queued append bytes are at the
    /// ceiling; retry after in-flight appends decide.
    QueuedBytes,
}

/// Releases the inflight slot on drop — hold it for the handler's
/// lifetime. (Streaming response bodies outlive the handler; their
/// long-lived cost is the live-subscription dimension, not this
/// counter.)
pub struct QuotaGuard {
    admission: Arc<ProjectAdmission>,
}

impl Drop for QuotaGuard {
    fn drop(&mut self) {
        self.admission.inflight.fetch_sub(1, Ordering::Relaxed);
    }
}

/// One live subscription (§17.2). Attached to the STREAMING response
/// body, so it releases when the stream ends or the client goes away —
/// not when the handler returns.
pub struct SubscriptionGuard {
    admission: Arc<ProjectAdmission>,
}

impl Drop for SubscriptionGuard {
    fn drop(&mut self) {
        self.admission.live_subs.fetch_sub(1, Ordering::Relaxed);
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
                        // Review item 5: EVICT idle entries before
                        // refusing — a tracker that filled once must
                        // not refuse project 1,025 until restart.
                        // Never evict a project with inflight requests
                        // or live subscriptions; their guards point at
                        // the Arc we would orphan.
                        m.retain(|_, a| {
                            a.inflight.load(Ordering::Relaxed) > 0
                                || a.live_subs.load(Ordering::Relaxed) > 0
                                || now_ms - a.last_seen_ms.load(Ordering::Relaxed) < IDLE_EVICT_MS
                        });
                        if m.len() >= MAX_TRACKED_PROJECTS {
                            // Refuse to TRACK, never to merge: an
                            // untracked project sharing a bucket with
                            // strangers would couple their fates.
                            return Err(QuotaRefusal::TrackerCapacity);
                        }
                    }
                    let a = Arc::new(ProjectAdmission {
                        last_seen_ms: std::sync::atomic::AtomicI64::new(now_ms),
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
                        read_bytes: Mutex::new(Bucket {
                            level: quotas.read_bytes_per_sec as f64,
                            last_ms: now_ms,
                        }),
                        inflight: AtomicU64::new(0),
                        live_subs: AtomicU64::new(0),
                        streams: Mutex::new(StreamCount::default()),
                        queued_bytes: AtomicU64::new(0),
                    });
                    m.insert(project.clone(), a.clone());
                    a
                }
            }
        };

        admission.last_seen_ms.store(now_ms, Ordering::Relaxed);
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
        // Review item 5: the two debits are ATOMIC — both buckets are
        // held, both refilled, both CHECKED, and only then both
        // charged. The first cut charged bytes before records could
        // refuse, so a refused batch still burned byte budget.
        let mut bytes_b = admission.append_bytes.lock().unwrap();
        let mut recs_b = admission.append_records.lock().unwrap();
        let mut plan: [Option<(f64, f64)>; 2] = [None, None]; // (rate, cost)
        let check = |b: &mut Bucket,
                     rate_u: u64,
                     cost: f64,
                     slot: &mut Option<(f64, f64)>|
         -> Result<(), QuotaRefusal> {
            if rate_u == 0 {
                return Ok(());
            }
            let rate = rate_u as f64;
            let _ = b.take(rate, 0.0, now_ms); // refill only
            let full = b.level >= rate - f64::EPSILON;
            if full && cost > rate {
                // Oversized single op from a full bucket: admit once,
                // go negative; later ops wait the debt out.
                *slot = Some((rate, cost));
                return Ok(());
            }
            if b.level < cost {
                return Err(QuotaRefusal::Rate {
                    retry_after_secs: (((cost - b.level) / rate).ceil().max(1.0)) as u64,
                });
            }
            *slot = Some((rate, cost));
            Ok(())
        };
        check(
            &mut bytes_b,
            quotas.append_bytes_per_sec,
            payload_bytes as f64,
            &mut plan[0],
        )?;
        check(
            &mut recs_b,
            quotas.append_records_per_sec,
            records as f64,
            &mut plan[1],
        )?;
        if let Some((_, cost)) = plan[0] {
            bytes_b.level -= cost;
        }
        if let Some((_, cost)) = plan[1] {
            recs_b.level -= cost;
        }
        Ok(())
    }

    /// Read admission (§17.2): reads are refused while the project's
    /// read-byte bucket is IN DEBT from earlier responses. The check is
    /// cheap and runs before serving; the debit lands after.
    pub fn check_read(
        &self,
        project: &ProjectId,
        quotas: &ProjectQuotas,
        now_ms: i64,
    ) -> Result<(), QuotaRefusal> {
        if quotas.read_bytes_per_sec == 0 {
            return Ok(());
        }
        let Some(admission) = self.tracked(project) else {
            return Ok(()); // request-rate admit tracks first
        };
        let rate = quotas.read_bytes_per_sec as f64;
        let mut b = admission.read_bytes.lock().unwrap();
        let _ = b.take(rate, 0.0, now_ms); // refill only
        if b.level < 0.0 {
            return Err(QuotaRefusal::Rate {
                retry_after_secs: ((-b.level / rate).ceil().max(1.0)) as u64,
            });
        }
        Ok(())
    }

    /// Post-hoc read debit with the SERVED byte count. Deliberately
    /// unconditional and negative-capable: the response was already
    /// sent, so the debt is real either way.
    pub fn debit_read(&self, project: &ProjectId, quotas: &ProjectQuotas, bytes: u64, now_ms: i64) {
        if quotas.read_bytes_per_sec == 0 || bytes == 0 {
            return;
        }
        if let Some(admission) = self.tracked(project) {
            let rate = quotas.read_bytes_per_sec as f64;
            let mut b = admission.read_bytes.lock().unwrap();
            let _ = b.take(rate, 0.0, now_ms); // refill first
            b.level -= bytes as f64;
        }
    }

    /// §17.2 subscriptions: acquire one live-subscription slot. The
    /// guard rides the streaming response body.
    pub fn admit_subscription(
        &self,
        project: &ProjectId,
        quotas: &ProjectQuotas,
    ) -> Result<Option<SubscriptionGuard>, QuotaRefusal> {
        if quotas.max_live_subscriptions == 0 {
            return Ok(None);
        }
        let Some(admission) = self.tracked(project) else {
            return Ok(None);
        };
        let prev = admission.live_subs.fetch_add(1, Ordering::Relaxed);
        if prev >= quotas.max_live_subscriptions {
            admission.live_subs.fetch_sub(1, Ordering::Relaxed);
            return Err(QuotaRefusal::Concurrency);
        }
        Ok(Some(SubscriptionGuard { admission }))
    }

    /// SR2-4: does the project's stream count still need its catalog
    /// seed? The caller counts (async, catalog pages) only when this
    /// says so, then passes the count to `reserve_stream`.
    pub fn needs_stream_seed(&self, project: &ProjectId) -> bool {
        self.tracked(project)
            .map(|a| !a.streams.lock().unwrap().seeded)
            .unwrap_or(false)
    }

    /// SR2-4: reserve one stream slot under `max_streams`, race-safely
    /// (count checked and bumped under one lock; concurrent racers
    /// serialize here, losers refuse typed). `seed` supplies the
    /// catalog count when this project has not been seeded since boot;
    /// the first reservation wins the seed, later ones ignore theirs.
    pub fn reserve_stream(
        &self,
        project: &ProjectId,
        quotas: &ProjectQuotas,
        seed: Option<u64>,
    ) -> Result<Option<StreamReservation>, QuotaRefusal> {
        if quotas.max_streams == 0 {
            return Ok(None);
        }
        let Some(admission) = self.tracked(project) else {
            return Ok(None);
        };
        let mut st = admission.streams.lock().unwrap();
        if !st.seeded {
            let Some(n) = seed else {
                // Caller must seed first; refuse closed rather than
                // guess (a wrong zero would admit past the cap).
                return Err(QuotaRefusal::StreamLimit);
            };
            st.seeded = true;
            st.count = n;
        }
        if st.count >= quotas.max_streams {
            return Err(QuotaRefusal::StreamLimit);
        }
        st.count += 1;
        drop(st);
        Ok(Some(StreamReservation {
            admission,
            committed: false,
        }))
    }

    /// SR2-4: a terminal hard delete frees the slot. Unseeded (or
    /// untracked) projects no-op — their next seed recounts the
    /// catalog, which already reflects the deletion.
    pub fn release_stream(&self, project: &ProjectId) {
        if let Some(a) = self.tracked(project) {
            let mut st = a.streams.lock().unwrap();
            if st.seeded {
                st.count = st.count.saturating_sub(1);
            }
        }
    }

    /// SR2-4: charge `bytes` to the project's committer-queue budget
    /// BEFORE the append is enqueued; the guard releases when the
    /// append DECIDES. 0 = not configured.
    pub fn charge_queued(
        &self,
        project: &ProjectId,
        quotas: &ProjectQuotas,
        bytes: u64,
    ) -> Result<Option<QueuedBytesGuard>, QuotaRefusal> {
        if quotas.queued_append_bytes == 0 {
            return Ok(None);
        }
        let Some(admission) = self.tracked(project) else {
            return Ok(None);
        };
        let new = admission.queued_bytes.fetch_add(bytes, Ordering::Relaxed) + bytes;
        if new > quotas.queued_append_bytes {
            admission.queued_bytes.fetch_sub(bytes, Ordering::Relaxed);
            return Err(QuotaRefusal::QueuedBytes);
        }
        Ok(Some(QueuedBytesGuard { admission, bytes }))
    }

    fn tracked(&self, project: &ProjectId) -> Option<Arc<ProjectAdmission>> {
        self.projects.lock().unwrap().get(project).cloned()
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

    /// SR-6 tracker-capacity churn: at the cap the tracker refuses
    /// NEW projects with the typed refusal while every entry is
    /// recent, evicts the idle mass (never a project with live
    /// guards) once the horizon passes, and re-tracks returning
    /// projects — a tracker that filled once must not refuse until
    /// restart, and must never orphan an entry whose guards are held.
    #[test]
    fn tracker_capacity_churn_evicts_idle_never_active() {
        let r = QuotaRegistry::default();
        let quotas = q(0, 2);
        let t0: i64 = 1_000_000;
        let name = |i: usize| pid(&format!("churn_{i}"));
        // Pin churn_0 with BOTH its concurrency slots held live.
        let _g0 = r.admit(&name(0), &quotas, t0).expect("pin 1");
        let _g1 = r.admit(&name(0), &quotas, t0).expect("pin 2");
        for i in 1..MAX_TRACKED_PROJECTS {
            r.admit(&name(i), &quotas, t0).expect("fill");
        }
        // At capacity with every entry recent: typed refusal —
        // track-or-refuse, never merge into a stranger's buckets.
        match r.admit(&name(MAX_TRACKED_PROJECTS), &quotas, t0 + 1_000) {
            Err(QuotaRefusal::TrackerCapacity) => {}
            Ok(_) => panic!("expected TrackerCapacity, got admission"),
            Err(e) => panic!("expected TrackerCapacity, got {e:?}"),
        }
        // Past the idle horizon the idle mass is evictable: thousands
        // of NEW projects admit (the first triggers the sweep).
        let t1 = t0 + IDLE_EVICT_MS + 1_000;
        for i in 0..2_000 {
            r.admit(&name(MAX_TRACKED_PROJECTS + 1 + i), &quotas, t1)
                .expect("churn admit");
        }
        // churn_0 sat idle far past the horizon through the sweep, but
        // its guards are live: retention is observable as its inflight
        // count — the third admit refuses on concurrency. (Had the
        // sweep orphaned it, a FRESH entry with inflight=0 would have
        // admitted here.)
        match r.admit(&name(0), &quotas, t1) {
            Err(QuotaRefusal::Concurrency) => {}
            Ok(_) => panic!("pinned project was evicted (fresh entry admitted)"),
            Err(e) => panic!("pinned project was evicted: {e:?}"),
        }
        // An idle-evicted project simply re-tracks on return (full
        // burst — documented, an idle project lost no debt worth
        // keeping at this horizon).
        r.admit(&name(1), &quotas, t1)
            .expect("evicted project returns");
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
    fn read_debit_runs_negative_and_blocks_until_refilled() {
        let r = QuotaRegistry::default();
        let p = pid("proj_r");
        let quotas = ProjectQuotas {
            read_bytes_per_sec: 100,
            ..Default::default()
        };
        let _g = r.admit(&p, &quotas, 1_000).unwrap();
        // First read passes (no debt), serves 350 bytes -> level -250.
        assert!(r.check_read(&p, &quotas, 1_000).is_ok());
        r.debit_read(&p, &quotas, 350, 1_000);
        match r.check_read(&p, &quotas, 1_000) {
            Err(QuotaRefusal::Rate { retry_after_secs }) => {
                assert!(retry_after_secs >= 2, "debt horizon: {retry_after_secs}")
            }
            _ => panic!("in-debt bucket must refuse reads"),
        }
        // 2.6s later the 250-byte debt has refilled past zero.
        assert!(r.check_read(&p, &quotas, 3_600).is_ok());
    }

    #[test]
    fn subscription_slots_release_with_the_guard() {
        let r = QuotaRegistry::default();
        let p = pid("proj_s");
        let quotas = ProjectQuotas {
            max_live_subscriptions: 1,
            ..Default::default()
        };
        let _g = r.admit(&p, &quotas, 1_000).unwrap();
        let s1 = r.admit_subscription(&p, &quotas).unwrap();
        assert!(s1.is_some());
        assert!(matches!(
            r.admit_subscription(&p, &quotas),
            Err(QuotaRefusal::Concurrency)
        ));
        drop(s1);
        assert!(r.admit_subscription(&p, &quotas).unwrap().is_some());
        // Unlimited (0) never allocates a guard.
        let unlimited = ProjectQuotas::default();
        assert!(r.admit_subscription(&p, &unlimited).unwrap().is_none());
    }

    #[test]
    fn refused_batch_charges_nothing_atomic_debit() {
        // Review item 5: bytes budget generous, records budget tiny.
        // A batch that the RECORDS bucket refuses must not burn BYTES.
        let r = QuotaRegistry::default();
        let p = pid("proj_at");
        let quotas = ProjectQuotas {
            append_bytes_per_sec: 1_000,
            append_records_per_sec: 2,
            ..Default::default()
        };
        let _g = r.admit(&p, &quotas, 1_000).unwrap();
        // Spend one record so the records bucket is NOT full (the
        // oversized-from-full rule must not apply).
        assert!(r.admit_append(&p, &quotas, 100, 1, 1_000).is_ok());
        // Refused on records (needs 2, has 1) — bytes must be
        // untouched by the refused attempt.
        assert!(matches!(
            r.admit_append(&p, &quotas, 800, 2, 1_000),
            Err(QuotaRefusal::Rate { .. })
        ));
        // Exactly the remaining byte budget still fits: had the
        // refused batch charged bytes, this would fail.
        assert!(r.admit_append(&p, &quotas, 900, 1, 1_000).is_ok());
    }

    #[test]
    fn tracker_evicts_idle_projects_never_active_ones() {
        let r = QuotaRegistry::default();
        // Fill the tracker at t=0; keep p0 ACTIVE via a held guard.
        let g0 = r.admit(&pid("p0"), &q(0, 0), 0).unwrap();
        for i in 1..MAX_TRACKED_PROJECTS {
            let _ = r.admit(&pid(&format!("p{i}")), &q(0, 0), 0).unwrap();
        }
        // Before the idle horizon: full tracker refuses the newcomer.
        assert!(matches!(
            r.admit(&pid("p_new"), &q(0, 0), IDLE_EVICT_MS - 1),
            Err(QuotaRefusal::TrackerCapacity)
        ));
        // Past the horizon: idle entries evict, the newcomer fits...
        assert!(r.admit(&pid("p_new"), &q(0, 0), IDLE_EVICT_MS + 1).is_ok());
        // ...and ONLY the project with INFLIGHT work survived the
        // sweep beside the newcomer (p_new's own guard dropped at the
        // assert, so p0's held guard is the one live slot).
        let (tracked, inflight) = r.stats();
        assert_eq!(tracked, 2, "p0 (active) + p_new: {tracked}");
        assert_eq!(inflight, 1, "p0's held guard: {inflight}");
        drop(g0);
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
