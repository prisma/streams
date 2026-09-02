//! The shard directory (WP-02 / PR 6-A): the topology prefixes, the
//! serving map, the single-flight open gate and the ownership policy
//! behind narrow methods — extracted from `http::AppState`, which had
//! been the service locator for shard state. Resolution policy lives
//! here, transport-neutral; the HTTP adapter maps `ResolveError` in one
//! place.

use std::collections::HashMap;
use std::sync::{Arc, RwLock, Weak};
use std::time::Duration;

use crate::shard::ShardEngine;
use crate::sharddir::{EngineIncarnation, OpenFn, OpenGate, OpenOutcome};

/// How a resolution counts for sweep custody (R29).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Adoption {
    /// Customer traffic: stamps the adoption sequence, revoking any
    /// sweep custody so the scheduler cannot close the engine under it.
    External,
    /// Internal maintenance (scaler, tombstone walk): never stamps — an
    /// internal touch must not leak the engine out of the rotation.
    Internal,
}

/// Why a shard could not be resolved here. Transport-neutral: the HTTP
/// adapter maps these to 409/503/500 in exactly one place.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ResolveError {
    /// The ring assigns the shard to `owner`; the caller redirects there
    /// (a stale router must correct itself, never fence the owner).
    NotOwner {
        prefix: String,
        owner: String,
    },
    /// An open is in flight or held off; retry after `retry_after_secs`.
    Opening {
        prefix: String,
        code: &'static str,
        retry_after_secs: u64,
    },
    OpenFailed {
        prefix: String,
        error: String,
    },
}

/// The fate of a `remove_if` decision, settled under ONE write guard.
pub enum RemoveOutcome {
    /// Removed from the serving map; the caller closes it.
    Removed(Arc<ShardEngine>),
    /// The decision declined: the SAME engine was reinstated under the
    /// same guard — no observable empty-slot window existed.
    Kept,
    /// Nothing was resident under that prefix.
    Absent,
}

/// The shard directory: the topology prefixes, the serving map, the
/// single-flight open gate and the ownership policy, behind narrow
/// methods. Resolution policy (possession yields to the ring, external
/// adoption stamps under the READ guard, single-flight bounded opens)
/// lives here; no caller reaches the map.
#[derive(Clone)]
pub struct ShardDirectory {
    inner: Arc<DirInner>,
}

struct DirInner {
    prefixes: Vec<String>,
    /// Serving map. Created HERE, together with the gate that inserts
    /// into it (PR 6.1-B): no constructor can give the directory and its
    /// gate different maps.
    shards: crate::sharddir::ServingMap,
    gate: OpenGate,
    ownership: crate::ownership::OwnershipService,
    /// How long a request waits on an in-flight open before a retryable
    /// refusal.
    open_wait: Duration,
}

/// The two open timings the directory's gate applies: the ceiling on
/// one open attempt, and how long a request waits on an in-flight open
/// before a retryable refusal.
#[derive(Clone, Copy, Debug)]
pub struct OpenTiming {
    pub open_deadline: Duration,
    pub open_wait: Duration,
}

/// PR 6.1-B: the ONE capability an opener gets from the directory — to
/// report that the engine of a given incarnation closed. It reaches the
/// directory's internals weakly and nothing else: an opener cannot see
/// the runtime, only tell its directory that a resident is gone.
#[derive(Clone)]
pub struct ShardCloseNotifier {
    inner: Weak<DirInner>,
}

impl ShardCloseNotifier {
    /// The engine of `incarnation` closed. Evicts it from the serving
    /// map and arms the anti-flap holdoff — only if it is still the
    /// resident; a late notification for a replaced engine is ignored.
    /// Returns whether it evicted.
    pub fn closed(&self, prefix: &str, incarnation: EngineIncarnation) -> bool {
        match self.inner.upgrade() {
            Some(dir) => dir.gate.notify_closed(prefix, incarnation),
            None => false,
        }
    }
}

impl ShardDirectory {
    /// Build the directory, its serving map and its open gate together.
    /// `opener` receives the close notifier it must wire into every
    /// engine it produces; it captures nothing else of the runtime.
    pub fn new(
        prefixes: Vec<String>,
        ownership: crate::ownership::OwnershipService,
        timing: OpenTiming,
        opener: impl FnOnce(ShardCloseNotifier) -> OpenFn,
    ) -> Self {
        let inner = Arc::new_cyclic(|weak: &Weak<DirInner>| {
            let shards: crate::sharddir::ServingMap = Arc::new(RwLock::new(HashMap::new()));
            let notifier = ShardCloseNotifier {
                inner: weak.clone(),
            };
            let gate = OpenGate::new(shards.clone(), opener(notifier), timing.open_deadline);
            DirInner {
                prefixes,
                shards,
                gate,
                ownership,
                open_wait: timing.open_wait,
            }
        });
        Self { inner }
    }

    /// The close capability for engines opened outside the opener
    /// (tests, adoption paths).
    #[cfg(test)]
    pub fn close_notifier(&self) -> ShardCloseNotifier {
        ShardCloseNotifier {
            inner: Arc::downgrade(&self.inner),
        }
    }

    /// Tests only: the resident's incarnation and a holdoff reset.
    #[cfg(test)]
    pub fn resident_incarnation(&self, prefix: &str) -> Option<EngineIncarnation> {
        self.inner.gate.resident_incarnation(prefix)
    }

    #[cfg(test)]
    pub fn clear_holdoff(&self, prefix: &str) {
        self.inner.gate.clear_holdoff(prefix)
    }

    /// The topology's shard prefixes (layout-4 bit prefixes).
    pub fn prefixes(&self) -> &[String] {
        &self.inner.prefixes
    }

    /// The prefix a route hash lands in.
    pub fn prefix_for(&self, hash: &[u8; 16]) -> String {
        crate::registry::shard_for_hash(&self.inner.prefixes, hash)
    }

    /// Shard engine for `hash`, opening the shard log on first use (which
    /// fences any previous owner). Possession yields to the ring: an
    /// engine this instance still holds for a shard the ring moved away
    /// is closed and the caller redirected, never served from a view
    /// frozen at the fence point. A shard that was just fenced away is
    /// held off (anti-flap while the router converges).
    pub async fn resolve(
        &self,
        hash: &[u8; 16],
        adoption: Adoption,
    ) -> Result<Arc<ShardEngine>, ResolveError> {
        let prefix = self.prefix_for(hash);
        let foreign = self.inner.ownership.foreign_owner(&prefix);
        let external = adoption == Adoption::External;
        if let Some(e) = {
            let guard = self.inner.shards.read().unwrap();
            let e = guard.get(&prefix).map(|r| r.engine.clone());
            // R29 custody: EXTERNAL resolution stamps the adoption
            // sequence and revokes any sweep custody, INSIDE the read
            // guard — the scheduler's close takes the write lock
            // first, so every resolution that could still hold this
            // engine is visible to the close's re-check.
            if external && let Some(ref e) = e {
                crate::billing::stamp_external(e);
            }
            e
        } {
            if foreign.is_none() {
                return Ok(e);
            }
            // Possession must yield to the ring (fleet2 leg C: a scan
            // snapshot froze a live segment at 252 of 510 records).
            if let Some(r) = self.inner.shards.write().unwrap().remove(&prefix) {
                r.engine.begin_close();
            }
        }
        // R2/R3: only the ring owner may claim a shard.
        if let Some(owner) = foreign {
            return Err(ResolveError::NotOwner { prefix, owner });
        }
        // Single-flight open with a bounded wait. A slow WAL replay
        // continues in its own task regardless of what this request does
        // — the caller only ever gets a retryable refusal, never the
        // power to abandon or duplicate an open (the eu-central-1 storm).
        match self
            .inner
            .gate
            .get_or_open(&prefix, self.inner.open_wait)
            .await
        {
            OpenOutcome::Ready(engine) => {
                // R29: a customer who coalesced into (or raced) an open
                // the sweep started still counts as external adoption.
                if external {
                    crate::billing::stamp_external(&engine);
                }
                Ok(engine)
            }
            OpenOutcome::Wait {
                code,
                retry_after_secs,
            } => Err(ResolveError::Opening {
                prefix,
                code,
                retry_after_secs,
            }),
            OpenOutcome::Failed(error) => Err(ResolveError::OpenFailed { prefix, error }),
        }
    }

    /// Open (or join the single-flight open of) `prefix` with an explicit
    /// patience — the fleet's eager move-in and the sweep's discovery,
    /// which honor the same holdoffs as the request path.
    pub async fn open_or_wait(&self, prefix: &str, wait: Duration) -> OpenOutcome {
        self.inner.gate.get_or_open(prefix, wait).await
    }

    /// The resident engine for `prefix`, if open (no adoption stamp).
    pub fn open(&self, prefix: &str) -> Option<Arc<ShardEngine>> {
        self.inner
            .shards
            .read()
            .unwrap()
            .get(prefix)
            .map(|r| r.engine.clone())
    }

    pub fn is_open(&self, prefix: &str) -> bool {
        self.inner.shards.read().unwrap().contains_key(prefix)
    }

    pub fn open_count(&self) -> usize {
        self.inner.shards.read().unwrap().len()
    }

    /// Every open engine — the instance's memory and pipelines. An
    /// owned-but-cold shard is absent BY DESIGN.
    pub fn engines(&self) -> Vec<Arc<ShardEngine>> {
        self.inner
            .shards
            .read()
            .unwrap()
            .values()
            .map(|r| r.engine.clone())
            .collect()
    }

    pub fn engines_by_prefix(&self) -> Vec<(String, Arc<ShardEngine>)> {
        self.inner
            .shards
            .read()
            .unwrap()
            .iter()
            .map(|(p, r)| (p.clone(), r.engine.clone()))
            .collect()
    }

    pub fn held_prefixes(&self) -> Vec<String> {
        self.inner.shards.read().unwrap().keys().cloned().collect()
    }

    /// Drop `prefix` from the serving map; the caller owns the close.
    pub fn evict(&self, prefix: &str) -> Option<Arc<ShardEngine>> {
        self.inner
            .shards
            .write()
            .unwrap()
            .remove(prefix)
            .map(|r| r.engine)
    }

    /// R30: ONE write guard held through remove -> decide -> possible
    /// reinstatement. Releasing the guard between removal and the
    /// decision let a request observe an empty slot and start a SECOND
    /// open while the first engine was about to be reinstated. With the
    /// guard held, external resolution (which stamps under the READ
    /// guard) is strictly ordered against the decision: whatever stamped
    /// before the write lock is visible to `decide`, and nothing can
    /// resolve or re-open the prefix until the slot's fate is settled.
    pub fn remove_if(
        &self,
        prefix: &str,
        decide: impl FnOnce(&Arc<ShardEngine>) -> bool,
    ) -> RemoveOutcome {
        let mut guard = self.inner.shards.write().unwrap();
        let Some(resident) = guard.remove(prefix) else {
            return RemoveOutcome::Absent;
        };
        if decide(&resident.engine) {
            RemoveOutcome::Removed(resident.engine)
        } else {
            guard.insert(prefix.to_string(), resident);
            RemoveOutcome::Kept
        }
    }
}

#[cfg(test)]
mod directory_tests {
    use super::*;
    use crate::ownership::OwnershipService;
    use std::sync::atomic::{AtomicUsize, Ordering};

    /// A directory over an empty serving map whose opener is scripted:
    /// `calls` counts opens, `behave` decides each one's fate.
    fn directory(
        instance: &str,
        behave: impl Fn() -> anyhow::Result<Arc<ShardEngine>> + Send + Sync + 'static,
    ) -> (ShardDirectory, OwnershipService, Arc<AtomicUsize>) {
        let calls = Arc::new(AtomicUsize::new(0));
        let c = calls.clone();
        let opener: OpenFn = Box::new(move |_prefix: String, _inc: EngineIncarnation| {
            c.fetch_add(1, Ordering::Relaxed);
            let r = behave();
            Box::pin(async move { r })
        });
        let ownership = OwnershipService::new(instance);
        let dir = ShardDirectory::new(
            ["00", "01", "10", "11"].map(str::to_string).to_vec(),
            ownership.clone(),
            OpenTiming {
                open_deadline: Duration::from_secs(60),
                open_wait: Duration::from_millis(50),
            },
            |_notifier| opener,
        );
        (dir, ownership, calls)
    }

    fn hash_in(dir: &ShardDirectory, prefix: &str) -> [u8; 16] {
        // Brute-force a route hash that lands in `prefix`.
        for b in 0u8..=255 {
            let mut h = [0u8; 16];
            h[0] = b;
            if dir.prefix_for(&h) == prefix {
                return h;
            }
        }
        panic!("no hash lands in {prefix}");
    }

    /// A shard the ring assigns elsewhere is refused with the owner's
    /// name — and the opener is never consulted (a stale router must
    /// not fence the rightful owner).
    #[tokio::test]
    async fn foreign_shard_is_refused_with_its_owner_and_never_opened() {
        let (dir, ownership, calls) = directory("a", || anyhow::bail!("must not open"));
        ownership.set_ring_active(["a", "b"].map(str::to_string).to_vec());
        let prefix = "01".to_string();
        ownership.set_override(&prefix, "b");
        let Err(err) = dir
            .resolve(&hash_in(&dir, &prefix), Adoption::External)
            .await
        else {
            panic!("a foreign shard must be refused");
        };
        assert_eq!(
            err,
            ResolveError::NotOwner {
                prefix: prefix.clone(),
                owner: "b".into()
            }
        );
        assert_eq!(
            calls.load(Ordering::Relaxed),
            0,
            "no open for a foreign shard"
        );
        assert!(!dir.is_open(&prefix));
    }

    /// An open failure is a typed error carrying the prefix and cause.
    #[tokio::test]
    async fn open_failure_is_typed() {
        let (dir, _o, calls) = directory("", || anyhow::bail!("scripted open failure"));
        let Err(err) = dir.resolve(&hash_in(&dir, "10"), Adoption::Internal).await else {
            panic!("a failed open must be refused");
        };
        match err {
            ResolveError::OpenFailed { prefix, error } => {
                assert_eq!(prefix, "10");
                assert!(error.contains("scripted open failure"), "{error}");
            }
            other => panic!("expected OpenFailed, got {other:?}"),
        }
        assert_eq!(calls.load(Ordering::Relaxed), 1);
    }

    /// An open slower than the caller's patience is a retryable,
    /// typed refusal — never an abandoned or duplicated open.
    #[tokio::test]
    async fn slow_open_is_reported_as_retryable() {
        let (dir, _o, calls) = directory("", || anyhow::bail!("unreachable"));
        // Replace the scripted opener's behaviour by racing the gate:
        // a directory whose opener PENDS is built inline here.
        let calls_pending = Arc::new(AtomicUsize::new(0));
        let c = calls_pending.clone();
        let opener: OpenFn = Box::new(move |_p: String, _inc: EngineIncarnation| {
            c.fetch_add(1, Ordering::Relaxed);
            Box::pin(std::future::pending())
        });
        let slow = ShardDirectory::new(
            dir.prefixes().to_vec(),
            OwnershipService::new(""),
            OpenTiming {
                open_deadline: Duration::from_secs(60),
                open_wait: Duration::from_millis(30),
            },
            |_notifier| opener,
        );
        let Err(err) = slow
            .resolve(&hash_in(&slow, "11"), Adoption::External)
            .await
        else {
            panic!("a slow open must be refused, not awaited");
        };
        match err {
            ResolveError::Opening {
                prefix,
                code,
                retry_after_secs,
            } => {
                assert_eq!(prefix, "11");
                assert_eq!(code, "shard_opening");
                assert!(retry_after_secs >= 1);
            }
            other => panic!("expected Opening, got {other:?}"),
        }
        assert_eq!(
            calls_pending.load(Ordering::Relaxed),
            1,
            "one single-flight open"
        );
        assert_eq!(calls.load(Ordering::Relaxed), 0);
        // A second caller joins the same in-flight open: still one.
        let _ = slow
            .resolve(&hash_in(&slow, "11"), Adoption::External)
            .await;
        assert_eq!(calls_pending.load(Ordering::Relaxed), 1);
    }

    /// The custody primitive on an empty slot reports Absent and the
    /// decision closure is never consulted.
    #[test]
    fn remove_if_on_an_absent_prefix_is_absent() {
        let (dir, _o, _c) = directory("", || anyhow::bail!("unused"));
        let consulted = std::cell::Cell::new(false);
        match dir.remove_if("00", |_| {
            consulted.set(true);
            true
        }) {
            RemoveOutcome::Absent => {}
            _ => panic!("nothing was resident"),
        }
        assert!(!consulted.get());
        assert_eq!(dir.open_count(), 0);
        assert!(dir.held_prefixes().is_empty());
        assert!(dir.evict("00").is_none());
    }

    /// Routing is the topology's bit-prefix hash, exposed once.
    #[test]
    fn prefix_for_is_the_topology_hash() {
        let (dir, _o, _c) = directory("", || anyhow::bail!("unused"));
        assert_eq!(dir.prefixes().len(), 4);
        for prefix in ["00", "01", "10", "11"] {
            let h = hash_in(&dir, prefix);
            assert_eq!(
                dir.prefix_for(&h),
                crate::registry::shard_for_hash(dir.prefixes(), &h)
            );
        }
    }
}
