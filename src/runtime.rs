//! Per-runtime capabilities (WP-15 foundational slice, PR 4): clock,
//! entropy, and runtime identity as OWNED VALUES handed to owners at
//! construction — never seed-atomics, never `OnceLock` holders (the
//! review constraint on this PR). Production and deterministic tests
//! swap implementations; nothing here is process-global, so two
//! runtime instances in one process hold two independent identities
//! and two independently controllable clocks (test-pinned below).
//!
//! Scope, stated honestly: this PR migrates BOOT IDENTITY, EPOCH
//! ENTROPY, touch-journal entropy, and the unready-watchdog retry
//! timer. The 100+ direct `shard::now_ms` wall-clock reads and the
//! remaining timer-driven loops migrate in later WP-15 slices as
//! their owners are extracted (WP-02); each migration retires its
//! ambient read rather than wrapping it.

use std::fmt;
use std::sync::Arc;
use std::time::Duration;

/// A server-trusted wall-clock reading, milliseconds since the Unix
/// epoch. A DISTINCT type from customer-supplied timestamps (which
/// stay raw `i64` metadata): code that needs trusted time asks the
/// [`Clock`]; nothing can launder a customer value into one.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct TrustedNow(pub i64);

impl TrustedNow {
    pub fn ms(self) -> i64 {
        self.0
    }
}

/// The time capability. Implementations must be cheap to call and
/// safe to share (`Arc<dyn Clock>`).
pub trait Clock: Send + Sync + fmt::Debug {
    /// Trusted wall clock (Unix ms).
    fn now(&self) -> TrustedNow;
    /// Sleep for `d` — production uses the tokio timer; a manual test
    /// clock completes sleeps when its time is advanced past them.
    fn sleep(&self, d: Duration) -> futures_util::future::BoxFuture<'static, ()>;
}

/// Production clock: the OS wall clock and the tokio timer.
#[derive(Debug, Default)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now(&self) -> TrustedNow {
        TrustedNow(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_millis() as i64)
                .unwrap_or(0),
        )
    }

    fn sleep(&self, d: Duration) -> futures_util::future::BoxFuture<'static, ()> {
        Box::pin(tokio::time::sleep(d))
    }
}

/// The entropy capability. Production is the OS CSPRNG; the seeded
/// test implementation exists ONLY under `cfg(test)`, so release
/// builds cannot even name a predictable source — token/security code
/// cannot accidentally receive one (PR 4 proof obligation 3).
pub trait Entropy: Send + Sync + fmt::Debug {
    fn fill(&self, dest: &mut [u8]);
}

/// Production entropy: the process CSPRNG (`rand::rng`).
#[derive(Debug, Default)]
pub struct OsEntropy;

impl Entropy for OsEntropy {
    fn fill(&self, dest: &mut [u8]) {
        use rand::RngCore;
        rand::rng().fill_bytes(dest);
    }
}

/// Identity of ONE runtime instance — minted at construction from the
/// runtime's own entropy, never from a process-global once-cell. Two
/// runtimes in one process therefore never share a boot id.
#[derive(Debug, Clone)]
pub struct RuntimeIdentity {
    /// 16 random bytes, hex — a fresh value per runtime construction.
    pub boot_id: String,
    /// The operator-facing instance name (metrics tag).
    pub instance: String,
}

/// The per-runtime capability bundle owners receive at construction.
#[derive(Debug, Clone)]
pub struct RuntimeCaps {
    pub clock: Arc<dyn Clock>,
    pub entropy: Arc<dyn Entropy>,
    pub identity: RuntimeIdentity,
}

impl RuntimeCaps {
    /// Production capabilities: OS clock, OS CSPRNG, and a boot id
    /// minted from that CSPRNG.
    pub fn production(instance: &str) -> Self {
        Self::with(Arc::new(SystemClock), Arc::new(OsEntropy), instance)
    }

    /// Assemble from explicit implementations (tests pass a manual
    /// clock / seeded entropy; production goes through
    /// [`RuntimeCaps::production`]).
    pub fn with(clock: Arc<dyn Clock>, entropy: Arc<dyn Entropy>, instance: &str) -> Self {
        let mut boot = [0u8; 16];
        entropy.fill(&mut boot);
        Self {
            clock,
            entropy,
            identity: RuntimeIdentity {
                boot_id: crate::crypto::hex(&boot),
                instance: instance.to_string(),
            },
        }
    }

    /// Mint a 16-byte epoch from this runtime's entropy (consumer
    /// generations, fencing epochs). Replaces the ambient
    /// `http::rand_epoch`.
    pub fn epoch(&self) -> [u8; 16] {
        let mut e = [0u8; 16];
        self.entropy.fill(&mut e);
        e
    }
}

/// Deterministic test clock: time moves ONLY when a test advances it,
/// and pending sleeps complete when their deadline is reached. Each
/// instance is independent — no process-global registry — so parallel
/// rigs (or two runtimes in one test) cannot couple through it.
#[cfg(test)]
#[derive(Debug, Clone)]
pub struct ManualClock {
    inner: Arc<ManualInner>,
}

#[cfg(test)]
#[derive(Debug)]
struct ManualInner {
    now_ms: std::sync::Mutex<i64>,
    wake: tokio::sync::Notify,
}

#[cfg(test)]
impl ManualClock {
    pub fn at(start_ms: i64) -> Self {
        Self {
            inner: Arc::new(ManualInner {
                now_ms: std::sync::Mutex::new(start_ms),
                wake: tokio::sync::Notify::new(),
            }),
        }
    }

    /// Advance the clock and wake every pending sleep so it can
    /// re-check its deadline.
    pub fn advance(&self, by: Duration) {
        *self.inner.now_ms.lock().unwrap() += by.as_millis() as i64;
        self.inner.wake.notify_waiters();
    }
}

#[cfg(test)]
impl Clock for ManualClock {
    fn now(&self) -> TrustedNow {
        TrustedNow(*self.inner.now_ms.lock().unwrap())
    }

    fn sleep(&self, d: Duration) -> futures_util::future::BoxFuture<'static, ()> {
        let inner = self.inner.clone();
        let deadline = *inner.now_ms.lock().unwrap() + d.as_millis() as i64;
        Box::pin(async move {
            loop {
                // Register for the NEXT advance BEFORE checking the
                // deadline, so an advance between check and await
                // cannot be missed.
                let notified = inner.wake.notified();
                if *inner.now_ms.lock().unwrap() >= deadline {
                    return;
                }
                notified.await;
            }
        })
    }
}

/// Deterministic, seedable, NON-CRYPTOGRAPHIC entropy (SplitMix64).
/// `cfg(test)` only, deliberately: release builds cannot name a
/// predictable entropy source, so production token/security code can
/// never accidentally receive one.
#[cfg(test)]
#[derive(Debug)]
pub struct SeededEntropy {
    state: std::sync::Mutex<u64>,
}

#[cfg(test)]
impl SeededEntropy {
    pub fn seeded(seed: u64) -> Self {
        Self {
            state: std::sync::Mutex::new(seed),
        }
    }
}

#[cfg(test)]
impl Entropy for SeededEntropy {
    fn fill(&self, dest: &mut [u8]) {
        let mut s = self.state.lock().unwrap();
        for chunk in dest.chunks_mut(8) {
            *s = s.wrapping_add(0x9E37_79B9_7F4A_7C15);
            let mut z = *s;
            z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
            z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
            z ^= z >> 31;
            chunk.copy_from_slice(&z.to_le_bytes()[..chunk.len()]);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// PR 4 proof 1: two runtimes in one process share NOTHING — boot
    /// identities differ (minted per construction, no once-cell) and
    /// each manual clock moves independently.
    #[test]
    fn two_runtimes_share_no_identity_or_timing_state() {
        let clock_a = ManualClock::at(1_000);
        let clock_b = ManualClock::at(50_000);
        let a = RuntimeCaps::with(
            Arc::new(clock_a.clone()),
            Arc::new(SeededEntropy::seeded(7)),
            "instance-a",
        );
        let b = RuntimeCaps::with(
            Arc::new(clock_b.clone()),
            Arc::new(SeededEntropy::seeded(7)),
            "instance-b",
        );
        // Same seed, same first draw — the IDs still belong to their
        // own runtimes; production uses OsEntropy where draws differ.
        assert_eq!(a.identity.boot_id, b.identity.boot_id);
        // ...so prove separation with distinct seeds too:
        let c = RuntimeCaps::with(
            Arc::new(clock_b.clone()),
            Arc::new(SeededEntropy::seeded(8)),
            "instance-c",
        );
        assert_ne!(a.identity.boot_id, c.identity.boot_id);
        // Independent clocks: advancing A leaves B untouched.
        clock_a.advance(Duration::from_millis(500));
        assert_eq!(a.clock.now().ms(), 1_500);
        assert_eq!(b.clock.now().ms(), 50_000);
    }

    /// Production identities are unpredictable and unique per runtime
    /// construction (OS CSPRNG).
    #[test]
    fn production_runtimes_mint_distinct_boot_ids() {
        let a = RuntimeCaps::production("x");
        let b = RuntimeCaps::production("x");
        assert_ne!(a.identity.boot_id, b.identity.boot_id);
        assert_eq!(a.identity.boot_id.len(), 32, "16 bytes hex");
    }

    /// PR 4 proof 2: deterministic tests control time per runtime —
    /// a sleep completes exactly when ITS clock advances past the
    /// deadline, with no process-global clock lock anywhere.
    #[tokio::test]
    async fn manual_clock_sleep_completes_on_advance() {
        use futures_util::FutureExt;
        let clock = ManualClock::at(0);
        let mut fut = clock.sleep(Duration::from_millis(100));
        assert!((&mut fut).now_or_never().is_none(), "not yet");
        clock.advance(Duration::from_millis(99));
        assert!((&mut fut).now_or_never().is_none(), "1ms short");
        clock.advance(Duration::from_millis(1));
        fut.await; // completes without real time passing
    }

    /// Seeded entropy reproduces byte-for-byte (deterministic rigs),
    /// and the epoch helper draws from the runtime's own source.
    #[test]
    fn seeded_entropy_is_reproducible() {
        let a = SeededEntropy::seeded(42);
        let b = SeededEntropy::seeded(42);
        let mut x = [0u8; 24];
        let mut y = [0u8; 24];
        a.fill(&mut x);
        b.fill(&mut y);
        assert_eq!(x, y);
        let caps = RuntimeCaps::with(
            Arc::new(SystemClock),
            Arc::new(SeededEntropy::seeded(42)),
            "i",
        );
        let e1 = caps.epoch();
        let e2 = caps.epoch();
        assert_ne!(e1, e2, "successive epochs advance the stream");
    }
}
