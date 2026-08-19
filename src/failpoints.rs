//! The SEMANTIC failpoint registry (#108): every failpoint is a typed
//! `Fp` variant — enumerable, describable — backed by ONE state
//! machine keyed by `(Fp, stream name)`. Real functions, real durable
//! writes — the point is to stop BETWEEN them rather than reconstruct
//! the intended post-crash state by hand.
//!
//! Scope note (the audit surface): one injection family lives OUTSIDE
//! this registry BY DESIGN — the shard group-write failure
//! (`shard.rs`, #107) is armed through the ENGINE HANDLE because its
//! state is per-route-hash and per-engine-instance; a process-global
//! registry would cross-arm both engines of a two-engine fleet rig.
//! Everything name-keyed and request-scoped belongs HERE; new
//! failpoints add a variant + a helper pair and NOTHING else.
#![cfg(test)]

//! The SEMANTIC failpoint registry (#108, first increment). Every
//! failpoint is a typed `Fp` variant — enumerable, describable —
//! backed by ONE state machine keyed by `(Fp, stream name)`:
//!
//!   armed    set by a test; a request reaching the site parks (or,
//!            for flags, acts) while armed FOR ITS NAME.
//!   held     the one-shot composite (`pause_oneshot`): arrival
//!            consumes the arm and holds until released, so exactly
//!            one request enters the window and later arms for the
//!            same name cannot leak into it.
//!   arrivals per-(Fp, name) — a test observes ITS request in the
//!            window by ITS stream name. The old per-failpoint
//!            global counters were the parallel-flake family: two
//!            tests watching one counter woke on each other's
//!            parks.
//!
//! Arming and releasing are BOTH per name, and there is
//! deliberately no "release everything": that is what once let one
//! test disarm another's failpoint. The narrative helpers below are
//! one-line sugar over the typed core and double as the site
//! contract documentation; new failpoints add a variant + a helper
//! pair and NOTHING else.
use std::collections::HashMap;
use std::sync::Mutex;

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum Fp {
    // Flags (the site checks and acts; nothing parks).
    StopAfterTombstone,
    StopBeforeMarkCommitted,
    StopAfterSealIntent,
    // Parks (the site awaits release).
    CreateBeforeReady,
    AppendBeforeEnqueue,
    CloseBeforeEnqueue, // one-shot composite: consume arm, hold
    CloseBeforeMark,
    ProductSealBeforeClaim,
    ProductFinalBeforeAppend,
    ForkBeforeSourceRef,
    InitBeforeSeed,
    ForkAfterSourceRef,
    ReleaseAfterEpochCheck,
    PullBeforeReceive,
    ConsumerSagaBeforeRefresh,
    DeleteBeforeDecision,
    ScalerBeforePublish,
}

impl Fp {
    pub const ALL: [Fp; 17] = [
        Fp::StopAfterTombstone,
        Fp::StopBeforeMarkCommitted,
        Fp::StopAfterSealIntent,
        Fp::CreateBeforeReady,
        Fp::AppendBeforeEnqueue,
        Fp::CloseBeforeEnqueue,
        Fp::CloseBeforeMark,
        Fp::ProductSealBeforeClaim,
        Fp::ProductFinalBeforeAppend,
        Fp::ForkBeforeSourceRef,
        Fp::InitBeforeSeed,
        Fp::ForkAfterSourceRef,
        Fp::ReleaseAfterEpochCheck,
        Fp::PullBeforeReceive,
        Fp::ConsumerSagaBeforeRefresh,
        Fp::DeleteBeforeDecision,
        Fp::ScalerBeforePublish,
    ];

    /// The site contract: WHERE the point fires, stated as the
    /// window it opens. This is the enumerable registry the DST
    /// program audits against.
    pub fn site(self) -> &'static str {
        match self {
            Fp::StopAfterTombstone => {
                "delete cascade: after the named generation is tombstoned \
                 and its debt recorded, before the parent ref releases"
            }
            Fp::StopBeforeMarkCommitted => {
                "close: after the final append is durable, before \
                 mark_final_committed"
            }
            Fp::StopAfterSealIntent => {
                "seal: after the seal intent publishes, before the \
                 committer sees it"
            }
            Fp::CreateBeforeReady => {
                "create: after the fork reference installs, before \
                 readiness publishes"
            }
            Fp::AppendBeforeEnqueue => "append: before the committer enqueue",
            Fp::CloseBeforeEnqueue => {
                "close: before its enqueue; ONE-SHOT — first arrival \
                 consumes the arm and holds until release"
            }
            Fp::CloseBeforeMark => {
                "close: between the acknowledged final append and \
                 mark_final_committed"
            }
            Fp::ProductSealBeforeClaim => "product seal: before the claim CAS",
            Fp::ProductFinalBeforeAppend => "product seal: before the final-bearing append submits",
            Fp::ForkBeforeSourceRef => "fork create: before the source reference installs",
            Fp::InitBeforeSeed => "create: before the tail row seeds",
            Fp::ForkAfterSourceRef => "fork create: after the source reference installs",
            Fp::ReleaseAfterEpochCheck => {
                "fork-ref release: after the incarnation epoch check, \
                 before the release write"
            }
            Fp::PullBeforeReceive => {
                "consumer pull: after config load, before the Receive \
                 submits"
            }
            Fp::ConsumerSagaBeforeRefresh => {
                "consumer deletion saga: before a fan-out round's \
                 descriptor refresh"
            }
            Fp::DeleteBeforeDecision => "stream delete: before the soft-versus-hard decision",
            Fp::ScalerBeforePublish => {
                "scaler two-phase transition: between the parent seal \
                 and the successor-publication CAS — every resume of \
                 the NAMED stream parks (readers only ever spawn \
                 resume, so no request blocks here)"
            }
        }
    }
}

#[derive(Default)]
struct FpState {
    armed: bool,
    held: bool,
    arrivals: usize,
}

impl Fp {
    fn idx(self) -> usize {
        Fp::ALL.iter().position(|f| *f == self).expect("in ALL")
    }
}

/// Per-failpoint count of ARMED-or-HELD entries across all names —
/// the lock-free fast path. Site checks (`hit`/`pause`) run on
/// EVERY request in test builds; with nothing armed for a
/// failpoint they must cost one relaxed load, not a global lock
/// plus a key allocation (a throughput-gate test caught the
/// difference within the suite).
static ACTIVE: [std::sync::atomic::AtomicUsize; Fp::ALL.len()] =
    [const { std::sync::atomic::AtomicUsize::new(0) }; Fp::ALL.len()];

fn active(fp: Fp) -> bool {
    ACTIVE[fp.idx()].load(std::sync::atomic::Ordering::Acquire) > 0
}

fn reg() -> &'static Mutex<HashMap<(Fp, String), FpState>> {
    static M: std::sync::OnceLock<Mutex<HashMap<(Fp, String), FpState>>> =
        std::sync::OnceLock::new();
    M.get_or_init(|| Mutex::new(HashMap::new()))
}

fn gate() -> &'static tokio::sync::Notify {
    static N: std::sync::OnceLock<tokio::sync::Notify> = std::sync::OnceLock::new();
    N.get_or_init(tokio::sync::Notify::new)
}

pub fn arm(fp: Fp, name: &str) {
    let mut g = reg().lock().unwrap();
    let st = g.entry((fp, name.to_string())).or_default();
    if !st.armed && !st.held {
        ACTIVE[fp.idx()].fetch_add(1, std::sync::atomic::Ordering::Release);
    }
    st.armed = true;
}

pub fn release(fp: Fp, name: &str) {
    if let Some(st) = reg().lock().unwrap().get_mut(&(fp, name.to_string())) {
        if st.armed || st.held {
            ACTIVE[fp.idx()].fetch_sub(1, std::sync::atomic::Ordering::Release);
        }
        st.armed = false;
        st.held = false;
    }
    gate().notify_waiters();
}

/// Requests of `name` that have ARRIVED at this failpoint since the
/// process started (monotone; per name, so parallel tests compose
/// without watching each other's parks).
pub fn parked(fp: Fp, name: &str) -> usize {
    reg()
        .lock()
        .unwrap()
        .get(&(fp, name.to_string()))
        .map_or(0, |st| st.arrivals)
}

fn is_armed(fp: Fp, name: &str) -> bool {
    reg()
        .lock()
        .unwrap()
        .get(&(fp, name.to_string()))
        .is_some_and(|st| st.armed)
}

fn is_held(fp: Fp, name: &str) -> bool {
    reg()
        .lock()
        .unwrap()
        .get(&(fp, name.to_string()))
        .is_some_and(|st| st.held)
}

/// Flag check: consume-free "is this site sabotaged for name".
pub(crate) fn hit(fp: Fp, name: &str) -> bool {
    if !active(fp) {
        return false;
    }
    is_armed(fp, name)
}

/// Wait until this failpoint is released for `name`, counting the
/// arrival exactly once so a test can OBSERVE that its request
/// really is in the window instead of sleeping and hoping.
pub(crate) async fn pause(fp: Fp, name: &str) {
    if !active(fp) {
        return;
    }
    let mut counted = false;
    loop {
        {
            let mut g = reg().lock().unwrap();
            let Some(st) = g.get_mut(&(fp, name.to_string())) else {
                return;
            };
            if !st.armed {
                return;
            }
            if !counted {
                counted = true;
                st.arrivals += 1;
            }
        }
        let n = gate().notified();
        if !is_armed(fp, name) {
            return;
        }
        n.await;
    }
}

/// One-shot park: the FIRST arrival consumes the arm and holds
/// until released; later requests for the same name sail through.
pub(crate) async fn pause_oneshot(fp: Fp, name: &str) {
    if !active(fp) {
        return;
    }
    {
        let mut g = reg().lock().unwrap();
        let Some(st) = g.get_mut(&(fp, name.to_string())) else {
            return;
        };
        if !st.armed {
            return;
        }
        // Armed -> Held is not a deactivation: ACTIVE keeps its
        // count until release() clears the hold.
        st.armed = false;
        st.held = true;
        st.arrivals += 1;
    }
    loop {
        if !is_held(fp, name) {
            return;
        }
        let n = gate().notified();
        if !is_held(fp, name) {
            return;
        }
        n.await;
    }
}

// ---- narrative sugar (the site contract, one line each) ----------

pub fn stop_after_tombstone(name: &str) {
    arm(Fp::StopAfterTombstone, name);
}
pub fn stop_after_tombstone_off(name: &str) {
    release(Fp::StopAfterTombstone, name);
}
pub(crate) fn should_stop_after_tombstone(name: &str) -> bool {
    hit(Fp::StopAfterTombstone, name)
}
pub fn stop_before_mark_committed(name: &str) {
    arm(Fp::StopBeforeMarkCommitted, name);
}
pub fn stop_before_mark_committed_off(name: &str) {
    release(Fp::StopBeforeMarkCommitted, name);
}
pub(crate) fn should_stop_before_mark_committed(name: &str) -> bool {
    hit(Fp::StopBeforeMarkCommitted, name)
}
pub fn stop_after_seal_intent(name: &str) {
    arm(Fp::StopAfterSealIntent, name);
}
pub fn stop_after_seal_intent_off(name: &str) {
    release(Fp::StopAfterSealIntent, name);
}
pub(crate) fn should_stop_after_seal_intent(name: &str) -> bool {
    hit(Fp::StopAfterSealIntent, name)
}

pub fn park_create_before_ready(name: &str) {
    arm(Fp::CreateBeforeReady, name);
}
pub fn release_create_before_ready(name: &str) {
    release(Fp::CreateBeforeReady, name);
}
pub(crate) async fn pause_create_before_ready(name: &str) {
    pause(Fp::CreateBeforeReady, name).await;
}

pub fn park_append_before_enqueue(name: &str) {
    arm(Fp::AppendBeforeEnqueue, name);
}
pub fn release_append_before_enqueue(name: &str) {
    release(Fp::AppendBeforeEnqueue, name);
}
pub(crate) async fn pause_append_before_enqueue(name: &str) {
    pause(Fp::AppendBeforeEnqueue, name).await;
}

pub fn park_close_before_enqueue(name: &str) {
    arm(Fp::CloseBeforeEnqueue, name);
}
pub fn release_close_before_enqueue(name: &str) {
    release(Fp::CloseBeforeEnqueue, name);
}
pub(crate) async fn pause_close_before_enqueue(name: &str) {
    pause_oneshot(Fp::CloseBeforeEnqueue, name).await;
}

pub fn park_close_before_mark(name: &str) {
    arm(Fp::CloseBeforeMark, name);
}
pub fn release_close_before_mark(name: &str) {
    release(Fp::CloseBeforeMark, name);
}
pub(crate) async fn pause_close_before_mark(name: &str) {
    pause(Fp::CloseBeforeMark, name).await;
}

pub fn park_product_seal_before_claim(name: &str) {
    arm(Fp::ProductSealBeforeClaim, name);
}
pub fn release_product_seal_before_claim(name: &str) {
    release(Fp::ProductSealBeforeClaim, name);
}
pub async fn pause_product_seal_before_claim(name: &str) {
    pause(Fp::ProductSealBeforeClaim, name).await;
}

pub fn park_product_final_before_append(name: &str) {
    arm(Fp::ProductFinalBeforeAppend, name);
}
pub fn release_product_final_before_append(name: &str) {
    release(Fp::ProductFinalBeforeAppend, name);
}
pub async fn pause_product_final_before_append(name: &str) {
    pause(Fp::ProductFinalBeforeAppend, name).await;
}

pub fn park_fork_before_source_ref(name: &str) {
    arm(Fp::ForkBeforeSourceRef, name);
}
pub fn release_fork_before_source_ref(name: &str) {
    release(Fp::ForkBeforeSourceRef, name);
}
pub(crate) async fn pause_fork_before_source_ref(name: &str) {
    pause(Fp::ForkBeforeSourceRef, name).await;
}

pub fn park_init_before_seed(name: &str) {
    arm(Fp::InitBeforeSeed, name);
}
pub fn release_init_before_seed(name: &str) {
    release(Fp::InitBeforeSeed, name);
}
pub(crate) async fn pause_init_before_seed(name: &str) {
    pause(Fp::InitBeforeSeed, name).await;
}

pub fn park_fork_after_source_ref(name: &str) {
    arm(Fp::ForkAfterSourceRef, name);
}
pub fn release_fork_after_source_ref(name: &str) {
    release(Fp::ForkAfterSourceRef, name);
}
pub(crate) async fn pause_fork_after_source_ref(name: &str) {
    pause(Fp::ForkAfterSourceRef, name).await;
}

pub fn park_release_after_epoch_check(name: &str) {
    arm(Fp::ReleaseAfterEpochCheck, name);
}
pub fn release_release_after_epoch_check(name: &str) {
    release(Fp::ReleaseAfterEpochCheck, name);
}
pub(crate) async fn pause_release_after_epoch_check(name: &str) {
    pause(Fp::ReleaseAfterEpochCheck, name).await;
}

pub fn park_pull_before_receive(name: &str) {
    arm(Fp::PullBeforeReceive, name);
}
pub fn release_pull_before_receive(name: &str) {
    release(Fp::PullBeforeReceive, name);
}
pub(crate) async fn pause_pull_before_receive(name: &str) {
    pause(Fp::PullBeforeReceive, name).await;
}

pub fn park_consumer_saga_before_refresh(name: &str) {
    arm(Fp::ConsumerSagaBeforeRefresh, name);
}
pub fn release_consumer_saga_before_refresh(name: &str) {
    release(Fp::ConsumerSagaBeforeRefresh, name);
}
pub(crate) async fn pause_consumer_saga_before_refresh(name: &str) {
    pause(Fp::ConsumerSagaBeforeRefresh, name).await;
}

pub fn park_delete_before_decision(name: &str) {
    arm(Fp::DeleteBeforeDecision, name);
}
pub fn release_delete_before_decision(name: &str) {
    release(Fp::DeleteBeforeDecision, name);
}
pub(crate) async fn pause_delete_before_decision(name: &str) {
    pause(Fp::DeleteBeforeDecision, name).await;
}
pub fn arm_scaler_before_publish(name: &str) {
    arm(Fp::ScalerBeforePublish, name);
}
pub fn release_scaler_before_publish(name: &str) {
    release(Fp::ScalerBeforePublish, name);
}
pub(crate) async fn pause_scaler_before_publish(name: &str) {
    pause(Fp::ScalerBeforePublish, name).await;
}
