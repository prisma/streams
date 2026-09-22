//! The anti-flap ledger's verdict: which departures are EVIDENCE of a sick
//! prefix (a strike, escalating the holdoff) and which are releases the
//! runtime chose (the base holdoff, the ledger untouched).
//!
//! Review rank 12: the billing sweep opens a cold shard, probes it and
//! closes it within seconds, every cycle (`billing::sweep_owned_outboxes`,
//! `close_scheduler_engine`). PR 6.1.1-B routed that close through the same
//! arming body as an engine that died young, so each cycle earned a strike.
//! Strikes decay only when an engine outlives `SHORT_LIVED`, which a probe
//! engine never does, and the sweep's 300 s cadence always outlasts the 60 s
//! cap: every cold shard drifted to the ceiling, and a customer's first
//! request after a sweep close met 503 `shard_moving` with a Retry-After of
//! up to a minute.

use std::time::Duration;

use super::{HOLDOFF_BASE, SHORT_LIVED, holdoff_for};
use crate::shard_directory::RetirementReason;

/// How a resident left the serving map.
#[derive(Clone, Copy, Debug)]
pub(super) enum Departure {
    /// The engine reported its own close: fenced by a new owner, or a fatal
    /// store fault (`OpenGate::notify_closed`).
    Died,
    /// A caller retired it for the reason it stated (`OpenGate::retire_resident`).
    Retired(RetirementReason),
}

impl Departure {
    /// The ONE strike decision. Only evidence of a sick prefix escalates: an
    /// engine that died, or that the ring moved or the fleet evicted while
    /// young; rapid open-then-die cycles ARE the storm the gate exists to
    /// prevent. A release the runtime chose says nothing about the store.
    fn is_evidence(self) -> bool {
        use RetirementReason::{FleetEviction, OwnershipMoved, Shutdown, SweepEviction};
        match self {
            Departure::Died | Departure::Retired(OwnershipMoved | FleetEviction) => true,
            Departure::Retired(SweepEviction | Shutdown) => false,
        }
    }
}

/// The ledger after a departure: the strikes the prefix carries and the
/// holdoff before its next open. Pure over explicit inputs, so the
/// `SHORT_LIVED` line is a test input and never a wall-clock race.
pub(super) fn ledger_after(
    departure: Departure,
    lived: Option<Duration>,
    strikes: u32,
) -> (u32, Duration) {
    if !departure.is_evidence() {
        return (strikes, HOLDOFF_BASE);
    }
    match lived {
        Some(l) if l >= SHORT_LIVED => (0, HOLDOFF_BASE),
        _ => {
            let strikes = strikes.saturating_add(1);
            (strikes, holdoff_for(strikes))
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use super::{Departure, ledger_after};
    use crate::shard_directory::{OpenTiming, RetireOutcome, RetirementReason, ShardDirectory};
    use crate::sharddir::{
        EngineIncarnation, HOLDOFF_BASE, OpenFn, OpenOutcome, SHORT_LIVED, holdoff_for,
    };

    const PREFIX: &str = "0";
    /// Bounded: a reopen that never becomes Ready fails by assertion, never
    /// hangs. A reopen waits for the previous incarnation's termination,
    /// which includes the worker grace under the mutation profile.
    const OPEN_WAIT: Duration = Duration::from_secs(30);

    /// A real engine over an in-memory store (`unwind::tests` keeps the same shape).
    async fn open_engine(prefix: &str) -> Arc<crate::shard::ShardEngine> {
        let store: Arc<dyn object_store::ObjectStore> =
            Arc::new(object_store::memory::InMemory::new());
        let db = slatedb::Db::builder(prefix, store.clone())
            .build()
            .await
            .expect("open db");
        let (absorb_tx, _absorb_rx) = crate::history::absorber_channel();
        let maintenance = crate::shard::load_or_rebuild_maintenance(&db)
            .await
            .expect("load maintenance");
        crate::shard::ShardEngine::start(
            prefix.to_string(),
            Arc::new(db),
            store,
            crate::shard::ShardConfig::default(),
            absorb_tx,
            None,
            maintenance,
        )
    }

    /// A directory whose opener produces real engines: retirement runs the real
    /// close path and a reopen waits for the real termination.
    fn directory() -> ShardDirectory {
        let opener: OpenFn = Box::new(|prefix: String, _inc: EngineIncarnation| {
            Box::pin(async move { Ok(open_engine(&prefix).await) })
        });
        ShardDirectory::new(
            vec![PREFIX.to_string()],
            crate::ownership::OwnershipService::new(""),
            OpenTiming {
                open_deadline: Duration::from_secs(60),
                open_wait: Duration::from_millis(50),
            },
            |_notifier| opener,
        )
    }

    async fn open(dir: &ShardDirectory) -> EngineIncarnation {
        match dir.open_or_wait(PREFIX, OPEN_WAIT).await {
            OpenOutcome::Ready(_) => dir.resident_incarnation(PREFIX).expect("resident"),
            OpenOutcome::Wait { code, .. } => panic!("open must be ready, got Wait({code})"),
            OpenOutcome::Failed(e) => panic!("open failed: {e}"),
        }
    }

    /// The prefix's ledger as the gate holds it: strikes and the holdoff left.
    fn ledger(dir: &ShardDirectory) -> (u32, Option<Duration>) {
        let gate = dir.gate_for_tests();
        let st = gate.inner.st.lock().unwrap();
        let g = st.get(PREFIX).expect("gate state for the prefix");
        let left = g
            .holdoff_until
            .map(|until| until.saturating_duration_since(Instant::now()));
        (g.strikes, left)
    }

    /// Forget the holdoff and ONLY the holdoff (`clear_holdoff` also zeroes the
    /// strikes), so a reopen is allowed at once while the ledger is exactly what
    /// the previous departure left.
    fn forget_holdoff(dir: &ShardDirectory) {
        let gate = dir.gate_for_tests();
        if let Some(g) = gate.inner.st.lock().unwrap().get_mut(PREFIX) {
            g.holdoff_until = None;
        }
    }

    fn retired(dir: &ShardDirectory, reason: RetirementReason) {
        assert!(
            matches!(
                dir.retire(PREFIX, reason, |_, _| true),
                RetireOutcome::Retired(_)
            ),
            "{reason:?}: the resident was there to retire"
        );
    }

    /// Review rank 12: the billing sweep opens a cold shard, probes it and closes
    /// it within seconds, every cycle. Judged like an engine that died young,
    /// every cycle earned a strike, strikes never decayed, and cold shards
    /// drifted to the 60 s ceiling. A release the runtime chose is not evidence:
    /// the base holdoff, and the ledger exactly as it was.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_sweep_eviction_or_shutdown_leaves_the_strike_ledger_alone() {
        let dir = directory();
        let cycles = [
            RetirementReason::SweepEviction,
            RetirementReason::Shutdown,
            RetirementReason::SweepEviction,
        ];
        for (cycle, reason) in cycles.into_iter().enumerate() {
            open(&dir).await;
            retired(&dir, reason);
            let (strikes, left) = ledger(&dir);
            assert_eq!(strikes, 0, "cycle {cycle}: {reason:?} is not a strike");
            let left = left.expect("a retirement still arms the holdoff (PR 6.1.1-B)");
            assert!(
                left <= HOLDOFF_BASE,
                "cycle {cycle}: {reason:?} arms the base holdoff, not an escalated one ({left:?})"
            );
            assert!(matches!(
                dir.open_or_wait(PREFIX, Duration::from_millis(20)).await,
                OpenOutcome::Wait {
                    code: "shard_moving",
                    ..
                }
            ));
            forget_holdoff(&dir);
        }
    }

    /// The storm signature still escalates: an engine that dies, or that the ring
    /// moves or the fleet evicts while young, is a strike every time.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_young_engine_that_dies_or_is_moved_away_still_strikes() {
        let dir = directory();
        let inc = open(&dir).await;
        let gate = dir.gate_for_tests();
        assert!(
            gate.notify_closed(PREFIX, inc),
            "the live incarnation evicts itself"
        );
        assert!(
            !dir.is_open(PREFIX),
            "an engine that died is gone from the map"
        );
        assert!(!gate.notify_closed(PREFIX, inc), "a stale close is a no-op");
        assert_eq!(ledger(&dir).0, 1, "an engine that died young is a strike");
        forget_holdoff(&dir);
        for (expected, reason) in [
            (2, RetirementReason::OwnershipMoved),
            (3, RetirementReason::FleetEviction),
        ] {
            open(&dir).await;
            retired(&dir, reason);
            let (strikes, left) = ledger(&dir);
            assert_eq!(
                strikes, expected,
                "{reason:?} of a young engine is a strike"
            );
            // 12 s / 24 s minus microseconds: a > 9 s stall between two adjacent
            // statements would be needed to read this below the 3 s base.
            assert!(
                left.expect("armed") > HOLDOFF_BASE,
                "{reason:?}: an escalated holdoff, got {left:?}"
            );
            forget_holdoff(&dir);
        }
    }

    /// The ledger over explicit inputs: the `SHORT_LIVED` line is a test input,
    /// never a wall-clock race. Every `cargo mutants` mutant of this module
    /// dies here.
    #[test]
    fn the_ledger_judges_evidence_by_lifetime_and_ignores_releases() {
        let died = Departure::Died;
        let moved = Departure::Retired(RetirementReason::OwnershipMoved);
        let evicted = Departure::Retired(RetirementReason::FleetEviction);
        let swept = Departure::Retired(RetirementReason::SweepEviction);
        let shutdown = Departure::Retired(RetirementReason::Shutdown);
        let one_ns = Duration::from_nanos(1);
        // Evidence: a young engine is a strike, escalating; a long-lived one resets.
        assert_eq!(ledger_after(died, None, 0), (1, holdoff_for(1)));
        assert_eq!(
            ledger_after(moved, Some(Duration::ZERO), 1),
            (2, holdoff_for(2))
        );
        assert_eq!(
            ledger_after(evicted, Some(SHORT_LIVED - one_ns), 4),
            (5, holdoff_for(5))
        );
        assert_eq!(ledger_after(died, Some(SHORT_LIVED), 4), (0, HOLDOFF_BASE));
        assert_eq!(
            ledger_after(moved, Some(SHORT_LIVED * 2), 1),
            (0, HOLDOFF_BASE)
        );
        // Releases: the base holdoff, the ledger untouched: never reset, never grown.
        assert_eq!(
            ledger_after(swept, Some(Duration::ZERO), 0),
            (0, HOLDOFF_BASE)
        );
        assert_eq!(
            ledger_after(swept, Some(Duration::ZERO), 3),
            (3, HOLDOFF_BASE)
        );
        assert_eq!(
            ledger_after(shutdown, Some(SHORT_LIVED * 2), 3),
            (3, HOLDOFF_BASE)
        );
        assert_eq!(
            ledger_after(swept, None, u32::MAX),
            (u32::MAX, HOLDOFF_BASE)
        );
        assert_eq!(holdoff_for(5), Duration::from_secs(60), "the cap");
    }
}
