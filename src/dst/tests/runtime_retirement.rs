//! Runtime retirement.

use super::fixture_http::{engine_shutdown, http_rig};
use super::fixture_requests::hreq;
use super::fixture_storage::{mem, open_engine};
use object_store::ObjectStore;
use std::sync::Arc;

/// PR 6.1.1-B: an engine that reports its own close, the way the
/// production opener and the principal rig wire it.
async fn open_engine_with_on_close(
    store: Arc<dyn ObjectStore>,
    prefix: &str,
    on_close: Arc<dyn Fn() + Send + Sync>,
) -> Arc<crate::shard::ShardEngine> {
    let db = slatedb::Db::builder(prefix, store.clone())
        .with_settings(slatedb::config::Settings {
            flush_interval: Some(std::time::Duration::from_millis(5)),
            manifest_poll_interval: std::time::Duration::from_millis(50),
            ..Default::default()
        })
        .build()
        .await
        .expect("open db");
    let (absorb_tx, _absorb_rx) = crate::history::absorber_channel();
    let maint = crate::shard::load_or_rebuild_maintenance(&db)
        .await
        .expect("load maintenance");
    crate::shard::ShardEngine::start(
        prefix.to_string(),
        Arc::new(db),
        store,
        crate::shard::ShardConfig::default(),
        absorb_tx,
        Some(on_close),
        maint,
    )
}

/// PR 6.1.1-B: ONE retirement protocol, proven through the REAL close
/// path. Retiring a shard must remove exactly that resident, arm the
/// anti-flap holdoff and close the engine as ONE step — the previous
/// shape removed the engine first, so its close callback found an empty
/// slot and silently skipped the holdoff, letting a request reopen the
/// prefix the instant it was evicted. A late notification from the
/// retired incarnation must then be an idempotent no-op that cannot
/// touch the replacement.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn retirement_arms_the_holdoff_and_a_stale_close_cannot_evict_a_replacement() {
    let store = mem();
    let st = store.clone();
    let dir = crate::shard_directory::ShardDirectory::new(
        vec!["0".into(), "1".into()],
        crate::ownership::OwnershipService::new(""),
        crate::shard_directory::OpenTiming {
            open_deadline: std::time::Duration::from_secs(60),
            open_wait: std::time::Duration::from_millis(50),
        },
        // The opener wires the close notifier exactly as production and
        // the principal rig do: an engine's own close evicts itself.
        |notifier| {
            Box::new(
                move |prefix: String, incarnation: crate::sharddir::EngineIncarnation| {
                    let st = st.clone();
                    let notifier = notifier.clone();
                    Box::pin(async move {
                        let p = prefix.clone();
                        let cb: Arc<dyn Fn() + Send + Sync> = Arc::new(move || {
                            notifier.closed(&p, incarnation);
                        });
                        Ok(open_engine_with_on_close(st, &prefix, cb).await)
                    })
                },
            )
        },
    );
    let prefix = "0";
    let crate::sharddir::OpenOutcome::Ready(a) = dir
        .open_or_wait(prefix, std::time::Duration::from_secs(30))
        .await
    else {
        panic!("open A must be ready");
    };
    let inc_a = dir.resident_incarnation(prefix).expect("A is the resident");

    // RETIRE A the way the fleet loop does: removed, held off, closed.
    let crate::shard_directory::RetireOutcome::Retired(retired) = dir.retire(
        prefix,
        crate::shard_directory::RetirementReason::FleetEviction,
        |_, _| true,
    ) else {
        panic!("A was resident");
    };
    assert!(Arc::ptr_eq(&retired, &a));
    assert!(!dir.is_open(prefix));
    match dir
        .open_or_wait(prefix, std::time::Duration::from_millis(20))
        .await
    {
        crate::sharddir::OpenOutcome::Wait { code, .. } => {
            assert_eq!(code, "shard_moving", "retirement must arm the holdoff");
        }
        _ => panic!("a just-retired shard must not reopen immediately"),
    }

    // After the holdoff, the replacement opens.
    dir.clear_holdoff(prefix);
    let crate::sharddir::OpenOutcome::Ready(b) = dir
        .open_or_wait(prefix, std::time::Duration::from_secs(30))
        .await
    else {
        panic!("open B must be ready");
    };
    let inc_b = dir.resident_incarnation(prefix).expect("B is the resident");
    assert_ne!(inc_a, inc_b, "every open is its own incarnation");
    assert!(!Arc::ptr_eq(&a, &b));

    // A's LATE database close (the acker path fires the same callback a
    // second time) must change nothing.
    let notifier = dir.close_notifier();
    assert!(!notifier.closed(prefix, inc_a), "a stale close is a no-op");
    assert!(dir.is_open(prefix), "B still serves");
    assert!(dir.engines().iter().any(|e| Arc::ptr_eq(e, &b)));

    // B's own retirement arms the next holdoff.
    assert!(matches!(
        dir.retire(
            prefix,
            crate::shard_directory::RetirementReason::Shutdown,
            |_, _| true
        ),
        crate::shard_directory::RetireOutcome::Retired(_)
    ));
    match dir
        .open_or_wait(prefix, std::time::Duration::from_millis(20))
        .await
    {
        crate::sharddir::OpenOutcome::Wait { code, .. } => assert_eq!(code, "shard_moving"),
        _ => panic!("B's retirement must arm its own holdoff"),
    }
}

/// PR 6.1.1-B: a DECLINING decision reinstates the very same engine
/// under the same write guard — no empty-slot window, no holdoff, no
/// close. This is the R30 sweep's custody CAS losing to customer
/// adoption.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_declined_retirement_keeps_the_engine_and_arms_nothing() {
    let store = mem();
    let st = store.clone();
    let dir = crate::shard_directory::ShardDirectory::new(
        vec!["0".into(), "1".into()],
        crate::ownership::OwnershipService::new(""),
        crate::shard_directory::OpenTiming {
            open_deadline: std::time::Duration::from_secs(60),
            open_wait: std::time::Duration::from_millis(50),
        },
        |_notifier| {
            Box::new(
                move |prefix: String, _inc: crate::sharddir::EngineIncarnation| {
                    let st = st.clone();
                    Box::pin(async move { Ok(open_engine(st, &prefix).await) })
                },
            )
        },
    );
    let prefix = "0";
    let crate::sharddir::OpenOutcome::Ready(a) = dir
        .open_or_wait(prefix, std::time::Duration::from_secs(30))
        .await
    else {
        panic!("open must be ready");
    };
    let inc = dir.resident_incarnation(prefix).unwrap();
    let seen = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let s2 = seen.clone();
    assert!(matches!(
        dir.retire(
            prefix,
            crate::shard_directory::RetirementReason::SweepEviction,
            move |engine, incarnation| {
                // The decision sees the resident and its incarnation.
                assert_eq!(incarnation, inc);
                assert!(!engine.is_closed());
                s2.store(true, std::sync::atomic::Ordering::SeqCst);
                false
            }
        ),
        crate::shard_directory::RetireOutcome::Kept
    ));
    assert!(seen.load(std::sync::atomic::Ordering::SeqCst));
    assert!(dir.is_open(prefix), "the same engine was reinstated");
    assert_eq!(dir.resident_incarnation(prefix), Some(inc));
    let crate::sharddir::OpenOutcome::Ready(again) = dir
        .open_or_wait(prefix, std::time::Duration::from_millis(20))
        .await
    else {
        panic!("a kept engine keeps serving");
    };
    assert!(Arc::ptr_eq(&a, &again), "no holdoff, no replacement");
    dir.retire(
        prefix,
        crate::shard_directory::RetirementReason::Shutdown,
        |_, _| true,
    );
}

/// PR 6.1.2-A: retirement and opening take the gate's two locks in ONE
/// order, so they cannot deadlock.
///
/// 6.1.1-B made retirement hold the SERVING MAP write guard and then arm
/// the holdoff, which locks the GATE STATE. `get_or_open` does the
/// reverse: it re-checks the serving map while holding the gate state.
/// That is an AB/BA deadlock, and because the gate state is ONE mutex
/// shared by every prefix, it strands shards that never meet — which is
/// why this proof retires "0" while an open of "1" is in the window.
///
/// The interleaving is forced, not raced: the opening side parks INSIDE
/// the gate state lock, immediately before the serving-map re-check.
/// Against the old order this test hangs and fails on its deadline;
/// against one order both sides complete.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn retirement_and_opening_take_the_gate_locks_in_one_order() {
    use std::sync::mpsc;
    let store = mem();
    let st = store.clone();
    let dir = crate::shard_directory::ShardDirectory::new(
        vec!["0".into(), "1".into()],
        crate::ownership::OwnershipService::new(""),
        crate::shard_directory::OpenTiming {
            open_deadline: std::time::Duration::from_secs(60),
            open_wait: std::time::Duration::from_secs(30),
        },
        |_notifier| {
            Box::new(
                move |prefix: String, _inc: crate::sharddir::EngineIncarnation| {
                    let st = st.clone();
                    Box::pin(async move {
                        // The opening side only has to REACH the gate
                        // state window; what it opens is irrelevant, and
                        // failing keeps the proof free of store timing.
                        if prefix == "1" {
                            anyhow::bail!("the opening side never needs an engine");
                        }
                        Ok(open_engine(st, &prefix).await)
                    })
                },
            )
        },
    );

    // "0" must hold a resident: an absent slot returns before the
    // retirement ever reaches the holdoff, and there is nothing to race.
    let crate::sharddir::OpenOutcome::Ready(_a) = dir
        .open_or_wait("0", std::time::Duration::from_secs(30))
        .await
    else {
        panic!("the retiring side needs a resident");
    };

    let gate = dir.gate_for_tests();
    gate.test_park().arm("1");

    // The OPENING side: parks holding the gate state.
    let (open_done_tx, open_done) = mpsc::channel();
    let opening = dir.clone();
    std::thread::spawn(move || {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .expect("opening runtime");
        let _ = rt.block_on(opening.open_or_wait("1", std::time::Duration::from_secs(30)));
        let _ = open_done_tx.send(());
    });
    assert!(
        gate.test_park()
            .wait_arrived(std::time::Duration::from_secs(10)),
        "the opening side never reached the gate-state window"
    );

    // The RETIRING side, on an UNRELATED prefix.
    let (entered_tx, entered) = mpsc::channel();
    let (retire_done_tx, retire_done) = mpsc::channel();
    let retiring = dir.clone();
    std::thread::spawn(move || {
        // Retirement closes the engine, and closing spawns the db close:
        // this thread needs a reactor of its own.
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .expect("retiring runtime");
        let _guard = rt.enter();
        let _ = entered_tx.send(());
        retiring.retire(
            "0",
            crate::shard_directory::RetirementReason::Shutdown,
            |_, _| true,
        );
        let _ = retire_done_tx.send(());
    });
    entered
        .recv_timeout(std::time::Duration::from_secs(10))
        .expect("the retiring side never started");
    // It is now blocked on its first acquisition. Under the old order it
    // holds the serving map while it waits; under one order it holds
    // nothing.
    std::thread::sleep(std::time::Duration::from_millis(150));

    // Release the opening side INTO the serving-map read it was about to
    // take. Old order: it waits for the map the retiring side holds,
    // while the retiring side waits for the gate state it holds. Neither
    // of these arrives.
    gate.test_park().release();
    open_done
        .recv_timeout(std::time::Duration::from_secs(20))
        .expect("the opening side deadlocked against a retirement of another prefix");
    retire_done
        .recv_timeout(std::time::Duration::from_secs(20))
        .expect("the retirement deadlocked against an open of another prefix");
    assert!(!dir.is_open("0"), "the retirement completed");
}

/// PR 6.1.2-A: `engine_shutdown` — the shared test oracle for a restart,
/// snapshot or quiescence boundary — must actually shut engines down.
///
/// 6.1.1-B reduced it to cloning the resident handles and dropping the
/// clones, which does nothing at all: the directory still owns its own
/// `Arc`, so no resident is removed, no close is initiated and no
/// engine-owned loop stops. ~170 tests took their restart boundary from
/// that. This characterizes what the helper now guarantees.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn engine_shutdown_really_retires_and_closes_every_resident() {
    let store = mem();
    let (state, addr) = http_rig(store).await;
    let (st, _, _) = hreq(
        addr,
        "PUT",
        "/v1/stream/shutdown-oracle",
        &[("content-type", "application/json")],
        b"",
    )
    .await;
    assert!(st == 200 || st == 201, "create {st}");

    // A handle held OUTSIDE the directory: the previous implementation
    // dropped exactly this kind of clone and called it a shutdown.
    let observed = state.shards.engines();
    assert_eq!(observed.len(), 1, "one resident engine");
    let engine = observed[0].clone();
    assert!(!engine.is_closed(), "serving before shutdown");
    assert_eq!(state.shards.open_count(), 1);

    engine_shutdown(&state).await;

    assert_eq!(
        state.shards.open_count(),
        0,
        "shutdown must leave no resident"
    );
    assert!(
        engine.is_closed(),
        "shutdown must initiate close on the engine itself, not just drop a handle"
    );
    assert!(state.shards.held_prefixes().is_empty());
}
