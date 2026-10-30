#![cfg(test)]
//! The sealer's decisions on their own: each trigger and deadline fires
//! exactly at its setting, in their order; a failed seal holds new seals
//! off for a doubling wait that a success clears; and a forced flush
//! restarts the memtable's count, so one flush is not followed by another
//! on writes it already flushed. S-gap step 2: the memtable trigger is
//! three quarters of the shard log's L0 size, a seal every 42 WAL objects
//! and a flush every third seal (or at the memtable trigger, a deadline or
//! a requested seal), and a deadline waits for a seal in flight until twice
//! the deadline.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use rand::{SeedableRng, rngs::StdRng};

use super::tests::{append, hashes, manual, random_bytes, rig};
use super::{Deadline, Measure, SealSettings, SealState, Trigger, memtable_trigger};

const S: SealSettings = SealSettings {
    memtable_bytes: 100,
    unsealed_bytes: 200,
    wal_objects_seal: 10,
    seals_per_flush: 3,
    wal_objects_deadline: 20,
    age_seal: Duration::from_secs(30),
    age_deadline: Duration::from_secs(60),
    tick: Duration::from_millis(10),
    max_pages_per_chunk: 8,
    max_object_bytes: 1 << 20,
    max_streams: 8,
    auto: false,
    backoff_min: Duration::from_millis(400),
    backoff_max: Duration::from_millis(1000),
    dispatch_warn: Duration::from_secs(2),
    dispatch_stall: Duration::from_secs(5),
};

const QUIET: Measure = Measure {
    memtable: 99,
    unsealed: 199,
    wal_objects: 9,
    oldest: Some(Duration::from_millis(29_999)),
    memtable_age: Some(Duration::from_millis(59_999)),
};

#[test]
fn each_trigger_fires_at_its_setting_and_not_below() {
    assert_eq!(QUIET.trigger(&S), None);
    let cases = [
        (
            Measure {
                memtable: 100,
                ..QUIET
            },
            Trigger::Memtable,
        ),
        (
            Measure {
                unsealed: 200,
                ..QUIET
            },
            Trigger::Unsealed,
        ),
        (
            Measure {
                wal_objects: 10,
                ..QUIET
            },
            Trigger::WalObjects,
        ),
        (
            Measure {
                oldest: Some(Duration::from_secs(30)),
                ..QUIET
            },
            Trigger::Age,
        ),
    ];
    for (m, want) in cases {
        assert_eq!(m.trigger(&S), Some(want), "{m:?}");
    }
    let all = Measure {
        memtable: 100,
        unsealed: 200,
        wal_objects: 10,
        oldest: Some(Duration::from_secs(30)),
        memtable_age: None,
    };
    assert_eq!(
        all.trigger(&S),
        Some(Trigger::Memtable),
        "the memtable first"
    );
    let rest = Measure { memtable: 0, ..all };
    assert_eq!(rest.trigger(&S), Some(Trigger::Unsealed));
    let rest = Measure {
        unsealed: 0,
        ..rest
    };
    assert_eq!(rest.trigger(&S), Some(Trigger::WalObjects));
    let none = Measure {
        oldest: None,
        ..QUIET
    };
    assert_eq!(none.trigger(&S), None, "nothing unsealed has no age");
}

#[test]
fn each_deadline_fires_at_its_setting_and_not_below() {
    let quiet = Measure {
        wal_objects: 19,
        ..QUIET
    };
    assert_eq!(quiet.deadline(&S), None);
    let wal = Measure {
        wal_objects: 20,
        ..quiet
    };
    assert_eq!(wal.deadline(&S), Some(Deadline::WalObjects));
    let age = Measure {
        memtable_age: Some(Duration::from_secs(60)),
        ..quiet
    };
    assert_eq!(age.deadline(&S), Some(Deadline::Age));
    let both = Measure {
        memtable_age: Some(Duration::from_secs(60)),
        ..wal
    };
    assert_eq!(both.deadline(&S), Some(Deadline::WalObjects));
}

#[test]
fn a_deadline_is_overdue_at_twice_its_setting() {
    let at = |wal_objects, age| Measure {
        wal_objects,
        memtable_age: Some(Duration::from_secs(age)),
        ..QUIET
    };
    assert!(!at(39, 119).overdue(&S));
    assert!(at(40, 0).overdue(&S), "twice the WAL deadline");
    assert!(at(0, 120).overdue(&S), "twice the age deadline");
}

/// The seal cadence (S-gap step 2, call 11, k = 3): the memtable trigger is
/// three quarters of the shard log's L0 size, the default's at the default
/// 8 MiB L0; a seal every 42 WAL objects, a flush every third seal, and the
/// replay deadline at 128.
#[test]
fn the_seal_cadence_follows_the_l0_size_and_the_128_object_window() {
    assert_eq!(memtable_trigger(8 << 20), 6 << 20);
    assert_eq!(memtable_trigger(32 << 20), 24 << 20);
    let d = SealSettings::default();
    assert_eq!(d.memtable_bytes, memtable_trigger(8 << 20));
    assert_eq!(
        (
            d.unsealed_bytes,
            d.wal_objects_seal,
            d.seals_per_flush,
            d.wal_objects_deadline
        ),
        (24 << 20, 42, 3, 128)
    );
    assert!(
        d.wal_objects_seal * d.seals_per_flush <= d.wal_objects_deadline,
        "k seals fit inside the replay deadline"
    );
    let metrics = crate::spill::metrics::for_log("trigger-tests/freeze");
    assert_eq!(metrics.freeze_bytes(), None, "nothing recorded yet");
    metrics.note_freeze_bytes(32 << 20);
    assert_eq!(metrics.freeze_bytes(), Some(32 << 20));
    let sp = SealState::default();
    sp.follow_freeze_bytes(32 << 20);
    assert_eq!(sp.settings().memtable_bytes, 24 << 20);
}

/// A shard log built for production (`compaction_guard::shard_log_builder`)
/// records its L0 size, and its sealer seals at three quarters of it: the
/// shared cell's 32 MiB L0 seals at 24 MiB.
#[tokio::test(flavor = "multi_thread", worker_threads = 2, crate = "crate::test_rt")]
async fn a_production_shard_log_seals_at_three_quarters_of_its_l0_size() {
    let store: std::sync::Arc<dyn object_store::ObjectStore> =
        std::sync::Arc::new(object_store::memory::InMemory::new());
    let prefix = super::tests::unique("freeze");
    let path = crate::sharddir::shard_db_path(&prefix);
    let settings = slatedb::config::Settings {
        l0_sst_size_bytes: 32 << 20,
        max_unflushed_bytes: 64 << 20,
        ..slatedb::config::Settings::default()
    };
    let db =
        crate::native_queue::compaction_guard::shard_log_builder(&path, store.clone(), settings)
            .build()
            .await
            .unwrap();
    let engine = crate::shard::ShardEngine::start(
        prefix,
        std::sync::Arc::new(db),
        store,
        crate::shard::ShardConfig::default(),
        None,
        Default::default(),
    );
    tokio::time::timeout(Duration::from_secs(10), engine.layout.seal.wait_ready())
        .await
        .expect("the sealer's open finishes");
    assert_eq!(engine.layout.seal.settings().memtable_bytes, 24 << 20);
    engine.begin_close();
}

#[tokio::test(crate = "crate::test_rt")]
async fn a_failed_seal_holds_seals_off_for_a_doubling_wait_until_one_succeeds() {
    let sp = SealState::default();
    sp.configure(|s| *s = S);
    let wait = || sp.retry_after.lock().unwrap().map(|(_, w)| w);
    assert!(!sp.backing_off(), "no failure yet");
    for want in [400, 800, 1000, 1000] {
        sp.after_seal(false);
        assert!(sp.backing_off(), "held off at once after a failure");
        assert_eq!(wait(), Some(Duration::from_millis(want)));
    }
    sp.after_seal(true);
    assert!(!sp.backing_off(), "a success clears the wait");
    assert_eq!(wait(), None);
    sp.after_seal(false);
    assert_eq!(wait(), Some(Duration::from_millis(400)), "and restarts it");
    sp.configure(|s| s.backoff_min = Duration::from_millis(20));
    sp.after_seal(true);
    sp.after_seal(false);
    assert!(sp.backing_off());
    tokio::time::sleep(Duration::from_millis(40)).await;
    assert!(!sp.backing_off(), "free once the wait passed");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2, crate = "crate::test_rt")]
async fn a_forced_flush_restarts_the_memtable_count() {
    let rig = rig("baseline", manual(), Duration::from_millis(2)).await;
    let mut rng = StdRng::seed_from_u64(0xBA5E);
    for hash in hashes(&mut rng, 4) {
        append(&rig.engine, hash, vec![random_bytes(&mut rng, 4 << 10)]).await;
    }
    let before = rig.engine.seal_measure().memtable;
    assert!(before > 0, "the appends reached the memtable count");
    rig.engine.flush_memtable().await.unwrap();
    assert_eq!(
        rig.engine.seal_measure().memtable,
        0,
        "the count restarts at the flush (it was {before})"
    );
    rig.engine.begin_close();
}

/// The seal stats a shard serves (`GET /v1/debug/timings`) count what the
/// sealer did: one seal of four pages, its directory, no stall.
#[tokio::test(flavor = "multi_thread", worker_threads = 2, crate = "crate::test_rt")]
async fn the_seal_stats_count_what_the_sealer_did() {
    let rig = rig("stats", manual(), Duration::from_millis(2)).await;
    let mut rng = StdRng::seed_from_u64(0x57A7);
    let hash = hashes(&mut rng, 1)[0];
    for _ in 0..4 {
        append(&rig.engine, hash, vec![random_bytes(&mut rng, 300)]).await;
    }
    let report = rig.engine.seal_once(Trigger::Requested).await.unwrap();
    let stats = rig.engine.seal_json();
    assert_eq!(stats["seals"], 1);
    assert_eq!(stats["objects"], 1);
    assert_eq!(stats["page_bytes"], report.page_bytes);
    assert_eq!(stats["object_bytes"], report.object_bytes);
    assert_eq!(stats["directory_entries"], report.chunks);
    assert_eq!(stats["directory_streams"], 1);
    assert_eq!(stats["dispatch_stall_warnings"], 0);
    rig.engine.begin_close();
}

/// Only the deadline can fire: every trigger out of reach, the WAL deadline
/// at `objects`, the loop on and quick.
fn deadline_only(objects: u64) -> impl FnOnce(&mut SealSettings) {
    move |s| {
        *s = SealSettings {
            memtable_bytes: u64::MAX,
            unsealed_bytes: u64::MAX,
            wal_objects_seal: u64::MAX,
            wal_objects_deadline: objects,
            age_seal: Duration::from_secs(3600),
            age_deadline: Duration::from_secs(3600),
            tick: Duration::from_millis(5),
            auto: true,
            ..*s
        };
    }
}

/// While a seal holds the shard (its PUT delayed), appends past the WAL
/// deadline; then the deadline counters.
async fn past_the_deadline_during_a_seal(
    rig: &super::tests::Rig,
    rng: &mut StdRng,
    appends: usize,
) -> (u64, u64, u64) {
    let sp = &rig.engine.layout.seal;
    let hash = hashes(rng, 1)[0];
    let t0 = std::time::Instant::now();
    while sp.serial.try_lock().is_ok() {
        assert!(
            t0.elapsed() < Duration::from_secs(5),
            "the seal never started"
        );
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    for _ in 0..appends {
        append(&rig.engine, hash, vec![random_bytes(rng, 300)]).await;
    }
    tokio::time::sleep(Duration::from_millis(200)).await;
    let c = &sp.counters;
    let n = |a: &AtomicU64| a.load(Ordering::Relaxed);
    (
        n(&c.deadline_flushes),
        n(&c.deadline_flushes_deferred),
        n(&c.deadline_flushes_during_seal),
    )
}

/// A deadline reached while a seal is in flight waits for the seal, whose
/// own flush restarts the count, and counts the wait once; with no seal it
/// flushes; past twice the deadline it no longer waits.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_deadline_waits_for_the_seal_in_flight_until_twice_the_deadline() {
    let rig = rig("deadline-defer", manual(), Duration::from_millis(2)).await;
    let sp = &rig.engine.layout.seal;
    let mut rng = StdRng::seed_from_u64(0xDEFE);
    let hash = hashes(&mut rng, 1)[0];
    append(&rig.engine, hash, vec![random_bytes(&mut rng, 300)]).await;
    sp.configure(deadline_only(8));
    sp.set_auto(false);
    *sp.hooks.put_delay.lock().unwrap() = Duration::from_millis(800);
    let mut rng1 = StdRng::seed_from_u64(1);
    let during = async {
        sp.set_auto(true);
        past_the_deadline_during_a_seal(&rig, &mut rng1, 9).await
    };
    let (sealed, counted) =
        futures_util::future::join(rig.engine.seal_once(Trigger::Requested), during).await;
    sealed.unwrap();
    assert_eq!(counted, (0, 1, 0), "the deadline waited for the seal, once");
    assert_eq!(
        rig.engine.seal_measure().wal_objects,
        0,
        "the seal's flush restarted the count"
    );
    for _ in 0..9 {
        append(&rig.engine, hash, vec![random_bytes(&mut rng, 300)]).await;
    }
    let t0 = std::time::Instant::now();
    while sp.counters.deadline_flushes.load(Ordering::Relaxed) == 0 {
        assert!(
            t0.elapsed() < Duration::from_secs(5),
            "no seal, yet the deadline never flushed"
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    *sp.hooks.put_delay.lock().unwrap() = Duration::from_millis(1500);
    let mut rng2 = StdRng::seed_from_u64(2);
    let overdue = past_the_deadline_during_a_seal(&rig, &mut rng2, 18);
    let (sealed, counted) =
        futures_util::future::join(rig.engine.seal_once(Trigger::Requested), overdue).await;
    sealed.unwrap();
    assert!(
        counted.2 >= 1,
        "past twice the deadline the flush ran during the seal: {counted:?}"
    );
    *sp.hooks.put_delay.lock().unwrap() = Duration::ZERO;
    rig.engine.begin_close();
}

/// A seal flushes the memtable only every `seals_per_flush` seals: the
/// first two leave the memtable's count running and the third restarts it;
/// a requested seal (the idle release) always flushes; and the seal trigger
/// counts WAL objects from the last seal's start, not from the flush.
#[tokio::test(flavor = "multi_thread", worker_threads = 2, crate = "crate::test_rt")]
async fn a_seal_flushes_every_third_time_unless_requested() {
    let rig = rig("every-k", manual(), Duration::from_millis(2)).await;
    let mut rng = StdRng::seed_from_u64(0x0E3);
    let hash = hashes(&mut rng, 1)[0];
    let seal = |trigger| {
        let engine = rig.engine.clone();
        async move { engine.seal_once(trigger).await.unwrap() }
    };
    for round in 1..=3u64 {
        append(&rig.engine, hash, vec![random_bytes(&mut rng, 4 << 10)]).await;
        seal(Trigger::WalObjects).await;
        let m = rig.engine.seal_measure();
        if round < 3 {
            assert!(m.memtable > 0, "seal {round} left the memtable unflushed");
            assert_eq!(
                rig.engine.trigger_measure().wal_objects,
                m.wal_objects - rig.engine.layout.seal.wal_at_seal.load(Ordering::Relaxed),
                "the seal trigger counts from the seal's start"
            );
        } else {
            assert_eq!(m.memtable, 0, "the third seal flushed");
        }
    }
    append(&rig.engine, hash, vec![random_bytes(&mut rng, 4 << 10)]).await;
    seal(Trigger::Requested).await;
    assert_eq!(
        rig.engine.seal_measure().memtable,
        0,
        "a requested seal flushes"
    );
    rig.engine.begin_close();
}
