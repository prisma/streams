#![cfg(test)]
//! The sealer's decisions on their own: each trigger and deadline fires
//! exactly at its setting, in their order; a failed seal holds new seals
//! off for a doubling wait that a success clears; and a forced flush
//! restarts the memtable's count, so one flush is not followed by another
//! on writes it already flushed.

use std::time::Duration;

use rand::{SeedableRng, rngs::StdRng};

use super::tests::{append, hashes, manual, random_bytes, rig};
use super::{Deadline, Measure, SealSettings, SealState, Trigger};

const S: SealSettings = SealSettings {
    memtable_bytes: 100,
    unsealed_bytes: 200,
    wal_objects_seal: 10,
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

#[tokio::test]
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
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
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
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
