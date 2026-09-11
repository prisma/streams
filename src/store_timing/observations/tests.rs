use super::{
    CellSamples, Ev, HttpEv, HttpStats, RING_CAP, SLOW_CAP, StoreStats, parse_server_timing_us,
    pct, percentile_index,
};
use std::collections::{HashMap, VecDeque};
use std::sync::{Mutex, atomic::AtomicI64};

fn store() -> StoreStats {
    StoreStats {
        ring: Mutex::new(VecDeque::new()),
        slow: Mutex::new(VecDeque::new()),
        inflight: AtomicI64::new(0),
        inflight_peak: AtomicI64::new(0),
    }
}

fn event(ts_ms: u64, dur_us: u32) -> Ev {
    Ev {
        ts_ms,
        op: 0,
        class: 0,
        dur_us,
        ok: true,
    }
}

fn poison<T>(mutex: &Mutex<T>, change: impl FnOnce(&mut T)) {
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let mut value = mutex.lock().unwrap();
        change(&mut value);
        panic!("interrupted diagnostic update");
    }));
    assert!(result.is_err());
}

#[test]
fn every_retained_percentile_rank_matches_the_original_float_formula() {
    assert_eq!(pct(&[], 99), 0);
    for len in 1..=RING_CAP {
        for (percent, quantile) in [(50, 0.50_f64), (90, 0.90), (99, 0.99)] {
            let original: usize = ((len as f64 - 1.0) * quantile)
                .round()
                .to_string()
                .parse()
                .unwrap();
            assert_eq!(
                percentile_index(len, percent),
                original,
                "len={len} p={percent}"
            );
        }
    }
    assert_eq!(pct(&[999, 1000, 1999, 2000], 50), 1);
    assert_eq!(pct(&[999, 1000, 1999, 2000], 99), 2);
}

#[test]
fn provider_decimal_durations_keep_fractional_and_nonfinite_semantics() {
    for (input, expected) in [
        ("-1", 0),
        ("NaN", 0),
        ("inf", u32::MAX),
        ("-inf", 0),
        ("0.0009", 0),
        ("0.0019", 1),
        ("4294967.295", u32::MAX),
        ("1e99", u32::MAX),
    ] {
        assert_eq!(
            parse_server_timing_us(&format!("total;dur={input}")),
            Some(expected),
            "{input}"
        );
    }
    assert_eq!(parse_server_timing_us("total;dur=bad;dur=1"), Some(1000));
    assert_eq!(parse_server_timing_us("total;dur=bad"), None);
}

#[test]
fn cells_preserve_error_counts_server_only_samples_and_millisecond_units() {
    let stats = store();
    stats.record(event(99, 9000), "wal/old");
    stats.record(event(100, 1500), "wal/current");
    stats.record(
        Ev {
            ok: false,
            ..event(101, 4500)
        },
        "wal/error",
    );
    let http = HttpStats::new();
    http.record_timing(HttpEv {
        ts_ms: 99,
        op: 0,
        class: 0,
        server_us: 5000,
    });
    http.record_timing(HttpEv {
        ts_ms: 100,
        op: 0,
        class: 0,
        server_us: 2500,
    });
    http.record_timing(HttpEv {
        ts_ms: 101,
        op: 2,
        class: 2,
        server_us: 7999,
    });
    let mut cells = stats.cells(100);
    http.collect_server_cells(100, &mut cells);
    assert_eq!(cells.len(), 2);
    assert_eq!(
        cells.remove(&(0, 0)).unwrap().into_json(),
        serde_json::json!({"n":2,"err":1,"p50_ms":4,"p90_ms":4,"p99_ms":4,"max_ms":4,"sn":1,"sp50_ms":2,"sp99_ms":2})
    );
    assert_eq!(
        cells.remove(&(2, 2)).unwrap().into_json(),
        serde_json::json!({"n":0,"err":0,"p50_ms":0,"p90_ms":0,"p99_ms":0,"max_ms":0,"sn":1,"sp50_ms":7,"sp99_ms":7})
    );
    assert!(CellSamples::default().into_json().get("sn").is_none());
}

#[test]
fn one_region_count_supplies_both_aggregate_views() {
    let http = HttpStats::new();
    for _ in 0..3 {
        http.record_region("iad", 0, 0);
    }
    for _ in 0..2 {
        http.record_region("iad", 2, 2);
    }
    http.record_region("sin", 2, 2);
    let snapshot = http.region_snapshot();
    assert_eq!(snapshot.served_from["iad"], 5);
    assert_eq!(snapshot.served_from["sin"], 1);
    assert_eq!(snapshot.served_from_by_class["iad"]["put:wal"], 3);
    assert_eq!(snapshot.served_from_by_class["iad"]["get:sst"], 2);
    for (region, cells) in &snapshot.served_from_by_class {
        assert_eq!(snapshot.served_from[region], cells.values().sum::<u64>());
    }
}

#[test]
fn bounded_rings_keep_the_latest_samples_and_slow_path_tail() {
    let stats = store();
    let path = "é".repeat(60);
    for index in 0..RING_CAP + 2 {
        stats.record(event(u64::try_from(index).unwrap(), 300_000), &path);
    }
    let ring = stats.ring.lock().unwrap();
    assert_eq!(ring.len(), RING_CAP);
    assert_eq!(ring.front().unwrap().ts_ms, 2);
    let slow = stats.slow.lock().unwrap();
    assert_eq!(slow.len(), SLOW_CAP);
    assert_eq!(slow.back().unwrap().path, "é".repeat(48));
    assert_eq!(slow.back().unwrap().dur_ms, 300);
}

#[test]
fn partial_store_observation_poison_refuses_reads_and_writes() {
    let stats = store();
    poison(&stats.ring, |ring| {
        ring.clear();
        ring.push_back(event(1, 300_000));
    });
    assert!(std::panic::catch_unwind(|| stats.cells(0)).is_err());
    assert!(std::panic::catch_unwind(|| stats.record(event(2, 300_000), "wal/new")).is_err());
    let stats = store();
    poison(&stats.slow, |ring| ring.clear());
    assert!(std::panic::catch_unwind(|| stats.record(event(2, 300_000), "wal/new")).is_err());
}

#[test]
fn partial_http_observation_poison_refuses_both_snapshot_and_publication() {
    let http = HttpStats::new();
    http.record_region("iad", 0, 0);
    poison(&http.regions, |regions| regions.clear());
    assert!(std::panic::catch_unwind(|| http.region_snapshot()).is_err());
    assert!(std::panic::catch_unwind(|| http.record_region("sin", 2, 2)).is_err());
    poison(&http.ring, |ring| {
        ring.clear();
        ring.push_back(HttpEv {
            ts_ms: 1,
            op: 0,
            class: 0,
            server_us: 1,
        });
    });
    assert!(
        std::panic::catch_unwind(|| http.collect_server_cells(0, &mut HashMap::new())).is_err()
    );
    assert!(
        std::panic::catch_unwind(|| http.record_timing(HttpEv {
            ts_ms: 2,
            op: 0,
            class: 0,
            server_us: 2
        }))
        .is_err()
    );
}
