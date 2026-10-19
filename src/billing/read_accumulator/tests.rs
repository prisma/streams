#![cfg(test)]

use super::{
    MeterSource, READ_BATCH_ROW_BYTES, READ_SEALED_MAX_BATCHES, ReadUsageAccumulator, RowDelta,
};
use crate::billing::BillingIdentity;
use proptest::{prelude::any, prop_assert_eq};
use std::panic::{AssertUnwindSafe, catch_unwind};

fn accumulator() -> ReadUsageAccumulator {
    ReadUsageAccumulator::new(MeterSource {
        cell: "cell".into(),
        instance: "instance".into(),
        boot: "boot".into(),
    })
}

fn identity(stream: u8) -> BillingIdentity {
    BillingIdentity {
        account_id: "account".into(),
        project_id: "project".into(),
        stream_id: stream.to_string(),
        stream_name: format!("stream-{stream}"),
    }
}

#[test]
fn a_full_sealed_queue_retains_observations_until_the_drain() {
    let acc = accumulator();
    for _ in 0..READ_SEALED_MAX_BATCHES + 2 {
        acc.meter(
            &identity(0),
            RowDelta {
                read_operations: 1,
                ..Default::default()
            },
        );
        acc.seal_if_aged(0);
    }
    assert_eq!(acc.unflushed().0, 1);
    assert_eq!(acc.unflushed().2, READ_SEALED_MAX_BATCHES);
    assert_eq!(acc.snapshot_active()[0].1.read_operations, 2);
    let first = acc.drain_sealed(1);
    acc.seal_if_aged(0);
    assert_eq!(acc.unflushed().0, 0);
    let mut all = first;
    all.extend(acc.drain_sealed(usize::MAX));
    assert_eq!(
        all.iter()
            .flat_map(|b| &b.rows)
            .map(|r| r.read_operations)
            .sum::<u64>(),
        u64::try_from(READ_SEALED_MAX_BATCHES).unwrap() + 2
    );
}

#[test]
fn poisoned_transfers_refuse_observation_drain_and_requeue() {
    let acc = accumulator();
    let poison = catch_unwind(AssertUnwindSafe(|| {
        let mut state = acc.state.lock().unwrap();
        state.active.rows.insert(
            identity(0),
            RowDelta {
                read_operations: 7,
                ..Default::default()
            },
        );
        panic!("interrupted active-to-sealed transfer");
    }));
    assert!(poison.is_err());
    let operations: [fn(&ReadUsageAccumulator); 5] = [
        |acc| acc.meter(&identity(1), RowDelta::default()),
        |acc| acc.seal_if_aged(0),
        |acc| {
            acc.drain_sealed(1);
        },
        |acc| acc.requeue(Vec::new()),
        |acc| {
            acc.unflushed();
        },
    ];
    for operation in operations {
        assert!(catch_unwind(AssertUnwindSafe(|| operation(&acc))).is_err());
    }
}

#[test]
fn sequence_arithmetic_retains_the_previous_atomic_wrap() {
    let acc = accumulator();
    acc.state.lock().unwrap().seq = u64::MAX;
    for _ in 0..2 {
        acc.meter(
            &identity(0),
            RowDelta {
                read_operations: 1,
                ..Default::default()
            },
        );
        acc.seal_if_aged(0);
    }
    assert_eq!(
        acc.drain_sealed(2)
            .iter()
            .map(|b| b.seq)
            .collect::<Vec<_>>(),
        vec![u64::MAX, 0]
    );
}

proptest::proptest! {
    #![proptest_config(proptest::test_runner::Config::with_cases(1024))]
    #[test]
    fn sealing_and_requeue_preserve_each_identity_and_meter(
        observations in proptest::collection::vec((0u8..4, any::<u32>()), 1..129),
        drain_width in 1usize..8,
    ) {
        let acc = accumulator();
        let mut delivered = Vec::new();
        for (index, &(stream, bytes)) in observations.iter().enumerate() {
            acc.meter(&identity(stream), RowDelta {
                read_payload_bytes: u64::from(bytes), read_records: 1,
                read_operations: 2, queue_operations: 3, append_requests: 4,
            });
            if index % 3 == 0 { acc.seal_if_aged(0); }
            if index % 5 == 0 {
                let batch = acc.drain_sealed(drain_width);
                if index % 2 == 0 { acc.requeue(batch); }
                else { delivered.extend(batch); }
            }
        }
        acc.seal_if_aged(0);
        delivered.extend(acc.drain_sealed(usize::MAX));
        prop_assert_eq!(acc.unflushed(), (0, 0, 0));
        for stream in 0..4 {
            let expected = observations.iter().filter(|(id, _)| *id == stream).collect::<Vec<_>>();
            let count = u64::try_from(expected.len()).unwrap();
            let rows = delivered.iter().flat_map(|b| &b.rows)
                .filter(|r| r.identity == identity(stream)).collect::<Vec<_>>();
            prop_assert_eq!(rows.iter().map(|r| r.read_payload_bytes).sum::<u64>(),
                expected.iter().map(|(_, bytes)| u64::from(*bytes)).sum::<u64>());
            prop_assert_eq!(rows.iter().map(|r| r.read_records).sum::<u64>(), count);
            prop_assert_eq!(rows.iter().map(|r| r.read_operations).sum::<u64>(), 2 * count);
            prop_assert_eq!(rows.iter().map(|r| r.queue_operations).sum::<u64>(), 3 * count);
            prop_assert_eq!(rows.iter().map(|r| r.append_requests).sum::<u64>(), 4 * count);
        }
        let mut sequences = delivered.iter().map(|b| b.seq).collect::<Vec<_>>();
        sequences.sort_unstable();
        prop_assert_eq!(sequences, (0..u64::try_from(delivered.len()).unwrap()).collect::<Vec<_>>());
    }
}

/// The actual sealing transition runs with a Loom-protected production state.
/// Two concurrent sealers and one observer, preemption bound 2, 1,000 branches
/// per execution, no duration/permutation cutoff. Clock reads and metrics are
/// outside this model; public drain/requeue behavior has service/property tests.
#[test]
fn quality_loom_sealing_transfers_rows_and_sequence_as_one_operation() {
    use super::{ActiveMap, ReadUsageState};
    use loom::sync::{Arc, Mutex};
    use std::collections::{HashMap, VecDeque};

    let mut model = loom::model::Builder::new();
    model.max_threads = 4;
    model.max_branches = 1000;
    model.preemption_bound = Some(2);
    model.max_permutations = None;
    model.max_duration = None;
    crate::loom_check::check(&model, || {
        let owner = Arc::new(accumulator());
        let state = Arc::new(Mutex::new(ReadUsageState {
            active: ActiveMap {
                rows: HashMap::from([(
                    identity(0),
                    RowDelta {
                        read_operations: 1,
                        ..Default::default()
                    },
                )]),
                opened_ms: 1,
                est_bytes: 8,
            },
            sealed: VecDeque::new(),
            seq: 0,
        }));
        let mut sealers = Vec::new();
        for _ in 0..2 {
            let owner = owner.clone();
            let state = state.clone();
            sealers.push(loom::thread::spawn(move || {
                owner.seal_locked(&mut state.lock().unwrap())
            }));
        }
        let observed = state.clone();
        let observer = loom::thread::spawn(move || {
            let state = observed.lock().unwrap();
            let active: u64 = state.active.rows.values().map(|r| r.read_operations).sum();
            let sealed: u64 = state
                .sealed
                .iter()
                .flat_map(|b| &b.rows)
                .map(|r| r.read_operations)
                .sum();
            assert_eq!(active + sealed, 1);
            assert_eq!(state.seq, u64::try_from(state.sealed.len()).unwrap());
        });
        for sealer in sealers {
            sealer.join().unwrap();
        }
        observer.join().unwrap();
        let state = state.lock().unwrap();
        assert!(state.active.rows.is_empty());
        assert_eq!(state.active.est_bytes, 0);
        assert_eq!(state.seq, 1);
        assert_eq!(state.sealed.len(), 1);
        assert_eq!(state.sealed.front().unwrap().seq, 0);
    });
}

#[test]
fn reserved_observations_and_zero_drains_leave_custody_unchanged() {
    let acc = accumulator();
    let mut reserved = identity(0);
    reserved.stream_name = "_internal".into();
    acc.meter(
        &reserved,
        RowDelta {
            read_operations: 1,
            ..Default::default()
        },
    );
    assert_eq!(acc.unflushed(), (0, 0, 0));
    acc.meter(
        &identity(0),
        RowDelta {
            read_operations: 1,
            ..Default::default()
        },
    );
    acc.seal_if_aged(0);
    assert!(acc.drain_sealed(0).is_empty());
    assert_eq!(acc.unflushed(), (0, 0, 1));
    assert_eq!(acc.drain_sealed(1).len(), 1);
}

#[test]
fn positive_age_waits_for_the_window_and_zero_age_forces_sealing() {
    let acc = accumulator();
    acc.meter(
        &identity(0),
        RowDelta {
            read_operations: 1,
            ..Default::default()
        },
    );
    acc.state.lock().unwrap().active.opened_ms = i64::MAX;
    acc.seal_if_aged(1);
    assert_eq!(acc.unflushed().0, 1);
    assert_eq!(acc.unflushed().2, 0);
    acc.seal_if_aged(0);
    assert_eq!(acc.drain_sealed(1).len(), 1);
    acc.meter(
        &identity(1),
        RowDelta {
            read_operations: 1,
            ..Default::default()
        },
    );
    acc.state.lock().unwrap().active.opened_ms = 0;
    acc.seal_if_aged(1);
    assert_eq!(acc.drain_sealed(1).len(), 1);
}

/// An identity whose read row is bounded at exactly 512 encoded bytes, so
/// 128 rows fill a batch to `READ_BATCH_ROW_BYTES` exactly.
fn wide_identity(stream: u32) -> BillingIdentity {
    let id = BillingIdentity {
        account_id: "account".into(),
        project_id: "project".into(),
        stream_id: format!("{stream:032x}"),
        stream_name: format!("{stream:0>204}"),
    };
    assert_eq!(id.read_row_bound(), 512);
    id
}

fn one_read() -> RowDelta {
    RowDelta {
        read_operations: 1,
        ..Default::default()
    }
}

#[test]
fn a_new_identity_seals_the_map_before_its_row_would_pass_the_bound() {
    let acc = accumulator();
    for stream in 0..128 {
        acc.meter(&wide_identity(stream), one_read());
    }
    assert_eq!(acc.unflushed(), (128, READ_BATCH_ROW_BYTES, 0));
    // Repeated observations never grow a row past its bound.
    acc.meter(&wide_identity(0), one_read());
    assert_eq!(acc.unflushed(), (128, READ_BATCH_ROW_BYTES, 0));
    acc.meter(&wide_identity(128), one_read());
    assert_eq!(acc.unflushed(), (1, 512, 1));
    let sealed = acc.drain_sealed(1);
    assert_eq!(sealed[0].rows.len(), 128);
    let encoded = serde_json::to_vec(&sealed[0].rows).unwrap().len();
    assert!(encoded <= READ_BATCH_ROW_BYTES, "{encoded}");
}

#[test]
fn a_map_that_grew_while_sealing_paused_seals_as_bounded_batches() {
    let acc = accumulator();
    for _ in 0..READ_SEALED_MAX_BATCHES {
        acc.meter(&identity(0), one_read());
        acc.seal_if_aged(0);
    }
    for stream in 0..300 {
        acc.meter(&wide_identity(stream), one_read());
    }
    assert_eq!(acc.unflushed(), (300, 300 * 512, READ_SEALED_MAX_BATCHES));
    let paused = acc.drain_sealed(usize::MAX);
    acc.seal_if_aged(0);
    let sealed = acc.drain_sealed(usize::MAX);
    let runs = sealed.iter().map(|b| b.rows.len()).collect::<Vec<_>>();
    assert_eq!(runs.iter().sum::<usize>(), 300);
    assert!(runs.iter().all(|rows| *rows <= 128), "{runs:?}");
    let sequences = paused.iter().chain(&sealed).map(|b| b.seq);
    assert!(sequences.eq(0..u64::try_from(READ_SEALED_MAX_BATCHES + runs.len()).unwrap()));
}
