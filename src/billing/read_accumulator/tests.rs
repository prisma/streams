#![cfg(test)]

use super::{MeterSource, READ_SEALED_MAX_BATCHES, ReadUsageAccumulator, RowDelta};
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
