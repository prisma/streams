#![cfg(test)]

use super::{ReadSpool, ResidentRows};
use crate::billing::{BillingIdentity, MeterSource, ReadBatch, ReadRow};
use proptest::{prelude::any, prop_assert_eq};
use std::{collections::HashMap, sync::Arc};

fn batch(seq: u64) -> ReadBatch {
    ReadBatch {
        source: MeterSource {
            cell: "cell".into(),
            instance: "instance".into(),
            boot: "boot".into(),
        },
        seq,
        from_ms: 0,
        to_ms: 1,
        rows: vec![ReadRow {
            identity: BillingIdentity {
                account_id: "account".into(),
                project_id: "project".into(),
                stream_id: seq.to_string(),
                stream_name: "orders".into(),
            },
            read_payload_bytes: 17,
            read_records: 1,
            read_operations: 1,
            queue_operations: 0,
            append_requests: 0,
        }],
    }
}

#[tokio::test]
async fn persisted_sizes_and_zero_budget_reads_follow_the_exact_backlog() {
    let config = crate::config::ServerConfig::load(
        crate::config::CliArgs::deterministic(),
        &crate::config::MapEnvironment::empty(),
    );
    let spool = ReadSpool::open(
        Arc::new(object_store::memory::InMemory::new()),
        "",
        "resident-ledger",
        &config,
    )
    .await
    .unwrap();
    let batches = [batch(7), batch(99)];
    let lengths = batches
        .iter()
        .map(|b| u64::try_from(serde_json::to_vec(b).unwrap().len()).unwrap())
        .collect::<Vec<_>>();
    let keys = spool.persist_all(&batches).await.unwrap();
    assert_eq!(spool.resident(), (2, lengths.iter().sum()));
    assert!(spool.pending(0).await.unwrap().is_empty());
    assert_eq!(spool.pending(1).await.unwrap().len(), 1);
    spool
        .remove(&[keys[0].clone(), keys[0].clone(), b"absent".to_vec()])
        .await
        .unwrap();
    assert_eq!(spool.resident(), (1, lengths[1]));
    assert_eq!(spool.pending(2).await.unwrap()[0].1.seq, 99);
    spool.remove(&keys).await.unwrap();
    assert_eq!(spool.resident(), (0, 0));
    let poisoned = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let mut resident = spool.resident.lock().unwrap();
        resident.bytes = 1;
        panic!("interrupted resident size accounting");
    }));
    assert!(poisoned.is_err());
    assert!(std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| spool.resident())).is_err());
    spool.close_for_tests().await;
}

#[test]
fn replacement_and_repeated_removal_do_not_inflate_resident_bytes() {
    let mut resident = ResidentRows {
        sizes: HashMap::new(),
        bytes: 0,
    };
    let key = vec![1];
    resident.insert(key.clone(), 7);
    resident.insert(key.clone(), 19);
    assert_eq!((resident.sizes.len(), resident.bytes), (1, 19));
    resident.remove([&key, &key, &vec![99]]);
    assert_eq!((resident.sizes.len(), resident.bytes), (0, 0));
}

proptest::proptest! {
    #![proptest_config(proptest::test_runner::Config::with_cases(1024))]
    #[test]
    fn resident_totals_equal_the_retained_key_inventory(
        operations in proptest::collection::vec((0u8..16, proptest::option::of(any::<u32>())), 0..129),
    ) {
        let mut resident = ResidentRows { sizes: HashMap::new(), bytes: 0 };
        for (key, value) in operations {
            let key = vec![key];
            match value {
                Some(bytes) => resident.insert(key, u64::from(bytes)),
                None => resident.remove([&key]),
            }
            prop_assert_eq!(resident.bytes, resident.sizes.values().sum::<u64>());
        }
    }
}

/// Actual resident-ledger mutations share the Loom mutex. Three workers,
/// preemption bound 2 and 1,000 branches per execution; no run truncation.
#[test]
fn quality_loom_replacement_and_removal_publish_consistent_totals() {
    use loom::sync::{Arc, Mutex};
    let mut model = loom::model::Builder::new();
    model.max_threads = 4;
    model.max_branches = 1000;
    model.preemption_bound = Some(2);
    model.max_permutations = None;
    model.max_duration = None;
    model.check(|| {
        let state = Arc::new(Mutex::new(ResidentRows {
            sizes: HashMap::from([(vec![1], 5)]),
            bytes: 5,
        }));
        let insert = state.clone();
        let insert = loom::thread::spawn(move || insert.lock().unwrap().insert(vec![1], 19));
        let remove = state.clone();
        let remove =
            loom::thread::spawn(move || remove.lock().unwrap().remove([&vec![1], &vec![1]]));
        let observe = state.clone();
        let observe = loom::thread::spawn(move || {
            let state = observe.lock().unwrap();
            assert_eq!(state.bytes, state.sizes.values().sum::<u64>());
        });
        insert.join().unwrap();
        remove.join().unwrap();
        observe.join().unwrap();
        let state = state.lock().unwrap();
        assert!(matches!((state.sizes.len(), state.bytes), (0, 0) | (1, 19)));
    });
}

#[tokio::test]
async fn cancelled_open_guard_closes_and_releases_its_database() {
    let config = crate::config::ServerConfig::load(
        crate::config::CliArgs::deterministic(),
        &crate::config::MapEnvironment::empty(),
    );
    let spool = ReadSpool::open(
        Arc::new(object_store::memory::InMemory::new()),
        "",
        "cancel-open-guard",
        &config,
    )
    .await
    .unwrap();
    let db = spool.db.clone();
    let weak = Arc::downgrade(&db);
    let mut status = db.subscribe();
    drop(spool);
    let mut opening = Box::pin(async move {
        let _guard = super::SpoolOpenGuard(Some(db));
        std::future::pending::<()>().await;
    });
    assert!(futures_util::poll!(opening.as_mut()).is_pending());
    drop(opening);
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        let reason = status
            .wait_for(|state| state.close_reason.is_some())
            .await
            .unwrap()
            .close_reason
            .clone();
        assert!(matches!(reason, Some(slatedb::CloseReason::Clean)));
        // A close marker precedes joining the DB workers. The last outer Arc
        // belongs to the guard's close future until that operation completes.
        while weak.upgrade().is_some() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}
