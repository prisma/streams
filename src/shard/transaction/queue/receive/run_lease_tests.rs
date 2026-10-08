#![cfg(test)]
//! Run leases: one Receive leases consecutive records of a routing key, up
//! to `max`, each under its own lease row; while any record of a key holds a
//! live lease, every other Receive skips the key; acks settle record by
//! record in any order; an expired run is delivered again from its lowest
//! unacked offset; max-deliveries poison is judged per record, only while
//! its key is open, the lowest poisoned record of a key per Receive.
use crate::queue::{self, QueueOp, QueueOut};
use crate::shard::{AbsorbSignal, AppendFinish, AppendReq, ShardConfig, ShardEngine};
use bytes::Bytes;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot};

const HASH: [u8; 16] = [93; 16];
const GROUP: &str = "g";
/// Long enough that a lease never expires inside one step of a scenario.
const HOLD_MS: u32 = 30_000;
/// Short enough that a scenario can wait it out.
const BRIEF_MS: u32 = 800;
const EXPIRE: std::time::Duration = std::time::Duration::from_millis(1_000);

/// `(offset, lease generation, attempts)` of each leased or poisoned record.
type Granted = Vec<(u64, u32, u32)>;

struct Rig {
    engine: Arc<ShardEngine>,
    db: Arc<slatedb::Db>,
    keys: HashMap<u64, [u8; 16]>,
    next: u64,
    _absorb: mpsc::Receiver<AbsorbSignal>,
}

impl Rig {
    /// One record per byte of `layout`, the byte its routing key.
    async fn new(name: &str, layout: &str) -> Self {
        let store: Arc<dyn object_store::ObjectStore> =
            Arc::new(object_store::memory::InMemory::new());
        let db = Arc::new(
            slatedb::Db::builder(name.to_string(), store.clone())
                .build()
                .await
                .unwrap(),
        );
        let (tx, absorb) = mpsc::channel(64);
        let engine = ShardEngine::start(
            name.to_string(),
            db.clone(),
            store,
            ShardConfig::default(),
            tx,
            None,
            Default::default(),
        );
        let mut rig = Self {
            engine,
            db,
            keys: HashMap::new(),
            next: 0,
            _absorb: absorb,
        };
        for key in layout.bytes() {
            rig.append(key).await;
        }
        rig
    }

    async fn append(&mut self, key: u8) {
        let (resp, ack) = oneshot::channel();
        let key_hash = key_hash(key);
        let req = AppendReq {
            enqueued_at: std::time::Instant::now(),
            hash: HASH,
            route: HASH,
            entries: vec![Bytes::from(vec![key; 8])],
            usage: crate::usage::counters(&HASH),
            routing_key: char::from(key).to_string(),
            key_hash,
            producer_lineage: Vec::new(),
            key_version: 0,
            subkey: [1; 32],
            ts_hint_ms: None,
            seq: None,
            bytes: 0,
            finish: AppendFinish::Open,
            producer: None,
            deferred_error: None,
            sealed_reject_new: None,
            touch: None,
            seal_gen: None,
            billing: None,
            resp,
        };
        assert!(self.engine.try_enqueue(req).is_ok(), "enqueue");
        let last = ack.await.unwrap().unwrap().last_offset;
        assert_eq!(last, self.next, "one record per append");
        self.keys.insert(last, key_hash);
        self.next += 1;
    }

    /// One Receive by group `g`; answers `(leased, poisoned)`.
    async fn pull(
        &self,
        max: usize,
        visibility_ms: u32,
        max_deliveries: u32,
    ) -> (Granted, Granted) {
        let out = self
            .engine
            .submit_queue(
                HASH,
                QueueOp::Receive {
                    consumer: GROUP.into(),
                    cgen: 1,
                    max,
                    visibility_ms,
                    max_deliveries,
                    keys: self.keys.clone(),
                    covered_to: self.next,
                },
            )
            .await
            .unwrap();
        let QueueOut::Received {
            leased, poisoned, ..
        } = out
        else {
            panic!("a Receive answers Received, got {out:?}")
        };
        let strip = |rows: Vec<(u64, u32, u32, [u8; 16])>| {
            rows.into_iter()
                .map(|(off, lease_gen, attempts, key_hash)| {
                    assert_eq!(Some(&key_hash), self.keys.get(&off), "offset {off}");
                    (off, lease_gen, attempts)
                })
                .collect()
        };
        (strip(leased), strip(poisoned))
    }

    /// Acks `(offset, lease generation)` tokens; answers `(acked, stale)`.
    async fn ack(&self, tokens: &[(u64, u32)]) -> (usize, usize) {
        let (acked, _, stale) = self.settle(tokens.to_vec(), Vec::new()).await;
        (acked, stale)
    }

    /// Extends `(offset, lease generation)` tokens to `visibility_ms` from
    /// now; answers `(extended, stale)`.
    async fn extend(&self, tokens: &[(u64, u32)], visibility_ms: u32) -> (usize, usize) {
        let extends = tokens
            .iter()
            .map(|&(off, lease_gen)| (off, lease_gen, visibility_ms))
            .collect();
        let (_, extended, stale) = self.settle(Vec::new(), extends).await;
        (extended, stale)
    }

    async fn settle(
        &self,
        acks: Vec<(u64, u32)>,
        extends: Vec<(u64, u32, u32)>,
    ) -> (usize, usize, usize) {
        let out = self
            .engine
            .submit_queue(
                HASH,
                QueueOp::Settle {
                    consumer: GROUP.into(),
                    cgen: 1,
                    acks,
                    retries: Vec::new(),
                    extends,
                    max_deliveries: 5,
                },
            )
            .await
            .unwrap();
        let QueueOut::Settled {
            acked,
            extended,
            stale,
            ..
        } = out
        else {
            panic!("a Settle answers Settled, got {out:?}")
        };
        (acked, extended, stale)
    }

    /// The stored lease row of `off`: `(delivery count, lease generation, key hash)`.
    async fn lease_row(&self, off: u64) -> Option<(u32, u32, [u8; 16])> {
        let row = self
            .db
            .get(queue::lease_key(&HASH, GROUP, 1, off))
            .await
            .unwrap()?;
        let lease = queue::decode_lease(&row).unwrap();
        Some((lease.delivery_count, lease.lease_gen, lease.key_hash))
    }

    async fn ack_row(&self, off: u64) -> bool {
        let key = queue::ack_key(&HASH, GROUP, 1, off);
        self.db.get(key).await.unwrap().is_some()
    }

    async fn cursor(&self) -> u64 {
        let row = self.db.get(queue::cursor_key(&HASH, GROUP, 1)).await;
        u64::from_le_bytes(row.unwrap().unwrap().as_ref().try_into().unwrap())
    }

    /// The engine's close closes the store too (`begin_close` spawns its
    /// storage close), so the rig's own close may find it closed already:
    /// either close may win, and the store must end closed cleanly.
    async fn close(self) {
        self.engine.begin_close();
        if let Err(error) = self.db.close().await {
            let clean = slatedb::ErrorKind::Closed(slatedb::CloseReason::Clean);
            assert_eq!(error.kind(), clean, "{error}");
        }
    }
}

/// The routing-key hash a test record of `key` carries: the Receive
/// treats it as an opaque blocking identity.
fn key_hash(key: u8) -> [u8; 16] {
    [key; 16]
}

fn fresh(offsets: &[u64]) -> Granted {
    offsets.iter().map(|&off| (off, 1, 1)).collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_pull_leases_a_run_of_one_key_up_to_max_under_one_lease_per_record() {
    let rig = Rig::new("run-lease-max", "aaaaaa").await;
    let (leased, poisoned) = rig.pull(4, HOLD_MS, 5).await;
    assert_eq!(
        leased,
        fresh(&[0, 1, 2, 3]),
        "four of the key's six records"
    );
    assert!(poisoned.is_empty());
    let a = key_hash(b'a');
    for off in 0..4 {
        assert_eq!(rig.lease_row(off).await, Some((1, 1, a)), "offset {off}");
    }
    for off in 4..6 {
        assert_eq!(rig.lease_row(off).await, None, "offset {off} past max");
    }
    assert_eq!(rig.cursor().await, 0);
    rig.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_leased_run_blocks_its_key_for_every_other_pull_until_its_last_ack() {
    // a0 a1 a2 b3 a4 b5
    let rig = Rig::new("run-lease-block", "aaabab").await;
    assert_eq!(rig.pull(3, HOLD_MS, 5).await.0, fresh(&[0, 1, 2]));
    assert_eq!(
        rig.pull(10, HOLD_MS, 5).await.0,
        fresh(&[3, 5]),
        "a4 waits behind the run; b flows"
    );
    assert_eq!(rig.pull(10, HOLD_MS, 5).await.0, fresh(&[]));
    assert_eq!(rig.ack(&[(0, 1), (1, 1)]).await, (2, 0));
    assert_eq!(
        rig.pull(10, HOLD_MS, 5).await.0,
        fresh(&[]),
        "a2's live lease still blocks a"
    );
    assert_eq!(rig.ack(&[(2, 1)]).await, (1, 0));
    assert_eq!(rig.pull(10, HOLD_MS, 5).await.0, fresh(&[4]));
    assert_eq!(rig.cursor().await, 3);
    rig.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_crash_after_a_partial_ack_redelivers_from_the_lowest_unacked_offset() {
    let rig = Rig::new("run-lease-crash", "aaaaaaaa").await;
    assert_eq!(rig.pull(6, BRIEF_MS, 5).await.0, fresh(&[0, 1, 2, 3, 4, 5]));
    // Acks in any order; then the consumer stops.
    assert_eq!(rig.ack(&[(4, 1), (0, 1), (1, 1)]).await, (3, 0));
    assert_eq!(rig.cursor().await, 2);
    assert!(rig.ack_row(4).await, "the out-of-order ack is stored");
    assert_eq!(
        rig.pull(10, HOLD_MS, 5).await.0,
        fresh(&[]),
        "2, 3, 5 in flight"
    );
    tokio::time::sleep(EXPIRE).await;
    assert_eq!(
        rig.pull(4, HOLD_MS, 5).await.0,
        vec![(2, 2, 2), (3, 2, 2), (5, 2, 2), (6, 1, 1)],
        "the expired rest again in offset order, then the key's next record"
    );
    let a = key_hash(b'a');
    assert_eq!(rig.lease_row(5).await, Some((2, 2, a)));
    assert_eq!(rig.lease_row(7).await, None);
    assert_eq!(
        rig.ack(&[(2, 1), (3, 1)]).await,
        (0, 2),
        "the first delivery's tokens are stale"
    );
    assert_eq!(rig.ack(&[(2, 2), (3, 2)]).await, (2, 0));
    assert_eq!(
        rig.cursor().await,
        5,
        "2, 3 and the stored 4 settle the cursor"
    );
    assert!(!rig.ack_row(4).await, "the cursor absorbed the stored ack");
    rig.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn max_one_leases_one_record_per_pull_and_blocks_its_key_as_before() {
    let rig = Rig::new("run-lease-one", "aab").await;
    assert_eq!(rig.pull(1, HOLD_MS, 5).await.0, fresh(&[0]));
    assert_eq!(
        rig.pull(1, HOLD_MS, 5).await.0,
        fresh(&[2]),
        "a1 waits behind a0"
    );
    assert_eq!(rig.pull(1, HOLD_MS, 5).await.0, fresh(&[]));
    assert_eq!(rig.ack(&[(0, 1)]).await, (1, 0));
    assert_eq!(rig.pull(1, HOLD_MS, 5).await.0, fresh(&[1]));
    rig.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mixed_keys_lease_every_open_key_run_in_offset_order() {
    // c0 a1 b2 a3 c4 b5 a6 c7
    let rig = Rig::new("run-lease-mixed", "cabacbac").await;
    assert_eq!(rig.pull(1, HOLD_MS, 5).await.0, fresh(&[0]), "c0 blocks c");
    assert_eq!(
        rig.pull(10, HOLD_MS, 5).await.0,
        fresh(&[1, 2, 3, 5, 6]),
        "the runs of a and b, interleaved by offset; c skipped"
    );
    assert_eq!(rig.ack(&[(0, 1)]).await, (1, 0));
    assert_eq!(
        rig.pull(1, HOLD_MS, 5).await.0,
        fresh(&[4]),
        "max cuts c's run"
    );
    assert_eq!(rig.pull(10, HOLD_MS, 5).await.0, fresh(&[]), "c4 holds c7");
    rig.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn max_deliveries_poison_is_judged_per_record_inside_a_run() {
    let rig = Rig::new("run-lease-poison", "aaa").await;
    assert_eq!(rig.pull(3, BRIEF_MS, 2).await.0, fresh(&[0, 1, 2]));
    assert_eq!(rig.ack(&[(0, 1)]).await, (1, 0));
    tokio::time::sleep(EXPIRE).await;
    assert_eq!(
        rig.pull(3, BRIEF_MS, 2).await,
        (vec![(1, 2, 2), (2, 2, 2)], vec![]),
        "the unacked two are delivered a second time"
    );
    tokio::time::sleep(EXPIRE).await;
    // Both reached max deliveries on their own counts. A Receive reports
    // one poisoned record per key, the lowest first, so the dead-letter
    // handoff settles a key's records in offset order.
    assert_eq!(rig.pull(3, BRIEF_MS, 2).await, (vec![], vec![(1, 2, 2)]));
    assert_eq!(rig.ack(&[(1, 2)]).await, (1, 0), "the handoff's settle");
    assert_eq!(rig.pull(3, BRIEF_MS, 2).await, (vec![], vec![(2, 2, 2)]));
    assert_eq!(rig.ack(&[(2, 2)]).await, (1, 0));
    assert_eq!(rig.pull(3, BRIEF_MS, 2).await, (vec![], vec![]));
    assert_eq!(rig.cursor().await, 3);
    rig.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_expired_record_waits_while_a_record_of_its_run_is_still_leased() {
    let rig = Rig::new("run-lease-extend", "aab").await;
    assert_eq!(rig.pull(3, BRIEF_MS, 1).await.0, fresh(&[0, 1, 2]));
    // The consumer extends a0 only; a1 and b2 expire at max deliveries.
    assert_eq!(rig.extend(&[(0, 1)], HOLD_MS).await, (1, 0));
    tokio::time::sleep(EXPIRE).await;
    assert_eq!(
        rig.pull(3, BRIEF_MS, 1).await,
        (vec![], vec![(2, 1, 1)]),
        "a1 is not judged while a0 holds a; b2 is"
    );
    assert_eq!(rig.ack(&[(0, 1)]).await, (1, 0));
    assert_eq!(
        rig.pull(3, BRIEF_MS, 1).await,
        (vec![], vec![(1, 1, 1), (2, 1, 1)]),
        "with a open, a1 is judged in its turn"
    );
    rig.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_record_delivered_twice_goes_alone_until_settled_and_its_run_resumes_after_it() {
    let rig = Rig::new("run-lease-isolate", "aaaaaa").await;
    assert_eq!(rig.pull(4, BRIEF_MS, 5).await.0, fresh(&[0, 1, 2, 3]));
    tokio::time::sleep(EXPIRE).await;
    let second: Granted = (0..4).map(|off| (off, 2, 2)).collect();
    assert_eq!(
        rig.pull(4, BRIEF_MS, 5).await.0,
        second,
        "one failure keeps the run"
    );
    tokio::time::sleep(EXPIRE).await;
    assert_eq!(
        rig.pull(4, HOLD_MS, 5).await.0,
        vec![(0, 3, 3)],
        "a head delivered twice goes alone: its successors keep their attempts"
    );
    for off in 0..4 {
        assert_eq!(rig.ack(&[(off, 3)]).await, (1, 0), "offset {off}");
        let next = if off < 3 {
            vec![(off + 1, 3, 3)]
        } else {
            fresh(&[4, 5])
        };
        assert_eq!(
            rig.pull(4, HOLD_MS, 5).await.0,
            next,
            "after offset {off}: each record of the failed run alone, then a fresh run"
        );
    }
    let a = key_hash(b'a');
    assert_eq!(rig.lease_row(3).await, None, "acked");
    assert_eq!(rig.lease_row(5).await, Some((1, 1, a)));
    rig.close().await;
}
