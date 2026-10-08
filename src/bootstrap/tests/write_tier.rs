//! The write tier of a server that sets none of the pump's settings,
//! measured on one shard engine built from the default command line as
//! `run` builds every engine (`shard_config` for the engine,
//! `shard_settings` for its shard log), over an in-memory store whose every
//! WAL write takes [`WAL_PUT`].
//!
//! The group-commit pump flushes when a commit waits. It starts a flush no
//! sooner than the gap after the previous flush STARTED, and 1 ms after it
//! decides to (the herd-settle of a non-zero gather), so two flushes start
//! at least gap + 1 ms apart. An append that finds no flush started within
//! the last gap therefore waits for the herd-settle and one WAL write; an
//! append that follows a flush started `t` ago waits `gap - t` more, then
//! the herd-settle and its WAL write. A WAL write shorter than the gap
//! overlaps it: a producer with one append in flight is acknowledged once
//! per gap + 1 ms, not once per gap + 1 ms + WAL write.
//!
//! SlateDB's own flush timer, the pump's failsafe, writes whatever the WAL
//! buffer holds when it fires, including commits that wait out the gap. At
//! the default it fires once a minute (and once at open), so never inside
//! these measurements: every WAL object after the warm-up is a pump flush.
#![cfg(test)]

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use tokio::time::Instant;

use crate::dst::{FaultPlan, FaultProfile, FaultStore, ObjClass, Outcome, StoreOp, Workload};

/// The binary's default `WAL_FLUSH_GAP_MS`: the one write tier.
const GAP: Duration = Duration::from_millis(100);
/// The pump's herd-settle before every flush while the gather is on.
const SETTLE: Duration = Duration::from_millis(1);
/// What one WAL write costs in this rig (Tigris: about 40 ms).
const WAL_PUT: Duration = Duration::from_millis(50);
/// Appends sent back to back, each the moment the previous one is acked.
const BACK_TO_BACK: u32 = 5;
/// A busy shard's offered load: one append every this often, none waiting
/// for another's acknowledgement.
const OFFER_EVERY: Duration = Duration::from_millis(5);
/// How long that load is offered: longer than any period SlateDB's timer
/// had before the 60 s failsafe (1 s plus a per-shard offset below 0.5 s),
/// so at least one of its ticks would fall inside.
const BUSY: Duration = Duration::from_secs(2);

/// One default shard engine and the store that times its WAL writes.
struct Rig {
    store: Arc<FaultStore>,
    engine: Arc<crate::shard::ShardEngine>,
    workload: Workload,
}

impl Rig {
    async fn open() -> Self {
        let put_ms = u64::try_from(WAL_PUT.as_millis()).expect("a WAL write in milliseconds");
        let every_wal_put = FaultPlan {
            error_pct: 0,
            lost_response_pct: 0,
            latency_pct: 100,
            latency_ms: (put_ms, put_ms),
        };
        let store = FaultStore::new(
            Arc::new(object_store::memory::InMemory::new()),
            1,
            FaultProfile::clean().with_op_class(StoreOp::Put, ObjClass::Wal, every_wal_put),
        );
        let config = crate::config::ServerConfig::load(
            crate::config::CliArgs::deterministic(),
            &crate::config::MapEnvironment::empty(),
        );
        let settings = crate::config::validation::shard_settings(&config.cli, &config.engine);
        let shard = super::shard_config(
            &config.cli,
            config.history.clone(),
            config.engine.compactor_options(),
        );
        let db = slatedb::Db::builder("write-tier", store.clone())
            .with_settings(settings)
            .build()
            .await
            .expect("open the shard log");
        let (absorb_tx, _absorb_rx) = crate::history::absorber_channel();
        let maintenance = crate::shard::load_or_rebuild_maintenance(&db)
            .await
            .expect("load maintenance");
        let engine = crate::shard::ShardEngine::start(
            "write-tier".to_string(),
            Arc::new(db),
            store.clone(),
            shard,
            absorb_tx,
            None,
            maintenance,
        );
        let workload = Workload::new(store.coverage());
        Rig {
            store,
            engine,
            workload,
        }
    }

    /// One acknowledged append, and how long its producer waited for it.
    async fn append(&self, body: &str) -> Duration {
        let sent = Instant::now();
        let outcome = self
            .workload
            .attempt_with_deadline(
                &self.engine,
                [9u8; 16],
                &crate::crypto::StreamKey([7u8; 32]),
                "tier",
                body,
                None,
                None,
            )
            .await;
        assert!(
            matches!(
                outcome,
                Outcome::Acked {
                    duplicate: false,
                    ..
                }
            ),
            "{body}: {outcome:?}"
        );
        sent.elapsed()
    }

    /// (WAL objects written, pump flushes) so far.
    fn ledger(&self) -> (u64, u64) {
        (
            self.store.count(StoreOp::Put, ObjClass::Wal),
            self.engine.pump_flushes.load(Ordering::Relaxed),
        )
    }

    /// The ledger once the pump has counted its last flush. The acker
    /// releases an acknowledgement on the same watermark change that ends
    /// the pump's flush, so the producer can hold its last acknowledgement
    /// before the pump has counted the flush that made it durable; with no
    /// commit pending, a gap later the pump is idle.
    async fn settled_ledger(&self) -> (u64, u64) {
        tokio::time::sleep(GAP).await;
        self.ledger()
    }
}

/// An append on a shard whose last flush started more than a gap ago
/// waits for the herd-settle and one WAL write, not for the gap. Appends
/// sent back to back start their flushes gap + 1 ms apart, start to start:
/// from the first send to the last of five more acknowledgements takes at
/// least 6 x 1 + 5 x 100 + 50 = 556 ms, and less than the 806 ms the gap
/// would need if it ran from the end of each WAL write. Every append is
/// its own WAL object and its own pump flush.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_isolated_append_waits_for_one_wal_write_and_the_rest_of_the_gap() {
    let rig = Rig::open().await;
    // The engine's open leaves its maintenance row in the WAL buffer: one
    // append makes it durable, then the shard rests for a gap.
    rig.append("warm-up").await;
    tokio::time::sleep(GAP).await;
    let (wal_before, flushes_before) = rig.ledger();

    let first_sent = Instant::now();
    let idle = rig.append("idle").await;
    let mut back_to_back = Vec::new();
    for n in 0..BACK_TO_BACK {
        back_to_back.push(rig.append(&format!("next-{n}")).await);
    }
    let train = first_sent.elapsed();
    tokio::time::sleep(GAP).await;
    let rested = rig.append("rested").await;

    eprintln!(
        "write tier at the default gap, WAL write {WAL_PUT:?}: rested shard {idle:?}; \
         back to back {back_to_back:?}; first send to last ack {train:?}; \
         rested again {rested:?}"
    );
    for wait in [idle, rested] {
        assert!(
            wait >= SETTLE + WAL_PUT && wait < GAP,
            "an append on a shard that rested a gap waits for the herd-settle and one \
             WAL write, not for the gap: waited {wait:?}"
        );
    }
    let train_flushes = BACK_TO_BACK + 1;
    let start_to_start = train_flushes * SETTLE + BACK_TO_BACK * GAP + WAL_PUT;
    let end_to_start = start_to_start + BACK_TO_BACK * WAL_PUT;
    assert!(
        train >= start_to_start && train < end_to_start,
        "{train_flushes} appends back to back take at least {start_to_start:?} (flush \
         starts gap + settle apart) and less than {end_to_start:?} (the gap counted from \
         the end of each WAL write): took {train:?}"
    );
    let appends = u64::from(train_flushes) + 1;
    assert_eq!(
        rig.settled_ledger().await,
        (wal_before + appends, flushes_before + appends),
        "every append is one WAL object and one pump flush"
    );
    rig.engine
        .await_terminated(Duration::from_secs(30))
        .await
        .expect("terminate");
}

/// A busy shard writes one WAL object per pump flush and no other. With an
/// append offered every 5 ms for 2 s, none waiting for another, commits
/// wait out the gap in the WAL buffer for most of every cycle, and the pump
/// flushes them about once per gap + 1 ms. SlateDB's own timer, the
/// failsafe, fires once a minute and so not in these 2 s; when it fired
/// every second, each tick found such commits in the buffer and wrote them
/// as a WAL object of its own, one object more per tick than the pump
/// flushed.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_busy_shard_writes_one_wal_object_per_pump_flush() {
    let rig = Rig::open().await;
    // The open leaves the maintenance row in the WAL buffer: one append
    // makes it durable before the ledger is read.
    rig.append("warm-up").await;
    tokio::time::sleep(GAP).await;
    let (wal_before, flushes_before) = rig.ledger();

    let offers =
        u32::try_from(BUSY.as_millis() / OFFER_EVERY.as_millis()).expect("the offers fit a u32");
    let shard = &rig;
    let waits = futures_util::future::join_all((0..offers).map(|n| async move {
        tokio::time::sleep(OFFER_EVERY * n).await;
        shard.append(&format!("busy-{n}")).await
    }))
    .await;
    let (wal, flushes) = rig.settled_ledger().await;
    let (written, flushed) = (wal - wal_before, flushes - flushes_before);

    eprintln!(
        "busy shard, one append every {OFFER_EVERY:?} for {BUSY:?}: {} acked, {written} \
         WAL objects, {flushed} pump flushes, slowest ack {:?}",
        waits.len(),
        waits.iter().max(),
    );
    assert_eq!(
        written,
        flushed,
        "every WAL object a busy shard writes is a pump flush; SlateDB's failsafe timer \
         wrote {} of them",
        written.saturating_sub(flushed)
    );
    let busiest_cycle = GAP + SETTLE + WAL_PUT;
    assert!(
        u128::from(flushed) >= BUSY.as_millis() / busiest_cycle.as_millis(),
        "the pump flushed throughout the load, at least once per gap, settle and WAL \
         write: {flushed} flushes in {BUSY:?}"
    );
    rig.engine
        .await_terminated(Duration::from_secs(30))
        .await
        .expect("terminate");
}
