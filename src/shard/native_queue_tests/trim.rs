//! A live queue's trim (S3b-DESIGN §4.3, §7.3, §7.4; part 2 step 3): the
//! gc retires a sealed chunk once it lies behind the frontier and nothing
//! references it, decrements its object, and records what it released in
//! the queue's Trimming row and released intervals. Every row is asserted
//! exactly: the refcounts, `'H'`, `'k'`, the hints and the cursor.
#![cfg(test)]
use std::sync::Arc;

use super::{HASH, clocked, msg, op, send_op, settle};
use crate::native_queue::intervals::{Interval, Released};
use crate::native_queue::{NqOp, NqOut, PrepareFrom, QueueConfig, SendMessage, SettleKind, hints};
use crate::shard::ShardEngine;
use crate::spill::gc::{self, Head, HeadState};

/// Seal what the queue holds unsealed: one more chunk.
async fn seal(e: &Arc<ShardEngine>) {
    e.seal_once(crate::shard::seal::Trigger::Requested)
        .await
        .unwrap();
}

/// Send `body` and seal it alone: each message its own chunk.
async fn send_sealed(e: &Arc<ShardEngine>, m: SendMessage) {
    op(e, send_op(vec![m])).await.unwrap();
    seal(e).await;
}

/// A claim of up to `max` with the lane's sealed pages prepared.
async fn claim(e: &ShardEngine, max: usize) -> Vec<(u64, u32)> {
    let (prepared, _) = e
        .prepare_queue_pages(HASH, PrepareFrom::Frontier, usize::MAX)
        .await
        .unwrap();
    let claim = NqOp::Claim {
        max,
        max_bytes: 1 << 20,
        lease_ms: 30_000,
        prepared,
    };
    let NqOut::Claimed { deliveries, .. } = op(e, claim).await.unwrap() else {
        panic!("a claim answers deliveries");
    };
    deliveries.iter().map(|d| (d.offset, d.lease_gen)).collect()
}

async fn ack(e: &ShardEngine, leases: &[(u64, u32)]) {
    let items = leases
        .iter()
        .map(|&(o, g)| (o, g, SettleKind::Ack))
        .collect();
    op(e, settle(items)).await.unwrap();
}

/// The live chunks of the queue, by offset span, with their objects'
/// remaining chunk counts (0 when the `'R'` row is gone).
async fn chunks(e: &ShardEngine) -> Vec<((u64, u64), u64)> {
    let mut out = Vec::new();
    for c in e.live_chunks(HASH).await.unwrap() {
        let raw = e.db.get(gc::refcount_key(c.object)).await.unwrap();
        let live = raw
            .as_deref()
            .and_then(gc::decode_refcount)
            .map_or(0, |r| r.live_chunks);
        out.push(((c.first, c.last), live));
    }
    out
}

async fn head(e: &ShardEngine) -> Option<Head> {
    e.head_row(&HASH).await.unwrap()
}

async fn hint_count(e: &ShardEngine) -> usize {
    let (from, to) = hints::key_range();
    let mut scan = e.db.scan(from..to).await.unwrap();
    let mut n = 0;
    while scan.next().await.unwrap().is_some() {
        n += 1;
    }
    n
}

fn iv(first: u64, last: u64) -> Interval {
    Interval { first, last }
}

/// Three sealed chunks, one message each. The claim leases 0 and 1; only
/// 0 is acked: the pass retires chunk 0 (behind the frontier, unreferenced)
/// and keeps chunk 1 (an open delivery) and chunk 2 (ahead of the
/// frontier). Its batch deletes the hints it read and writes the cursor and
/// a Trimming row whose prefix stands after chunk 0.
#[tokio::test(flavor = "multi_thread", worker_threads = 2, crate = "crate::test_rt")]
async fn a_pass_retires_exactly_the_unreferenced_chunks_behind_the_frontier() {
    let (e, _db, _c) = clocked("nq-trim-retire", QueueConfig::default()).await;
    for body in ["a", "b", "c"] {
        send_sealed(&e, msg(body)).await;
    }
    let leased = claim(&e, 2).await;
    assert_eq!(leased.iter().map(|l| l.0).collect::<Vec<_>>(), [0, 1]);
    ack(&e, &leased[..1]).await;
    assert!(hint_count(&e).await > 0, "the seals and the claim hinted");
    let report = e.trim_pass().await.unwrap();
    assert_eq!((report.examined, report.released), (3, 1));
    assert_eq!(
        chunks(&e).await,
        [((0, 0), 0), ((1, 1), 1), ((2, 2), 1)],
        "chunk 0's object reaches zero; 1 and 2 keep theirs"
    );
    assert_eq!(head(&e).await, Some(Head::trimming(2, 1)));
    assert_eq!(e.intervals(&HASH).await.unwrap(), []);
    assert_eq!(hint_count(&e).await, 0, "every hint consumed");
    assert!(e.db.get(hints::cursor_key()).await.unwrap().is_some());
    let again = e.trim_pass().await.unwrap();
    assert_eq!(again, Default::default(), "nothing hinted: nothing read");
}

/// A chunk above the prefix released while the one below it is held joins
/// an interval (a `'k'` row); releasing the held one moves the prefix
/// through the interval and deletes its row. A fold then keeps no entry of
/// a released chunk. A scheduled message's bit holds its chunk.
#[tokio::test(flavor = "multi_thread", worker_threads = 2, crate = "crate::test_rt")]
async fn released_chunks_above_a_held_one_form_an_interval_the_prefix_absorbs() {
    let (e, _db, c) = clocked("nq-trim-intervals", QueueConfig::default()).await;
    for body in ["a", "b", "c"] {
        send_sealed(&e, msg(body)).await;
    }
    let scheduled = SendMessage {
        deliver_at: Some(c + 3_600_000),
        ..msg("later")
    };
    send_sealed(&e, scheduled).await;
    let leased = claim(&e, 3).await;
    assert_eq!(leased.iter().map(|l| l.0).collect::<Vec<_>>(), [0, 1, 2]);
    assert_eq!(
        claim(&e, 1).await,
        [],
        "the frontier passes the scheduled 3"
    );
    ack(&e, &leased[1..]).await;
    e.trim_pass().await.unwrap();
    assert_eq!(
        chunks(&e).await,
        [((0, 0), 1), ((1, 1), 0), ((2, 2), 0), ((3, 3), 1)],
        "0 is leased and 3 scheduled: both held"
    );
    let rows = (head(&e).await, e.intervals(&HASH).await.unwrap());
    assert_eq!(rows, (Some(Head::trimming(4, 0)), vec![iv(1, 2)]));
    ack(&e, &leased[..1]).await;
    e.trim_pass().await.unwrap();
    assert_eq!(chunks(&e).await[0], ((0, 0), 0));
    let rows = (head(&e).await, e.intervals(&HASH).await.unwrap());
    assert_eq!(
        rows,
        (Some(Head::trimming(4, 3)), vec![]),
        "the prefix absorbs [1, 2]"
    );
    let released = e.reclaimed_heads([HASH]).await.unwrap();
    assert!(released[&HASH].holds(2, 2) && !released[&HASH].holds(3, 3));
}

/// No purge acts on a Trimming row, though it owes chunks below its head
/// (an open delivery holds one there): purging it would delete the queue's
/// rows and decrement a chunk a lease still reads (control 3). A purge
/// request turns it into Requested with its prefix kept, so what the trim
/// decremented stays decremented (§5.9, §7.4).
#[tokio::test(flavor = "multi_thread", worker_threads = 2, crate = "crate::test_rt")]
async fn the_purge_leaves_a_trimming_queue_alone_until_a_request_which_keeps_its_prefix() {
    let (e, _db, _c) = clocked("nq-trim-purge", QueueConfig::default()).await;
    for body in ["a", "b", "c"] {
        send_sealed(&e, msg(body)).await;
    }
    let leased = claim(&e, 2).await;
    ack(&e, &leased[..1]).await;
    e.trim_pass().await.unwrap();
    assert_eq!(head(&e).await, Some(Head::trimming(2, 1)), "it owes [1, 2)");
    assert_eq!(
        e.purge_due().await.unwrap(),
        0,
        "a Trimming row waits for no purge"
    );
    assert_eq!(
        chunks(&e).await,
        [((0, 0), 0), ((1, 1), 1), ((2, 2), 1)],
        "the leased chunk keeps its count"
    );
    e.request_purge(HASH, 0).await.unwrap();
    let requested = head(&e).await.unwrap();
    assert_eq!(
        (requested.state, requested.head, requested.reclaimed),
        (HeadState::Requested, 2, 1),
        "the prefix the trim reached is kept"
    );
}

/// A purge request keeps the queue's `'k'` rows until the purge's last
/// decrement, so the folds' view keeps them too: after the request a fold
/// still drops the entries of the chunks the trim released above a held
/// one, in every `'H'` state, as a split's recount does (TLA-047's view,
/// control `nc_k_view_by_state`).
#[tokio::test(flavor = "multi_thread", worker_threads = 2, crate = "crate::test_rt")]
async fn a_fold_after_a_purge_request_still_drops_what_the_trim_released() {
    let (e, _db, _c) = clocked("nq-trim-requested-k", QueueConfig::default()).await;
    for body in ["a", "b", "c"] {
        send_sealed(&e, msg(body)).await;
    }
    let leased = claim(&e, 3).await;
    ack(&e, &leased[1..]).await;
    e.trim_pass().await.unwrap();
    assert_eq!(
        (head(&e).await, e.intervals(&HASH).await.unwrap()),
        (Some(Head::trimming(3, 0)), vec![iv(1, 2)]),
        "0 is leased: 1 and 2 form an interval above it"
    );
    e.request_purge(HASH, 0).await.unwrap();
    let requested = head(&e).await.unwrap();
    assert_eq!(
        (requested.state, requested.head, requested.reclaimed),
        (HeadState::Requested, 3, 0)
    );
    assert_eq!(e.intervals(&HASH).await.unwrap(), [iv(1, 2)], "'k' stays");
    let released = e.reclaimed_heads([HASH]).await.unwrap();
    assert_eq!(released[&HASH], Released::new(0, &[iv(1, 2)]));
}

/// The cleaner's horizon of a chunk (§7.4): the latest hard expiry of its
/// open deliveries (retention end plus failure retention); for a chunk held
/// only by a scheduled message, now plus twice the retention plus the
/// failure retention; none for a chunk nothing holds.
#[tokio::test(flavor = "multi_thread", worker_threads = 2, crate = "crate::test_rt")]
async fn a_chunks_horizon_is_its_latest_hold() {
    let (e, db, c) = clocked("nq-trim-horizon", QueueConfig::default()).await;
    for body in ["a", "b"] {
        send_sealed(&e, msg(body)).await;
    }
    let scheduled = SendMessage {
        deliver_at: Some(c + 3_600_000),
        ..msg("later")
    };
    send_sealed(&e, scheduled).await;
    let leased = claim(&e, 2).await;
    ack(&e, &leased[1..]).await;
    let cfg = QueueConfig::default();
    let raw = db
        .get(crate::native_queue::keys::by_offset(
            &HASH,
            crate::native_queue::keys::DELIVERY,
            0,
        ))
        .await
        .unwrap()
        .unwrap();
    let row = crate::native_queue::rows::DeliveryRow::decode(&raw).unwrap();
    let held = row.cycle_deadline + cfg.failure_retention_ms;
    assert_eq!(e.chunk_horizon(&HASH, 0, 0).await, Ok(Some(held)));
    assert_eq!(e.chunk_horizon(&HASH, 1, 1).await, Ok(None), "acked");
    let before = crate::shard::now_ms();
    let far = e.chunk_horizon(&HASH, 2, 2).await.unwrap().unwrap();
    let after = crate::shard::now_ms();
    let span = 2 * cfg.retention_ms + cfg.failure_retention_ms;
    assert!((before + span..=after + span).contains(&far), "{far}");
}

/// A queue's chunks never join (§7.4): its released intervals rely on them
/// tiling its offsets, so a read of eight whole sealed chunks offers none to
/// consolidation, where a stream's would be offered.
#[tokio::test(flavor = "multi_thread", worker_threads = 2, crate = "crate::test_rt")]
async fn a_queue_read_offers_no_chunk_to_consolidation() {
    let (e, _db, _c) = clocked("nq-trim-whole", QueueConfig::default()).await;
    for n in 0..8 {
        send_sealed(&e, msg(&format!("m{n}"))).await;
    }
    let (pages, _) = e
        .prepare_queue_pages(HASH, PrepareFrom::Frontier, usize::MAX)
        .await
        .unwrap();
    assert_eq!(pages.len(), 8, "every chunk read whole");
    let offered = &e.layout.seal.fold.consolidator.offered;
    let offered = offered.load(std::sync::atomic::Ordering::Relaxed);
    assert_eq!(offered, 0);
}

/// Make every message of the queue sealed, leased and acked: `n` chunks,
/// one each, nothing holding them.
async fn n_acked_chunks(e: &Arc<ShardEngine>, n: usize) {
    for i in 0..n {
        send_sealed(e, msg(&format!("m{i}"))).await;
    }
    let leased = claim(e, n).await;
    assert_eq!(leased.len(), n);
    ack(e, &leased).await;
}

/// A pass examines at most `trim_chunks` chunks and hints the rest again
/// (§7.3): four retirable chunks with a cap of two take two passes, the
/// first leaving exactly one new hint, the second none.
#[tokio::test(flavor = "multi_thread", worker_threads = 2, crate = "crate::test_rt")]
async fn a_pass_over_its_chunk_cap_hints_the_rest_again() {
    let (e, _db, _c) = clocked("nq-trim-cap", QueueConfig::default()).await;
    e.gc_state().configure(|s| s.trim_chunks = 2);
    n_acked_chunks(&e, 4).await;
    let first = e.trim_pass().await.unwrap();
    assert_eq!((first.examined, first.released), (2, 2));
    assert_eq!(hint_count(&e).await, 1, "the two left, hinted again");
    assert_eq!(head(&e).await, Some(Head::trimming(4, 2)));
    let second = e.trim_pass().await.unwrap();
    assert_eq!((second.hints, second.examined, second.released), (1, 2, 2));
    assert_eq!(
        (hint_count(&e).await, head(&e).await),
        (0, Some(Head::trimming(4, 4)))
    );
}

/// A pass whose batch fails after its reads writes nothing: the engine
/// closes, and a fresh engine on the same log redoes it from the same rows
/// to the same result, its objects counted down exactly once (§7.3: no
/// state of a pass lives outside its batch).
#[tokio::test(flavor = "multi_thread", worker_threads = 2, crate = "crate::test_rt")]
async fn a_pass_whose_batch_fails_is_redone_from_its_rows_alone() {
    const NAME: &str = "nq-trim-crash";
    let (e, db, _c) = clocked(NAME, QueueConfig::default()).await;
    n_acked_chunks(&e, 2).await;
    let before = chunks(&e).await;
    let hints = hint_count(&e).await;
    let fold = &e.layout.seal.fold.hooks;
    fold.fail_next_batch
        .store(true, std::sync::atomic::Ordering::Relaxed);
    assert!(e.trim_pass().await.is_err(), "the batch fails");
    assert_eq!(
        (hint_count(&e).await, head(&e).await),
        (hints, None),
        "nothing written"
    );
    let store = e.data_store().clone();
    drop(e);
    let e = ShardEngine::start(
        NAME.to_owned(),
        db.clone(),
        store,
        crate::shard::ShardConfig::default(),
        None,
        Default::default(),
    );
    e.seal_ready().await.expect("the open's rebuild");
    assert_eq!(chunks(&e).await, before, "the counts as they were");
    let report = e.trim_pass().await.unwrap();
    assert_eq!(report.released, 2);
    assert_eq!(chunks(&e).await, [((0, 0), 0), ((1, 1), 0)]);
    assert_eq!(e.trim_pass().await.unwrap().released, 0, "once");
}

/// A seal in flight (its entries published, its commit not yet applied)
/// holds a chunk whose object counts nothing yet: a pass that names it,
/// even at the prefix, leaves it alone (else its decrement finds no `'R'`
/// row and the whole batch is refused as inexact); once the seal commits,
/// its own hint lets the next pass give it back.
#[tokio::test(flavor = "multi_thread", worker_threads = 2, crate = "crate::test_rt")]
async fn a_chunk_of_a_seal_in_flight_is_never_given_back() {
    let (e, _db, _c) = clocked("nq-trim-flight", QueueConfig::default()).await;
    n_acked_chunks(&e, 1).await;
    e.trim_pass().await.unwrap();
    assert_eq!(head(&e).await, Some(Head::trimming(1, 1)));
    op(&e, send_op(vec![msg("late")])).await.unwrap();
    let leased = claim(&e, 1).await;
    ack(&e, &leased).await;
    let hold = crate::shard::seal::test_hooks::Hold::new();
    *e.layout.seal.hooks.after_publish.lock().unwrap() = Some(hold.clone());
    let pass = async {
        hold.wait_entered().await;
        // A hint naming the in-flight chunk at the prefix (as a racing
        // batch could leave one), drawn from the shard's sequence.
        let row = hints::encode(&[(1, 2)]);
        let gseq = e.next_trim_seq();
        e.db.put(hints::key(gseq, &HASH), row).await.unwrap();
        e.db.flush().await.unwrap();
        let report = e.trim_pass().await.unwrap();
        *e.layout.seal.hooks.after_publish.lock().unwrap() = None;
        hold.release();
        report
    };
    let (sealed, report) = tokio::join!(seal(&e), pass);
    let () = sealed;
    assert_eq!(
        (report.examined, report.released),
        (0, 0),
        "nothing of it examined"
    );
    assert_eq!(head(&e).await, Some(Head::trimming(1, 1)));
    assert_eq!(
        e.trim_pass().await.unwrap().released,
        1,
        "the seal's own hint"
    );
    assert_eq!(head(&e).await, Some(Head::trimming(2, 2)));
}
