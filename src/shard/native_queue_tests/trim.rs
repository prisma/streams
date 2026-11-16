//! A live queue's trim (S3b-DESIGN §4.3, §7.3, §7.4; part 2 step 3): the
//! gc retires a sealed chunk once it lies behind the frontier and nothing
//! references it, decrements its object, and records what it released in
//! the queue's Trimming row and released intervals. Every row is asserted
//! exactly: the refcounts, `'H'`, `'k'`, the hints and the cursor.
#![cfg(test)]
use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use super::{HASH, clocked, msg, op, send_op, settle};
use crate::dst::{FaultStore, ObjClass, StoreOp};
use crate::native_queue::intervals::{self, Interval, Released};
use crate::native_queue::range::RangeRow;
use crate::native_queue::{
    NqOp, NqOut, PrepareFrom, QueueConfig, SendMessage, SettleKind, hints, keys,
};
use crate::shard::ShardEngine;
use crate::shard::seal::test_hooks::Hold;
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

/// A split's plan reads a queue's intervals from the snapshot it read the sentinel rows from
/// (`gc::trim::intervals_at`, GATE-DECISION-f1b-trim-race): at the snapshot's sequence and Memory
/// durability, so it sees a trim's batch once it is written, before its WAL flush, where the
/// durable read (`intervals`) does not yet; once the batch is durable, both agree.
#[tokio::test(flavor = "multi_thread", worker_threads = 2, crate = "crate::test_rt")]
async fn a_snapshots_intervals_include_a_trim_batch_written_and_not_yet_durable() {
    use crate::shard::seal::gc::trim::intervals_at;
    let (e, _db, _) = clocked("nq-trim-snapshot", QueueConfig::default()).await;
    for body in ["a", "b", "c", "d"] {
        send_sealed(&e, msg(body)).await;
    }
    let leased = claim(&e, 4).await;
    ack(&e, &leased[1..2]).await;
    e.trim_pass().await.unwrap();
    assert_eq!(e.intervals(&HASH).await.unwrap(), [iv(1, 1)]);
    ack(&e, &leased[2..3]).await;
    let hold = e.hold_next_written_batch();
    let trim = async { e.trim_pass().await.unwrap() };
    let read = async {
        hold.wait_entered().await;
        let snapshot = e.db.snapshot().unwrap();
        let seen = (
            intervals_at(&snapshot, &HASH).await.unwrap(),
            e.intervals(&HASH).await.unwrap(),
        );
        hold.release();
        seen
    };
    let (report, seen) = futures_util::future::join(trim, read).await;
    assert_eq!(report.released, 1, "the third chunk");
    assert_eq!(
        seen,
        (vec![iv(1, 2)], vec![iv(1, 1)]),
        "(snapshot, durable)"
    );
    let snapshot = e.db.snapshot().unwrap();
    let now = intervals_at(&snapshot, &HASH).await.unwrap();
    assert_eq!(
        (now, e.intervals(&HASH).await.unwrap()),
        (vec![iv(1, 2)], vec![iv(1, 2)])
    );
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

/// A fold drops the entries of the chunks a trim released into an interval
/// and keeps the held chunk below them (F2, `kept_entries`): after the
/// fold the queue's index names exactly the held chunk.
#[tokio::test(flavor = "multi_thread", worker_threads = 2, crate = "crate::test_rt")]
async fn a_fold_keeps_the_held_chunk_below_a_released_interval_and_drops_the_interval() {
    let (e, _db, _c) = clocked("nq-trim-fold-k", QueueConfig::default()).await;
    for body in ["a", "b", "c"] {
        send_sealed(&e, msg(body)).await;
    }
    let leased = claim(&e, 3).await;
    ack(&e, &leased[1..]).await;
    e.trim_pass().await.unwrap();
    assert_eq!(e.intervals(&HASH).await.unwrap(), [iv(1, 2)]);
    e.fold_once().await.unwrap();
    let spans: Vec<(u64, u64)> = chunks(&e).await.iter().map(|c| c.0).collect();
    assert_eq!(spans, [(0, 0)], "the held chunk alone");
}

/// **The cleaner's read of a chunk's hold** (S3b-DESIGN §7.4; the cleaner
/// ruling of 2026-11-16, Q2, T1). Four seals: `a` alone; `b` with a
/// scheduled `s`; `d` with `e`; `g` alone. A claim of three leases `a`, `b`
/// and `d` (the frontier passes `s`, stops inside `[3, 4]`), then `a` is
/// acked. At the queue's clock `c`: `[0, 0]` is free; `[1, 2]` holds the
/// larger of `b`'s delivery and `s`'s scheduled bit, `c + 2r + f`; `[3, 4]`,
/// which the frontier is inside, and the backlog `[5, 5]` hold `c + r + f`.
/// A hash with no queue state row is an ordinary stream, and a deleting
/// queue is the purge's.
#[tokio::test(flavor = "multi_thread", worker_threads = 2, crate = "crate::test_rt")]
async fn the_cleaners_read_gives_each_chunk_its_latest_hold() {
    use crate::shard::seal::gc::trim::{Hold, QueueView};
    const NEIGHBOUR: [u8; 16] = [7; 16];
    let (e, _db, c) = clocked("nq-trim-hold", QueueConfig::default()).await;
    let cfg = QueueConfig::default();
    let (r, f) = (cfg.retention_ms, cfg.failure_retention_ms);
    send_sealed(&e, msg("a")).await;
    let scheduled = SendMessage {
        deliver_at: Some(c + 3_600_000),
        ..msg("s")
    };
    op(&e, send_op(vec![msg("b"), scheduled])).await.unwrap();
    seal(&e).await;
    op(&e, send_op(vec![msg("d"), msg("e")])).await.unwrap();
    seal(&e).await;
    send_sealed(&e, msg("g")).await;
    let leased = claim(&e, 3).await;
    let offsets: Vec<u64> = leased.iter().map(|l| l.0).collect();
    assert_eq!(offsets, [0, 1, 3]);
    ack(&e, &leased[..1]).await;
    let QueueView::Live(q) = e.queue_view(&HASH).await.unwrap() else {
        panic!("a live queue");
    };
    let mut holds = Vec::new();
    for span in [(0, 0), (1, 2), (3, 4), (5, 5)] {
        holds.push(e.chunk_hold(&HASH, q, span, c).await.unwrap());
    }
    let (late, backlog) = (Hold::Until(c + 2 * r + f), Hold::Until(c + r + f));
    assert_eq!(holds, [Hold::Free, late, backlog, backlog]);
    assert_eq!(e.queue_view(&NEIGHBOUR).await.unwrap(), QueueView::NotQueue);
    let create = NqOp::Create {
        cfg,
        container: super::CONTAINER,
    };
    e.submit_native_queue(NEIGHBOUR, create).await.unwrap();
    e.submit_native_queue(NEIGHBOUR, NqOp::BeginDelete)
        .await
        .unwrap();
    assert_eq!(e.queue_view(&NEIGHBOUR).await.unwrap(), QueueView::Deleting);
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

/// The live chunks the `'R'` row of `object` counts (0 once it is gone).
async fn counted(e: &ShardEngine, object: crate::spill::object::ObjectId) -> u64 {
    let raw = e.db.get(gc::refcount_key(object)).await.unwrap();
    raw.as_deref()
        .and_then(gc::decode_refcount)
        .map_or(0, |r| r.live_chunks)
}

/// **A purge decrements no chunk the trim already released** (S3b-DESIGN
/// §5.9, F4; TLA-047 control 8). Two queues share an object: one seal takes
/// the queue's second message and the neighbour queue's one message. The
/// queue's first message stays leased and its second is acked, so the trim
/// releases the shared object's queue chunk into a `'k'` interval and the
/// object counts only the neighbour's chunk. The queue is then deleted and
/// purged: the purge reads `'k'` first and decrements only the queue's other
/// chunk, so the shared object keeps the neighbour's count of 1, and the
/// last batch deletes the `'k'` rows with `reclaimed = head`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2, crate = "crate::test_rt")]
async fn a_purge_decrements_no_chunk_the_trim_released() {
    const NEIGHBOUR: [u8; 16] = [7; 16];
    let (e, _db, _c) = clocked("nq-trim-purge-shared", QueueConfig::default()).await;
    let neighbour = |op| e.submit_native_queue(NEIGHBOUR, op);
    let create = NqOp::Create {
        cfg: QueueConfig::default(),
        container: super::CONTAINER,
    };
    neighbour(create).await.unwrap();
    send_sealed(&e, msg("a")).await;
    op(&e, send_op(vec![msg("b")])).await.unwrap();
    neighbour(send_op(vec![msg("n")])).await.unwrap();
    seal(&e).await;
    let shared = e.live_chunks(NEIGHBOUR).await.unwrap()[0].object;
    assert_eq!(
        counted(&e, shared).await,
        2,
        "the queue's chunk [1, 1] and the neighbour's"
    );
    let leased = claim(&e, 2).await;
    ack(&e, &leased[1..]).await;
    e.trim_pass().await.unwrap();
    assert_eq!(e.intervals(&HASH).await.unwrap(), [iv(1, 1)]);
    assert_eq!(
        counted(&e, shared).await,
        1,
        "the trim released the queue's chunk"
    );
    op(&e, NqOp::BeginDelete).await.unwrap();
    while !matches!(
        op(&e, NqOp::DeleteStep { max_rows: 64 }).await.unwrap(),
        NqOut::DeleteStepped { remaining: false }
    ) {}
    let kept = e.intervals(&HASH).await.unwrap();
    assert_eq!(kept, [iv(1, 1)], "the delete leaves 'k' to the purge");
    e.request_purge(HASH, 0).await.unwrap();
    let outcome = e.purge_stream_at(HASH, u64::MAX).await.unwrap();
    assert!(
        matches!(
            outcome,
            crate::shard::seal::gc::purge::PurgeOutcome::Reclaimed { chunks: 1 }
        ),
        "{outcome:?}"
    );
    assert_eq!(
        counted(&e, shared).await,
        1,
        "the neighbour's chunk still counts"
    );
    let done = head(&e).await.unwrap();
    assert_eq!((done.state, done.reclaimed), (HeadState::Purged, done.head));
    assert_eq!(
        e.intervals(&HASH).await.unwrap(),
        [],
        "'k' went with the last decrement"
    );
}

/// Delete the queue `hash` to its end, then purge it at once: the purge's outcome.
async fn delete_and_purge(
    e: &Arc<ShardEngine>,
    hash: [u8; 16],
) -> crate::shard::seal::gc::purge::PurgeOutcome {
    let step = |op| e.submit_native_queue(hash, op);
    step(NqOp::BeginDelete).await.unwrap();
    while !matches!(
        step(NqOp::DeleteStep { max_rows: 64 }).await.unwrap(),
        NqOut::DeleteStepped { remaining: false }
    ) {}
    e.request_purge(hash, 0).await.unwrap();
    e.purge_stream_at(hash, u64::MAX).await.unwrap()
}

/// **A deleted queue's purge reclaims every chunk up to its stream's next
/// offset and deletes its tail** (S3b-DESIGN §5.9 step 4.2). A queue never
/// trimmed (no `'H'` row) holds three sealed chunks: one acked, one leased,
/// one untaken. Deleted and purged, its first step puts the head at the
/// stream's next offset, 3, and deletes the queue's tail; the second
/// decrements all three chunks, so each object reaches zero.
#[tokio::test(flavor = "multi_thread", worker_threads = 2, crate = "crate::test_rt")]
async fn a_deleted_queues_purge_reclaims_every_chunk_up_to_its_next_offset() {
    let (e, db, _c) = clocked("nq-trim-purge-next", QueueConfig::default()).await;
    for body in ["a", "b", "c"] {
        send_sealed(&e, msg(body)).await;
    }
    let objects: Vec<_> = e
        .live_chunks(HASH)
        .await
        .unwrap()
        .iter()
        .map(|c| c.object)
        .collect();
    assert_eq!(objects.len(), 3, "one chunk per seal");
    let leased = claim(&e, 2).await;
    ack(&e, &leased[..1]).await;
    let outcome = delete_and_purge(&e, HASH).await;
    assert!(
        matches!(
            outcome,
            crate::shard::seal::gc::purge::PurgeOutcome::Reclaimed { chunks: 3 }
        ),
        "{outcome:?}"
    );
    let done = head(&e).await.unwrap();
    assert_eq!(
        (done.state, done.head, done.reclaimed),
        (HeadState::Purged, 3, 3)
    );
    for id in &objects {
        assert_eq!(counted(&e, *id).await, 0, "{id:?}");
    }
    let tail = db.get(crate::shard::queue_tail_key(&HASH)).await.unwrap();
    assert!(tail.is_none(), "the queue's tail is gone");
}

/// A purge request on a trimmed queue sets its instant once: the Trimming
/// row becomes Requested at the request's close instant plus the purge
/// delay, and a later request keeps the row as it is.
#[tokio::test(flavor = "multi_thread", worker_threads = 2, crate = "crate::test_rt")]
async fn a_purge_request_sets_a_trimmed_queues_instant_once() {
    let (e, _db, _c) = clocked("nq-trim-purge-instant", QueueConfig::default()).await;
    for body in ["a", "b"] {
        send_sealed(&e, msg(body)).await;
    }
    ack(&e, &claim(&e, 1).await).await;
    e.trim_pass().await.unwrap();
    assert_eq!(head(&e).await.unwrap().state, HeadState::Trimming);
    let delay = u64::try_from(e.gc_state().settings().purge_delay.as_millis()).unwrap();
    e.request_purge(HASH, 5_000).await.unwrap();
    let first = head(&e).await.unwrap();
    assert_eq!(
        (first.state, first.not_before_ms),
        (HeadState::Requested, 5_000 + delay)
    );
    e.request_purge(HASH, 9_000).await.unwrap();
    assert_eq!(head(&e).await, Some(first), "a Requested row is kept");
}

/// A pass reads at most `trim_hints` hint rows: two seals' hints take two
/// passes, one each.
#[tokio::test(flavor = "multi_thread", worker_threads = 2, crate = "crate::test_rt")]
async fn a_pass_reads_at_most_its_hint_budget() {
    let (e, _db, _c) = clocked("nq-trim-hint-budget", QueueConfig::default()).await;
    e.gc_state().configure(|s| s.trim_hints = 1);
    for body in ["a", "b"] {
        send_sealed(&e, msg(body)).await;
    }
    assert_eq!(hint_count(&e).await, 2);
    for left in [1, 0] {
        assert_eq!(e.trim_pass().await.unwrap().hints, 1);
        assert_eq!(hint_count(&e).await, left);
    }
}

/// A pass leaves a queue the purge owns alone: once a purge request turned
/// its Trimming row Requested, an ack's hint releases nothing and the row
/// stays Requested.
#[tokio::test(flavor = "multi_thread", worker_threads = 2, crate = "crate::test_rt")]
async fn a_pass_releases_nothing_of_a_queue_whose_purge_was_requested() {
    let (e, _db, _c) = clocked("nq-trim-requested", QueueConfig::default()).await;
    for body in ["a", "b", "c"] {
        send_sealed(&e, msg(body)).await;
    }
    let leased = claim(&e, 3).await;
    ack(&e, &leased[..1]).await;
    e.trim_pass().await.unwrap();
    e.request_purge(HASH, 0).await.unwrap();
    ack(&e, &leased[1..2]).await;
    let report = e.trim_pass().await.unwrap();
    assert_eq!((report.hints, report.released), (1, 0));
    assert_eq!(head(&e).await.unwrap().state, HeadState::Requested);
    assert_eq!(chunks(&e).await[1], ((1, 1), 1), "chunk 1 keeps its count");
}

/// A pass examines the chunk at the queue's prefix and the chunks its
/// hints name, no other: four chunks, the claim of two and the ack of the
/// second name `[0, 2)`; the pass examines chunks 0 and 1 and releases 1.
#[tokio::test(flavor = "multi_thread", worker_threads = 2, crate = "crate::test_rt")]
async fn a_pass_examines_the_prefix_chunk_and_the_hinted_ones_only() {
    let (e, _db, _c) = clocked("nq-trim-examined", QueueConfig::default()).await;
    for body in ["a", "b", "c", "d"] {
        send_sealed(&e, msg(body)).await;
    }
    let first = e.trim_pass().await.unwrap();
    assert_eq!((first.examined, first.released), (4, 0), "the seals' hints");
    let leased = claim(&e, 2).await;
    ack(&e, &leased[1..]).await;
    let report = e.trim_pass().await.unwrap();
    assert_eq!((report.examined, report.released), (2, 1));
}

/// `clocked` over a fault store whose WAL writes a test can hold, the gc's
/// own passes off: the engine, its database, the clock and the store.
async fn clocked_on_faults(
    name: &str,
) -> (Arc<ShardEngine>, Arc<slatedb::Db>, i64, Arc<FaultStore>) {
    let inner = Arc::new(object_store::memory::InMemory::new());
    let store = FaultStore::new(inner, 0x7A1, crate::dst::FaultProfile::clean());
    let db = crate::keyspace::db_builder(name, store.clone());
    let db = Arc::new(db.build().await.unwrap());
    let e = ShardEngine::start(
        name.to_owned(),
        db.clone(),
        store.clone(),
        crate::shard::ShardConfig::default(),
        None,
        Default::default(),
    );
    e.gc_state().configure(|s| s.auto = false);
    super::create(&e, QueueConfig::default()).await;
    let c = crate::shard::now_ms() + crate::native_queue::DAY_MS;
    super::set_clock(&db, c).await;
    (e, db, c, store)
}

/// Park every WAL write until `release_hold`: what is written from now on
/// is in memory, not durable.
fn hold_the_wal(store: &FaultStore) {
    store.hold_class(StoreOp::Put, ObjClass::Wal, 1_000);
}

/// Waits, two seconds at most, until `ready` answers true.
async fn until<F: Future<Output = bool>>(what: &str, mut ready: impl FnMut() -> F) {
    for _ in 0..200 {
        if ready().await {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("{what}: not within two seconds");
}

/// Whether memory holds exactly `n` hint rows.
async fn hints_now(e: &ShardEngine, n: usize) -> bool {
    hint_count(e).await == n
}

/// **A pass reads only durable hints** (S3b-DESIGN §7.4): an ack whose
/// WAL write has not landed puts a hint only memory holds; the pass reads
/// none, so it writes nothing and waits for no WAL write. Once the WAL
/// lands, the next pass releases the acked chunk.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_pass_reads_no_hint_whose_wal_write_has_not_landed() {
    let (e, _db, _c, store) = clocked_on_faults("nq-trim-durable-hints").await;
    for body in ["a", "b"] {
        send_sealed(&e, msg(body)).await;
    }
    let leased = claim(&e, 2).await;
    e.trim_pass().await.unwrap();
    assert_eq!(hint_count(&e).await, 0);
    hold_the_wal(&store);
    let check = async {
        until("the ack's hint is in memory", || hints_now(&e, 1)).await;
        let pass = tokio::time::timeout(Duration::from_secs(2), e.trim_pass()).await;
        store.release_hold();
        pass.expect("the pass waited for a WAL write").unwrap()
    };
    let ((), report) = futures_util::future::join(ack(&e, &leased[..1]), check).await;
    assert_eq!(report, Default::default(), "nothing durable to read");
    assert_eq!(e.trim_pass().await.unwrap().released, 1, "the WAL landed");
}

/// Whether the queue's range row 0 as memory holds it has lost `offset`'s
/// scheduled bit (a row with no bit left is deleted).
async fn sbit_cleared(e: &ShardEngine, offset: u64) -> bool {
    let raw =
        e.db.get(keys::by_offset(&HASH, keys::RANGE, 0))
            .await
            .unwrap();
    !raw.is_some_and(|raw| RangeRow::decode(&raw).unwrap().sbits.contains(offset))
}

/// Park the next index batches (the gc's and the fold's) before they reach
/// the shard log: once one parks, its writer has read all it reads.
fn hold_index_batches(e: &ShardEngine) -> Arc<Hold> {
    let hold = Hold::new();
    *e.layout.seal.fold.hooks.batch_hold.lock().unwrap() = Some(hold.clone());
    hold
}

/// Once `held`'s batch parked, let the WAL land and the batch go on.
async fn land(store: &FaultStore, held: &Hold, what: &str) {
    until(what, || async { held.entered.load(Ordering::Acquire) > 0 }).await;
    store.release_hold();
    held.release();
}

/// **A pass reads a chunk's references as they are durable**: a cancel
/// of the scheduled message that holds a chunk, whose WAL write has not
/// landed, clears its bit only in memory, and the pass keeps the chunk.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_pass_keeps_a_chunk_whose_scheduled_bit_is_cleared_only_in_memory() {
    let (e, db, c, store) = clocked_on_faults("nq-trim-durable-sbits").await;
    let later = SendMessage {
        deliver_at: Some(c + 3_600_000),
        ..msg("later")
    };
    op(&e, send_op(vec![msg("a"), later])).await.unwrap();
    seal(&e).await;
    let leased = claim(&e, 2).await;
    assert_eq!(leased.len(), 1, "0 leased, the scheduled 1 passed");
    ack(&e, &leased).await;
    assert_eq!(super::state(&db).await.frontier, 2);
    hold_the_wal(&store);
    let check = async {
        until("the cancel cleared the bit in memory", || {
            sbit_cleared(&e, 1)
        })
        .await;
        let held = hold_index_batches(&e);
        let release = land(&store, &held, "the pass parked its batch");
        let (report, ()) = futures_util::future::join(e.trim_pass(), release).await;
        report.unwrap()
    };
    let (cancelled, report) =
        futures_util::future::join(op(&e, NqOp::Cancel { offsets: vec![1] }), check).await;
    cancelled.unwrap();
    assert_eq!(
        (report.examined, report.released),
        (1, 0),
        "held by its bit"
    );
}

/// Whether the queue's intervals as memory holds them are `want`.
async fn intervals_now(e: &ShardEngine, want: &[Interval]) -> bool {
    let (from, to) = intervals::key_range(&HASH);
    let mut rows = e.db.scan(from..to).await.unwrap();
    let mut out = Vec::new();
    while let Some(kv) = rows.next().await.unwrap() {
        out.push(intervals::decode(&kv.key, &kv.value).unwrap());
    }
    out == want
}

/// **A fold drops only what the shard log durably holds as released**:
/// while a pass's grown interval waits for its WAL write, a fold drops the
/// entry of the chunk released before and keeps the one that pass released
/// (a later fold drops it).
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_fold_drops_no_entry_of_an_interval_whose_wal_write_has_not_landed() {
    let (e, _db, _c, store) = clocked_on_faults("nq-trim-durable-k").await;
    for body in ["a", "b", "c", "d"] {
        send_sealed(&e, msg(body)).await;
    }
    let leased = claim(&e, 4).await;
    ack(&e, &leased[1..2]).await;
    e.trim_pass().await.unwrap();
    assert_eq!(e.intervals(&HASH).await.unwrap(), [iv(1, 1)]);
    ack(&e, &leased[2..3]).await;
    hold_the_wal(&store);
    let grown = [iv(1, 2)];
    let fold = async {
        until("the pass's interval is in memory", || {
            intervals_now(&e, &grown)
        })
        .await;
        let held = hold_index_batches(&e);
        let release = land(&store, &held, "the fold parked its batch");
        let (folded, ()) = futures_util::future::join(e.fold_once(), release).await;
        folded.unwrap();
    };
    let (report, ()) = futures_util::future::join(e.trim_pass(), fold).await;
    assert_eq!(report.unwrap().released, 1);
    let spans: Vec<(u64, u64)> = chunks(&e).await.iter().map(|c| c.0).collect();
    assert_eq!(spans, [(0, 0), (2, 2), (3, 3)], "[2, 2] kept");
}
