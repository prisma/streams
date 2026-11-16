#![cfg(test)]
//! The cleaner's queue rule (S3b-DESIGN §7.4; S3b part 2 step 4; the
//! cleaner ruling of 2026-11-16, tests T2-T7), on a gc rig whose seals cut
//! a queue's pages four to a chunk and put two queues' chunks (and plain
//! streams') in one object: a queue's live set loses what its trim
//! released, read again under `refs` before the copy commits; a queue's
//! chunks are never joined; an object with a free queue chunk waits for the
//! trim; and the saving counts until the queue chunks' latest hold. Each
//! test asserts the objects' `'R'` counts exactly.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;

use super::super::super::gc_tests::{gc_rig, purge_now, unnamed};
use super::super::super::test_hooks::Hold as HoldPoint;
use super::super::super::tests::{Oracle, append_recorded};
use super::super::CleanHook;
use super::CleanReport;
use crate::native_queue::{
    NqOp, NqOut, PrepareFrom, QueueConfig, SendMessage, SettleItem, SettleKind, Token,
};
use crate::shard::ShardEngine;
use crate::spill::gc;
use crate::spill::object::ObjectId;

const QUEUE: [u8; 16] = [9; 16];
const NEIGHBOUR: [u8; 16] = [7; 16];
const LEDGER: [u8; 16] = [0x50; 16];
const DEAD: [u8; 16] = [0x51; 16];

async fn op(e: &ShardEngine, hash: [u8; 16], op: NqOp) -> NqOut {
    e.submit_native_queue(hash, op).await.unwrap()
}

/// `len` bytes that do not compress (a seeded xorshift).
fn noise(seed: u64, len: usize) -> Vec<u8> {
    let mut x = seed | 1;
    (0..len)
        .map(|_| {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            x.to_le_bytes()[0]
        })
        .collect()
}

/// A send of `n` messages of `len` incompressible bytes each.
fn send(n: usize, len: usize) -> NqOp {
    let messages = (0..n)
        .map(|i| SendMessage {
            envelope: Bytes::from(noise(i as u64 + len as u64, len)),
            deliver_at: None,
            dedup_tag: None,
        })
        .collect();
    NqOp::Send {
        messages,
        subkey: [3; 32],
        key_version: 1,
    }
}

async fn seal(e: &Arc<ShardEngine>) {
    e.seal_once(crate::shard::seal::Trigger::Requested)
        .await
        .unwrap();
}

/// A claim of up to `max` of `QUEUE`, its sealed pages prepared: each
/// lease's offset and generation.
async fn claim(e: &ShardEngine, max: usize) -> Vec<(u64, u32)> {
    let (prepared, _) = e
        .prepare_queue_pages(QUEUE, PrepareFrom::Frontier, usize::MAX)
        .await
        .unwrap();
    let claim = NqOp::Claim {
        max,
        max_bytes: 1 << 24,
        lease_ms: 600_000,
        prepared,
    };
    let NqOut::Claimed { deliveries, .. } = op(e, QUEUE, claim).await else {
        panic!("a claim answers deliveries");
    };
    deliveries.iter().map(|d| (d.offset, d.lease_gen)).collect()
}

async fn ack(e: &ShardEngine, leases: &[(u64, u32)]) {
    let items = leases
        .iter()
        .map(|&(offset, lease_gen)| SettleItem {
            token: Token { offset, lease_gen },
            kind: SettleKind::Ack,
            code: None,
        })
        .collect();
    op(e, QUEUE, NqOp::Settle { items }).await;
}

/// The live chunks the `'R'` row of `object` counts (0 once it is gone).
async fn counted(e: &ShardEngine, object: ObjectId) -> u64 {
    let raw = e.db.get(gc::refcount_key(object)).await.unwrap();
    raw.as_deref()
        .and_then(gc::decode_refcount)
        .map_or(0, |r| r.live_chunks)
}

/// The queue's live chunks: (object, span).
async fn queue_chunks(e: &ShardEngine) -> Vec<(ObjectId, (u64, u64))> {
    let live = e.live_chunks(QUEUE).await.unwrap();
    live.iter().map(|c| (c.object, (c.first, c.last))).collect()
}

/// A test rule: anything less than half live, at any saving (the rig's
/// objects are small).
fn half_dead(object_len: u64, live_bytes: u64, live_chunks: u64, _saving_ms: u64) -> bool {
    live_chunks > 0 && 2 * live_bytes < object_len
}

/// A gc rig (four pages a chunk) with `QUEUE` (of `cfg`) and `NEIGHBOUR`
/// created, its passes run by hand and purges due at once.
async fn rig(label: &str, cfg: QueueConfig) -> (super::super::super::tests::Rig, Arc<ShardEngine>) {
    let rig = gc_rig(label).await;
    let e = rig.engine.clone();
    e.gc_state().configure(|s| {
        s.auto = false;
        s.purge_delay = Duration::ZERO;
        s.clean_pays = half_dead;
    });
    for (hash, cfg) in [(QUEUE, cfg), (NEIGHBOUR, QueueConfig::default())] {
        op(
            &e,
            hash,
            NqOp::Create {
                cfg,
                container: [5; 16],
            },
        )
        .await;
    }
    (rig, e)
}

/// Delete the queue `hash` to its end, then purge it at once.
async fn delete_and_purge(e: &Arc<ShardEngine>, hash: [u8; 16]) {
    op(e, hash, NqOp::BeginDelete).await;
    while !matches!(
        op(e, hash, NqOp::DeleteStep { max_rows: 64 }).await,
        NqOut::DeleteStepped { remaining: false }
    ) {}
    e.request_purge(hash, 0).await.unwrap();
    e.purge_stream_at(hash, u64::MAX).await.unwrap();
}

/// `n` messages of `QUEUE` sealed with two large ones of `NEIGHBOUR`.
async fn shared_seal(e: &Arc<ShardEngine>, n: usize) {
    op(e, QUEUE, send(n, 16)).await;
    op(e, NEIGHBOUR, send(2, 8_000)).await;
    seal(e).await;
}

/// **T2: a queue with no `'H'` row keeps its chunks apart.** Two seals,
/// each four untaken queue messages beside the neighbour's large ones; the
/// neighbour is purged. The pass copies both queue chunks, each with its
/// own span (the tip joined them into `[0, 7]`), and both old objects reach
/// zero.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_never_trimmed_queues_chunks_are_copied_apart() {
    let (_rig, e) = rig("clean-q-apart", QueueConfig::default()).await;
    shared_seal(&e, 4).await;
    shared_seal(&e, 4).await;
    let old: Vec<ObjectId> = queue_chunks(&e).await.iter().map(|c| c.0).collect();
    delete_and_purge(&e, NEIGHBOUR).await;
    let report = e.clean_pass().await.unwrap();
    let want = CleanReport {
        released: 2,
        copied_chunks: 2,
        abandoned: false,
    };
    assert_eq!(report, want);
    let spans: Vec<(u64, u64)> = queue_chunks(&e).await.iter().map(|c| c.1).collect();
    assert_eq!(spans, [(0, 3), (4, 7)]);
    for id in &old {
        assert_eq!(counted(&e, *id).await, 0, "{id:?}");
    }
    e.begin_close();
}

/// **T3: a trimmed queue's copy holds only what its trim kept.** One seal
/// cuts eight queue messages into `[0, 3]` and `[4, 7]` beside the
/// neighbour's; all eight are leased and `[4, 7]` acked, so the trim
/// releases it into a `'k'` interval behind the held `[0, 3]`. Once the
/// neighbour is purged, the pass copies `[0, 3]` alone and the old object's
/// row reaches zero (the tip skipped the Trimming queue as pinned).
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_trimmed_queues_copy_holds_only_its_kept_chunk() {
    let (_rig, e) = rig("clean-q-trimmed", QueueConfig::default()).await;
    shared_seal(&e, 8).await;
    let old = queue_chunks(&e).await[0].0;
    let leased = claim(&e, 8).await;
    ack(&e, &leased[4..]).await;
    e.trim_pass().await.unwrap();
    assert_eq!(e.intervals(&QUEUE).await.unwrap().len(), 1, "[4, 7] in 'k'");
    delete_and_purge(&e, NEIGHBOUR).await;
    assert_eq!(counted(&e, old).await, 1, "kept by [0, 3]");
    let report = e.clean_pass().await.unwrap();
    let want = CleanReport {
        released: 1,
        copied_chunks: 1,
        abandoned: false,
    };
    assert_eq!(report, want);
    assert_eq!(counted(&e, old).await, 0);
    let moved: Vec<(u64, u64)> = queue_chunks(&e)
        .await
        .iter()
        .filter(|c| c.0 != old)
        .map(|c| c.1)
        .collect();
    assert_eq!(moved, [(0, 3)]);
    e.begin_close();
}

/// Two shared objects, `[0, 3]` and `[4, 7]`, all leased, the neighbour
/// purged: each a candidate kept by one held queue chunk. The objects.
async fn two_held(e: &Arc<ShardEngine>) -> (Vec<ObjectId>, Vec<(u64, u32)>) {
    shared_seal(e, 4).await;
    shared_seal(e, 4).await;
    let old: Vec<ObjectId> = queue_chunks(e).await.iter().map(|c| c.0).collect();
    let leased = claim(e, 8).await;
    delete_and_purge(e, NEIGHBOUR).await;
    (old, leased)
}

/// Run a pass that parks after its copy's PUT, ack `release` and run a trim
/// while it is parked, and return the pass's report.
async fn race_a_trim(e: &Arc<ShardEngine>, release: &[(u64, u32)]) -> CleanReport {
    let hold = HoldPoint::new();
    *e.gc_state().clean_hook.lock().unwrap() = Some(CleanHook::Hold(hold.clone()));
    let race = async {
        hold.wait_entered().await;
        ack(e, release).await;
        e.trim_pass().await.unwrap();
        hold.release();
    };
    let (report, ()) = tokio::join!(e.clean_pass(), race);
    report.unwrap()
}

/// **T4: a trim between the copy's PUT and `refs` abandons it** (pins:
/// the tip never copies a queue chunk). Both shared objects are copied;
/// meanwhile `[4, 7]` is acked and the trim releases it into an interval
/// behind the held `[0, 3]`. Under `refs` the pass reads the intervals
/// again, abandons the copy whole and decrements nothing: the old objects
/// hold exactly the trim's one decrement (1 and 0), and the copy is an
/// unnamed object the sweep deletes. Control: `counted` reading no `'k'`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_trim_into_an_interval_while_the_copy_waits_abandons_it() {
    let (rig, e) = rig("clean-q-race-k", QueueConfig::default()).await;
    let (old, leased) = two_held(&e).await;
    let report = race_a_trim(&e, &leased[4..]).await;
    assert!(report.abandoned, "{report:?}");
    assert_eq!(
        (counted(&e, old[0]).await, counted(&e, old[1]).await),
        (1, 0)
    );
    assert_eq!(unnamed(&rig, &["hseg"]).await.len(), 1, "the copy");
    e.begin_close();
}

/// **T4, by the prefix:** meanwhile `[0, 3]` is acked and the trim moves
/// `reclaimed` past it. The pass abandons the copy; the old objects hold
/// exactly the trim's decrement (0 and 1). Control: `counted` ignoring
/// `reclaimed`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_trim_past_the_prefix_while_the_copy_waits_abandons_it() {
    let (rig, e) = rig("clean-q-race-prefix", QueueConfig::default()).await;
    let (old, leased) = two_held(&e).await;
    let report = race_a_trim(&e, &leased[..4]).await;
    assert!(report.abandoned, "{report:?}");
    assert_eq!(
        (counted(&e, old[0]).await, counted(&e, old[1]).await),
        (0, 1)
    );
    assert_eq!(unnamed(&rig, &["hseg"]).await.len(), 1, "the copy");
    e.begin_close();
}

/// `two_held` on a Trimming queue: a third queue message sealed alone is
/// leased and acked, so the trim releases it into an interval behind the
/// held ones. The engine and its rig.
async fn trimming(label: &str) -> (super::super::super::tests::Rig, Arc<ShardEngine>) {
    let (rig, e) = rig(label, QueueConfig::default()).await;
    let _ = two_held(&e).await;
    op(&e, QUEUE, send(1, 16)).await;
    seal(&e).await;
    ack(&e, &claim(&e, 1).await).await;
    assert_eq!(e.trim_pass().await.unwrap().released, 1);
    let head = e.head_row(&QUEUE).await.unwrap().unwrap();
    assert_eq!(head.state, gc::HeadState::Trimming);
    (rig, e)
}

/// A pass over a queue the purge owns: nothing copied, and only the two
/// candidates' tables read, no chunk.
async fn skipped(rig: &super::super::super::tests::Rig, e: &ShardEngine, why: &str) {
    let gets = || {
        rig.fault
            .count(crate::dst::StoreOp::Get, crate::dst::ObjClass::Other)
    };
    let before = gets();
    assert_eq!(
        e.clean_pass().await.unwrap(),
        CleanReport::default(),
        "{why}"
    );
    assert!(gets() - before <= 2, "{why}: the tables at most, no chunk");
}

/// **T5: a queue the purge owns is not copied** (pins: the tip never
/// copies a queue chunk). On a Trimming queue, a purge request between the
/// copy's PUT and `refs` abandons the copy. A queue Requested, deleting, or
/// with its state row deleted beside its Trimming row when the pass gathers
/// is skipped: nothing copied, no chunk read.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_queue_the_purge_owns_is_not_copied() {
    let (_rig, e) = trimming("clean-q-purge-race").await;
    let hold = HoldPoint::new();
    *e.gc_state().clean_hook.lock().unwrap() = Some(CleanHook::Hold(hold.clone()));
    let race = async {
        hold.wait_entered().await;
        let later = crate::shard::now_ms().unsigned_abs() + 3_600_000;
        e.request_purge(QUEUE, later).await.unwrap();
        hold.release();
    };
    let (report, ()) = tokio::join!(e.clean_pass(), race);
    assert!(report.unwrap().abandoned, "Requested under refs");
    e.begin_close();
    let (rig, e) = trimming("clean-q-purge-requested").await;
    let later = crate::shard::now_ms().unsigned_abs() + 3_600_000;
    e.request_purge(QUEUE, later).await.unwrap();
    skipped(&rig, &e, "Requested").await;
    e.begin_close();
    let (rig, e) = trimming("clean-q-purge-deleting").await;
    op(&e, QUEUE, NqOp::BeginDelete).await;
    skipped(&rig, &e, "deleting").await;
    e.begin_close();
    let (rig, e) = trimming("clean-q-purge-stateless").await;
    op(&e, QUEUE, NqOp::BeginDelete).await;
    while !matches!(
        op(&e, QUEUE, NqOp::DeleteStep { max_rows: 64 }).await,
        NqOut::DeleteStepped { remaining: false }
    ) {}
    let head = e.head_row(&QUEUE).await.unwrap().unwrap();
    assert_eq!(head.state, gc::HeadState::Trimming, "no request yet");
    skipped(&rig, &e, "no state row beside a Trimming row").await;
    e.begin_close();
}

/// A ledger chunk, a dead stream's large chunk and one queue chunk in one
/// object; the dead stream purged. The object.
async fn ledger_and_queue(rig: &super::super::super::tests::Rig, e: &Arc<ShardEngine>) -> ObjectId {
    let (oracle, dead) = (Mutex::new(Oracle::new()), Mutex::new(Oracle::new()));
    append_recorded(e, &oracle, LEDGER, vec![Bytes::from(vec![1; 32])]).await;
    let big = vec![Bytes::from(vec![0xDD; 900]), Bytes::from(vec![0xEE; 900])];
    append_recorded(e, &dead, DEAD, big).await;
    op(e, QUEUE, send(1, 16)).await;
    seal(e).await;
    let id = queue_chunks(e).await[0].0;
    purge_now(&rig.engine, DEAD).await;
    id
}

/// **T7: an object with a free queue chunk waits for the trim.** A ledger
/// chunk and a queue chunk acked behind the frontier (free, not yet
/// trimmed) share an object a dead stream's purge left mostly dead. The pass
/// copies nothing (the tip copied both); the trim then gives the queue chunk
/// back, and the next pass copies the ledger's chunk alone and the old
/// object reaches zero.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn an_object_with_a_free_queue_chunk_waits_for_the_trim() {
    let (rig, e) = rig("clean-q-free", QueueConfig::default()).await;
    let id = ledger_and_queue(&rig, &e).await;
    let leased = claim(&e, 1).await;
    ack(&e, &leased).await;
    assert_eq!(counted(&e, id).await, 2, "the ledger's and the queue's");
    assert_eq!(e.clean_pass().await.unwrap(), CleanReport::default());
    assert_eq!(e.trim_pass().await.unwrap().released, 1);
    assert_eq!(counted(&e, id).await, 1, "the ledger's");
    let want = CleanReport {
        released: 1,
        copied_chunks: 1,
        abandoned: false,
    };
    assert_eq!(e.clean_pass().await.unwrap(), want);
    assert_eq!(counted(&e, id).await, 0);
    e.begin_close();
}

/// One object of the neighbour's ~1 MB, kept by one leased message of a
/// queue of `cfg` (and, `with_ledger`, by a ledger's live chunk), under the
/// production rule: the pass's report.
async fn held(label: &str, cfg: QueueConfig, with_ledger: bool) -> CleanReport {
    let (_rig, e) = rig(label, cfg).await;
    e.gc_state()
        .configure(|s| s.clean_pays = super::clean_pays_over);
    if with_ledger {
        let oracle = Mutex::new(Oracle::new());
        append_recorded(&e, &oracle, LEDGER, vec![Bytes::from(vec![1; 32])]).await;
    }
    op(&e, QUEUE, send(1, 16)).await;
    op(&e, NEIGHBOUR, send(16, 64_000)).await;
    seal(&e).await;
    claim(&e, 1).await;
    delete_and_purge(&e, NEIGHBOUR).await;
    let report = e.clean_pass().await.unwrap();
    e.begin_close();
    report
}

/// **T6, in the engine: the saving counts until the queue chunk's hold.**
/// An object of about 1 MB dead kept by one leased queue message pays over
/// the default queue's 18 days (retention plus failure retention), not over
/// two hours (a queue of one hour each), under the production prices.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_queue_chunk_held_for_hours_does_not_pay_a_copy() {
    let far = held("clean-q-far", QueueConfig::default(), false).await;
    assert_eq!((far.released, far.copied_chunks), (1, 1), "{far:?}");
    let near = held("clean-q-near", hours(), false).await;
    assert_eq!(near, CleanReport::default(), "two hours do not pay");
}

/// A queue whose messages are held for an hour each.
fn hours() -> QueueConfig {
    let hour = crate::native_queue::HOUR_MS;
    QueueConfig {
        retention_ms: hour,
        failure_retention_ms: hour,
        dedup_window_ms: hour,
        ..QueueConfig::default()
    }
}

/// **T6, beside an ordinary chunk: the saving counts a month.** The object
/// that does not pay over its queue chunk's two hours, with a ledger's live
/// chunk beside the queue's: the ledger's chunk holds the object for good,
/// so its dead bytes save for a month and the copy pays, both chunks
/// moved. Control: the saving taken from the queue chunk alone.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn an_ordinary_chunk_beside_a_held_queue_chunk_saves_for_a_month() {
    let report = held("clean-q-mixed", hours(), true).await;
    let want = CleanReport {
        released: 1,
        copied_chunks: 2,
        abandoned: false,
    };
    assert_eq!(report, want);
}
