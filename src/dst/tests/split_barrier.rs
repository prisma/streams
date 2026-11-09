#![cfg(test)]
//! The split's barrier on a serving engine (S4-DESIGN §5.2): it takes effect after the group
//! that carries it. That group commits and is answered; operations queued after it wait, never
//! offered to a group, until the split releases the barrier. A resume serves them in order; a
//! barrier dropped unreleased closes the engine rather than resume a parent it cannot vouch for.
//! Past the 2 s hold, the held appends and every later one are answered `503 shard_splitting`,
//! `Retry-After: 1`, until the release. Once the split published, a held append whose child
//! another instance owns is answered that owner's `409 not_ring_owner`.

use super::billing_closure_debts::{JSON, engine_of, raw_put, stored};
use super::fixture_http::{engine_shutdown, http_rig, http_rig_named};
use super::fixture_requests::hreq;
use super::fixture_storage::mem;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

type State = Arc<crate::http::AppState>;
type Engine = Arc<crate::shard::ShardEngine>;
type Addr = std::net::SocketAddr;

const NAME: &str = "barrier";
const BOUND: Duration = Duration::from_secs(5);

/// Wait until `ready`, at most [`BOUND`].
async fn until(what: &str, ready: impl Fn() -> bool) {
    let t0 = Instant::now();
    while !ready() {
        assert!(t0.elapsed() < BOUND, "{what}");
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
}

/// An append of `{"n": n}` to the stream: its status. `answered` is set once it returns.
async fn append(addr: Addr, n: u32, answered: &AtomicBool) -> u16 {
    let answer = append_answer(addr, n).await;
    answered.store(true, Ordering::SeqCst);
    answer.0
}

/// An append of `{"n": n}`: its status, `Retry-After` and error code.
async fn append_answer(addr: Addr, n: u32) -> (u16, Option<String>, Option<String>) {
    let body = format!(r#"[{{"n":{n}}}]"#);
    let path = format!("/v1/stream/{NAME}");
    let (status, headers, body) = hreq(addr, "POST", &path, &[JSON], body.as_bytes()).await;
    let error: serde_json::Value = serde_json::from_slice(&body).unwrap_or_default();
    let code = error["error"]["code"].as_str().map(str::to_string);
    (status, headers.get("retry-after").cloned(), code)
}

/// The stream's records, read back.
async fn records(addr: Addr) -> serde_json::Value {
    let (status, _, body) = hreq(addr, "GET", &format!("/v1/stream/{NAME}"), &[], b"").await;
    assert_eq!(status, 200, "{}", String::from_utf8_lossy(&body));
    serde_json::from_slice(&body).unwrap_or_default()
}

/// A rig serving the stream with one record, `{"n":1}`, and the engine of its one segment.
async fn serving() -> (State, Addr, Engine) {
    let (state, addr) = http_rig(mem()).await;
    raw_put(addr, NAME, &[JSON], br#"[{"n":1}]"#).await;
    let sref = state.deployment.raw_adapter_sref(NAME);
    let desc = stored(&state, &sref).await;
    let engine = engine_of(&state, &desc, 0).await;
    (state, addr, engine)
}

/// **The barrier parks the committer after its own group, and a resume serves the held
/// appends in order.** An append and the barrier are queued into one group: the append is
/// answered and the barrier parks. An append queued after it is still unanswered 200 ms later;
/// after the resume it is answered, and the stream reads `1, 2, 3`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_barrier_parks_after_its_group_and_a_resume_serves_the_held_appends_in_order() {
    let (state, addr, engine) = serving().await;
    let hold = engine.test_hold_commit().await;
    let base = engine.appends_enqueued();
    let parked = async {
        until("the append is enqueued", || {
            engine.appends_enqueued() > base
        })
        .await;
        let release = async {
            until("the barrier is queued", || engine.queued_ops() > 0).await;
            drop(hold);
        };
        let (barrier, ()) = futures_util::future::join(engine.barrier(), release).await;
        barrier.expect("the barrier parks")
    };
    let in_group = AtomicBool::new(false);
    let both = futures_util::future::join(append(addr, 2, &in_group), parked);
    let (status, barrier) = tokio::time::timeout(BOUND, both).await.expect("parked");
    assert_eq!(status, 204, "the barrier's group is answered");
    let answered = AtomicBool::new(false);
    let control = async {
        until("the held append is enqueued", || {
            engine.appends_enqueued() > base + 1
        })
        .await;
        tokio::time::sleep(Duration::from_millis(200)).await;
        let waited = !answered.load(Ordering::SeqCst);
        barrier.resume();
        waited
    };
    let both = futures_util::future::join(append(addr, 3, &answered), control);
    let (status, waited) = tokio::time::timeout(BOUND, both).await.expect("served");
    assert!(waited, "an append after the barrier waits for the release");
    assert_eq!(status, 204);
    assert_eq!(
        records(addr).await,
        serde_json::json!([{"n": 1}, {"n": 2}, {"n": 3}])
    );
    engine_shutdown(&state).await;
}

/// **A barrier dropped unreleased closes its engine.** A split that dies in the pause never
/// resumes the parent: the engine closes, and the append it held is answered (by a later
/// engine or with a refusal), never left waiting.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_barrier_dropped_unreleased_closes_its_engine() {
    let (state, addr, engine) = serving().await;
    let barrier = tokio::time::timeout(BOUND, engine.barrier())
        .await
        .expect("parked")
        .expect("the barrier parks");
    let base = engine.appends_enqueued();
    let answered = AtomicBool::new(false);
    let control = async {
        until("the held append is enqueued", || {
            engine.appends_enqueued() > base
        })
        .await;
        let open = !engine.is_closed();
        drop(barrier);
        until("the engine closes", || engine.is_closed()).await;
        open
    };
    let both = futures_util::future::join(append(addr, 2, &answered), control);
    let (_, open) = tokio::time::timeout(BOUND, both)
        .await
        .expect("the held append is answered");
    assert!(open, "open while parked");
    engine_shutdown(&state).await;
}

/// **Past its hold, a parked engine answers `503 shard_splitting` until the release.** An
/// append held by the barrier is answered after the 2 s hold, `503 shard_splitting` with
/// `Retry-After: 1`, and so is one sent after it, at once. After the resume an append is
/// served, and the stream holds neither refused record.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn past_its_hold_a_parked_engine_answers_503_shard_splitting_until_the_release() {
    let (state, addr, engine) = serving().await;
    let barrier = tokio::time::timeout(BOUND, engine.barrier())
        .await
        .expect("parked")
        .expect("the barrier parks");
    let parked_at = Instant::now();
    let splitting = (
        503,
        Some("1".to_string()),
        Some("shard_splitting".to_string()),
    );
    let held = tokio::time::timeout(BOUND, append_answer(addr, 2)).await;
    assert_eq!(held.expect("answered after the hold"), splitting);
    let waited = parked_at.elapsed();
    assert!(
        waited >= Duration::from_secs(2),
        "held for the hold: {waited:?}"
    );
    let sent_at = Instant::now();
    let later = tokio::time::timeout(Duration::from_secs(1), append_answer(addr, 3)).await;
    assert_eq!(later.expect("answered at once"), splitting);
    assert!(sent_at.elapsed() < Duration::from_secs(1));
    barrier.resume();
    assert_eq!(
        append_answer(addr, 4).await.0,
        204,
        "served after the release"
    );
    assert_eq!(records(addr).await, serde_json::json!([{"n": 1}, {"n": 4}]));
    engine_shutdown(&state).await;
}

/// **A retired parent's held append is answered by its new owner.** The barrier parks; an
/// append waits; the split publishes (here: the ring gives the shard to inst-b) and retires the
/// parent with a reroute that owns no child. The reroute is asked once, for the held append's
/// route, and the append is answered `409 not_ring_owner` naming inst-b, as is a later one.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_retired_parents_held_append_is_answered_by_its_new_owner() {
    let (state, addr) = http_rig_named(mem(), "inst-a").await;
    raw_put(addr, NAME, &[JSON], br#"[{"n":1}]"#).await;
    let desc = stored(&state, &state.deployment.raw_adapter_sref(NAME)).await;
    let engine = engine_of(&state, &desc, 0).await;
    let barrier = tokio::time::timeout(BOUND, engine.barrier())
        .await
        .expect("parked")
        .expect("the barrier parks");
    let asked = Arc::new(std::sync::Mutex::new(0_usize));
    let reroute: crate::topology::gate::Reroute = {
        let asked = asked.clone();
        Arc::new(move |_route: &[u8; 16]| {
            *asked.lock().unwrap() += 1;
            None
        })
    };
    let base = engine.appends_enqueued();
    let control = async {
        until("the held append is enqueued", || {
            engine.appends_enqueued() > base
        })
        .await;
        let members = ["inst-a", "inst-b"].map(str::to_string).to_vec();
        state.ownership.set_ring_active(members);
        state.ownership.set_override("", "inst-b");
        barrier.retire(reroute);
    };
    let (held, ()) = futures_util::future::join(replayed(addr, 2), control).await;
    let moved = (
        409,
        Some("inst-b".to_string()),
        Some("not_ring_owner".to_string()),
    );
    assert_eq!(held, moved, "the held append");
    assert_eq!(*asked.lock().unwrap(), 1, "the reroute was asked for it");
    assert_eq!(replayed(addr, 3).await, moved, "a later append");
    engine_shutdown(&state).await;
}

/// An append of `{"n": n}`: its status, `Streams-Replay-To` and error code.
async fn replayed(addr: Addr, n: u32) -> (u16, Option<String>, Option<String>) {
    let body = format!(r#"[{{"n":{n}}}]"#);
    let path = format!("/v1/stream/{NAME}");
    let (status, headers, body) = hreq(addr, "POST", &path, &[JSON], body.as_bytes()).await;
    let error: serde_json::Value = serde_json::from_slice(&body).unwrap_or_default();
    let code = error["error"]["code"].as_str().map(str::to_string);
    (status, headers.get("streams-replay-to").cloned(), code)
}
