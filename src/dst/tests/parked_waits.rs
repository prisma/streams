//! Parked waits (shared-cells PLAN step 5, finding M1 and the cost
//! review's 2.3-2.4). A request waiting for data is parked, not in
//! flight: a read long-poll, a consumer pull and a watch wait leave the
//! write gate, the survival bound and the fleet heartbeat; they park
//! within the live-connection pool and their project's share of it; and a
//! waiting pull walks its lineage again only when something can have made
//! a message deliverable, never every 50 ms.

use super::fixture_cell::{Cell, CellSpec, open_cell};
use super::fixture_http::{HttpRigOptions, engine_shutdown, http_rig, http_rig_build};
use super::fixture_requests::{PRISMA_KEY, preq};
use super::fixture_runtime::RigRuntime;
use super::fixture_storage::mem;
use crate::auth::ceiling::SharedBounds;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

type State = Arc<crate::http::AppState>;

async fn call(addr: SocketAddr, method: &str, path: &str, body: &[u8]) -> (u16, Vec<u8>) {
    let (st, _, b) = preq(
        addr,
        method,
        path,
        &[("prisma-encryption-key", PRISMA_KEY)],
        body,
    )
    .await;
    (st, b)
}

/// A JSON stream `name` with consumer `g` (its config `consumer`).
async fn queue(addr: SocketAddr, name: &str, consumer: &[u8]) {
    let (st, _) = call(
        addr,
        "PUT",
        &format!("/v1/streams/{name}"),
        br#"{"format":{"kind":"json"}}"#,
    )
    .await;
    assert_eq!(st, 201);
    let (st, _) = call(
        addr,
        "PUT",
        &format!("/v1/streams/{name}/consumers/g"),
        consumer,
    )
    .await;
    assert_eq!(st, 201);
}

async fn append(addr: SocketAddr, name: &str, key: &str, n: u64) {
    let (st, _, b) = preq(
        addr,
        "POST",
        &format!("/v1/streams/{name}/records"),
        &[
            ("prisma-encryption-key", PRISMA_KEY),
            ("prisma-routing-key", key),
        ],
        format!(r#"{{"k":"{key}","n":{n}}}"#).as_bytes(),
    )
    .await;
    assert_eq!(st, 200, "{}", String::from_utf8_lossy(&b));
}

/// One pull of consumer `g`: its messages as (routing key, n, attempts),
/// the lease tokens, and how long it took.
async fn pull(
    addr: SocketAddr,
    name: &str,
    body: &str,
) -> (Vec<(String, i64, i64)>, Vec<String>, Duration) {
    let started = Instant::now();
    let (st, b) = call(
        addr,
        "POST",
        &format!("/v1/streams/{name}/consumers/g:pull"),
        body.as_bytes(),
    )
    .await;
    assert_eq!(st, 200, "{}", String::from_utf8_lossy(&b));
    let v: serde_json::Value = serde_json::from_slice(&b).unwrap();
    let messages = v["messages"].as_array().unwrap();
    let got = messages
        .iter()
        .map(|m| {
            let key = m["routingKey"].as_str().unwrap().to_string();
            (
                key,
                m["value"]["n"].as_i64().unwrap(),
                m["attempts"].as_i64().unwrap(),
            )
        })
        .collect();
    let tokens = messages
        .iter()
        .map(|m| m["leaseToken"].as_str().unwrap().to_string())
        .collect();
    (got, tokens, started.elapsed())
}

/// Commands this runtime's shard engines have queued for their committers:
/// appends and queue operations (every walk of a pull is one receive).
fn commits(state: &State) -> u64 {
    state
        .shards
        .engines_by_prefix()
        .iter()
        .map(|(_, engine)| engine.appends_enqueued())
        .sum()
}

/// Wait until `done` holds (at most 10 s).
async fn settled(done: impl Fn() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(10);
    while !done() && Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

/// The commits of one empty pull with `wait_ms` on the idle stream `name`.
async fn idle_pull_commits(state: &State, addr: SocketAddr, name: &str, wait_ms: u64) -> u64 {
    let before = commits(state);
    let (got, _, took) = pull(addr, name, &format!(r#"{{"waitMs":{wait_ms}}}"#)).await;
    assert_eq!(
        (got.len(), took >= Duration::from_millis(wait_ms)),
        (0, true)
    );
    commits(state) - before
}

/// An idle consumer's pull that waits a second walks its lineage exactly
/// twice more than one that does not wait: the first wait's re-walk and
/// the walk at its deadline. Four such pulls at once on one consumer walk
/// exactly as often each: a walk that leases nothing wakes no other pull.
/// Before: a walk every 50 ms, about 20 more per second waited.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_idle_pull_walks_twice_more_while_it_waits_and_wakes_no_other_pull() {
    let (state, addr) = http_rig(mem()).await;
    queue(addr, "idle", b"{}").await;
    let at_once = idle_pull_commits(&state, addr, "idle", 0).await;
    let waiting = idle_pull_commits(&state, addr, "idle", 1_000).await;
    assert_eq!(
        waiting - at_once,
        2,
        "walks while waiting a second ({at_once} at once)"
    );
    let before = commits(&state);
    let pulls = (0..4).map(|_| pull(addr, "idle", r#"{"waitMs":1000}"#));
    for (got, _, _) in futures_util::future::join_all(pulls).await {
        assert!(got.is_empty());
    }
    assert_eq!(
        commits(&state) - before,
        4 * waiting,
        "four idle pulls at once"
    );
    engine_shutdown(&state).await;
}

/// A record appended while a pull waits reaches it at once, not at the
/// pull's deadline.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_append_reaches_a_waiting_pull_at_once() {
    let (state, addr) = http_rig(mem()).await;
    queue(addr, "wake", b"{}").await;
    let waiting = pull(addr, "wake", r#"{"waitMs":10000}"#);
    let appending = async {
        tokio::time::sleep(Duration::from_millis(300)).await;
        append(addr, "wake", "a", 7).await;
    };
    let ((got, _, took), ()) = futures_util::future::join(waiting, appending).await;
    assert_eq!(got, vec![("a".to_string(), 7, 1)]);
    assert!(took < Duration::from_secs(3), "delivered after {took:?}");
    engine_shutdown(&state).await;
}

/// A lease that expires while another pull of the consumer waits reaches
/// that pull at the lease's deadline: an expiry commits nothing, so only
/// the deadline the waiting pull reads from the consumer's leases wakes it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_expired_lease_reaches_a_waiting_pull_at_its_deadline() {
    let (state, addr) = http_rig(mem()).await;
    queue(addr, "expiry", br#"{"visibilityTimeoutMs":1000}"#).await;
    append(addr, "expiry", "a", 1).await;
    let (got, _, _) = pull(addr, "expiry", "{}").await;
    assert_eq!(got, vec![("a".to_string(), 1, 1)], "the first lease");
    let (got, _, took) = pull(addr, "expiry", r#"{"waitMs":10000}"#).await;
    assert_eq!(got, vec![("a".to_string(), 1, 2)], "redelivered on expiry");
    assert!(took < Duration::from_secs(4), "redelivered after {took:?}");
    engine_shutdown(&state).await;
}

/// An ack that unblocks a key's next record wakes a pull waiting on the
/// consumer: the settle moves the consumer's queue state.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_ack_that_unblocks_a_key_wakes_a_waiting_pull() {
    let (state, addr) = http_rig(mem()).await;
    queue(addr, "fifo", br#"{"visibilityTimeoutMs":60000}"#).await;
    append(addr, "fifo", "a", 0).await;
    append(addr, "fifo", "a", 1).await;
    let (got, tokens, _) = pull(addr, "fifo", r#"{"max":10}"#).await;
    assert_eq!(
        got,
        vec![("a".to_string(), 0, 1)],
        "a/1 is blocked behind a/0"
    );
    let waiting = pull(addr, "fifo", r#"{"waitMs":10000}"#);
    let acking = async {
        tokio::time::sleep(Duration::from_millis(300)).await;
        let ack = format!(r#"{{"acks":[{{"leaseToken":"{}"}}]}}"#, tokens[0]);
        call(
            addr,
            "POST",
            "/v1/streams/fifo/consumers/g:settle",
            ack.as_bytes(),
        )
        .await
        .0
    };
    let ((got, _, took), acked) = futures_util::future::join(waiting, acking).await;
    assert_eq!(acked, 200);
    assert_eq!(got, vec![("a".to_string(), 1, 1)]);
    assert!(took < Duration::from_secs(3), "delivered after {took:?}");
    engine_shutdown(&state).await;
}

/// A record committed after a pull's first walk read the stream, and
/// before that walk's receive, reaches the pull at once: the first wait
/// always walks again.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_record_committed_during_the_first_walk_reaches_the_pull() {
    use crate::failpoints::{Fp, parked};
    let (state, addr) = http_rig(mem()).await;
    queue(addr, "race-pull", b"{}").await;
    let arrived = parked(Fp::PullBeforeReceive, "race-pull");
    crate::failpoints::park_pull_before_receive("race-pull");
    let waiting = pull(addr, "race-pull", r#"{"waitMs":10000}"#);
    let racing = async {
        settled(|| parked(Fp::PullBeforeReceive, "race-pull") > arrived).await;
        append(addr, "race-pull", "a", 3).await;
        crate::failpoints::release_pull_before_receive("race-pull");
        Instant::now()
    };
    let ((got, _, _), released) = futures_util::future::join(waiting, racing).await;
    assert_eq!(got, vec![("a".to_string(), 3, 1)]);
    let took = released.elapsed();
    assert!(
        took < Duration::from_secs(3),
        "delivered {took:?} after the walk resumed"
    );
    engine_shutdown(&state).await;
}

/// A long-poll, a consumer pull and a watch wait that are waiting are
/// parked: in flight, exactly as parked, and the write gate at an
/// in-flight cap of 2 admits an append beside the three of them.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn waiting_reads_pulls_and_watches_are_parked_and_leave_the_write_gate() {
    let (state, addr) = http_rig(mem()).await;
    let watched = br#"{"format":{"kind":"json"},"watches":[{"name":"w","fields":["/k"]}]}"#;
    assert_eq!(
        call(addr, "PUT", "/v1/streams/parked", watched).await.0,
        201
    );
    assert_eq!(
        call(addr, "PUT", "/v1/streams/parked/consumers/g", b"{}")
            .await
            .0,
        201
    );
    assert_eq!(
        call(
            addr,
            "PUT",
            "/v1/streams/other",
            br#"{"format":{"kind":"json"}}"#
        )
        .await
        .0,
        201
    );
    state.admission.set_max_inflight(2);
    let khex = crate::product::watch_key_hex("w", &["/k".to_string()], &["\"z\"".to_string()]);
    let wait = format!("/v1/streams/parked/watches/w/keys/{khex}?cursor=now&timeoutMs=3000");
    let waits = async {
        futures_util::future::join3(
            call(
                addr,
                "GET",
                "/v1/streams/parked/records:long-poll?cursor=now&waitMs=3000",
                b"",
            ),
            call(
                addr,
                "POST",
                "/v1/streams/parked/consumers/g:pull",
                br#"{"waitMs":3000}"#,
            ),
            call(addr, "GET", &wait, b""),
        )
        .await
    };
    let observe = async {
        settled(|| state.admission.snapshot().inflight == 3).await;
        tokio::time::sleep(Duration::from_millis(300)).await;
        let held = (
            state.admission.snapshot().inflight,
            state.admission.parked(),
        );
        let (st, _) = call(addr, "POST", "/v1/streams/other/records", br#"{"k":"x"}"#).await;
        (held, st)
    };
    let (answers, (held, appended)) = futures_util::future::join(waits, observe).await;
    assert_eq!(
        (held, appended),
        ((3, 3), 200),
        "(in flight, parked), the append"
    );
    assert_eq!((answers.0.0, answers.1.0, answers.2.0), (204, 200, 200));
    assert_eq!(
        (
            state.admission.snapshot().inflight,
            state.admission.parked()
        ),
        (0, 0)
    );
    engine_shutdown(&state).await;
}

/// (in flight, parked) while each of the first `projects` projects of
/// `cell` holds `count` waiting long-polls on its `orders`.
async fn held_long_polls(cell: &Cell, count: usize, projects: usize) -> (i64, i64) {
    let poll = "/v1/streams/orders/records:long-poll?cursor=now&waitMs=1500";
    let polls = (0..projects).flat_map(|i| (0..count).map(move |_| cell.call(i, "GET", poll, b"")));
    let observe = async {
        let inflight = i64::try_from(count * projects).unwrap();
        settled(|| cell.state.admission.snapshot().inflight == inflight).await;
        tokio::time::sleep(Duration::from_millis(200)).await;
        let admission = &cell.state.admission;
        (admission.snapshot().inflight, admission.parked())
    };
    futures_util::future::join(futures_util::future::join_all(polls), observe)
        .await
        .1
}

/// On a cell shared 8 ways over 16 live connections, a project's share is
/// 2: of its 4 waiting long-polls exactly 2 are parked, and the other 2
/// wait as active requests. Two projects at their shares in a pool sized
/// 3 park exactly 3 waits between them.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_projects_waits_park_only_within_its_share_and_the_pool() {
    let bounds = SharedBounds {
        subscriptions: 16,
        ..SharedBounds::default()
    };
    let cell = open_cell(CellSpec {
        bounds,
        ..CellSpec::open(2)
    })
    .await;
    for i in 0..2 {
        let created = cell
            .call(
                i,
                "PUT",
                "/v1/streams/orders",
                br#"{"format":{"kind":"json"}}"#,
            )
            .await;
        assert_eq!(created.0, 201);
    }
    assert_eq!(
        held_long_polls(&cell, 4, 1).await,
        (4, 2),
        "one project over its share"
    );
    cell.state.admission.set_live_pool(3);
    assert_eq!(
        held_long_polls(&cell, 2, 2).await,
        (4, 3),
        "two projects at their shares, pool 3"
    );
    engine_shutdown(&cell.state).await;
}

/// The first heartbeat `streams-1` publishes after `since_ms`.
async fn beat_after(state: &State, since_ms: i64) -> crate::fleet::Heartbeat {
    loop {
        tokio::time::sleep(Duration::from_millis(100)).await;
        let beats = state.fleet.peek_heartbeat_set().await.unwrap_or_default();
        let beat = beats
            .into_iter()
            .find(|b| b.instance == "streams-1" && b.ts_ms > since_ms);
        if let Some(beat) = beat {
            return beat;
        }
    }
}

/// A fleet instance holding 400 parked pulls reports none of them in
/// flight in its heartbeat, so the scaler's edge-slot dimension, in flight
/// over 105 admitted slots, sizes the fleet by the load and not by the
/// waits: 400 waits in flight wanted four servers (cost review 2.3). The
/// desired count itself also follows the process's CPU, which the suite's
/// own load moves, so the beat is what this leg pins.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn parked_pulls_never_reach_the_heartbeat() {
    let fleet = mem();
    let rig = http_rig_build(
        mem(),
        RigRuntime::first(),
        HttpRigOptions {
            fleet_store: Some(fleet.clone()),
            instance: Some("streams-1".into()),
            ..Default::default()
        },
    )
    .await;
    assert!(matches!(
        rig.state
            .shards
            .open_or_wait("00", Duration::from_secs(5))
            .await,
        crate::sharddir::OpenOutcome::Ready(_)
    ));
    assert!(crate::fleet::start_configured(
        rig.state.clone(),
        &rig.tasks
    ));
    let addr = rig.addr;
    queue(addr, "fleet-idle", b"{}").await;
    let state = &rig.state;
    let pulls = (0..400).map(|_| pull(addr, "fleet-idle", r#"{"waitMs":9000}"#));
    let observe = async {
        settled(|| state.admission.snapshot().inflight == 400).await;
        tokio::time::sleep(Duration::from_millis(500)).await;
        let beat = beat_after(state, crate::shard::now_ms()).await;
        (state.admission.parked(), beat.inflight)
    };
    let (answers, held) =
        futures_util::future::join(futures_util::future::join_all(pulls), observe).await;
    assert_eq!(held, (400, 0), "(parked, the heartbeat's in flight)");
    assert!(answers.iter().all(|(got, _, _)| got.is_empty()));
    let report = rig.tasks.shutdown(Duration::from_secs(3)).await;
    assert!(report.aborted.is_empty(), "{report:?}");
    engine_shutdown(&rig.state).await;
}
