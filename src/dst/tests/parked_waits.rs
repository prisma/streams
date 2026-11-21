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
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
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

/// An idle consumer's pull answers empty at its `waitMs`: its wait ends at
/// the deadline rather than walking the lineage again without end. Bounded
/// at three seconds, so a wait that always walks again fails here by name
/// instead of holding the pull open.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn an_idle_pull_answers_empty_at_its_wait_deadline() {
    let (state, addr) = http_rig(mem()).await;
    queue(addr, "deadline", b"{}").await;
    let waiting = pull(addr, "deadline", r#"{"waitMs":300}"#);
    let Ok((got, tokens, took)) = tokio::time::timeout(Duration::from_secs(3), waiting).await
    else {
        panic!("an idle pull with waitMs 300 did not answer within 3 s");
    };
    assert_eq!((got.len(), tokens.len()), (0, 0), "nothing to deliver");
    assert!(
        took >= Duration::from_millis(300),
        "answered before its waitMs: {took:?}"
    );
    engine_shutdown(&state).await;
}

/// A record appended while a pull waits reaches it at once, not at the
/// pull's deadline.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
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
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
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
/// consumer: the settle moves the consumer's queue state. The first pull
/// takes `max` 1, so a/1 stays behind a/0's lease (a larger `max` leases
/// the run a/0, a/1, edge change #123).
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn an_ack_that_unblocks_a_key_wakes_a_waiting_pull() {
    let (state, addr) = http_rig(mem()).await;
    queue(addr, "fifo", br#"{"visibilityTimeoutMs":60000}"#).await;
    append(addr, "fifo", "a", 0).await;
    append(addr, "fifo", "a", 1).await;
    let (got, tokens, _) = pull(addr, "fifo", r#"{"max":1}"#).await;
    assert_eq!(
        got,
        vec![("a".to_string(), 0, 1)],
        "the first pull leases a/0 alone"
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
    assert!(
        took >= Duration::from_millis(250) && took < Duration::from_secs(3),
        "a/1 waits for a/0's ack: delivered after {took:?}"
    );
    engine_shutdown(&state).await;
}

/// A record committed after a pull's first walk read the stream, and
/// before that walk's receive, reaches the pull at once: the first wait
/// always walks again.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
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
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
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
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
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
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
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
    // A fleet instance opens nothing before a view (edge change #263): this instance alone, as a
    // tick that has read only its own beat would publish, opens it; the fleet's tick replaces it.
    rig.state.ownership.set_view(
        vec!["streams-1".to_string()],
        std::collections::HashMap::new(),
    );
    assert!(matches!(
        rig.state
            .shards
            .open_or_wait("", Duration::from_secs(5))
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

/// Two waves of eight appends to `name` at an in-flight cap of 8: the first while `waiting`, the
/// second once the first is in flight (and `waiting` holds), then `release`, a moment after the
/// second is in flight or shed. The appends acked and the writes shed meanwhile.
async fn two_waves(
    state: &State,
    addr: SocketAddr,
    name: &str,
    waiting: impl Fn() -> bool,
    release: impl FnOnce(),
) -> (usize, u64) {
    state.admission.set_max_inflight(8);
    let shed = || state.admission.snapshot().shed.inflight;
    let before = shed();
    let path = format!("/v1/streams/{name}/records");
    let send = |n: u64| {
        let path = path.clone();
        async move {
            let body = format!(r#"{{"n":{n}}}"#);
            call(addr, "POST", &path, body.as_bytes()).await.0
        }
    };
    let rest = async {
        settled(|| state.admission.snapshot().inflight == 8 && waiting()).await;
        assert!(waiting(), "the first wave waits");
        tokio::time::sleep(Duration::from_millis(200)).await;
        let released = async {
            settled(|| state.admission.snapshot().inflight == 16 || shed() > before).await;
            tokio::time::sleep(Duration::from_millis(200)).await;
            release();
        };
        let second = futures_util::future::join_all((9..=16).map(send));
        futures_util::future::join(second, released).await.0
    };
    let first = futures_util::future::join_all((1..=8).map(send));
    let (mut answers, second) = futures_util::future::join(first, rest).await;
    answers.extend(second);
    let acked = answers.iter().filter(|status| **status == 200).count();
    (acked, shed() - before)
}

/// A JSON stream `name` holding one record, and the prefix of the rig's one shard, open.
async fn one_record(state: &State, addr: SocketAddr, name: &str) -> String {
    let stream = format!("/v1/streams/{name}");
    let made = call(addr, "PUT", &stream, br#"{"format":{"kind":"json"}}"#).await;
    assert_eq!(made.0, 201);
    let path = format!("{stream}/records");
    assert_eq!(call(addr, "POST", &path, br#"{"n":0}"#).await.0, 200);
    state.shards.held_prefixes().remove(0)
}

/// **Writes waiting on a shard's open leave the write gate, up to the cap** (§22 rev 2, K1-K2c,
/// GATE-DECISION-s22-addendum-rev2-2026-11-20). The rig's one shard is closed and its reopen held
/// at its first manifest read, as a target's open builds while a cell's writes arrive. At an
/// in-flight cap of 8, eight appends reach the open and wait; eight more are then sent. Each of the
/// sixteen is admitted, none is shed, and every one is acked once the open lands.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn writes_waiting_on_an_open_leave_the_write_gate_up_to_the_cap() {
    use crate::dst::{FaultProfile, FaultStore, ObjClass, StoreOp};
    let store = FaultStore::new(mem(), 2611, FaultProfile::clean());
    let (state, addr) = http_rig(store.clone()).await;
    let prefix = one_record(&state, addr, "moving").await;
    let reason = crate::shard_directory::RetirementReason::PlannedMove;
    if let crate::shard_directory::RetireOutcome::Retired(engine) =
        state.shards.retire(&prefix, reason, |_, _| true)
    {
        engine
            .shutdown_handle()
            .wait(Duration::from_secs(5))
            .await
            .unwrap();
    }
    state.shards.clear_holdoff(&prefix);
    let engaged = store.hold_class(StoreOp::Get, ObjClass::Manifest, 1);
    let held = || engaged.load(std::sync::atomic::Ordering::SeqCst) >= 1;
    let counted = two_waves(&state, addr, "moving", held, || store.release_hold()).await;
    assert_eq!(counted, (16, 0), "(appends acked, writes shed)");
    engine_shutdown(&state).await;
}

/// **Writes served by an open shard still count in the write gate** (§22 rev 2, K1-K2c; the control
/// of the two DSTs around it). The rig's one shard stays open and its first WAL write is held, so
/// its appends stay in flight on an open shard of this instance: no move holds it and no open is
/// waited on, so none takes a waiting place. At an in-flight cap of 8 the first eight are admitted
/// and the second eight shed; the first eight are acked once the write lands.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn writes_served_by_an_open_shard_still_count_in_the_write_gate() {
    use crate::dst::{FaultProfile, FaultStore, ObjClass, StoreOp};
    let store = FaultStore::new(mem(), 2612, FaultProfile::clean());
    let (state, addr) = http_rig(store.clone()).await;
    let prefix = one_record(&state, addr, "served").await;
    assert!(state.shards.is_open(&prefix), "the shard stays open");
    let engaged = store.hold_class(StoreOp::Put, ObjClass::Wal, 1);
    let held = || engaged.load(std::sync::atomic::Ordering::SeqCst) >= 1;
    let counted = two_waves(&state, addr, "served", held, || store.release_hold()).await;
    // No write took a waiting place: one that had would have left the gate's count, and more than
    // eight would have been admitted (edge change #272: the gate judges `write_slots`' count).
    assert_eq!(counted, (8, 8), "(appends acked, writes shed)");
    engine_shutdown(&state).await;
}

/// **Writes held by a planned move leave the write gate, up to the cap** (§22 rev 2, K1-K2c). A move
/// holds the rig's one shard (the directory's handoff mark, its resident closed), as a giver holds
/// a moving leaf while its target builds. At an in-flight cap of 8, eight appends wait on the hold
/// and eight more are then sent. When the hold ends, the shard reopens here: each of the sixteen
/// is admitted, none is shed, and every one is acked.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn writes_held_by_a_move_leave_the_write_gate_up_to_the_cap() {
    let (state, addr) = http_rig(mem()).await;
    let prefix = one_record(&state, addr, "held").await;
    let until = tokio::time::Instant::now() + Duration::from_secs(9);
    let (mark, resident) = state.shards.begin_handoff(&prefix, until).unwrap();
    if let Some(engine) = resident {
        engine.begin_close();
        engine
            .shutdown_handle()
            .wait(Duration::from_secs(5))
            .await
            .unwrap();
    }
    let mark = std::sync::Mutex::new(Some(mark));
    let release = || drop(mark.lock().unwrap().take());
    let counted = two_waves(&state, addr, "held", || true, release).await;
    assert_eq!(counted, (16, 0), "(appends acked, writes shed)");
    engine_shutdown(&state).await;
}

/// **Writes parked at a split's barrier leave the write gate, up to the cap** (Addendum 25 of
/// GATE-DECISION-meas-full-forward-wedge-2026-11-19: split `1`'s 7 sheds on an idle store, a
/// 771 ms pause). The rig's one leaf splits with its pause held at the barrier's park, as a
/// split's pause holds every write to its parent. At an in-flight cap of 8, eight appends park at
/// the barrier and eight more are then sent; the split is then let go and publishes. Each of the
/// sixteen is admitted, none is shed, and every one is acked by the children. Red before the fix:
/// a write parked at the barrier holds no waiting place, so the second eight are shed.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn writes_parked_at_a_splits_barrier_leave_the_write_gate_up_to_the_cap() {
    let rig = http_rig_build(
        mem(),
        RigRuntime::first(),
        HttpRigOptions {
            prefixes: vec![String::new()],
            ..Default::default()
        },
    )
    .await;
    super::topology_policy::store_topology(&rig).await;
    let prefix = one_record(&rig.state, rig.addr, "splitting").await;
    let parent = rig.state.shards.open(&prefix).expect("the leaf is open");
    let (reached, reached_rx) = tokio::sync::oneshot::channel();
    let (go_tx, go) = tokio::sync::oneshot::channel();
    let point = crate::topology::gate::PausePoint { reached, go };
    parent.gc_state().gate.pause_at_park(point);
    let clock = rig.state.runtime.clock.clone();
    let split =
        crate::topology::split::split(&rig.state.shards, &parent, "op-cap", "inst-a", &*clock);
    let go_tx = std::sync::Mutex::new(Some(go_tx));
    let release = || {
        let go = go_tx.lock().unwrap().take().expect("released once");
        assert!(go.send(()).is_ok(), "the split waits for go");
    };
    let waves = async {
        reached_rx.await.expect("the split parks");
        two_waves(&rig.state, rig.addr, "splitting", || true, release).await
    };
    let (published, counted) = futures_util::future::join(split, waves).await;
    let refused = published.err().map(|refused| refused.to_string());
    assert_eq!(
        (refused, counted),
        (None, (16, 0)),
        "(the split's refusal, (appends acked, writes shed))"
    );
    let report = rig.tasks.shutdown(Duration::from_secs(3)).await;
    assert!(report.aborted.is_empty(), "{report:?}");
    engine_shutdown(&rig.state).await;
}

/// Once sixteen are in flight, `last` sent and answered; then the paused split let go (`go`).
async fn past_the_line(
    state: &State,
    last: impl std::future::Future<Output = u16>,
    go: tokio::sync::oneshot::Sender<()>,
) -> u16 {
    settled(|| state.admission.snapshot().inflight == 16).await;
    let answered = last.await;
    assert!(go.send(()).is_ok(), "the split waits for go");
    answered
}

/// **Writes holding a place at a split's barrier are netted out at the line, exactly** (package 4's
/// over-shed cross-check, GATE-DECISION-meas-full-forward-wedge-2026-11-19 Addendum 27 (b)). At an
/// in-flight cap of 8, eight appends park at the barrier and take the eight waiting places; eight
/// more park with the places full, so they count, and the write gate is at its line; one more is
/// then shed, and only it. The split is let go and the sixteen are acked by the children. A gate
/// that counted a place-holder against itself sheds the second eight too; one that did not cap the
/// places admits the seventeenth.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn writes_holding_a_place_at_a_splits_barrier_are_netted_out_at_the_line_exactly() {
    let rig = http_rig_build(
        mem(),
        RigRuntime::first(),
        HttpRigOptions {
            prefixes: vec![String::new()],
            ..Default::default()
        },
    )
    .await;
    super::topology_policy::store_topology(&rig).await;
    let prefix = one_record(&rig.state, rig.addr, "at-the-line").await;
    let parent = rig.state.shards.open(&prefix).expect("the leaf is open");
    let (reached, reached_rx) = tokio::sync::oneshot::channel();
    let (go_tx, go) = tokio::sync::oneshot::channel();
    parent
        .gc_state()
        .gate
        .pause_at_park(crate::topology::gate::PausePoint { reached, go });
    let clock = rig.state.runtime.clock.clone();
    let split =
        crate::topology::split::split(&rig.state.shards, &parent, "op-line", "inst-a", &*clock);
    let (state, addr) = (&rig.state, rig.addr);
    let send = |n: u64| async move {
        let (path, body) = ("/v1/streams/at-the-line/records", format!(r#"{{"n":{n}}}"#));
        call(addr, "POST", path, body.as_bytes()).await.0
    };
    let waves = async {
        reached_rx.await.expect("the split parks");
        state.admission.set_max_inflight(8);
        let shed = || state.admission.snapshot().shed.inflight;
        let before = shed();
        let first = futures_util::future::join_all((1..=8).map(send));
        let rest = async {
            settled(|| state.admission.snapshot().inflight == 8).await;
            let second = futures_util::future::join_all((9..=16).map(send));
            futures_util::future::join(second, past_the_line(state, send(17), go_tx)).await
        };
        let (first, (second, last)) = futures_util::future::join(first, rest).await;
        let acked = first
            .iter()
            .chain(&second)
            .filter(|status| **status == 200)
            .count();
        (acked, last == 200, shed() - before)
    };
    let (published, counted) = futures_util::future::join(split, waves).await;
    let refused = published.err().map(|refused| refused.to_string());
    assert_eq!(
        (refused, counted),
        (None, (16, false, 1)),
        "(the split's refusal, (appends acked, the seventeenth acked, writes shed))"
    );
    let report = rig.tasks.shutdown(Duration::from_secs(3)).await;
    assert!(report.aborted.is_empty(), "{report:?}");
    engine_shutdown(&rig.state).await;
}

/// A raw stream created at `addr` whose route lands in `leaf`: its path.
async fn raw_stream_in(state: &State, addr: SocketAddr, leaf: &str) -> String {
    use super::billing_closure_debts::{JSON, raw_put, stored};
    for n in 0..200 {
        let name = format!("barrier-{leaf}-{n}");
        raw_put(addr, &name, &[JSON], b"").await;
        let desc = stored(state, &state.deployment.raw_adapter_sref(&name)).await;
        if state
            .shards
            .prefix_for(&desc.resolve_segment("").shard_route)
            == leaf
        {
            return format!("/v1/stream/{name}");
        }
    }
    panic!("no stream lands in {leaf}");
}

/// Record `n` appended to the raw stream `path`: the answer's status.
async fn raw_append(addr: SocketAddr, path: &str, n: u64) -> u16 {
    let json = [super::billing_closure_debts::JSON];
    let body = format!(r#"[{{"n":{n}}}]"#);
    super::fixture_requests::hreq(addr, "POST", path, &json, body.as_bytes())
        .await
        .0
}

/// Waves of eight appends to `path`, one every 50 ms for `span`: every answer's status.
async fn steady(addr: SocketAddr, path: &str, span: Duration) -> Vec<u16> {
    let t0 = Instant::now();
    let mut statuses = Vec::new();
    while t0.elapsed() < span {
        let wave = (0..8).map(|n| raw_append(addr, path, n));
        statuses.extend(futures_util::future::join_all(wave).await);
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    statuses
}

/// A raw stream name, `tag`-`n`, whose route lands in `leaf` (computed, not created).
fn name_in(state: &State, leaf: &str, tag: &str) -> String {
    (0..1_000)
        .map(|n| format!("{tag}-{n}"))
        .find(|name| {
            let sref = state.deployment.raw_adapter_sref(name);
            state
                .shards
                .prefix_for(&crate::crypto::RouteHash::for_stream(&sref).0)
                == leaf
        })
        .unwrap_or_else(|| panic!("no {tag} name lands in {leaf}"))
}

/// What a request parked at the split's barrier gets: (requests answered as asked, other leaves'
/// appends not acked during the pause, and after the release, writes shed), and the split's
/// refusal, if it was refused.
type ThroughAPause = (Option<String>, (usize, usize, usize, u64));

/// A rig over the leaves `0` and `1` with `prepare` run on it; `1`'s split held at the barrier's
/// park, and at an in-flight cap of 8 eight requests `parked(n)` sent to `1` (each answering whether
/// it was answered as asked); `0`'s waves of eight appends run for 300 ms of the pause and a second
/// after it ends; then `then` reads before the rig stops.
async fn through_a_pause<P, F, X, T>(
    tag: &str,
    prepare: P,
    parked: F,
    then: X,
) -> (ThroughAPause, T)
where
    P: AsyncFnOnce(&State, SocketAddr),
    F: AsyncFn(&State, SocketAddr, u64) -> bool,
    X: AsyncFnOnce(&State, SocketAddr) -> T,
{
    let rig = http_rig_build(
        mem(),
        RigRuntime::first(),
        HttpRigOptions {
            prefixes: vec!["0".into(), "1".into()],
            ..Default::default()
        },
    )
    .await;
    super::topology_policy::store_topology(&rig).await;
    let (state, addr) = (&rig.state, rig.addr);
    let other = raw_stream_in(state, addr, "0").await;
    assert_eq!(raw_append(addr, &other, 0).await, 204, "{other}");
    prepare(state, addr).await;
    state.admission.set_max_inflight(8);
    let shed = || state.admission.snapshot().shed.inflight;
    let before = shed();
    let parent = state.shards.open("1").expect("the leaf is open");
    let (reached, reached_rx) = tokio::sync::oneshot::channel();
    let (go_tx, go) = tokio::sync::oneshot::channel();
    let point = crate::topology::gate::PausePoint { reached, go };
    parent.gc_state().gate.pause_at_park(point);
    let clock = state.runtime.clock.clone();
    let split = crate::topology::split::split(&state.shards, &parent, tag, "inst-a", &*clock);
    let control = async {
        reached_rx.await.expect("the split parks");
        let held = futures_util::future::join_all((1..=8).map(|n| parked(state, addr, n)));
        let around = async {
            settled(|| state.admission.snapshot().inflight == 8).await;
            let during = steady(addr, &other, Duration::from_millis(300)).await;
            assert!(go_tx.send(()).is_ok(), "the split waits for go");
            let after = steady(addr, &other, Duration::from_secs(1)).await;
            (during, after)
        };
        futures_util::future::join(held, around).await
    };
    let (published, (held, (during, after))) = futures_util::future::join(split, control).await;
    let not_acked = |statuses: &[u16]| statuses.iter().filter(|status| **status != 204).count();
    let answered = held.iter().filter(|answered| **answered).count();
    let counted = (
        answered,
        not_acked(&during),
        not_acked(&after),
        shed() - before,
    );
    let read = then(state, addr).await;
    let report = rig.tasks.shutdown(Duration::from_secs(3)).await;
    assert!(report.aborted.is_empty(), "{report:?}");
    engine_shutdown(&rig.state).await;
    (
        (published.err().map(|refused| refused.to_string()), counted),
        read,
    )
}

/// **Other leaves' writes are not shed through a split's pause and its release** (Addendum 25 of
/// GATE-DECISION-meas-full-forward-wedge-2026-11-19, condition 2): eight appends to `1` parked at
/// its split's barrier, and `0`'s waves (`through_a_pause`). Every parked append is acked by the
/// children, every append to `0` is answered `204`, and none is shed. Red before the fix: the eight
/// parked appends hold the cap, so `0`'s waves are shed.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn other_leaves_writes_are_not_shed_through_a_splits_pause_and_release() {
    let held = std::sync::OnceLock::new();
    let prepare = async |state: &State, addr: SocketAddr| {
        let path = raw_stream_in(state, addr, "1").await;
        assert_eq!(raw_append(addr, &path, 0).await, 204, "{path}");
        held.set(path).unwrap();
    };
    let parked = async |_: &State, addr: SocketAddr, n: u64| {
        raw_append(addr, held.get().unwrap(), n).await == 204
    };
    let (counted, ()) =
        through_a_pause("op-appends", prepare, parked, async |_: &State, _| {}).await;
    assert_eq!(
        counted,
        (None, (8, 0, 0, 0)),
        "(refused, (parked acked, during, after, shed))"
    );
}

/// **A queue's sends parked at a split's barrier leave the write gate** (Addendum 25): eight sends
/// parked at `1`'s barrier beside `0`'s waves; every append to `0` is `204`, none is shed (red
/// before the fix). A split reroutes only appends, so each send is `201` stored exactly once or
/// `503 queue_unavailable` stored never (Addendum 28 pins today's; the reroute makes every one 201).
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_queues_sends_parked_at_a_splits_barrier_leave_the_write_gate() {
    let queue = std::sync::OnceLock::new();
    let prepare = async |state: &State, addr: SocketAddr| {
        let name = (0..1_000)
            .map(|n| format!("barrier-q-{n}"))
            .find(|name| {
                let sref = crate::tenant::ProjectId::new("proj-test")
                    .unwrap()
                    .stream_ref(name);
                let route = crate::crypto::RouteHash::for_stream(&sref).0;
                state.shards.prefix_for(&route) == "1"
            })
            .expect("a queue name lands in 1");
        let key = [("streams-encryption-key", PRISMA_KEY)];
        let made = preq(addr, "PUT", &format!("/v1/queues/{name}"), &key, b"").await;
        assert_eq!(made.0, 201, "create the queue {name}");
        queue.set(name).unwrap();
    };
    let answers = std::sync::Mutex::new(Vec::new());
    let parked = async |_: &State, addr: SocketAddr, n: u64| {
        let key = [("streams-encryption-key", PRISMA_KEY)];
        let path = format!("/v1/queues/{}/messages", queue.get().unwrap());
        let body = format!(r#"{{"body":{{"n":{n}}}}}"#);
        let sent = preq(addr, "POST", &path, &key, body.as_bytes());
        let answered = tokio::time::timeout(Duration::from_secs(20), sent).await;
        let (status, code) = answered.map_or((0, String::new()), |(status, _, raw)| {
            let answer: serde_json::Value = serde_json::from_slice(&raw).unwrap_or_default();
            let code = answer["error"]["code"].as_str().or(answer["code"].as_str());
            (status, code.unwrap_or_default().to_string())
        });
        answers.lock().unwrap().push((n, status, code));
        status != 0
    };
    let stored = async |_: &State, addr: SocketAddr| stored_ns(addr, queue.get().unwrap()).await;
    let (counted, stored) = through_a_pause("op-queue", prepare, parked, stored).await;
    let answers = std::mem::take(&mut *answers.lock().unwrap());
    eprintln!("QUEUE_SENDS_AT_A_SPLIT answers={answers:?} stored={stored:?}");
    for (n, status, code) in &answers {
        let copies = stored.iter().filter(|held| *held == n).count();
        let exact = match status {
            201 => copies == 1,
            503 => code == "queue_unavailable" && copies == 0,
            _ => false,
        };
        assert!(exact, "send {n}: {status} {code}, stored {copies} times");
    }
    let served = answers
        .iter()
        .filter(|(_, status, _)| *status == 201)
        .count();
    assert_eq!(
        (counted, stored.len()),
        ((None, (8, 0, 0, 0)), served),
        "((refused, (sends answered, during, after, shed)), messages stored)"
    );
}

/// The `n` of every message stored in the queue `name`, leased in claims of up to 64.
async fn stored_ns(addr: SocketAddr, name: &str) -> Vec<u64> {
    let key = [("streams-encryption-key", PRISMA_KEY)];
    let path = format!("/v1/queues/{name}/groups/default:claim");
    let mut ns = Vec::new();
    loop {
        let claim = br#"{"max":64,"leaseMs":60000,"waitMs":0}"#;
        let (status, _, raw) = preq(addr, "POST", &path, &key, claim).await;
        assert_eq!(status, 200, "{}", String::from_utf8_lossy(&raw));
        let answer: serde_json::Value = serde_json::from_slice(&raw).unwrap_or_default();
        let messages = answer["messages"].as_array().cloned().unwrap_or_default();
        if messages.is_empty() {
            return ns;
        }
        ns.extend(messages.iter().filter_map(|m| m["body"]["n"].as_u64()));
    }
}

/// **Creates with initial content parked at a split's barrier leave the write gate** (Addendum 25,
/// creation's path): eight raw streams whose routes land in `1`, each created with one record while
/// its split's barrier holds `1`, beside `0`'s waves. Every create is answered `201`, every append
/// to `0` `204`, and none is shed. Red before the fix.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn creates_with_content_parked_at_a_splits_barrier_leave_the_write_gate() {
    let prepare = async |_: &State, _: SocketAddr| {};
    let parked = async |state: &State, addr: SocketAddr, n: u64| {
        let name = name_in(state, "1", &format!("barrier-create-{n}"));
        let json = [super::billing_closure_debts::JSON];
        let path = format!("/v1/stream/{name}");
        let body = format!(r#"[{{"n":{n}}}]"#);
        super::fixture_requests::hreq(addr, "PUT", &path, &json, body.as_bytes())
            .await
            .0
            == 201
    };
    let (counted, ()) =
        through_a_pause("op-creates", prepare, parked, async |_: &State, _| {}).await;
    assert_eq!(
        counted,
        (None, (8, 0, 0, 0)),
        "(refused, (creates answered, during, after, shed))"
    );
}
