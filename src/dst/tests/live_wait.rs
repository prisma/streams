//! The multiplexed wait (layout 6A package 2, `POST
//! /v1/projects/{p}/live:wait`): one long-poll answers whichever of a
//! client's tails, group pulls and watches is ready, with exactly what the
//! per-stream routes would deliver. It answers every subscription from
//! `now` at once with a pinned cursor; wakes on exactly the stream that
//! moved; answers empty at its deadline; answers every ready subscription
//! in request order within one shared byte budget; answers a subscription
//! the token's grants do not cover with the per-stream route's own refusal
//! and serves the rest; parks once however many streams it watches;
//! meters one read operation an answer; walks a drained group never and a
//! woken one within what was appended.

use super::fixture_auth::{auth_rig, rig_bearer};
use super::fixture_http::{engine_shutdown, http_rig};
use super::fixture_requests::{PRISMA_KEY, preq};
use super::fixture_storage::mem;
use crate::project_policy::{CredentialGrant, CredentialStatus, GrantSnapshot};
use crate::tenant::{ProjectId, Scope, ScopeSet, StreamGrant};
use serde_json::{Value, json};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

type State = Arc<crate::http::AppState>;

const KEY: (&str, &str) = ("prisma-encryption-key", PRISMA_KEY);

/// The rig's own project: the deployment tenant the product surface serves
/// outside enforce.
fn project(state: &State) -> String {
    state.deployment.deployment_tenant().as_str().to_string()
}

/// POST a live wait as `extra` (none: the rig's open bearer): its status,
/// JSON body and how long it took.
async fn live_wait(
    addr: SocketAddr,
    project: String,
    extra: &[(&str, &str)],
    body: &Value,
) -> (u16, Value, Duration) {
    let started = Instant::now();
    let (st, _, b) = preq(
        addr,
        "POST",
        &format!("/v1/projects/{project}/live:wait"),
        extra,
        body.to_string().as_bytes(),
    )
    .await;
    let v = serde_json::from_slice(&b).unwrap_or(Value::Null);
    (st, v, started.elapsed())
}

/// The answer's entries by id, in order.
fn ids(answer: &Value) -> Vec<String> {
    answer["results"]
        .as_array()
        .unwrap()
        .iter()
        .map(|e| e["id"].as_str().unwrap().to_string())
        .collect()
}

/// The entry `id` of an answer.
fn entry<'v>(answer: &'v Value, id: &str) -> &'v Value {
    answer["results"]
        .as_array()
        .unwrap()
        .iter()
        .find(|e| e["id"] == id)
        .unwrap_or_else(|| panic!("no entry {id} in {answer}"))
}

async fn create(addr: SocketAddr, name: &str, doc: &[u8]) {
    let (st, _, b) = preq(addr, "PUT", &format!("/v1/streams/{name}"), &[KEY], doc).await;
    assert_eq!(st, 201, "create {name}: {}", String::from_utf8_lossy(&b));
}

async fn append(addr: SocketAddr, name: &str, routing_key: &str, record: &Value) {
    let (st, _, b) = preq(
        addr,
        "POST",
        &format!("/v1/streams/{name}/records"),
        &[KEY, ("prisma-routing-key", routing_key)],
        record.to_string().as_bytes(),
    )
    .await;
    assert_eq!(st, 200, "append {name}: {}", String::from_utf8_lossy(&b));
}

fn tail(id: &str, stream: &str, cursor: &str) -> Value {
    json!({"id": id, "kind": "tail", "stream": stream, "key": PRISMA_KEY, "cursor": cursor})
}

fn group(id: &str, stream: &str) -> Value {
    json!({"id": id, "kind": "group", "stream": stream, "key": PRISMA_KEY, "group": "g"})
}

/// Each tail's cursor, pinned by a wait from `now`.
async fn pinned(state: &State, addr: SocketAddr, streams: &[&str]) -> Vec<String> {
    let subs: Vec<Value> = streams
        .iter()
        .map(|s| tail(&format!("t-{s}"), s, "now"))
        .collect();
    let (st, answer, _) = live_wait(addr, project(state), &[], &json!({"subs": subs})).await;
    assert_eq!(st, 200, "{answer}");
    streams
        .iter()
        .map(|s| {
            let e = entry(&answer, &format!("t-{s}"));
            e["cursor"].as_str().unwrap().to_string()
        })
        .collect()
}

/// Commands this runtime's shard engines have queued for their committers:
/// appends and queue operations (a group walk's configuration read and its
/// Receive each count one).
fn commits(state: &State) -> u64 {
    state
        .shards
        .engines_by_prefix()
        .iter()
        .map(|(_, engine)| engine.appends_enqueued())
        .sum()
}

/// The active billing window's counters for `name`'s incarnation.
async fn counts(state: &State, name: &str) -> crate::billing::RowDelta {
    let sref = state.deployment.raw_adapter_sref(name);
    let desc = state.registry.get(&sref).await.unwrap().unwrap();
    let id = desc.stream_epoch.clone();
    state
        .billing
        .reads()
        .snapshot_active()
        .into_iter()
        .find(|(row, _)| row.stream_id == id)
        .map(|(_, delta)| delta)
        .unwrap_or_default()
}

/// Red at 63139112 (no route): `404 != 200`. Every subscription from
/// `now` is answered at once with a cursor pinned at its stream's tail and
/// no records, so the gap between two requests is never lost.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn subscriptions_from_now_are_answered_at_once_with_pinned_cursors() {
    let (state, addr) = http_rig(mem()).await;
    for s in ["n1", "n2"] {
        create(addr, s, br#"{"format":{"kind":"json"}}"#).await;
    }
    append(addr, "n1", "", &json!({"before": 1})).await;
    let body = json!({"subs": [tail("a", "n1", "now"), tail("b", "n2", "now")]});
    let (st, answer, took) = live_wait(addr, project(&state), &[], &body).await;
    assert_eq!(st, 200, "{answer}");
    assert_eq!(ids(&answer), ["a", "b"]);
    for id in ["a", "b"] {
        let e = entry(&answer, id);
        assert_eq!(
            (&e["records"], &e["upToDate"]),
            (&json!([]), &json!(true)),
            "{e}"
        );
        assert!(!e["cursor"].as_str().unwrap().is_empty());
    }
    assert!(took < Duration::from_secs(2), "answered after {took:?}");
    engine_shutdown(&state).await;
}

/// An append to one of three watched streams answers exactly that stream's
/// subscription with exactly that record and a cursor past it; the other
/// two stay out of the answer with their cursors the client's. Waiting
/// again from every cursor answers empty at the deadline, not earlier.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_append_wakes_exactly_its_tail_and_an_idle_wait_answers_empty_at_its_deadline() {
    let (state, addr) = http_rig(mem()).await;
    let streams = ["w1", "w2", "w3"];
    for s in streams {
        create(addr, s, br#"{"format":{"kind":"json"}}"#).await;
    }
    let cursors = pinned(&state, addr, &streams).await;
    let subs: Vec<Value> = streams
        .iter()
        .zip(&cursors)
        .map(|(s, c)| tail(s, s, c))
        .collect();
    let body = json!({"waitMs": 10_000, "subs": subs});
    let waiting = live_wait(addr, project(&state), &[], &body);
    let appending = async {
        tokio::time::sleep(Duration::from_millis(300)).await;
        append(addr, "w2", "", &json!({"n": 7})).await;
    };
    let ((st, answer, took), ()) = futures_util::future::join(waiting, appending).await;
    assert_eq!(st, 200, "{answer}");
    assert_eq!(ids(&answer), ["w2"]);
    let woken = entry(&answer, "w2");
    assert_eq!(woken["records"], json!([{"n": 7}]));
    assert_eq!(woken["upToDate"], json!(true));
    assert_ne!(woken["cursor"].as_str().unwrap(), cursors[1]);
    assert!(took < Duration::from_secs(3), "woken after {took:?}");
    let next = [
        tail("w1", "w1", &cursors[0]),
        tail("w2", "w2", woken["cursor"].as_str().unwrap()),
        tail("w3", "w3", &cursors[2]),
    ];
    let (st, answer, took) = live_wait(
        addr,
        project(&state),
        &[],
        &json!({"waitMs": 600, "subs": next}),
    )
    .await;
    assert_eq!((st, &answer), (200, &json!({"results": []})));
    assert!(
        took >= Duration::from_millis(600),
        "answered after {took:?}"
    );
    engine_shutdown(&state).await;
}

/// Every subscription ready at once is answered, in request order, within
/// one byte budget split evenly between the ready tails: the stream with
/// more than its share answers `upToDate: false` and continues from its
/// cursor; an idle one stays out.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ready_tails_are_answered_in_request_order_within_one_shared_budget() {
    let (state, addr) = http_rig(mem()).await;
    let streams = ["p1", "p2", "p3"];
    for s in streams {
        create(addr, s, br#"{"format":{"kind":"json"}}"#).await;
    }
    let cursors = pinned(&state, addr, &streams).await;
    let pad = "x".repeat(1_000);
    for n in 0..40 {
        append(addr, "p1", "", &json!({"n": n, "pad": pad})).await;
    }
    append(addr, "p3", "", &json!({"small": true})).await;
    let subs: Vec<Value> = streams
        .iter()
        .zip(&cursors)
        .map(|(s, c)| tail(s, s, c))
        .collect();
    let body = json!({"maxBytes": 16_384, "subs": subs});
    let (st, answer, _) = live_wait(addr, project(&state), &[], &body).await;
    assert_eq!(st, 200, "{answer}");
    assert_eq!(ids(&answer), ["p1", "p3"]);
    let big = entry(&answer, "p1");
    let served = big["records"].as_array().unwrap().len();
    assert!((1..40).contains(&served), "p1 served {served} of 40");
    assert_eq!(big["upToDate"], json!(false));
    assert_eq!(entry(&answer, "p3")["records"], json!([{"small": true}]));
    let rest = json!({"subs": [tail("p1", "p1", big["cursor"].as_str().unwrap())]});
    let (_, answer, _) = live_wait(addr, project(&state), &[], &rest).await;
    let first = &entry(&answer, "p1")["records"][0]["n"];
    assert_eq!(first, &json!(served), "continues where the share ended");
    engine_shutdown(&state).await;
}

/// One answer covering three tails meters ONE read operation, on the first
/// answered tail's stream, and each stream's own records and bytes; an
/// empty answer meters one operation on the first tail's stream.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn one_answer_meters_one_read_operation_and_each_streams_bytes() {
    let (state, addr) = http_rig(mem()).await;
    let streams = ["m1", "m2", "m3"];
    for s in streams {
        create(addr, s, br#"{"format":{"kind":"json"}}"#).await;
    }
    let cursors = pinned(&state, addr, &streams).await;
    let before: Vec<_> = futures_util::future::join_all(streams.map(|s| counts(&state, s))).await;
    append(addr, "m2", "", &json!({"a": 1})).await;
    append(addr, "m3", "", &json!({"b": 22})).await;
    append(addr, "m3", "", &json!({"c": 333})).await;
    let subs: Vec<Value> = streams
        .iter()
        .zip(&cursors)
        .map(|(s, c)| tail(s, s, c))
        .collect();
    let (st, answer, _) = live_wait(addr, project(&state), &[], &json!({"subs": subs})).await;
    assert_eq!(
        (st, ids(&answer)),
        (200, vec!["m2".to_string(), "m3".to_string()])
    );
    let after: Vec<_> = futures_util::future::join_all(streams.map(|s| counts(&state, s))).await;
    let moved: Vec<(u64, u64, u64)> = before
        .iter()
        .zip(&after)
        .map(|(b, a)| {
            (
                a.read_operations - b.read_operations,
                a.read_records - b.read_records,
                a.read_payload_bytes - b.read_payload_bytes,
            )
        })
        .collect();
    let a = json!({"a": 1}).to_string().len() as u64;
    let bc = (json!({"b": 22}).to_string().len() + json!({"c": 333}).to_string().len()) as u64;
    assert_eq!(moved, [(0, 0, 0), (1, 1, a), (0, 2, bc)]);
    let idle: Vec<Value> = [
        tail("m1", "m1", &cursors[0]),
        tail("m2", "m2", entry(&answer, "m2")["cursor"].as_str().unwrap()),
    ]
    .into();
    let (_, empty, _) = live_wait(
        addr,
        project(&state),
        &[],
        &json!({"waitMs": 200, "subs": idle}),
    )
    .await;
    assert_eq!(empty, json!({"results": []}));
    let last = counts(&state, "m1").await;
    assert_eq!(last.read_operations - after[0].read_operations, 1);
    engine_shutdown(&state).await;
}

/// A wait watching twenty streams parks once: one place in the live pool,
/// however many streams it watches. More subscriptions than a request
/// carries are refused whole, 429 `live_subscription_limit`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_wait_on_twenty_streams_parks_once_and_too_many_subscriptions_are_refused() {
    let (state, addr) = http_rig(mem()).await;
    let names: Vec<String> = (0..20).map(|i| format!("pool-{i}")).collect();
    for n in &names {
        create(addr, n, br#"{"format":{"kind":"json"}}"#).await;
    }
    let refs: Vec<&str> = names.iter().map(String::as_str).collect();
    let cursors = pinned(&state, addr, &refs).await;
    let subs: Vec<Value> = refs
        .iter()
        .zip(&cursors)
        .map(|(s, c)| tail(s, s, c))
        .collect();
    let body = json!({"waitMs": 1_500, "subs": subs});
    let waiting = live_wait(addr, project(&state), &[], &body);
    let looking = async {
        tokio::time::sleep(Duration::from_millis(500)).await;
        state.admission.parked()
    };
    let ((st, _, _), parked) = futures_util::future::join(waiting, looking).await;
    assert_eq!((st, parked), (200, 1));
    let many: Vec<Value> = (0..=256)
        .map(|i| tail(&format!("t{i}"), "pool-0", "now"))
        .collect();
    let (st, answer, _) = live_wait(addr, project(&state), &[], &json!({"subs": many})).await;
    assert_eq!(
        (st, &answer["error"]["code"]),
        (429, &json!("live_subscription_limit"))
    );
    engine_shutdown(&state).await;
}

/// A group whose consumer has nothing to deliver waits without a walk:
/// before its first delivery and after it settled everything, a whole
/// wait commits nothing (no configuration read, no Receive); an append
/// wakes it with the record leased to it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_drained_group_waits_without_walking_and_an_append_wakes_it_leased() {
    let (state, addr) = http_rig(mem()).await;
    create(addr, "q1", br#"{"format":{"kind":"json"}}"#).await;
    let (st, _, _) = preq(addr, "PUT", "/v1/streams/q1/consumers/g", &[KEY], b"{}").await;
    assert_eq!(st, 201);
    let idle = json!({"waitMs": 400, "subs": [group("g", "q1")]});
    let before = commits(&state);
    let (_, answer, took) = live_wait(addr, project(&state), &[], &idle).await;
    assert_eq!(answer, json!({"results": []}));
    assert_eq!(
        commits(&state) - before,
        0,
        "a new consumer on an empty stream"
    );
    assert!(took >= Duration::from_millis(400));
    let woken = json!({"waitMs": 10_000, "subs": [group("g", "q1")]});
    for n in [5, 6] {
        let waiting = live_wait(addr, project(&state), &[], &woken);
        let appending = async {
            tokio::time::sleep(Duration::from_millis(300)).await;
            append(addr, "q1", "k", &json!({"n": n})).await;
        };
        let ((_, answer, took), ()) = futures_util::future::join(waiting, appending).await;
        let leased = &entry(&answer, "g")["messages"];
        assert_eq!(leased.as_array().unwrap().len(), 1, "{answer}");
        assert_eq!(
            (
                &leased[0]["value"],
                &leased[0]["routingKey"],
                &leased[0]["attempts"]
            ),
            (&json!({"n": n}), &json!("k"), &json!(1))
        );
        assert!(took < Duration::from_secs(3), "woken after {took:?}");
        let ack = json!({"acks": [{"leaseToken": leased[0]["leaseToken"]}]});
        let (st, _, _) = preq(
            addr,
            "POST",
            "/v1/streams/q1/consumers/g:settle",
            &[KEY],
            ack.to_string().as_bytes(),
        )
        .await;
        assert_eq!(st, 200);
        let before = commits(&state);
        let (_, answer, _) = live_wait(addr, project(&state), &[], &idle).await;
        assert_eq!(answer, json!({"results": []}));
        assert_eq!(commits(&state) - before, 0, "settled through {n}: drained");
    }
    engine_shutdown(&state).await;
}

/// A lease the wait delivered and the client never settled is delivered
/// again through the next wait at its deadline, attempt 2: an expiry
/// commits nothing, so only the lease deadline the group watches wakes it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_unsettled_lease_is_redelivered_through_the_wait_at_its_deadline() {
    let (state, addr) = http_rig(mem()).await;
    create(addr, "q2", br#"{"format":{"kind":"json"}}"#).await;
    let cfg = br#"{"visibilityTimeoutMs":1000}"#;
    let (st, _, _) = preq(addr, "PUT", "/v1/streams/q2/consumers/g", &[KEY], cfg).await;
    assert_eq!(st, 201);
    append(addr, "q2", "a", &json!({"n": 1})).await;
    let body = json!({"waitMs": 10_000, "subs": [group("g", "q2")]});
    let (_, answer, _) = live_wait(addr, project(&state), &[], &body).await;
    assert_eq!(entry(&answer, "g")["messages"][0]["attempts"], json!(1));
    let (_, answer, took) = live_wait(addr, project(&state), &[], &body).await;
    let again = &entry(&answer, "g")["messages"][0];
    assert_eq!(
        (&again["value"], &again["attempts"]),
        (&json!({"n": 1}), &json!(2))
    );
    assert!(took < Duration::from_secs(4), "redelivered after {took:?}");
    engine_shutdown(&state).await;
}

/// A drained group woken by a small append reserves what was appended, not
/// a whole 4 MiB coverage: with all but 512 KiB of the instance's read
/// memory held elsewhere it still delivers at once, where a whole-coverage
/// reservation would wait out the deadline and answer empty.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_woken_group_reserves_what_was_appended_not_a_whole_coverage() {
    let (state, addr) = http_rig(mem()).await;
    create(addr, "q3", br#"{"format":{"kind":"json"}}"#).await;
    let (st, _, _) = preq(addr, "PUT", "/v1/streams/q3/consumers/g", &[KEY], b"{}").await;
    assert_eq!(st, 201);
    let first = json!({"waitMs": 100, "subs": [group("g", "q3")]});
    let (_, answer, _) = live_wait(addr, project(&state), &[], &first).await;
    assert_eq!(answer, json!({"results": []}), "the consumer is drained");
    state.admission.set_read_memory_capacity(4 << 20);
    let held = crate::admission::read_memory::ReadHold::try_reserve(
        &state.admission,
        None,
        0,
        (4 << 20) - (512 << 10),
    )
    .expect("room for the elsewhere-held reads");
    let body = json!({"waitMs": 3_000, "maxBytes": 4096, "subs": [group("g", "q3")]});
    let waiting = live_wait(addr, project(&state), &[], &body);
    let appending = async {
        tokio::time::sleep(Duration::from_millis(300)).await;
        append(addr, "q3", "k", &json!({"n": 9})).await;
    };
    let ((_, answer, took), ()) = futures_util::future::join(waiting, appending).await;
    assert_eq!(entry(&answer, "g")["messages"][0]["value"], json!({"n": 9}));
    assert!(took < Duration::from_secs(2), "delivered after {took:?}");
    drop(held);
    engine_shutdown(&state).await;
}

/// A watch from `now` is answered at once with the journal's cursor; from
/// that cursor, a matching append answers it invalidated, `changed`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_watch_pins_its_cursor_and_a_matching_append_invalidates_it() {
    let (state, addr) = http_rig(mem()).await;
    create(
        addr,
        "wt",
        br#"{"format":{"kind":"json"},"watches":[{"name":"byId","fields":["/id"]}]}"#,
    )
    .await;
    let key = crate::product::watch_key_hex("byId", &["/id".to_string()], &["\"c7\"".to_string()]);
    let sub = |cursor: &str| {
        json!({"id": "w", "kind": "watch", "stream": "wt", "key": PRISMA_KEY,
               "watch": "byId", "watchKey": key, "cursor": cursor})
    };
    let (_, answer, took) =
        live_wait(addr, project(&state), &[], &json!({"subs": [sub("now")]})).await;
    let pinned = entry(&answer, "w")["event"].clone();
    assert_eq!(pinned["invalidated"], json!(false), "{answer}");
    assert!(took < Duration::from_secs(2));
    let body = json!({"waitMs": 10_000, "subs": [sub(pinned["cursor"].as_str().unwrap())]});
    let waiting = live_wait(addr, project(&state), &[], &body);
    let appending = async {
        tokio::time::sleep(Duration::from_millis(300)).await;
        append(addr, "wt", "", &json!({"id": "c7"})).await;
    };
    let ((_, answer, took), ()) = futures_util::future::join(waiting, appending).await;
    let event = &entry(&answer, "w")["event"];
    assert_eq!(
        (&event["invalidated"], &event["reason"]),
        (&json!(true), &json!("changed")),
        "{answer}"
    );
    assert!(took < Duration::from_secs(3), "woken after {took:?}");
    engine_shutdown(&state).await;
}

const PROJECT: (&str, &str) = ("proj-live", "ws-live");

/// `c-full` holds every scope over every name; `c-a` reads records and
/// pulls under "a" only; `c-pull` only pulls, over every name.
fn bearers(svc: &crate::auth::AuthService) -> (String, String, String) {
    let all = Scope::ALL.map(Scope::as_str).join(" ");
    let under_a = crate::tenant::normalize_prefix_set(&["a"]).unwrap();
    let a_scopes = "streams.records.read streams.consumers.pull";
    let grants = [
        ("c-full", all.as_str(), StreamGrant::All),
        ("c-a", a_scopes, StreamGrant::Prefixes(under_a.into())),
        ("c-pull", "streams.consumers.pull", StreamGrant::All),
    ];
    let credentials = grants.into_iter().map(|(cred, scopes, grant)| {
        let credential = CredentialGrant {
            credential_id: Arc::from(cred),
            project_id: ProjectId::new(PROJECT.0).unwrap(),
            grant_version: 2,
            status: CredentialStatus::Active,
            scopes: ScopeSet::parse(scopes).0,
            grant,
            expires_at: None,
        };
        (Arc::from(cred), credential)
    });
    svc.publish_grants(GrantSnapshot {
        credentials: credentials.collect(),
        fetched_at_unix: crate::shard::now_ms() / 1000,
        feed_version: 2,
    })
    .unwrap();
    (
        rig_bearer(PROJECT, "c-full", &all, 2),
        rig_bearer(PROJECT, "c-a", a_scopes, 2),
        rig_bearer(PROJECT, "c-pull", "streams.consumers.pull", 2),
    )
}

/// The `error` a per-stream long-poll on `name` answers `bearer`.
async fn per_stream_refusal(addr: SocketAddr, bearer: &str, name: &str) -> (u16, Value) {
    let (st, _, b) = preq(
        addr,
        "GET",
        &format!("/v1/streams/{name}/records:long-poll?cursor=now&waitMs=0"),
        &[("authorization", bearer), KEY],
        b"",
    )
    .await;
    let v: Value = serde_json::from_slice(&b).unwrap_or(Value::Null);
    (st, v["error"].clone())
}

/// Every subscription is checked against the token's grants: one outside
/// the prefix grant, one under a token without `records.read`, and one
/// with a wrong key are each answered with exactly the refusal their
/// per-stream route gives that token, while the covered one is served. A
/// path naming another project is the usage route's 404.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn every_subscription_is_held_to_the_tokens_grants_and_answered_its_own_refusal() {
    let (svc, state, addr) = auth_rig(PROJECT.0, PROJECT.1, &[], None).await;
    let (full, under_a, pull_only) = bearers(&svc);
    for name in ["a/x", "b/y"] {
        let (st, _, b) = preq(
            addr,
            "PUT",
            &format!("/v1/streams/{name}"),
            &[("authorization", &full), KEY],
            br#"{"format":{"kind":"json"}}"#,
        )
        .await;
        assert_eq!(st, 201, "{}", String::from_utf8_lossy(&b));
    }
    let as_a = [("authorization", under_a.as_str())];
    let wrong = json!({"id": "k", "kind": "tail", "stream": "a/x", "cursor": "now",
                       "key": "AQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQE="});
    let body = json!({"subs": [tail("ok", "a/x", "now"), tail("no", "b/y", "now"), wrong]});
    let (st, answer, _) = live_wait(addr, PROJECT.0.into(), &as_a, &body).await;
    assert_eq!(st, 200, "{answer}");
    assert_eq!(ids(&answer), ["ok", "no", "k"]);
    assert_eq!(entry(&answer, "ok")["records"], json!([]));
    let (status, error) = per_stream_refusal(addr, &under_a, "b/y").await;
    assert_eq!((status, &entry(&answer, "no")["error"]), (403, &error));
    assert_eq!(entry(&answer, "no")["status"], json!(403));
    assert_eq!(entry(&answer, "k")["status"], json!(403));
    assert_eq!(entry(&answer, "k")["error"]["code"], json!("wrong_key"));
    let as_pull = [("authorization", pull_only.as_str())];
    let body = json!({"subs": [tail("s", "a/x", "now")]});
    let (_, answer, _) = live_wait(addr, PROJECT.0.into(), &as_pull, &body).await;
    let (status, error) = per_stream_refusal(addr, &pull_only, "a/x").await;
    assert_eq!((status, &entry(&answer, "s")["error"]), (403, &error));
    let (st, answer, _) = live_wait(addr, "proj-other".into(), &as_a, &body).await;
    assert_eq!(
        (st, &answer["error"]["code"]),
        (404, &json!("unknown_project"))
    );
    engine_shutdown(&state).await;
}

/// A wait whose stream's engine retires under it is not left on the
/// retired handle until its deadline: it answers at once with the read's
/// own retryable refusal (503 `temporarily_unavailable`, as the per-stream
/// read answers while the shard reopens), and the client's replay, made as
/// the SDK makes it, waits on the replacement engine and is answered with
/// exactly the next record.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_retired_engine_answers_its_wait_retryably_and_the_replay_gets_the_next_record() {
    let (state, addr) = http_rig(mem()).await;
    create(addr, "er", br#"{"format":{"kind":"json"}}"#).await;
    append(addr, "er", "", &json!({"n": 0})).await;
    let cursors = pinned(&state, addr, &["er"]).await;
    let sref = state.deployment.raw_adapter_sref("er");
    let desc = state.registry.get(&sref).await.unwrap().unwrap();
    let prefix = state
        .shards
        .prefix_for(&desc.resolve_segment("").shard_route);
    let body = json!({"waitMs": 10_000, "subs": [tail("er", "er", &cursors[0])]});
    let waiting = live_wait(addr, project(&state), &[], &body);
    let retiring = async {
        tokio::time::sleep(Duration::from_millis(300)).await;
        state
            .shards
            .open(&prefix)
            .expect("the tail's engine is resident")
            .begin_close();
    };
    let ((st, answer, took), ()) = futures_util::future::join(waiting, retiring).await;
    let refused = entry(&answer, "er");
    assert_eq!(
        (st, &refused["status"], &refused["error"]["retryable"]),
        (200, &json!(503), &json!(true)),
        "{answer}"
    );
    assert!(took < Duration::from_secs(3), "answered after {took:?}");
    let replay = replay_past_503(addr, project(&state), &body);
    let appending = async {
        tokio::time::sleep(Duration::from_millis(500)).await;
        append_past_503(addr, "er", &json!({"n": 1})).await;
    };
    let ((st, answer), ()) = futures_util::future::join(replay, appending).await;
    assert_eq!(st, 200);
    assert_eq!(
        entry(&answer, "er")["records"],
        json!([{"n": 1}]),
        "{answer}"
    );
    engine_shutdown(&state).await;
}

/// A wait sent again while it answers the subscription `er` with a
/// retryable 503, as the SDK replays it.
async fn replay_past_503(addr: SocketAddr, project: String, body: &Value) -> (u16, Value) {
    for _ in 0..100 {
        let (st, answer, _) = live_wait(addr, project.clone(), &[], body).await;
        if entry(&answer, "er")["status"] != json!(503) {
            return (st, answer);
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("the wait kept answering 503");
}

/// An append sent again while the shard reopens behind a retryable 503,
/// as a client replays it.
async fn append_past_503(addr: SocketAddr, name: &str, record: &Value) {
    let path = format!("/v1/streams/{name}/records");
    for _ in 0..100 {
        let body = record.to_string();
        let (st, _, _) = preq(addr, "POST", &path, &[KEY], body.as_bytes()).await;
        if st == 200 {
            return;
        }
        assert_eq!(st, 503);
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("the shard never reopened");
}

/// A tail waiting through a split's seal window (the parent fenced, its
/// successors not yet published) is neither answered `not_owner` nor left
/// on the closed parent: once the split publishes, its wait answers the
/// hop to the successor (a moved cursor, no records), and the client's
/// wait from there gets the first record appended under its routing key,
/// all well before one deadline.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_tail_waiting_through_a_splits_seal_window_follows_it_to_the_first_record_after() {
    let _l = super::fixture_failpoints::gap_lock().lock().await;
    let (state, addr, _guard, split) = super::fixture_http::rig_in_seal_gap("sgap", 3).await;
    let (st, pinned, _) = live_wait(
        addr,
        project(&state),
        &[],
        &json!({"subs": [gap_tail("now")]}),
    )
    .await;
    assert_eq!(st, 200, "{pinned}");
    let cursor = entry(&pinned, "t")["cursor"].as_str().unwrap().to_string();
    let started = Instant::now();
    let following = follow_gap_tail(addr, project(&state), cursor);
    let publishing = async {
        tokio::time::sleep(Duration::from_millis(500)).await;
        crate::failpoints::release_scaler_before_publish("sgap");
        split.await.unwrap();
        super::fixture_http::await_published(&state, "sgap").await;
        append(addr, "sgap", "ga", &json!({"k": "ga", "n": 99})).await;
    };
    let (records, ()) = futures_util::future::join(following, publishing).await;
    assert_eq!(records, json!([{"k": "ga", "n": 99}]));
    let took = started.elapsed();
    assert!(took < Duration::from_secs(8), "delivered after {took:?}");
    engine_shutdown(&state).await;
}

/// The seal-window stream's tail under routing key `ga` from `cursor`.
fn gap_tail(cursor: &str) -> Value {
    json!({"id": "t", "kind": "tail", "stream": "sgap", "key": PRISMA_KEY,
           "routingKey": "ga", "cursor": cursor})
}

/// Wait on the seal-window tail from `cursor`, following every answered
/// cursor as a client does, until an answer carries records: those.
async fn follow_gap_tail(addr: SocketAddr, project: String, mut cursor: String) -> Value {
    for _ in 0..5 {
        let body = json!({"waitMs": 10_000, "subs": [gap_tail(&cursor)]});
        let (st, answer, _) = live_wait(addr, project.clone(), &[], &body).await;
        assert_eq!(st, 200, "{answer}");
        let e = entry(&answer, "t");
        assert!(e["error"].is_null(), "{answer}");
        if e["records"] != json!([]) {
            return e["records"].clone();
        }
        cursor = e["cursor"].as_str().unwrap().to_string();
    }
    panic!("no records after five answers");
}

/// A wait reserves its answer, `maxBytes`, as a page read reserves its
/// page: with the instance's read memory held elsewhere it is refused
/// 503 `read_memory_busy` (retryable) after the reservation's wait.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_wait_reserves_its_answer_and_is_refused_when_read_memory_is_held() {
    let (state, addr) = http_rig(mem()).await;
    create(addr, "rm", br#"{"format":{"kind":"json"}}"#).await;
    state.admission.set_read_memory_capacity(1 << 20);
    let held =
        crate::admission::read_memory::ReadHold::try_reserve(&state.admission, None, 0, 1 << 20)
            .expect("the whole read memory");
    let body = json!({"maxBytes": 65_536, "subs": [tail("t", "rm", "now")]});
    let (st, answer, _) = live_wait(addr, project(&state), &[], &body).await;
    assert_eq!(
        (st, &answer["error"]["code"], &answer["error"]["retryable"]),
        (503, &json!("read_memory_busy"), &json!(true))
    );
    drop(held);
    let (st, _, _) = live_wait(addr, project(&state), &[], &body).await;
    assert_eq!(st, 200);
    engine_shutdown(&state).await;
}

/// A drained group woken by more than the free read memory reserves what
/// was appended: its walk waits for room rather than reading past it, so
/// with 64 KiB free and 160 KiB appended the wait answers empty at its
/// deadline, the messages still unleased for the next wait.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_woken_group_waits_for_room_for_what_was_appended() {
    let (state, addr) = http_rig(mem()).await;
    create(addr, "q4", br#"{"format":{"kind":"json"}}"#).await;
    let (st, _, _) = preq(addr, "PUT", "/v1/streams/q4/consumers/g", &[KEY], b"{}").await;
    assert_eq!(st, 201);
    state.admission.set_read_memory_capacity(1 << 20);
    let held = crate::admission::read_memory::ReadHold::try_reserve(
        &state.admission,
        None,
        0,
        (1 << 20) - (64 << 10),
    )
    .expect("all but 64 KiB of the read memory");
    let body = json!({"waitMs": 1_500, "maxBytes": 4096, "subs": [group("g", "q4")]});
    let waiting = live_wait(addr, project(&state), &[], &body);
    let pad = "x".repeat(2_000);
    let appending = async {
        tokio::time::sleep(Duration::from_millis(300)).await;
        let batch: Vec<Value> = (0..80).map(|n| json!({"n": n, "pad": pad})).collect();
        let (st, _, b) = preq(
            addr,
            "POST",
            "/v1/streams/q4/records:batch",
            &[KEY, ("prisma-routing-key", "k")],
            serde_json::to_vec(&batch).unwrap().as_slice(),
        )
        .await;
        assert_eq!(st, 200, "{}", String::from_utf8_lossy(&b));
    };
    let ((_, answer, took), ()) = futures_util::future::join(waiting, appending).await;
    assert_eq!(answer, json!({"results": []}));
    assert!(
        took >= Duration::from_millis(1_500),
        "answered after {took:?}"
    );
    drop(held);
    // An undrained group's walk reserves a whole coverage (4 MiB): room as
    // the instance's read memory has it (a quarter of the shed line).
    state.admission.set_read_memory_capacity(64 << 20);
    let (_, answer, _) = live_wait(addr, project(&state), &[], &body).await;
    assert_eq!(entry(&answer, "g")["messages"][0]["value"]["n"], json!(0));
    engine_shutdown(&state).await;
}

/// A tail polling through a split's seal window holds its answer's read
/// reservation only while it reads: between polls it waits with nothing
/// reserved, as a parked wait does.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_tail_polling_through_a_seal_window_releases_its_reservation_between_polls() {
    let _l = super::fixture_failpoints::gap_lock().lock().await;
    let (state, addr, _guard, split) = super::fixture_http::rig_in_seal_gap("sgrel", 2).await;
    let (_, pinned, _) = live_wait(
        addr,
        project(&state),
        &[],
        &json!({"subs": [gap_tail_of("sgrel", "now")]}),
    )
    .await;
    let cursor = entry(&pinned, "t")["cursor"].as_str().unwrap().to_string();
    let body = json!({"waitMs": 1_500, "subs": [gap_tail_of("sgrel", &cursor)]});
    let waiting = live_wait(addr, project(&state), &[], &body);
    let sampling = async {
        tokio::time::sleep(Duration::from_millis(300)).await;
        let mut held = Vec::new();
        for _ in 0..10 {
            held.push(state.admission.read_memory().0);
            tokio::time::sleep(Duration::from_millis(77)).await;
        }
        held
    };
    let ((st, _, _), held) = futures_util::future::join(waiting, sampling).await;
    assert_eq!(st, 200);
    let idle = held.iter().filter(|&&bytes| bytes == 0).count();
    assert!(idle >= 8, "read memory held between polls: {held:?}");
    crate::failpoints::release_scaler_before_publish("sgrel");
    split.await.unwrap();
    engine_shutdown(&state).await;
}

/// A seal-window stream's tail under routing key `ga` from `cursor`.
fn gap_tail_of(stream: &str, cursor: &str) -> Value {
    json!({"id": "t", "kind": "tail", "stream": stream, "key": PRISMA_KEY,
           "routingKey": "ga", "cursor": cursor})
}

/// A wait is parked from its start, not only once nothing was ready: a
/// first walk of a group with a backlog that waits for read memory waits
/// parked, outside the write gate's active requests, so a cell full of
/// such waits never sheds its writers.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_wait_waiting_for_read_memory_on_its_first_walk_waits_parked() {
    let (state, addr) = http_rig(mem()).await;
    create(addr, "q5", br#"{"format":{"kind":"json"}}"#).await;
    let (st, _, _) = preq(addr, "PUT", "/v1/streams/q5/consumers/g", &[KEY], b"{}").await;
    assert_eq!(st, 201);
    append_batch(addr, "q5", 80, 2_000).await;
    state.admission.set_read_memory_capacity(1 << 20);
    let held = crate::admission::read_memory::ReadHold::try_reserve(
        &state.admission,
        None,
        0,
        (1 << 20) - (64 << 10),
    )
    .expect("all but 64 KiB of the read memory");
    let body = json!({"waitMs": 1_500, "maxBytes": 4096, "subs": [group("g", "q5")]});
    let waiting = live_wait(addr, project(&state), &[], &body);
    let looking = async {
        tokio::time::sleep(Duration::from_millis(500)).await;
        state.admission.parked()
    };
    let ((_, answer, _), parked) = futures_util::future::join(waiting, looking).await;
    assert_eq!((parked, &answer), (1, &json!({"results": []})));
    drop(held);
    engine_shutdown(&state).await;
}

/// A group with a small backlog is walked under a reservation sized to the
/// records past its consumer's cursor (three of ~120 B: 3 x (120 + 64) +
/// 4 KiB), not a whole 4 MiB coverage: with all but 16 KiB of the read
/// memory held elsewhere, a first wait leases its backlog at once.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_group_with_a_small_backlog_reserves_its_backlog_not_a_whole_coverage() {
    let (state, addr) = http_rig(mem()).await;
    create(addr, "q6", br#"{"format":{"kind":"json"}}"#).await;
    let (st, _, _) = preq(addr, "PUT", "/v1/streams/q6/consumers/g", &[KEY], b"{}").await;
    assert_eq!(st, 201);
    append_batch(addr, "q6", 3, 100).await;
    state.admission.set_read_memory_capacity(4 << 20);
    let held = crate::admission::read_memory::ReadHold::try_reserve(
        &state.admission,
        None,
        0,
        (4 << 20) - (16 << 10),
    )
    .expect("all but 16 KiB of the read memory");
    let body = json!({"waitMs": 3_000, "maxBytes": 4096, "subs": [group("g", "q6")]});
    let (_, answer, took) = live_wait(addr, project(&state), &[], &body).await;
    assert_eq!(
        entry(&answer, "g")["messages"][0]["value"]["n"],
        json!(0),
        "{answer}"
    );
    assert!(took < Duration::from_secs(2), "leased after {took:?}");
    drop(held);
    engine_shutdown(&state).await;
}

/// `n` records of about `pad` bytes appended to `name` in one batch under
/// routing key `k`.
async fn append_batch(addr: SocketAddr, name: &str, n: usize, pad: usize) {
    let pad = "x".repeat(pad);
    let batch: Vec<Value> = (0..n).map(|i| json!({"n": i, "pad": pad})).collect();
    let (st, _, b) = preq(
        addr,
        "POST",
        &format!("/v1/streams/{name}/records:batch"),
        &[KEY, ("prisma-routing-key", "k")],
        serde_json::to_vec(&batch).unwrap().as_slice(),
    )
    .await;
    assert_eq!(st, 200, "{}", String::from_utf8_lossy(&b));
}
