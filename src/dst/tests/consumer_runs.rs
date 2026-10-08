//! Run leases end to end: `:pull` delivers consecutive records of one routing
//! key up to `max`, each with its own lease token; while any of them is
//! leased the key is blocked for every other pull of the consumer group;
//! `:settle` acks record by record; a consumer that stops part-way gets the
//! unacked rest again, in offset order, once their visibility expires; a run
//! that reaches `maxAttempts` is dead-lettered record by record in offset
//! order.
use super::fixture_http::{engine_shutdown, http_rig};
use super::fixture_requests::{PRISMA_KEY, preq};
use super::fixture_storage::mem;
use std::net::SocketAddr;

const KEY: [(&str, &str); 1] = [("prisma-encryption-key", PRISMA_KEY)];

/// One delivered message: `(routing key, value.n, attempts, lease token)`.
type Delivered = (String, i64, i64, String);

/// A JSON stream `name` holding one `{"n": i}` record per routing key in
/// `keys`, `i` counting that key's records, and a consumer `w` configured
/// by `consumer`.
async fn stream(addr: SocketAddr, name: &str, keys: &[&str], consumer: &[u8]) {
    let path = format!("/v1/streams/{name}");
    let (st, _, b) = preq(addr, "PUT", &path, &KEY, br#"{"format":{"kind":"json"}}"#).await;
    assert_eq!(st, 201, "{}", String::from_utf8_lossy(&b));
    let mut counts: std::collections::HashMap<&str, i64> = Default::default();
    for key in keys {
        let n = counts.entry(key).or_default();
        let headers = [KEY[0], ("prisma-routing-key", key)];
        let body = format!("{{\"n\":{n}}}");
        let (st, _, _) = preq(
            addr,
            "POST",
            &format!("{path}/records"),
            &headers,
            body.as_bytes(),
        )
        .await;
        assert_eq!(st, 200);
        *n += 1;
    }
    let (st, _, b) = preq(addr, "PUT", &format!("{path}/consumers/w"), &KEY, consumer).await;
    assert_eq!(st, 201, "{}", String::from_utf8_lossy(&b));
}

async fn pull(addr: SocketAddr, name: &str, body: &str) -> Vec<Delivered> {
    let path = format!("/v1/streams/{name}/consumers/w:pull");
    let (st, _, b) = preq(addr, "POST", &path, &KEY, body.as_bytes()).await;
    assert_eq!(st, 200, "{}", String::from_utf8_lossy(&b));
    let v: serde_json::Value = serde_json::from_slice(&b).unwrap();
    v["messages"]
        .as_array()
        .unwrap()
        .iter()
        .map(|m| {
            (
                m["routingKey"].as_str().unwrap().to_string(),
                m["value"]["n"].as_i64().unwrap(),
                m["attempts"].as_i64().unwrap(),
                m["leaseToken"].as_str().unwrap().to_string(),
            )
        })
        .collect()
}

/// Acks `tokens` in one settle; answers `(acked, stale, backlog)`.
async fn ack(addr: SocketAddr, name: &str, tokens: &[&str]) -> (i64, i64, i64) {
    let acks: Vec<String> = tokens
        .iter()
        .map(|t| format!("{{\"leaseToken\":\"{t}\"}}"))
        .collect();
    let body = format!("{{\"acks\":[{}]}}", acks.join(","));
    let path = format!("/v1/streams/{name}/consumers/w:settle");
    let (st, _, b) = preq(addr, "POST", &path, &KEY, body.as_bytes()).await;
    assert_eq!(st, 200, "{}", String::from_utf8_lossy(&b));
    let v: serde_json::Value = serde_json::from_slice(&b).unwrap();
    let field = |name: &str| v[name].as_i64().unwrap();
    (field("acked"), field("stale"), field("backlog"))
}

/// `(routing key, value.n, attempts)` of each message, tokens dropped.
fn shape(messages: &[Delivered]) -> Vec<(&str, i64, i64)> {
    messages
        .iter()
        .map(|(k, n, a, _)| (k.as_str(), *n, *a))
        .collect()
}

/// `<routing key>/<value.n>@<attempts>` of each dead-letter copy in `stream`.
async fn dead_letters(addr: SocketAddr, stream: &str) -> Vec<String> {
    let path = format!("/v1/streams/{stream}/records");
    let (st, _, b) = preq(addr, "GET", &path, &KEY, b"").await;
    assert_eq!(st, 200);
    let copies: Vec<serde_json::Value> = serde_json::from_slice(&b).unwrap();
    copies
        .iter()
        .map(|c| {
            let key = c["routingKey"].as_str().unwrap();
            format!("{key}/{}@{}", c["value"]["n"], c["attempts"])
        })
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_pull_delivers_a_run_of_one_routing_key_with_one_lease_token_per_record() {
    let (state, addr) = http_rig(mem()).await;
    stream(addr, "runs1", &["o", "o", "p", "o", "o", "o"], b"{}").await;
    let first = pull(addr, "runs1", r#"{"max":4}"#).await;
    assert_eq!(
        shape(&first),
        vec![("o", 0, 1), ("o", 1, 1), ("p", 0, 1), ("o", 2, 1)],
        "o's run up to max, p in offset order"
    );
    let tokens: std::collections::HashSet<&str> = first.iter().map(|m| m.3.as_str()).collect();
    assert_eq!(tokens.len(), 4, "one lease token per record");
    assert_eq!(
        shape(&pull(addr, "runs1", r#"{"max":10}"#).await),
        vec![],
        "o3 and o4 wait behind o's leased run"
    );
    assert_eq!(
        ack(addr, "runs1", &[&first[0].3, &first[1].3]).await,
        (2, 0, 4)
    );
    assert_eq!(
        shape(&pull(addr, "runs1", r#"{"max":10}"#).await),
        vec![],
        "o2's live lease still blocks o"
    );
    assert_eq!(
        ack(addr, "runs1", &[&first[3].3, &first[2].3]).await,
        (2, 0, 2)
    );
    let rest = pull(addr, "runs1", r#"{"max":10}"#).await;
    assert_eq!(shape(&rest), vec![("o", 3, 1), ("o", 4, 1)]);
    assert_eq!(
        ack(addr, "runs1", &[&rest[1].3, &rest[0].3]).await,
        (2, 0, 0)
    );
    engine_shutdown(&state).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_consumer_that_stops_part_way_gets_the_unacked_rest_again_in_offset_order() {
    let (state, addr) = http_rig(mem()).await;
    stream(addr, "runs2", &["o", "o", "o", "o", "o"], b"{}").await;
    let first = pull(addr, "runs2", r#"{"max":5,"visibilityMs":1000}"#).await;
    assert_eq!(
        shape(&first),
        vec![
            ("o", 0, 1),
            ("o", 1, 1),
            ("o", 2, 1),
            ("o", 3, 1),
            ("o", 4, 1)
        ]
    );
    // Acks 0, 1 and 3 of five, out of order, then stops.
    assert_eq!(
        ack(addr, "runs2", &[&first[3].3, &first[0].3, &first[1].3]).await,
        (3, 0, 2)
    );
    tokio::time::sleep(std::time::Duration::from_millis(1_200)).await;
    let again = pull(addr, "runs2", r#"{"max":10}"#).await;
    assert_eq!(
        shape(&again),
        vec![("o", 2, 2), ("o", 4, 2)],
        "the unacked rest, lowest offset first, second attempt"
    );
    assert_eq!(
        ack(addr, "runs2", &[&first[2].3, &first[4].3]).await,
        (0, 2, 2),
        "the first delivery's tokens are superseded"
    );
    assert_eq!(
        ack(addr, "runs2", &[&again[0].3, &again[1].3]).await,
        (2, 0, 0)
    );
    engine_shutdown(&state).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_run_that_reaches_max_attempts_is_dead_lettered_in_offset_order() {
    let (state, addr) = http_rig(mem()).await;
    let dlq = "/v1/streams/runs3-dlq";
    let (st, _, _) = preq(addr, "PUT", dlq, &KEY, br#"{"format":{"kind":"json"}}"#).await;
    assert_eq!(st, 201);
    let consumer = br#"{"maxAttempts":1,"deadLetterStream":"runs3-dlq"}"#;
    stream(addr, "runs3", &["p", "p", "p", "q"], consumer).await;
    let first = pull(addr, "runs3", r#"{"max":3,"visibilityMs":1000}"#).await;
    assert_eq!(shape(&first), vec![("p", 0, 1), ("p", 1, 1), ("p", 2, 1)]);
    tokio::time::sleep(std::time::Duration::from_millis(1_200)).await;
    // A Receive judges one poisoned record per key, the lowest first: this
    // pull dead-letters p0 and returns q0's lease; the next leases nothing,
    // so its handoff loop dead-letters p1, then p2, in order.
    let next = pull(addr, "runs3", r#"{"max":10}"#).await;
    assert_eq!(shape(&next), vec![("q", 0, 1)]);
    assert_eq!(dead_letters(addr, "runs3-dlq").await, vec!["p/0@1"]);
    assert_eq!(shape(&pull(addr, "runs3", r#"{"max":10}"#).await), vec![]);
    assert_eq!(
        dead_letters(addr, "runs3-dlq").await,
        vec!["p/0@1", "p/1@1", "p/2@1"]
    );
    assert_eq!(ack(addr, "runs3", &[&next[0].3]).await, (1, 0, 0));
    engine_shutdown(&state).await;
}
