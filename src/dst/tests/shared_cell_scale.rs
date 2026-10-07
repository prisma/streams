//! Shared-cell scale (Layer A of the shared-cells plan, section 4.1):
//! one enforce-mode cell serves `cell_scale()` projects (128 in the
//! suite, 1,000 with `MT_CERT_PROJECTS=1000`) in 16 workspaces. Every
//! project reuses the same names, routing keys, producer id, consumer
//! group and encryption key; one project in eight writes, the rest stay
//! idle with empty streams. Every assertion is exact: each project's
//! answer equals its own ledger, never a superset or a count bound.

use super::fixture_auth::rig_sse;
use super::fixture_cell::{
    Answers, CREATE, Cell, CellSpec, KEYS, Ledger, NAMES, WORKSPACES, append_all, burst,
    cell_scale, error_code, for_each, journal, key_query, marker, open_cell, padded, project,
    record, seed, workspace,
};
use super::fixture_http::engine_shutdown;
use super::fixture_livefeed::{hub_sse_collect, sse_head};
use super::fixture_requests::preq;
use crate::project_policy::ProjectQuotas;
use serde_json::Value;

/// A1 (reads, scans, metadata, catalog, forks): for EVERY project and
/// every reused name, the read of every routing key from the beginning
/// returns exactly that project's ledger in order, the scan returns
/// exactly its records with their keys, the metadata names its own
/// stream, and the catalog lists exactly its own three names. Idle
/// projects read `[]`. A customer credential cannot fork on either
/// surface, so no project gains a stream.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn every_project_reads_exactly_its_own_records() {
    let cell = open_cell(CellSpec::open(cell_scale())).await;
    cell.create_everywhere().await;
    let ledger = seed(&cell).await;
    for_each(cell.projects, |i| fork_attempt_creates_nothing(&cell, i)).await;
    for_each(cell.projects, |i| own_view_is_exact(&cell, &ledger, i)).await;
    engine_shutdown(&cell.state).await;
}

/// Project `i`'s whole view: reads per key, scans, metadata, catalog.
async fn own_view_is_exact(cell: &Cell, ledger: &Ledger, i: usize) {
    for (s, name) in NAMES.iter().enumerate() {
        for (k, key) in KEYS.iter().enumerate() {
            let path = format!("/v1/streams/{name}/records{}", key_query(key));
            let got = cell.json(i, "GET", &path).await;
            let want = Value::Array(ledger.of(i, s, k));
            assert_eq!(got, want, "{} read {name} key {key:?}", project(i));
        }
        let scan = cell
            .json(i, "GET", &format!("/v1/streams/{name}:scan"))
            .await;
        let mut items: Vec<(String, Value)> = scan
            .as_array()
            .unwrap()
            .iter()
            .map(|it| {
                (
                    it["routingKey"].as_str().unwrap().to_string(),
                    it["value"].clone(),
                )
            })
            .collect();
        items.sort_by_key(|(_, v)| marker(v));
        assert_eq!(items, ledger.stream(i, s), "{} scan {name}", project(i));
        let meta = cell.json(i, "GET", &format!("/v1/streams/{name}")).await;
        let shape = (
            meta["name"].as_str(),
            meta["sealed"].as_bool(),
            meta["contentType"].as_str(),
        );
        let want = (Some(*name), Some(false), Some("application/json"));
        assert_eq!(shape, want, "{} metadata: {meta}", project(i));
    }
    assert_eq!(catalog(cell, i).await, sorted_names(), "{}", project(i));
}

/// The names project `i`'s catalog lists, sorted.
async fn catalog(cell: &Cell, i: usize) -> Vec<String> {
    let v = cell.json(i, "GET", "/v1/streams").await;
    let mut names: Vec<String> = v["streams"]
        .as_array()
        .unwrap()
        .iter()
        .map(|s| s["name"].as_str().unwrap().to_string())
        .collect();
    names.sort();
    names
}

fn sorted_names() -> Vec<String> {
    let mut names: Vec<String> = NAMES.iter().map(|n| (*n).to_string()).collect();
    names.sort();
    names
}

/// A customer bearer forking `orders` on the raw surface (fleet identity
/// only under enforce) and the product surface (no fork field) creates
/// nothing anywhere.
async fn fork_attempt_creates_nothing(cell: &Cell, i: usize) {
    let headers = [
        ("authorization", cell.bearers[i].as_str()),
        ("content-type", "application/json"),
        ("stream-forked-from", "orders"),
    ];
    let (st, _, b) = preq(cell.addr, "PUT", "/v1/stream/orders-fork", &headers, b"").await;
    assert_eq!(
        st,
        401,
        "{} raw fork: {}",
        project(i),
        String::from_utf8_lossy(&b)
    );
    let body = br#"{"format":{"kind":"json"},"forkedFrom":"orders"}"#;
    let (st, _, b) = cell.call(i, "PUT", "/v1/streams/orders-fork", body).await;
    let v: Value = serde_json::from_slice(&b).unwrap_or_default();
    assert_eq!(
        (st, v["error"]["code"].as_str()),
        (400, Some("invalid_config")),
        "{} product fork: {v}",
        project(i)
    );
}

/// A1 (consumer and producer surfaces): every project's group `g1` on
/// `orders` drains exactly its own records, and producer `p-1` at
/// sequence 0 is new in every project once and a duplicate only within
/// its own project afterwards.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn consumer_groups_and_producers_stay_inside_their_project() {
    let cell = open_cell(CellSpec::open(cell_scale())).await;
    cell.create_everywhere().await;
    let ledger = seed(&cell).await;
    for_each(cell.projects, |i| {
        group_drains_exactly_its_own(&cell, &ledger, i)
    })
    .await;
    for duplicate in [false, true] {
        for_each(cell.projects, |i| produce_once(&cell, i, duplicate)).await;
    }
    let shared = &cell;
    for_each(cell.projects, |i| async move {
        let got = shared
            .json(i, "GET", "/v1/streams/events/records?routingKey=kp")
            .await;
        let want = Value::Array(vec![record(i, "events", "kp", 0)]);
        assert_eq!(got, want, "{} producer records", project(i));
    })
    .await;
    engine_shutdown(&cell.state).await;
}

/// Configure `g1` on `orders`, then pull and acknowledge until a pull is
/// empty: the delivered records are exactly project `i`'s.
async fn group_drains_exactly_its_own(cell: &Cell, ledger: &Ledger, i: usize) {
    let path = "/v1/streams/orders/consumers/g1";
    let (st, _, b) = cell.call(i, "PUT", path, b"{}").await;
    assert_eq!(
        st,
        201,
        "{} g1: {}",
        project(i),
        String::from_utf8_lossy(&b)
    );
    let mut got = Vec::new();
    for _ in 0..16 {
        let pull = format!("{path}:pull");
        let (st, _, b) = cell.call(i, "POST", &pull, br#"{"max":100}"#).await;
        assert_eq!(
            st,
            200,
            "{} pull: {}",
            project(i),
            String::from_utf8_lossy(&b)
        );
        let v: Value = serde_json::from_slice(&b).unwrap();
        let msgs = v["messages"].as_array().unwrap();
        if msgs.is_empty() {
            break;
        }
        let acks: Vec<String> = msgs
            .iter()
            .map(|m| {
                format!(
                    r#"{{"leaseToken":"{}"}}"#,
                    m["leaseToken"].as_str().unwrap()
                )
            })
            .collect();
        let settle = format!(r#"{{"acks":[{}]}}"#, acks.join(","));
        let (st, _, b) = cell
            .call(i, "POST", &format!("{path}:settle"), settle.as_bytes())
            .await;
        assert_eq!(
            st,
            200,
            "{} settle: {}",
            project(i),
            String::from_utf8_lossy(&b)
        );
        got.extend(msgs.iter().map(|m| {
            (
                m["routingKey"].as_str().unwrap().to_string(),
                m["value"].clone(),
            )
        }));
    }
    got.sort_by_key(|(_, v)| marker(v));
    assert_eq!(got, ledger.stream(i, 0), "{} g1 delivered", project(i));
}

/// Append producer `p-1` sequence 0 on `events`/`kp`; the answer must
/// say `duplicate` exactly as given.
async fn produce_once(cell: &Cell, i: usize, duplicate: bool) {
    let body = record(i, "events", "kp", 0).to_string();
    let headers = [
        ("prisma-encryption-key", super::fixture_requests::PRISMA_KEY),
        ("authorization", cell.bearers[i].as_str()),
        ("prisma-routing-key", "kp"),
        ("producer-id", "p-1"),
        ("producer-epoch", "1"),
        ("producer-seq", "0"),
    ];
    let path = "/v1/streams/events/records";
    let (st, _, b) = preq(cell.addr, "POST", path, &headers, body.as_bytes()).await;
    let v: Value = serde_json::from_slice(&b).unwrap_or_default();
    assert_eq!(
        (st, v["duplicate"].as_bool()),
        (200, Some(duplicate)),
        "{} producer: {v}",
        project(i)
    );
}

/// The record a live writer or a fence appends: sequences past any
/// seeded volume, so they never collide with the ledger.
const LIVE: usize = 100;
const FENCE: usize = 999;

/// `count` projects spread evenly over the cell, each `offset` past a
/// multiple of eight (0 is idle, 3 is active, 5 is idle and unwatched).
fn spread(projects: usize, count: usize, offsets: [usize; 2]) -> Vec<usize> {
    (0..count)
        .map(|j| (j * projects / count) / 8 * 8 + offsets[j % 2])
        .collect()
}

/// The record markers in an SSE transcript, in order (head stripped,
/// chunked framing removed).
fn sse_markers(transcript: &str) -> Vec<String> {
    let body = transcript.split_once("\r\n\r\n").map_or("", |(_, b)| b);
    let mut text = String::new();
    let mut rest = body;
    while let Some((size, tail)) = rest.split_once("\r\n") {
        let Ok(n) = usize::from_str_radix(size.trim(), 16) else {
            break;
        };
        let end = n.min(tail.len());
        text.push_str(&tail[..end]);
        if n == 0 || end < n {
            break;
        }
        rest = tail[n..].strip_prefix("\r\n").unwrap_or(&tail[n..]);
    }
    text.split("\"t\":\"")
        .skip(1)
        .filter_map(|r| r.split('"').next())
        .map(str::to_string)
        .collect()
}

/// A1 (live surfaces): 16 projects, idle and active alternately, each
/// hold two `orders` subscriptions from the beginning (the first rides
/// the direct path, the second the hub). Those 16 and 16 unwatched
/// projects append live records, then each watched project appends a
/// fence. Every transcript up to its fence carries exactly its own
/// project's markers in order: history, live records, fence. Every
/// project's watch list holds exactly its own definitions, and a
/// neighbour's matching append never wakes a watch wait.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn live_subscribers_and_watches_see_only_their_project() {
    let cell = open_cell(CellSpec::open(cell_scale())).await;
    cell.create_everywhere().await;
    let ledger = seed(&cell).await;
    let watched = spread(cell.projects, 16, [0, 3]);
    let mut subs = Vec::new();
    for &i in &watched {
        for _ in 0..2 {
            let q = "?cursor=beginning";
            let mut sock = rig_sse(cell.addr, "orders", &cell.bearers[i], q, None).await;
            let (head, _) = hub_sse_collect(&mut sock, 10, |t| t.contains("upToDate")).await;
            assert!(head.contains(" 200 "), "{} subscribe: {head}", project(i));
            subs.push((i, sock, head));
        }
    }
    let writers: Vec<usize> = watched
        .iter()
        .copied()
        .chain(spread(cell.projects, 16, [5, 5]))
        .collect();
    let live = |i: usize| -> Vec<Value> {
        (LIVE..LIVE + 2)
            .map(|q| record(i, "orders", "", q))
            .collect()
    };
    for &i in &writers {
        append_all(&cell, i, "orders", "", &live(i)).await;
    }
    for &i in &watched {
        append_all(&cell, i, "orders", "", &[record(i, "orders", "", FENCE)]).await;
    }
    for (i, mut sock, head) in subs {
        let fence = marker(&record(i, "orders", "", FENCE));
        let (rest, _) = hub_sse_collect(&mut sock, 10, |t| t.contains(&fence)).await;
        let want: Vec<String> = ledger
            .of(i, 0, 0)
            .into_iter()
            .chain(live(i))
            .map(|v| marker(&v))
            .chain([fence])
            .collect();
        assert_eq!(
            sse_markers(&format!("{head}{rest}")),
            want,
            "{}",
            project(i)
        );
    }
    watches_stay_inside_their_project(&cell, (watched[0], watched[1])).await;
    engine_shutdown(&cell.state).await;
}

/// Every project creates `w` with the shared watch `by-customer` and its
/// own `own-<i>`, and lists exactly those two. Then `b` waits on key
/// `c42` from its current cursor: `a`'s matching append on its own `w`
/// does not invalidate it, and `b`'s own does.
async fn watches_stay_inside_their_project(cell: &Cell, (a, b): (usize, usize)) {
    for_each(cell.projects, |i| async move {
        let body = format!(
            r#"{{"format":{{"kind":"json"}},"watches":[{{"name":"by-customer","fields":["/customerId"]}},{{"name":"own-{i}","fields":["/x"]}}]}}"#
        );
        let (st, _, b) = cell.call(i, "PUT", "/v1/streams/w", body.as_bytes()).await;
        assert_eq!(st, 201, "{} w: {}", project(i), String::from_utf8_lossy(&b));
        let v = cell.json(i, "GET", "/v1/streams/w/watches").await;
        let names: Vec<&str> = v["watches"]
            .as_array()
            .unwrap()
            .iter()
            .map(|w| w["name"].as_str().unwrap())
            .collect();
        assert_eq!(names, ["by-customer", &format!("own-{i}")], "{}", project(i));
    })
    .await;
    let fields = ["/customerId".to_string()];
    let khex = crate::product::watch_key_hex("by-customer", &fields, &["\"c42\"".to_string()]);
    let wait = format!("/v1/streams/w/watches/by-customer/keys/{khex}");
    // `b`'s position before anything touched `c42` (a wait that ends at
    // once); each wait then resumes from the previous answer's cursor,
    // so no outcome depends on when a wait parks.
    let start = cell
        .json(b, "GET", &format!("{wait}?cursor=now&timeoutMs=1"))
        .await;
    let mut cursor = start["cursor"].as_str().unwrap().to_string();
    for (writer, woken) in [(a, false), (b, true)] {
        let touch = br#"{"customerId":"c42"}"#;
        let (st, _, _) = cell
            .call(writer, "POST", "/v1/streams/w/records", touch)
            .await;
        assert_eq!(st, 200, "{} touch", project(writer));
        let path = format!("{wait}?cursor={cursor}&timeoutMs=1500");
        let answer = cell.json(b, "GET", &path).await;
        let seen = (project(b), project(writer));
        assert_eq!(answer["invalidated"], woken, "{seen:?}: {answer}");
        cursor = answer["cursor"].as_str().unwrap().to_string();
    }
}

/// Every key of every name, read once from the beginning.
async fn read_every_key_once(cell: &Cell, i: usize) {
    for name in NAMES {
        for key in KEYS {
            let path = format!("/v1/streams/{name}/records{}", key_query(key));
            cell.json(i, "GET", &path).await;
        }
    }
}

/// Drain the meters into `_usage` and roll the ledger up to its end.
async fn roll_up(state: &std::sync::Arc<crate::http::AppState>) {
    state.billing.reads().seal_if_aged(0);
    for _ in 0..400 {
        if crate::billing::drain_once(state).await.unwrap() == 0 {
            break;
        }
    }
    for _ in 0..400 {
        if crate::billing::rollup_step(state).await.unwrap() == 0 {
            break;
        }
    }
}

/// A3: after the A1 workload and one read of every key, drain and roll
/// up. Every project's usage equals its ledger exactly (ingest records
/// and payload bytes, append requests, read records and payload bytes)
/// under its own workspace; a neighbour's usage path is `404
/// unknown_project`; each workspace's rows sum exactly its projects'
/// ledgers and hold no other project's row; nothing bills to the
/// deployment account; and the month reconciles.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn books_are_exact_per_project_and_workspace() {
    let _clock = crate::billing::billing_clock_lock().read().await;
    let cell = open_cell(CellSpec::open(cell_scale())).await;
    cell.create_everywhere().await;
    let ledger = seed(&cell).await;
    for_each(cell.projects, |i| read_every_key_once(&cell, i)).await;
    roll_up(&cell.state).await;
    for_each(cell.projects, |i| {
        usage_matches_the_ledger(&cell, &ledger, i)
    })
    .await;
    let (y, m) = crate::billing::utc_year_month(crate::billing::billing_now_ms());
    let month = crate::billing::month_str(y, m);
    let rollup = cell.state.rollup.get().unwrap();
    let mut sums = vec![(0u64, 0u64); WORKSPACES];
    let mut want = vec![(0u64, 0u64); WORKSPACES];
    for p in 0..cell.projects {
        let (records, bytes) = ledger.totals(p);
        want[p % WORKSPACES].0 += records;
        want[p % WORKSPACES].1 += bytes;
        for (w, sum) in sums.iter_mut().enumerate() {
            let row = rollup
                .project_row(&month, &workspace(w), &project(p))
                .await
                .unwrap();
            assert_eq!(
                row.is_some(),
                w == p % WORKSPACES,
                "{} row under {}",
                project(p),
                workspace(w)
            );
            let row = row.unwrap_or_default();
            sum.0 += row.ingest_records;
            sum.1 += row.ingest_bytes;
        }
        let deployment = cell.state.deployment.account_id();
        let row = rollup
            .project_row(&month, deployment, &project(p))
            .await
            .unwrap();
        assert!(
            row.is_none(),
            "{} billed the deployment account",
            project(p)
        );
    }
    assert_eq!(sums, want, "workspace sums");
    let rep = rollup.reconcile_month(&month).await.unwrap();
    assert!(rep.ok, "books must balance: {:?}", rep.mismatches);
    assert_eq!(rep.projects, cell.projects, "{rep:?}");
    engine_shutdown(&cell.state).await;
}

/// Project `i`'s usage answer against its ledger, and its probe of a
/// neighbour's usage path.
async fn usage_matches_the_ledger(cell: &Cell, ledger: &Ledger, i: usize) {
    let v = cell
        .json(i, "GET", &format!("/v1/projects/{}/usage", project(i)))
        .await;
    let (records, bytes) = ledger.totals(i);
    let own = workspace(i);
    let got = (
        v["accountId"].as_str(),
        v["ingestRecords"].as_u64(),
        v["ingestPayloadBytes"].as_u64(),
        v["appendRequests"].as_u64(),
        v["readRecords"].as_u64(),
        v["readPayloadBytes"].as_u64(),
    );
    let want = (
        Some(own.as_str()),
        Some(records),
        Some(bytes),
        Some(ledger.append_requests(i)),
        Some(records),
        Some(bytes),
    );
    assert_eq!(got, want, "{} usage: {v}", project(i));
    let neighbour = project((i + 1) % cell.projects);
    let (st, _, b) = cell
        .call(i, "GET", &format!("/v1/projects/{neighbour}/usage"), b"")
        .await;
    let v: Value = serde_json::from_slice(&b).unwrap_or_default();
    assert_eq!(
        (st, v["error"]["code"].as_str()),
        (404, Some("unknown_project")),
        "{} probing {neighbour}: {v}",
        project(i)
    );
    let v = cell
        .json(i, "GET", "/v1/streams/orders/usage/current")
        .await;
    let orders = ledger.stream(i, 0).len() as u64;
    assert_eq!(
        v["ingestRecords"].as_u64(),
        Some(orders),
        "{} orders usage: {v}",
        project(i)
    );
}

/// The eight hostile projects (index 8h + 6): hostile `h` floods one
/// quota field.
fn hostile(i: usize) -> Option<usize> {
    (i % 8 == 6 && i / 8 < 8).then_some(i / 8)
}

/// One tight field per hostile project (Q-hostile).
fn tight(h: usize) -> ProjectQuotas {
    let mut q = ProjectQuotas::default();
    match h {
        0 => q.requests_per_sec = 5,
        1 => q.append_bytes_per_sec = 1_000,
        2 => q.append_records_per_sec = 5,
        3 => q.read_bytes_per_sec = 500,
        4 => q.max_inflight_requests = 2,
        5 => q.max_live_subscriptions = 2,
        6 => q.max_streams = 4,
        _ => q.queued_append_bytes = 200,
    }
    q
}

/// Every other project carries every field, configured well above its
/// paced load: configured quotas, never reached.
fn roomy() -> ProjectQuotas {
    ProjectQuotas {
        requests_per_sec: 1_000,
        append_bytes_per_sec: 1_000_000,
        append_records_per_sec: 1_000,
        read_bytes_per_sec: 4_000_000,
        max_inflight_requests: 64,
        max_live_subscriptions: 16,
        max_streams: 64,
        queued_append_bytes: 1_000_000,
    }
}

/// The typed refusal each hostile field answers with.
const REFUSAL: [&str; 8] = [
    "project_rate_limit",
    "project_rate_limit",
    "project_rate_limit",
    "project_rate_limit",
    "project_concurrency_limit",
    "project_concurrency_limit",
    "stream_limit",
    "queued_bytes",
];

/// A2: the eight hostile projects flood their tight field while every
/// project at index 8k + 1 runs a paced load under configured quotas.
/// Every hostile refusal is the typed 429 of its field and lands on the
/// hostile project, each hostile flood is refused at least once, the
/// paced projects see no refusal at all, the journal holds exactly the
/// refusals each project saw, and afterwards up to 100 projects that
/// never spoke are admitted on their first request.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn quotas_refuse_only_their_project() {
    let mut spec = CellSpec::open(cell_scale());
    spec.quotas = |i| hostile(i).map_or_else(roomy, tight);
    let cell = open_cell(spec).await;
    let paced: Vec<usize> = (0..cell.projects).filter(|i| i % 8 == 1).collect();
    let hostiles: Vec<usize> = (0..cell.projects)
        .filter(|i| hostile(*i).is_some())
        .collect();
    let floods = futures_util::future::join_all(hostiles.iter().map(|&i| flood(&cell, i)));
    let calm = futures_util::future::join_all(paced.iter().map(|&i| paced_load(&cell, i)));
    let (floods, calm) = futures_util::future::join(floods, calm).await;
    let mut seen = std::collections::BTreeMap::new();
    for (&i, answers) in hostiles.iter().zip(floods) {
        let want = REFUSAL[hostile(i).unwrap()];
        let refused: Vec<_> = answers.iter().filter(|(st, _)| *st >= 300).collect();
        assert!(!refused.is_empty(), "{} was never refused", project(i));
        for (st, code) in &refused {
            assert_eq!((*st, code.as_deref()), (429, Some(want)), "{}", project(i));
        }
        seen.insert(project(i), refused.len());
    }
    for (&i, answers) in paced.iter().zip(calm) {
        assert!(
            answers.iter().all(|(st, _)| *st < 300),
            "{} refused: {answers:?}",
            project(i)
        );
    }
    let journaled = journal(&cell.state).await;
    let mut counted = std::collections::BTreeMap::new();
    for e in journaled.iter().filter(|e| e["status"] == 429) {
        *counted
            .entry(e["project_id"].as_str().unwrap().to_string())
            .or_insert(0) += 1;
    }
    assert_eq!(counted, seen, "journaled refusals per project");
    let silent: Vec<usize> = (0..cell.projects)
        .filter(|i| i % 8 != 1 && hostile(*i).is_none())
        .take(100)
        .collect();
    let (silent, shared) = (&silent, &cell);
    for_each(silent.len(), |j| async move {
        let i = silent[j];
        assert_eq!(
            shared.call(i, "GET", "/v1/streams", b"").await.0,
            200,
            "{}",
            project(i)
        );
    })
    .await;
    engine_shutdown(&cell.state).await;
}

/// Hostile project `i` floods its tight field; every answer, in order.
async fn flood(cell: &Cell, i: usize) -> Answers {
    let (st, _, b) = cell.call(i, "PUT", "/v1/streams/orders", CREATE).await;
    assert_eq!(st, 201, "{}: {}", project(i), String::from_utf8_lossy(&b));
    let records = "/v1/streams/orders/records";
    let (small, large) = (padded(40), padded(300));
    match hostile(i).unwrap() {
        0 => burst(cell, i, ("GET", records, b""), 30, true).await,
        1 => burst(cell, i, ("POST", records, &large), 12, false).await,
        2 => burst(cell, i, ("POST", records, &small), 20, false).await,
        3 => {
            burst(cell, i, ("POST", records, &padded(400)), 4, false).await;
            burst(cell, i, ("GET", records, b""), 20, false).await
        }
        4 => {
            let park = "/v1/streams/orders/records:long-poll?cursor=now&waitMs=1000";
            burst(cell, i, ("GET", park, b""), 12, true).await
        }
        5 => subscription_flood(cell, i, 6).await,
        6 => {
            let mut out = Vec::new();
            for j in 0..8 {
                let (st, _, b) = cell
                    .call(i, "PUT", &format!("/v1/streams/extra-{j}"), CREATE)
                    .await;
                out.push((st, error_code(&b)));
            }
            out
        }
        _ => {
            let mut out = burst(cell, i, ("POST", records, &small), 10, false).await;
            out.extend(burst(cell, i, ("POST", records, &large), 10, false).await);
            out
        }
    }
}

/// Open `count` live subscriptions at once as project `i` and hold them
/// until every answer head is in.
async fn subscription_flood(cell: &Cell, i: usize, count: usize) -> Answers {
    let mut socks = Vec::new();
    for _ in 0..count {
        socks.push(rig_sse(cell.addr, "orders", &cell.bearers[i], "", None).await);
    }
    let mut out = Vec::new();
    for sock in &mut socks {
        let (st, _) = sse_head(sock).await;
        let code = if st == 200 {
            None
        } else {
            let (rest, _) = hub_sse_collect(sock, 2, |t| t.contains("\"code\"")).await;
            error_code(rest.as_bytes())
        };
        out.push((st, code));
    }
    out
}

/// A paced project's ordinary load: a stream, then ten appends and ten
/// reads, 10 ms apart.
async fn paced_load(cell: &Cell, i: usize) -> Answers {
    let mut out = Vec::new();
    let (st, _, b) = cell.call(i, "PUT", "/v1/streams/orders", CREATE).await;
    out.push((st, error_code(&b)));
    for q in 0..10 {
        let body = record(i, "orders", "", q).to_string();
        let (st, _, b) = cell
            .call(i, "POST", "/v1/streams/orders/records", body.as_bytes())
            .await;
        out.push((st, error_code(&b)));
        let (st, _, b) = cell.call(i, "GET", "/v1/streams/orders/records", b"").await;
        out.push((st, error_code(&b)));
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    out
}
