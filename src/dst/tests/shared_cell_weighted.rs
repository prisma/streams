//! A shared cell of every tenant class (layout 6A package 1, DESIGN-6A
//! §4): streams and queues of all sizes on one cell, weighted shares
//! (edge changes #196-#198) beside overload parity for queues (#189-#195).
//! The module's name puts it in the mt-cert job's Layer A leg
//! (`dst::dst_tests::shared_cell_`), which runs it at 1,000 projects
//! (`MT_CERT_PROJECTS=1000`, `cell_scale`). Every assertion is exact: the
//! wire answers, each tenant's stored records and its queue's counters.

use super::fixture_cell::{CREATE, Cell, CellSpec, cell_scale, error_code, open_cell};
use super::fixture_http::engine_shutdown;
use crate::auth::ceiling::SharedBounds;
use crate::project_policy::ProjectQuotas;

/// The tenants of the mixed cell, by project index.
const M: usize = 0;
const S: usize = 1;
const T: usize = 2;
const QUEUE: usize = 3;
const FLOODER: usize = 4;

/// The rig's shed line (MiB), as `fair_shed.rs` sets it.
const LINE_MB: u64 = 1000;
/// Rounds of each pressure phase: one operation of every tenant each.
const ROUNDS: usize = 5;
/// The in-flight cap the in-flight phase sets.
const CAP: i64 = 8;

type Answer = (u16, Option<String>);

async fn ask(cell: &Cell, i: usize, method: &str, path: &str, body: &[u8]) -> Answer {
    let (status, _, b) = cell.call(i, method, path, body).await;
    (status, error_code(&b))
}

/// One append of tenant `i` to its stream `orders`: M's carry 50,000
/// bytes, every other tenant's a few.
async fn append(cell: &Cell, i: usize) -> Answer {
    let body = if i == M {
        format!("{{\"pad\":\"{}\"}}", "m".repeat(50_000))
    } else {
        format!("{{\"t\":{i}}}")
    };
    ask(
        cell,
        i,
        "POST",
        "/v1/streams/orders/records",
        body.as_bytes(),
    )
    .await
}

async fn send(cell: &Cell, i: usize) -> Answer {
    let body = br#"{"body":{"job":1}}"#;
    ask(cell, i, "POST", "/v1/queues/jobs/messages", body).await
}

/// The queue tenant's work of one round: a send, a claim that takes one
/// message and the ack of it.
async fn queue_round(cell: &Cell) -> [Answer; 3] {
    let sent = send(cell, QUEUE).await;
    let path = "/v1/queues/jobs/groups/default:claim";
    let (status, _, b) = cell.call(QUEUE, "POST", path, br#"{"max":1}"#).await;
    let claimed: serde_json::Value = serde_json::from_slice(&b).unwrap_or_default();
    let token = claimed["messages"][0]["token"].as_str().unwrap_or("none");
    let ops = format!(r#"{{"ops":[{{"token":"{token}","op":"ack"}}]}}"#);
    let path = "/v1/queues/jobs/groups/default:settle";
    let (settled, _, sb) = cell.call(QUEUE, "POST", path, ops.as_bytes()).await;
    let outcome: serde_json::Value = serde_json::from_slice(&sb).unwrap_or_default();
    let acked = outcome["results"][0]["outcome"] == "ok";
    [
        sent,
        (status, error_code(&b)),
        (settled, (!acked).then(|| "not_acked".to_string())),
    ]
}

/// One round of every compliant tenant: M, S and T append, the queue
/// tenant sends, claims and acks. What each was answered.
async fn compliant_round(cell: &Cell) -> Vec<Answer> {
    let mut answers = Vec::new();
    for i in [M, S, T] {
        answers.push(append(cell, i).await);
    }
    answers.extend(queue_round(cell).await);
    answers
}

/// What a compliant round is answered when every operation is served.
fn served() -> Vec<Answer> {
    let ok = (200, None);
    vec![
        ok.clone(),
        ok.clone(),
        ok.clone(),
        (201, None),
        ok.clone(),
        ok,
    ]
}

/// The flooder's append and send, each refused with `code`.
async fn flooder_refused(cell: &Cell, code: &str, phase: &str) {
    let refused = (429, Some(code.to_string()));
    assert_eq!(append(cell, FLOODER).await, refused, "{phase}: its append");
    assert_eq!(send(cell, FLOODER).await, refused, "{phase}: its send");
}

/// The records tenant `i` stored in `orders`.
async fn stored(cell: &Cell, i: usize) -> usize {
    let read = cell
        .json(i, "GET", "/v1/streams/orders/records?maxBytes=8388608")
        .await;
    read.as_array().map_or(0, Vec::len)
}

/// Tenant `i`'s queue counters: (sent, acked, queued).
async fn queue_counts(cell: &Cell, i: usize) -> (u64, u64, u64) {
    let v = cell.json(i, "GET", "/v1/queues/jobs").await;
    let s = &v["stats"];
    let n = |x: &serde_json::Value| x.as_u64().unwrap();
    (
        n(&s["counters"]["sent"]),
        n(&s["counters"]["acked"]),
        n(&s["queuedMessages"]),
    )
}

/// One cell, shared 8 ways, of every class: M at weights (2, 5) appending
/// 50,000-byte records, S and T unit stream tenants, a unit queue tenant
/// that sends, claims and acks, and a unit flooder of appends and sends
/// (200 of the cell's 231 recent requests). Over the RSS shed line and at
/// the in-flight cap the flooder's appends and sends are refused (`429
/// project_memory_pressure`, `429 rate_limited`) and every other class is
/// served, M's large appends and the queue tenant's claims and acks
/// included; past the shed line's hard line the backstop sheds every
/// class's writes alike (`429 rate_limited`) while claims are still
/// served; under the line everything is served again. Each tenant stores
/// exactly what it was answered for, and the queue's counters agree.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_mixed_cell_under_pressure_sheds_its_flooder_and_starves_no_class() {
    let cell = mixed_cell().await;
    let admission = &cell.state.admission;
    admission.set_rss_shed_mb(LINE_MB);
    admission.record_rss_mb(LINE_MB + 1);
    for round in 0..ROUNDS {
        assert_eq!(
            compliant_round(&cell).await,
            served(),
            "over the line, round {round}"
        );
        flooder_refused(&cell, "project_memory_pressure", "over the line").await;
    }
    admission.record_rss_mb(0);
    admission.set_max_inflight(CAP);
    admission.add_inflight_for_test(CAP);
    for round in 0..ROUNDS {
        assert_eq!(
            compliant_round(&cell).await,
            served(),
            "at the cap, round {round}"
        );
        flooder_refused(&cell, "rate_limited", "at the cap").await;
    }
    admission.add_inflight_for_test(-CAP);
    admission.record_rss_mb(LINE_MB * 9 / 8 + 1);
    let shed = (429, Some("rate_limited".to_string()));
    for i in [M, S, T] {
        assert_eq!(
            append(&cell, i).await,
            shed,
            "past the hard line: tenant {i}"
        );
    }
    assert_eq!(send(&cell, QUEUE).await, shed, "past the hard line: a send");
    let claim = ask(
        &cell,
        QUEUE,
        "POST",
        "/v1/queues/jobs/groups/default:claim",
        b"{}",
    );
    assert_eq!(claim.await.0, 200, "claims are not shed");
    admission.record_rss_mb(0);
    assert_eq!(compliant_round(&cell).await, served(), "under the line");
    let rounds = 2 * ROUNDS + 1;
    let want = [
        (M, 20 + rounds),
        (S, 10 + rounds),
        (T, 1 + rounds),
        (FLOODER, 160),
    ];
    let mut got = Vec::new();
    for (i, _) in want {
        got.push((i, stored(&cell, i).await));
    }
    assert_eq!(
        got,
        want.to_vec(),
        "every served append stored once, no refused one"
    );
    let r = u64::try_from(rounds).unwrap();
    assert_eq!(
        queue_counts(&cell, QUEUE).await,
        (r, r, 0),
        "sent and acked"
    );
    assert_eq!(
        queue_counts(&cell, FLOODER).await,
        (40, 0, 40),
        "its refused sends stored nothing"
    );
    engine_shutdown(&cell.state).await;
}

/// The mixed cell: M at (2, 5), S, T, the queue tenant and the flooder,
/// each with its stream or queue, and the recent load under every line
/// (the flooder 160 appends and 40 sends; M 20, S 10, T 1 appends).
async fn mixed_cell() -> Cell {
    let mut spec = CellSpec::open(5);
    spec.cli = |cli| cli.project_share_k = 8;
    spec.bounds = SharedBounds {
        append_bytes_per_sec: 2_060_000,
        ..SharedBounds::default()
    };
    spec.quotas = |i| ProjectQuotas {
        request_weight: if i == M { 2 } else { 0 },
        byte_weight: if i == M { 5 } else { 0 },
        ..ProjectQuotas::default()
    };
    let cell = open_cell(spec).await;
    for i in [M, S, T, FLOODER] {
        let created = ask(&cell, i, "PUT", "/v1/streams/orders", CREATE).await;
        assert_eq!(created.0, 201, "tenant {i}");
    }
    for i in [QUEUE, FLOODER] {
        let created = ask(&cell, i, "PUT", "/v1/queues/jobs", b"").await;
        assert_eq!(created.0, 201, "tenant {i}'s queue");
    }
    for _ in 0..160 {
        assert_eq!(append(&cell, FLOODER).await, (200, None));
    }
    for _ in 0..40 {
        assert_eq!(send(&cell, FLOODER).await, (201, None));
    }
    for (i, n) in [(M, 20), (S, 10), (T, 1)] {
        for _ in 0..n {
            assert_eq!(append(&cell, i).await, (200, None), "tenant {i}");
        }
    }
    cell
}

/// `MT_CERT`-scale weighted certification: on a cell of `cell_scale()`
/// projects shared 8 ways over the 1 GiB profile's bounds, one M at (2, 5)
/// and one byte weight 2 among unit projects, every project's verified
/// quotas are its weighted ceilings exactly, and each holds them: M's
/// ten 128,000-byte appends are admitted, the byte-weight-2 project meets
/// its 515,000 B/s at its fifth and a unit project its 257,500 B/s at its
/// third (`429 project_rate_limit`), and every project reads only its own
/// records.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn every_project_on_a_weighted_cell_holds_its_own_weighted_ceilings() {
    let mut spec = CellSpec::open(cell_scale());
    spec.cli = |cli| cli.project_share_k = 8;
    spec.bounds = BOUNDS;
    spec.quotas = |i| ProjectQuotas {
        request_weight: if i == 0 { 2 } else { 0 },
        byte_weight: [5, 2].get(i).copied().unwrap_or(0),
        ..ProjectQuotas::default()
    };
    let cell = open_cell(spec).await;
    let now = crate::shard::now_ms() / 1000;
    for i in 0..cell.projects {
        let token = cell.bearers[i].trim_start_matches("Bearer ");
        let quotas = cell.svc.verify_customer(token, now).unwrap().quotas;
        assert_eq!(published(&quotas), expected(i), "project {i}");
    }
    let tenants = [0, 1, 2, cell.projects - 1];
    for i in tenants {
        let created = ask(&cell, i, "PUT", "/v1/streams/orders", CREATE).await;
        assert_eq!(created.0, 201, "project {i}");
    }
    let body = format!("{{\"pad\":\"{}\",\"p\":0}}", "x".repeat(128_000));
    let mut admitted = Vec::new();
    for i in tenants {
        let mut n = 0;
        for _ in 0..10 {
            let body = body.replace("\"p\":0", &format!("\"p\":{i}"));
            let (status, code) = ask(
                &cell,
                i,
                "POST",
                "/v1/streams/orders/records",
                body.as_bytes(),
            )
            .await;
            match (status, code.as_deref()) {
                (200, None) => n += 1,
                (429, Some("project_rate_limit")) => break,
                other => panic!("project {i}: {other:?}"),
            }
        }
        admitted.push(n);
    }
    assert_eq!(
        admitted,
        vec![10, 4, 2, 2],
        "M, byte weight 2, two unit projects"
    );
    for (i, n) in tenants.iter().zip(&admitted) {
        let read = cell
            .json(*i, "GET", "/v1/streams/orders/records?maxBytes=8388608")
            .await;
        let own = read
            .as_array()
            .unwrap()
            .iter()
            .filter(|r| r["p"].as_u64() == u64::try_from(*i).ok())
            .count();
        assert_eq!(
            (read.as_array().unwrap().len(), own),
            (*n, *n),
            "project {i} reads its own"
        );
    }
    engine_shutdown(&cell.state).await;
}

/// The 1 GiB shared-cell profile's bounds (deploy/profiles/shared-cell.env).
const BOUNDS: SharedBounds = SharedBounds {
    requests_per_sec: 1_411,
    append_bytes_per_sec: 2_060_000,
    read_bytes_per_sec: 30_000_000,
    inflight: 512,
    subscriptions: 1_200,
    streams: 65_536,
    queued_append_bytes: 512 << 20,
};

/// The bounded quotas and weights a verified principal carries.
fn published(q: &ProjectQuotas) -> [u64; 9] {
    [
        q.requests_per_sec,
        q.append_bytes_per_sec,
        q.read_bytes_per_sec,
        q.max_inflight_requests,
        q.max_live_subscriptions,
        q.max_streams,
        q.queued_append_bytes,
        q.request_weight,
        q.byte_weight,
    ]
}

/// What project `i` must be published at: M at (2, 5), project 1 at byte
/// weight 2, every other at the unit share.
fn expected(i: usize) -> [u64; 9] {
    match i {
        0 => [352, 1_287_500, 18_750_000, 128, 300, 16_384, 64 << 20, 2, 5],
        1 => [176, 515_000, 7_500_000, 64, 150, 8_192, 64 << 20, 1, 2],
        _ => [176, 257_500, 3_750_000, 64, 150, 8_192, 64 << 20, 1, 1],
    }
}
