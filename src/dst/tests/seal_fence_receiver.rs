//! The seal-fence receiver (NEXT-WORK §5 F1-a, edge change #88):
//! `POST /v1/internal/seal-fence/{*name}?fence_to=` at the segment's owner.
//! It demands its exact operation, re-derives every target fact from its
//! own registry, fences only a reserved generation, refuses on a non-owner
//! without opening a writer, and answers the owner committer's
//! closed-report only after the fence is durable.

use super::fixture_auth::{FLEET_KID, sr_rig, sr2_workload_jwt};
use super::fixture_http::{engine_shutdown, http_rig, http_rig_named};
use super::fixture_requests::hreq;
use super::fixture_storage::mem;
use std::sync::Arc;

type Headers = Vec<(String, String)>;

const FLEET: &str = "Bearer dst-internal-token";

/// The counter every scenario raises the stream's reservations to.
const RESERVED: u64 = 7;

/// A one-record raw stream whose seal counter stands at [`RESERVED`], as a
/// takeover's reservation leaves it; the descriptor as the registry holds it.
async fn stage(
    state: &Arc<crate::http::AppState>,
    addr: std::net::SocketAddr,
    name: &str,
    auth: &str,
) -> crate::registry::StreamDesc {
    let headers = [
        ("content-type", "application/json"),
        ("authorization", auth),
    ];
    let path = format!("/v1/stream/{name}");
    let (st, _, _) = hreq(addr, "PUT", &path, &headers, br#"[{"n":0}]"#).await;
    assert!(st == 200 || st == 201, "stage {name}: {st}");
    let sref = state.deployment.raw_adapter_sref(name);
    let raised = state
        .registry
        .cas_update(&sref, |d| {
            d.seal_gen_counter = RESERVED;
            true
        })
        .await
        .unwrap();
    assert!(raised, "the counter of {name} was not raised");
    state.registry.invalidate(&sref);
    state.registry.get(&sref).await.unwrap().unwrap()
}

fn target(desc: &crate::registry::StreamDesc) -> crate::product::InternalTarget {
    crate::product::InternalTarget::of(desc, 0).unwrap()
}

fn target_headers(target: &crate::product::InternalTarget) -> Headers {
    target
        .headers()
        .iter()
        .map(|(k, v)| ((*k).to_string(), v.clone()))
        .collect()
}

/// POST the fence with `query` (`?fence_to=...` or nothing).
async fn post_fence(
    addr: std::net::SocketAddr,
    name: &str,
    query: &str,
    auth: &str,
    target: &Headers,
) -> (u16, std::collections::HashMap<String, String>, Vec<u8>) {
    let mut headers: Vec<(&str, &str)> = target
        .iter()
        .map(|(k, v)| (k.as_str(), v.as_str()))
        .collect();
    headers.push(("authorization", auth));
    let path = format!("/v1/internal/seal-fence/{name}{query}");
    hreq(addr, "POST", &path, &headers, b"").await
}

/// The receiver's two verdicts, byte for byte.
const OPEN: &str = r#"{"closed":false}"#;
const CLOSED: &str = r#"{"closed":true}"#;

fn answer(st: u16, body: &[u8]) -> (u16, String) {
    (st, String::from_utf8_lossy(body).into_owned())
}

fn error_code(body: &[u8]) -> String {
    serde_json::from_slice::<serde_json::Value>(body)
        .ok()
        .and_then(|v| v["error"]["code"].as_str().map(str::to_string))
        .unwrap_or_default()
}

/// The segment's DURABLE fence row (None = never fenced).
async fn fence_row(engine: &crate::shard::ShardEngine, identity: [u8; 16]) -> Option<u64> {
    let raw = engine
        .db
        .get(crate::shard::seal_fence_key(&identity))
        .await
        .unwrap()?;
    Some(u64::from_le_bytes(raw[..8].try_into().unwrap()))
}

async fn owner_engine(
    state: &Arc<crate::http::AppState>,
    desc: &crate::registry::StreamDesc,
) -> Arc<crate::shard::ShardEngine> {
    let route = desc.segment_route_by_id(0).unwrap();
    for _ in 0..100 {
        if let Some(engine) = state.engine_for_scaler(&route).await {
            return engine;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    panic!("the owner never resolved its engine");
}

/// Entered-proof (as in durability_fences): the request reached the engine
/// and is STILL pending after a grace period.
async fn held<T>(
    engine: &crate::shard::ShardEngine,
    entered: u64,
    task: &tokio::task::JoinHandle<T>,
    what: &str,
) {
    for _ in 0..500 {
        if engine.appends_enqueued() >= entered {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    assert!(
        engine.appends_enqueued() >= entered,
        "{what}: the request never reached the engine"
    );
    tokio::time::sleep(std::time::Duration::from_millis(150)).await;
    assert!(!task.is_finished(), "{what} concluded before durability");
}

/// Wrong ingress, credentials: only the fleet token or a workload JWT
/// naming exactly `seal-fence` may place a fence, and a refused request
/// reaches neither the registry nor the engine.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn seal_fence_receiver_demands_its_exact_operation() {
    let scopes = "streams.create streams.records.append streams.records.read";
    let (state, addr, _tok) = sr_rig("proj-sfr", "ws_sfr", "c_sfr", "sfr-1", scopes).await;
    let desc = stage(&state, addr, "sfr", FLEET).await;
    let target = target(&desc);
    let th = target_headers(&target);
    let engine = owner_engine(&state, &desc).await;
    let now = crate::shard::now_ms() / 1000;
    let token = |ops: &[&str]| format!("Bearer {}", sr2_workload_jwt(FLEET_KID, ops, now));
    let query = format!("?fence_to={RESERVED}");
    for ops in [
        &[][..],
        &["segment-read"][..],
        &["segment-close"][..],
        &["everything"][..],
    ] {
        let enqueued = engine.appends_enqueued();
        let (st, _, body) = post_fence(addr, "sfr", &query, &token(ops), &th).await;
        assert_eq!(
            st, 401,
            "seal-fence must demand its exact operation ({ops:?})"
        );
        assert_eq!(error_code(&body), "unauthorized");
        assert_eq!(
            engine.appends_enqueued(),
            enqueued,
            "{ops:?} reached the engine"
        );
        assert_eq!(fence_row(&engine, target.identity).await, None);
    }
    let fence = token(&["seal-fence"]);
    let (st, _, body) = post_fence(addr, "sfr", &query, &fence, &th).await;
    assert_eq!(answer(st, &body), (200, OPEN.into()));
    assert_eq!(fence_row(&engine, target.identity).await, Some(RESERVED));
    // Least privilege both ways: a seal-fence token cannot close a segment.
    let mut headers: Vec<(&str, &str)> = th.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect();
    headers.push(("authorization", &fence));
    let path = "/v1/internal/segment-close/sfr?seg_id=0&seal_gen=0";
    let (st, _, _) = hreq(addr, "POST", path, &headers, b"").await;
    assert_eq!(st, 401, "a seal-fence token closed a segment");
    engine_shutdown(&state).await;
}

/// Wrong ingress, target: every refusal of the target or the generation
/// is decided before the engine, and only the bound target at a reserved
/// generation places the fence.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn seal_fence_receiver_binds_target_and_generation() {
    let (state, addr) = http_rig(mem()).await;
    let desc = stage(&state, addr, "sfb", FLEET).await;
    let bound = target(&desc);
    let engine = owner_engine(&state, &desc).await;
    let reserved = format!("?fence_to={RESERVED}");
    let corrupt = |edit: fn(&mut crate::product::InternalTarget)| {
        let mut t = target(&desc);
        edit(&mut t);
        target_headers(&t)
    };
    let no_epoch: Headers = target_headers(&bound)
        .into_iter()
        .filter(|(k, _)| !k.ends_with("epoch"))
        .collect();
    let refusals: Vec<(&str, Headers, String, u16, &str)> = vec![
        (
            "another incarnation",
            corrupt(|t| t.stream_epoch[0] ^= 0xff),
            reserved.clone(),
            409,
            "stale_target",
        ),
        (
            "another segment",
            corrupt(|t| t.seg_id = 7),
            reserved.clone(),
            409,
            "stale_target",
        ),
        (
            "another identity",
            corrupt(|t| t.identity = [0; 16]),
            reserved.clone(),
            409,
            "stale_target",
        ),
        (
            "no epoch",
            no_epoch,
            reserved.clone(),
            400,
            "invalid_target",
        ),
        (
            "another project",
            corrupt(|t| t.project_id = crate::tenant::ProjectId::new("proj-other").unwrap()),
            reserved.clone(),
            404,
            "not_found",
        ),
        (
            "an unreserved generation",
            target_headers(&bound),
            format!("?fence_to={}", RESERVED + 1),
            409,
            "fence_unreserved",
        ),
        (
            "no generation",
            target_headers(&bound),
            String::new(),
            400,
            "",
        ),
        (
            "a non-numeric generation",
            target_headers(&bound),
            "?fence_to=seven".into(),
            400,
            "",
        ),
    ];
    for (what, headers, query, status, code) in refusals {
        let enqueued = engine.appends_enqueued();
        let (st, _, body) = post_fence(addr, "sfb", &query, FLEET, &headers).await;
        assert_eq!(st, status, "{what}: {}", String::from_utf8_lossy(&body));
        if !code.is_empty() {
            assert_eq!(error_code(&body), code, "{what}");
        }
        assert_eq!(
            engine.appends_enqueued(),
            enqueued,
            "{what} reached the engine"
        );
        assert_eq!(
            fence_row(&engine, bound.identity).await,
            None,
            "{what} fenced"
        );
    }
    let (st, _, body) = post_fence(addr, "sfb", &reserved, FLEET, &target_headers(&bound)).await;
    assert_eq!(answer(st, &body), (200, OPEN.into()));
    assert_eq!(fence_row(&engine, bound.identity).await, Some(RESERVED));
    engine_shutdown(&state).await;
}

/// Wrong ingress, owner: a receiver the ring does not assign the shard to
/// answers the directory's redirect and opens no writer, however often it
/// is asked.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn seal_fence_receiver_on_a_non_owner_redirects_without_opening_a_writer() {
    let (state, addr) = http_rig_named(mem(), "inst-a").await;
    let desc = stage(&state, addr, "sfo", FLEET).await;
    let th = target_headers(&target(&desc));
    // The ring moves the shard to a live peer (an override counts only
    // for an active member).
    let ring = vec!["inst-a".to_string(), "elsewhere".to_string()];
    state.ownership.set_ring_active(ring);
    state.ownership.set_override("00", "elsewhere");
    let query = format!("?fence_to={RESERVED}");
    for attempt in 0..2 {
        let (st, headers, body) = post_fence(addr, "sfo", &query, FLEET, &th).await;
        assert_eq!(
            st,
            409,
            "attempt {attempt}: {}",
            String::from_utf8_lossy(&body)
        );
        assert_eq!(error_code(&body), "not_ring_owner");
        assert_eq!(
            headers.get("streams-replay-to").map(String::as_str),
            Some("elsewhere")
        );
        let open: Vec<String> = state
            .shards
            .engines_by_prefix()
            .into_iter()
            .map(|(p, _)| p)
            .collect();
        assert!(
            !open.contains(&"00".to_string()),
            "a non-owner holds a writer: {open:?}"
        );
    }
    engine_shutdown(&state).await;
}

/// Crash around durability, the receiver's half: the 200 waits for the
/// fence's group to be durable; `false` is then a barrier (a close below
/// the fence is superseded, one at it closes), and a repeat at the same or
/// a lower generation reports the close without lowering the fence.
#[expect(
    clippy::disallowed_methods,
    reason = "seal-fence durability fixture; the fence request is proven to have entered the engine and to be pending behind the held dispatch, and it is joined after the release; running it inline would deadlock behind the barrier"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn seal_fence_receiver_answers_after_durability_and_is_idempotent() {
    let (state, addr) = http_rig(mem()).await;
    let desc = stage(&state, addr, "sfd", FLEET).await;
    let bound = target(&desc);
    let th = target_headers(&bound);
    let engine = owner_engine(&state, &desc).await;
    let query = format!("?fence_to={RESERVED}");
    let guard = engine.test_hold_dispatch().await;
    let entered = engine.appends_enqueued() + 1;
    let (q, h) = (query.clone(), th.clone());
    let fence = tokio::spawn(async move { post_fence(addr, "sfd", &q, FLEET, &h).await });
    held(&engine, entered, &fence, "the fence").await;
    drop(guard);
    let (st, _, body) = fence.await.unwrap();
    assert_eq!(answer(st, &body), (200, OPEN.into()));
    assert_eq!(fence_row(&engine, bound.identity).await, Some(RESERVED));
    let close = |generation: u64| {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let req = crate::shard::CloseReq {
            hash: bound.identity,
            generation: Some(generation),
            resp: tx,
        };
        engine.try_close(req).unwrap();
        rx
    };
    let below = close(RESERVED - 1).await.unwrap();
    assert!(
        matches!(below, Err(crate::shard::AppendErr::SealSuperseded)),
        "{below:?}"
    );
    let at = close(RESERVED).await.unwrap();
    assert!(
        at.is_ok_and(|ack| ack.closed),
        "a close at the fence did not close"
    );
    for fence_to in [RESERVED, 3] {
        let query = format!("?fence_to={fence_to}");
        let (st, _, body) = post_fence(addr, "sfd", &query, FLEET, &th).await;
        assert_eq!(answer(st, &body), (200, CLOSED.into()), "{fence_to}");
        assert_eq!(fence_row(&engine, bound.identity).await, Some(RESERVED));
    }
    engine_shutdown(&state).await;
}

/// Crash around durability, a failed write: a fence whose group failed is
/// never reported, nothing durable backs it, and the retry places it. The
/// group failpoint trips on a client write of the segment, so an append of
/// the same stream shares the fence's held group.
#[expect(
    clippy::disallowed_methods,
    reason = "seal-fence failed-group fixture; the append and the fence are proven to have entered the held committer before the failure is armed, and both are joined after the release; running either inline would deadlock behind the held commit"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn seal_fence_receiver_never_answers_closed_for_a_failed_group() {
    let (state, addr) = http_rig(mem()).await;
    let desc = stage(&state, addr, "sff", FLEET).await;
    let bound = target(&desc);
    let th = target_headers(&bound);
    let engine = owner_engine(&state, &desc).await;
    let query = format!("?fence_to={RESERVED}");
    let hold = engine.test_hold_commit().await;
    let entered = engine.appends_enqueued() + 1;
    let append = tokio::spawn(async move {
        let headers = [
            ("content-type", "application/json"),
            ("authorization", FLEET),
        ];
        hreq(addr, "POST", "/v1/stream/sff", &headers, br#"[{"n":1}]"#).await
    });
    held(&engine, entered, &append, "the append").await;
    let (q, h) = (query.clone(), th.clone());
    let fence = tokio::spawn(async move { post_fence(addr, "sff", &q, FLEET, &h).await });
    held(&engine, entered + 1, &fence, "the fence").await;
    engine.fail_next_group_for(bound.identity);
    drop(hold);
    let (appended, _, _) = append.await.unwrap();
    assert!(
        !(200..300).contains(&appended),
        "the failed append: {appended}"
    );
    let (st, _, body) = fence.await.unwrap();
    assert_eq!(
        st,
        503,
        "a failed group answered: {}",
        String::from_utf8_lossy(&body)
    );
    assert_eq!(error_code(&body), "fence_unconfirmed");
    assert_eq!(
        engine.group_failures_tripped(),
        1,
        "the failpoint never fired"
    );
    let reopened = owner_engine(&state, &desc).await;
    assert_eq!(
        fence_row(&reopened, bound.identity).await,
        None,
        "a failed group fenced"
    );
    let mut retried = (0, String::new());
    for _ in 0..50 {
        let (st, _, body) = post_fence(addr, "sff", &query, FLEET, &th).await;
        retried = answer(st, &body);
        if st != 503 {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    assert_eq!(retried, (200, OPEN.into()));
    let reopened = owner_engine(&state, &desc).await;
    assert_eq!(fence_row(&reopened, bound.identity).await, Some(RESERVED));
    engine_shutdown(&state).await;
}
