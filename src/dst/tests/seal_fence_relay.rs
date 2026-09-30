//! The seal-fence sender (NEXT-WORK §5 F1-a, edge change #89): a takeover
//! coordinated on an instance that does not own the old final's segment
//! fences it at the owner over `POST /v1/internal/seal-fence`, and every
//! answer but the owner's parsed closed-report leaves the seal resumable.
//!
//! Two instances over one store: the ring is active on both and gives the
//! rig's shards to B, and A coordinates the takeover. A never opens a
//! writer for the stream's shard while B owns it. Every test arms a name
//! failpoint, so each holds `gap_lock`.

use super::fixture_failpoints::gap_lock;
use super::fixture_http::{
    HttpRigOptions, engine_shutdown, http_rig_build, http_rig_named, http_rig_named_at,
};
use super::fixture_requests::{PRISMA_KEY, hreq, preq};
use super::fixture_runtime::RigRuntime;
use super::fixture_storage::mem;
use crate::application::lifecycle::SealError;
use crate::product::EnterSeal;
use crate::registry::SealState;
use std::sync::Arc;

type State = Arc<crate::http::AppState>;
type Addr = std::net::SocketAddr;
type Reply = (u16, std::collections::HashMap<String, String>, Vec<u8>);

const CT: (&str, &str) = ("content-type", "application/json");
const CLOSE: (&str, &str) = ("stream-closed", "true");

struct Pair {
    a: State,
    addr_a: Addr,
    b: State,
    addr_b: Addr,
}

/// The ring {inst-a, inst-b, inst-c} is active and gives both of the rig's
/// shards to `owner`: "00", and "" for a route outside it.
fn ring(state: &State, owner: &str) {
    let members = ["inst-a", "inst-b", "inst-c"].map(str::to_string).to_vec();
    state.ownership.set_ring_active(members);
    state.ownership.set_override("00", owner);
    state.ownership.set_override("", owner);
}

/// A coordinates (`inst-a`) and B owns the shards (`inst-b`); A knows B's URL.
async fn pair(store: Arc<dyn object_store::ObjectStore>) -> Pair {
    let (a, addr_a) = http_rig_named(store.clone(), "inst-a").await;
    pair_with(a, addr_a, store).await
}

async fn pair_with(a: State, addr_a: Addr, store: Arc<dyn object_store::ObjectStore>) -> Pair {
    let (b, addr_b) = http_rig_named_at(store, "inst-b", RigRuntime::incarnation(1)).await;
    ring(&a, "inst-b");
    ring(&b, "inst-b");
    a.peer.set_peer("inst-b", &format!("http://{addr_b}"));
    Pair {
        a,
        addr_a,
        b,
        addr_b,
    }
}

async fn desc(state: &State, name: &str) -> crate::registry::StreamDesc {
    let sref = state.deployment.raw_adapter_sref(name);
    state.registry.invalidate(&sref);
    state.registry.get(&sref).await.unwrap().unwrap()
}

async fn claim(state: &State, name: &str) -> Option<SealState> {
    desc(state, name).await.sealing.clone()
}

/// The live claim's lease lapses (claimed_ms older than SEAL_CLAIM_MS).
async fn lapse(state: &State, name: &str) -> SealState {
    let sref = state.deployment.raw_adapter_sref(name);
    let lapsed = state
        .registry
        .cas_update(&sref, |d| {
            let Some(claim) = d.sealing.as_mut() else {
                return false;
            };
            claim.claimed_ms -= crate::registry::SEAL_CLAIM_MS + 1_000;
            true
        })
        .await
        .unwrap();
    assert!(lapsed, "{name} holds no claim to lapse");
    let claim = claim(state, name).await.unwrap();
    assert!(claim.owes_final(), "the staged claim owes no final");
    claim
}

async fn put_stream(addr: Addr, name: &str) {
    let path = format!("/v1/stream/{name}");
    let (st, _, body) = hreq(addr, "PUT", &path, &[CT], br#"[{"n":0}]"#).await;
    let body = String::from_utf8_lossy(&body);
    assert!(st == 200 || st == 201, "stage {name}: {st} {body}");
}

async fn old_close(addr: Addr, name: &str) -> Reply {
    let path = format!("/v1/stream/{name}");
    hreq(addr, "POST", &path, &[CT, CLOSE], br#"[{"fin":"a"}]"#).await
}

/// A one-record raw stream on B whose final-bearing close `[{"fin":"a"}]`
/// stopped after its intent (`committed` = false) or after its commit,
/// before the mark (`committed` = true); its claim has lapsed.
async fn stage_owed(p: &Pair, name: &str, committed: bool) -> SealState {
    put_stream(p.addr_b, name).await;
    if committed {
        crate::failpoints::stop_before_mark_committed(name);
    } else {
        crate::failpoints::stop_after_seal_intent(name);
    }
    let (st, _, _) = old_close(p.addr_b, name).await;
    crate::failpoints::stop_before_mark_committed_off(name);
    crate::failpoints::stop_after_seal_intent_off(name);
    assert_eq!(
        st, 503,
        "the failpoint did not stop the old close of {name}"
    );
    lapse(&p.b, name).await
}

/// The old final-bearing close `[{"fin":"a"}]`, sent to B and parked after
/// its intent, before its enqueue; its claim has lapsed.
#[expect(
    clippy::disallowed_methods,
    reason = "parked old-close fixture; the close is proven parked before its claim is lapsed, and every caller joins the returned handle after releasing it; running it inline would block the scenario on its own failpoint"
)]
async fn stage_parked(p: &Pair, name: &'static str) -> (tokio::task::JoinHandle<Reply>, SealState) {
    put_stream(p.addr_b, name).await;
    let fp = crate::failpoints::Fp::CloseBeforeEnqueue;
    let before = crate::failpoints::parked(fp, name);
    crate::failpoints::park_close_before_enqueue(name);
    let addr_b = p.addr_b;
    let close = tokio::spawn(async move { old_close(addr_b, name).await });
    for _ in 0..300 {
        if crate::failpoints::parked(fp, name) > before {
            return (close, lapse(&p.b, name).await);
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    panic!("the old close of {name} never parked");
}

/// `claim_seal` for a final-bearing operation `b-op`, coordinated on `at`.
async fn take_over(at: &State, name: &str) -> Result<EnterSeal, SealError> {
    let sref = at.deployment.raw_adapter_sref(name);
    let epoch = desc(at, name).await.stream_epoch.clone();
    let intent = crate::registry::SealIntent::Final {
        routing_key: String::new(),
        request_hash: "b-op".into(),
        final_committed: false,
    };
    crate::product::claim_seal(at, &sref, "b-op", &intent, &epoch).await
}

/// The owner's engine for segment 0 (opening it if the owner must).
async fn owner_engine(state: &State, name: &str) -> Arc<crate::shard::ShardEngine> {
    let route = desc(state, name).await.segment_route_by_id(0).unwrap();
    for _ in 0..100 {
        if let Some(engine) = state.engine_for_scaler(&route).await {
            return engine;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    panic!("the owner never resolved its engine for {name}");
}

async fn identity(state: &State, name: &str) -> [u8; 16] {
    desc(state, name).await.dynamic_segment_identity(0)
}

/// The segment's DURABLE fence row at `engine` (None = never fenced).
async fn fence_row(engine: &crate::shard::ShardEngine, identity: [u8; 16]) -> Option<u64> {
    let raw = engine
        .db
        .get(crate::shard::seal_fence_key(&identity))
        .await
        .unwrap()?;
    Some(u64::from_le_bytes(raw[..8].try_into().unwrap()))
}

async fn wait_enqueued(engine: &crate::shard::ShardEngine, at_least: u64, what: &str) {
    for _ in 0..500 {
        if engine.appends_enqueued() >= at_least {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    panic!("{what} never reached the owner's engine");
}

/// A close of the segment at `generation`, straight into `engine`'s queue.
async fn close_at(
    engine: &crate::shard::ShardEngine,
    identity: [u8; 16],
    generation: u64,
) -> Result<crate::shard::AppendAck, crate::shard::AppendErr> {
    let (tx, rx) = tokio::sync::oneshot::channel();
    let req = crate::shard::CloseReq {
        hash: identity,
        generation: Some(generation),
        resp: tx,
    };
    engine.try_close(req).unwrap();
    rx.await.unwrap()
}

/// The records the stream holds, read at `addr`.
async fn records(addr: Addr, name: &str) -> Vec<serde_json::Value> {
    let (_, _, body) = hreq(addr, "GET", &format!("/v1/stream/{name}"), &[], b"").await;
    serde_json::from_slice(&body).unwrap()
}

fn count_fin(records: &[serde_json::Value], fin: &str) -> usize {
    let fin = serde_json::Value::from(fin);
    records
        .iter()
        .filter(|r| r.get("fin") == Some(&fin))
        .count()
}

/// `state` holds no engine for the shard of the stream's segment.
async fn no_writer(state: &State, name: &str) {
    let route = desc(state, name).await.segment_route_by_id(0).unwrap();
    let prefix = state.shards.prefix_for(&route);
    let open: Vec<String> = state
        .shards
        .engines_by_prefix()
        .into_iter()
        .map(|(p, _)| p)
        .collect();
    assert!(
        !open.contains(&prefix),
        "a non-owner holds a writer for {prefix:?}: {open:?}"
    );
}

/// Retry the takeover on `at` while the owner answers 503 (reopening after
/// a failure or a move), until it installs; the installed generation, which
/// is the newest reservation the registry holds.
async fn installs(at: &State, owner: &State, name: &str) -> u64 {
    let mut outcome = take_over(at, name).await;
    for _ in 0..50 {
        let Err(SealError::Resumable(message)) = &outcome else {
            break;
        };
        assert!(message.contains("503"), "not an owner reopening: {message}");
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        outcome = take_over(at, name).await;
    }
    let Ok(EnterSeal::Installed { generation }) = outcome else {
        panic!("takeover did not install: {outcome:?}");
    };
    let d = desc(owner, name).await;
    assert_eq!(
        d.seal_gen_counter, generation,
        "installed below the newest reservation"
    );
    let installed = d.sealing.clone().unwrap();
    assert_eq!(
        (installed.operation_id.as_str(), installed.claim_generation),
        ("b-op", generation)
    );
    generation
}

fn resumable(outcome: &Result<EnterSeal, SealError>, contains: &str) {
    let Err(SealError::Resumable(message)) = outcome else {
        panic!("not resumable: {outcome:?}");
    };
    assert!(
        message.contains(contains),
        "resumable for another reason: {message}"
    );
}

/// Wrong ingress, product surface: a plain `:seal` handled by the non-owner
/// takes the lapsed claim over through the owner's durable fence and seals.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_non_owner_seal_takes_over_through_the_owners_fence() {
    let _serial = gap_lock().lock().await;
    let p = pair(mem()).await;
    let old = stage_owed(&p, "rly1", false).await;
    let key = [("prisma-encryption-key", PRISMA_KEY)];
    let (st, _, body) = preq(p.addr_a, "POST", "/v1/streams/rly1:seal", &key, b"{}").await;
    assert_eq!(st, 200, "{}", String::from_utf8_lossy(&body));
    let d = desc(&p.b, "rly1").await;
    assert!(
        d.sealed && d.sealing.is_none(),
        "not sealed: {:?}",
        d.sealing
    );
    let engine = owner_engine(&p.b, "rly1").await;
    let row = fence_row(&engine, identity(&p.b, "rly1").await).await;
    assert_eq!(
        row,
        Some(old.claim_generation + 1),
        "the owner's durable fence"
    );
    assert_eq!(
        records(p.addr_b, "rly1").await,
        vec![serde_json::json!({"n": 0})]
    );
    no_writer(&p.a, "rly1").await;
    engine_shutdown(&p.b).await;
}

/// Wrong ingress, raw surface: a close with another final, handled by the
/// non-owner, installs its claim through the owner's fence and then meets
/// the existing ownership answer; its exact retry at the owner completes.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_non_owner_raw_close_takeover_installs_then_redirects() {
    let _serial = gap_lock().lock().await;
    let p = pair(mem()).await;
    let old = stage_owed(&p, "rly2", false).await;
    let fin_b = br#"[{"fin":"b"}]"#;
    let (st, headers, body) = hreq(p.addr_a, "POST", "/v1/stream/rly2", &[CT, CLOSE], fin_b).await;
    assert_eq!(st, 409, "{}", String::from_utf8_lossy(&body));
    assert_eq!(
        headers.get("streams-replay-to").map(String::as_str),
        Some("inst-b")
    );
    let installed = claim(&p.b, "rly2").await.unwrap();
    assert_ne!(
        installed.operation_id, old.operation_id,
        "the old claim stands"
    );
    assert_eq!(installed.claim_generation, old.claim_generation + 1);
    assert!(installed.owes_final());
    let (st, _, body) = hreq(p.addr_b, "POST", "/v1/stream/rly2", &[CT, CLOSE], fin_b).await;
    assert!(
        st == 200 || st == 204,
        "{st} {}",
        String::from_utf8_lossy(&body)
    );
    let held = records(p.addr_b, "rly2").await;
    assert_eq!(
        (count_fin(&held, "a"), count_fin(&held, "b")),
        (0, 1),
        "{held:?}"
    );
    no_writer(&p.a, "rly2").await;
    engine_shutdown(&p.b).await;
}

/// Competing operations, the old one won first: its close committed before
/// the takeover, so the relayed fence reports closed and the coordinator
/// completes the old operation's transition from the foreign instance.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_relayed_fence_reports_an_old_close_that_already_won() {
    let _serial = gap_lock().lock().await;
    let p = pair(mem()).await;
    let old = stage_owed(&p, "rly3", true).await;
    let outcome = take_over(&p.a, "rly3").await;
    assert!(
        matches!(outcome, Ok(EnterSeal::AlreadySealed)),
        "{outcome:?}"
    );
    let d = desc(&p.b, "rly3").await;
    assert!(
        d.sealed && d.sealing.is_none(),
        "not sealed: {:?}",
        d.sealing
    );
    assert_eq!(d.seal_op.as_deref(), Some(old.operation_id.as_str()));
    assert_eq!(count_fin(&records(p.addr_b, "rly3").await, "a"), 1);
    no_writer(&p.a, "rly3").await;
    engine_shutdown(&p.b).await;
}

/// Competing operations, the old final queued at the owner ahead of the
/// relayed fence: the fence is decided after it and reports the close.
#[expect(
    clippy::disallowed_methods,
    reason = "queued-ahead relay fixture; the old close and the relayed takeover are proven to have entered the held committer in that order, and both are joined after the release; running either inline would deadlock behind the held commit"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_relayed_fence_is_decided_after_the_old_final_queued_ahead_of_it() {
    let _serial = gap_lock().lock().await;
    let p = pair(mem()).await;
    let (close, old) = stage_parked(&p, "rly4").await;
    let engine = owner_engine(&p.b, "rly4").await;
    let hold = engine.test_hold_commit().await;
    let base = engine.appends_enqueued();
    crate::failpoints::release_close_before_enqueue("rly4");
    wait_enqueued(&engine, base + 1, "the old close").await;
    let a = p.a.clone();
    let takeover = tokio::spawn(async move { take_over(&a, "rly4").await });
    wait_enqueued(&engine, base + 2, "the relayed fence").await;
    drop(hold);
    let outcome = takeover.await.unwrap();
    assert!(
        matches!(outcome, Ok(EnterSeal::AlreadySealed)),
        "{outcome:?}"
    );
    let (st, _, body) = close.await.unwrap();
    assert!(
        st == 200 || st == 204,
        "the old close: {st} {}",
        String::from_utf8_lossy(&body)
    );
    let d = desc(&p.b, "rly4").await;
    assert!(
        d.sealed && d.sealing.is_none(),
        "not sealed: {:?}",
        d.sealing
    );
    assert_eq!(d.seal_op.as_deref(), Some(old.operation_id.as_str()));
    assert_eq!(count_fin(&records(p.addr_b, "rly4").await, "a"), 1);
    no_writer(&p.a, "rly4").await;
    engine_shutdown(&p.b).await;
}

/// Competing operations, the old final arrives after the relayed fence:
/// closed=false was a barrier, so the old close is refused and cannot
/// close the segment under the installed claim.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_relayed_fence_refuses_the_old_final_that_arrives_after_it() {
    let _serial = gap_lock().lock().await;
    let p = pair(mem()).await;
    let (close, old) = stage_parked(&p, "rly5").await;
    let outcome = take_over(&p.a, "rly5").await;
    let generation = old.claim_generation + 1;
    assert!(
        matches!(outcome, Ok(EnterSeal::Installed { generation: g }) if g == generation),
        "takeover did not install: {outcome:?}"
    );
    crate::failpoints::release_close_before_enqueue("rly5");
    let (st, _, body) = close.await.unwrap();
    assert!(
        st >= 400,
        "the superseded old close: {st} {}",
        String::from_utf8_lossy(&body)
    );
    assert_eq!(
        records(p.addr_b, "rly5").await,
        vec![serde_json::json!({"n": 0})]
    );
    let d = desc(&p.b, "rly5").await;
    assert!(!d.sealed, "sealed under the superseded close");
    let installed = d.sealing.clone().unwrap();
    assert_eq!(
        (installed.operation_id.as_str(), installed.claim_generation),
        ("b-op", generation)
    );
    let engine = owner_engine(&p.b, "rly5").await;
    let identity = identity(&p.b, "rly5").await;
    let below = close_at(&engine, identity, old.claim_generation).await;
    assert!(
        matches!(below, Err(crate::shard::AppendErr::SealSuperseded)),
        "{below:?}"
    );
    no_writer(&p.a, "rly5").await;
    engine_shutdown(&p.b).await;
}

/// Owner movement: ownership moves again between the coordinator's resolve
/// and the owner's. The owner's 409 is resumable and is not chased; the
/// claim is untouched, the retry after convergence installs, and the
/// durable fence outlives a later move of the shard to the coordinator.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_second_ownership_move_leaves_the_takeover_resumable_and_unchased() {
    let _serial = gap_lock().lock().await;
    let p = pair(mem()).await;
    let old = stage_owed(&p, "rly6", false).await;
    ring(&p.b, "inst-c");
    let outcome = take_over(&p.a, "rly6").await;
    resumable(&outcome, "answered 409");
    no_writer(&p.b, "rly6").await;
    assert_eq!(
        claim(&p.b, "rly6").await,
        Some(old.clone()),
        "the claim moved"
    );
    let reserved = desc(&p.b, "rly6").await.seal_gen_counter;
    assert_eq!(reserved, old.claim_generation + 1, "the reservation stands");
    ring(&p.b, "inst-b");
    // The anti-flap holdoff of B's yielded shard is the router's to wait out.
    for prefix in ["00", ""] {
        p.b.shards.clear_holdoff(prefix);
    }
    let generation = installs(&p.a, &p.b, "rly6").await;
    assert!(
        generation > reserved,
        "installed below the standing reservation"
    );
    engine_shutdown(&p.b).await;
    ring(&p.a, "inst-a");
    ring(&p.b, "inst-a");
    let engine = owner_engine(&p.a, "rly6").await;
    let identity = identity(&p.a, "rly6").await;
    assert_eq!(fence_row(&engine, identity).await, Some(generation));
    let below = close_at(&engine, identity, old.claim_generation).await;
    assert!(
        matches!(below, Err(crate::shard::AppendErr::SealSuperseded)),
        "{below:?}"
    );
    engine_shutdown(&p.a).await;
}

/// Wrong ingress, the sender's side: without the owner's URL, and with a
/// credential the owner refuses, the takeover is resumable and nothing moves.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_relay_without_a_peer_url_or_credential_leaves_the_claim_untouched() {
    let _serial = gap_lock().lock().await;
    let p = pair(mem()).await;
    let old = stage_owed(&p, "rly7", false).await;
    p.a.peer.set_peers(std::collections::HashMap::new());
    let outcome = take_over(&p.a, "rly7").await;
    resumable(&outcome, "has no published peer URL");
    assert_eq!(claim(&p.b, "rly7").await, Some(old));
    let engine = owner_engine(&p.b, "rly7").await;
    assert_eq!(fence_row(&engine, identity(&p.b, "rly7").await).await, None);
    engine_shutdown(&p.b).await;

    let store = mem();
    let options = HttpRigOptions {
        instance: Some("inst-a".to_string()),
        fleet_auth: Some((Some("not-the-fleet-token".to_string()), None)),
        ..Default::default()
    };
    let (a, addr_a) = http_rig_build(store.clone(), RigRuntime::first(), options)
        .await
        .parts();
    let q = pair_with(a, addr_a, store).await;
    let old = stage_owed(&q, "rly7", false).await;
    let outcome = take_over(&q.a, "rly7").await;
    resumable(&outcome, "answered 401");
    assert_eq!(claim(&q.b, "rly7").await, Some(old));
    let engine = owner_engine(&q.b, "rly7").await;
    assert_eq!(fence_row(&engine, identity(&q.b, "rly7").await).await, None);
    no_writer(&q.a, "rly7").await;
    engine_shutdown(&q.b).await;
}

/// Crash around durability, a failed write: the relayed fence shares a held
/// owner group with the old close, and the group fails. Nothing installs
/// and the claim is untouched; the retry installs above a durable fence.
#[expect(
    clippy::disallowed_methods,
    reason = "failed-group relay fixture; the old close and the relayed takeover are proven to have entered the held committer before the failure is armed, and both are joined after the release; running either inline would deadlock behind the held commit"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_relayed_fence_in_a_failed_owner_group_never_installs() {
    let _serial = gap_lock().lock().await;
    let p = pair(mem()).await;
    let (close, old) = stage_parked(&p, "rly8").await;
    let engine = owner_engine(&p.b, "rly8").await;
    let identity = identity(&p.b, "rly8").await;
    let hold = engine.test_hold_commit().await;
    let base = engine.appends_enqueued();
    crate::failpoints::release_close_before_enqueue("rly8");
    wait_enqueued(&engine, base + 1, "the old close").await;
    let a = p.a.clone();
    let takeover = tokio::spawn(async move { take_over(&a, "rly8").await });
    wait_enqueued(&engine, base + 2, "the relayed fence").await;
    engine.fail_next_group_for(identity);
    drop(hold);
    resumable(&takeover.await.unwrap(), "answered 503");
    let (st, _, _) = close.await.unwrap();
    assert!(
        !(200..300).contains(&st),
        "the old close in the failed group: {st}"
    );
    assert_eq!(
        engine.group_failures_tripped(),
        1,
        "the failpoint never fired"
    );
    assert_eq!(
        claim(&p.b, "rly8").await,
        Some(old.clone()),
        "the claim moved"
    );
    let generation = installs(&p.a, &p.b, "rly8").await;
    let reopened = owner_engine(&p.b, "rly8").await;
    assert_eq!(fence_row(&reopened, identity).await, Some(generation));
    let below = close_at(&reopened, identity, old.claim_generation).await;
    assert!(
        matches!(below, Err(crate::shard::AppendErr::SealSuperseded)),
        "{below:?}"
    );
    no_writer(&p.a, "rly8").await;
    engine_shutdown(&p.b).await;
}

/// Crash around durability, after it: the owner makes the relayed fence
/// durable and restarts before its reply is sent. The coordinator is left
/// resumable with the claim untouched, or installed at that fence; never
/// sealed. The durable fence outlives the restart, and the takeover
/// completes against the new incarnation.
#[expect(
    clippy::disallowed_methods,
    reason = "lost-reply relay fixture; the relayed takeover is proven to have entered the owner's engine and to be pending behind the held dispatch, and it is joined after the owner's shutdown and the release; running it inline would deadlock behind the barrier"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_durable_relayed_fence_whose_reply_is_lost_survives_the_owners_restart() {
    let _serial = gap_lock().lock().await;
    let store = mem();
    let p = pair(store.clone()).await;
    let old = stage_owed(&p, "rly9", false).await;
    let first = old.claim_generation + 1;
    let engine = owner_engine(&p.b, "rly9").await;
    let identity = identity(&p.b, "rly9").await;
    let guard = engine.test_hold_dispatch().await;
    let base = engine.appends_enqueued();
    let a = p.a.clone();
    let takeover = tokio::spawn(async move { take_over(&a, "rly9").await });
    wait_enqueued(&engine, base + 1, "the relayed fence").await;
    tokio::time::sleep(std::time::Duration::from_millis(150)).await;
    assert!(
        !takeover.is_finished(),
        "the takeover concluded before the fence was durable"
    );
    engine_shutdown(&p.b).await;
    drop(guard);
    let outcome = takeover.await.unwrap();
    let installed = match outcome {
        Ok(EnterSeal::Installed { generation }) => Some(generation),
        Err(SealError::Resumable(_)) => None,
        other => panic!("the lost reply decided the takeover: {other:?}"),
    };
    if installed.is_none() {
        assert_eq!(
            claim(&p.b, "rly9").await,
            Some(old.clone()),
            "the claim moved"
        );
    }
    let (b2, addr_b2) = http_rig_named_at(store, "inst-b", RigRuntime::incarnation(2)).await;
    ring(&b2, "inst-b");
    p.a.peer.set_peer("inst-b", &format!("http://{addr_b2}"));
    let restarted = owner_engine(&b2, "rly9").await;
    let row = fence_row(&restarted, identity).await;
    assert!(
        row >= Some(first),
        "the durable fence did not survive: {row:?}"
    );
    let generation = match installed {
        Some(generation) => generation,
        None => installs(&p.a, &b2, "rly9").await,
    };
    assert_eq!(installed.map_or(generation, |_| first), generation);
    assert_eq!(fence_row(&restarted, identity).await, Some(generation));
    let below = close_at(&restarted, identity, old.claim_generation).await;
    assert!(
        matches!(below, Err(crate::shard::AppendErr::SealSuperseded)),
        "{below:?}"
    );
    no_writer(&p.a, "rly9").await;
    engine_shutdown(&b2).await;
}
