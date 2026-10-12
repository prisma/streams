//! Anonymous capability lookups (public-beta phase B step 15, shared-cells
//! M2): what an unverified capability costs the store, and which budget it
//! spends, for each thing the cell's descriptor cache can hold for the
//! stream it claims. A fresh entry decides for free; an expired entry, an
//! expired absence or nothing at all sends the claim to one budgeted store
//! fetch that fills nothing; only a capability that verified reads through
//! the cache, and the descriptor that read returns is the one it is proved
//! against. A served project's claims spend its own budget (20 a second, 40
//! at most) and the cell's (100 a second, 200 at most); a claim under a
//! stale feed, and every claim off enforce, spends the cell's alone. A spent
//! budget answers `429 watch_lookup_busy` with `Retry-After: 1` (owner
//! decision P2 of 2026-10-12).

use super::fixture_cell::{CellSpec, error_code, open_cell, project};
use super::fixture_http::{engine_shutdown, http_rig};
use super::fixture_requests::{PRISMA_KEY, preq};
use super::fixture_storage::{mem, skey};
use crate::dst::StoreOp;
use crate::dst::trace_store::TraceStore;
use crate::registry::StreamDesc;
use crate::tenant::TenantStreamRef;

/// A stream with one watch, `by-customer` on `/customerId`.
const WATCHED: &[u8] =
    br#"{"format":{"kind":"json"},"watches":[{"name":"by-customer","fields":["/customerId"]}]}"#;
/// The watch key every leg observes.
const KHEX: &str = "00000000000000c4";
/// A refusal of a capability the cell checked: `(status, code, retry-after)`.
type Answer = (u16, Option<String>, Option<String>);

fn forbidden() -> Answer {
    (403, Some("watch_unauthorized".into()), None)
}

fn busy() -> Answer {
    (429, Some("watch_lookup_busy".into()), Some("1".into()))
}

/// A capability for `sref`'s watch at `KHEX`, signed for the incarnation
/// `desc` (valid for two minutes), as `<project>.<exp>.<sig>`.
fn capability(sref: &TenantStreamRef, desc: &StreamDesc) -> String {
    let epoch = desc.epoch();
    let signing = crate::crypto::wait_sig_key(&crate::crypto::touch_token(&skey(), &epoch), &epoch);
    let exp = crate::shard::now_ms() / 1000 + 120;
    let sig = crate::crypto::watch_capability_sig(
        &signing,
        sref,
        &desc.stream_epoch,
        "by-customer",
        KHEX,
        "GET",
        exp,
    );
    format!("{}.{exp}.{sig}", sref.project_id().as_str())
}

/// One watch wait of 100 ms on `name` carrying `cap` (and the stream key
/// when `keyed`): its status, error code and `retry-after`.
async fn watch(addr: std::net::SocketAddr, name: &str, cap: &str, keyed: bool) -> Answer {
    let path =
        format!("/v1/streams/{name}/watches/by-customer/keys/{KHEX}?timeoutMs=100&cap={cap}");
    let key = [("prisma-encryption-key", PRISMA_KEY)];
    let headers: &[(&str, &str)] = if keyed { &key } else { &[] };
    let (status, headers, body) = preq(addr, "GET", &path, headers, b"").await;
    (
        status,
        error_code(&body),
        headers.get("retry-after").cloned(),
    )
}

/// `count` concurrent garbage capabilities naming `claimed`, on `name` or,
/// without one, each on a name of its own: how many were refused 403 after
/// a check and how many 429 by a spent budget (nothing else is answered),
/// and how long the round took, over which the budgets refill.
async fn garbage(
    addr: std::net::SocketAddr,
    claimed: &str,
    name: Option<&str>,
    count: usize,
) -> (usize, usize, std::time::Duration) {
    let started = std::time::Instant::now();
    let cap = format!("{claimed}.1.x");
    let answers = futures_util::future::join_all((0..count).map(|j| {
        let own = name.map_or_else(|| format!("g{j}"), str::to_string);
        let cap = cap.clone();
        async move { watch(addr, &own, &cap, false).await }
    }))
    .await;
    let refused = answers.iter().filter(|a| **a == forbidden()).count();
    let spent = answers.iter().filter(|a| **a == busy()).count();
    assert_eq!(refused + spent, count, "garbage capabilities: {answers:?}");
    (refused, spent, started.elapsed())
}

/// Store GETs of `sref`'s descriptor on `trace` after event `after`.
fn descriptor_gets(trace: &TraceStore, sref: &TenantStreamRef, after: u64) -> usize {
    let hex = |b: &[u8]| b.iter().map(|b| format!("{b:02x}")).collect::<String>();
    let path = format!(
        "registry/v4/projects/{}/streams/{}.json",
        hex(sref.project_id().as_bytes()),
        hex(sref.name().as_str().as_bytes())
    );
    let events = trace.events().into_iter();
    events
        .filter(|e| e.seq > after && e.op == StoreOp::Get && e.path == path)
        .count()
}

fn last_event(trace: &TraceStore) -> u64 {
    trace.events().last().map_or(0, |e| e.seq)
}

/// A fresh entry decides for free: 64 garbage capabilities naming a served
/// project's stream whose slot its create just filled, and 64 naming a name
/// whose slot holds a fresh absence, are all `403` with no descriptor GET,
/// and none is `429` though 64 passes the project's burst of 40: a check
/// against what the cell holds spends no budget.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_fresh_entry_refuses_a_capability_without_a_lookup() {
    let trace = TraceStore::verbatim(mem());
    let cell = open_cell(CellSpec {
        store: trace.clone(),
        ..CellSpec::open(2)
    })
    .await;
    assert_eq!(cell.call(1, "PUT", "/v1/streams/w", WATCHED).await.0, 201);
    let absent = cell.call(1, "GET", "/v1/streams/ghost", b"").await;
    assert_eq!(absent.0, 404, "the absence the cell now holds");
    let id = crate::tenant::ProjectId::new(&project(1)).unwrap();
    for name in ["w", "ghost"] {
        let after = last_event(&trace);
        let split = garbage(cell.addr, &project(1), Some(name), 64).await;
        let gets = descriptor_gets(&trace, &id.stream_ref(name), after);
        assert_eq!(
            (split.0, split.1, gets),
            (64, 0, 0),
            "{name}: refused, busy, GETs"
        );
    }
    engine_shutdown(&cell.state).await;
}

/// An expired entry sends a capability it refuses to one budgeted store
/// fetch that fills nothing, so a stream recreated elsewhere (in a fleet)
/// is checked against its current incarnation; and a capability that
/// verifies against a stale entry is proved against what the cache's own
/// read returns, so it is refused. The slot is planted with the stream's
/// first incarnation after a recreate, as a fleet peer's stale slot is,
/// and expired. Exact descriptor GETs: a garbage capability against the
/// expired current entry 1; the first incarnation's capability 1 (the
/// cache's read; no lookup); the current incarnation's capability against
/// the expired stale entry 1 + 1 (the lookup, then the cache's read), and
/// it observes.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_expired_entry_is_checked_against_the_store_under_budget() {
    let trace = TraceStore::verbatim(mem());
    let cell = open_cell(CellSpec {
        store: trace.clone(),
        ..CellSpec::open(2)
    })
    .await;
    let sref = crate::tenant::ProjectId::new(&project(1))
        .unwrap()
        .stream_ref("w");
    assert_eq!(cell.call(1, "PUT", "/v1/streams/w", WATCHED).await.0, 201);
    let first = cell.state.registry.get(&sref).await.unwrap().unwrap();
    assert_eq!(cell.call(1, "DELETE", "/v1/streams/w", b"").await.0, 204);
    assert_eq!(cell.call(1, "PUT", "/v1/streams/w", WATCHED).await.0, 201);
    let current = cell.state.registry.get(&sref).await.unwrap().unwrap();
    assert_ne!(
        first.stream_epoch, current.stream_epoch,
        "a new incarnation"
    );
    let (stale_cap, current_cap) = (capability(&sref, &first), capability(&sref, &current));
    let registry = &cell.state.registry;
    let mut answers = Vec::new();
    let cases: [(&str, &StreamDesc, String); 3] = [
        ("garbage", &current, format!("{}.1.x", project(1))),
        ("first incarnation's", &first, stale_cap),
        ("current incarnation's", &first, current_cap),
    ];
    for (what, held, cap) in cases {
        registry.test_poison_cache(&sref, held.clone());
        registry.expire_cache_for_tests(&sref);
        let after = last_event(&trace);
        let (status, code, _) = watch(cell.addr, "w", &cap, false).await;
        answers.push((what, status, code, descriptor_gets(&trace, &sref, after)));
    }
    let refused = Some("watch_unauthorized".to_string());
    let expected = [
        ("garbage", 403, refused.clone(), 1),
        ("first incarnation's", 403, refused, 1),
        ("current incarnation's", 200, None, 2),
    ];
    assert_eq!(answers, expected, "capabilities against an expired slot");
    engine_shutdown(&cell.state).await;
}

/// Under a stale policy feed a claim cannot be placed, so only the cell's
/// budget applies: a round of 320 garbage capabilities naming a served
/// project on names of their own fetches at least the cell's burst of 200
/// (far past the project's 40) and at most what its 100 a second refills
/// meanwhile; the rest are `429 watch_lookup_busy`, `Retry-After: 1`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_claim_under_a_stale_feed_spends_only_the_cells_lookups() {
    let cell = open_cell(CellSpec::open(2)).await;
    let stale_at = crate::shard::now_ms() / 1000 - crate::auth::POLICY_STALENESS_MAX_SECS - 1;
    let (mut policies, _) = cell.feeds(stale_at);
    policies.feed_version += 1;
    cell.svc.publish_policies(policies).unwrap();
    assert_cell_budget(garbage(cell.addr, &project(1), None, 320).await);
    engine_shutdown(&cell.state).await;
}

/// What the cell's budget alone (100 a second, 200 at most, full at the
/// start) lets a round fetch, the rest refused by it: at least its burst
/// (far past a project's 40) and at most what refilled while the round
/// ran, so on any host fast enough to send the round within 1.2 s some are
/// refused.
fn assert_cell_budget((fetched, spent, took): (usize, usize, std::time::Duration)) {
    let refilled = usize::try_from((100 * took.as_millis()).div_ceil(1000)).unwrap();
    assert!(
        (200..=200 + refilled).contains(&fetched),
        "{fetched} fetched and {spent} refused busy in {took:?}, by the cell's budget alone"
    );
}

/// Off enforce (single-tenant, the deployment's own project) a capability
/// that does not verify is refused even beside the matching stream key:
/// the claim is checked before the key is (phase B step 15), where before
/// the key alone observed (#NEW-phase-b-1). The key alone still observes,
/// and garbage claims spend only the cell's budget.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn off_enforce_a_key_beside_a_capability_that_does_not_verify_is_refused() {
    let (state, addr) = http_rig(mem()).await;
    let key = [("prisma-encryption-key", PRISMA_KEY)];
    assert_eq!(
        preq(addr, "PUT", "/v1/streams/w", &key, WATCHED).await.0,
        201
    );
    let tenant = state.deployment.raw_adapter_sref("w");
    let claimed = tenant.project_id().as_str().to_string();
    let path = format!("/v1/streams/w/watches/by-customer/keys/{KHEX}?timeoutMs=100");
    let alone = preq(addr, "GET", &path, &key, b"").await.0;
    let beside = watch(addr, "w", &format!("{claimed}.1.x"), true).await;
    assert_eq!(
        (alone, beside),
        (200, forbidden()),
        "the key alone, then beside garbage"
    );
    assert_cell_budget(garbage(addr, &claimed, None, 320).await);
    engine_shutdown(&state).await;
}
