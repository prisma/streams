//! Read-byte quota (§17.2) on every page-serving product route.

use super::fixture_auth::{auth_rig, mint_token, rig_policy, rig_publish_policy};
use super::fixture_http::engine_shutdown;
use super::fixture_requests::{PRISMA_KEY, preq};

/// RED (review rank 28): `GET {collection}:scan` pages back decrypted
/// record bodies, so a page draws on the SAME project read-byte
/// bucket as `GET records`: the project is admitted at entry only
/// while the bucket is not in debt, a served page is debited by its
/// framed body bytes, and a refused page (wrong key) is never debited.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn scan_pages_draw_on_the_read_byte_quota() {
    let (svc, state, addr) = auth_rig("proj-rv", "ws-rv", &["c-rv"], None).await;
    // Policy v2: an 8 B/s read budget, published before the first request
    // so the project's bucket is created from it.
    let mut policy = rig_policy("proj-rv", "ws-rv", 1, 2);
    policy.quotas.read_bytes_per_sec = 8;
    rig_publish_policy(&svc, policy, 2).unwrap();
    let bearer = mint_token("c-rv", "proj-rv", "ws-rv", 1, 1, "t", 3600);
    let a = ("authorization", bearer.as_str());
    let ekey = ("prisma-encryption-key", PRISMA_KEY);
    let wrong = (
        "prisma-encryption-key",
        "CAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAg=",
    );
    let text = |b: &[u8]| String::from_utf8_lossy(b).into_owned();
    let (st, _, _) = preq(
        addr,
        "PUT",
        "/v1/streams/sq",
        &[ekey, a],
        br#"{"format":{"kind":"json"}}"#,
    )
    .await;
    assert_eq!(st, 201);
    // A ~2 KiB record: the page's debt takes minutes to refill at 8 B/s,
    // so no scheduling gap between two requests can clear it.
    let record = format!("{{\"pad\":\"{}\"}}", "x".repeat(2048));
    let (st, _, _) = preq(
        addr,
        "POST",
        "/v1/streams/sq/records",
        &[ekey, a],
        record.as_bytes(),
    )
    .await;
    assert_eq!(st, 200);
    // A refused page is not read volume: 403 leaves the bucket full.
    let (st, _, b) = preq(addr, "GET", "/v1/streams/sq:scan", &[wrong, a], b"").await;
    assert_eq!(st, 403, "{}", text(&b));
    // No debt yet: the first page serves, and it exceeds the budget.
    let (st, _, b) = preq(addr, "GET", "/v1/streams/sq:scan", &[ekey, a], b"").await;
    assert_eq!(
        st,
        200,
        "a refused scan must not have been debited: {}",
        text(&b)
    );
    assert!(
        b.len() > 2048,
        "the page must exceed the budget: {}",
        b.len()
    );
    // That page put the project in debt: the next scan is refused with
    // the read-quota class, and so is a records read (one bucket).
    let (st, h, b) = preq(addr, "GET", "/v1/streams/sq:scan", &[ekey, a], b"").await;
    assert_eq!(
        st,
        429,
        "scan in read debt must refuse typed: {st} {}",
        text(&b)
    );
    assert!(
        text(&b).contains("project_rate_limit"),
        "typed code: {}",
        text(&b)
    );
    assert!(
        h.get("retry-after").and_then(|s| s.parse::<u64>().ok()) >= Some(1),
        "retry-after: {h:?}"
    );
    let (st, _, b) = preq(addr, "GET", "/v1/streams/sq/records", &[ekey, a], b"").await;
    assert_eq!(
        st,
        429,
        "a scan page is debited to the shared bucket: {st} {}",
        text(&b)
    );
    engine_shutdown(&state).await;
}
