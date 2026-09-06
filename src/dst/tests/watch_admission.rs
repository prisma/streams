//! Account and capability observation admission share one explicit owner.
use super::fixture_auth::{rig_policy, rig_publish_policy, sr_rig};
use super::fixture_http::engine_shutdown;
use super::fixture_requests::{PRISMA_KEY, preq};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn account_watch_wait_charges_one_request_and_releases_its_subscription() {
    let (state, addr, bearer) = sr_rig(
        "watch-once",
        "ws-once",
        "cred-once",
        "kid-once",
        "streams.create streams.watches.manage streams.metadata.read",
    )
    .await;
    let (status, _, body) = preq(
        addr,
        "PUT",
        "/v1/streams/orders",
        &[
            ("authorization", &bearer),
            ("prisma-encryption-key", PRISMA_KEY),
        ],
        br#"{"format":{"kind":"json"},"watches":[{"name":"w","fields":["/id"]}]}"#,
    )
    .await;
    assert_eq!(status, 201, "{}", String::from_utf8_lossy(&body));
    let mut policy = rig_policy("watch-once", "ws-once", 1, 2);
    policy.quotas.max_inflight_requests = 1;
    policy.quotas.max_live_subscriptions = 1;
    rig_publish_policy(&state.auth, policy, 2).unwrap();
    // The HTTP adapter already holds the only request slot. A second admit
    // inside watch waiting would reject this request with 429.
    for _ in 0..2 {
        let (status, _, body) = preq(
            addr,
            "GET",
            "/v1/streams/orders/watches/w/keys/0000000000000000?cursor=now&timeoutMs=1",
            &[
                ("authorization", &bearer),
                ("prisma-encryption-key", PRISMA_KEY),
            ],
            b"",
        )
        .await;
        assert_eq!(status, 200, "{}", String::from_utf8_lossy(&body));
        let result: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(result["invalidated"], false);
    }
    assert_eq!(state.quotas.stats().1, 0);
    engine_shutdown(&state).await;
}
