//! Watch observation.

use super::fixture_auth::sr_rig;
use super::fixture_http::{engine_shutdown, http_rig, http_rig_at, http_rig_opts};
use super::fixture_requests::{PRISMA_KEY, preq};
use super::fixture_runtime::RigRuntime;
use super::fixture_storage::{mem, skey};

/// Stage 2b: watches — definitions listed from the descriptor; a wait
/// wakes only when a MATCHING record commits (after durability); the
/// derived URL sig is a valid observation capability; a stale cursor is
/// an explicit resync.
///
/// close_shard must match by the stream's ROUTE hash, not the
/// storage-hash map key: fencing the route-owning shard resyncs a
/// parked watch wait immediately, and fencing the shard that merely
/// contains the STORAGE hash must not touch the journal. Pre-fix this
/// failed both ways (route-vs-storage hash-domain mismatch: journals
/// keyed by storage_hash, close matched route-space prefixes).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn touch_close_shard_matches_route_hash_not_storage_hash() {
    let store = mem();
    // 1-bit prefixes: every hash belongs to "0" or "1".
    let (state, addr) = http_rig_opts(
        store,
        vec!["0".into(), "1".into()],
        crate::shard::ShardConfig::default(),
    )
    .await;

    // Find a stream whose route- and storage-space shard prefixes
    // DISAGREE (stream_epoch is random, so probe; ~50% per name).
    let mut picked = None;
    for i in 0..64 {
        let name = format!("wtc-{i}");
        let (st, _, b) = preq(
            addr,
            "PUT",
            &format!("/v1/streams/{name}"),
            &[("prisma-encryption-key", PRISMA_KEY)],
            br#"{"format":{"kind":"json"},"watches":[{"name":"by-customer","fields":["/customerId"]}]}"#,
        )
        .await;
        assert_eq!(st, 201, "{}", String::from_utf8_lossy(&b));
        let desc = state
            .registry
            .get(&state.deployment.raw_adapter_sref(&name))
            .await
            .unwrap()
            .unwrap();
        let rp = state.shards.prefix_for(
            &crate::crypto::RouteHash::for_stream(&state.deployment.raw_adapter_sref(&name)).0,
        );
        let sp = state.shards.prefix_for(&desc.storage_hash());
        if rp != sp {
            picked = Some((name, rp, sp));
            break;
        }
    }
    let (name, route_prefix, storage_prefix) = picked.expect("straddling stream in 64 tries");

    // Plant the journal via the REAL writer path: matching append ->
    // TouchFeed -> post-durability acker ingest.
    let fields = vec!["/customerId".to_string()];
    let khex = crate::product::watch_key_hex("by-customer", &fields, &["\"c42\"".to_string()]);
    let (st, _, _) = preq(
        addr,
        "POST",
        &format!("/v1/streams/{name}/records"),
        &[
            ("prisma-encryption-key", PRISMA_KEY),
            ("prisma-routing-key", "c42"),
        ],
        b"{\"customerId\":\"c42\",\"total\":1}",
    )
    .await;
    assert_eq!(st, 200);

    // Park a waiter.
    let path =
        format!("/v1/streams/{name}/watches/by-customer/keys/{khex}?cursor=now&timeoutMs=8000");
    let wait = tokio::spawn({
        let path = path.clone();
        async move {
            preq(
                addr,
                "GET",
                &path,
                &[("prisma-encryption-key", PRISMA_KEY)],
                b"",
            )
            .await
        }
    });
    tokio::time::sleep(std::time::Duration::from_millis(150)).await;

    // Direction A: fencing the STORAGE-prefix shard must be a no-op —
    // prove the journal survived by waking the waiter with a REAL
    // touch afterwards (not a resync).
    state.touch.close_shard(&storage_prefix);
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    let (st, _, _) = preq(
        addr,
        "POST",
        &format!("/v1/streams/{name}/records"),
        &[
            ("prisma-encryption-key", PRISMA_KEY),
            ("prisma-routing-key", "c42"),
        ],
        b"{\"customerId\":\"c42\",\"total\":2}",
    )
    .await;
    assert_eq!(st, 200);
    let (st, _, b) = wait.await.unwrap();
    assert_eq!(st, 200, "{}", String::from_utf8_lossy(&b));
    let v: serde_json::Value = serde_json::from_slice(&b).unwrap();
    assert_eq!(v["invalidated"], true, "{v}");
    assert_ne!(
        v["reason"], "resync",
        "journal must survive a storage-prefix close: {v}"
    );

    // Direction B: fence the ROUTE-prefix shard; a parked waiter must
    // wake promptly. The acker's post-durability journal ingest can
    // land AFTER the append's 200 (H2 hook), so the cursor from A's
    // wake may trail late ingests — settle first: short waits until a
    // clean timeout proves no unseen generations remain, then park on
    // that baseline.
    let mut cursor_a = v["cursor"].as_str().unwrap().to_string();
    let mut settled = false;
    for _ in 0..10 {
        let p = format!(
            "/v1/streams/{name}/watches/by-customer/keys/{khex}?cursor={cursor_a}&timeoutMs=300"
        );
        let (st, _, b) = preq(
            addr,
            "GET",
            &p,
            &[("prisma-encryption-key", PRISMA_KEY)],
            b"",
        )
        .await;
        assert_eq!(st, 200);
        let sv: serde_json::Value = serde_json::from_slice(&b).unwrap();
        cursor_a = sv["cursor"].as_str().unwrap().to_string();
        if sv["invalidated"] == false {
            settled = true;
            break;
        }
    }
    assert!(settled, "journal generation never settled");
    let path2 = format!(
        "/v1/streams/{name}/watches/by-customer/keys/{khex}?cursor={cursor_a}&timeoutMs=8000"
    );
    let wait = tokio::spawn(async move {
        preq(
            addr,
            "GET",
            &path2,
            &[("prisma-encryption-key", PRISMA_KEY)],
            b"",
        )
        .await
    });
    tokio::time::sleep(std::time::Duration::from_millis(1000)).await;
    if wait.is_finished() {
        let (st, _, b) = wait.await.unwrap();
        panic!(
            "spurious wake before fence: st={st} body={}",
            String::from_utf8_lossy(&b)
        );
    }
    let t0 = std::time::Instant::now();
    state.touch.close_shard(&route_prefix);
    let (st, _, b) = wait.await.unwrap();
    assert_eq!(st, 200, "{}", String::from_utf8_lossy(&b));
    let v: serde_json::Value = serde_json::from_slice(&b).unwrap();
    // A fence wake may resolve as an explicit resync (Stale) or, when
    // the post-close re-fetch observes an advanced generation, as a
    // proven change — both are PROMPT invalidations. The bug's symptom
    // was neither: the waiter dangled to its full long-poll timeout
    // with invalidated:false because close_shard never matched.
    assert_eq!(v["invalidated"], true, "fence must invalidate: {v}");
    assert!(
        t0.elapsed() < std::time::Duration::from_secs(4),
        "fence wake must be immediate, not the 8s long-poll timeout (got {v})"
    );
    engine_shutdown(&state).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn product_watch_wakes_on_matching_append() {
    let store = mem();
    let (state, addr) = http_rig(store).await;
    let (st, _, b) = preq(
        addr,
        "PUT",
        "/v1/streams/wt",
        &[("prisma-encryption-key", PRISMA_KEY)],
        br#"{"format":{"kind":"json"},"watches":[{"name":"by-customer","fields":["/customerId"]}]}"#,
    )
    .await;
    assert_eq!(st, 201, "{}", String::from_utf8_lossy(&b));

    // Management endpoints.
    let (st, _, b) = preq(
        addr,
        "GET",
        "/v1/streams/wt/watches",
        &[("prisma-encryption-key", PRISMA_KEY)],
        b"",
    )
    .await;
    assert_eq!(st, 200);
    let v: serde_json::Value = serde_json::from_slice(&b).unwrap();
    assert_eq!(v["watches"][0]["name"], "by-customer");
    let (st, _, _) = preq(
        addr,
        "GET",
        "/v1/streams/wt/watches/by-customer",
        &[("prisma-encryption-key", PRISMA_KEY)],
        b"",
    )
    .await;
    assert_eq!(st, 200);
    let (st, _, _) = preq(
        addr,
        "GET",
        "/v1/streams/wt/watches/nope",
        &[("prisma-encryption-key", PRISMA_KEY)],
        b"",
    )
    .await;
    assert_eq!(st, 404);

    let fields = vec!["/customerId".to_string()];
    let khex = crate::product::watch_key_hex("by-customer", &fields, &["\"c42\"".to_string()]);

    // Concurrent wait + matching append -> invalidated.
    let path = format!("/v1/streams/wt/watches/by-customer/keys/{khex}?cursor=now&timeoutMs=5000");
    let wait = tokio::spawn(async move {
        preq(
            addr,
            "GET",
            &path,
            &[("prisma-encryption-key", PRISMA_KEY)],
            b"",
        )
        .await
    });
    tokio::time::sleep(std::time::Duration::from_millis(150)).await;
    let (st, _, _) = preq(
        addr,
        "POST",
        "/v1/streams/wt/records",
        &[
            ("prisma-encryption-key", PRISMA_KEY),
            ("prisma-routing-key", "c42"),
        ],
        b"{\"customerId\":\"c42\",\"total\":9}",
    )
    .await;
    assert_eq!(st, 200);
    let (st, _, b) = wait.await.unwrap();
    assert_eq!(st, 200, "{}", String::from_utf8_lossy(&b));
    let v: serde_json::Value = serde_json::from_slice(&b).unwrap();
    assert_eq!(v["invalidated"], true, "{v}");
    let cursor = v["cursor"].as_str().unwrap().to_string();

    // A NON-matching append does not wake this key.
    let (st, _, _) = preq(
        addr,
        "POST",
        "/v1/streams/wt/records",
        &[
            ("prisma-encryption-key", PRISMA_KEY),
            ("prisma-routing-key", "other"),
        ],
        b"{\"customerId\":\"other\"}",
    )
    .await;
    assert_eq!(st, 200);
    let path =
        format!("/v1/streams/wt/watches/by-customer/keys/{khex}?cursor={cursor}&timeoutMs=300");
    let (st, _, b) = preq(
        addr,
        "GET",
        &path,
        &[("prisma-encryption-key", PRISMA_KEY)],
        b"",
    )
    .await;
    assert_eq!(st, 200);
    let v: serde_json::Value = serde_json::from_slice(&b).unwrap();
    assert_eq!(v["invalidated"], false, "{v}");
    let cursor = v["cursor"].as_str().unwrap().to_string();

    // Derived URL sig authorizes WITHOUT the encryption key.
    let skey_local = skey();
    let desc = state
        .registry
        .get(&state.deployment.raw_adapter_sref("wt"))
        .await
        .unwrap()
        .unwrap();
    let epoch = desc.epoch_bytes().unwrap();
    let tok = crate::crypto::touch_token(&skey_local, &epoch);
    let sk = crate::crypto::wait_sig_key(&tok, &epoch);
    let exp = crate::shard::now_ms() / 1000 + 120;
    let cap = format!(
        "proj-test.{exp}.{}",
        crate::crypto::watch_capability_sig(
            &sk,
            &state.deployment.raw_adapter_sref("wt"),
            &desc.stream_epoch,
            "by-customer",
            &khex,
            "GET",
            exp,
        )
    );
    let path = format!(
        "/v1/streams/wt/watches/by-customer/keys/{khex}?cursor={cursor}&timeoutMs=200&cap={cap}"
    );
    let (st, _, b) = preq(addr, "GET", &path, &[], b"").await;
    assert_eq!(st, 200, "{}", String::from_utf8_lossy(&b));
    // The Prisma-Watch Authorization scheme is the preferred carrier.
    let path_hdr =
        format!("/v1/streams/wt/watches/by-customer/keys/{khex}?cursor=now&timeoutMs=200");
    let hdr = format!("Prisma-Watch {cap}");
    let (st, _, b) = preq(addr, "GET", &path_hdr, &[("authorization", &hdr)], b"").await;
    assert_eq!(st, 200, "{}", String::from_utf8_lossy(&b));
    // An EXPIRED capability is refused (§15: <=5 min lifetime).
    let old_exp = crate::shard::now_ms() / 1000 - 3600;
    let stale = format!(
        "{old_exp}.{}",
        crate::crypto::watch_capability_sig(
            &sk,
            &state.deployment.raw_adapter_sref("wt"),
            &desc.stream_epoch,
            "by-customer",
            &khex,
            "GET",
            old_exp,
        )
    );
    let path = format!(
        "/v1/streams/wt/watches/by-customer/keys/{khex}?cursor=now&timeoutMs=200&cap={stale}"
    );
    let (st, _, _) = preq(addr, "GET", &path, &[], b"").await;
    assert_eq!(st, 403, "expired capability must be refused");
    // A capability minted BEYOND the 5-minute maximum is refused too.
    let far = crate::shard::now_ms() / 1000 + 86_400;
    let long = format!(
        "proj-test.{far}.{}",
        crate::crypto::watch_capability_sig(
            &sk,
            &state.deployment.raw_adapter_sref("wt"),
            &desc.stream_epoch,
            "by-customer",
            &khex,
            "GET",
            far,
        )
    );
    let path = format!(
        "/v1/streams/wt/watches/by-customer/keys/{khex}?cursor=now&timeoutMs=200&cap={long}"
    );
    let (st, _, _) = preq(addr, "GET", &path, &[], b"").await;
    assert_eq!(st, 403, "over-lifetime capability must be refused");
    // Wrong cap, no key: 403.
    let path = format!(
        "/v1/streams/wt/watches/by-customer/keys/{khex}?cursor=now&timeoutMs=200&cap=1.deadbeef"
    );
    let (st, _, _) = preq(addr, "GET", &path, &[], b"").await;
    assert_eq!(st, 403);

    // A stale (foreign-epoch) cursor is an explicit resync.
    let path =
        format!("/v1/streams/wt/watches/by-customer/keys/{khex}?cursor=999999:1&timeoutMs=200");
    let (st, _, b) = preq(
        addr,
        "GET",
        &path,
        &[("prisma-encryption-key", PRISMA_KEY)],
        b"",
    )
    .await;
    assert_eq!(st, 200);
    let v: serde_json::Value = serde_json::from_slice(&b).unwrap();
    assert_eq!(v["invalidated"], true);
    assert_eq!(v["reason"], "resync");
    engine_shutdown(&state).await;
}

/// A signed watch URL is a DURABLE capability. The signature is checked
/// against the verifier persisted in the descriptor at create — not
/// against a cached stream key — so an issued URL keeps working on a
/// process that has never seen the collection, after a restart, and for
/// a collection nobody has appended to in days. The second rig here is
/// exactly that stranger: same store, cold caches, no key ever
/// presented to it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn watch_urls_verify_on_a_process_that_never_saw_the_key() {
    let store = mem();
    let (state, addr) = http_rig(store.clone()).await;
    let (st, _, b) = preq(
        addr,
        "PUT",
        "/v1/streams/wsig",
        &[("prisma-encryption-key", PRISMA_KEY)],
        br#"{"format":{"kind":"json"},"watches":[{"name":"by-customer","fields":["/customerId"]}]}"#,
    )
    .await;
    assert_eq!(st, 201, "{}", String::from_utf8_lossy(&b));

    // Derive exactly the way the SDK does: metadata carries the
    // incarnation salt, and everything else comes from the stream key.
    let (st, _, b) = preq(addr, "GET", "/v1/streams/wsig", &[], b"").await;
    assert_eq!(st, 200);
    let meta: serde_json::Value = serde_json::from_slice(&b).unwrap();
    let epoch_hex = meta["epoch"].as_str().expect("epoch exposed for watches");
    let epoch: [u8; 16] = crate::crypto::unhex(epoch_hex).unwrap().try_into().unwrap();
    let key = crate::crypto::StreamKey::from_b64(PRISMA_KEY).unwrap();
    let khex = crate::product::watch_key_hex(
        "by-customer",
        &["/customerId".to_string()],
        &[r#""c1""#.to_string()],
    );
    let tok = crate::crypto::touch_token(&key, &epoch);
    let exp = crate::shard::now_ms() / 1000 + 120;

    // A second server over the same store: never saw the key, never
    // absorbed a record, never issued this URL — its OWN incarnation.
    let (state2, addr2) = http_rig_at(store, RigRuntime::incarnation(1)).await;
    let cap = format!(
        "proj-test.{exp}.{}",
        crate::crypto::watch_capability_sig(
            &crate::crypto::wait_sig_key(&tok, &epoch),
            &state2.deployment.raw_adapter_sref("wsig"),
            epoch_hex,
            "by-customer",
            &khex,
            "GET",
            exp,
        )
    );
    let path = format!(
        "/v1/streams/wsig/watches/by-customer/keys/{khex}?cursor=now&timeoutMs=150&cap={cap}"
    );
    let (st, _, b) = preq(addr2, "GET", &path, &[], b"").await;
    assert_eq!(
        st,
        200,
        "cold process must verify: {}",
        String::from_utf8_lossy(&b)
    );

    // A forged capability is refused, on both.
    let bad_exp = crate::shard::now_ms() / 1000 + 120;
    let bad = format!(
        "/v1/streams/wsig/watches/by-customer/keys/{khex}?cursor=now&timeoutMs=150&cap={bad_exp}.00000000000000000000000000000000"
    );
    let (st, _, _) = preq(addr2, "GET", &bad, &[], b"").await;
    assert_eq!(st, 403);
    let (st, _, _) = preq(addr, "GET", &bad, &[], b"").await;
    assert_eq!(st, 403);
    engine_shutdown(&state).await;
    engine_shutdown(&state2).await;
}

/// RED (Søren review): a verified watch capability is a bearer
/// credential for its project, but the wait path never consults the
/// CURRENT project policy — a capability keeps working after the
/// project is suspended, until its own expiry.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn watch_capability_respects_project_suspension() {
    let scopes = "streams.create streams.records.append streams.records.read \
                  streams.metadata.read streams.watches.manage";
    let (state, addr, tok) = sr_rig("proj-capb", "ws_capb", "c_capb", "cap-1", scopes).await;
    let auth = ("authorization", tok.as_str());
    let ekey = ("prisma-encryption-key", PRISMA_KEY);

    let (st, _, _) = preq(
        addr,
        "PUT",
        "/v1/streams/orders",
        &[ekey, auth],
        br#"{"format":{"kind":"json"},"watches":[{"name":"by-x","fields":["/id"]}]}"#,
    )
    .await;
    assert_eq!(st, 201);
    let bref = crate::tenant::ProjectId::new("proj-capb")
        .unwrap()
        .stream_ref("orders");
    let desc = state.registry.get(&bref).await.unwrap().unwrap();
    let epoch = desc.epoch_bytes().unwrap();
    let khex = format!("{:016x}", 7u64);
    let tokk = crate::crypto::touch_token(&skey(), &epoch);
    let sk = crate::crypto::wait_sig_key(&tokk, &epoch);
    let exp = crate::shard::now_ms() / 1000 + 120;
    let cap = format!(
        "proj-capb.{exp}.{}",
        crate::crypto::watch_capability_sig(
            &sk,
            &bref,
            &desc.stream_epoch,
            "by-x",
            &khex,
            "GET",
            exp,
        )
    );
    let path =
        format!("/v1/streams/orders/watches/by-x/keys/{khex}?cursor=now&timeoutMs=100&cap={cap}");
    // Active: the capability authorizes the wait.
    let (st, _, b) = preq(addr, "GET", &path, &[], b"").await;
    assert_eq!(st, 200, "active wait: {}", String::from_utf8_lossy(&b));

    // Suspend the project (policy feed v2). The capability has not
    // expired — the CURRENT policy must refuse the wait anyway.
    let pid = crate::tenant::ProjectId::new("proj-capb").unwrap();
    let mut projects = std::collections::HashMap::new();
    projects.insert(
        pid.clone(),
        crate::project_policy::ProjectPolicy {
            project_id: pid,
            workspace_id: crate::tenant::WorkspaceId::new("ws_capb").unwrap(),
            cell_id: std::sync::Arc::from("test-cell"),
            project_policy_version: 2,
            ownership_version: 1,
            status: crate::project_policy::ProjectStatus::Suspended,
            quotas: crate::project_policy::ProjectQuotas::default(),
        },
    );
    state
        .auth
        .publish_policies(crate::project_policy::PolicySnapshot {
            projects,
            fetched_at_unix: crate::shard::now_ms() / 1000,
            feed_version: 2,
        })
        .unwrap();
    let (st, _, b) = preq(addr, "GET", &path, &[], b"").await;
    assert_eq!(
        st,
        403,
        "a suspended project's capability must be refused: {} {}",
        st,
        String::from_utf8_lossy(&b)
    );
    engine_shutdown(&state).await;
}

/// RED (Søren review): capability waits bypass project admission — a
/// project at its inflight ceiling can still open unbounded 25-second
/// waiters through capability URLs.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn watch_capability_waits_occupy_project_admission() {
    let scopes = "streams.create streams.records.append streams.records.read \
                  streams.metadata.read streams.watches.manage";
    let (state, addr, tok) = sr_rig("proj-capq", "ws_capq", "c_capq", "capq-1", scopes).await;
    let auth = ("authorization", tok.as_str());
    let ekey = ("prisma-encryption-key", PRISMA_KEY);
    let (st, _, _) = preq(
        addr,
        "PUT",
        "/v1/streams/orders",
        &[ekey, auth],
        br#"{"format":{"kind":"json"},"watches":[{"name":"by-x","fields":["/id"]}]}"#,
    )
    .await;
    assert_eq!(st, 201);
    // Tighten the project's inflight ceiling to 1 AFTER setup.
    let pid = crate::tenant::ProjectId::new("proj-capq").unwrap();
    let mut projects = std::collections::HashMap::new();
    projects.insert(
        pid.clone(),
        crate::project_policy::ProjectPolicy {
            project_id: pid,
            workspace_id: crate::tenant::WorkspaceId::new("ws_capq").unwrap(),
            cell_id: std::sync::Arc::from("test-cell"),
            project_policy_version: 2,
            ownership_version: 1,
            status: crate::project_policy::ProjectStatus::Active,
            quotas: crate::project_policy::ProjectQuotas {
                max_inflight_requests: 1,
                ..Default::default()
            },
        },
    );
    state
        .auth
        .publish_policies(crate::project_policy::PolicySnapshot {
            projects,
            fetched_at_unix: crate::shard::now_ms() / 1000,
            feed_version: 2,
        })
        .unwrap();
    let bref = crate::tenant::ProjectId::new("proj-capq")
        .unwrap()
        .stream_ref("orders");
    let desc = state.registry.get(&bref).await.unwrap().unwrap();
    let epoch = desc.epoch_bytes().unwrap();
    let khex = format!("{:016x}", 9u64);
    let tokk = crate::crypto::touch_token(&skey(), &epoch);
    let sk = crate::crypto::wait_sig_key(&tokk, &epoch);
    let exp = crate::shard::now_ms() / 1000 + 120;
    let cap = format!(
        "proj-capq.{exp}.{}",
        crate::crypto::watch_capability_sig(
            &sk,
            &bref,
            &desc.stream_epoch,
            "by-x",
            &khex,
            "GET",
            exp,
        )
    );
    // Waiter 1 holds the project's single admission slot for ~3s.
    let long_path =
        format!("/v1/streams/orders/watches/by-x/keys/{khex}?cursor=now&timeoutMs=3000&cap={cap}");
    let h = tokio::spawn(async move { preq(addr, "GET", &long_path, &[], b"").await });
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    // Waiter 2 must be refused with the project concurrency class.
    let short_path =
        format!("/v1/streams/orders/watches/by-x/keys/{khex}?cursor=now&timeoutMs=100&cap={cap}");
    let (st, _, b) = preq(addr, "GET", &short_path, &[], b"").await;
    assert_eq!(
        st,
        429,
        "the second capability wait must hit the project ceiling: {} {}",
        st,
        String::from_utf8_lossy(&b)
    );
    let _ = h.await;
    engine_shutdown(&state).await;
}

/// RED (review finding 3): watch capabilities must FAIL CLOSED when
/// project policy is stale, exactly like customer-JWT requests do.
/// Today status_and_quotas reads the last-loaded snapshot without a
/// freshness check, so capability traffic keeps serving on a policy
/// that may be minutes past a suspension the cell never heard about.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stale_policy_fails_watch_capabilities_closed() {
    let scopes = "streams.create streams.records.append streams.records.read \
                  streams.metadata.read streams.watches.manage";
    let (state, addr, tok) = sr_rig("proj-stalec", "ws_stalec", "c_stalec", "stc-1", scopes).await;
    let auth = ("authorization", tok.as_str());
    let ekey = ("prisma-encryption-key", PRISMA_KEY);
    let (st, _, _) = preq(
        addr,
        "PUT",
        "/v1/streams/orders",
        &[ekey, auth],
        br#"{"format":{"kind":"json"},"watches":[{"name":"by-x","fields":["/id"]}]}"#,
    )
    .await;
    assert_eq!(st, 201);
    let bref = crate::tenant::ProjectId::new("proj-stalec")
        .unwrap()
        .stream_ref("orders");
    let desc = state.registry.get(&bref).await.unwrap().unwrap();
    let epoch = desc.epoch_bytes().unwrap();
    let khex = format!("{:016x}", 7u64);
    let tokk = crate::crypto::touch_token(&skey(), &epoch);
    let sk = crate::crypto::wait_sig_key(&tokk, &epoch);
    let exp = crate::shard::now_ms() / 1000 + 120;
    let cap = format!(
        "proj-stalec.{exp}.{}",
        crate::crypto::watch_capability_sig(
            &sk,
            &bref,
            &desc.stream_epoch,
            "by-x",
            &khex,
            "GET",
            exp
        )
    );
    let path =
        format!("/v1/streams/orders/watches/by-x/keys/{khex}?cursor=now&timeoutMs=100&cap={cap}");
    // Fresh Active policy: the wait serves.
    let (st, _, b) = preq(addr, "GET", &path, &[], b"").await;
    assert_eq!(st, 200, "fresh wait: {}", String::from_utf8_lossy(&b));

    // Republish Active — but STALE: fetched past the freshness window.
    // Customer JWTs already 503 in this state; the capability must too.
    let pid = crate::tenant::ProjectId::new("proj-stalec").unwrap();
    let mut projects = std::collections::HashMap::new();
    projects.insert(
        pid.clone(),
        crate::project_policy::ProjectPolicy {
            project_id: pid,
            workspace_id: crate::tenant::WorkspaceId::new("ws_stalec").unwrap(),
            cell_id: std::sync::Arc::from("test-cell"),
            project_policy_version: 2,
            ownership_version: 1,
            status: crate::project_policy::ProjectStatus::Active,
            quotas: crate::project_policy::ProjectQuotas::default(),
        },
    );
    state
        .auth
        .publish_policies(crate::project_policy::PolicySnapshot {
            projects,
            fetched_at_unix: crate::shard::now_ms() / 1000
                - crate::auth::POLICY_STALENESS_MAX_SECS
                - 1,
            feed_version: 2,
        })
        .unwrap();
    let (st, _, b) = preq(addr, "GET", &path, &[], b"").await;
    assert_eq!(
        st,
        503,
        "stale policy must fail the capability CLOSED (got {st}: {})",
        String::from_utf8_lossy(&b)
    );
    engine_shutdown(&state).await;
}
