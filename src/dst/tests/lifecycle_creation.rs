//! Lifecycle creation.

use super::fixture_failpoints::gap_lock;
use super::fixture_http::{engine_shutdown, http_rig};
use super::fixture_requests::{PRISMA_KEY, RIG_KEY_B64, hreq, preq};
use super::fixture_storage::mem;

/// Readiness is not a stopwatch. An abandoned initialization used to
/// stop blocking reads once its claim aged past 15 s, so a stream whose
/// creator died mid-write started serving as complete — the original
/// field anomaly with a delay. A stale claim decides who may REDO the
/// work; it never publishes half-built content.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_stale_initialization_never_becomes_visible() {
    let store = mem();
    let (state, addr) = http_rig(store).await;
    let ct = [("content-type", "application/json")];
    let (st, _, _) = hreq(addr, "PUT", "/v1/stream/stale1", &ct, br#"[{"n":1}]"#).await;
    assert_eq!(st, 201);
    // A creator that died long ago: claim present, ancient.
    state
        .registry
        .cas_update(&state.deployment.raw_adapter_sref("stale1"), |d| {
            d.init = Some(crate::registry::InitState {
                request_hash: "abandoned".into(),
                key_fingerprint: d.key_fingerprint.clone(),
                claimed_ms: crate::shard::now_ms() - crate::registry::INIT_CLAIM_MS * 100,
            });
            true
        })
        .await
        .unwrap();
    state
        .registry
        .invalidate(&state.deployment.raw_adapter_sref("stale1"));

    let (st, _, _) = hreq(addr, "GET", "/v1/stream/stale1", &[], b"").await;
    assert_eq!(st, 503, "an abandoned create must not read as complete");
    let (st, _, _) = hreq(addr, "POST", "/v1/stream/stale1", &ct, br#"[{"n":2}]"#).await;
    assert_eq!(st, 503, "…nor accept appends");
    let (st, _, _) = preq(
        addr,
        "GET",
        "/v1/streams/stale1",
        &[("prisma-encryption-key", PRISMA_KEY)],
        b"",
    )
    .await;
    assert_eq!(st, 503, "…nor describe itself through the product route");
    // …and it is not in the catalog.
    let (st, _, b) = preq(addr, "GET", "/v1/streams?limit=100", &[], b"").await;
    assert_eq!(st, 200);
    let v: serde_json::Value = serde_json::from_slice(&b).unwrap();
    let names: Vec<&str> = v["streams"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|s| s["name"].as_str())
        .collect();
    assert!(
        !names.contains(&"stale1"),
        "half-built stream in catalog: {names:?}"
    );
    engine_shutdown(&state).await;
}

/// Resuming an initialization writes the initial content with the
/// REQUEST's key. The resume path skips the idempotent-PUT validation
/// where the key would normally be compared, so a replay of the same
/// body under a different key completed the creation with a key the
/// descriptor's own fingerprint does not match — a stream that cannot
/// decrypt its first record.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_initialization_cannot_be_resumed_with_another_key() {
    const OTHER_KEY: &str = "CQkJCQkJCQkJCQkJCQkJCQkJCQkJCQkJCQkJCQkJCQk=";
    let store = mem();
    let (state, addr) = http_rig(store).await;
    let body = br#"[{"n":1}]"#;
    let mine = [
        ("content-type", "application/json"),
        ("stream-encryption-key", RIG_KEY_B64),
    ];
    let theirs = [
        ("content-type", "application/json"),
        ("stream-encryption-key", OTHER_KEY),
    ];
    let (st, _, _) = hreq(addr, "PUT", "/v1/stream/keyinit", &mine, body).await;
    assert_eq!(st, 201);
    // Reopen the initialization, as a crashed creator would leave it.
    let plant = |stale: bool| {
        let state = state.clone();
        async move {
            state
                .registry
                .cas_update(&state.deployment.raw_adapter_sref("keyinit"), |d| {
                    d.init = Some(crate::registry::InitState {
                        request_hash: crate::http::create_request_hash(
                            "application/json",
                            None,
                            None,
                            false,
                            br#"[{"n":1}]"#,
                            None,
                        ),
                        key_fingerprint: d.key_fingerprint.clone(),
                        claimed_ms: if stale {
                            crate::shard::now_ms() - crate::registry::INIT_CLAIM_MS * 100
                        } else {
                            crate::shard::now_ms()
                        },
                    });
                    true
                })
                .await
                .unwrap();
            state
                .registry
                .invalidate(&state.deployment.raw_adapter_sref("keyinit"));
        }
    };
    for stale in [false, true] {
        plant(stale).await;
        let (st, _, b) = hreq(addr, "PUT", "/v1/stream/keyinit", &theirs, body).await;
        assert_eq!(
            st,
            403,
            "wrong key resumed an initialization (stale={stale}): {}",
            String::from_utf8_lossy(&b)
        );
        // The descriptor is untouched: still initializing, still ours.
        let d = state
            .registry
            .get(&state.deployment.raw_adapter_sref("keyinit"))
            .await
            .unwrap()
            .unwrap();
        assert!(d.init.is_some(), "a refused resume must not complete it");
    }
    // The RIGHT key resumes and completes it.
    plant(false).await;
    let (st, _, _) = hreq(addr, "PUT", "/v1/stream/keyinit", &mine, body).await;
    assert!(st == 200 || st == 201);
    let d = state
        .registry
        .get(&state.deployment.raw_adapter_sref("keyinit"))
        .await
        .unwrap()
        .unwrap();
    assert!(d.init.is_none(), "the right key completes initialization");
    engine_shutdown(&state).await;
}

/// A catalog page that crosses a dense run of tombstoned, expired or
/// half-built streams comes back underfull. That used to be read as
/// "end of catalog", which made every live stream after the run
/// unreachable — the walk continues while the PROVIDER has more.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn catalog_paging_survives_dense_dead_entries() {
    let store = mem();
    let (state, addr) = http_rig(store).await;
    let key = [("prisma-encryption-key", PRISMA_KEY)];
    // 40 collections; everything except the last two is then killed,
    // so any page-sized window early in the walk is all corpses.
    for i in 0..40 {
        let (st, _, _) = preq(
            addr,
            "PUT",
            &format!("/v1/streams/cat{i:03}"),
            &key,
            br#"{"format":{"kind":"json"}}"#,
        )
        .await;
        assert_eq!(st, 201);
    }
    for i in 0..38 {
        let (st, _, _) = preq(addr, "DELETE", &format!("/v1/streams/cat{i:03}"), &key, b"").await;
        assert!(st == 204 || st == 200, "delete cat{i:03}: {st}");
    }
    // Walk with a small limit: the first pages are empty but not final.
    let mut seen: Vec<String> = Vec::new();
    let mut cursor: Option<String> = None;
    for _ in 0..40 {
        let path = match &cursor {
            None => "/v1/streams?limit=3".to_string(),
            Some(c) => format!("/v1/streams?limit=3&cursor={c}"),
        };
        let (st, _, b) = preq(addr, "GET", &path, &[], b"").await;
        assert_eq!(st, 200, "{}", String::from_utf8_lossy(&b));
        let v: serde_json::Value = serde_json::from_slice(&b).unwrap();
        for s in v["streams"].as_array().unwrap() {
            seen.push(s["name"].as_str().unwrap().to_string());
        }
        match v["cursor"].as_str() {
            Some(c) => cursor = Some(c.to_string()),
            None => break,
        }
    }
    assert!(
        seen.contains(&"cat038".to_string()) && seen.contains(&"cat039".to_string()),
        "live streams behind a run of dead ones were unreachable: {seen:?}"
    );
    engine_shutdown(&state).await;
}

/// CRT-007: a create whose INITIAL CONTENT write fails must not
/// publish Ready — and the exact replay resumes the same
/// initialization and delivers the content exactly once.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn create_replay_recovers_from_a_failed_initial_write() {
    let _serial = gap_lock().lock().await;
    let store = mem();
    let (state, addr) = http_rig(store).await;
    let ct = [("content-type", "application/json")];

    // Park the initial-content append pre-enqueue so the failpoint can
    // be armed with the descriptor's real identity, deterministically.
    let before = crate::failpoints::parked(crate::failpoints::Fp::InitBeforeSeed, "crt007");
    crate::failpoints::park_init_before_seed("crt007");
    let body = br#"[{"seed":1},{"seed":2}]"#;
    let creator =
        tokio::spawn(async move { hreq(addr, "PUT", "/v1/stream/crt007", &ct, body).await });
    let mut parked = false;
    for _ in 0..300 {
        if crate::failpoints::parked(crate::failpoints::Fp::InitBeforeSeed, "crt007") > before {
            parked = true;
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    assert!(parked, "the initial append never reached the park");
    state
        .registry
        .invalidate(&state.deployment.raw_adapter_sref("crt007"));
    let desc = state
        .registry
        .get(&state.deployment.raw_adapter_sref("crt007"))
        .await
        .unwrap()
        .unwrap();
    assert!(desc.init.is_some(), "no initialization claim");
    let seg = desc.resolve_segment("");
    let route = desc.segment_route_by_id(seg.seg_id).unwrap();
    let identity = desc.dynamic_segment_identity(seg.seg_id);
    let engine = state.engine_for(&route).await.unwrap();
    engine.fail_next_group_for(identity);
    crate::failpoints::release_init_before_seed("crt007");

    let (cs, _, _) = creator.await.unwrap();
    assert_eq!(
        engine.group_failures_tripped(),
        1,
        "the failpoint never fired"
    );
    // 408 (outcome unknown) and 5xx are both honest here — anything
    // but success.
    assert!(
        cs >= 400,
        "create reported success over a failed seed write: {cs}"
    );
    state
        .registry
        .invalidate(&state.deployment.raw_adapter_sref("crt007"));
    let d = state
        .registry
        .get(&state.deployment.raw_adapter_sref("crt007"))
        .await
        .unwrap()
        .unwrap();
    assert!(
        d.init.is_some(),
        "a failed initialization published readiness"
    );

    // The exact replay resumes and completes.
    let (st, _, _) = hreq(addr, "PUT", "/v1/stream/crt007", &ct, body).await;
    assert!(st == 200 || st == 201, "replay: {st}");
    state
        .registry
        .invalidate(&state.deployment.raw_adapter_sref("crt007"));
    let d = state
        .registry
        .get(&state.deployment.raw_adapter_sref("crt007"))
        .await
        .unwrap()
        .unwrap();
    assert!(d.init.is_none(), "replay did not publish readiness");
    let (_, _, bd) = hreq(addr, "GET", "/v1/stream/crt007", &[], b"").await;
    let recs: Vec<serde_json::Value> = serde_json::from_slice(&bd).unwrap();
    assert_eq!(recs.len(), 2, "seed content exactly once: {recs:?}");
    engine_shutdown(&state).await;
}

/// AUDIT P0 (the field create anomaly, made deterministic): a replayed
/// PUT must never observe a published-but-uninitialized descriptor and
/// answer success for a stream whose initial content never landed. The
/// replay JOINS the initialization; a DIFFERENT request conflicts; and
/// reads/appends against an initializing stream get a retryable answer
/// rather than an empty stream.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn create_replay_never_loses_the_initial_body() {
    let store = mem();
    let (state, addr) = http_rig(store).await;
    let ct = [("content-type", "application/json")];

    // Two identical PUTs racing (an edge replay). Exactly one creates;
    // BOTH must see the initial content durable when they answer.
    let a =
        tokio::spawn(
            async move { hreq(addr, "PUT", "/v1/stream/replay1", &ct, br#"[{"n":1}]"#).await },
        );
    let b =
        tokio::spawn(
            async move { hreq(addr, "PUT", "/v1/stream/replay1", &ct, br#"[{"n":1}]"#).await },
        );
    let (sa, _, ba) = a.await.unwrap();
    let (sb, _, bb) = b.await.unwrap();
    assert!(
        sa == 201 || sa == 200,
        "A {sa}: {}",
        String::from_utf8_lossy(&ba)
    );
    assert!(
        sb == 201 || sb == 200,
        "B {sb}: {}",
        String::from_utf8_lossy(&bb)
    );
    let (st, _, body) = hreq(addr, "GET", "/v1/stream/replay1", &[], b"").await;
    assert_eq!(st, 200);
    let recs: Vec<serde_json::Value> = serde_json::from_slice(&body).unwrap();
    assert_eq!(recs.len(), 1, "initial body must be durable: {recs:?}");
    assert_eq!(recs[0]["n"], 1);

    // A descriptor stuck in Initializing (the creator died): reads and
    // appends are retryable, NOT an empty stream, and the SAME request
    // resumes it while a different one conflicts.
    let desc = state
        .registry
        .get(&state.deployment.raw_adapter_sref("replay1"))
        .await
        .unwrap()
        .unwrap();
    assert!(desc.init.is_none(), "a completed create publishes Ready");
    // Plant an initializing incarnation by hand (a crashed creator).
    let (st, _, _) = hreq(addr, "PUT", "/v1/stream/replay2", &ct, br#"[{"n":7}]"#).await;
    assert_eq!(st, 201);
    state
        .registry
        .cas_update(&state.deployment.raw_adapter_sref("replay2"), |d| {
            d.init = Some(crate::registry::InitState {
                request_hash: "deadbeef".into(),
                key_fingerprint: d.key_fingerprint.clone(),
                claimed_ms: crate::shard::now_ms(),
            });
            true
        })
        .await
        .unwrap();
    state
        .registry
        .invalidate(&state.deployment.raw_adapter_sref("replay2"));
    let (st, _, _) = hreq(addr, "GET", "/v1/stream/replay2", &[], b"").await;
    assert_eq!(st, 503, "reads of an initializing stream are retryable");
    let (st, _, _) = hreq(addr, "POST", "/v1/stream/replay2", &ct, br#"[{"n":8}]"#).await;
    assert_eq!(st, 503, "appends to an initializing stream are retryable");
    // A DIFFERENT creation request conflicts rather than hijacking it.
    let (st, _, _) = hreq(addr, "PUT", "/v1/stream/replay2", &ct, br#"[{"other":1}]"#).await;
    assert_eq!(
        st, 409,
        "a different request must not steal an in-flight create"
    );

    // A STALE claim (dead creator) stops blocking so the name is never
    // wedged: the same request takes over and completes it.
    state
        .registry
        .cas_update(&state.deployment.raw_adapter_sref("replay2"), |d| {
            d.init = Some(crate::registry::InitState {
                request_hash: "deadbeef".into(),
                key_fingerprint: d.key_fingerprint.clone(),
                claimed_ms: crate::shard::now_ms() - crate::registry::INIT_CLAIM_MS - 1_000,
            });
            true
        })
        .await
        .unwrap();
    state
        .registry
        .invalidate(&state.deployment.raw_adapter_sref("replay2"));
    let (st, _, _) = hreq(addr, "PUT", "/v1/stream/replay2", &ct, br#"[{"other":1}]"#).await;
    assert_eq!(
        st, 409,
        "stale claim + different config is still a config conflict"
    );
    engine_shutdown(&state).await;
}
