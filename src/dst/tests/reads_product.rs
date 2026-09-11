//! Reads product.

use super::fixture_failpoints::gap_lock;
use super::fixture_http::{engine_shutdown, http_rig};
use super::fixture_requests::{PRISMA_KEY, preq};
use super::fixture_storage::{mem, skey};

/// Stage 6: product keyed reads — signed cursors bind stream + key,
/// pagination reassembles exactly, headers speak Prisma (never
/// Stream-*), and state flags (up-to-date, sealed) survive translation.
#[expect(
    clippy::too_many_lines,
    reason = "product paging scenario; signed cursors bound to stream and key, exact reassembly and the response headers form one causal sequence; helper phases would hide which page broke the binding"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn product_read_pages_and_binds_cursors() {
    let store = mem();
    let (state, addr) = http_rig(store).await;
    let (st, _, _) = preq(
        addr,
        "PUT",
        "/v1/streams/reads",
        &[("prisma-encryption-key", PRISMA_KEY)],
        br#"{"format":{"kind":"json"}}"#,
    )
    .await;
    assert_eq!(st, 201);
    // 5 fat records on c1 (pagination fodder), 3 small on the default key.
    let fat = format!("{{\"pad\":\"{}\"}}", "x".repeat(3000));
    for _ in 0..5 {
        let (st, _, _) = preq(
            addr,
            "POST",
            "/v1/streams/reads/records",
            &[
                ("prisma-encryption-key", PRISMA_KEY),
                ("prisma-routing-key", "c1"),
            ],
            fat.as_bytes(),
        )
        .await;
        assert_eq!(st, 200);
    }
    for n in 0..3 {
        let body = format!("{{\"d\":{n}}}");
        let (st, _, _) = preq(
            addr,
            "POST",
            "/v1/streams/reads/records",
            &[("prisma-encryption-key", PRISMA_KEY)],
            body.as_bytes(),
        )
        .await;
        assert_eq!(st, 200);
    }

    // Full keyed read.
    let (st, h, b) = preq(
        addr,
        "GET",
        "/v1/streams/reads/records?routingKey=c1",
        &[("prisma-encryption-key", PRISMA_KEY)],
        b"",
    )
    .await;
    assert_eq!(st, 200, "{}", String::from_utf8_lossy(&b));
    let recs: Vec<serde_json::Value> = serde_json::from_slice(&b).unwrap();
    assert_eq!(recs.len(), 5);
    assert_eq!(h.get("prisma-up-to-date").map(String::as_str), Some("true"));
    assert!(h.contains_key("prisma-next-cursor"));
    assert!(!h.contains_key("prisma-sealed"));
    // The product surface never leaks protocol headers.
    assert!(!h.contains_key("stream-next-offset"), "raw header leaked");
    assert!(!h.contains_key("stream-up-to-date"));

    // Paginate with a small budget: exact reassembly, no dupes/gaps.
    let mut got = 0usize;
    let mut cursor = String::from("beginning");
    for _ in 0..12 {
        let path = format!("/v1/streams/reads/records?routingKey=c1&cursor={cursor}&maxBytes=4096");
        let (st, h, b) = preq(
            addr,
            "GET",
            &path,
            &[("prisma-encryption-key", PRISMA_KEY)],
            b"",
        )
        .await;
        assert_eq!(st, 200);
        let recs: Vec<serde_json::Value> = serde_json::from_slice(&b).unwrap();
        got += recs.len();
        cursor = h.get("prisma-next-cursor").unwrap().clone();
        if h.get("prisma-up-to-date").map(String::as_str) == Some("true") {
            break;
        }
    }
    assert_eq!(got, 5, "pagination must reassemble exactly");

    // Default-key read sees ONLY the default key's records.
    let (st, _, b) = preq(
        addr,
        "GET",
        "/v1/streams/reads/records",
        &[("prisma-encryption-key", PRISMA_KEY)],
        b"",
    )
    .await;
    assert_eq!(st, 200);
    let recs: Vec<serde_json::Value> = serde_json::from_slice(&b).unwrap();
    assert_eq!(
        recs.len(),
        3,
        "keyed records must not bleed into the default key"
    );

    // A cursor is bound to its routing key: reuse on another key is 400.
    let path = format!("/v1/streams/reads/records?routingKey=other&cursor={cursor}");
    let (st, _, b) = preq(
        addr,
        "GET",
        &path,
        &[("prisma-encryption-key", PRISMA_KEY)],
        b"",
    )
    .await;
    assert_eq!(st, 400, "{}", String::from_utf8_lossy(&b));
    let v: serde_json::Value = serde_json::from_slice(&b).unwrap();
    assert_eq!(v["error"]["code"], "invalid_cursor");

    // A scan cursor on the key endpoint is a different token class.
    let desc = state
        .registry
        .get(&state.deployment.raw_adapter_sref("reads"))
        .await
        .unwrap()
        .unwrap();
    let epoch = desc.epoch_bytes().unwrap();
    let sc = crate::product_cursor::ScanCursor {
        epoch,
        map_version: 0,
        segments: vec![(0, 10)],
        current_index: 0,
        current_offset: 0,
        expires_at_ms: i64::MAX,
    }
    .encode(state.deployment.deployment_tenant(), &skey());
    let path = format!("/v1/streams/reads/records?routingKey=c1&cursor={sc}");
    let (st, _, _) = preq(
        addr,
        "GET",
        &path,
        &[("prisma-encryption-key", PRISMA_KEY)],
        b"",
    )
    .await;
    assert_eq!(st, 400);

    // cursor=now: empty, up-to-date.
    let (st, h, b) = preq(
        addr,
        "GET",
        "/v1/streams/reads/records?routingKey=c1&cursor=now",
        &[("prisma-encryption-key", PRISMA_KEY)],
        b"",
    )
    .await;
    assert_eq!(st, 200);
    assert_eq!(h.get("prisma-up-to-date").map(String::as_str), Some("true"));
    let recs: Vec<serde_json::Value> = serde_json::from_slice(&b).unwrap();
    assert!(recs.is_empty());

    // Wrong encryption key: 403.
    let wrong = "CAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAg";
    let (st, _, _) = preq(
        addr,
        "GET",
        "/v1/streams/reads/records?routingKey=c1",
        &[("prisma-encryption-key", wrong)],
        b"",
    )
    .await;
    assert_eq!(st, 403);

    // Seal, then a tail read reports Prisma-Sealed.
    let (st, _, _) = preq(
        addr,
        "POST",
        "/v1/streams/reads:seal",
        &[("prisma-encryption-key", PRISMA_KEY)],
        b"{}",
    )
    .await;
    assert!(st == 200 || st == 204);
    let (st, h, _) = preq(
        addr,
        "GET",
        "/v1/streams/reads/records?routingKey=c1",
        &[("prisma-encryption-key", PRISMA_KEY)],
        b"",
    )
    .await;
    assert_eq!(st, 200);
    assert_eq!(h.get("prisma-sealed").map(String::as_str), Some("true"));
    engine_shutdown(&state).await;
}

/// Stage 6: product cursors survive a split — pagination hands the
/// cursor across the sealed parent into the successor and reassembles
/// the key's sequence exactly.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn product_read_follows_split_lineage() {
    let _l = gap_lock().lock().await;
    let store = mem();
    let (state, addr) = http_rig(store).await;
    let (st, _, _) = preq(
        addr,
        "PUT",
        "/v1/streams/lin",
        &[("prisma-encryption-key", PRISMA_KEY)],
        br#"{"format":{"kind":"json"}}"#,
    )
    .await;
    assert_eq!(st, 201);
    for n in 0..5 {
        for k in ["ga", "gb"] {
            let body = format!("{{\"k\":\"{k}\",\"n\":{n}}}");
            let (st, _, _) = preq(
                addr,
                "POST",
                "/v1/streams/lin/records",
                &[
                    ("prisma-encryption-key", PRISMA_KEY),
                    ("prisma-routing-key", k),
                ],
                body.as_bytes(),
            )
            .await;
            assert_eq!(st, 200);
        }
    }
    assert!(
        crate::scaler3::execute_split(
            &state,
            &state.deployment.raw_adapter_sref("lin"),
            0,
            0x8000_0000_0000_0000
        )
        .await
    );
    for n in 5..10 {
        for k in ["ga", "gb"] {
            let body = format!("{{\"k\":\"{k}\",\"n\":{n}}}");
            let (st, _, _) = preq(
                addr,
                "POST",
                "/v1/streams/lin/records",
                &[
                    ("prisma-encryption-key", PRISMA_KEY),
                    ("prisma-routing-key", k),
                ],
                body.as_bytes(),
            )
            .await;
            assert_eq!(st, 200);
        }
    }
    let mut ns: Vec<i64> = Vec::new();
    let mut cursor = String::from("beginning");
    for _ in 0..16 {
        let path = format!("/v1/streams/lin/records?routingKey=ga&cursor={cursor}");
        let (st, h, b) = preq(
            addr,
            "GET",
            &path,
            &[("prisma-encryption-key", PRISMA_KEY)],
            b"",
        )
        .await;
        assert_eq!(st, 200, "{}", String::from_utf8_lossy(&b));
        let recs: Vec<serde_json::Value> = serde_json::from_slice(&b).unwrap();
        for r in &recs {
            assert_eq!(r["k"], "ga");
            ns.push(r["n"].as_i64().unwrap());
        }
        cursor = h.get("prisma-next-cursor").unwrap().clone();
        if h.get("prisma-up-to-date").map(String::as_str) == Some("true") {
            break;
        }
    }
    assert_eq!(
        ns,
        (0..10).collect::<Vec<i64>>(),
        "exact per-key order across the split"
    );
    engine_shutdown(&state).await;
}

/// Stage 6: the long-poll transport — a timeout answers 204 with a
/// rearm cursor; a wake serves the new record.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn product_long_poll_times_out_and_wakes() {
    let store = mem();
    let (state, addr) = http_rig(store).await;
    let (st, _, _) = preq(
        addr,
        "PUT",
        "/v1/streams/lp",
        &[("prisma-encryption-key", PRISMA_KEY)],
        br#"{"format":{"kind":"json"}}"#,
    )
    .await;
    assert_eq!(st, 201);
    let (st, _, _) = preq(
        addr,
        "POST",
        "/v1/streams/lp/records",
        &[
            ("prisma-encryption-key", PRISMA_KEY),
            ("prisma-routing-key", "w"),
        ],
        b"{\"n\":0}",
    )
    .await;
    assert_eq!(st, 200);
    let (st, h, _) = preq(
        addr,
        "GET",
        "/v1/streams/lp/records?routingKey=w",
        &[("prisma-encryption-key", PRISMA_KEY)],
        b"",
    )
    .await;
    assert_eq!(st, 200);
    let cursor = h.get("prisma-next-cursor").unwrap().clone();

    // Timeout: nothing new within waitMs.
    let path = format!("/v1/streams/lp/records:long-poll?routingKey=w&cursor={cursor}&waitMs=200");
    let (st, h, _) = preq(
        addr,
        "GET",
        &path,
        &[("prisma-encryption-key", PRISMA_KEY)],
        b"",
    )
    .await;
    assert_eq!(st, 204);
    assert!(h.contains_key("prisma-next-cursor"));
    assert!(!h.contains_key("stream-next-offset"));

    // Wake: a concurrent append answers the poll with the record.
    let path = format!("/v1/streams/lp/records:long-poll?routingKey=w&cursor={cursor}&waitMs=5000");
    let poll = tokio::spawn(async move {
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
        "/v1/streams/lp/records",
        &[
            ("prisma-encryption-key", PRISMA_KEY),
            ("prisma-routing-key", "w"),
        ],
        b"{\"n\":1}",
    )
    .await;
    assert_eq!(st, 200);
    let (st, h, b) = poll.await.unwrap();
    assert_eq!(st, 200, "{}", String::from_utf8_lossy(&b));
    let recs: Vec<serde_json::Value> = serde_json::from_slice(&b).unwrap();
    assert_eq!(recs.len(), 1);
    assert_eq!(recs[0]["n"], 1);
    assert!(h.contains_key("prisma-next-cursor"));
    engine_shutdown(&state).await;
}

/// Stage 6: cross-key scan — every record at snapshot creation exactly
/// once, later appends excluded, expiry honored, token classes enforced.
#[expect(
    clippy::too_many_lines,
    reason = "snapshot scan scenario; the records at snapshot creation, later appends and expiry form one causal sequence; helper phases would hide which record leaked into or out of the snapshot"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn product_scan_is_snapshot_exact() {
    let store = mem();
    let (state, addr) = http_rig(store).await;
    let (st, _, _) = preq(
        addr,
        "PUT",
        "/v1/streams/scn",
        &[("prisma-encryption-key", PRISMA_KEY)],
        br#"{"format":{"kind":"json"}}"#,
    )
    .await;
    assert_eq!(st, 201);
    for (k, n) in [("a", 0), ("a", 1), ("b", 0), ("", 0)] {
        let body = format!("{{\"k\":\"{k}\",\"n\":{n}}}");
        let mut hdrs = vec![("prisma-encryption-key", PRISMA_KEY)];
        if !k.is_empty() {
            hdrs.push(("prisma-routing-key", k));
        }
        let (st, _, _) = preq(
            addr,
            "POST",
            "/v1/streams/scn/records",
            &hdrs,
            body.as_bytes(),
        )
        .await;
        assert_eq!(st, 200);
    }

    // One-page scan: complete, all four records with their keys.
    let (st, h, b) = preq(
        addr,
        "GET",
        "/v1/streams/scn:scan",
        &[("prisma-encryption-key", PRISMA_KEY)],
        b"",
    )
    .await;
    assert_eq!(st, 200, "{}", String::from_utf8_lossy(&b));
    assert_eq!(
        h.get("prisma-scan-complete").map(String::as_str),
        Some("true")
    );
    assert!(!h.contains_key("prisma-next-scan-cursor"));
    let items: Vec<serde_json::Value> = serde_json::from_slice(&b).unwrap();
    assert_eq!(items.len(), 4);
    let mut seen: Vec<(String, i64)> = items
        .iter()
        .map(|i| {
            (
                i["routingKey"].as_str().unwrap().to_string(),
                i["value"]["n"].as_i64().unwrap(),
            )
        })
        .collect();
    seen.sort();
    assert_eq!(
        seen,
        vec![
            ("".into(), 0),
            ("a".into(), 0),
            ("a".into(), 1),
            ("b".into(), 0)
        ]
    );

    // Paginated scan with a snapshot bound: fat records force pages; a
    // record appended MID-SCAN must not appear.
    let fat = format!("{{\"pad\":\"{}\"}}", "y".repeat(3000));
    for _ in 0..4 {
        let (st, _, _) = preq(
            addr,
            "POST",
            "/v1/streams/scn/records",
            &[
                ("prisma-encryption-key", PRISMA_KEY),
                ("prisma-routing-key", "fat"),
            ],
            fat.as_bytes(),
        )
        .await;
        assert_eq!(st, 200);
    }
    let (st, h, b) = preq(
        addr,
        "GET",
        "/v1/streams/scn:scan?maxBytes=4096",
        &[("prisma-encryption-key", PRISMA_KEY)],
        b"",
    )
    .await;
    assert_eq!(st, 200);
    let mut total: usize = serde_json::from_slice::<Vec<serde_json::Value>>(&b)
        .unwrap()
        .len();
    let mut cursor = h.get("prisma-next-scan-cursor").cloned();
    assert!(cursor.is_some(), "fat records must not fit one 4 KiB page");
    // Mid-scan append: outside the snapshot.
    let (st, _, _) = preq(
        addr,
        "POST",
        "/v1/streams/scn/records",
        &[
            ("prisma-encryption-key", PRISMA_KEY),
            ("prisma-routing-key", "late"),
        ],
        b"{\"late\":true}",
    )
    .await;
    assert_eq!(st, 200);
    for _ in 0..24 {
        let Some(c) = cursor.clone() else { break };
        let path = format!("/v1/streams/scn:scan?cursor={c}&maxBytes=4096");
        let (st, h, b) = preq(
            addr,
            "GET",
            &path,
            &[("prisma-encryption-key", PRISMA_KEY)],
            b"",
        )
        .await;
        assert_eq!(st, 200, "{}", String::from_utf8_lossy(&b));
        let items: Vec<serde_json::Value> = serde_json::from_slice(&b).unwrap();
        for i in &items {
            assert_ne!(
                i["routingKey"], "late",
                "snapshot must exclude mid-scan appends"
            );
        }
        total += items.len();
        if h.get("prisma-scan-complete").map(String::as_str) == Some("true") {
            cursor = None;
        } else {
            cursor = Some(h.get("prisma-next-scan-cursor").unwrap().clone());
        }
    }
    assert!(cursor.is_none(), "scan must complete");
    assert_eq!(
        total, 8,
        "4 originals + 4 fat, exactly once, no late record"
    );

    // Expired cursor: 410 scan_expired.
    let desc = state
        .registry
        .get(&state.deployment.raw_adapter_sref("scn"))
        .await
        .unwrap()
        .unwrap();
    let epoch = desc.epoch_bytes().unwrap();
    let expired = crate::product_cursor::ScanCursor {
        epoch,
        map_version: 0,
        segments: vec![(0, 8)],
        current_index: 0,
        current_offset: 0,
        expires_at_ms: 1,
    }
    .encode(state.deployment.deployment_tenant(), &skey());
    let path = format!("/v1/streams/scn:scan?cursor={expired}");
    let (st, _, b) = preq(
        addr,
        "GET",
        &path,
        &[("prisma-encryption-key", PRISMA_KEY)],
        b"",
    )
    .await;
    assert_eq!(st, 410, "{}", String::from_utf8_lossy(&b));
    let v: serde_json::Value = serde_json::from_slice(&b).unwrap();
    assert_eq!(v["error"]["code"], "scan_expired");

    // A key cursor on the scan endpoint: wrong token class.
    let kc = crate::product_cursor::KeyCursor {
        epoch,
        key_hash: crate::crypto::stream_hash("a"),
        seg_id: 0,
        offset: 0,
    }
    .encode(state.deployment.deployment_tenant(), &skey());
    let path = format!("/v1/streams/scn:scan?cursor={kc}");
    let (st, _, _) = preq(
        addr,
        "GET",
        &path,
        &[("prisma-encryption-key", PRISMA_KEY)],
        b"",
    )
    .await;
    assert_eq!(st, 400);
    engine_shutdown(&state).await;
}

/// Stage 6: scan traverses split lineage — sealed parent + both
/// children, every record exactly once.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn product_scan_traverses_split_lineage() {
    let _l = gap_lock().lock().await;
    let store = mem();
    let (state, addr) = http_rig(store).await;
    let (st, _, _) = preq(
        addr,
        "PUT",
        "/v1/streams/scnlin",
        &[("prisma-encryption-key", PRISMA_KEY)],
        br#"{"format":{"kind":"json"}}"#,
    )
    .await;
    assert_eq!(st, 201);
    for n in 0..5 {
        for k in ["ga", "gb"] {
            let body = format!("{{\"k\":\"{k}\",\"n\":{n}}}");
            let (st, _, _) = preq(
                addr,
                "POST",
                "/v1/streams/scnlin/records",
                &[
                    ("prisma-encryption-key", PRISMA_KEY),
                    ("prisma-routing-key", k),
                ],
                body.as_bytes(),
            )
            .await;
            assert_eq!(st, 200);
        }
    }
    assert!(
        crate::scaler3::execute_split(
            &state,
            &state.deployment.raw_adapter_sref("scnlin"),
            0,
            0x8000_0000_0000_0000
        )
        .await
    );
    for n in 5..10 {
        for k in ["ga", "gb"] {
            let body = format!("{{\"k\":\"{k}\",\"n\":{n}}}");
            let (st, _, _) = preq(
                addr,
                "POST",
                "/v1/streams/scnlin/records",
                &[
                    ("prisma-encryption-key", PRISMA_KEY),
                    ("prisma-routing-key", k),
                ],
                body.as_bytes(),
            )
            .await;
            assert_eq!(st, 200);
        }
    }
    let mut seen: Vec<(String, i64)> = Vec::new();
    let mut cursor: Option<String> = None;
    for _ in 0..24 {
        let path = match &cursor {
            None => "/v1/streams/scnlin:scan?maxBytes=4096".to_string(),
            Some(c) => format!("/v1/streams/scnlin:scan?cursor={c}&maxBytes=4096"),
        };
        let (st, h, b) = preq(
            addr,
            "GET",
            &path,
            &[("prisma-encryption-key", PRISMA_KEY)],
            b"",
        )
        .await;
        assert_eq!(st, 200, "{}", String::from_utf8_lossy(&b));
        let items: Vec<serde_json::Value> = serde_json::from_slice(&b).unwrap();
        for i in &items {
            seen.push((
                i["routingKey"].as_str().unwrap().to_string(),
                i["value"]["n"].as_i64().unwrap(),
            ));
        }
        if h.get("prisma-scan-complete").map(String::as_str) == Some("true") {
            cursor = None;
            break;
        }
        cursor = Some(h.get("prisma-next-scan-cursor").unwrap().clone());
    }
    assert!(cursor.is_none(), "scan must complete");
    let mut sorted = seen.clone();
    sorted.sort();
    sorted.dedup();
    assert_eq!(seen.len(), 20, "10 ga + 10 gb exactly once, got {seen:?}");
    assert_eq!(sorted.len(), 20, "duplicates in scan: {seen:?}");
    engine_shutdown(&state).await;
}

/// Stage 6: bytes-stream scan encodes values as base64.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn product_scan_bytes_stream() {
    let store = mem();
    let (state, addr) = http_rig(store).await;
    let (st, _, _) = preq(
        addr,
        "PUT",
        "/v1/streams/scb",
        &[("prisma-encryption-key", PRISMA_KEY)],
        br#"{"format":{"kind":"bytes"}}"#,
    )
    .await;
    assert_eq!(st, 201);
    let (st, _, _) = preq(
        addr,
        "POST",
        "/v1/streams/scb/records",
        &[
            ("prisma-encryption-key", PRISMA_KEY),
            ("prisma-routing-key", "bin"),
        ],
        b"\x00\x01\xffraw",
    )
    .await;
    assert_eq!(st, 200);
    let (st, h, b) = preq(
        addr,
        "GET",
        "/v1/streams/scb:scan",
        &[("prisma-encryption-key", PRISMA_KEY)],
        b"",
    )
    .await;
    assert_eq!(st, 200, "{}", String::from_utf8_lossy(&b));
    assert_eq!(
        h.get("prisma-scan-complete").map(String::as_str),
        Some("true")
    );
    let items: Vec<serde_json::Value> = serde_json::from_slice(&b).unwrap();
    assert_eq!(items.len(), 1);
    assert_eq!(items[0]["routingKey"], "bin");
    use base64::Engine;
    let raw = base64::engine::general_purpose::STANDARD
        .decode(items[0]["valueB64"].as_str().unwrap())
        .unwrap();
    assert_eq!(raw, b"\x00\x01\xffraw");
    engine_shutdown(&state).await;
}
