//! Billing usage.

use super::fixture_http::{engine_shutdown, http_rig, install_read_spool, install_rollup};
use super::fixture_requests::{PRISMA_KEY, hreq, preq};
use super::fixture_storage::mem;
use std::sync::Arc;

// ---------------------------------------------------------------
// Telemetry cutover (docs/OBSERVABILITY-BILLING.md): the `_` namespace
// belongs to the system planes.
// ---------------------------------------------------------------

/// No customer credential — even a fully valid one — may create, read,
/// or append to a reserved system stream on either public surface, and
/// creation captures the billing tenant identity in the descriptor.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn reserved_namespace_refuses_customer_credentials() {
    let store = mem();
    let (state, addr) = http_rig(store).await;
    let key = [("prisma-encryption-key", PRISMA_KEY)];
    for target in ["_usage", "_ops_metrics", "_ops_events", "_anything"] {
        // Product surface: create / append / read.
        let (st, _, _) = preq(
            addr,
            "PUT",
            &format!("/v1/streams/{target}"),
            &key,
            br#"{"format":{"kind":"json"}}"#,
        )
        .await;
        assert_eq!(st, 403, "product create of {target}");
        let (st, _, _) = preq(
            addr,
            "POST",
            &format!("/v1/streams/{target}/records"),
            &key,
            br#"{"x":1}"#,
        )
        .await;
        assert_eq!(st, 403, "product append to {target}");
        // Raw surface.
        let (st, _, _) = hreq(addr, "PUT", &format!("/v1/stream/{target}"), &[], b"").await;
        assert_eq!(st, 403, "raw create of {target}");
        let (st, _, _) = hreq(addr, "GET", &format!("/v1/stream/{target}"), &[], b"").await;
        assert_eq!(st, 403, "raw read of {target}");
    }
    // A name merely CONTAINING an underscore-led inner segment is a
    // normal customer stream.
    let (st, _, _) = preq(
        addr,
        "PUT",
        "/v1/streams/customers/_acme/orders",
        &key,
        br#"{"format":{"kind":"json"}}"#,
    )
    .await;
    assert_eq!(st, 201, "inner underscore segments are customer names");
    // Billing identity is persisted at creation.
    let desc = state
        .registry
        .get(&state.deployment.raw_adapter_sref("customers/_acme/orders"))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(desc.account_id.as_deref(), Some("acct_test"));
    assert_eq!(desc.project_id.as_str(), "proj-test");
    engine_shutdown(&state).await;
}

/// §5 metering coverage matrix, driven end to end over HTTP: the ONE
/// read meter sees exact payload bytes (never framing, brackets or
/// encryption expansion), operations count where data doesn't, and
/// internal relays add nothing.
#[expect(
    clippy::too_many_lines,
    reason = "read metering matrix scenario; every delivery mode, cursor shape and byte accounting case is checked against one rig and one ledger; splitting the matrix into helpers would hide which case charged which bytes"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn read_meter_covers_the_matrix_exactly() {
    let store = mem();
    let (state, addr) = http_rig(store).await;
    let key = [("prisma-encryption-key", PRISMA_KEY)];
    let (st, _, _) = preq(
        addr,
        "PUT",
        "/v1/streams/meter1",
        &key,
        br#"{"format":{"kind":"json"}}"#,
    )
    .await;
    assert_eq!(st, 201);
    // Two records with EXACTLY known payload sizes.
    let p1 = br#"{"n":1,"pad":"aaaaaaaaaa"}"#; // 26 bytes
    let p2 = br#"{"n":22,"pad":"bbbbbb"}"#; // 23 bytes
    for (k, p) in [("k1", &p1[..]), ("k2", &p2[..])] {
        let (st, _, _) = preq(
            addr,
            "POST",
            "/v1/streams/meter1/records",
            &[
                ("prisma-encryption-key", PRISMA_KEY),
                ("prisma-routing-key", k),
            ],
            p,
        )
        .await;
        assert_eq!(st, 200);
    }
    let row = |state: &Arc<crate::http::AppState>, name: &str| -> crate::billing::RowDelta {
        state
            .billing
            .reads()
            .snapshot_active()
            .into_iter()
            .find(|(id, _)| id.stream_name == name)
            .map(|(_, d)| d)
            .unwrap_or_default()
    };
    let after_appends = row(&state, "meter1");
    assert_eq!(after_appends.append_requests, 2, "two append ops");
    assert_eq!(
        after_appends.read_payload_bytes, 0,
        "appends meter no read bytes"
    );

    // Keyed product read: payload bytes only — the JSON array wrapper
    // and framing must NOT appear in the meter.
    let (st, _, body) = preq(
        addr,
        "GET",
        "/v1/streams/meter1/records?routingKey=k1",
        &key,
        b"",
    )
    .await;
    assert_eq!(st, 200);
    assert!(body.len() > p1.len(), "wire body carries framing");
    let m = row(&state, "meter1");
    assert_eq!(
        m.read_payload_bytes,
        p1.len() as u64,
        "read meters exactly the record payload"
    );
    assert_eq!(m.read_records, 1);
    assert_eq!(m.read_operations, 1);

    // Scan: both records, payload bytes only, one operation.
    let (st, _, _) = preq(addr, "GET", "/v1/streams/meter1:scan", &key, b"").await;
    assert_eq!(st, 200);
    let m2 = row(&state, "meter1");
    assert_eq!(
        m2.read_payload_bytes,
        (p1.len() + p1.len() + p2.len()) as u64,
        "scan adds exactly the two payloads"
    );
    assert_eq!(m2.read_operations, 2);
    assert_eq!(m2.read_records, 3);

    // Consumer pull delivers both payloads and counts ONE queue op.
    let (st, _, _) = preq(
        addr,
        "PUT",
        "/v1/streams/meter1/consumers/c1",
        &key,
        br#"{"visibilityTimeoutMs":30000,"maxAttempts":3}"#,
    )
    .await;
    assert_eq!(st, 201);
    let (st, _, b) = preq(
        addr,
        "POST",
        "/v1/streams/meter1/consumers/c1:pull",
        &key,
        br#"{"max":10}"#,
    )
    .await;
    assert_eq!(st, 200);
    let msgs = serde_json::from_slice::<serde_json::Value>(&b).unwrap()["messages"]
        .as_array()
        .unwrap()
        .len();
    assert_eq!(msgs, 2);
    let m3 = row(&state, "meter1");
    assert_eq!(m3.queue_operations, 1, "pull is one queue operation");
    assert_eq!(
        m3.read_payload_bytes,
        m2.read_payload_bytes + (p1.len() + p2.len()) as u64,
        "pull delivers both payloads at exact size"
    );

    // Raw HEAD: one read operation, zero data bytes.
    let bytes_before_head = row(&state, "meter1").read_payload_bytes;
    let ops_before_head = row(&state, "meter1").read_operations;
    let (st, _, _) = preq(addr, "HEAD", "/v1/stream/meter1", &[], b"").await;
    assert_eq!(st, 200);
    let m4 = row(&state, "meter1");
    assert_eq!(m4.read_payload_bytes, bytes_before_head);
    assert_eq!(m4.read_operations, ops_before_head + 1);

    // Internal segment-read (the fleet relay path) must add NOTHING:
    // the public coordinator that requested it meters, not the server.
    let desc = state
        .registry
        .get(&state.deployment.raw_adapter_sref("meter1"))
        .await
        .unwrap()
        .unwrap();
    let target = crate::product::InternalTarget::of(&desc, 0).unwrap();
    let mut hdrs: Vec<(String, String)> = vec![
        (
            "authorization".into(),
            "Bearer dst-internal-token".to_string(),
        ),
        ("stream-encryption-key".into(), PRISMA_KEY.to_string()),
    ];
    for (k, v) in target.headers() {
        hdrs.push((k.to_string(), v));
    }
    let hdr_refs: Vec<(&str, &str)> = hdrs.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect();
    let before_internal = row(&state, "meter1");
    let (st, _, _) = preq(
        addr,
        "GET",
        "/v1/internal/segment-read/meter1?offset=&head=1",
        &hdr_refs,
        b"",
    )
    .await;
    assert_eq!(st, 200, "internal head probe serves");
    let after_internal = row(&state, "meter1");
    assert_eq!(
        before_internal, after_internal,
        "an internal relay changed the customer meter"
    );

    // Rotation: sealing moves the rows into a batch with this boot's
    // source identity and a monotone sequence.
    state.billing.reads().seal_if_aged(0);
    let batches = state.billing.reads().drain_sealed(8);
    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].source.boot, state.runtime.identity.boot_id);
    assert!(
        batches[0]
            .rows
            .iter()
            .any(|r| r.identity.stream_name == "meter1"
                && r.identity.account_id == "acct_test"
                && !r.identity.stream_id.is_empty()),
        "sealed rows carry the full billing identity"
    );
    assert!(state.billing.reads().snapshot_active().is_empty());
    engine_shutdown(&state).await;
}

/// Resets the injected billing clock even on panic.
struct ClockGuard;
impl Drop for ClockGuard {
    fn drop(&mut self) {
        crate::billing::BILLING_CLOCK_OVERRIDE.store(0, std::sync::atomic::Ordering::Relaxed);
    }
}

/// §6 + round-21 blocker 1: billing runs on TRUSTED server time. The
/// injected billing clock drives months; customer `Stream-Timestamp`
/// headers — far future or far past — move record metadata only.
/// Ingest cannot be shifted between invoice months, a future record
/// clock cannot park storage accrual, and deletion bills the real
/// elapsed duration. Plus the durable-outbox battery: duplicates add
/// zero, acks are version-fenced, month rollover writes exact finals,
/// hard delete zeroes the gauge.
#[expect(
    clippy::too_many_lines,
    reason = "billing metadata scenario; the exact, durable and acknowledgeable properties are proved on one append sequence through restart; helper phases would hide which boundary the metadata survived"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn billing_meta_is_exact_durable_and_ackable() {
    let _xw = crate::billing::billing_clock_lock().write().await;
    let _reset = ClockGuard;
    let store = mem();
    let (state, addr) = http_rig(store).await;
    let ct = [
        ("stream-encryption-key", PRISMA_KEY),
        ("content-type", "application/json"),
    ];
    let (st, _, _) = hreq(addr, "PUT", "/v1/stream/bill1", &ct, b"").await;
    assert_eq!(st, 201);
    let sep = crate::billing::month_start_ms(2026, 9);
    let set_clock = |ms: i64| {
        crate::billing::BILLING_CLOCK_OVERRIDE.store(ms, std::sync::atomic::Ordering::Relaxed)
    };
    let p1 = br#"{"n":1,"pad":"aaaaaaaaaa"}"#; // 26 B payload
    let body = format!("[{}]", std::str::from_utf8(p1).unwrap());

    // Append 1 at billing time Sep 15 — carrying a FAR-FUTURE customer
    // timestamp (2030). The header must not move the invoice month.
    set_clock(sep + 14 * 86_400_000);
    let future_ns = (crate::billing::month_start_ms(2030, 1) * 1_000_000).to_string();
    let (st, _, _) = hreq(
        addr,
        "POST",
        "/v1/stream/bill1",
        &[
            ("stream-encryption-key", PRISMA_KEY),
            ("content-type", "application/json"),
            ("stream-timestamp", &future_ns),
        ],
        body.as_bytes(),
    )
    .await;
    assert_eq!(st, 204);

    let desc = state
        .registry
        .get(&state.deployment.raw_adapter_sref("bill1"))
        .await
        .unwrap()
        .unwrap();
    let seg = desc.resolve_segment("");
    let identity = seg.identity;
    let route = seg.shard_route;
    let engine = state.engine_for(&route).await.unwrap();

    let bm = engine.billing_meta(identity).await.expect("meta row");
    assert_eq!(
        (bm.month_year, bm.month_month),
        (2026, 9),
        "a far-future Stream-Timestamp must not move the billing month"
    );
    assert_eq!(bm.ingest_payload_bytes_total, p1.len() as u64);
    assert_eq!(bm.usage_version, 1);
    assert_eq!(bm.account_id, "acct_test");
    let frame1 = bm.owned_frame_bytes_current;
    assert!(frame1 > p1.len() as u64);
    assert_eq!(
        engine.usage_dirty_scan().await.unwrap(),
        vec![(identity, 1)]
    );

    // Append 2 (producer) at Sep 16 with a FAR-PAST header: billing
    // stays at commit time. Then its duplicate adds exactly zero.
    set_clock(sep + 15 * 86_400_000);
    let past_ns = (crate::billing::month_start_ms(2020, 1) * 1_000_000).to_string();
    let phdrs = [
        ("stream-encryption-key", PRISMA_KEY),
        ("content-type", "application/json"),
        ("stream-timestamp", past_ns.as_str()),
        ("producer-id", "pX"),
        ("producer-epoch", "1"),
        ("producer-seq", "0"),
    ];
    let (st, _, _) = hreq(addr, "POST", "/v1/stream/bill1", &phdrs, body.as_bytes()).await;
    assert!(st == 200 || st == 204);
    let v2 = engine.billing_meta(identity).await.unwrap();
    assert_eq!(v2.usage_version, 2);
    assert_eq!(
        (v2.month_year, v2.month_month),
        (2026, 9),
        "a far-past header must not move billing either"
    );
    let (st, _, _) = hreq(addr, "POST", "/v1/stream/bill1", &phdrs, body.as_bytes()).await;
    assert!(st == 200 || st == 204);
    let dup = engine.billing_meta(identity).await.unwrap();
    assert_eq!(dup.usage_version, 2, "duplicate adds zero");
    assert_eq!(dup.owned_frame_bytes_current, 2 * frame1);

    engine.submit_usage_ack(identity, 2, Vec::new());
    for _ in 0..100 {
        if engine.usage_dirty_scan().await.unwrap().is_empty() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    assert!(engine.usage_dirty_scan().await.unwrap().is_empty());

    // Billing time moves to Oct 10: September closes with the EXACT
    // integral (1 day at frame1 + 15 days at 2·frame1).
    let oct = crate::billing::month_start_ms(2026, 10);
    set_clock(oct + 9 * 86_400_000);
    let (st, _, _) = hreq(addr, "POST", "/v1/stream/bill1", &ct, body.as_bytes()).await;
    assert_eq!(st, 204);
    let m3 = engine.billing_meta(identity).await.unwrap();
    assert_eq!(m3.usage_version, 3);
    assert_eq!((m3.month_year, m3.month_month), (2026, 10));
    let finals = engine.usage_month_finals().await.unwrap();
    assert_eq!(finals.len(), 1);
    let (fkey, fsnap) = &finals[0];
    assert_eq!(fsnap.month, "2026-09");
    let day = 86_400_000u128;
    let expect = day * frame1 as u128 + (15 * day) * (2 * frame1) as u128;
    assert_eq!(fsnap.storage_byte_ms_month.parse::<u128>().unwrap(), expect);

    // Stale ack refused; exact ack clears marker + final.
    engine.submit_usage_ack(identity, 2, Vec::new());
    tokio::time::sleep(std::time::Duration::from_millis(120)).await;
    assert_eq!(
        engine.usage_dirty_scan().await.unwrap(),
        vec![(identity, 3)]
    );
    engine.submit_usage_ack(identity, 3, vec![fkey.clone()]);
    for _ in 0..100 {
        if engine.usage_dirty_scan().await.unwrap().is_empty() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    assert!(engine.usage_month_finals().await.unwrap().is_empty());

    // Delete at billing time Oct 20. The record clock said 2030; the
    // REAL elapsed October storage (10 days at 3·frame1) is still
    // integrated before the gauge zeroes.
    set_clock(oct + 19 * 86_400_000);
    let (st, _, _) = hreq(addr, "DELETE", "/v1/stream/bill1", &ct, b"").await;
    assert!(st == 204 || st == 200);
    let mut closed = None;
    for _ in 0..150 {
        if let Some(m) = engine.billing_meta(identity).await
            && m.owned_frame_bytes_current == 0
        {
            closed = Some(m);
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    let closed = closed.expect("hard delete never closed the gauge");
    assert_eq!(closed.usage_version, 4);
    let oct_ms: u128 = closed.month_byte_ms();
    // October = the rollover span (Oct 1 → Oct 10 at the pre-append
    // gauge 2f) plus the post-append span (Oct 10 → Oct 20 at 3f).
    assert_eq!(
        oct_ms,
        (9 * day) * (2 * frame1) as u128 + (10 * day) * (3 * frame1) as u128,
        "deletion bills the real elapsed duration, not the record clock"
    );
    engine_shutdown(&state).await;
}

/// §6-§10 end to end: ingest and reads flow meter → durable outbox →
/// `_usage` ledger → rollup → the customer usage API, exactly once.
/// Replaying the drain and the rollup page must change NOTHING
/// (idempotence at both hops), and the dashboard answer is a point
/// read with exact numbers.
#[expect(
    clippy::too_many_lines,
    reason = "usage pipeline scenario; appends, rollup absorption, restart and the exactly-once totals form one causal sequence on one rig; splitting the phases would hide which boundary could double count"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn usage_pipeline_end_to_end_exactly_once() {
    // Month-sensitive on the REAL clock: hold the read side so the
    // clock-injecting tests cannot move billing months mid-assert.
    let _xr = crate::billing::billing_clock_lock().read().await;
    let store = mem();
    let (state, addr) = http_rig(store).await;
    // This rig instance runs the rollup too.
    let rollup = crate::rollup::UsageRollup::open(state.data_store.clone(), "", &state.config)
        .await
        .unwrap();
    install_rollup(&state, rollup);

    let key = [("prisma-encryption-key", PRISMA_KEY)];
    let (st, _, _) = preq(
        addr,
        "PUT",
        "/v1/streams/pipe1",
        &key,
        br#"{"format":{"kind":"json"}}"#,
    )
    .await;
    assert_eq!(st, 201);
    let p1 = br#"{"n":1,"pad":"aaaaaaaaaa"}"#; // 26 B
    for k in ["k1", "k2"] {
        let (st, _, _) = preq(
            addr,
            "POST",
            "/v1/streams/pipe1/records",
            &[
                ("prisma-encryption-key", PRISMA_KEY),
                ("prisma-routing-key", k),
            ],
            p1,
        )
        .await;
        assert_eq!(st, 200);
    }
    // One keyed read = 26 read bytes.
    let (st, _, _) = preq(
        addr,
        "GET",
        "/v1/streams/pipe1/records?routingKey=k1",
        &key,
        b"",
    )
    .await;
    assert_eq!(st, 200);

    // Drain: read batches + dirty snapshots reach `_usage`; the outbox
    // acks down to empty.
    state.billing.reads().seal_if_aged(0);
    let n = crate::billing::drain_once(&state).await.expect("drain");
    assert!(n >= 2, "expected snapshots + a read batch, drained {n}");
    // Drain again with nothing new: nothing to emit.
    for _ in 0..100 {
        if crate::billing::drain_once(&state).await.unwrap() == 0 {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    assert_eq!(crate::billing::drain_once(&state).await.unwrap(), 0);

    // Rollup: consume the ledger to the end.
    let mut applied = 0;
    for _ in 0..50 {
        let k = crate::billing::rollup_step(&state).await.expect("rollup");
        applied += k;
        if k == 0 {
            break;
        }
    }
    assert!(applied >= n, "rollup consumed the drained page(s)");

    // The customer answer: exact ingest, exact read bytes, storage > 0.
    let (st, _, body) = preq(addr, "GET", "/v1/streams/pipe1/usage/current", &key, b"").await;
    assert_eq!(st, 200, "{}", String::from_utf8_lossy(&body));
    let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(v["ingestPayloadBytes"], 2 * p1.len() as u64);
    assert_eq!(v["ingestRecords"], 2);
    assert_eq!(v["readPayloadBytes"], p1.len() as u64);
    assert_eq!(v["appendRequests"], 2);
    assert_eq!(v["status"], "provisional");
    assert!(v["ownedStoredBytesNow"].as_u64().unwrap() > 2 * p1.len() as u64);
    assert!(
        v["storageByteSeconds"]
            .as_str()
            .unwrap()
            .parse::<u128>()
            .unwrap()
            > 0,
        "provisional storage extrapolates from the gauge"
    );

    // REPLAY the rollup from cursor zero — the design's §16 crash case
    // (rows written, cursor lost). Re-application must be a no-op.
    let before = serde_json::to_string(&v).unwrap();
    // Reset the cursor by applying an empty page carrying cursor "".
    state
        .rollup
        .get()
        .unwrap()
        .apply_page(&[], "")
        .await
        .unwrap();
    let mut reapplied = 0;
    for _ in 0..50 {
        let k = crate::billing::rollup_step(&state)
            .await
            .expect("rollup replay");
        reapplied += k;
        if k == 0 {
            break;
        }
    }
    assert!(reapplied >= applied, "the replay re-read the ledger");
    let (st, _, body2) = preq(addr, "GET", "/v1/streams/pipe1/usage/current", &key, b"").await;
    assert_eq!(st, 200);
    let v2: serde_json::Value = serde_json::from_slice(&body2).unwrap();
    for f in [
        "ingestPayloadBytes",
        "ingestRecords",
        "readPayloadBytes",
        "appendRequests",
    ] {
        assert_eq!(v2[f], v[f], "replay changed {f}: {before} vs {v2}");
    }

    // MT Stage 4c: the project-level answer authorizes against the
    // typed cell tenant (AppState.tenant), not a parallel string. The
    // cell's own project aggregates the same drained data; a foreign
    // project, the retired divergent rig string "proj_test", and a
    // grammatically invalid ID all get the SAME not-found answer — the
    // response distinguishes neither foreignness nor grammar.
    let (st, _, body) = preq(addr, "GET", "/v1/projects/proj-test/usage", &key, b"").await;
    assert_eq!(st, 200, "{}", String::from_utf8_lossy(&body));
    let pv: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(pv["projectId"], "proj-test");
    assert_eq!(pv["ingestRecords"], 2);
    assert_eq!(pv["ingestPayloadBytes"], 2 * p1.len() as u64);
    for foreign in ["proj-other", "proj_test", "pr..j"] {
        let path = format!("/v1/projects/{foreign}/usage");
        let (st, _, body) = preq(addr, "GET", &path, &key, b"").await;
        assert_eq!(st, 404, "{foreign}: {}", String::from_utf8_lossy(&body));
        let e: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(e["error"]["code"], "unknown_project", "{foreign}");
    }

    // The ledger itself never appears in its own books (§8.4).
    let (st, _, _) = preq(addr, "GET", "/v1/streams/_usage/usage", &key, b"").await;
    assert_eq!(st, 403, "reserved names refuse even the usage endpoint");
    engine_shutdown(&state).await;
}

/// §12: operational events are typed, deterministically identified,
/// durably journaled to `_ops_events`, and visible on the operator
/// surface. A raw create/delete emits lifecycle events; the drain
/// appends them to the reserved stream; a customer credential still
/// cannot read that stream.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ops_events_journal_end_to_end() {
    let store = mem();
    let (state, addr) = http_rig(store).await;
    let ct = [
        ("stream-encryption-key", PRISMA_KEY),
        ("content-type", "application/json"),
    ];
    let (st, _, _) = hreq(addr, "PUT", "/v1/stream/opsev1", &ct, b"").await;
    assert_eq!(st, 201);
    let (st, _, _) = hreq(addr, "DELETE", "/v1/stream/opsev1", &ct, b"").await;
    assert!(st == 204 || st == 200);
    let epoch = {
        // The tombstone still names the incarnation.
        let d = state
            .registry
            .get(&state.deployment.raw_adapter_sref("opsev1"))
            .await
            .unwrap()
            .unwrap();
        d.stream_epoch.clone()
    };

    // The recent ring already shows both, newest first.
    let recent = state.runtime.ops.recent(64);
    let created_id = format!("life/{epoch}/created");
    let deleted_id = format!("life/{epoch}/hard_deleted");
    assert!(recent.iter().any(|e| e.event_id == created_id));
    assert!(recent.iter().any(|e| e.event_id == deleted_id));

    // Drain to the durable journal.
    let n = crate::ops::drain_ops_once(&state).await.expect("ops drain");
    assert!(n >= 2, "drained {n}");

    // The journal holds them, readable with the SYSTEM key through the
    // in-process path...
    let mut hdrs = axum::http::HeaderMap::new();
    hdrs.insert(
        "stream-encryption-key",
        axum::http::HeaderValue::from_str(PRISMA_KEY).unwrap(),
    );
    let resp = crate::http::read_inner(
        state.clone(),
        crate::tenant::system_project().stream_ref("_ops_events"),
        crate::http::ReadParams::default(),
        hdrs,
        false,
        true,
        crate::http::SseSurface::Raw,
    )
    .await;
    assert_eq!(resp.status(), 200);
    let body = axum::body::to_bytes(resp.into_body(), 16 << 20)
        .await
        .unwrap();
    let events: Vec<crate::ops::OpsEvent> = serde_json::from_slice(&body).unwrap();
    assert!(
        events
            .iter()
            .any(|e| e.event_id == created_id && e.event_type == "stream_created")
    );
    assert!(events.iter().any(|e| e.event_id == deleted_id));
    assert!(events.iter().all(|e| !e.cell.is_empty()), "cell stamped");

    // ...and NOT with a customer credential over HTTP.
    let (st, _, _) = hreq(addr, "GET", "/v1/stream/_ops_events", &ct, b"").await;
    assert_eq!(st, 403, "reserved stream refuses the public surface");

    // Operator surface answers.
    let (st, _, b) = hreq(addr, "GET", "/v1/debug/ops-events", &[], b"").await;
    assert_eq!(st, 200);
    let v: serde_json::Value = serde_json::from_slice(&b).unwrap();
    assert!(v["events"].as_array().unwrap().len() >= 2);
    engine_shutdown(&state).await;
}

/// §11/§13: mergeable snapshots reach `_ops_metrics`, the ops rollup
/// materializes raw + 1-minute tiers behind its own exactly-once
/// cursor, and the alert evaluator opens and resolves with durable
/// alert events.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ops_metrics_and_alerts_flow() {
    let store = mem();
    let (state, _addr) = http_rig(store).await;
    let rollup =
        crate::rollup::UsageRollup::open(state.data_store.clone(), "opsflow", &state.config)
            .await
            .unwrap();
    install_rollup(&state, rollup);

    // Two snapshot emissions land in the same minute bucket.
    crate::ops::emit_metrics_once(&state).await.expect("emit 1");
    crate::ops::emit_metrics_once(&state).await.expect("emit 2");
    let mut total = 0;
    for _ in 0..20 {
        let n = crate::billing::ops_rollup_step(&state)
            .await
            .expect("ops rollup");
        total += n;
        if n == 0 {
            break;
        }
    }
    assert!(total >= 2, "rollup consumed {total} snapshots");
    let now = crate::shard::now_ms();
    let minute = now - now.rem_euclid(60_000);
    let m1 = state
        .rollup
        .get()
        .unwrap()
        .ops_m1("", minute)
        .await
        .or(state
            .rollup
            .get()
            .unwrap()
            .ops_m1("", minute - 60_000)
            .await)
        .expect("m1 aggregate exists");
    assert!(m1.samples >= 2, "both snapshots merged into one minute");
    assert!(m1.gauges_max.contains_key("open_engines"));

    // Alerts: force the read-meter backpressure rule by filling the
    // sealed queue beyond its cap, evaluate, then drain and re-evaluate.
    for i in 0..crate::billing::READ_SEALED_MAX_BATCHES + 2 {
        let id = crate::billing::BillingIdentity {
            account_id: "a".into(),
            project_id: "p".into(),
            stream_id: format!("{i:032x}"),
            stream_name: format!("s{i}"),
        };
        state.billing.reads().meter(
            &id,
            crate::billing::RowDelta {
                read_operations: 1,
                ..Default::default()
            },
        );
        state.billing.reads().seal_if_aged(0);
    }
    let snap = crate::ops::collect_snapshot(&state);
    crate::ops::evaluate_alerts(&state, &snap).await;
    let open = state.runtime.ops.open_alerts();
    assert!(
        open.iter()
            .any(|a| a.fingerprint == "read_meter_backpressure"),
        "backpressure alert must open: {open:?}"
    );
    // Drain the queue (ledger works in this rig), re-evaluate: resolved.
    while !state.billing.reads().drain_sealed(64).is_empty() {}
    let snap2 = crate::ops::collect_snapshot(&state);
    crate::ops::evaluate_alerts(&state, &snap2).await;
    assert!(
        !state
            .runtime
            .ops
            .open_alerts()
            .iter()
            .any(|a| a.fingerprint == "read_meter_backpressure"),
        "alert must resolve once the queue drains"
    );
    // Both transitions are journaled.
    let recent = state.runtime.ops.recent(64);
    assert!(recent.iter().any(|e| e.event_type == "alert_opened"));
    assert!(recent.iter().any(|e| e.event_type == "alert_resolved"));
    engine_shutdown(&state).await;
}

/// §19.6 crash points not already pinned structurally: "emit succeeds,
/// ack lost" re-emits an IDENTICAL snapshot the rollup deduplicates to
/// zero, and §17.2's batching gate — N dirty streams drain as ONE
/// ledger append, and an idle drain appends nothing at all.
#[expect(
    clippy::too_many_lines,
    reason = "telemetry crash-point scenario; each crash point and cost gate is exercised against the same pipeline state; helper phases would hide which crash point the gate protected"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn telemetry_crash_points_and_cost_gates() {
    // Month-sensitive on the REAL clock: hold the read side so the
    // clock-injecting tests cannot move billing months mid-assert.
    let _xr = crate::billing::billing_clock_lock().read().await;
    let store = mem();
    let (state, addr) = http_rig(store).await;
    let rollup = crate::rollup::UsageRollup::open(state.data_store.clone(), "p6", &state.config)
        .await
        .unwrap();
    install_rollup(&state, rollup);
    let key = [("prisma-encryption-key", PRISMA_KEY)];
    // Three streams, one record each.
    for i in 0..3 {
        let (st, _, _) = preq(
            addr,
            "PUT",
            &format!("/v1/streams/cg{i}"),
            &key,
            br#"{"format":{"kind":"json"}}"#,
        )
        .await;
        assert_eq!(st, 201);
        let (st, _, _) = preq(
            addr,
            "POST",
            &format!("/v1/streams/cg{i}/records"),
            &[
                ("prisma-encryption-key", PRISMA_KEY),
                ("prisma-routing-key", "k"),
            ],
            br#"{"n":1}"#,
        )
        .await;
        assert_eq!(st, 200);
    }
    // §17.2: all three dirty segments (+ the read batch) drain in ONE
    // round = one ledger append, not one per stream.
    state.billing.reads().seal_if_aged(0);
    let n = crate::billing::drain_once(&state).await.unwrap();
    assert!(n >= 3, "one batched drain covered all dirty streams: {n}");
    for _ in 0..100 {
        if crate::billing::drain_once(&state).await.unwrap() == 0 {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    // Idle: zero appends, zero per-stream PUTs of any kind.
    assert_eq!(crate::billing::drain_once(&state).await.unwrap(), 0);

    // Consume the ledger.
    let mut applied = 0;
    for _ in 0..50 {
        let k = crate::billing::rollup_step(&state).await.unwrap();
        applied += k;
        if k == 0 {
            break;
        }
    }
    assert!(applied >= n);
    let (st, _, body) = preq(addr, "GET", "/v1/streams/cg0/usage/current", &key, b"").await;
    assert_eq!(st, 200);
    let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
    let base_bytes = v["ingestPayloadBytes"].as_u64().unwrap();
    assert_eq!(base_bytes, 7, r#"{{"n":1}} is 7 bytes"#);

    // CRASH POINT "emit ok, ack lost": re-emit the CURRENT snapshot
    // with its deterministic id (exactly what a re-drain does when the
    // ack never landed), then re-consume. Numbers must not move.
    let desc = state
        .registry
        .get(&state.deployment.raw_adapter_sref("cg0"))
        .await
        .unwrap()
        .unwrap();
    let seg = desc.resolve_segment("k");
    let engine = state.engine_for(&seg.shard_route).await.unwrap();
    let meta = engine.billing_meta(seg.identity).await.unwrap();
    let snap = meta.to_snapshot(false);
    let env = crate::billing::UsageEnvelope {
        v: 1,
        event_id: snap.deterministic_event_id(),
        event_time_ms: meta.storage_accounted_through_ms,
        emitted_ms: crate::shard::now_ms(),
        cell: "cell_test".into(),
        payload: crate::billing::UsagePayload::SegmentSnapshot(snap),
    };
    // Append the duplicate straight to the ledger via the system path.
    let mut hdrs = axum::http::HeaderMap::new();
    hdrs.insert(
        "stream-encryption-key",
        axum::http::HeaderValue::from_str(PRISMA_KEY).unwrap(),
    );
    hdrs.insert(
        "content-type",
        axum::http::HeaderValue::from_static("application/json"),
    );
    let r = crate::http::append(
        state.clone(),
        crate::tenant::system_project().stream_ref("_usage"),
        hdrs,
        axum::body::Body::from(serde_json::to_vec(&[env]).unwrap()),
        None,
        None,
        None,
    )
    .await;
    assert!(r.status().is_success());
    for _ in 0..50 {
        if crate::billing::rollup_step(&state).await.unwrap() == 0 {
            break;
        }
    }
    let (st, _, body2) = preq(addr, "GET", "/v1/streams/cg0/usage/current", &key, b"").await;
    assert_eq!(st, 200);
    let v2: serde_json::Value = serde_json::from_slice(&body2).unwrap();
    assert_eq!(
        v2["ingestPayloadBytes"].as_u64().unwrap(),
        base_bytes,
        "a re-emitted snapshot (lost ack) must apply as zero"
    );
    engine_shutdown(&state).await;
}

/// Round-21 blocker 3: sealed read batches are DURABLE. They enter a
/// per-instance spool before the ledger sees them, survive a process
/// "crash" (a second spool handle on the same store), and leave only
/// after `_usage` acknowledged. During a ledger outage the spool
/// absorbs on disk while the accumulator keeps rotating.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn read_batches_survive_crash_in_the_spool() {
    let store = mem();
    let (state, addr) = http_rig(store).await;
    let spool =
        crate::billing::ReadSpool::open(state.data_store.clone(), "sp1", "inst", &state.config)
            .await
            .unwrap();
    install_read_spool(&state, spool);
    let key = [("prisma-encryption-key", PRISMA_KEY)];
    let (st, _, _) = preq(
        addr,
        "PUT",
        "/v1/streams/spool1",
        &key,
        br#"{"format":{"kind":"json"}}"#,
    )
    .await;
    assert_eq!(st, 201);
    let (st, _, _) = preq(
        addr,
        "GET",
        "/v1/streams/spool1/records?routingKey=k",
        &key,
        b"",
    )
    .await;
    assert_eq!(st, 200);
    state.billing.reads().seal_if_aged(0);
    // Persist to the spool WITHOUT reaching the ledger: simulate the
    // pre-append half of a drain by spooling directly.
    let sealed = state.billing.reads().drain_sealed(16);
    assert_eq!(sealed.len(), 1);
    let sp = state.billing.read_spool().unwrap();
    sp.persist(&sealed[0]).await.unwrap();

    // "Crash": a fresh spool handle over the same store still has it.
    let recovered =
        crate::billing::ReadSpool::open(state.data_store.clone(), "sp1", "inst", &state.config)
            .await
            .unwrap();
    let pending = recovered.pending(16).await.unwrap();
    assert_eq!(pending.len(), 1, "the sealed batch survived the crash");
    assert_eq!(pending[0].1.rows.len(), 1);
    assert_eq!(pending[0].1.rows[0].identity.stream_name, "spool1");

    // A full drain now emits from the spool and clears it after ack.
    let n = crate::billing::drain_once(&state).await.unwrap();
    assert!(n >= 1);
    assert_eq!(state.billing.read_spool().unwrap().depth().await, 0);
    engine_shutdown(&state).await;
}
