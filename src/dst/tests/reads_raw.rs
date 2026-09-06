//! Reads raw.

use super::fixture_failpoints::{FailpointGuard, gap_lock};
use super::fixture_http::{await_published, engine_shutdown, http_rig, rig_in_seal_gap};
use super::fixture_requests::RIG_KEY_B64;
use super::fixture_requests::{PRISMA_KEY, drain_no_closure, hreq, preq, read_page};
use super::fixture_storage::{append_sized, mem, skey, wait_all_absorbed};
use crate::dst::{FaultPlan, FaultStore};
use object_store::ObjectStore;
use std::sync::Arc;

/// GET during the seal gap: records + resume cursor, never closure,
/// never a final Up-To-Date; after release, the same client drains the
/// full lineage.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn seal_gap_get_never_reports_closure() {
    let _l = gap_lock().lock().await;
    let (state, addr, _guard, split) = rig_in_seal_gap("gapget", 6).await;

    let mut tok: Option<String> = None;
    let mut got = 0usize;
    for _ in 0..16 {
        let (st, h, recs) = read_page(addr, "gapget", Some("ga"), tok.as_deref()).await;
        assert!(st == 200 || st == 204, "gap page status {st}");
        assert!(
            !h.contains_key("stream-closed") && !h.contains_key("prisma-sealed"),
            "seal gap reported closure: {h:?}"
        );
        assert!(
            !h.contains_key("stream-up-to-date"),
            "seal gap reported finality: {h:?}"
        );
        got += recs.len();
        let nxt = h
            .get("prisma-next-cursor")
            .or_else(|| h.get("stream-next-offset"))
            .cloned();
        if nxt.is_none() || nxt == tok {
            break;
        }
        tok = nxt;
    }
    assert_eq!(got, 6, "every pre-seal record stays readable in the gap");

    crate::failpoints::release_scaler_before_publish("gapget");
    // The reader-spawned resume() may win the publication CAS; the
    // split task's own bool only says who published. The outcome gate
    // is await_published.
    split.await.unwrap();
    await_published(&state, "gapget").await;
    let (recs, last) = drain_no_closure(addr, "gapget", Some("ga")).await;
    assert_eq!(recs.len(), 6);
    assert_eq!(
        last.get("prisma-up-to-date")
            .or_else(|| last.get("stream-up-to-date"))
            .map(String::as_str),
        Some("true"),
        "published lineage ends Up-To-Date"
    );
}

/// HEAD during the gap must not report closure.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn seal_gap_head_no_closure() {
    let _l = gap_lock().lock().await;
    let (state, addr, _guard, split) = rig_in_seal_gap("gaphead", 3).await;
    let (st, h, _) = hreq(addr, "HEAD", "/v1/stream/gaphead", &[], b"").await;
    assert_eq!(st, 200);
    assert!(
        !h.contains_key("stream-closed") && !h.contains_key("prisma-sealed"),
        "HEAD reported closure in the gap: {h:?}"
    );
    crate::failpoints::release_scaler_before_publish("gaphead");
    split.await.unwrap();
    await_published(&state, "gaphead").await;
    let (st, h, _) = hreq(addr, "HEAD", "/v1/stream/gaphead", &[], b"").await;
    assert_eq!(st, 200);
    assert!(!h.contains_key("stream-closed") && !h.contains_key("prisma-sealed"));
}

/// A long-poll already parked at the tail when the seal lands must wake
/// WITHOUT closure and with a usable rearm token.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn seal_gap_long_poll_wakes_without_closure() {
    let _l = gap_lock().lock().await;
    let store = mem();
    let (state, addr) = http_rig(store).await;
    let (st, _, _) = hreq(
        addr,
        "PUT",
        "/v1/stream/gappoll",
        &[("content-type", "application/json")],
        b"",
    )
    .await;
    assert!(st == 200 || st == 201, "create {st}");
    for i in 0..3 {
        let body = serde_json::json!({ "k": "ga", "n": i }).to_string();
        preq(
            addr,
            "POST",
            "/v1/streams/gappoll/records",
            &[
                ("prisma-encryption-key", PRISMA_KEY),
                ("prisma-routing-key", "ga"),
            ],
            body.as_bytes(),
        )
        .await;
    }
    // Find the tail token, then park a long-poll on it.
    let (_, h, _) = read_page(addr, "gappoll", None, None).await;
    let tail = h.get("stream-next-offset").unwrap().clone();
    let poll = {
        let tail = tail.clone();
        tokio::spawn(async move {
            hreq(
                addr,
                "GET",
                &format!("/v1/stream/gappoll?offset={tail}&live=long-poll&timeout=8s"),
                &[],
                b"",
            )
            .await
        })
    };
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    crate::failpoints::arm_scaler_before_publish("gappoll");
    let _guard = FailpointGuard("gappoll".to_string());
    let split = {
        let state = state.clone();
        tokio::spawn(async move {
            crate::scaler3::execute_split(
                &state,
                &state.deployment.raw_adapter_sref("gappoll"),
                0,
                0x8000_0000_0000_0000,
            )
            .await
        })
    };
    let (st, h, _) = poll.await.unwrap();
    assert!(st == 200 || st == 204, "poll woke with {st}");
    assert!(
        !h.contains_key("stream-closed") && !h.contains_key("prisma-sealed"),
        "seal wake reported closure: {h:?}"
    );
    crate::failpoints::release_scaler_before_publish("gappoll");
    split.await.unwrap();
    await_published(&state, "gappoll").await;
    let (recs, _) = drain_no_closure(addr, "gappoll", Some("ga")).await;
    assert_eq!(recs.len(), 3);
}

/// A read STARTING inside the gap (cold client, offset 0).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn seal_gap_cold_read_mid_gap() {
    let _l = gap_lock().lock().await;
    let (state, addr, _guard, split) = rig_in_seal_gap("gapcold", 4).await;
    let (st, h, recs) = read_page(addr, "gapcold", Some("gb"), None).await;
    assert_eq!(st, 200);
    assert!(
        !h.contains_key("stream-closed") && !h.contains_key("prisma-sealed"),
        "{h:?}"
    );
    assert!(!h.contains_key("stream-up-to-date"), "{h:?}");
    assert_eq!(recs.len(), 4, "gap serves everything below the seal");
    crate::failpoints::release_scaler_before_publish("gapcold");
    split.await.unwrap();
    await_published(&state, "gapcold").await;
    let (recs, _) = drain_no_closure(addr, "gapcold", Some("gb")).await;
    assert_eq!(recs.len(), 4);
}

/// A client that vanishes mid-request during the gap, then retries.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn seal_gap_cancel_then_retry() {
    let _l = gap_lock().lock().await;
    let (state, addr, _guard, split) = rig_in_seal_gap("gapcancel", 4).await;
    {
        use tokio::io::AsyncWriteExt;
        let mut s = tokio::net::TcpStream::connect(addr).await.unwrap();
        let req = format!(
            "GET /v1/streams/gapcancel/records?routingKey=ga HTTP/1.1\r\nhost: x\r\nprisma-encryption-key: {RIG_KEY_B64}\r\n\r\n"
        );
        s.write_all(req.as_bytes()).await.unwrap();
        drop(s); // vanish without reading the response
    }
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    let (st, h, recs) = read_page(addr, "gapcancel", Some("ga"), None).await;
    assert_eq!(st, 200);
    assert!(
        !h.contains_key("stream-closed") && !h.contains_key("prisma-sealed"),
        "{h:?}"
    );
    assert_eq!(
        recs.len(),
        4,
        "retry after cancellation serves the gap view"
    );
    crate::failpoints::release_scaler_before_publish("gapcancel");
    split.await.unwrap();
    await_published(&state, "gapcancel").await;
    let (recs, _) = drain_no_closure(addr, "gapcancel", Some("ga")).await;
    assert_eq!(recs.len(), 4);
}

/// The cross-instance shape: a reader whose CACHED descriptor predates
/// the whole transition (no pending, one segment) meets the sealed
/// engine handle. The standard path must refresh + redispatch instead
/// of trusting the stale map and reporting closure.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn seal_gap_stale_descriptor_redispatches() {
    let _l = gap_lock().lock().await;
    let store = mem();
    let (state, addr) = http_rig(store).await;
    let (st, _, _) = hreq(
        addr,
        "PUT",
        "/v1/stream/gapstale",
        &[("content-type", "application/json")],
        b"",
    )
    .await;
    assert!(st == 200 || st == 201, "create {st}");
    for i in 0..4 {
        let body = serde_json::json!({ "k": "ga", "n": i }).to_string();
        preq(
            addr,
            "POST",
            "/v1/streams/gapstale/records",
            &[
                ("prisma-encryption-key", PRISMA_KEY),
                ("prisma-routing-key", "ga"),
            ],
            body.as_bytes(),
        )
        .await;
    }
    // The pre-transition descriptor this "instance" will keep believing.
    let stale = state
        .registry
        .get(&state.deployment.raw_adapter_sref("gapstale"))
        .await
        .unwrap()
        .unwrap();
    assert!(stale.segments.as_ref().is_none_or(|m| m.pending.is_none()));

    crate::failpoints::arm_scaler_before_publish("gapstale");
    let _guard = FailpointGuard("gapstale".to_string());
    let split = {
        let state = state.clone();
        tokio::spawn(async move {
            crate::scaler3::execute_split(
                &state,
                &state.deployment.raw_adapter_sref("gapstale"),
                0,
                0x8000_0000_0000_0000,
            )
            .await
        })
    };
    // Wait for the seal, then plant the STALE descriptor over the fresh
    // cache entry — the reader now sees exactly what a lagging sibling
    // instance would.
    let identity = stale.resolve_segment("").identity;
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    loop {
        let closed = match state
            .engine_for_scaler(
                &crate::crypto::RouteHash::for_stream(
                    &state.deployment.raw_adapter_sref("gapstale"),
                )
                .0,
            )
            .await
        {
            Some(e) => match e.stream_handle(identity).await {
                Ok(h) => h.state.lock().unwrap().durable.closed,
                Err(_) => false,
            },
            None => false,
        };
        if closed {
            break;
        }
        assert!(std::time::Instant::now() < deadline, "seal never landed");
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    state
        .registry
        .test_poison_cache(&state.deployment.raw_adapter_sref("gapstale"), stale);

    let (st, h, recs) = read_page(addr, "gapstale", Some("ga"), None).await;
    assert_eq!(st, 200);
    assert!(
        !h.contains_key("stream-closed") && !h.contains_key("prisma-sealed"),
        "stale descriptor let closure through: {h:?}"
    );
    assert_eq!(recs.len(), 4);
    crate::failpoints::release_scaler_before_publish("gapstale");
    split.await.unwrap();
    await_published(&state, "gapstale").await;
    let (recs, _) = drain_no_closure(addr, "gapstale", Some("ga")).await;
    assert_eq!(recs.len(), 4);
}

// ---- oversized keyed records / long runs (review blocker: the first
// record must ALWAYS make progress; consumed_to is first-class) -------

/// Byte-budgeted keyed drain returning (offsets, payload_sizes, pages).
/// Panics if a page makes no progress — the stall this guards against.
async fn drain_keyed_paged(
    engine: &Arc<crate::shard::ShardEngine>,
    hash: [u8; 16],
    key: &crate::crypto::StreamKey,
    rk: &str,
    page_bytes: usize,
) -> (Vec<u64>, Vec<usize>, usize) {
    let handle = engine.stream_handle(hash).await.expect("handle");
    let mut from = 0u64;
    let mut offs = Vec::new();
    let mut sizes = Vec::new();
    let mut pages = 0usize;
    loop {
        pages += 1;
        assert!(pages <= 128, "drain did not settle");
        let res = crate::http::read_merged(
            key,
            &hash,
            &handle,
            engine,
            from,
            Some(rk),
            page_bytes,
            crate::shard::Deliver::Durable,
        )
        .await
        .expect("keyed read");
        for rec in &res.recs {
            offs.push(rec.off);
            sizes.push(rec.payload.len());
        }
        if res.completed {
            return (offs, sizes, pages);
        }
        match res.last {
            Some(last) if last + 1 > from => from = last + 1,
            other => panic!(
                "incomplete page made no progress (from={from}, last={other:?}) — \
                 the oversized-run stall"
            ),
        }
    }
}

/// One record far larger than the page budget, surrounded by small
/// records of another key: it must be served (allow-first), and the
/// drain must complete with exact contents.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn oversized_keyed_record_pages_through() {
    let store = mem();
    let key = skey();
    let hash = [0xC1u8; 16];
    let db = slatedb::Db::builder("dst-bigrec", store.clone())
        .with_settings(slatedb::config::Settings {
            flush_interval: Some(std::time::Duration::from_millis(5)),
            manifest_poll_interval: std::time::Duration::from_millis(50),
            ..Default::default()
        })
        .build()
        .await
        .expect("open db");
    let (absorb_tx, absorb_rx) = crate::history::absorber_channel();
    // R25-A: tests use the REAL load path — a fresh DB rebuilds to
    // zero; a reopened DB restores its durable backlog, exactly as
    // the production opener does.
    let __maint = crate::shard::load_or_rebuild_maintenance(&db)
        .await
        .expect("load maintenance");
    let engine = crate::shard::ShardEngine::start(
        "dst-bigrec".to_string(),
        Arc::new(db),
        store.clone(),
        crate::shard::ShardConfig::default(),
        absorb_tx,
        None,
        __maint,
    );
    let _absorber = crate::history::Absorber::start(
        store.clone(),
        engine.clone(),
        Arc::new(crate::history::KeyCache::default()),
        crate::history::AbsorberConfig {
            threshold_bytes: 1,
            threshold_age: std::time::Duration::from_millis(1),
            tick: std::time::Duration::from_millis(20),
            sweep_every: u32::MAX,
            ..Default::default()
        },
        absorb_rx,
    );
    let mut want: Vec<(u64, usize)> = Vec::new();
    for _ in 0..3 {
        append_sized(&engine, hash, &key, "other", 4 * 1024).await;
    }
    want.push((
        append_sized(&engine, hash, &key, "big", 12 * 1024 * 1024).await,
        12 * 1024 * 1024,
    ));
    for _ in 0..3 {
        append_sized(&engine, hash, &key, "other", 4 * 1024).await;
    }
    for _ in 0..4 {
        want.push((
            append_sized(&engine, hash, &key, "big", 4 * 1024).await,
            4 * 1024,
        ));
    }
    wait_all_absorbed(&engine, &[hash]).await;
    let _ds: Arc<dyn ObjectStore> = store.clone();
    let (offs, sizes, pages) = drain_keyed_paged(&engine, hash, &key, "big", 1024 * 1024).await;
    assert_eq!(
        offs,
        want.iter().map(|(o, _)| *o).collect::<Vec<_>>(),
        "exact offsets in order"
    );
    assert_eq!(
        sizes,
        want.iter().map(|(_, s)| *s).collect::<Vec<_>>(),
        "the 12 MiB record arrived intact"
    );
    assert!(pages >= 2, "budget forces pagination (pages={pages})");
    engine.begin_close();
}

/// A contiguous single-key run larger than the 16 MiB plan budget AND
/// the page budget: sub-run planning + span truncation page it through,
/// including across a postings-cache wipe mid-drain (the restart /
/// cold-instance shape).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn long_keyed_run_pages_with_progress() {
    let store = mem();
    let key = skey();
    let hash = [0xC2u8; 16];
    let db = slatedb::Db::builder("dst-bigrun", store.clone())
        .with_settings(slatedb::config::Settings {
            flush_interval: Some(std::time::Duration::from_millis(5)),
            manifest_poll_interval: std::time::Duration::from_millis(50),
            ..Default::default()
        })
        .build()
        .await
        .expect("open db");
    let (absorb_tx, absorb_rx) = crate::history::absorber_channel();
    // R25-A: tests use the REAL load path — a fresh DB rebuilds to
    // zero; a reopened DB restores its durable backlog, exactly as
    // the production opener does.
    let __maint = crate::shard::load_or_rebuild_maintenance(&db)
        .await
        .expect("load maintenance");
    let engine = crate::shard::ShardEngine::start(
        "dst-bigrun".to_string(),
        Arc::new(db),
        store.clone(),
        crate::shard::ShardConfig::default(),
        absorb_tx,
        None,
        __maint,
    );
    let _absorber = crate::history::Absorber::start(
        store.clone(),
        engine.clone(),
        Arc::new(crate::history::KeyCache::default()),
        crate::history::AbsorberConfig {
            threshold_bytes: 1,
            threshold_age: std::time::Duration::from_millis(1),
            tick: std::time::Duration::from_millis(20),
            sweep_every: u32::MAX,
            ..Default::default()
        },
        absorb_rx,
    );
    let mut want = Vec::new();
    for _ in 0..24 {
        want.push(append_sized(&engine, hash, &key, "run", 1024 * 1024).await);
    }
    wait_all_absorbed(&engine, &[hash]).await;
    let _ds: Arc<dyn ObjectStore> = store.clone();

    // Page manually so the cache wipe lands between pages.
    let handle = engine.stream_handle(hash).await.expect("handle");
    let mut from = 0u64;
    let mut offs = Vec::new();
    let mut pages = 0usize;
    loop {
        pages += 1;
        assert!(pages <= 64, "drain did not settle");
        let res = crate::http::read_merged(
            &key,
            &hash,
            &handle,
            &engine,
            from,
            Some("run"),
            4 * 1024 * 1024,
            crate::shard::Deliver::Durable,
        )
        .await
        .expect("keyed read");
        for rec in &res.recs {
            offs.push(rec.off);
            assert_eq!(rec.payload.len(), 1024 * 1024);
        }
        if res.completed {
            break;
        }
        match res.last {
            Some(last) if last + 1 > from => from = last + 1,
            other => panic!("no progress at from={from} ({other:?})"),
        }
        if pages == 2 {
            // Cold-instance restart between partial pages.
            engine.postings_cache.sweep_idle(std::time::Duration::ZERO);
        }
    }
    assert_eq!(offs, want, "all 24 MiB drained exactly once, in order");
    assert!(pages >= 4, "24 MiB through 4 MiB pages (pages={pages})");
    engine.begin_close();
}

/// AUDIT P0: the singular route is the DEFAULT-KEY Durable Stream, and
/// stays one strict sequence while the product surface writes other
/// keys and splits the collection underneath it. The required
/// cross-surface test: product keys + split, raw default-key traffic,
/// raw reads see exactly their own records with resumable cursors.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn raw_route_is_the_default_key_view_across_splits() {
    let _l = gap_lock().lock().await;
    let store = mem();
    let (state, addr) = http_rig(store).await;
    let ct = [("content-type", "application/json")];
    let _pk = [("prisma-encryption-key", PRISMA_KEY)];
    let (st, _, _) = hreq(addr, "PUT", "/v1/stream/dualkey", &ct, b"").await;
    assert!(st == 200 || st == 201);

    // Raw (default-key) and product (other keys) traffic interleaved.
    for i in 0..3 {
        let body = format!("[{{\"raw\":{i}}}]");
        let (st, _, _) = hreq(addr, "POST", "/v1/stream/dualkey", &ct, body.as_bytes()).await;
        assert!(st == 200 || st == 204);
        for k in ["ka", "kb"] {
            let body = format!("{{\"k\":\"{k}\",\"n\":{i}}}");
            let (st, _, _) = preq(
                addr,
                "POST",
                "/v1/streams/dualkey/records",
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
    // Before the split: the raw read sees ONLY its own records.
    let (st, _, b) = hreq(addr, "GET", "/v1/stream/dualkey", &[], b"").await;
    assert_eq!(st, 200);
    let recs: Vec<serde_json::Value> = serde_json::from_slice(&b).unwrap();
    assert_eq!(recs.len(), 3, "raw sees only the default key: {recs:?}");
    assert!(recs.iter().all(|r| r.get("raw").is_some()));

    // Split the collection through the product surface.
    assert!(
        crate::scaler3::execute_split(
            &state,
            &state.deployment.raw_adapter_sref("dualkey"),
            0,
            0x8000_0000_0000_0000
        )
        .await
    );
    for i in 3..6 {
        let body = format!("[{{\"raw\":{i}}}]");
        let (st, _, _) = hreq(addr, "POST", "/v1/stream/dualkey", &ct, body.as_bytes()).await;
        assert!(st == 200 || st == 204, "raw append after split: {st}");
        for k in ["ka", "kb"] {
            let body = format!("{{\"k\":\"{k}\",\"n\":{i}}}");
            let (st, _, _) = preq(
                addr,
                "POST",
                "/v1/streams/dualkey/records",
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

    // AFTER the split the raw route is STILL the default-key sequence:
    // every record, in order, nothing from other keys — paginated with
    // resumable raw offsets.
    let mut seen: Vec<i64> = Vec::new();
    let mut tok: Option<String> = None;
    for _ in 0..16 {
        let path = match &tok {
            None => "/v1/stream/dualkey".to_string(),
            Some(t) => format!("/v1/stream/dualkey?offset={t}"),
        };
        let (st, h, b) = hreq(addr, "GET", &path, &[], b"").await;
        assert_eq!(st, 200, "raw page after split");
        let recs: Vec<serde_json::Value> = serde_json::from_slice(&b).unwrap();
        for r in &recs {
            assert!(r.get("k").is_none(), "another key's record leaked: {r}");
            seen.push(r["raw"].as_i64().unwrap());
        }
        if h.get("stream-up-to-date").map(String::as_str) == Some("true") {
            break;
        }
        let nxt = h.get("stream-next-offset").cloned();
        if nxt == tok || nxt.is_none() {
            break;
        }
        tok = nxt;
    }
    assert_eq!(
        seen,
        (0..6).collect::<Vec<i64>>(),
        "default-key order across the split"
    );

    // Live reads on the raw route keep working after the split (one
    // key, one lineage — no keyless-live impossibility).
    let (st, h, _) = hreq(
        addr,
        "GET",
        "/v1/stream/dualkey?offset=now&live=long-poll&timeout=200ms",
        &[],
        b"",
    )
    .await;
    assert!(st == 200 || st == 204, "raw long-poll after split: {st}");
    assert!(
        !h.contains_key("stream-ordering"),
        "no internals leak to the raw route"
    );
    assert!(!h.contains_key("stream-segment-map-version"));

    // The removed keyed extensions are rejected, not honored.
    let (st, _, _) = hreq(addr, "GET", "/v1/stream/dualkey?key=ka", &[], b"").await;
    assert_eq!(st, 400, "?key= is removed from the raw route");
    let (st, _, _) = hreq(
        addr,
        "POST",
        "/v1/stream/dualkey",
        &[("content-type", "application/json"), ("stream-key", "ka")],
        br#"[{"x":1}]"#,
    )
    .await;
    assert_eq!(st, 400, "Stream-Key is removed from the raw route");
    engine_shutdown(&state).await;
}

/// AUDIT P0: the catalog paginates without scanning the world. With
/// far more streams than one page, listing walks them in NAME order
/// through provider continuation, every stream is reachable (nothing
/// falls outside a fixed window), pages never restart from the
/// beginning, and a page's descriptor GETs are bounded by the page
/// size — not by the catalog size.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn catalog_pages_without_scanning_the_world() {
    let inner = mem();
    let counting = FaultStore::uniform(inner, 7, FaultPlan::new(0, 0, 0));
    let store: Arc<dyn ObjectStore> = counting.clone();
    let (state, addr) = http_rig(store).await;
    // 1,200 streams: an order of magnitude more than one page, and
    // enough that a scan-everything implementation is obvious in the
    // request count.
    const N: usize = 1_200;
    for i in 0..N {
        let name = format!("cat-{i:05}");
        let d = crate::registry::PersistedDescriptor {
            seal_gen_counter: 0,
            account_id: None,
            project_id: crate::tenant::ProjectId::new("proj-test").unwrap(),
            name: name.clone(),
            stream_epoch: format!("{:032x}", i),
            key_fingerprint: "fp".into(),
            created_ms: 1,
            expires_at_ms: None,
            deleted: false,
            soft_deleted: false,
            logical_close_ms: None,
            forked_from: None,
            fork_children: Vec::new(),
            init: None,
            sealing: None,
            seal_op: None,
            content_type: "application/json".into(),
            ttl_secs: None,
            segments: None,
            sealed: false,
            watch_definitions: Vec::new(),
            watch_sig_key: None,
            parent_ref_pending: false,
            layout_version: crate::registry::LAYOUT_VERSION,
        };
        state.registry.create(d).await.unwrap();
    }
    // Page through the whole catalog.
    let mut seen: Vec<String> = Vec::new();
    let mut cursor: Option<String> = None;
    let ops_before = counting.ops();
    let mut pages = 0usize;
    for _ in 0..64 {
        let path = match &cursor {
            None => "/v1/streams?limit=100".to_string(),
            Some(c) => format!("/v1/streams?limit=100&cursor={c}"),
        };
        let (st, _, b) = preq(addr, "GET", &path, &[], b"").await;
        assert_eq!(st, 200, "{}", String::from_utf8_lossy(&b));
        let v: serde_json::Value = serde_json::from_slice(&b).unwrap();
        let names: Vec<String> = v["streams"]
            .as_array()
            .unwrap()
            .iter()
            .map(|s| s["name"].as_str().unwrap().to_string())
            .collect();
        pages += 1;
        seen.extend(names);
        match v["cursor"].as_str() {
            Some(c) => cursor = Some(c.to_string()),
            None => break,
        }
    }
    let ops_after = counting.ops();
    assert_eq!(
        seen.len(),
        N,
        "every stream is reachable, got {} in {pages} pages",
        seen.len()
    );
    let mut sorted = seen.clone();
    sorted.sort();
    assert_eq!(seen, sorted, "catalog pages walk in name order");
    sorted.dedup();
    assert_eq!(sorted.len(), N, "no stream is listed twice");
    // Cost: a scan-everything implementation costs pages * N GETs
    // (12 * 1200 = 14,400 here). Page-local cost is ~N total.
    let per_page_budget = (N + pages * 40) as u64;
    assert!(
        ops_after - ops_before < per_page_budget,
        "catalog cost must be page-local: {} store ops for {pages} pages over {N} streams",
        ops_after - ops_before
    );
    // A cursor is opaque (not a bare, editable stream name).
    let (_, _, b) = preq(addr, "GET", "/v1/streams?limit=10", &[], b"").await;
    let v: serde_json::Value = serde_json::from_slice(&b).unwrap();
    let c = v["cursor"].as_str().unwrap();
    assert!(!c.starts_with("cat-"), "cursor must be opaque, got {c}");
    engine_shutdown(&state).await;
}
