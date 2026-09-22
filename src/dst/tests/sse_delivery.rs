//! Sse delivery.

use super::fixture_auth::{auth_rig, mint_token, rig_create, rig_sse};
use super::fixture_failpoints::gap_lock;
use super::fixture_http::{engine_shutdown, http_rig};
use super::fixture_livefeed::{hub_sse_collect, lf_connect, lf_record_and_status};
use super::fixture_requests::RIG_KEY_B64;
use super::fixture_requests::{PRISMA_KEY, hreq, preq};
use super::fixture_storage::{mem, skey};

/// P0 (static audit): the v2 gather previously accumulated up to
/// #267 lag-disconnect pin: sse_send must give up within its bounded
/// deadline when the subscriber queue stays full — the slow-consumer
/// policy on a durable, cursor-addressable stream is DISCONNECT and
/// resume-from-cursor, never unbounded buffering. Paused time makes
/// the 10 s deadline instant.
#[tokio::test(start_paused = true)]
async fn sse_send_disconnects_a_stalled_subscriber() {
    let (tx, mut rx) = tokio::sync::mpsc::channel::<crate::sse::auth::SseChunk>(4);
    for _ in 0..4 {
        assert!(crate::http::sse_send(&tx, bytes::Bytes::from_static(b"x")).await);
    }
    // Queue full, receiver stalled: the fifth send must time out.
    assert!(
        !crate::http::sse_send(&tx, bytes::Bytes::from_static(b"y")).await,
        "a full queue with a stalled receiver must disconnect, not buffer"
    );
    // A draining receiver keeps the subscriber alive.
    let _ = rx.recv().await;
    assert!(crate::http::sse_send(&tx, bytes::Bytes::from_static(b"z")).await);
}

/// Keyed SSE follows the lineage (review deferral, now wired): a
/// subscriber from offset 0 receives every pre-split AND post-split
/// record for its key in order, then an upToDate control.
#[expect(
    clippy::too_many_lines,
    reason = "lineage delivery scenario; subscribing from offset zero, splitting and receiving every pre-split and post-split record form one causal sequence; helper phases would hide which record the subscriber skipped"
)]
#[expect(
    clippy::excessive_nesting,
    reason = "sse_follows_lineage_across_split; the fixture nests the line scan inside the chunk scan of each read; flattening it would separate the control frames from the chunk that carries them"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sse_follows_lineage_across_split() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let _l = gap_lock().lock().await;
    let store = mem();
    let (state, addr) = http_rig(store).await;
    let (st, _, _) = hreq(
        addr,
        "PUT",
        "/v1/stream/sselin",
        &[("content-type", "application/json")],
        b"",
    )
    .await;
    assert!(st == 200 || st == 201);
    for r in 0..5 {
        for k in ["ga", "gb"] {
            let body = format!("{{\"k\":\"{k}\",\"n\":{r}}}");
            preq(
                addr,
                "POST",
                "/v1/streams/sselin/records",
                &[
                    ("prisma-encryption-key", PRISMA_KEY),
                    ("prisma-routing-key", k),
                ],
                body.as_bytes(),
            )
            .await;
        }
    }
    assert!(
        crate::scaler3::execute_split(
            &state,
            &state.deployment.raw_adapter_sref("sselin"),
            0,
            0x8000_0000_0000_0000
        )
        .await
    );
    for r in 5..10 {
        for k in ["ga", "gb"] {
            let body = format!("{{\"k\":\"{k}\",\"n\":{r}}}");
            let (st, _, _) = preq(
                addr,
                "POST",
                "/v1/streams/sselin/records",
                &[
                    ("prisma-encryption-key", PRISMA_KEY),
                    ("prisma-routing-key", k),
                ],
                body.as_bytes(),
            )
            .await;
            assert!(st == 200 || st == 204);
        }
    }

    let mut sck = tokio::net::TcpStream::connect(addr).await.unwrap();
    let req = format!(
        "GET /v1/streams/sselin/records:sse?routingKey=ga HTTP/1.1\r\nhost: x\r\ncontent-length: 0\r\nprisma-encryption-key: {RIG_KEY_B64}\r\n\r\n"
    );
    sck.write_all(req.as_bytes()).await.unwrap();
    let mut buf = vec![0u8; 8192];
    let mut acc: Vec<u8> = Vec::new();
    let mut ns: Vec<i64> = Vec::new();
    let mut saw_utd = false;
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    'read: while std::time::Instant::now() < deadline {
        let n = match tokio::time::timeout(std::time::Duration::from_secs(5), sck.read(&mut buf))
            .await
        {
            Ok(r) => r.expect("sse read"),
            Err(_) => panic!(
                "sse read timed out; acc={} bytes, ns={ns:?}, utd={saw_utd}, tail:\n{}",
                acc.len(),
                String::from_utf8_lossy(&acc[acc.len().saturating_sub(600)..])
            ),
        };
        if n == 0 {
            break;
        }
        acc.extend_from_slice(&buf[..n]);
        let text = String::from_utf8_lossy(&acc).to_string();
        ns.clear();
        saw_utd = false;
        for chunk in text.split("\n\n") {
            let mut is_control = false;
            for line in chunk.lines() {
                if line.starts_with("event: control") {
                    is_control = true;
                }
                if let Some(d) = line.strip_prefix("data:") {
                    if is_control {
                        if d.contains("\"upToDate\":true") {
                            saw_utd = true;
                        }
                        assert!(
                            !d.contains("streamClosed"),
                            "no closure on a live lineage: {d}"
                        );
                    } else if let Ok(v) = serde_json::from_str::<serde_json::Value>(d) {
                        // data events carry the JSON-array framing.
                        let rec = if v.is_array() { v[0].clone() } else { v };
                        if rec["k"] == "ga" {
                            ns.push(rec["n"].as_i64().unwrap());
                        }
                    }
                }
            }
        }
        if ns.len() >= 10 && saw_utd {
            break 'read;
        }
    }
    assert_eq!(
        ns,
        (0..10).collect::<Vec<i64>>(),
        "every generation's records, in order"
    );
    assert!(saw_utd, "upToDate control after the drain");
    drop(sck);
    engine_shutdown(&state).await;
}

/// Stage 6: product SSE control frames carry SIGNED key cursors with
/// product field names — never a raw Stream-Next-Offset token.
#[expect(
    clippy::too_many_lines,
    reason = "signed control scenario; the product control frames, their signed key cursors and the resumed subscription form one causal sequence; helper phases would hide which frame carried a raw cursor"
)]
#[expect(
    clippy::excessive_nesting,
    reason = "product_sse_controls_carry_signed_cursors; the fixture nests the line scan inside the chunk scan of each read; flattening it would separate the control frames from the chunk that carries them"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn product_sse_controls_carry_signed_cursors() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let store = mem();
    let (state, addr) = http_rig(store).await;
    let (st, _, _) = preq(
        addr,
        "PUT",
        "/v1/streams/psse",
        &[("prisma-encryption-key", PRISMA_KEY)],
        br#"{"format":{"kind":"json"}}"#,
    )
    .await;
    assert_eq!(st, 201);
    for n in 0..2 {
        let body = format!("{{\"n\":{n}}}");
        let (st, _, _) = preq(
            addr,
            "POST",
            "/v1/streams/psse/records",
            &[
                ("prisma-encryption-key", PRISMA_KEY),
                ("prisma-routing-key", "s1"),
            ],
            body.as_bytes(),
        )
        .await;
        assert_eq!(st, 200);
    }
    let mut sck = tokio::net::TcpStream::connect(addr).await.unwrap();
    let req = format!(
        "GET /v1/streams/psse/records:sse?routingKey=s1 HTTP/1.1\r\nhost: x\r\ncontent-length: 0\r\nprisma-encryption-key: {PRISMA_KEY}\r\n\r\n"
    );
    sck.write_all(req.as_bytes()).await.unwrap();
    let mut buf = vec![0u8; 8192];
    let mut acc: Vec<u8> = Vec::new();
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(8);
    let mut data_n: Vec<i64> = Vec::new();
    let mut cursor_tok: Option<String> = None;
    while std::time::Instant::now() < deadline {
        let n = tokio::time::timeout(std::time::Duration::from_secs(4), sck.read(&mut buf))
            .await
            .expect("sse read timeout")
            .expect("sse read");
        if n == 0 {
            break;
        }
        acc.extend_from_slice(&buf[..n]);
        let text = String::from_utf8_lossy(&acc).to_string();
        assert!(
            !text.contains("streamNextOffset"),
            "product SSE leaked a raw offset token:\n{text}"
        );
        data_n.clear();
        cursor_tok = None;
        // Every data event is PAIRED with a control event, so controls
        // for record 0 (cursor offset 1) and record 1 (offset 2) both
        // arrive. Only a control that follows BOTH data events proves
        // the final cursor — grabbing whichever control happens to be
        // in the buffer raced the pair flush (latent flake surfaced by
        // unrelated scheduling shifts).
        for chunk in text.split("\n\n") {
            let mut is_control = false;
            for line in chunk.lines() {
                if line.starts_with("event: control") {
                    is_control = true;
                }
                if let Some(d) = line.strip_prefix("data:") {
                    if is_control {
                        if data_n.len() >= 2
                            && let Ok(v) = serde_json::from_str::<serde_json::Value>(d)
                            && let Some(c) = v["nextCursor"].as_str()
                        {
                            cursor_tok = Some(c.to_string());
                        }
                    } else if let Ok(v) = serde_json::from_str::<serde_json::Value>(d) {
                        let rec = if v.is_array() { v[0].clone() } else { v };
                        if let Some(x) = rec["n"].as_i64() {
                            data_n.push(x);
                        }
                    }
                }
            }
        }
        if data_n.len() >= 2 && cursor_tok.is_some() {
            break;
        }
    }
    assert_eq!(data_n, vec![0, 1], "catch-up records in order");
    let tok = cursor_tok.expect("control frame with nextCursor");
    let desc = state
        .registry
        .get(&state.deployment.raw_adapter_sref("psse"))
        .await
        .unwrap()
        .unwrap();
    let epoch = desc.epoch_bytes().unwrap();
    let kh = crate::crypto::stream_hash("s1");
    let kc = crate::product_cursor::KeyCursor::decode(
        &tok,
        state.deployment.deployment_tenant(),
        &skey(),
        &epoch,
        &kh,
    )
    .expect("signed cursor decodes");
    assert_eq!(kc.offset, 2, "cursor sits after the two records");
    drop(sck);
    engine_shutdown(&state).await;
}

// ------------------------------------------------------------------
// Round-4 finding 4 (red): EXACT-ONCE termination accounting. The
// producer task and the response-body gate each hold their own
// LeaseWatch for the same connection; unsynchronized, one invalidated
// subscription produced one OR TWO termination counts depending on
// scheduling — and a watcher alone can miss being first entirely (the
// producer quitting hands the body a plain EOF). The canary checklist
// reads termination-by-reason telemetry, so the count must be exactly
// one per invalidated subscription.
// ------------------------------------------------------------------
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn termination_reasons_count_exactly_once_per_subscription() {
    let _serial = gap_lock().lock().await; // process-global counters
    let (_svc, _state, addr) = auth_rig("proj-tc1", "ws_tc", &["c1"], None).await;
    // Expires in 5 s: long enough to park two connections, short
    // enough to watch both deadlines fire.
    let tok = mint_token("c1", "proj-tc1", "ws_tc", 1, 1, "tc1", 5);
    rig_create(addr, "tc1", &tok).await;
    let mut promoter = rig_sse(addr, "tc1", &tok, "?cursor=now", None).await;
    let (a, _) = hub_sse_collect(&mut promoter, 8, |t| t.contains("upToDate")).await;
    assert!(a.contains("upToDate"), "promoter parks:\n{a}");
    let mut sub = rig_sse(addr, "tc1", &tok, "", None).await;
    let (b, _) = hub_sse_collect(&mut sub, 8, |t| t.contains("upToDate")).await;
    assert!(b.contains("upToDate"), "hub sub parks:\n{b}");

    let idx = crate::auth::LeaseInvalidReason::TokenExpired.index();
    let before =
        crate::sse::auth::LEASE_TERMINATIONS[idx].load(std::sync::atomic::Ordering::Relaxed);
    // No activity: both subscriptions must die at token expiry.
    let (_, eof_p) = hub_sse_collect(&mut promoter, 20, |_| false).await;
    let (_, eof_s) = hub_sse_collect(&mut sub, 10, |_| false).await;
    assert!(
        eof_p && eof_s,
        "both subscriptions must terminate at expiry"
    );
    let after =
        crate::sse::auth::LEASE_TERMINATIONS[idx].load(std::sync::atomic::Ordering::Relaxed);
    assert_eq!(
        after - before,
        2,
        "two invalidated subscriptions = exactly two termination counts \
         (one per connection), not one per LeaseWatch"
    );
}

// ------------------------------------------------------------------
// Review rank 13 (red): a RAW session sharing its feed is owed the
// standalone upToDate a solo one already gets when a batch at the
// durable frontier carries nothing for it.
// ------------------------------------------------------------------

/// A RAW live subscription on `name` from the current frontier
/// (`offset=now`): the raw surface always rides the default lane.
async fn raw_sse_connect(addr: std::net::SocketAddr, name: &str) -> tokio::net::TcpStream {
    use tokio::io::AsyncWriteExt;
    let mut sck = tokio::net::TcpStream::connect(addr).await.unwrap();
    let req = format!(
        "GET /v1/stream/{name}?live=sse&offset=now HTTP/1.1\r\nhost: x\r\ncontent-length: 0\r\nstream-encryption-key: {RIG_KEY_B64}\r\n\r\n"
    );
    sck.write_all(req.as_bytes()).await.unwrap();
    sck
}

/// One product append under routing key `ka`: the default lane scans
/// it and matches nothing, a match-free window at the frontier.
async fn foreign_append(addr: std::net::SocketAddr, name: &str, i: u64) {
    let (st, _, _) = preq(
        addr,
        "POST",
        &format!("/v1/streams/{name}/records"),
        &[
            ("prisma-encryption-key", PRISMA_KEY),
            ("prisma-routing-key", "ka"),
        ],
        format!(r#"{{"k":"ka","i":{i}}}"#).as_bytes(),
    )
    .await;
    assert!(st == 200 || st == 204, "foreign append {i}: {st}");
}

/// The raw `streamNextOffset` token a control names for `next`: the
/// encoding `sse_control_ep` uses (segment 0; START when next is 0).
fn raw_next_tok(next: u64) -> String {
    let off = next
        .checked_sub(1)
        .map_or(crate::offsets::Offset::START, |o| {
            crate::offsets::Offset(Some(o))
        });
    crate::offsets::encode_ep(0, off)
}

/// Payloads of the control frames carrying `upToDate`, in wire order.
/// Frames are `\n\n`-separated; the chunked-transfer framing lines
/// between them carry neither prefix and fall out of the line scan.
fn up_to_date_controls(t: &str) -> Vec<String> {
    t.split("\n\n")
        .filter(|frame| frame.lines().any(|l| l.starts_with("event: control")))
        .filter_map(|frame| frame.lines().find_map(|l| l.strip_prefix("data:")))
        .filter(|d| d.contains("\"upToDate\":true"))
        .map(str::to_owned)
        .collect()
}

/// A standalone/paired control naming `next` that carries upToDate: the
/// stop condition AND the assertion input, so a TCP read that splits a
/// control between its token and its flag cannot end the collect early
/// (frame order: streamNextOffset, streamCursor, upToDate).
fn status_at(t: &str, next: u64) -> bool {
    let tok = raw_next_tok(next);
    up_to_date_controls(t).iter().any(|d| d.contains(&tok))
}

/// The default-lane feed of `name`: raw and product sessions share it.
async fn default_lane_feed(
    state: &std::sync::Arc<crate::http::AppState>,
    name: &str,
) -> std::sync::Arc<crate::sse::feed::LiveFeed> {
    let sref = state.deployment.raw_adapter_sref(name);
    state.registry.invalidate(&sref);
    let desc = state.registry.get(&sref).await.unwrap().unwrap();
    let key = crate::sse::session::feed_key_of(&desc, &Some(String::new()));
    state
        .livefeed
        .registry()
        .feed_for_test(&key)
        .expect("the default-lane feed exists while sessions are attached")
}

/// Review rank 13 (red): the shared `Take::Batch` arm cleared the RAW
/// status on `head_here` alone, while the Solo arms guard on a record
/// actually sent, so a RAW session that shared its feed was left
/// without `upToDate` after a batch that carried nothing for it, and
/// wire behaviour depended on the subscriber count. Raw and product
/// sessions share one default-lane feed, so the second subscriber is
/// a product one; the last window also pins the per-surface framing
/// after a shared RECORD (raw: ONE paired control carrying upToDate,
/// product: bare control + standalone status).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shared_raw_subscriber_gets_up_to_date_after_a_match_free_batch() {
    let store = mem();
    let (state, addr) = http_rig(store).await;
    let ct = ("content-type", "application/json");
    let (st, _, _) = hreq(addr, "PUT", "/v1/stream/rawshare", &[ct], b"").await;
    assert!(st == 200 || st == 201, "create {st}");

    // SOLO control (passes today): a match-free window at the frontier
    // yields the standalone status at next=1; the Solo arm guards on
    // last_off, so nothing sent means the status is still owed.
    let mut raw = raw_sse_connect(addr, "rawshare").await;
    let (a0, _) = hub_sse_collect(&mut raw, 8, |t| t.contains("upToDate")).await;
    assert!(
        a0.contains(&raw_next_tok(0)),
        "raw parks at the head:\n{a0}"
    );
    foreign_append(addr, "rawshare", 0).await;
    let (a1, _) = hub_sse_collect(&mut raw, 8, |t| status_at(t, 1)).await;
    assert_eq!(
        up_to_date_controls(&a1).len(),
        1,
        "solo raw: ONE standalone upToDate at next=1 for a match-free window:\n{a1}"
    );

    // SHARED: a product session joins the same default-lane feed.
    let mut prod = lf_connect(addr, "rawshare", "?cursor=now").await;
    let (p0, _) = hub_sse_collect(&mut prod, 8, |t| t.contains("upToDate")).await;
    assert!(
        p0.contains("\"upToDate\":true"),
        "product parks at the head:\n{p0}"
    );
    assert_eq!(
        default_lane_feed(&state, "rawshare")
            .await
            .subscriber_count(),
        2,
        "raw and product sessions share ONE feed"
    );

    // THE RED: a second match-free window at the frontier is a shared
    // batch with no record for either session. Both are owed the
    // standalone status at next=2; the raw session was left silent.
    foreign_append(addr, "rawshare", 1).await;
    let (p1, _) = hub_sse_collect(&mut prod, 8, |t| t.contains("\"upToDate\":true")).await;
    assert!(
        p1.contains("\"upToDate\":true"),
        "shared product: status at the frontier:\n{p1}"
    );
    let (a2, _) = hub_sse_collect(&mut raw, 5, |t| status_at(t, 2)).await;
    assert_eq!(
        up_to_date_controls(&a2).len(),
        1,
        "shared raw: ONE standalone upToDate at next=2 for a match-free window (solo got one at next=1):\n{a2}"
    );

    // Paired-control exactness after a shared RECORD: raw folds
    // upToDate into its ONE paired control and never adds a standalone
    // duplicate; product keeps bare control + standalone status.
    let (st, _, _) = hreq(addr, "POST", "/v1/stream/rawshare", &[ct], br#"[{"r":1}]"#).await;
    assert!(st == 200 || st == 204, "raw append {st}");
    let (a3, _) = hub_sse_collect(&mut raw, 8, |t| t.contains(&raw_next_tok(3))).await;
    let (a3x, _) = hub_sse_collect(&mut raw, 2, |_| false).await;
    let a3 = format!("{a3}{a3x}");
    assert!(a3.contains("\"r\":1"), "shared raw: the record:\n{a3}");
    assert_eq!(
        a3.matches("event: control").count(),
        1,
        "raw: ONE paired control:\n{a3}"
    );
    assert_eq!(
        up_to_date_controls(&a3).len(),
        1,
        "raw: upToDate rides the paired control only (no standalone duplicate):\n{a3}"
    );
    let (p2, _) = hub_sse_collect(&mut prod, 8, |t| lf_record_and_status(t, "\"r\":1")).await;
    assert!(
        lf_record_and_status(&p2, "\"r\":1"),
        "shared product: record, then standalone upToDate:\n{p2}"
    );
    drop(raw);
    drop(prod);
    engine_shutdown(&state).await;
}
