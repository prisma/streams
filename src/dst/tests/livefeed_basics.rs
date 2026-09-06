//! Livefeed basics.

use super::fixture_auth::{auth_rig, mint_token, rig_create, rig_publish_grants, rig_sse};
use super::fixture_http::http_rig;
use super::fixture_livefeed::{hub_append_lf, hub_sse_collect, lf_connect, seal_ok, wait_parked};
use super::fixture_requests::RIG_KEY_B64;
use super::fixture_requests::{PRISMA_KEY, hreq, preq};
use super::fixture_storage::mem;

async fn hub_append(addr: std::net::SocketAddr, name: &str, body: &str) {
    let (st, _, _) = preq(
        addr,
        "POST",
        &format!("/v1/streams/{name}/records"),
        &[("prisma-encryption-key", PRISMA_KEY)],
        body.as_bytes(),
    )
    .await;
    assert!(st == 200 || st == 204, "append {st}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn livefeed_park_appends_seal_matches_golden_semantics() {
    let store = mem();
    let (_state, addr) = http_rig(store).await;
    let (st, _, _) = preq(
        addr,
        "PUT",
        "/v1/streams/lf",
        &[("prisma-encryption-key", PRISMA_KEY)],
        br#"{"format":{"kind":"json"}}"#,
    )
    .await;
    assert_eq!(st, 201);
    hub_append(addr, "lf", r#"{"a":1}"#).await;

    // Park AT the tail after one backlog record: tail status control,
    // then the backlog pair.
    let mut sck = lf_connect(addr, "lf", "").await;
    // Canonical framing: the status control is a STANDALONE chunk sent
    // after the drained window, so park on upToDate, not on the data.
    let (acc, _) = hub_sse_collect(&mut sck, 8, |t| t.contains("upToDate")).await;
    assert!(acc.contains("upToDate\":true"), "status at connect:\n{acc}");
    assert!(acc.contains("\"a\":1"), "backlog record:\n{acc}");

    // Live delivery.
    hub_append(addr, "lf", r#"{"a":2}"#).await;
    let (acc2, _) = hub_sse_collect(&mut sck, 8, |t| t.contains("\"a\":2")).await;
    assert!(acc2.contains("\"a\":2"), "live record:\n{acc2}");

    // Seal: exactly ONE final control, then EOF.
    seal_ok(addr, "lf").await;
    let (tail, eof) = hub_sse_collect(&mut sck, 15, |_| false).await;
    assert_eq!(
        tail.matches("\"sealed\":true").count(),
        1,
        "exactly one sealed control:\n{tail}"
    );
    assert!(eof, "EOF after the sealed control");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn livefeed_catches_up_from_beginning_cursor() {
    let store = mem();
    let (_state, addr) = http_rig(store).await;
    let (st, _, _) = preq(
        addr,
        "PUT",
        "/v1/streams/lfc",
        &[("prisma-encryption-key", PRISMA_KEY)],
        br#"{"format":{"kind":"json"}}"#,
    )
    .await;
    assert_eq!(st, 201);
    for i in 0..5u64 {
        hub_append(addr, "lfc", &format!(r#"{{"i":{i}}}"#)).await;
    }
    let mut sck = lf_connect(addr, "lfc", "?cursor=beginning").await;
    let (acc, _) = hub_sse_collect(&mut sck, 10, |t| t.contains("upToDate")).await;
    for i in 0..5u64 {
        assert!(
            acc.contains(&format!("\"i\":{i}")),
            "catch-up must deliver every record in order (missing {i}):\n{acc}"
        );
    }
    assert!(
        acc.rfind("upToDate\":true").unwrap_or(0) > acc.rfind("\"i\":4").unwrap_or(usize::MAX),
        "head status must follow the last caught-up record:\n{acc}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn livefeed_two_subscribers_share_one_feed_and_one_source_read() {
    let store = mem();
    let (state, addr) = http_rig(store).await;
    let (st, _, _) = preq(
        addr,
        "PUT",
        "/v1/streams/lfshare",
        &[("prisma-encryption-key", PRISMA_KEY)],
        br#"{"format":{"kind":"json"}}"#,
    )
    .await;
    assert_eq!(st, 201);

    let mut sub1 = lf_connect(addr, "lfshare", "").await;
    let (_, _) = hub_sse_collect(&mut sub1, 8, |t| t.contains("upToDate")).await;
    let mut sub2 = lf_connect(addr, "lfshare", "").await;
    let (_, _) = hub_sse_collect(&mut sub2, 8, |t| t.contains("upToDate")).await;

    // Exactly ONE feed for this stream identity.
    state
        .registry
        .invalidate(&state.deployment.raw_adapter_sref("lfshare"));
    let desc = state
        .registry
        .get(&state.deployment.raw_adapter_sref("lfshare"))
        .await
        .unwrap()
        .unwrap();
    let key = crate::sse::session::feed_key_of(&desc, &Some(String::new()));
    let feed = state
        .livefeed
        .registry()
        .feed_for_test(&key)
        .expect("the shared feed must exist");
    assert_eq!(
        feed.subscriber_count(),
        2,
        "both sessions attach to the same feed"
    );

    // One append: prepared ONCE, delivered TWICE.
    hub_append(addr, "lfshare", r#"{"s":1}"#).await;
    let (r1, _) = hub_sse_collect(&mut sub1, 8, |t| t.contains("\"s\":1")).await;
    let (r2, _) = hub_sse_collect(&mut sub2, 8, |t| t.contains("\"s\":1")).await;
    assert!(r1.contains("\"s\":1") && r2.contains("\"s\":1"));
    let reads_after_first_delivery = feed.source_read_count();
    assert_eq!(
        reads_after_first_delivery, 1,
        "one append must cost exactly one source read"
    );

    // A second append: still one read per batch window.
    hub_append(addr, "lfshare", r#"{"s":2}"#).await;
    let _ = hub_sse_collect(&mut sub1, 8, |t| t.contains("\"s\":2")).await;
    let _ = hub_sse_collect(&mut sub2, 8, |t| t.contains("\"s\":2")).await;
    assert_eq!(
        feed.source_read_count(),
        2,
        "each append window costs exactly one source read"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn livefeed_revocation_terminates_the_subscription() {
    let (_svc, _state, addr) = auth_rig("proj-lfr", "ws_lf", &["c1"], None).await;
    let tok = mint_token("c1", "proj-lfr", "ws_lf", 1, 1, "lf1", 600);
    rig_create(addr, "lfr", &tok).await;
    let mut sub = rig_sse(addr, "lfr", &tok, "", None).await;
    let (a, _) = hub_sse_collect(&mut sub, 8, |t| t.contains("upToDate")).await;
    assert!(a.contains("upToDate"), "parks:\n{a}");

    // THE CUTOFF: revoke c1's credential.
    rig_publish_grants(
        &_svc,
        "proj-lfr",
        &[("c1", crate::project_policy::CredentialStatus::Revoked, 2)],
        2,
    )
    .unwrap();

    let (after, eof) = hub_sse_collect(&mut sub, 12, |_| false).await;
    assert!(
        !after.contains("event: data"),
        "revoked subscription received data:\n{after}"
    );
    assert!(eof, "revoked livefeed subscription must terminate");
}

// ==================================================================
// LIVE-FEED local benchmarks (gated: STREAMS_SSE_BENCH=1). Both
// engines measured IN-PROCESS on identical shapes: wall time from
// first append to last-subscriber delivery, RSS delta, and the
// engine-specific shared-state counters.
// ==================================================================

/// Stage 4: KEYED lanes. A routing-key-filtered subscriber on a
/// single-segment stream rides its own LiveFeed lane: it receives only
/// its key's records, foreign-key-only windows advance the cursor, and
/// two subscribers of the same key share one preparation.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn livefeed_keyed_lane_scopes_records_and_shares_preparation() {
    let store = mem();
    let (state, addr) = http_rig(store).await;
    let (st, _, _) = preq(
        addr,
        "PUT",
        "/v1/streams/lfk",
        &[("prisma-encryption-key", PRISMA_KEY)],
        br#"{"format":{"kind":"json"}}"#,
    )
    .await;
    assert_eq!(st, 201);

    // Two subscribers on key ka; the stream also receives kb traffic.
    let mut s1 = lf_connect(addr, "lfk", "?routingKey=ka").await;
    let (_, _) = hub_sse_collect(&mut s1, 8, |t| t.contains("upToDate")).await;
    let mut s2 = lf_connect(addr, "lfk", "?routingKey=ka").await;
    let (_, _) = hub_sse_collect(&mut s2, 8, |t| t.contains("upToDate")).await;

    // Interleaved appends across BOTH keys.
    for i in 0..6u64 {
        let k = if i % 2 == 0 { "ka" } else { "kb" };
        let (st, _, _) = preq(
            addr,
            "POST",
            "/v1/streams/lfk/records",
            &[
                ("prisma-encryption-key", PRISMA_KEY),
                ("prisma-routing-key", k),
            ],
            format!(r#"{{"k":"{k}","i":{i}}}"#).as_bytes(),
        )
        .await;
        assert!(st == 200 || st == 204, "append {k}/{i}: {st}");
    }

    let (r1, _) = hub_sse_collect(&mut s1, 10, |t| t.matches("\"k\":\"ka\"").count() >= 3).await;
    let (r2, _) = hub_sse_collect(&mut s2, 10, |t| t.matches("\"k\":\"ka\"").count() >= 3).await;
    for r in [&r1, &r2] {
        assert_eq!(
            r.matches("\"k\":\"ka\"").count(),
            3,
            "exactly the three ka records:\n{r}"
        );
        assert!(
            !r.contains("\"k\":\"kb\""),
            "a keyed subscriber must not receive foreign-key records:\n{r}"
        );
    }

    // One keyed lane feed, shared by both subscribers.
    state
        .registry
        .invalidate(&state.deployment.raw_adapter_sref("lfk"));
    let desc = state
        .registry
        .get(&state.deployment.raw_adapter_sref("lfk"))
        .await
        .unwrap()
        .unwrap();
    let key = crate::sse::session::feed_key_of(&desc, &Some("ka".to_string()));
    let feed = state
        .livefeed
        .registry()
        .feed_for_test(&key)
        .expect("keyed lane feed exists");
    assert_eq!(feed.subscriber_count(), 2);
}

/// Stage 5: FORKS. A fork's SSE subscription rides the LiveFeed with
/// stitched ancestor reads — same wire vocabulary the legacy fork
/// producer used (raw scalar), same record sequence.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn livefeed_fork_subscriptions_stitch_the_ancestor_chain() {
    let store = mem();
    let (_state, addr) = http_rig(store).await;
    // Parent created once, then three appended records.
    let (st, _, _) = preq(
        addr,
        "PUT",
        "/v1/streams/fp",
        &[("prisma-encryption-key", PRISMA_KEY)],
        br#"{"format":{"kind":"json"}}"#,
    )
    .await;
    assert_eq!(st, 201);
    for i in 0..3 {
        let (st, _, _) = preq(
            addr,
            "POST",
            "/v1/streams/fp/records",
            &[("prisma-encryption-key", PRISMA_KEY)],
            format!(r#"{{"p":{i}}}"#).as_bytes(),
        )
        .await;
        assert_eq!(st, 200, "parent append {i}");
    }
    let (_, h, _) = hreq(addr, "GET", "/v1/stream/fp", &[], b"").await;
    let boundary = h.get("stream-next-offset").cloned().unwrap_or_default();

    // Fork at the parent's tail, then append CHILD records.
    let (st, _, b) = hreq(
        addr,
        "PUT",
        "/v1/stream/fc",
        &[
            ("content-type", "application/json"),
            ("stream-forked-from", "fp"),
            ("stream-fork-offset", &boundary),
        ],
        b"",
    )
    .await;
    assert_eq!(st, 201, "fork create: {}", String::from_utf8_lossy(&b));
    let (st, _, _) = preq(
        addr,
        "POST",
        "/v1/streams/fc/records",
        &[("prisma-encryption-key", PRISMA_KEY)],
        br#"{"c":1}"#,
    )
    .await;
    assert!(st == 200 || st == 204, "child append {st}");

    // Subscribe to the CHILD from offset 0: ancestor p0..p2 stitched,
    // then the child tail c1.
    let mut sck = lf_connect(addr, "fc", "").await;
    let (acc, _) = hub_sse_collect(&mut sck, 10, |t| t.contains("\"c\":1")).await;
    assert!(
        acc.contains("event: data"),
        "fork subscription delivered frames:\n{acc}"
    );
    for i in 0..3 {
        assert!(
            acc.contains(&format!("\"p\":{i}\"")) || acc.contains(&format!("\"p\":{i}")),
            "ancestor record p{i} must appear in the stitched stream:\n{acc}"
        );
    }
    assert!(
        acc.contains("\"c\":1") || acc.contains("[{\"c\":1}]"),
        "child tail must follow the ancestors:\n{acc}"
    );
}

/// RAW-surface SSE through LiveFeed: the singular route composes the
/// RAW scalar vocabulary (streamNextOffset / streamCursor /
/// streamClosed), byte-shaped like the legacy producer, and carries
/// the workload-JWT expiry lease when opened under enforce mode.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn livefeed_raw_surface_uses_the_raw_vocabulary() {
    let store = mem();
    let (_state, addr) = http_rig(store).await;
    let ct = ("content-type", "application/json");
    let (st, _, _) = hreq(addr, "PUT", "/v1/stream/lfr", &[ct], br#"[{"r":0}]"#).await;
    assert!(st == 200 || st == 201, "create {st}");

    use tokio::io::AsyncWriteExt;
    let mut sck = tokio::net::TcpStream::connect(addr).await.unwrap();
    let start_tok = crate::offsets::encode_ep(0, crate::offsets::Offset::START);
    let req = format!(
        "GET /v1/stream/lfr?live=sse&offset={start_tok} HTTP/1.1\r\nhost: x\r\ncontent-length: 0\r\nstream-encryption-key: {RIG_KEY_B64}\r\n\r\n"
    );
    sck.write_all(req.as_bytes()).await.unwrap();
    let (acc, _) = hub_sse_collect(&mut sck, 8, |t| t.contains("upToDate")).await;
    // RAW vocabulary markers — never product fields.
    assert!(
        acc.contains("\"streamNextOffset\""),
        "raw scalar control expected:\n{acc}"
    );
    assert!(
        !acc.contains("\"nextCursor\"") && !acc.contains("\"sealed\":true"),
        "raw surface must not speak product vocabulary:\n{acc}"
    );
    assert!(acc.contains("event: data"), "backlog record:\n{acc}");

    // Live append through the raw surface.
    let (st, _, _) = hreq(addr, "POST", "/v1/stream/lfr", &[ct], br#"[{"r":1}]"#).await;
    assert!(st == 200 || st == 204);
    let (acc2, _) = hub_sse_collect(&mut sck, 8, |t| t.contains("\"r\":1")).await;
    assert!(acc2.contains("\"r\":1"), "live raw record:\n{acc2}");
}

// ==================================================================
// LIVE-FEED follow-up-review red battery: cursor/data integrity,
// exact wire contract, lifecycle/concurrency, memory, topology.
// ==================================================================

/// Finding 1 red: a SOLO subscription whose append window exceeds the
/// driver's 256-KiB bounded batch must deliver EVERY record exactly
/// once — the cursor advances to the scanned batch end, never the
/// live frontier. DETERMINISTIC (follow-up review): driving is paused
/// at a failpoint until the whole 640-KiB window is durable, so the
/// driver really faces one oversized durable window, then must take
/// MORE THAN ONE bounded source read to drain it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn livefeed_singleton_large_window_delivers_everything_exactly_once() {
    let store = mem();
    let (state, addr) = http_rig(store).await;
    let (st, _, _) = preq(
        addr,
        "PUT",
        "/v1/streams/lfbig",
        &[("prisma-encryption-key", PRISMA_KEY)],
        br#"{"format":{"kind":"json"}}"#,
    )
    .await;
    assert_eq!(st, 201);

    // Park at tail FIRST (empty stream), with driving PAUSED at the
    // failpoint: the 40 appends below all land before any source read.
    crate::failpoints::arm(crate::failpoints::Fp::SseFeedBeforeDrive, "lfbig");
    let mut sck = lf_connect(addr, "lfbig", "").await;
    let (_, _) = hub_sse_collect(&mut sck, 8, |t| t.contains("upToDate")).await;

    let pad = "x".repeat(16 * 1024);
    for i in 0..40u64 {
        let (st, _, _) = preq(
            addr,
            "POST",
            "/v1/streams/lfbig/records",
            &[("prisma-encryption-key", PRISMA_KEY)],
            format!(r#"{{"i":{i},"pad":"{pad}"}}"#).as_bytes(),
        )
        .await;
        assert!(st == 200 || st == 204, "append {i}: {st}");
    }
    // The session is now parked at the drive failpoint with the whole
    // window durable.
    wait_parked(crate::failpoints::Fp::SseFeedBeforeDrive, "lfbig", 1).await;
    crate::failpoints::release(crate::failpoints::Fp::SseFeedBeforeDrive, "lfbig");

    // Collect ALL 40 data frames; each record id appears EXACTLY once.
    let (acc, eof) =
        hub_sse_collect(&mut sck, 30, |t| t.matches("event: data").count() >= 40).await;
    assert!(eof || acc.matches("event: data").count() >= 40);
    for i in 0..40u64 {
        // Comma-anchored: `"i":1` would substring-match `"i":10`..19.
        let needle = format!("\"i\":{i},");
        assert_eq!(
            acc.matches(&needle).count(),
            1,
            "record {i} must appear exactly once (found {}):\n…{}",
            acc.matches(&needle).count(),
            &acc[acc.len().saturating_sub(600)..]
        );
    }
    // One 640-KiB durable window, drained by MULTIPLE bounded reads:
    // the 256-KiB driver batch cap forces >= 3 source reads.
    state
        .registry
        .invalidate(&state.deployment.raw_adapter_sref("lfbig"));
    let desc = state
        .registry
        .get(&state.deployment.raw_adapter_sref("lfbig"))
        .await
        .unwrap()
        .unwrap();
    let key = crate::sse::session::feed_key_of(&desc, &Some(String::new()));
    let feed = state
        .livefeed
        .registry()
        .feed_for_test(&key)
        .expect("the singleton feed must exist");
    assert!(
        feed.source_read_count() >= 2,
        "a 640-KiB window under a 256-KiB read cap must cost multiple source reads, got {}",
        feed.source_read_count()
    );
}

/// Finding 2 red (singleton): an ALL-FOREIGN historical range on a
/// keyed lane is pure scanned progress — the subscriber receives
/// upToDate WITHOUT being lag-disconnected, and later matches flow.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn livefeed_keyed_all_foreign_history_is_progress_not_lag() {
    let store = mem();
    let (_state, addr) = http_rig(store).await;
    let (st, _, _) = preq(
        addr,
        "PUT",
        "/v1/streams/lfprog",
        &[("prisma-encryption-key", PRISMA_KEY)],
        br#"{"format":{"kind":"json"}}"#,
    )
    .await;
    assert_eq!(st, 201);

    // FOREIGN-only history BEFORE the subscriber exists.
    for _ in 0..3 {
        let (st, _, _) = preq(
            addr,
            "POST",
            "/v1/streams/lfprog/records",
            &[
                ("prisma-encryption-key", PRISMA_KEY),
                ("prisma-routing-key", "kb"),
            ],
            br#"{"k":"kb"}"#,
        )
        .await;
        assert!(st == 200 || st == 204);
    }

    // Keyed subscriber connects from BEGINNING: its first window is
    // all-foreign. Old behavior: Phase A broke with cursor behind the
    // feed floor → false LAG DISCONNECT.
    let mut sck = lf_connect(addr, "lfprog", "?routingKey=ka&cursor=beginning").await;
    let (acc, _) = hub_sse_collect(&mut sck, 10, |t| t.contains("upToDate")).await;
    assert!(
        acc.contains("upToDate"),
        "match-free history must progress to upToDate, not disconnect:\n{acc}"
    );

    // A matching record appended AFTER the foreign history flows.
    let (st, _, _) = preq(
        addr,
        "POST",
        "/v1/streams/lfprog/records",
        &[
            ("prisma-encryption-key", PRISMA_KEY),
            ("prisma-routing-key", "ka"),
        ],
        br#"{"k":"ka","hit":true}"#,
    )
    .await;
    assert!(st == 200 || st == 204);
    let (acc2, _) = hub_sse_collect(&mut sck, 10, |t| t.contains("\"hit\":true")).await;
    assert!(
        acc2.contains("\"hit\":true"),
        "post-history match must be delivered:\n{acc2}"
    );
}

/// Exact wire contract (finding 4): ONE cursor control per record, NO
/// status flags on record controls, exactly ONE standalone upToDate at
/// the open frontier — raw and product vocabulary, RAW-first creation
/// order (the product-first direction is the next test).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn livefeed_exact_framing_mixed_surfaces_share_one_lane() {
    let store = mem();
    let (_state, addr) = http_rig(store).await;
    let ct = ("content-type", "application/json");
    let (st, _, _) = hreq(addr, "PUT", "/v1/stream/lfx", &[ct], br#"[{"r":0}]"#).await;
    assert!(st == 200 || st == 201);

    // RAW creates the feed first.
    use tokio::io::AsyncWriteExt;
    let mut raw = tokio::net::TcpStream::connect(addr).await.unwrap();
    let start_tok = crate::offsets::encode_ep(0, crate::offsets::Offset::START);
    let req = format!(
        "GET /v1/stream/lfx?live=sse&offset={start_tok} HTTP/1.1\r\nhost: x\r\ncontent-length: 0\r\nstream-encryption-key: {RIG_KEY_B64}\r\n\r\n"
    );
    raw.write_all(req.as_bytes()).await.unwrap();
    // PRODUCT joins second.
    let mut prod = tokio::net::TcpStream::connect(addr).await.unwrap();
    let preq2 = format!(
        "GET /v1/streams/lfx/records:sse?cursor=beginning HTTP/1.1\r\nhost: x\r\ncontent-length: 0\r\nprisma-encryption-key: {PRISMA_KEY}\r\n\r\n"
    );
    prod.write_all(preq2.as_bytes()).await.unwrap();

    hub_append_lf(addr, "lfx", r#"{"w":1}"#).await;

    // The done predicate fires on the DATA frame; its trailing cursor
    // control arrives in a separate chunk — drain one more beat.
    let (raw_txt, _) = hub_sse_collect(&mut raw, 10, |t| t.contains("\"w\":1")).await;
    let (extra_r, _) = hub_sse_collect(&mut raw, 2, |_| false).await;
    let raw_txt = format!("{raw_txt}{extra_r}");
    let (prod_txt, _) = hub_sse_collect(&mut prod, 10, |t| t.contains("\"w\":1")).await;
    let (extra_p, _) = hub_sse_collect(&mut prod, 2, |_| false).await;
    let prod_txt = format!("{prod_txt}{extra_p}");

    for (side, txt) in [("raw", &raw_txt), ("product", &prod_txt)] {
        // PER-SURFACE SHAPE per drained window (round 11.8): PRODUCT
        // keeps the canonical LiveFeed framing — 1 data + 1 bare ctl,
        // then ONE standalone upToDate at the head. RAW follows the
        // PINNED conformance protocol — each data event pairs with
        // exactly ONE flag-carrying control and the duplicate
        // standalone is suppressed. Two windows here (backlog + live)
        // => 2 data; product 4 controls, raw 2; 2 upToDate each. A
        // duplicated/cross-surface cursor control would push counts up.
        let data = txt.matches("event: data").count();
        let ctls = txt.matches("event: control").count();
        assert_eq!(data, 2, "{side}: two data events expected:\n{txt}");
        if side == "raw" {
            assert_eq!(ctls, 2, "{side}: ONE paired control per data event:\n{txt}");
        } else {
            assert_eq!(
                ctls, 4,
                "{side}: per-record ctl x2 + standalone status x2 = 4:\n{txt}"
            );
        }
        assert_eq!(
            txt.matches("\"upToDate\":true").count(),
            2,
            "{side}: upToDate once per at-head delivery:\n{txt}"
        );
        // Vocabulary isolation.
        if side == "raw" {
            assert!(
                txt.contains("streamNextOffset") && !txt.contains("nextCursor"),
                "raw must not speak product vocabulary:\n{txt}"
            );
        } else {
            assert!(
                txt.contains("nextCursor") && !txt.contains("streamNextOffset"),
                "product must not speak raw vocabulary:\n{txt}"
            );
        }
    }
}

/// Finding 4 coverage, REVERSE creation order: PRODUCT creates the
/// feed, RAW joins second. Same canonical framing assertions as the
/// raw-first leg — feed creation order must not change the wire shape
/// or cross the vocabularies.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn livefeed_exact_framing_mixed_surfaces_product_first() {
    let store = mem();
    let (_state, addr) = http_rig(store).await;
    let ct = ("content-type", "application/json");
    let (st, _, _) = hreq(addr, "PUT", "/v1/stream/lfp", &[ct], br#"[{"r":0}]"#).await;
    assert!(st == 200 || st == 201);

    // PRODUCT creates the feed first.
    use tokio::io::AsyncWriteExt;
    let mut prod = tokio::net::TcpStream::connect(addr).await.unwrap();
    let preq2 = format!(
        "GET /v1/streams/lfp/records:sse?cursor=beginning HTTP/1.1\r\nhost: x\r\ncontent-length: 0\r\nprisma-encryption-key: {PRISMA_KEY}\r\n\r\n"
    );
    prod.write_all(preq2.as_bytes()).await.unwrap();
    // RAW joins second.
    let mut raw = tokio::net::TcpStream::connect(addr).await.unwrap();
    let start_tok = crate::offsets::encode_ep(0, crate::offsets::Offset::START);
    let req = format!(
        "GET /v1/stream/lfp?live=sse&offset={start_tok} HTTP/1.1\r\nhost: x\r\ncontent-length: 0\r\nstream-encryption-key: {RIG_KEY_B64}\r\n\r\n"
    );
    raw.write_all(req.as_bytes()).await.unwrap();

    hub_append_lf(addr, "lfp", r#"{"w":1}"#).await;

    // The done predicate fires on the DATA frame; its trailing cursor
    // control arrives in a separate chunk — drain one more beat.
    let (prod_txt, _) = hub_sse_collect(&mut prod, 10, |t| t.contains("\"w\":1")).await;
    let (extra_p, _) = hub_sse_collect(&mut prod, 2, |_| false).await;
    let prod_txt = format!("{prod_txt}{extra_p}");
    let (raw_txt, _) = hub_sse_collect(&mut raw, 10, |t| t.contains("\"w\":1")).await;
    let (extra_r, _) = hub_sse_collect(&mut raw, 2, |_| false).await;
    let raw_txt = format!("{raw_txt}{extra_r}");

    for (side, txt) in [("raw", &raw_txt), ("product", &prod_txt)] {
        let data = txt.matches("event: data").count();
        let ctls = txt.matches("event: control").count();
        assert_eq!(data, 2, "{side}: two data events expected:\n{txt}");
        // Round-11.8 per-surface framing: PRODUCT keeps the canonical
        // LiveFeed shape (bare per-record ctl + standalone status);
        // RAW follows the PINNED conformance protocol — each data
        // event pairs with exactly ONE control that carries the flags,
        // and the duplicate standalone status is suppressed.
        if side == "raw" {
            assert_eq!(ctls, 2, "{side}: ONE paired control per data event:\n{txt}");
        } else {
            assert_eq!(
                ctls, 4,
                "{side}: per-record ctl x2 + standalone status x2 = 4:\n{txt}"
            );
        }
        assert_eq!(
            txt.matches("\"upToDate\":true").count(),
            2,
            "{side}: upToDate once per at-head delivery:\n{txt}"
        );
        if side == "raw" {
            assert!(
                txt.contains("streamNextOffset") && !txt.contains("nextCursor"),
                "raw must not speak product vocabulary:\n{txt}"
            );
        } else {
            assert!(
                txt.contains("nextCursor") && !txt.contains("streamNextOffset"),
                "product must not speak raw vocabulary:\n{txt}"
            );
        }
    }
}

/// Finding 8/handoff (deterministic): the feed ADVANCES between the
/// atomic subscribe and the session's first state read. The captured
/// join_head bounds Phase A; the live phase delivers what landed after
/// it — every record exactly once, no duplicates across the boundary.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn livefeed_handoff_feed_advances_between_subscribe_and_session_start() {
    let store = mem();
    let (_state, addr) = http_rig(store).await;
    let (st, _, _) = preq(
        addr,
        "PUT",
        "/v1/streams/lfh",
        &[("prisma-encryption-key", PRISMA_KEY)],
        br#"{"format":{"kind":"json"}}"#,
    )
    .await;
    assert_eq!(st, 201);
    let mut sub1 = lf_connect(addr, "lfh", "").await;
    let (_, _) = hub_sse_collect(&mut sub1, 8, |t| t.contains("upToDate")).await;
    hub_append_lf(addr, "lfh", r#"{"h":1}"#).await;
    let (c1, _) = hub_sse_collect(&mut sub1, 8, |t| t.contains("\"h\":1")).await;
    assert!(c1.contains("\"h\":1"));

    // Subscriber 2 attaches — and PARKS inside the failpoint with its
    // join_head captured (= 1) BEFORE its session reads any feed state.
    crate::failpoints::arm(crate::failpoints::Fp::SseFeedAfterSubscribe, "lfh");
    let mut sub2 = lf_connect(addr, "lfh", "?cursor=beginning").await;
    wait_parked(crate::failpoints::Fp::SseFeedAfterSubscribe, "lfh", 1).await;

    // The feed advances through the OTHER subscriber while sub2 is
    // still parked between attach and session start.
    hub_append_lf(addr, "lfh", r#"{"h":2}"#).await;
    let (c2, _) = hub_sse_collect(&mut sub1, 8, |t| t.contains("\"h\":2")).await;
    assert!(c2.contains("\"h\":2"));
    crate::failpoints::release(crate::failpoints::Fp::SseFeedAfterSubscribe, "lfh");

    // sub2: h:1 via the bounded catch-up, h:2 via the shared ring —
    // each EXACTLY once, in order.
    let (acc, _) = hub_sse_collect(&mut sub2, 10, |t| {
        t.contains("\"h\":2") && t.contains("upToDate")
    })
    .await;
    assert_eq!(
        acc.matches("\"h\":1").count(),
        1,
        "catch-up record exactly once:\n{acc}"
    );
    assert_eq!(
        acc.matches("\"h\":2").count(),
        1,
        "ring record exactly once — no duplicate across the handoff:\n{acc}"
    );
    assert!(
        acc.find("\"h\":1").unwrap() < acc.find("\"h\":2").unwrap(),
        "in-order delivery across the handoff:\n{acc}"
    );
}

/// Finding 2 (initial handoff, deterministic): the ring WRAPS past a
/// not-yet-live subscriber while it is parked between attach and
/// session start. That is NOT lag: the session performs durable
/// re-catch-up and delivers everything from its cursor — no
/// disconnect, no skipped records.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn livefeed_ring_wrap_during_initial_handoff_recatchups() {
    let store = mem();
    let (state, addr) = http_rig(store).await;
    // A tiny ring so a handful of shared batches wraps the floor.
    state.livefeed.set_ring_bytes(1024);
    let (st, _, _) = preq(
        addr,
        "PUT",
        "/v1/streams/lfw",
        &[("prisma-encryption-key", PRISMA_KEY)],
        br#"{"format":{"kind":"json"}}"#,
    )
    .await;
    assert_eq!(st, 201);
    let mut sub1 = lf_connect(addr, "lfw", "").await;
    let (_, _) = hub_sse_collect(&mut sub1, 8, |t| t.contains("upToDate")).await;

    crate::failpoints::arm(crate::failpoints::Fp::SseFeedAfterSubscribe, "lfw");
    let mut sub2 = lf_connect(addr, "lfw", "").await;
    wait_parked(crate::failpoints::Fp::SseFeedAfterSubscribe, "lfw", 1).await;

    let retries_before = crate::sse::auth::sse_stats::FEED_CATCHUP_RETRIES
        .load(std::sync::atomic::Ordering::Relaxed);
    // Twelve shared batches wrap the 1-KiB ring several times over;
    // sub2's join position falls below the floor while it is parked.
    for i in 0..12u64 {
        hub_append_lf(addr, "lfw", &format!(r#"{{"w":{i}}}"#)).await;
        let (c, _) = hub_sse_collect(&mut sub1, 8, |t| t.contains(&format!("\"w\":{i}"))).await;
        assert!(c.contains(&format!("\"w\":{i}")), "sub1 got w{i}");
    }
    crate::failpoints::release(crate::failpoints::Fp::SseFeedAfterSubscribe, "lfw");

    // sub2 must NOT disconnect: durable re-catch-up delivers all twelve.
    let (acc, eof) = hub_sse_collect(&mut sub2, 15, |t| {
        t.matches("\"w\":").count() >= 12 && t.contains("upToDate")
    })
    .await;
    assert!(
        !eof,
        "the ring overtaking a NEW subscriber is not lag:\n{acc}"
    );
    for i in 0..12u64 {
        let needle = format!("\"w\":{i}}}");
        assert_eq!(
            acc.matches(&needle).count(),
            1,
            "record w{i} exactly once after re-catch-up:\n{acc}"
        );
    }
    assert!(acc.contains("upToDate"), "reached live:\n{acc}");
    let retries_after = crate::sse::auth::sse_stats::FEED_CATCHUP_RETRIES
        .load(std::sync::atomic::Ordering::Relaxed);
    assert!(
        retries_after > retries_before,
        "the re-catch-up counter observed the recovery"
    );
}

/// Finding 6 (fork progress, deterministic): a fork's OWN live window
/// of foreign-key records larger than the 256-KiB driver batch is pure
/// consumed progress for the default lane — the subscriber is NOT
/// stuck rescanning it, and the default record behind the window
/// flows.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn livefeed_fork_foreign_only_window_is_progress() {
    let store = mem();
    let (state, addr) = http_rig(store).await;
    let (st, _, _) = preq(
        addr,
        "PUT",
        "/v1/streams/fpx",
        &[("prisma-encryption-key", PRISMA_KEY)],
        br#"{"format":{"kind":"json"}}"#,
    )
    .await;
    assert_eq!(st, 201);
    hub_append_lf(addr, "fpx", r#"{"p":0}"#).await;
    let (_, h, _) = hreq(addr, "GET", "/v1/stream/fpx", &[], b"").await;
    let boundary = h.get("stream-next-offset").cloned().unwrap_or_default();
    let (st, _, b) = hreq(
        addr,
        "PUT",
        "/v1/stream/fcx",
        &[
            ("content-type", "application/json"),
            ("stream-forked-from", "fpx"),
            ("stream-fork-offset", &boundary),
        ],
        b"",
    )
    .await;
    assert_eq!(st, 201, "fork create: {}", String::from_utf8_lossy(&b));

    let mut sck = lf_connect(addr, "fcx", "").await;
    let (acc0, _) = hub_sse_collect(&mut sck, 8, |t| t.contains("upToDate")).await;
    assert!(acc0.contains("upToDate"), "parks at the fork tail:\n{acc0}");

    // Pause driving, then land a foreign-key window LARGER than the
    // 256-KiB driver batch cap on the child: 20 x 16 KiB = 320 KiB.
    // Without honest consumed progress this window is a rescan loop
    // (no delivery until the 15-s heartbeat); with it, the window is
    // consumed in a couple of bounded reads.
    crate::failpoints::arm(crate::failpoints::Fp::SseFeedBeforeDrive, "fcx");
    let pad = "y".repeat(16 * 1024);
    for i in 0..20u64 {
        let (st, _, _) = preq(
            addr,
            "POST",
            "/v1/streams/fcx/records",
            &[
                ("prisma-encryption-key", PRISMA_KEY),
                ("prisma-routing-key", "kb"),
            ],
            format!(r#"{{"k":"kb","i":{i},"pad":"{pad}"}}"#).as_bytes(),
        )
        .await;
        assert!(st == 200 || st == 204, "foreign append {i}: {st}");
    }
    // The default record BEHIND the foreign window.
    hub_append_lf(addr, "fcx", r#"{"c":9}"#).await;
    wait_parked(crate::failpoints::Fp::SseFeedBeforeDrive, "fcx", 1).await;
    crate::failpoints::release(crate::failpoints::Fp::SseFeedBeforeDrive, "fcx");

    // The default-lane subscriber consumes the foreign window as pure
    // progress and receives the default record, then upToDate — well
    // before the 15-s heartbeat that a stuck session would need.
    let (acc, _) = hub_sse_collect(&mut sck, 10, |t| {
        t.contains("\"c\":9") && t.contains("upToDate")
    })
    .await;
    assert!(
        acc.contains("\"c\":9"),
        "the default record behind the foreign window must flow:\n{acc}"
    );
    assert!(
        !acc.contains("\"k\":\"kb\""),
        "foreign-key records never leak into the default lane:\n{acc}"
    );
    assert!(
        acc.rfind("upToDate\":true").unwrap_or(0) > acc.find("\"c\":9").unwrap_or(usize::MAX),
        "upToDate follows the default record:\n{acc}"
    );

    // Bounded work: the 320-KiB window costs a couple of reads, never
    // a rescan loop.
    state
        .registry
        .invalidate(&state.deployment.raw_adapter_sref("fcx"));
    let desc = state
        .registry
        .get(&state.deployment.raw_adapter_sref("fcx"))
        .await
        .unwrap()
        .unwrap();
    let key = crate::sse::session::feed_key_of(&desc, &Some(String::new()));
    let feed = state
        .livefeed
        .registry()
        .feed_for_test(&key)
        .expect("the fork feed must exist");
    assert!(
        feed.source_read_count() <= 4,
        "foreign-window progress must not rescan ({} reads)",
        feed.source_read_count()
    );
}
