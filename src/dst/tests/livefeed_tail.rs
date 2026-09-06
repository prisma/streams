//! Livefeed tail.

use super::fixture_failpoints::gap_lock;
use super::fixture_http::http_rig;
use super::fixture_livefeed::{
    hub_append_lf, hub_sse_collect, last_next_cursor, lf_connect, lf_record_and_status, seal_ok,
    split_and_await, wait_parked,
};
use super::fixture_requests::{PRISMA_KEY, hreq, preq};
use super::fixture_storage::mem;

/// Finding 10 coverage: a BINARY stream through LiveFeed carries
/// `Stream-SSE-Data-Encoding: base64` and base64 data frames. (Exact
/// bytes_out accounting is pinned by the deterministic unit leg
/// `sse::session::tests::bytes_out_accounts_exactly_the_emitted_frames`;
/// the process-global usage map overflows into a shared aggregate
/// under a parallel suite, so an HTTP-level counter equality would be
/// flaky by construction.)
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn livefeed_binary_base64_header_and_exact_bytes_out() {
    let store = mem();
    let (_state, addr) = http_rig(store).await;
    let ct = ("content-type", "application/octet-stream");
    let (st, _, _) = hreq(addr, "PUT", "/v1/stream/lfb2", &[ct], b"").await;
    assert!(st == 200 || st == 201, "binary create: {st}");

    let mut sck = lf_connect(addr, "lfb2", "").await;
    // One binary record (non-UTF8 bytes prove base64, not lossy text).
    let payload: &[u8] = b"\x00\x01\x02\xfe\xffbinary-tail";
    let (st, _, _) = hreq(
        addr,
        "POST",
        "/v1/stream/lfb2",
        &[("content-type", "application/octet-stream")],
        payload,
    )
    .await;
    assert!(st == 200 || st == 204, "binary append: {st}");

    use base64::Engine;
    let want_b64 = base64::engine::general_purpose::STANDARD.encode(payload);
    let (acc, _) = hub_sse_collect(&mut sck, 10, |t| {
        t.contains(&format!("data:{want_b64}")) && t.contains("upToDate")
    })
    .await;
    let (more, _) = hub_sse_collect(&mut sck, 2, |_| false).await;
    let acc = format!("{acc}{more}");

    let lower = acc.to_ascii_lowercase();
    assert!(
        lower.contains("stream-sse-data-encoding: base64"),
        "the base64 encoding header is on the response:\n{}",
        &acc[..acc.len().min(600)]
    );
    assert!(
        acc.contains(&format!("data:{want_b64}")),
        "the binary payload is base64-framed:\n{acc}"
    );
    assert!(acc.contains("upToDate"), "status at head:\n{acc}");
    // EXACT bytes_out accounting is pinned deterministically by the
    // unit leg sse::session::tests::bytes_out_accounts_exactly_the_
    // emitted_frames (the process-global usage map falls back to a
    // shared overflow aggregate under a parallel suite, so a global-
    // counter equality here would be flaky by construction).
}

/// Terminal transcript (exact): the final data record, its bare cursor
/// control, then EXACTLY ONE sealed control and EOF — and NO open
/// upToDate status at the terminal position preceding it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn livefeed_seal_transcript_exact_tail() {
    let store = mem();
    let (_state, addr) = http_rig(store).await;
    let (st, _, _) = preq(
        addr,
        "PUT",
        "/v1/streams/lft",
        &[("prisma-encryption-key", PRISMA_KEY)],
        br#"{"format":{"kind":"json"}}"#,
    )
    .await;
    assert_eq!(st, 201);
    hub_append_lf(addr, "lft", r#"{"t":1}"#).await;
    let mut sck = lf_connect(addr, "lft", "?cursor=beginning").await;
    let (acc0, _) = hub_sse_collect(&mut sck, 8, |t| t.contains("upToDate")).await;
    assert!(acc0.contains("\"t\":1"), "backlog:\n{acc0}");

    seal_ok(addr, "lft").await;
    let (tail, eof) = hub_sse_collect(&mut sck, 15, |_| false).await;
    assert!(eof, "EOF after the terminal control");
    // The post-seal tail carries EXACTLY ONE control: the terminal.
    // The session was parked at an open frontier when the seal landed,
    // so its closure discovery must NOT emit a standalone open
    // upToDate at the terminal position first (the terminal control
    // itself legitimately carries upToDate+sealed together).
    assert!(
        !tail.contains("\"upToDate\":true}"),
        "no standalone open status may precede the terminal at the same position:\n{tail}"
    );
    assert_eq!(
        tail.matches("\"sealed\":true").count(),
        1,
        "exactly ONE sealed control in the tail:\n{tail}"
    );
    let full = format!("{acc0}{tail}");
    assert_eq!(
        full.matches("\"sealed\":true").count(),
        1,
        "exactly ONE sealed control in the whole transcript:\n{full}"
    );
    // The sealed control is the LAST control of the transcript.
    let last_ctl = &full[full.rfind("event: control").unwrap()..];
    assert!(
        last_ctl.contains("\"sealed\":true"),
        "the last control is the terminal one:\n{last_ctl}"
    );
}

// ==================================================================
// LIVE-FEED Stage 6 legs: source swap across splits. A parked or
// catching-up PRODUCT session survives a split IN PLACE (source
// generation +1, no disconnect, no false terminal), keyed lanes keep
// their isolation, raw takes the typed disconnect fallback, and a
// genuine seal after a split is still exactly one terminal control.
// ==================================================================

/// 6.5 headline leg: a subscriber PARKED at upToDate rides a split in
/// place — no EOF, no sealed control, and records on the successor
/// segment flow exactly once.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn livefeed_split_parked_subscriber_continues_in_place() {
    let store = mem();
    let (state, addr) = http_rig(store).await;
    let (st, _, _) = preq(
        addr,
        "PUT",
        "/v1/streams/lfs",
        &[("prisma-encryption-key", PRISMA_KEY)],
        br#"{"format":{"kind":"json"}}"#,
    )
    .await;
    assert_eq!(st, 201);
    hub_append_lf(addr, "lfs", r#"{"s":0}"#).await;
    let mut sck = lf_connect(addr, "lfs", "").await;
    let (acc0, _) = hub_sse_collect(&mut sck, 8, |t| t.contains("upToDate")).await;
    assert!(acc0.contains("\"s\":0"), "backlog:\n{acc0}");

    // SPLIT while the subscriber is parked at the frontier.
    split_and_await(&state, "lfs", 0).await;

    // Successor records flow on the SAME connection.
    hub_append_lf(addr, "lfs", r#"{"s":1}"#).await;
    let (acc1, eof1) = hub_sse_collect(&mut sck, 10, |t| lf_record_and_status(t, "\"s\":1")).await;
    assert!(
        !eof1,
        "a split must NOT disconnect a product livefeed session:\n{acc1}"
    );
    assert!(
        !acc1.contains("\"sealed\":true"),
        "a split must NOT emit a terminal control:\n{acc1}"
    );
    assert!(acc1.contains("\"s\":1"), "successor record flows:\n{acc1}");

    // A genuine seal AFTER the split is still exactly one terminal
    // control, then EOF.
    seal_ok(addr, "lfs").await;
    // 30s, matching the seal-gap rigs: loaded CI runners take
    // multiples of a laptop through engine-open + heartbeat paths; the
    // deadline exists to catch a REAL wedge, not to race the scheduler.
    let (tail, eof2) = hub_sse_collect(&mut sck, 30, |_| false).await;
    assert!(eof2, "EOF after the genuine seal");
    assert_eq!(
        tail.matches("\"sealed\":true").count(),
        1,
        "exactly one terminal control after the split:\n{tail}"
    );
}

/// The seal's two-step window, held open DETERMINISTICALLY (the
/// suite-wedge class this round root-caused): step 2 closes the live
/// segment — firing the handle notify the parked session consumes —
/// while step 3 (the Sealed publication) is withheld at the crash
/// boundary. The woken session drives against the pre-publication
/// topology (RetryLater, no version bump) and the publication itself
/// touches no feed state, so ONLY the session's own bounded re-drive
/// can deliver the terminal: with a dead heartbeat ticker and the
/// 3600-s no-lease nap, the pre-fix park wedged forever.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn livefeed_seal_publication_race_converges_without_heartbeat() {
    let _serial = gap_lock().lock().await; // global failpoint registry
    let store = mem();
    let (state, addr) = http_rig(store).await;
    let (st, _, _) = preq(
        addr,
        "PUT",
        "/v1/streams/lfsr",
        &[("prisma-encryption-key", PRISMA_KEY)],
        br#"{"format":{"kind":"json"}}"#,
    )
    .await;
    assert_eq!(st, 201);
    hub_append_lf(addr, "lfsr", r#"{"s":0}"#).await;
    let mut sck = lf_connect(addr, "lfsr", "").await;
    let (acc0, _) = hub_sse_collect(&mut sck, 8, |t| t.contains("upToDate")).await;
    assert!(acc0.contains("\"s\":0"), "backlog:\n{acc0}");
    split_and_await(&state, "lfsr", 0).await;
    hub_append_lf(addr, "lfsr", r#"{"s":1}"#).await;
    let (acc1, eof1) = hub_sse_collect(&mut sck, 10, |t| lf_record_and_status(t, "\"s\":1")).await;
    assert!(!eof1, "the split must not disconnect:\n{acc1}");

    // Hold the seal at the crash boundary: segment closes durable
    // (notify fires; the parked session wakes into the withheld
    // window), Sealed publication withheld.
    crate::failpoints::stop_before_sealed_publish("lfsr");
    let (st, _, body) = preq(
        addr,
        "POST",
        "/v1/streams/lfsr:seal",
        &[("prisma-encryption-key", PRISMA_KEY)],
        b"{}",
    )
    .await;
    let text = String::from_utf8_lossy(&body);
    assert!(
        st == 500 || st == 503,
        "the interruption must answer retryable failure: {st} {text}"
    );
    // No false terminal, no EOF while the publication is withheld.
    let (mid, eof_mid) = hub_sse_collect(&mut sck, 2, |_| false).await;
    assert!(!eof_mid, "no EOF while the publication is withheld:\n{mid}");
    assert!(
        !mid.contains("\"sealed\":true"),
        "no false terminal in the withheld window:\n{mid}"
    );

    // Release and complete the seal. NOTHING wakes the feed for this
    // publication — the terminal must arrive through the session's
    // bounded re-drive, well under the 15-s heartbeat.
    crate::failpoints::stop_before_sealed_publish_off("lfsr");
    seal_ok(addr, "lfsr").await;
    let t0 = std::time::Instant::now();
    let (tail, eof) = hub_sse_collect(&mut sck, 10, |_| false).await;
    assert!(eof, "EOF after the released seal:\n{tail}");
    assert!(
        t0.elapsed() < std::time::Duration::from_secs(8),
        "the terminal must arrive by bounded re-drive, not a heartbeat rescue ({:?})",
        t0.elapsed()
    );
    assert_eq!(
        tail.matches("\"sealed\":true").count(),
        1,
        "exactly one terminal control:\n{tail}"
    );
}

// ==================================================================
// STAGE 7B replacement legs (round-9 review Phase 2): the
// engine-neutral contracts the excluded hub_* tests pinned,
// re-asserted against the LiveFeed engine at the wire. The exclusion
// table (docs/LIVE-FEED.md §Transition record) named each replacement;
// the old hub tests die with the legacy engine.
// ==================================================================

/// Replaces `off_mode_subscriptions_are_untouched_by_staleness`:
/// AuthMode::Off means no lease — a parked LiveFeed subscriber stays
/// open indefinitely (no staleness notion applies) and stays LIVE.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn livefeed_off_mode_subscription_ignores_staleness() {
    let store = mem();
    let (_state, addr) = http_rig(store).await;
    let (st, _, _) = preq(
        addr,
        "PUT",
        "/v1/streams/lfoff",
        &[("prisma-encryption-key", PRISMA_KEY)],
        br#"{"format":{"kind":"json"}}"#,
    )
    .await;
    assert_eq!(st, 201);
    hub_append_lf(addr, "lfoff", r#"{"o":0}"#).await;
    let mut sck = lf_connect(addr, "lfoff", "").await;
    let (a, _) = hub_sse_collect(&mut sck, 8, |t| t.contains("upToDate")).await;
    assert!(a.contains("\"o\":0"), "backlog:\n{a}");
    let (_, eof) = hub_sse_collect(&mut sck, 5, |_| false).await;
    assert!(!eof, "Off mode: no lease, no staleness termination");
    // Liveness after the quiet hold: a new record still flows.
    hub_append_lf(addr, "lfoff", r#"{"o":1}"#).await;
    let (t, eof2) = hub_sse_collect(&mut sck, 8, |t| lf_record_and_status(t, "\"o\":1")).await;
    assert!(!eof2 && t.contains("\"o\":1"), "still live:\n{t}");
}

/// Replaces `hub_empty_seal_single_final_control`: sealing an EMPTY
/// stream emits exactly ONE terminal control, then EOF — no data ever.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn livefeed_empty_seal_single_final_control() {
    let store = mem();
    let (_state, addr) = http_rig(store).await;
    let (st, _, _) = preq(
        addr,
        "PUT",
        "/v1/streams/lfes",
        &[("prisma-encryption-key", PRISMA_KEY)],
        br#"{"format":{"kind":"json"}}"#,
    )
    .await;
    assert_eq!(st, 201);
    let mut sck = lf_connect(addr, "lfes", "").await;
    let (a, _) = hub_sse_collect(&mut sck, 8, |t| t.contains("upToDate")).await;
    assert!(
        !a.contains("event: data"),
        "an empty stream has no data:\n{a}"
    );
    seal_ok(addr, "lfes").await;
    let (tail, eof) = hub_sse_collect(&mut sck, 15, |_| false).await;
    assert!(eof, "EOF after the empty seal");
    assert_eq!(
        tail.matches("\"sealed\":true").count(),
        1,
        "exactly one terminal control on an empty seal:\n{tail}"
    );
    assert!(
        !tail.contains("event: data"),
        "no data may accompany an empty seal:\n{tail}"
    );
}

/// Replaces `hub_no_up_to_date_while_pump_holds_backlog`: upToDate is
/// a statement about the DURABLE stream — while durable records exist
/// that the feed has not yet driven, no upToDate may be claimed. The
/// drive is held at its failpoint with a durable backlog behind it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn livefeed_no_up_to_date_while_backlog_is_undriven() {
    let store = mem();
    let (_state, addr) = http_rig(store).await;
    let (st, _, _) = preq(
        addr,
        "PUT",
        "/v1/streams/lfhb",
        &[("prisma-encryption-key", PRISMA_KEY)],
        br#"{"format":{"kind":"json"}}"#,
    )
    .await;
    assert_eq!(st, 201);
    let mut sck = lf_connect(addr, "lfhb", "").await;
    let (a, _) = hub_sse_collect(&mut sck, 8, |t| t.contains("upToDate")).await;
    assert!(a.contains("\"upToDate\":true"), "parks at the tail:\n{a}");

    crate::failpoints::arm(crate::failpoints::Fp::SseFeedBeforeDrive, "lfhb");
    hub_append_lf(addr, "lfhb", r#"{"b":0}"#).await;
    hub_append_lf(addr, "lfhb", r#"{"b":1}"#).await;
    wait_parked(crate::failpoints::Fp::SseFeedBeforeDrive, "lfhb", 1).await;
    // The backlog is durable and UNDRIVEN: no data, and above all no
    // upToDate claim, may appear.
    let (held, eof) = hub_sse_collect(&mut sck, 2, |_| false).await;
    assert!(!eof, "no disconnect while the drive is held:\n{held}");
    assert!(
        !held.contains("\"upToDate\":true"),
        "upToDate claimed over an undriven durable backlog:\n{held}"
    );
    assert!(
        !held.contains("event: data"),
        "no data can flow while the drive is held:\n{held}"
    );
    crate::failpoints::release(crate::failpoints::Fp::SseFeedBeforeDrive, "lfhb");
    let (t, eof2) = hub_sse_collect(&mut sck, 10, |t| lf_record_and_status(t, "\"b\":1")).await;
    assert!(!eof2, "released drive keeps the session open:\n{t}");
    for n in ["\"b\":0", "\"b\":1"] {
        assert_eq!(t.matches(n).count(), 1, "{n} exactly once:\n{t}");
    }
    let last_data = t.rfind("\"b\":1").expect("frontier record");
    let utd = t.find("\"upToDate\":true").expect("honest status");
    assert!(
        utd > last_data,
        "the resumed upToDate must FOLLOW the drained backlog:\n{t}"
    );
}

/// Replaces `hub_prepared_batch_must_not_carry_stale_up_to_date`: a
/// late attacher draining the retained ring while newer durable
/// records exist must not see upToDate until it truly reaches the
/// frontier — retained batches can never carry a stale claim
/// (LiveFeed record frames are structurally bare; this pins the
/// SESSION's status honesty over the ring-drain path).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn livefeed_late_attach_never_claims_stale_up_to_date() {
    let store = mem();
    let (_state, addr) = http_rig(store).await;
    let (st, _, _) = preq(
        addr,
        "PUT",
        "/v1/streams/lfst",
        &[("prisma-encryption-key", PRISMA_KEY)],
        br#"{"format":{"kind":"json"}}"#,
    )
    .await;
    assert_eq!(st, 201);
    hub_append_lf(addr, "lfst", r#"{"s":0}"#).await;
    let mut s1 = lf_connect(addr, "lfst", "").await;
    let (a1, _) = hub_sse_collect(&mut s1, 8, |t| t.contains("upToDate")).await;
    assert!(a1.contains("\"s\":0"));

    // s2 attaches (shared: the ring retains from here) but is HELD
    // between attach and session start while the stream moves on.
    crate::failpoints::arm(crate::failpoints::Fp::SseFeedAfterSubscribe, "lfst");
    let mut s2 = lf_connect(addr, "lfst", "?cursor=beginning").await;
    wait_parked(crate::failpoints::Fp::SseFeedAfterSubscribe, "lfst", 1).await;
    hub_append_lf(addr, "lfst", r#"{"s":1}"#).await;
    hub_append_lf(addr, "lfst", r#"{"s":2}"#).await;
    // s1 drains the new records — the retained ring now holds batches
    // prepared while s2 was parked.
    let (d1, _) = hub_sse_collect(&mut s1, 10, |t| lf_record_and_status(t, "\"s\":2")).await;
    assert!(d1.contains("\"s\":2"));
    crate::failpoints::release(crate::failpoints::Fp::SseFeedAfterSubscribe, "lfst");

    let (a2, eof) = hub_sse_collect(&mut s2, 10, |t| lf_record_and_status(t, "\"s\":2")).await;
    assert!(!eof, "late attach must complete in place:\n{a2}");
    for i in 0..3u64 {
        let needle = format!("\"s\":{i}");
        assert_eq!(
            a2.matches(&needle).count(),
            1,
            "{needle} exactly once:\n{a2}"
        );
    }
    let last_data = a2.rfind("\"s\":2").expect("frontier record");
    let first_utd = a2.find("\"upToDate\":true").expect("status");
    assert!(
        first_utd > last_data,
        "upToDate claimed before the frontier record was delivered:\n{a2}"
    );
}

/// Replaces `hub_oversized_event_delivered_via_uncached_catchup` for
/// the LiveFeed posture. Singleton: an over-ring record is delivered
/// IN PLACE (solo drives retain nothing). Shared: the uncached
/// publication floors the ring — parked subscribers take the TYPED
/// lag disconnect (no terminal, no loss) and a durable resume from
/// their emitted cursor delivers the record exactly once. That
/// disconnect-and-resume trade (vs the hub's in-place uncached
/// catch-up) is the documented bounded-memory design.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn livefeed_oversized_record_solo_in_place_shared_resumes_durably() {
    let store = mem();
    let (state, addr) = http_rig(store).await;
    // Tiny feed ring: a 64-KiB record can never be retained.
    state.livefeed.set_ring_bytes(4096);
    let (st, _, _) = preq(
        addr,
        "PUT",
        "/v1/streams/lfbig2",
        &[("prisma-encryption-key", PRISMA_KEY)],
        br#"{"format":{"kind":"json"}}"#,
    )
    .await;
    assert_eq!(st, 201);
    let big = format!(r#"{{"big":"{}"}}"#, "y".repeat(64 * 1024));

    // SINGLETON: over-ring record delivered in place, no disconnect.
    let mut s1 = lf_connect(addr, "lfbig2", "").await;
    let (a, _) = hub_sse_collect(&mut s1, 8, |t| t.contains("upToDate")).await;
    assert!(a.contains("upToDate"));
    hub_append_lf(addr, "lfbig2", &big).await;
    let (solo, eof) =
        hub_sse_collect(&mut s1, 10, |t| lf_record_and_status(t, "\"big\":\"yy")).await;
    assert!(!eof, "solo oversized delivery stays in place:\n…");
    assert_eq!(
        solo.matches("\"big\":\"").count(),
        1,
        "oversized exactly once"
    );

    // SHARED: the second oversized publication goes uncached; both
    // subscribers take a RESUMABLE EOF (the typed reason is
    // server-side only — counter + log; no wire error control, NO
    // terminal) and resume durably with no gap and no duplicate.
    let mut s2 = lf_connect(addr, "lfbig2", "").await;
    let (b, _) = hub_sse_collect(&mut s2, 8, |t| t.contains("upToDate")).await;
    let cur2 = last_next_cursor(&b);
    let cur1 = last_next_cursor(&solo);
    hub_append_lf(addr, "lfbig2", &big).await;
    for (n, (sck, cur)) in [(1, (&mut s1, cur1)), (2, (&mut s2, cur2))] {
        let (tail, eof) = hub_sse_collect(sck, 10, |_| false).await;
        assert!(
            eof,
            "sub{n}: uncached publication takes the typed disconnect"
        );
        assert!(
            !tail.contains("\"sealed\":true"),
            "sub{n}: a lag disconnect is never a terminal:\n{tail}"
        );
        let mut resumed = lf_connect(addr, "lfbig2", &format!("?cursor={cur}")).await;
        let (r, eof2) = hub_sse_collect(&mut resumed, 10, |t| {
            lf_record_and_status(t, "\"big\":\"yy")
        })
        .await;
        assert!(!eof2, "sub{n}: the resume completes in place:\n…");
        assert_eq!(
            r.matches("\"big\":\"").count(),
            1,
            "sub{n}: the oversized record arrives exactly once on resume"
        );
    }
}

/// Replaces `hub_global_cap_exhaustion_goes_uncached_but_delivers` for
/// the LiveFeed posture: with the process retention budget exhausted,
/// a shared feed's publication goes UNCACHED — parked subscribers get
/// a resumable EOF (typed reason server-side only; no terminal, no
/// loss) and, once budget exists again, resume durably with no gap
/// and no duplicate.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn livefeed_budget_exhaustion_publishes_uncached_and_resumes() {
    let store = mem();
    let (state, addr) = http_rig(store).await;
    let (st, _, _) = preq(
        addr,
        "PUT",
        "/v1/streams/lfcap",
        &[("prisma-encryption-key", PRISMA_KEY)],
        br#"{"format":{"kind":"json"}}"#,
    )
    .await;
    assert_eq!(st, 201);
    hub_append_lf(addr, "lfcap", r#"{"c":0}"#).await;
    let mut s1 = lf_connect(addr, "lfcap", "").await;
    let (a1, _) = hub_sse_collect(&mut s1, 8, |t| t.contains("upToDate")).await;
    let cur1 = last_next_cursor(&a1);
    let mut s2 = lf_connect(addr, "lfcap", "").await;
    let (a2, _) = hub_sse_collect(&mut s2, 8, |t| t.contains("upToDate")).await;
    let cur2 = last_next_cursor(&a2);

    // Exhaust the process retention budget, then publish.
    let held = state.livefeed.budget().exhaust_for_test();
    hub_append_lf(addr, "lfcap", r#"{"c":1}"#).await;
    let mut cursors = Vec::new();
    for (n, (sck, cur)) in [(1, (&mut s1, cur1)), (2, (&mut s2, cur2))] {
        let (tail, eof) = hub_sse_collect(sck, 10, |_| false).await;
        assert!(
            eof,
            "sub{n}: an uncached publication takes the typed disconnect:\n{tail}"
        );
        assert!(
            !tail.contains("\"sealed\":true"),
            "sub{n}: never a terminal:\n{tail}"
        );
        cursors.push((n, cur));
    }
    // Budget returns; both resumes deliver the record exactly once.
    state.livefeed.budget().release_for_test(held);
    for (n, cur) in cursors {
        let mut resumed = lf_connect(addr, "lfcap", &format!("?cursor={cur}")).await;
        let (r, eof) =
            hub_sse_collect(&mut resumed, 10, |t| lf_record_and_status(t, "\"c\":1")).await;
        assert!(!eof, "sub{n}: the resume completes in place:\n{r}");
        assert_eq!(
            r.matches("\"c\":1").count(),
            1,
            "sub{n}: the record arrives exactly once on resume:\n{r}"
        );
    }
}
