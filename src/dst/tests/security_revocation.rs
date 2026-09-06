//! Security revocation.

use super::fixture_auth::{
    auth_rig, mint_token, rig_append, rig_create, rig_policy, rig_publish_grants,
    rig_publish_policy, rig_sse,
};
use super::fixture_failpoints::gap_lock;
use super::fixture_livefeed::{hub_rig_stream, hub_sse_collect, read_billing_sum, wait_parked};

/// Backlog appends under a burst can hit typed admission shed (429);
/// retry briefly — the legs are about delivery, not admission.
async fn rig_append_retry(addr: std::net::SocketAddr, name: &str, bearer: &str, body: &str) {
    for _ in 0..50 {
        match rig_append(addr, name, bearer, body).await {
            200 => return,
            429 | 503 => tokio::time::sleep(std::time::Duration::from_millis(40)).await,
            st => panic!("append {name}: {st}"),
        }
    }
    panic!("append {name}: admission never cleared");
}

fn data_frames(s: &str) -> usize {
    s.matches("event: data").count()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn subscription_terminates_when_only_the_grant_feed_goes_stale() {
    let (svc, _state, addr) = auth_rig("proj-st2", "ws_st", &["c1"], Some(3)).await;
    let tok = mint_token("c1", "proj-st2", "ws_st", 1, 1, "s2", 600);
    rig_create(addr, "st2", &tok).await;
    let mut sub = rig_sse(addr, "st2", &tok, "", None).await;
    let (b, _) = hub_sse_collect(&mut sub, 8, |t| t.contains("upToDate")).await;
    assert!(b.contains("upToDate"), "parks:\n{b}");
    // Keep the POLICY feed fresh; let GRANTS age past the window.
    tokio::time::sleep(std::time::Duration::from_millis(1500)).await;
    rig_publish_policy(&svc, rig_policy("proj-st2", "ws_st", 1, 1), 2).unwrap();
    let (_, eof) = hub_sse_collect(&mut sub, 14, |_| false).await;
    assert!(
        eof,
        "stale GRANT feed alone must terminate the subscription"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn subscription_survives_when_both_feeds_refresh_before_the_deadline() {
    let (svc, _state, addr) = auth_rig("proj-st3", "ws_st", &["c1"], Some(3)).await;
    let tok = mint_token("c1", "proj-st3", "ws_st", 1, 1, "s3", 600);
    rig_create(addr, "st3", &tok).await;
    let mut sub = rig_sse(addr, "st3", &tok, "", None).await;
    let (b, _) = hub_sse_collect(&mut sub, 8, |t| t.contains("upToDate")).await;
    assert!(b.contains("upToDate"), "parks:\n{b}");
    // Identical-content republication every 1.5 s moves the freshness
    // deadline forward: the subscription must stay open well past the
    // original 3 s window.
    for fv in 2..6 {
        tokio::time::sleep(std::time::Duration::from_millis(1500)).await;
        rig_publish_policy(&svc, rig_policy("proj-st3", "ws_st", 1, 1), fv).unwrap();
        rig_publish_grants(
            &svc,
            "proj-st3",
            &[("c1", crate::project_policy::CredentialStatus::Active, 1)],
            fv,
        )
        .unwrap();
    }
    let (_, eof) = hub_sse_collect(&mut sub, 2, |_| false).await;
    assert!(!eof, "refreshed feeds must keep the subscription open");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn off_mode_subscriptions_are_untouched_by_staleness() {
    // No auth service: Off posture, no lease. Parked hub subscriber
    // stays open indefinitely regardless of any feed notion.
    let (state, _addr, _promoter, mut sck) = hub_rig_stream("offst").await;
    let (a, _) = hub_sse_collect(&mut sck, 8, |t| t.contains("upToDate")).await;
    assert!(a.contains("upToDate"));
    assert!(state.livefeed.registry().len_for_test() >= 1);
    let (_, eof) = hub_sse_collect(&mut sck, 5, |_| false).await;
    assert!(!eof, "Off mode: no lease, no staleness termination");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn revocation_interrupts_active_direct_delivery() {
    // Same deterministic interleaving as the hub leg, on the DIRECT
    // producer path.
    let (svc, _state, addr) = auth_rig("proj-rv2", "ws_rv", &["c1", "c2"], None).await;
    let tok1 = mint_token("c1", "proj-rv2", "ws_rv", 1, 1, "d1", 600);
    let tok2 = mint_token("c2", "proj-rv2", "ws_rv", 1, 1, "d2", 600);
    rig_create(addr, "rv2", &tok1).await;
    for i in 0..4 {
        rig_append_retry(addr, "rv2", &tok1, &format!(r#"{{"i":{i}}}"#)).await;
    }
    // Single subscriber from the start = the DIRECT path.
    let mut sub = rig_sse(addr, "rv2", &tok1, "", None).await;
    let (acc, _) = hub_sse_collect(&mut sub, 8, |t| t.contains("upToDate")).await;
    assert!(data_frames(&acc) >= 2, "catch-up delivered:\n{acc}");

    // Producer provably parked at the tail (upToDate emitted): arm,
    // then force the next send through the failpoint with a trigger
    // append (same discipline as the hub leg).
    crate::failpoints::arm(crate::failpoints::Fp::SseBeforeSend, "rv2");
    assert_eq!(
        rig_append(addr, "rv2", &tok1, r#"{"TRIGGER":0}"#).await,
        200
    );
    wait_parked(crate::failpoints::Fp::SseBeforeSend, "rv2", 1).await;
    let bill_before = read_billing_sum(&_state);

    rig_publish_grants(
        &svc,
        "proj-rv2",
        &[
            ("c1", crate::project_policy::CredentialStatus::Revoked, 2),
            ("c2", crate::project_policy::CredentialStatus::Active, 1),
        ],
        2,
    )
    .unwrap();
    for m in 0..3 {
        assert_eq!(
            rig_append(addr, "rv2", &tok2, &format!(r#"{{"MARKER":{m}}}"#)).await,
            200
        );
    }

    crate::failpoints::release(crate::failpoints::Fp::SseBeforeSend, "rv2");
    let (after, eof) = hub_sse_collect(&mut sub, 15, |_| false).await;
    assert!(eof, "revoked direct subscription must terminate");
    assert_eq!(
        data_frames(&after),
        0,
        "ZERO post-cutoff frames may be yielded:\n{after}"
    );
    assert!(
        !after.contains("MARKER") && !after.contains("TRIGGER"),
        "post-cutoff records reached a revoked direct subscriber:\n{after}"
    );
    // Round-9 metering boundary: the discarded post-cutoff frames must
    // not have been billed either (metering rides the yield).
    assert_eq!(
        read_billing_sum(&_state),
        bill_before,
        "discarded post-cutoff frames were billed"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn token_expiry_interrupts_slowly_progressing_delivery() {
    // LEASE_TERMINATIONS is process-global; serialize with the other
    // expiry tests that move it (gap_lock convention).
    let _serial = gap_lock().lock().await;
    let (_svc, _state, addr) = auth_rig("proj-ex1", "ws_ex", &["c1"], None).await;
    // Setup (create, backlog, trigger) rides a LONG-lived token; only
    // the SUBSCRIPTION's lease is short (5 s) — the timing-sensitive
    // window is just connect + catch-up, not the whole rig setup on a
    // loaded runner. The producer is then parked at the send
    // failpoint PAST the lease's expiry.
    let tok_setup = mint_token("c1", "proj-ex1", "ws_ex", 1, 1, "e0", 600);
    rig_create(addr, "ex1", &tok_setup).await;
    for i in 0..4 {
        rig_append_retry(addr, "ex1", &tok_setup, &format!(r#"{{"i":{i}}}"#)).await;
    }
    let tok = mint_token("c1", "proj-ex1", "ws_ex", 1, 1, "e1", 5);
    let mut sub = rig_sse(addr, "ex1", &tok, "", None).await;
    let (acc, _) = hub_sse_collect(&mut sub, 8, |t| t.contains("upToDate")).await;
    assert!(data_frames(&acc) >= 2, "catch-up delivered:\n{acc}");

    // Producer provably parked at the tail: arm, force the next send
    // into the failpoint, and let the lease expire while it is held.
    crate::failpoints::arm(crate::failpoints::Fp::SseBeforeSend, "ex1");
    assert_eq!(
        rig_append(addr, "ex1", &tok_setup, r#"{"TRIGGER":0}"#).await,
        200
    );
    wait_parked(crate::failpoints::Fp::SseBeforeSend, "ex1", 1).await;
    tokio::time::sleep(std::time::Duration::from_secs(6)).await;

    // Resume PAST expiry: the body gate refuses every queued frame —
    // the client sees NOTHING further, then the chunked terminator.
    crate::failpoints::release(crate::failpoints::Fp::SseBeforeSend, "ex1");
    let (after, eof) = hub_sse_collect(&mut sub, 12, |_| false).await;
    assert!(eof, "expired token must end a slowly progressing delivery");
    assert_eq!(
        data_frames(&after),
        0,
        "ZERO post-expiry frames may be yielded:\n{after}"
    );
}

/// Round-9 review metering boundary: a frame QUEUED IN THE CHANNEL
/// across an authorization cutoff is discarded unyielded AND
/// unbilled. The body is parked BEFORE its next dequeue
/// (SseBodyBeforeYield), the producer enqueues the trigger frame, the
/// credential is revoked, the body resumes: EOF with zero data frames
/// and a ZERO read-billing delta — billing rides the authoritative
/// yield, never the enqueue.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn queued_frame_is_discarded_and_unbilled_at_revocation() {
    let (svc, state, addr) = auth_rig("proj-rvq", "ws_rv", &["c1", "c2"], None).await;
    let tok1 = mint_token("c1", "proj-rvq", "ws_rv", 1, 1, "q1", 600);
    let tok2 = mint_token("c2", "proj-rvq", "ws_rv", 1, 1, "q2", 600);
    rig_create(addr, "rvq", &tok1).await;
    rig_append_retry(addr, "rvq", &tok1, r#"{"i":0}"#).await;
    let mut sub = rig_sse(addr, "rvq", &tok1, "", None).await;
    let (acc, _) = hub_sse_collect(&mut sub, 8, |t| t.contains("upToDate")).await;
    assert!(data_frames(&acc) >= 1, "catch-up delivered:\n{acc}");

    // Every pre-cutoff yield is already billed (metering is
    // synchronous with the yield the client observed). The snapshot
    // sits BEFORE the trigger append: enqueue-time billing of the
    // never-yielded trigger frame (the pre-round-9 boundary) must land
    // INSIDE the measured window, not inside the baseline.
    let before = read_billing_sum(&state);
    // Park the BODY before its next dequeue, then land one frame: the
    // trigger sits IN the channel, undelivered, across the cutoff.
    crate::failpoints::arm(crate::failpoints::Fp::SseBodyBeforeYield, "rvq");
    assert_eq!(
        rig_append(addr, "rvq", &tok2, r#"{"TRIGGER":0}"#).await,
        200
    );
    wait_parked(crate::failpoints::Fp::SseBodyBeforeYield, "rvq", 1).await;

    rig_publish_grants(
        &svc,
        "proj-rvq",
        &[
            ("c1", crate::project_policy::CredentialStatus::Revoked, 2),
            ("c2", crate::project_policy::CredentialStatus::Active, 1),
        ],
        2,
    )
    .unwrap();
    crate::failpoints::release(crate::failpoints::Fp::SseBodyBeforeYield, "rvq");

    let (after, eof) = hub_sse_collect(&mut sub, 15, |_| false).await;
    assert!(eof, "revoked subscription must terminate");
    assert_eq!(
        data_frames(&after),
        0,
        "the queued frame must not be yielded:\n{after}"
    );
    assert!(
        !after.contains("TRIGGER"),
        "the queued record reached a revoked subscriber:\n{after}"
    );
    assert_eq!(
        read_billing_sum(&state),
        before,
        "a discarded queued frame must never be billed"
    );
}

/// Round-8 review blocker 1, SHARED-feed leg: two LiveFeed
/// subscribers on ONE shared feed, both parked at the send failpoint
/// with the trigger frame in hand; ONE credential is revoked. The
/// revoked subscriber yields ZERO post-cutoff frames and terminates;
/// the survivor receives every record exactly once and stays open —
/// the body gate is PER SUBSCRIBER, not per feed.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn livefeed_shared_feed_revocation_gates_one_subscriber_only() {
    let (svc, state, addr) = auth_rig("proj-rv3", "ws_rv", &["c1", "c2"], None).await;
    let tok1 = mint_token("c1", "proj-rv3", "ws_rv", 1, 1, "s1", 600);
    let tok2 = mint_token("c2", "proj-rv3", "ws_rv", 1, 1, "s2", 600);
    rig_create(addr, "rv3", &tok1).await;
    for i in 0..2 {
        rig_append_retry(addr, "rv3", &tok1, &format!(r#"{{"i":{i}}}"#)).await;
    }
    let mut sub_a = rig_sse(addr, "rv3", &tok1, "", None).await;
    let (acc_a, _) = hub_sse_collect(&mut sub_a, 8, |t| t.contains("upToDate")).await;
    assert!(data_frames(&acc_a) >= 2, "A catch-up delivered:\n{acc_a}");
    let mut sub_b = rig_sse(addr, "rv3", &tok2, "", None).await;
    let (acc_b, _) = hub_sse_collect(&mut sub_b, 8, |t| t.contains("upToDate")).await;
    assert!(data_frames(&acc_b) >= 2, "B catch-up delivered:\n{acc_b}");

    // Both PROVABLY parked at the tail: arm, then force BOTH sessions'
    // next send into the failpoint with one trigger append.
    crate::failpoints::arm(crate::failpoints::Fp::SseBeforeSend, "rv3");
    assert_eq!(
        rig_append(addr, "rv3", &tok2, r#"{"TRIGGER":0}"#).await,
        200
    );
    wait_parked(crate::failpoints::Fp::SseBeforeSend, "rv3", 2).await;
    let bill_before = read_billing_sum(&state);

    // Revoke c1 ONLY, land post-cutoff markers with c2, resume.
    rig_publish_grants(
        &svc,
        "proj-rv3",
        &[
            ("c1", crate::project_policy::CredentialStatus::Revoked, 2),
            ("c2", crate::project_policy::CredentialStatus::Active, 1),
        ],
        2,
    )
    .unwrap();
    for m in 0..2 {
        assert_eq!(
            rig_append(addr, "rv3", &tok2, &format!(r#"{{"MARKER":{m}}}"#)).await,
            200
        );
    }
    crate::failpoints::release(crate::failpoints::Fp::SseBeforeSend, "rv3");

    // A (revoked): zero frames, EOF.
    let (after_a, eof_a) = hub_sse_collect(&mut sub_a, 15, |_| false).await;
    assert!(eof_a, "revoked shared subscriber must terminate");
    assert_eq!(
        data_frames(&after_a),
        0,
        "ZERO post-cutoff frames on the revoked subscriber:\n{after_a}"
    );
    assert!(
        !after_a.contains("TRIGGER") && !after_a.contains("MARKER"),
        "post-cutoff records reached the revoked subscriber:\n{after_a}"
    );
    // B (active): trigger + both markers exactly once, connection open.
    let (after_b, eof_b) = hub_sse_collect(&mut sub_b, 10, |t| t.contains("\"MARKER\":1")).await;
    assert!(!eof_b, "the survivor must stay open:\n{after_b}");
    for needle in ["\"TRIGGER\":0", "\"MARKER\":0", "\"MARKER\":1"] {
        assert_eq!(
            after_b.matches(needle).count(),
            1,
            "survivor receives {needle} exactly once:\n{after_b}"
        );
    }
    // Round-9 metering boundary, exactly-once billing on a SHARED
    // feed: the survivor's three yielded records bill exactly three
    // read records; the revoked subscriber's discarded copies bill
    // ZERO.
    let (_, recs_after) = read_billing_sum(&state);
    let (_, recs_before) = bill_before;
    assert_eq!(
        recs_after - recs_before,
        3,
        "the shared cutoff must bill exactly the survivor's three records"
    );
}

/// Round-8 review blocker 1, MID-SUBSCRIPTION SOURCE-SWAP leg: the
/// subscriber rides a split in place (source generation +1), parks at
/// the send failpoint with a successor-segment frame in hand, and is
/// revoked. Zero post-cutoff frames, no fabricated terminal, EOF —
/// the swapped source must not carry an outdated authorization
/// binding past the cutoff.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn livefeed_source_swap_revocation_yields_zero_post_cutoff_frames() {
    let (svc, state, addr) = auth_rig("proj-rv4", "ws_rv", &["c1", "c2"], None).await;
    let tok1 = mint_token("c1", "proj-rv4", "ws_rv", 1, 1, "w1", 600);
    let tok2 = mint_token("c2", "proj-rv4", "ws_rv", 1, 1, "w2", 600);
    rig_create(addr, "rv4", &tok1).await;
    for i in 0..2 {
        rig_append_retry(addr, "rv4", &tok1, &format!(r#"{{"i":{i}}}"#)).await;
    }
    let mut sub = rig_sse(addr, "rv4", &tok1, "", None).await;
    let (acc, _) = hub_sse_collect(&mut sub, 8, |t| t.contains("upToDate")).await;
    assert!(data_frames(&acc) >= 2, "catch-up delivered:\n{acc}");

    // Split while parked: the session continues in place on the
    // successor source (mid-subscription swap). The stream lives under
    // the TOKEN's project, so the split targets the project-scoped
    // sref (split_and_await's raw-adapter sref names a different
    // tenant).
    let sref = crate::tenant::ProjectId::new("proj-rv4")
        .unwrap()
        .stream_ref("rv4");
    let _ = crate::scaler3::execute_split(&state, &sref, 0, 0x8000_0000_0000_0000).await;
    for _ in 0..200 {
        state.registry.invalidate(&sref);
        let d = state.registry.get(&sref).await.unwrap().unwrap();
        if d.segments
            .as_ref()
            .is_some_and(|m| m.pending.is_none() && m.segments.len() > 1)
        {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    }
    // Park the next send: the trigger lands on the SUCCESSOR segment.
    crate::failpoints::arm(crate::failpoints::Fp::SseBeforeSend, "rv4");
    assert_eq!(
        rig_append(addr, "rv4", &tok2, r#"{"TRIGGER":0}"#).await,
        200
    );
    wait_parked(crate::failpoints::Fp::SseBeforeSend, "rv4", 1).await;

    rig_publish_grants(
        &svc,
        "proj-rv4",
        &[
            ("c1", crate::project_policy::CredentialStatus::Revoked, 2),
            ("c2", crate::project_policy::CredentialStatus::Active, 1),
        ],
        2,
    )
    .unwrap();
    assert_eq!(rig_append(addr, "rv4", &tok2, r#"{"MARKER":0}"#).await, 200);
    crate::failpoints::release(crate::failpoints::Fp::SseBeforeSend, "rv4");

    let (after, eof) = hub_sse_collect(&mut sub, 15, |_| false).await;
    assert!(eof, "revoked post-swap subscription must terminate");
    assert_eq!(
        data_frames(&after),
        0,
        "ZERO post-cutoff frames after the source swap:\n{after}"
    );
    assert!(
        !after.contains("TRIGGER") && !after.contains("MARKER"),
        "post-cutoff successor records reached a revoked subscriber:\n{after}"
    );
    assert!(
        !after.contains("\"sealed\":true"),
        "a cutoff must never fabricate a terminal control:\n{after}"
    );
}
