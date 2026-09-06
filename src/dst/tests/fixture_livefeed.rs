//! Fixture livefeed.

use super::fixture_http::http_rig;
use super::fixture_requests::{PRISMA_KEY, preq};
use super::fixture_storage::mem;
use std::sync::Arc;

// ---- #270 hub terminal state machine (Søren review F1+F4) ----------
// Red-first against 6843b8de: the pump marked a genuinely CLOSED hub
// dead in the same synchronous cleanup, so notified subscribers woke
// into Dead and dropped the final batch and sealed control; catch-up
// never conveyed upToDate.

/// Drive one hub SSE subscriber over raw TCP and collect frames until
/// `done(acc)` or body end or deadline. Returns (accumulated text,
/// body_ended). Body end = the chunked terminal ("0\r\n\r\n") — an
/// HTTP/1 keep-alive server ends the BODY without closing the TCP
/// socket — or a raw EOF.
pub(super) async fn hub_sse_collect(
    sck: &mut tokio::net::TcpStream,
    deadline_secs: u64,
    done: impl Fn(&str) -> bool,
) -> (String, bool) {
    use tokio::io::AsyncReadExt;
    let mut buf = vec![0u8; 8192];
    let mut acc: Vec<u8> = Vec::new();
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(deadline_secs);
    while std::time::Instant::now() < deadline {
        let text = String::from_utf8_lossy(&acc).to_string();
        if text.ends_with("0\r\n\r\n") {
            return (text, true);
        }
        if done(&text) {
            return (text, false);
        }
        let n = match tokio::time::timeout(std::time::Duration::from_secs(1), sck.read(&mut buf))
            .await
        {
            Ok(r) => r.expect("sse read"),
            Err(_) => continue,
        };
        if n == 0 {
            return (String::from_utf8_lossy(&acc).to_string(), true);
        }
        acc.extend_from_slice(&buf[..n]);
    }
    (String::from_utf8_lossy(&acc).to_string(), false)
}

pub(super) async fn hub_sse_connect(
    addr: std::net::SocketAddr,
    name: &str,
) -> tokio::net::TcpStream {
    use tokio::io::AsyncWriteExt;
    let mut sck = tokio::net::TcpStream::connect(addr).await.unwrap();
    let req = format!(
        "GET /v1/streams/{name}/records:sse HTTP/1.1\r\nhost: x\r\ncontent-length: 0\r\nprisma-encryption-key: {PRISMA_KEY}\r\n\r\n"
    );
    sck.write_all(req.as_bytes()).await.unwrap();
    sck
}

/// Boot a rig, create the stream, and return a HUB subscriber. Post
/// #274/F8 the FIRST subscriber rides the direct path, so a PROMOTER
/// connection is opened (kept alive via the returned socket) and the
/// returned test subscriber is the SECOND — the one on the hub.
pub(super) async fn hub_rig_stream(
    name: &str,
) -> (
    Arc<crate::http::AppState>,
    std::net::SocketAddr,
    tokio::net::TcpStream,
    tokio::net::TcpStream,
) {
    let store = mem();
    let (state, addr) = http_rig(store).await;
    let (st, _, _) = preq(
        addr,
        "PUT",
        &format!("/v1/streams/{name}"),
        &[("prisma-encryption-key", PRISMA_KEY)],
        br#"{"format":{"kind":"json"}}"#,
    )
    .await;
    assert_eq!(st, 201);
    let mut promoter = hub_sse_connect(addr, name).await;
    let (acc, _) = hub_sse_collect(&mut promoter, 8, |t| t.contains("upToDate")).await;
    assert!(acc.contains("upToDate"), "promoter joins direct:\n{acc}");
    let sck = hub_sse_connect(addr, name).await;
    (state, addr, promoter, sck)
}

/// Rig-local read-billing sum (payload bytes, records) across every
/// identity in the ACTIVE accumulator — the yield-boundary metering
/// assertions compare snapshots of this. (The process-global
/// DELIVERED_RECORDS gauge is shared across parallel tests and cannot
/// carry a delta assertion; the accumulator is per rig.)
pub(super) fn read_billing_sum(state: &Arc<crate::http::AppState>) -> (u64, u64) {
    state
        .billing
        .reads()
        .snapshot_active()
        .iter()
        .fold((0, 0), |(b, r), (_, d)| {
            (b + d.read_payload_bytes, r + d.read_records)
        })
}

/// Read one HTTP response head. Returns (status, head text).
pub(super) async fn sse_head(sck: &mut tokio::net::TcpStream) -> (u16, String) {
    use tokio::io::AsyncReadExt;
    // ONE byte at a time: the SSE producer may coalesce the first body
    // chunk with the response headers (LiveFeed emits the head status
    // immediately), and a wider read would silently EAT that chunk —
    // the following body collect then never sees it (engine-matrix
    // flake `raw_sse_terminates_at_workload_token_expiry`).
    let mut buf = vec![0u8; 1];
    let mut acc: Vec<u8> = Vec::new();
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    loop {
        if let Some(pos) = find_head_end(&acc) {
            return (
                String::from_utf8_lossy(&acc[..pos])
                    .lines()
                    .next()
                    .and_then(|l| l.split_whitespace().nth(1))
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(0),
                String::from_utf8_lossy(&acc).to_string(),
            );
        }
        assert!(
            std::time::Instant::now() < deadline,
            "no response head: {}",
            String::from_utf8_lossy(&acc)
        );
        let n = tokio::time::timeout(std::time::Duration::from_secs(5), sck.read(&mut buf))
            .await
            .expect("head read timed out")
            .expect("head read");
        assert!(n > 0, "connection closed before a response head");
        acc.extend_from_slice(&buf[..n]);
    }
}

pub(super) fn find_head_end(b: &[u8]) -> Option<usize> {
    b.windows(4).position(|w| w == b"\r\n\r\n").map(|p| p + 4)
}

// ==================================================================
// LIVE-FEED Stage 0: golden wire-contract equivalence. The direct
// producer and the hub producer must produce IDENTICAL SSE transcripts
// for the same append sequence (modulo opaque cursor tokens): status
// control first, data+cursor pairing, exactly ONE sealed control, EOF
// after it. This corpus arbitrates the LiveFeed cutover.
// ==================================================================

// ==================================================================
// LIVE-FEED Stage 3 equivalence legs: the same wire contract the
// golden corpus pins on the legacy paths, asserted against the
// LiveFeed engine (THE engine since round 11.8).
// ==================================================================

pub(super) async fn lf_connect(
    addr: std::net::SocketAddr,
    name: &str,
    query: &str,
) -> tokio::net::TcpStream {
    use tokio::io::AsyncWriteExt;
    let mut sck = tokio::net::TcpStream::connect(addr).await.unwrap();
    let req = format!(
        "GET /v1/streams/{name}/records:sse{query} HTTP/1.1\r\nhost: x\r\ncontent-length: 0\r\nprisma-encryption-key: {PRISMA_KEY}\r\n\r\n"
    );
    sck.write_all(req.as_bytes()).await.unwrap();
    sck
}

/// Wait until at least `n` tasks are parked at a failpoint (test sync).
pub(super) async fn wait_parked(fp: crate::failpoints::Fp, name: &str, n: usize) {
    for _ in 0..200 {
        if crate::failpoints::parked(fp, name) >= n {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    }
    panic!("timed out waiting for {n} parked at {name}");
}

/// Execute a split and await the COMPLETED topology (pending cleared,
/// successors published). The return value is deliberately not
/// asserted: a livefeed session observing the pending transition
/// spawns the same resumable resume and may legitimately win the
/// completion race (the split is idempotent either way).
pub(super) async fn split_and_await(state: &Arc<crate::http::AppState>, name: &str, seg_id: u32) {
    let sref = state.deployment.raw_adapter_sref(name);
    let _ = crate::scaler3::execute_split(state, &sref, seg_id, 0x8000_0000_0000_0000).await;
    for _ in 0..200 {
        state.registry.invalidate(&sref);
        let d = state.registry.get(&sref).await.unwrap().unwrap();
        let done = d
            .segments
            .as_ref()
            .is_some_and(|m| m.pending.is_none() && m.segments.len() > 1);
        if done {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    }
    panic!("split of {name} (seg {seg_id}) did not complete");
}

/// Seal with the CLIENT contract: retryable refusals (transient
/// engine open under runner load, a resumable interrupted seal) retry
/// with bounded backoff — the round-10 matrix run answered a first
/// :seal with a 503-class refusal. Permanent verdicts fail fast.
pub(super) async fn seal_ok(addr: std::net::SocketAddr, name: &str) {
    for attempt in 0..8u32 {
        let (st, _, body) = preq(
            addr,
            "POST",
            &format!("/v1/streams/{name}:seal"),
            &[("prisma-encryption-key", PRISMA_KEY)],
            b"{}",
        )
        .await;
        if st == 200 {
            return;
        }
        // Round-10e review: ONLY the typed resumable refusal retries.
        // A 500 internal (invariant/corruption/store failure) must
        // fail the test immediately — a retry that happened to
        // succeed would hide a real regression behind a green run.
        let text = String::from_utf8_lossy(&body).to_string();
        assert!(
            st == 503 && text.contains("seal_incomplete"),
            "seal {name}: non-retryable {st}: {text}"
        );
        tokio::time::sleep(std::time::Duration::from_millis(100 * (attempt as u64 + 1))).await;
    }
    panic!("seal {name}: still refused after bounded retries");
}

pub(super) async fn hub_append_lf(addr: std::net::SocketAddr, name: &str, body: &str) {
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

// ==================================================================
// LIVE-FEED Stage 6 round-4 legs: wire-position correctness
// (decode-and-resume), shared-subscriber swap, and the deterministic
// seal-to-publication handoff.
// ==================================================================

/// Extract the LAST product nextCursor token from a transcript.
pub(super) fn last_next_cursor(t: &str) -> String {
    let at = t.rfind("\"nextCursor\":\"").expect("a product cursor");
    let rest = &t[at + "\"nextCursor\":\"".len()..];
    rest.split('"').next().expect("cursor value").to_string()
}

/// LiveFeed collect stop: the successor record arrived AND the
/// standalone status control after it. LiveFeed record controls are
/// BARE (no flags) — a stop condition counting per-record `upToDate`
/// flags matches the LEGACY hub shape only and burns its entire
/// deadline on the LiveFeed engine (30 s of idle collect per
/// subscriber is exactly where loaded CI runners breed flakes).
pub(super) fn lf_record_and_status(t: &str, needle: &str) -> bool {
    t.find(needle)
        .is_some_and(|at| t[at..].contains("\"upToDate\":true"))
}
