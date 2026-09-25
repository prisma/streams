//! The SSE session's unit tests.
#![cfg(test)]
use super::*;
use std::time::Duration;

/// A materialized one-segment map whose only live segment has a
/// NONZERO id (a lineage pruned down to a later segment) meets the
/// LiveFeed eligibility condition — and its cursors must name that
/// segment.
#[test]
fn product_cursor_names_the_actual_live_segment() {
    let mut desc = crate::sse::feed::tests::test_desc("segtest").to_persisted();
    desc.segments = Some(crate::segmap::SegmentMap {
        version: 7,
        next_seg_id: 6,
        pending: None,
        segments: vec![crate::segmap::SegmentDesc {
            seg_id: 5,
            lo: 0,
            hi: crate::segmap::KEYSPACE_END,
            shard_prefix: String::new(),
            route_hash: [0u8; 16],
            created_ms: 1,
            predecessors: vec![0],
            successors: Vec::new(),
            sealed_ms: None,
            sealed_next_offset: None,
        }],
    });
    let desc = crate::registry::StreamDesc::try_from(desc).unwrap();
    let key = crate::crypto::StreamKey([9u8; 32]);
    let epoch = [3u8; 16];
    let lane_rk = String::new();
    let ctx = SessionCtx {
        surface: Surface::Product,
        rk_hash: crate::crypto::stream_hash(&lane_rk),
        epoch,
        key: key.clone(),
        desc: desc.clone(),
        raw_cursor: None,
    };
    let seg_id = desc.resolve_segment(&lane_rk).seg_id;
    assert_eq!(seg_id, 5, "resolve must find the nonzero live segment");

    // The emitted bare cursor control carries a KeyCursor naming
    // segment 5, decodable and authenticated.
    let ctl = ctx.record_ctl(
        crate::sse::feed::WirePosition {
            seg_id,
            local_after: 42,
        },
        false,
    );
    let text = String::from_utf8(ctl.to_vec()).unwrap();
    let tok = text
        .split("\"nextCursor\":\"")
        .nth(1)
        .and_then(|rest| rest.split('"').next())
        .expect("product control carries nextCursor");
    let kc =
        crate::product_cursor::KeyCursor::decode(tok, &desc.project_id, &key, &epoch, &ctx.rk_hash)
            .expect("cursor decodes");
    assert_eq!(kc.seg_id, 5, "the cursor names the actual segment");
    assert_eq!(kc.offset, 42);

    // And the standalone status control names it too.
    let status = ctx.status_ctl(
        crate::sse::feed::WirePosition {
            seg_id,
            local_after: 42,
        },
        false,
    );
    let text = String::from_utf8(status.to_vec()).unwrap();
    let tok = text
        .split("\"nextCursor\":\"")
        .nth(1)
        .and_then(|rest| rest.split('"').next())
        .expect("status control carries nextCursor");
    let kc =
        crate::product_cursor::KeyCursor::decode(tok, &desc.project_id, &key, &epoch, &ctx.rk_hash)
            .expect("status cursor decodes");
    assert_eq!(kc.seg_id, 5);
}

/// Finding 7 (identity): the feed identity leg IS the domain-
/// separated storage hash — distinct incarnations never share a
/// feed, and the derivation matches the storage keyspace identity.
#[test]
fn feed_identity_is_the_storage_hash() {
    let desc = crate::sse::feed::tests::test_desc("ident");
    let key = feed_key_of(&desc, &None);
    assert_eq!(key.identity, desc.storage_hash());
    let mut other = crate::sse::feed::tests::test_desc("ident").to_persisted();
    other.stream_epoch = "ffffffffffffffffffffffffffffffff".into();
    let other = crate::registry::StreamDesc::try_from(other).unwrap();
    assert_ne!(
        feed_key_of(&other, &None).identity,
        key.identity,
        "a recreated stream (new epoch) is a new feed identity"
    );
}

/// Finding 10 coverage: bytes_out accounts EXACTLY the emitted
/// frame bytes — one counter increment per body chunk, no more,
/// no less (deterministic: a private Counters, no global state).
#[tokio::test]
async fn bytes_out_accounts_exactly_the_emitted_frames() {
    let usage = std::sync::Arc::new(crate::usage::Counters::default());
    let chunks: Vec<Result<Bytes, std::io::Error>> = vec![
        Ok(Bytes::from_static(b"event: data\ndata:AAEC\n\n")),
        Ok(Bytes::from_static(b"event: control\ndata:{}\n\n")),
        Ok(Bytes::from_static(b": keep-alive\n\n")),
    ];
    let want: usize = chunks.iter().map(|c| c.as_ref().unwrap().len()).sum();
    let resp = response_from_stream(futures_util::stream::iter(chunks), true, &usage);
    assert_eq!(
        resp.headers()
            .get("stream-sse-data-encoding")
            .and_then(|v| v.to_str().ok()),
        Some("base64"),
        "binary bodies carry the encoding header"
    );
    let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    assert_eq!(body.len(), want);
    assert_eq!(
        usage.bytes_out.load(Ordering::Relaxed),
        want as u64,
        "bytes_out IS the emitted frame bytes, exactly"
    );
}

/// Item 33: a failed live read owes exactly ONE bounded park; a park
/// with nothing owed waits the lease nap alone (the retry is not a
/// heartbeat).
#[test]
fn a_failed_live_read_owes_exactly_one_short_park() {
    let hour = Duration::from_secs(3600);
    let mut retry = ReadRetry::IDLE;
    assert_eq!(retry.nap(0, hour), hour, "no failed read, no retry timer");
    retry.failed();
    assert_eq!(
        retry.nap(0, hour),
        Duration::from_millis(250),
        "a failed read shortens the next park"
    );
    assert_eq!(
        retry.nap(0, hour),
        hour,
        "the owed retry belongs to one park"
    );
}

/// Failures without progress back off to the cap, and the lease
/// deadline still bounds every park.
#[test]
fn failures_at_one_cursor_double_to_the_cap() {
    let hour = Duration::from_secs(3600);
    let mut retry = ReadRetry::IDLE;
    let waits: Vec<u128> = (0..7)
        .map(|_| {
            retry.failed();
            retry.nap(5, hour).as_millis()
        })
        .collect();
    assert_eq!(waits, [250, 500, 1000, 2000, 4000, 5000, 5000]);
    retry.failed();
    assert_eq!(
        retry.nap(5, Duration::from_secs(1)),
        Duration::from_secs(1),
        "a retry never postpones the lease deadline"
    );
}

/// Progress resets the backoff: a failure at a later cursor is a new
/// episode.
#[test]
fn a_failure_after_progress_waits_the_first_delay_again() {
    let hour = Duration::from_secs(3600);
    let mut retry = ReadRetry::IDLE;
    retry.failed();
    assert_eq!(retry.nap(5, hour), Duration::from_millis(250));
    retry.failed();
    assert_eq!(retry.nap(5, hour), Duration::from_millis(500));
    retry.failed();
    assert_eq!(
        retry.nap(9, hour),
        Duration::from_millis(250),
        "a failure after progress starts over"
    );
}

/// One raw control frame's JSON fields.
fn raw_control(frame: &Bytes) -> serde_json::Map<String, serde_json::Value> {
    let text = std::str::from_utf8(frame).unwrap();
    let data = text
        .strip_prefix("event: control\ndata:")
        .and_then(|rest| rest.strip_suffix("\n\n"))
        .unwrap_or_else(|| panic!("not one control frame: {text:?}"));
    serde_json::from_str(data).unwrap()
}

/// The (segment, resume offset) a raw control's `streamNextOffset` names.
fn raw_resume(control: &serde_json::Map<String, serde_json::Value>) -> (u32, u64) {
    let token = control["streamNextOffset"].as_str().unwrap();
    crate::offsets::parse(token).unwrap()
}

/// The RAW surface's controls name the segment and the offset a reader
/// resumes at (the start before any record), carry a stream cursor beyond
/// a presented numeric cursor while the stream is open, fold `upToDate`
/// into the record control at the head and into the status control, and
/// name `streamClosed` instead of a cursor once the stream is closed.
#[test]
fn raw_controls_name_the_resume_position_cursor_and_flags() {
    let lane_rk = String::new();
    let ctx = SessionCtx {
        surface: Surface::RawToken,
        rk_hash: crate::crypto::stream_hash(&lane_rk),
        epoch: [3; 16],
        key: crate::crypto::StreamKey([9; 32]),
        desc: crate::sse::feed::tests::test_desc("rawctl"),
        raw_cursor: Some("99999999999".into()),
    };
    let at = |seg_id, local_after| crate::sse::feed::WirePosition {
        seg_id,
        local_after,
    };
    let first = raw_control(&ctx.record_ctl(at(0, 0), false));
    assert_eq!(raw_resume(&first), (0, 0));
    assert_eq!(first["streamCursor"], "100000000000");
    assert!(!first.contains_key("upToDate") && !first.contains_key("streamClosed"));

    let head = raw_control(&ctx.record_ctl(at(2, 42), true));
    assert_eq!(raw_resume(&head), (2, 42));
    assert_eq!(head["streamCursor"], "100000000000");
    assert_eq!(head["upToDate"], true);

    let open = raw_control(&ctx.status_ctl(at(2, 42), false));
    assert_eq!(open, head, "an open stream's status is the head control");

    let closed = raw_control(&ctx.status_ctl(at(2, 43), true));
    assert_eq!(raw_resume(&closed), (2, 43));
    assert_eq!(closed["upToDate"], true);
    assert_eq!(closed["streamClosed"], true);
    assert!(
        !closed.contains_key("streamCursor"),
        "a closed stream names no cursor: {closed:?}"
    );
}

/// Item 86: a catch-up read that advanced nothing is answered at one
/// owner. A fatal cutoff ends the session at once; a failed read and an
/// empty page each wait the bounded retry before the next read.
#[tokio::test(start_paused = true)]
async fn a_stalled_catch_up_read_owes_its_pass_one_verdict() {
    let empty = crate::sse::feed::SourceBatch {
        scan_from: 4,
        scan_to: 4,
        records: crate::application::read::PlainBatch::default(),
        completed: false,
    };
    use crate::sse::feed::{SourceCutoff, SourceReadError};
    for (leg, read, owed, waited) in [
        (
            "cutoff",
            Err(SourceReadError::Fatal(SourceCutoff::TargetMismatch)),
            Stall::Cutoff,
            0,
        ),
        (
            "failed",
            Err(SourceReadError::Retryable(anyhow::anyhow!("injected"))),
            Stall::Failed,
            100,
        ),
        ("empty", Ok(empty), Stall::NoProgress, 100),
    ] {
        let start = tokio::time::Instant::now();
        assert_eq!(catch_up::stalled(read).await, owed, "{leg}");
        assert_eq!(
            start.elapsed(),
            Duration::from_millis(waited),
            "{leg}: the wait owed before the next read"
        );
    }
}

/// Item 87 (red on eb742c42: the retried cause was dropped): a failed
/// catch-up read logs its cause before the bounded wait.
#[tokio::test(start_paused = true)]
async fn a_failed_catch_up_read_logs_its_cause() {
    let log = crate::sse::test_log::ErrorLog::capture();
    let failed = crate::sse::feed::SourceReadError::Retryable(anyhow::anyhow!("injected"));
    assert_eq!(catch_up::stalled(Err(failed)).await, Stall::Failed);
    assert_eq!(
        log.causes(),
        ["injected"],
        "the retried catch-up read names its cause"
    );
}
