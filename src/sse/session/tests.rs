//! The SSE session's unit tests.
#![cfg(test)]
use super::*;

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
