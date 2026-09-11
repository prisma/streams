//! Product transport tests: wire-pinned error messages, query parsing
//! and response rendering.
#![cfg(test)]
use super::*;

/// WP-03/PR 5: the wire error MESSAGES are pinned across the
/// single-sourcing — same code, same body text per case, including
/// the multi-violation precedence corner (`__ds/..` reports the
/// dot segment — since PR 4.1 that order is owned by the canonical
/// layer, not re-scanned here).
#[test]
fn name_error_messages_are_wire_pinned() {
    let msg = |raw: &str| match ProductStreamName::try_from(raw) {
        Ok(_) => panic!("{raw:?} must be rejected"),
        Err(e) => e.message(),
    };
    assert_eq!(msg(""), "stream name must be 1-512 UTF-8 bytes");
    assert_eq!(
        msg(&"x".repeat(513)),
        "stream name must be 1-512 UTF-8 bytes"
    );
    assert_eq!(msg("has\u{7}bell"), "control characters are not allowed");
    assert_eq!(msg("a//b"), "empty path segments are not allowed");
    assert_eq!(msg("a/./b"), "'.' and '..' segments are not allowed");
    assert_eq!(msg("__ds/x"), "the __ds namespace is reserved");
    assert_eq!(
        msg("a/records"),
        "'records', 'consumers' and 'watches' are reserved subresource names"
    );
    assert_eq!(
        msg("a/consumers/b"),
        "this name is already a subresource path (…/records, …/consumers/{name}, …/watches/…)"
    );
    // The precedence corner: reserved root AND a dot segment —
    // the segment message wins, as the old scan order dictated.
    assert_eq!(msg("__ds/.."), "'.' and '..' segments are not allowed");
    assert_eq!(msg("__ds//x"), "empty path segments are not allowed");
}

#[test]
fn name_rules() {
    assert!(canonical_name("orders").is_ok());
    assert!(canonical_name("customers/acme/orders").is_ok());
    assert!(canonical_name("").is_err());
    assert!(canonical_name(&"x".repeat(513)).is_err());
    assert!(canonical_name("a//b").is_err());
    assert!(canonical_name("a/./b").is_err());
    assert!(canonical_name("a/../b").is_err());
    assert!(canonical_name("__ds/x").is_err());
    assert!(canonical_name("__ds").is_err());
    assert!(canonical_name("a/records").is_err());
    assert!(canonical_name("a/consumers").is_err());
    assert!(canonical_name("a/watches").is_err());
    assert!(canonical_name("has\u{7}bell").is_err());
}

#[test]
fn validators_agree() {
    // Pins product::canonical_name to tenant::CanonicalStreamName:
    // every ACCEPT of canonical_name must be an ACCEPT of the
    // identity type (canonical_name may be STRICTER — it adds the
    // addressability rules — never looser). Divergence here means
    // an identity-layer bypass.
    let corpus = [
        "orders",
        "customers/acme/orders",
        "a",
        "a/b/c",
        "a/__ds",
        "records-ish",
        "a/recordsx",
        "deep/a/b/c/d/e",
        "",
        "a//b",
        "/a",
        "a/",
        "a/./b",
        "a/../b",
        ".",
        "..",
        "__ds",
        "__ds/x",
        "has\u{7}bell",
        "a/records",
        "a/consumers",
        "a/watches",
    ];
    for raw in corpus {
        let product_ok = canonical_name(raw).is_ok();
        let tenant_ok = crate::tenant::CanonicalStreamName::new(raw).is_ok();
        assert!(
            !product_ok || tenant_ok,
            "canonical_name accepted {raw:?} but CanonicalStreamName rejected it"
        );
    }
    let long = "x".repeat(513);
    assert!(canonical_name(&long).is_err());
    assert!(crate::tenant::CanonicalStreamName::new(&long).is_err());
    // The typed entry point returns the same acceptance set as
    // canonical_name itself.
    assert!(canonical_stream_name("customers/acme").is_ok());
    assert!(canonical_stream_name("a/records").is_err());
}

#[test]
fn subresource_split() {
    assert_eq!(
        split_subresource("customers/acme/orders/records"),
        Some(("customers/acme/orders", "records"))
    );
    assert_eq!(
        split_subresource("orders/consumers/fulfilment"),
        Some(("orders", "consumers/fulfilment"))
    );
    assert_eq!(split_subresource("orders"), None);
}

#[test]
fn idle_durations() {
    assert_eq!(parse_idle_secs("30d"), Some(30 * 86_400));
    assert_eq!(parse_idle_secs("12h"), Some(12 * 3_600));
    assert_eq!(parse_idle_secs("90"), Some(90));
    assert_eq!(parse_idle_secs("0d"), None);
    assert_eq!(parse_idle_secs("x"), None);
}

// Round-19 ABA: a peer RPC that names only (stream, segment) binds
// to whatever descriptor holds that name when it LANDS. These pin
// the guard that makes a stale relay refuse instead.
fn desc_with(name: &str, epoch_hex: &str) -> StreamDesc {
    crate::registry::PersistedDescriptor {
        name: name.to_string(),
        account_id: None,
        project_id: crate::tenant::ProjectId::new("proj-test").unwrap(),
        stream_epoch: epoch_hex.to_string(),
        seal_gen_counter: 0,
        key_fingerprint: String::new(),
        created_ms: 0,
        expires_at_ms: None,
        deleted: false,
        soft_deleted: false,
        logical_close_ms: None,
        forked_from: None,
        fork_children: Vec::new(),
        init: None,
        sealing: None,
        seal_op: None,
        content_type: "application/json".to_string(),
        ttl_secs: None,
        segments: None,
        sealed: false,
        watch_definitions: Vec::new(),
        watch_sig_key: None,
        parent_ref_pending: false,
        layout_version: crate::registry::LAYOUT_VERSION,
    }
    .try_into()
    .expect("valid descriptor fixture")
}

fn target_headers(d: &StreamDesc, seg: u32) -> HeaderMap {
    let t = InternalTarget::of(d, seg).expect("descriptor has an epoch");
    let mut h = HeaderMap::new();
    for (k, v) in t.headers() {
        h.insert(k, axum::http::HeaderValue::from_str(&v).unwrap());
    }
    h
}

#[test]
fn internal_target_accepts_its_own_incarnation() {
    let d = desc_with("orders", &"11".repeat(16));
    let h = target_headers(&d, 0);
    let (seg, id) = verify_internal_target(&d, &h).expect("same incarnation must verify");
    assert_eq!(seg, 0);
    assert_eq!(id, d.dynamic_segment_identity(0));
}

#[test]
fn internal_target_refuses_a_recreated_stream() {
    // The saga/read was issued against incarnation X...
    let x = desc_with("orders", &"11".repeat(16));
    let h = target_headers(&x, 0);
    // ...and the name now holds incarnation Y. The request must NOT
    // bind: a stale sweep would otherwise fence and delete Y's
    // generation-1 consumer state.
    let y = desc_with("orders", &"22".repeat(16));
    let err = verify_internal_target(&y, &h).expect_err("recreation must refuse");
    assert_eq!(err.status(), StatusCode::CONFLICT);
}

#[test]
fn internal_target_refuses_a_foreign_project() {
    // Same name, same epoch, DIFFERENT project: the §16 corruption
    // check must refuse the bind even when every other coordinate
    // matches — a silent project swap here is a cross-tenant bind.
    let d = desc_with("orders", &"55".repeat(16));
    let h = target_headers(&d, 0);
    let mut foreign = d.to_persisted();
    foreign.project_id = crate::tenant::ProjectId::new("proj-other").unwrap();
    let foreign = StreamDesc::try_from(foreign).unwrap();
    let err = verify_internal_target(&foreign, &h).expect_err("foreign project must refuse");
    assert_eq!(err.status(), StatusCode::CONFLICT);
}

#[test]
fn internal_target_refuses_an_unknown_segment() {
    let d = desc_with("orders", &"33".repeat(16));
    let mut h = target_headers(&d, 0);
    h.insert(
        "streams-internal-seg",
        axum::http::HeaderValue::from_static("7"),
    );
    let err = verify_internal_target(&d, &h).expect_err("unknown segment must refuse");
    assert_eq!(err.status(), StatusCode::CONFLICT);
}

#[test]
fn internal_target_refuses_a_mismatched_identity() {
    let d = desc_with("orders", &"44".repeat(16));
    let mut h = target_headers(&d, 0);
    h.insert(
        "streams-internal-identity",
        axum::http::HeaderValue::from_str(&crate::crypto::hex(&[9u8; 16])).unwrap(),
    );
    let err = verify_internal_target(&d, &h).expect_err("identity mismatch must refuse");
    assert_eq!(err.status(), StatusCode::CONFLICT);
}

#[test]
fn internal_target_requires_the_headers() {
    let d = desc_with("orders", &"55".repeat(16));
    let err = verify_internal_target(&d, &HeaderMap::new())
        .expect_err("an untargeted internal request must be rejected");
    assert_eq!(err.status(), StatusCode::BAD_REQUEST);
}

// Regression (two-instance rig): an ownership 409 translated to
// cursor_beyond_tail told SDKs to rewind healthy cursors, and
// dropping Streams-Replay-To hid the only signal routers use to
// converge — cross-owner lineage reads died as fake tail overruns
// and every post-split append to a foreign child failed opaquely.
#[test]
fn ownership_bounce_survives_read_translation() {
    let out = render_product_read_failure(crate::application::read::ReadFailure::Resolve(
        crate::shard_directory::ResolveError::NotOwner {
            prefix: "000".into(),
            owner: "streams-2".into(),
        },
    ));
    assert_eq!(out.status(), StatusCode::CONFLICT);
    assert_eq!(
        out.headers()
            .get("streams-replay-to")
            .and_then(|v| v.to_str().ok()),
        Some("streams-2")
    );
}

#[test]
fn plain_409_still_reads_as_beyond_tail() {
    // The applied rollback verdict is an explicit typed failure; it does
    // not infer rewind semantics from an unrelated HTTP status.
    let out = render_product_read_failure(crate::application::read::ReadFailure::CursorBeyondTail);
    assert_eq!(out.status(), StatusCode::CONFLICT);
    assert!(out.headers().get("streams-replay-to").is_none());
}
