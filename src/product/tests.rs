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

/// Stage 7 section 13: an idle duration is positive and within the
/// service maximum (2^32 - 1 seconds), whatever unit spells it.
#[test]
fn idle_durations_stop_at_the_service_maximum() {
    assert_eq!(parse_idle_secs("4294967295"), Some(4_294_967_295));
    assert_eq!(parse_idle_secs("4294967295s"), Some(4_294_967_295));
    assert_eq!(parse_idle_secs("49710d"), Some(49_710 * 86_400));
    assert_eq!(parse_idle_secs("4294967296"), None);
    assert_eq!(parse_idle_secs("49711d"), None);
    assert_eq!(parse_idle_secs("18446744073709551615"), None);
    // Fits u64 after the unit multiply, but not an i64 of milliseconds.
    assert_eq!(parse_idle_secs("106751991167301d"), None);
    // Past u64 in the unit multiply: always refused, still refused.
    assert_eq!(parse_idle_secs("18446744073709551615d"), None);
    // Zero stays refused in every spelling.
    assert_eq!(parse_idle_secs("0"), None);
    assert_eq!(parse_idle_secs("0s"), None);
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

#[test]
fn routing_key_edges_are_pinned() {
    assert_eq!(parse_routing_key(b""), Ok(""));
    assert_eq!(parse_routing_key(b"customer-42"), Ok("customer-42"));
    assert_eq!(parse_routing_key(b"a b\tc~"), Ok("a b\tc~"));
    assert!(parse_routing_key(&[b'k'; MAX_ROUTING_KEY_BYTES]).is_ok());
    assert_eq!(
        parse_routing_key(&[b'k'; MAX_ROUTING_KEY_BYTES + 1]),
        Err("routing key exceeds 1,024 bytes")
    );
    // Latin-1 (fetch), UTF-8 (curl, the seal document), control, DEL.
    for raw in [
        &b"caf\xe9"[..],
        "caf\u{e9}".as_bytes(),
        b"bad\x01key",
        b"del\x7f",
    ] {
        assert!(parse_routing_key(raw).is_err(), "{raw:?} was admitted");
    }
}

/// The writers' rule IS "a `Prisma-Routing-Key` header reads it back as
/// the same text": every byte value, alone and at the start, middle and
/// end of a key, against the http crate's own predicate. An admitted key
/// is byte-for-byte the input -- never the default key.
#[test]
fn a_routing_key_is_admitted_exactly_when_a_header_reads_it_back() {
    let mut cases = 0_u32;
    for byte in 0..=u8::MAX {
        for raw in [
            vec![byte],
            vec![byte, b'z'],
            vec![b'a', byte, b'z'],
            vec![b'a', byte],
        ] {
            let carried = HeaderValue::from_bytes(&raw).is_ok_and(|v| v.to_str().is_ok());
            match parse_routing_key(&raw) {
                Ok(key) => {
                    assert!(carried, "{raw:?} admitted; no header reads it back");
                    assert_eq!(key.as_bytes(), raw, "{raw:?} was rewritten");
                }
                Err(_) => assert!(!carried, "{raw:?} refused; a header carries it"),
            }
            cases += 1;
        }
    }
    assert_eq!(cases, 1_024);
}

/// Item 65: every request refusal keeps its status, body, retry hint,
/// placement header and journal tag, whichever owner classifies it.
#[tokio::test]
async fn every_auth_refusal_keeps_its_response() {
    use crate::auth::AuthError as E;
    use crate::project_policy::{CredentialStatus, ProjectStatus};
    const UNVERIFIED: (StatusCode, &str) = (
        StatusCode::UNAUTHORIZED,
        "the bearer token failed verification",
    );
    const STALE: (StatusCode, &str) = (
        StatusCode::SERVICE_UNAVAILABLE,
        "this cell's authorization data is stale; retry",
    );
    const WRONG_CELL: (StatusCode, &str) = (
        StatusCode::MISDIRECTED_REQUEST,
        "this cell does not serve the project; re-resolve the              project's endpoint (the credential itself is fine)",
    );
    let denied = |message| (StatusCode::FORBIDDEN, message);
    let rows = [
        (E::TokenTooLarge, UNVERIFIED),
        (E::Malformed("x"), UNVERIFIED),
        (E::KidMissing, UNVERIFIED),
        (E::KidUnknown, UNVERIFIED),
        (E::AlgNotAllowed, UNVERIFIED),
        (E::BadSignature, UNVERIFIED),
        (E::WrongIssuer, UNVERIFIED),
        (E::WrongAudience, UNVERIFIED),
        (E::WrongCell, WRONG_CELL),
        (E::Expired, UNVERIFIED),
        (E::NotYetValid, UNVERIFIED),
        (E::LifetimeTooLong, UNVERIFIED),
        (E::ClaimInvalid("x"), UNVERIFIED),
        (E::EmptyPrefixArray, UNVERIFIED),
        (
            E::ProjectNotActive(ProjectStatus::Suspended),
            denied("the project is not active"),
        ),
        (E::OwnershipVersionMismatch, UNVERIFIED),
        (E::WorkspaceMismatch, UNVERIFIED),
        (E::CredentialUnknown, UNVERIFIED),
        (
            E::CredentialNotActive(CredentialStatus::Revoked),
            denied("the credential is not active"),
        ),
        (E::CredentialExpired, UNVERIFIED),
        (E::CredentialProjectMismatch, UNVERIFIED),
        (E::GrantVersionMismatch, UNVERIFIED),
        (E::PolicyStale, STALE),
        (E::GrantsStale, STALE),
        (E::KeysStale, STALE),
        (
            E::MissingScope(crate::tenant::Scope::RecordsRead),
            denied("the credential does not grant the scope this operation requires"),
        ),
        (
            E::PrefixDenied,
            denied("the credential's stream grant does not cover this stream"),
        ),
    ];
    for (error, (status, message)) in rows {
        let kind = error.kind();
        let response = auth_failure_response(&error);
        assert_eq!(response.status(), status, "{kind}");
        let journaled = response
            .extensions()
            .get::<crate::audit::DenialTag>()
            .is_some();
        let caller_denied = status == StatusCode::UNAUTHORIZED || status == StatusCode::FORBIDDEN;
        assert_eq!(
            journaled, caller_denied,
            "{kind}: only caller denials are journaled"
        );
        let placement = status == StatusCode::MISDIRECTED_REQUEST;
        assert_eq!(
            response.headers().contains_key("prisma-error-code"),
            placement,
            "{kind}"
        );
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(body["error"]["code"], kind);
        assert_eq!(body["error"]["message"], message, "{kind}");
        let retryable = status == StatusCode::SERVICE_UNAVAILABLE;
        assert_eq!(body["error"]["retryable"], retryable, "{kind}");
    }
}

/// A transient append refusal keeps its wait on the product surface: the
/// per-stream limiter's 429 carries `retry-after` in decimal seconds and
/// `retryable: true` (pins `render_product_append_error`'s header value).
#[tokio::test]
async fn a_transient_append_refusal_keeps_its_retry_after() {
    use crate::application::append::{AppendCode, AppendFailure, FailureClass};
    let code = AppendCode::RateLimited("limit_bytes_per_sec");
    let refused = AppendFailure::new(FailureClass::Capacity, code, "x").retry(7);
    let response = render_product_append_error(refused);
    assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
    let retry = response.headers().get("retry-after");
    assert_eq!(retry.and_then(|v| v.to_str().ok()), Some("7"));
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
    let error = &body["error"];
    assert_eq!(
        (error["code"].as_str(), error["retryable"].as_bool()),
        (Some("rate_limited"), Some(true))
    );
}

/// The core's permanent capacity refusal renders as the ONE product 413:
/// `payload_too_large` with its limit in `details`, not retryable, no
/// `retry-after` (external review §5). This is the backstop arm a producer
/// request reaches, and the only renderer of the seal's final record.
#[tokio::test]
async fn a_core_capacity_refusal_renders_the_stable_413() {
    use crate::application::append::AppendFailure;
    let refusal = crate::usage::CapacityRefusal::new("records", 100.0, 101);
    let response = render_product_append_error(AppendFailure::from_capacity(refusal));
    assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
    assert_eq!(response.headers().get("retry-after"), None);
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
    let (error, d) = (&body["error"], &body["error"]["details"]);
    assert_eq!(
        (
            error["code"].as_str(),
            error["retryable"].as_bool(),
            d["capacity"].as_u64(),
            d["requested"].as_u64()
        ),
        (Some("payload_too_large"), Some(false), Some(100), Some(101)),
        "{body}"
    );
}

/// Generated JSON values: every scalar kind, strings that need escaping and
/// nested arrays and objects.
fn json_value() -> impl proptest::strategy::Strategy<Value = serde_json::Value> {
    use proptest::strategy::{Just, Strategy, Union};
    let leaf = Union::new([
        Just(serde_json::Value::Null).boxed(),
        proptest::arbitrary::any::<bool>()
            .prop_map(serde_json::Value::from)
            .boxed(),
        proptest::arbitrary::any::<i64>()
            .prop_map(serde_json::Value::from)
            .boxed(),
        (-1.0e9f64..1.0e9).prop_map(serde_json::Value::from).boxed(),
        "[a-z\"\\\\\u{e9} ]{0,12}"
            .prop_map(serde_json::Value::from)
            .boxed(),
    ]);
    leaf.prop_recursive(3, 24, 4, |inner| {
        let array = proptest::collection::vec(inner.clone(), 0..4);
        let object = proptest::collection::btree_map("[a-z]{1,3}", inner, 0..4);
        Union::new([
            array.prop_map(serde_json::Value::Array).boxed(),
            object
                .prop_map(|m| serde_json::Value::Object(m.into_iter().collect()))
                .boxed(),
        ])
    })
}

proptest::proptest! {
    #![proptest_config(proptest::prelude::ProptestConfig { cases: 1024, ..proptest::prelude::ProptestConfig::default() })]
    /// External review §5: the product handler's capacity verdict, decided
    /// before the project's volume debit, is exactly the append core's,
    /// because it measures what the core measures: the wire body the handler
    /// hands on (a single value travels as `[value]`) and the records the
    /// core's own parser counts in it. Single values, batches and opaque
    /// bytes, compact or pretty, against buckets a few units either side of
    /// the request.
    #[test]
    fn the_product_capacity_verdict_is_the_cores(
        kind in 0u8..3,
        value in json_value(),
        values in proptest::collection::vec(json_value(), 1..6),
        raw in proptest::collection::vec(proptest::arbitrary::any::<u8>(), 1..48),
        pretty in proptest::arbitrary::any::<bool>(),
        bytes_slack in 0u64..8,
        recs_slack in 0u64..4,
    ) {
        let render = |v: &serde_json::Value| {
            if pretty { serde_json::to_vec_pretty(v) } else { serde_json::to_vec(v) }.unwrap()
        };
        let mut desc = desc_with("cap", &"11".repeat(16));
        let (body, batch, records) = match kind {
            0 => (render(&value), false, 1),
            1 => (render(&serde_json::Value::Array(values.clone())), true, values.len()),
            _ => {
                let mut bytes = desc.to_persisted();
                bytes.content_type = "application/octet-stream".to_string();
                desc = bytes.try_into().unwrap();
                (raw, false, 1)
            }
        };
        // Bucket capacities straddle the request: the `[value]` wrapping and
        // the record count decide the verdict at the boundary.
        let limits = crate::config::AdmissionConfig {
            limit_bytes_per_sec: (body.len() as u64 + bytes_slack).saturating_sub(4).max(1) as f64,
            limit_recs_per_sec: (records as u64 + recs_slack).saturating_sub(2).max(1) as f64,
            limit_burst_secs: 1.0,
            ..Default::default()
        };
        let usage = crate::usage::UsageService::new(&limits, Arc::new(crate::runtime::ManualClock::at(0)));
        let Ok(parsed) = parse_append_body(&usage, &desc, &Bytes::from(body), batch) else {
            panic!("a generated body is well formed");
        };
        // content.rs::parse_content: the core counts entries in the wire body.
        let entries = if desc.is_json() {
            crate::application::creation::json_entries(&parsed.wire, false).unwrap().len()
        } else {
            1
        };
        proptest::prop_assert_eq!(parsed.count, entries);
        let core = usage.permanently_unadmittable(parsed.wire.len() as u64, entries as u64);
        proptest::prop_assert_eq!(parsed.over_capacity, core);
    }
}
