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
        "this cell does not serve the project; re-resolve the project's endpoint (the credential itself is fine)",
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
        // Shared cells Q10(c): a stale feed's 503 asks for a retry after
        // 1 s, so a raw client waits as the SDK does; nothing else does.
        let stale = status == StatusCode::SERVICE_UNAVAILABLE;
        assert_eq!(
            response.headers().get("retry-after").map(|v| v.as_bytes()),
            stale.then_some(b"1".as_slice()),
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

/// A rendered watch refusal as the wire and the journal see it, one column
/// each, `-` for none: status, body code (`-` for a body that is not a JSON
/// error), `retryable`, `retry-after`, `streams-replay-to`, and the journal
/// tag as `class:project`. No watch refusal carries `prisma-error-code`.
async fn watch_wire(failure: crate::application::watch::WatchFailure) -> String {
    let response = watch_failure_response(failure);
    let header = |name| {
        let value = response.headers().get(name);
        value.map_or("-", |v| v.to_str().unwrap())
    };
    assert_eq!(header("prisma-error-code"), "-");
    let tag = response.extensions().get::<crate::audit::DenialTag>();
    let journal = tag.map_or("-".to_string(), |t| {
        format!("{}:{}", t.code, t.project.as_deref().unwrap_or("-"))
    });
    let status = response.status().as_u16();
    let tail = format!(
        "{} {} {journal}",
        header("retry-after"),
        header("streams-replay-to")
    );
    let body = axum::body::to_bytes(response.into_body(), usize::MAX).await;
    let body: serde_json::Value = serde_json::from_slice(&body.unwrap()).unwrap_or_default();
    let (code, retryable) = (&body["error"]["code"], &body["error"]["retryable"]);
    let retryable = retryable
        .as_bool()
        .map_or("-".to_string(), |r| r.to_string());
    format!(
        "{status} {} {retryable} {tail}",
        code.as_str().unwrap_or("-")
    )
}

/// Every watch refusal and how it renders (`watch_wire`'s columns).
fn watch_refusals() -> Vec<(crate::application::watch::WatchFailure, &'static str)> {
    use crate::application::watch::WatchFailure as W;
    use crate::quota::QuotaRefusal as Q;
    use crate::shard_directory::ResolveError as R;
    let p = || crate::tenant::ProjectId::new("proj-w").unwrap();
    let prefix = || "0a".to_string();
    vec![
        (
            W::Unauthorized(Some(p())),
            "403 watch_unauthorized false - - watch_unauthorized:proj-w",
        ),
        (
            W::Unauthorized(None),
            "403 watch_unauthorized false - - watch_unauthorized:-",
        ),
        (W::PolicyStale(p()), "503 policy_stale true - - -"),
        (
            W::ProjectInactive(p()),
            "403 project_not_active false - - project_not_active:proj-w",
        ),
        (
            W::Quota(
                p(),
                Q::Rate {
                    retry_after_secs: 7,
                },
            ),
            "429 project_rate_limit true 7 - project_rate_limit:proj-w",
        ),
        (
            W::Quota(p(), Q::Concurrency),
            "429 project_concurrency_limit true - - project_concurrency_limit:proj-w",
        ),
        (
            W::Quota(p(), Q::TrackerCapacity),
            "503 project_tracker_capacity true - - project_tracker_capacity:proj-w",
        ),
        (
            W::Quota(p(), Q::StreamLimit),
            "429 stream_limit false - - project_stream_limit:proj-w",
        ),
        (
            W::Quota(p(), Q::QueuedBytes),
            "429 queued_bytes true - - project_queued_bytes:proj-w",
        ),
        (
            W::Quota(p(), Q::MemoryPressure),
            "429 project_memory_pressure true 1 - project_memory_pressure:proj-w",
        ),
        (
            W::Quota(
                p(),
                Q::StoredBytes {
                    limit: 1,
                    stored: 2,
                },
            ),
            "429 stored_bytes_limit false - - project_stored_bytes:proj-w",
        ),
        (
            W::LookupBusy,
            "429 watch_lookup_busy true 1 - watch_lookup_busy:-",
        ),
        (W::InvalidKey, "400 invalid_watch_key false - - -"),
        (W::Creating, "503 creating true - - -"),
        (W::NotFound, "404 not_found false - - -"),
        (W::UnknownWatch, "404 unknown_watch false - - -"),
        (W::Storage("store down".into()), "500 internal true - - -"),
        (
            W::Resolve(R::NotOwner {
                prefix: prefix(),
                owner: "streams-2".into(),
            }),
            "409 not_ring_owner - - streams-2 -",
        ),
        (
            W::Resolve(R::Opening {
                prefix: prefix(),
                code: "shard_opening",
                retry_after_secs: 3,
            }),
            "503 shard_opening - 3 - -",
        ),
        (
            W::Resolve(R::OpenFailed {
                prefix: prefix(),
                error: "x".into(),
            }),
            "500 shard_open - - - -",
        ),
    ]
}

/// Every watch refusal keeps its status, body code, retry hint, replay
/// header and journal tag: the capability refusal is journaled with the
/// project it names (or none), a spent lookup budget's retryable 429 with
/// none, an inactive project and every project quota refusal with their
/// project, and nothing else is journaled. A storage
/// failure carries its message; a routing failure keeps the one resolve
/// mapping, which has no `retryable` field.
#[tokio::test]
async fn every_watch_refusal_keeps_its_response() {
    for (failure, want) in watch_refusals() {
        assert_eq!(watch_wire(failure).await, want);
    }
    let failure = crate::application::watch::WatchFailure::Storage("store down".into());
    let body = axum::body::to_bytes(watch_failure_response(failure).into_body(), usize::MAX);
    let body: serde_json::Value = serde_json::from_slice(&body.await.unwrap()).unwrap();
    assert_eq!(body["error"]["message"], "store down");
}

/// Stored-cap B4 (T5): a project at its stored cap is refused 429
/// `stored_bytes_limit`, not retryable and with no `retry-after` (it clears
/// when the project stores less or its quota grows, not at a time a client
/// can wait for), its details the dimension, the quota and the figure it
/// was refused at.
#[tokio::test]
async fn the_stored_cap_refusal_states_the_quota_and_the_figure() {
    let refusal = crate::quota::QuotaRefusal::StoredBytes {
        limit: 262_144,
        stored: 262_624,
    };
    let response = quota_refusal_response(&refusal);
    assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
    assert_eq!(response.headers().get("retry-after"), None);
    let body = axum::body::to_bytes(response.into_body(), usize::MAX);
    let body: serde_json::Value = serde_json::from_slice(&body.await.unwrap()).unwrap();
    let error = &body["error"];
    assert_eq!(
        (error["code"].as_str(), error["retryable"].as_bool()),
        (Some("stored_bytes_limit"), Some(false))
    );
    let details = serde_json::Value::from_iter([
        ("dimension", serde_json::Value::from("stored_bytes")),
        ("limit", 262_144.into()),
        ("stored", 262_624.into()),
    ]);
    assert_eq!(error["details"], details);
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

/// A rendered append refusal as a client and the audit journal see it: the
/// status and error body, and `retry-after`, `streams-replay-to` and the
/// denial tag when the response carries them.
async fn append_refusal(response: Response) -> serde_json::Value {
    let mut seen = serde_json::Map::new();
    seen.insert("status".into(), response.status().as_u16().into());
    for (key, name) in [
        ("retryAfter", "retry-after"),
        ("replayTo", "streams-replay-to"),
    ] {
        if let Some(value) = response.headers().get(name).and_then(|v| v.to_str().ok()) {
            seen.insert(key.into(), value.into());
        }
    }
    if let Some(tag) = response.extensions().get::<crate::audit::DenialTag>() {
        seen.insert("tag".into(), tag.code.into());
    }
    let body = axum::body::to_bytes(response.into_body(), usize::MAX);
    let body: serde_json::Value = serde_json::from_slice(&body.await.unwrap()).unwrap();
    seen.insert("error".into(), body["error"].clone());
    seen.into()
}

/// The error table's rows that the failure's code decides.
fn append_code_rows() -> Vec<(crate::application::append::AppendFailure, serde_json::Value)> {
    use crate::application::append::{AppendCode as C, AppendFailure, FailureClass as F};
    use crate::shard::AppendErr;
    let commit = |error| AppendFailure::from_commit(0, false, error);
    let mut not_owner = AppendFailure::new(F::Conflict, C::NotOwner, "x");
    not_owner.owner = Some("http://10.0.0.2:8080".into());
    let backlog = AppendFailure::new(F::Unavailable, C::MaintenanceBackpressure, "x").retry(5);
    vec![
        (
            not_owner,
            json!({"status": 409, "replayTo": "http://10.0.0.2:8080", "error": {
                "code": "not_stream_owner", "retryable": true,
                "message": "another instance owns the target segment; retry through the router"}}),
        ),
        (
            commit(AppendErr::ProducerGap {
                expected: 3,
                received: 5,
            }),
            json!({"status": 409, "error": {
                "code": "producer_gap", "message": "producer sequence gap",
                "details": {"expected": 3, "received": 5}, "retryable": false}}),
        ),
        (
            commit(AppendErr::ProducerStale { current_epoch: 4 }),
            json!({"status": 403, "error": {
                "code": "stale_producer_epoch", "message": "producer epoch is stale",
                "details": {"currentEpoch": 4}, "retryable": false}}),
        ),
        (
            commit(AppendErr::ProducerSeqReused),
            json!({"status": 409, "error": {
                "code": "producer_sequence_reused", "retryable": false,
                "message": "same producer sequence with a different request"}}),
        ),
        (
            commit(AppendErr::ProducerEpochSeq),
            json!({"status": 400, "error": {
                "code": "producer_epoch_must_start_at_zero", "retryable": false,
                "message": "a new producer epoch must start at sequence 0"}}),
        ),
        (
            commit(AppendErr::Closed { next_offset: 7 }),
            json!({"status": 409, "error": {
                "code": "sealed", "message": "collection is sealed", "retryable": false}}),
        ),
        (
            backlog,
            json!({"status": 503, "retryAfter": "5", "error": {
                "code": "maintenance_backpressure", "retryable": true,
                "message": "maintenance backlog exceeds its bound; retry after it drains"}}),
        ),
        (
            commit(AppendErr::CtMismatch),
            json!({"status": 409, "error": {
                "code": "content_type_mismatch", "message": "content type mismatch",
                "retryable": false}}),
        ),
        // A group the committer stranded, of a plain append (#153).
        (
            commit(AppendErr::Moved).answering(false),
            json!({"status": 503, "retryAfter": "1", "error": {
                "code": "outcome_unknown", "retryable": false,
                "message": "the shard moved while this append was in flight; it may or may not have been written; retry only if a second copy is acceptable, or append through a producer"}}),
        ),
        // The same group of a producer append or a seal's final record.
        (
            commit(AppendErr::Moved).answering(true),
            json!({"status": 503, "retryAfter": "1", "error": {
                "code": "temporarily_unavailable", "message": "retry shortly", "retryable": true}}),
        ),
    ]
}

/// The error table's rows that the failure's class decides.
fn append_class_rows() -> Vec<(crate::application::append::AppendFailure, serde_json::Value)> {
    use crate::application::append::{AppendCode as C, AppendFailure, FailureClass as F};
    let failed = |status| {
        json!({"status": status, "error": {
            "code": "append_failed", "message": "append failed", "retryable": false}})
    };
    vec![
        (
            AppendFailure::new(F::Missing, C::NotFound, "x"),
            json!({"status": 404, "error": {
                "code": "not_found", "message": "stream not found", "retryable": false}}),
        ),
        (
            AppendFailure::new(F::Denied, C::WrongKey, "x"),
            json!({"status": 403, "tag": "stale_or_wrong_credentials", "error": {
                "code": "stale_or_wrong_credentials", "message": "forbidden", "retryable": false}}),
        ),
        (
            AppendFailure::new(F::Conflict, C::TargetIncarnationChanged, "x"),
            json!({"status": 409, "error": {
                "code": "conflict", "message": "producer or configuration conflict",
                "retryable": false}}),
        ),
        (
            AppendFailure::new(F::Invalid, C::BodyTooLarge, "x"),
            json!({"status": 413, "error": {
                "code": "body_too_large", "message": "request body exceeds the limit",
                "retryable": false}}),
        ),
        (
            AppendFailure::new(F::Invalid, C::InvalidBody, "x"),
            failed(400),
        ),
        (
            AppendFailure::new(F::Capacity, C::Overloaded, "x").retry(1),
            json!({"status": 429, "retryAfter": "1", "error": {
                "code": "rate_limited", "message": "admission or rate limit", "retryable": true}}),
        ),
        // An engine closed under the request (edge change #136): nothing written.
        (
            AppendFailure::new(F::Unavailable, C::ShardMoving, "x").retry(1),
            json!({"status": 503, "retryAfter": "1", "error": {
                "code": "temporarily_unavailable", "message": "retry shortly", "retryable": true}}),
        ),
        (
            AppendFailure::new(F::Timeout, C::AppendTimeout, "x"),
            failed(408),
        ),
        (
            AppendFailure::new(F::Internal, C::Internal, "x"),
            failed(500),
        ),
    ]
}

/// Every row of the product append's error table (`render_product_append_error`,
/// WIRE-MATRIX §2.7): each typed failure the shared append path returns, with
/// the exact status, body, `retry-after`, replay target and denial tag a
/// client and the audit journal receive.
#[tokio::test]
async fn every_append_failure_renders_its_product_row() {
    for (failure, want) in append_code_rows().into_iter().chain(append_class_rows()) {
        let label = failure.to_string();
        let got = append_refusal(render_product_append_error(failure)).await;
        assert_eq!(got, want, "{label}");
    }
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
    /// because it measures what the core measures: the records the core's own
    /// parser finds in the wire body the handler hands on (a single value
    /// travels as `[value]`), counted and measured as they are stored. Single
    /// values, batches and opaque
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
        // Bucket capacities straddle the request: the stored bytes and the
        // record count decide the verdict at the boundary.
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
        // content.rs::parse_content: the core counts the entries it stores
        // from the wire body and measures their stored bytes.
        let entries = if desc.is_json() {
            crate::application::creation::json_entries(&parsed.wire, false).unwrap()
        } else {
            vec![parsed.wire.clone()]
        };
        proptest::prop_assert_eq!(parsed.count, entries.len());
        let stored = entries.iter().map(|entry| entry.len() as u64).sum();
        let core = usage.permanently_unadmittable(stored, entries.len() as u64);
        proptest::prop_assert_eq!(parsed.over_capacity, core);
    }
}

/// A creation document is parsed up to exactly 64 KiB (65,536 bytes,
/// `MAX_CONFIG_BODY`); one byte more is refused `400 invalid_config`,
/// "configuration body exceeds 64 KiB", before it is parsed.
#[tokio::test]
async fn a_creation_document_is_parsed_up_to_exactly_64_kib() {
    let doc = |len: usize| {
        let shell = r#"{"format":{"kind":"json"},"watches":[{"name":"w","fields":["/"]}]}"#;
        let pointer = format!("\"/{}\"", "p".repeat(len - shell.len()));
        Bytes::from(shell.replace("\"/\"", &pointer))
    };
    let at = doc(64 * 1024);
    assert_eq!(at.len(), 65_536);
    assert!(
        parse_create_doc(&at).is_ok(),
        "a document of exactly 64 KiB"
    );
    let over = doc(64 * 1024 + 1);
    let Err(refused) = parse_create_doc(&over) else {
        panic!("a document of 65,537 bytes was parsed");
    };
    assert_eq!(refused.status(), StatusCode::BAD_REQUEST);
    let body = axum::body::to_bytes(refused.into_body(), usize::MAX).await;
    let body: serde_json::Value = serde_json::from_slice(&body.unwrap()).unwrap();
    let error = (&body["error"]["code"], &body["error"]["message"]);
    assert_eq!(
        error,
        (
            &json!("invalid_config"),
            &json!("configuration body exceeds 64 KiB")
        )
    );
}
