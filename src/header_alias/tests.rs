//! The fold of `Streams-<Name>` request headers into `Prisma-<Name>`, and
//! the `Streams-<Name>` form of every `Prisma-<Name>` response header.
#![cfg(test)]
use super::{
    PRODUCT_EXPOSE_HEADERS, PRODUCT_EXPOSE_MIRRORED, REQUEST_ALIASES, allow_aliases, conflict,
    expose_mirrors, extended, fold, mirror, stamp, streams_name, twin,
};
use axum::http::{HeaderMap, HeaderName, HeaderValue};

fn map(pairs: &[(&'static str, &'static str)]) -> HeaderMap {
    let mut headers = HeaderMap::new();
    for (name, value) in pairs {
        headers.append(*name, HeaderValue::from_static(value));
    }
    headers
}

fn values(headers: &HeaderMap, name: &str) -> Vec<String> {
    headers
        .get_all(name)
        .iter()
        .map(|v| String::from_utf8_lossy(v.as_bytes()).into_owned())
        .collect()
}

#[test]
fn an_alias_alone_becomes_its_prisma_name_with_every_value_in_order() {
    for (alias, name) in REQUEST_ALIASES {
        let mut headers = map(&[(alias, "a"), (alias, "b"), ("content-type", "x")]);
        assert_eq!(fold(&mut headers), Ok(()));
        assert_eq!(values(&headers, name), ["a", "b"], "{alias}");
        assert!(headers.get(alias).is_none(), "{alias} is folded away");
        assert_eq!(values(&headers, "content-type"), ["x"]);
        assert_eq!(headers.len(), 3);
    }
}

#[test]
fn equal_values_under_both_names_are_one_header() {
    for (alias, name) in REQUEST_ALIASES {
        let mut headers = map(&[(name, "v"), (alias, "v")]);
        assert_eq!(fold(&mut headers), Ok(()));
        assert_eq!(values(&headers, name), ["v"], "{alias}");
        assert!(headers.get(alias).is_none());
    }
}

#[test]
fn different_values_under_both_names_return_the_pair() {
    for (alias, name) in REQUEST_ALIASES {
        let mut headers = map(&[(name, "v"), (alias, "w")]);
        assert_eq!(fold(&mut headers), Err((alias, name)));
        let mut headers = map(&[(name, "v"), (alias, "v"), (alias, "w")]);
        assert_eq!(
            fold(&mut headers),
            Err((alias, name)),
            "an extra alias value differs"
        );
        let mut headers = map(&[(name, "v"), (name, "w"), (alias, "v")]);
        assert_eq!(
            fold(&mut headers),
            Err((alias, name)),
            "a missing alias value differs"
        );
    }
}

#[test]
fn a_request_without_aliases_is_untouched() {
    let mut headers = map(&[
        ("prisma-encryption-key", "k"),
        ("stream-encryption-key", "s"),
    ]);
    let before = headers.clone();
    assert_eq!(fold(&mut headers), Ok(()));
    assert_eq!(headers, before);
}

/// Every `Prisma-` request header the product handlers read has its
/// alias: a new `.get("prisma-…")` without a row here fails.
#[test]
fn every_prisma_request_header_the_product_surface_reads_has_an_alias() {
    let source = include_str!("../product.rs");
    let read: std::collections::BTreeSet<&str> = source
        .match_indices(".get(\"prisma-")
        .map(|(at, needle)| {
            let rest = &source[at + needle.len() - "prisma-".len()..];
            &rest[..rest.find('"').expect("closing quote")]
        })
        .collect();
    let aliased: std::collections::BTreeSet<&str> =
        REQUEST_ALIASES.iter().map(|(_, name)| *name).collect();
    assert_eq!(read, aliased);
}

#[test]
fn a_preflight_that_allows_a_prisma_name_allows_its_alias() {
    let mut headers = map(&[(
        "access-control-allow-headers",
        "authorization, Prisma-Encryption-Key, prisma-routing-key",
    )]);
    allow_aliases(&mut headers);
    assert_eq!(
        values(&headers, "access-control-allow-headers"),
        ["authorization, Prisma-Encryption-Key, prisma-routing-key, \
          streams-encryption-key, streams-routing-key"]
    );
    let mut headers = map(&[("content-type", "x")]);
    allow_aliases(&mut headers);
    assert_eq!(headers.len(), 1, "no allow list, nothing added");
}

/// The refusal is byte for byte what the product surface's `perr`
/// renders, plus the CORS origin a browser needs to read it.
#[tokio::test]
async fn the_conflict_refusal_has_the_product_error_shape() {
    let (alias, name) = REQUEST_ALIASES[0];
    let refusal = conflict(alias, name);
    let product = crate::product::perr(
        axum::http::StatusCode::BAD_REQUEST,
        "conflicting_header",
        "streams-encryption-key and prisma-encryption-key carry different values; send one of them",
        None,
        false,
    );
    assert_eq!(refusal.status(), product.status());
    for name in ["content-type", "cache-control"] {
        assert_eq!(
            refusal.headers().get(name),
            product.headers().get(name),
            "{name}"
        );
    }
    assert_eq!(
        refusal
            .headers()
            .get("access-control-allow-origin")
            .map(|v| v.as_bytes()),
        Some(&b"*"[..])
    );
    let body = |r: axum::response::Response| axum::body::to_bytes(r.into_body(), 4096);
    assert_eq!(body(refusal).await.unwrap(), body(product).await.unwrap());
}

#[test]
fn every_prisma_response_header_is_sent_as_its_streams_name_with_every_value() {
    let mut headers = map(&[
        ("prisma-next-cursor", "c1"),
        ("Prisma-Up-To-Date", "true"),
        ("prisma-pending-from", "3"),
        ("prisma-durable-cursor", "d1"),
        ("prisma-error-code", "wrong_cell"),
        ("prisma-x", "a"),
        ("prisma-x", "b"),
        ("streams-x", "stale"),
        ("streams-replay-to", "peer"),
        ("content-type", "application/json"),
    ]);
    mirror(&mut headers);
    for (name, expected) in [
        ("next-cursor", &["c1"][..]),
        ("up-to-date", &["true"]),
        ("pending-from", &["3"]),
        ("durable-cursor", &["d1"]),
        ("error-code", &["wrong_cell"]),
        ("x", &["a", "b"]),
    ] {
        assert_eq!(
            values(&headers, &format!("prisma-{name}")),
            expected,
            "{name}"
        );
        assert_eq!(
            values(&headers, &format!("streams-{name}")),
            expected,
            "{name}"
        );
    }
    assert_eq!(values(&headers, "streams-replay-to"), ["peer"]);
    assert_eq!(values(&headers, "content-type"), ["application/json"]);
    assert_eq!(headers.len(), 16);
}

#[test]
fn an_expose_list_naming_a_prisma_header_names_its_streams_form() {
    let mut headers = map(&[(
        "access-control-expose-headers",
        "content-type, Prisma-Next-Cursor, prisma-sealed, streams-sealed",
    )]);
    expose_mirrors(&mut headers);
    assert_eq!(
        values(&headers, "access-control-expose-headers"),
        [
            "content-type, Prisma-Next-Cursor, prisma-sealed, streams-sealed, \
          streams-next-cursor"
        ]
    );
    for list in ["*", "content-type, retry-after"] {
        let mut headers = map(&[("access-control-expose-headers", list)]);
        expose_mirrors(&mut headers);
        assert_eq!(values(&headers, "access-control-expose-headers"), [list]);
    }
}

#[test]
fn every_response_is_stamped_with_nosniff_the_origin_and_its_streams_forms() {
    let mut headers = map(&[("prisma-sealed", "true")]);
    stamp(&mut headers, "cell-1");
    assert_eq!(values(&headers, "x-content-type-options"), ["nosniff"]);
    assert_eq!(values(&headers, "prisma-streams-origin"), ["cell-1"]);
    assert_eq!(values(&headers, "streams-origin"), ["cell-1"]);
    assert_eq!(values(&headers, "streams-sealed"), ["true"]);
    assert_eq!(headers.len(), 5);
    let mut headers = HeaderMap::new();
    stamp(&mut headers, "not\na header value");
    assert_eq!(
        headers.len(),
        1,
        "an unusable marker is left out: {headers:?}"
    );
}

#[test]
fn a_prisma_streams_header_takes_the_streams_prefix_once() {
    for (name, twin) in [
        ("prisma-streams-origin", Some("streams-origin")),
        ("prisma-streams-data", Some("streams-data")),
        ("prisma-next-cursor", Some("streams-next-cursor")),
        ("prisma-streamsx", Some("streams-streamsx")),
        ("streams-origin", None),
        ("content-type", None),
    ] {
        assert_eq!(streams_name(name).as_deref(), twin, "{name}");
    }
    let mut headers = map(&[
        ("prisma-streams-data", "d"),
        ("access-control-expose-headers", "prisma-streams-data"),
    ]);
    mirror(&mut headers);
    expose_mirrors(&mut headers);
    assert_eq!(values(&headers, "streams-data"), ["d"]);
    assert_eq!(
        values(&headers, "access-control-expose-headers"),
        ["prisma-streams-data, streams-data"]
    );
    assert_eq!(headers.len(), 3);
}

/// Every name `twin` spells out is the name `streams_name` derives, and an
/// unlisted one is derived.
#[test]
fn the_spelled_out_twins_are_the_derived_names() {
    for name in [
        "prisma-streams-origin",
        "prisma-next-cursor",
        "prisma-up-to-date",
        "prisma-sealed",
        "prisma-next-scan-cursor",
        "prisma-scan-complete",
        "prisma-routing-key",
        "prisma-durable-cursor",
        "prisma-pending-from",
        "prisma-consumer-version",
        "prisma-error-code",
        "prisma-unlisted",
        "prisma-streams-unlisted",
    ] {
        let derived = streams_name(name).unwrap();
        let spelled = twin(&HeaderName::from_static(name)).unwrap();
        assert_eq!(spelled.as_str(), derived, "{name}");
    }
    assert_eq!(twin(&HeaderName::from_static("streams-origin")), None);
    assert_eq!(twin(&HeaderName::from_static("content-type")), None);
}

/// The expose-list extension before it was made allocation-free, kept as
/// the reference the new one must answer exactly as.
fn reference_expose_mirrors(headers: &mut HeaderMap) {
    let Some(exposed) = headers
        .get("access-control-expose-headers")
        .and_then(|value| value.to_str().ok())
    else {
        return;
    };
    let listed: Vec<String> = exposed
        .split(',')
        .map(|name| name.trim().to_ascii_lowercase())
        .collect();
    let mut extended = exposed.to_string();
    for name in &listed {
        if let Some(twin) = streams_name(name)
            && !listed.contains(&twin)
        {
            extended.push_str(", ");
            extended.push_str(&twin);
        }
    }
    if let Ok(value) = HeaderValue::from_str(&extended) {
        headers.insert("access-control-expose-headers", value);
    }
}

/// The product surface's expose list, and lists in mixed case, with
/// duplicates, twins already named, a `prisma-streams-` name, padding and
/// empty items, are extended byte for byte as the reference extends them.
#[test]
fn an_expose_list_is_extended_exactly_as_the_reference_extends_it() {
    let product = crate::product::with_product_cors(axum::response::Response::default());
    let product = product.headers()["access-control-expose-headers"]
        .to_str()
        .ok();
    assert_eq!(product, Some(PRODUCT_EXPOSE_HEADERS));
    assert_eq!(
        extended(PRODUCT_EXPOSE_HEADERS).as_deref(),
        Some(PRODUCT_EXPOSE_MIRRORED),
        "the spelled-out list is the derived one"
    );
    for list in [
        PRODUCT_EXPOSE_HEADERS,
        PRODUCT_EXPOSE_MIRRORED,
        "*",
        "",
        "content-type",
        "Prisma-Next-Cursor",
        "PRISMA-SEALED, Streams-Sealed",
        "prisma-sealed,prisma-sealed",
        "prisma-sealed, streams-sealed, prisma-sealed",
        " prisma-up-to-date ,, prisma-streams-origin,streams-origin",
        "prisma-, prisma-streams-, prisma-x",
        "prisma-Streams-Data, prisma-streamsx",
        "retry-after, prisma-a, prisma-b, streams-a",
    ] {
        let mut ours = map(&[]);
        ours.insert(
            "access-control-expose-headers",
            HeaderValue::from_str(list).unwrap(),
        );
        let mut reference = ours.clone();
        expose_mirrors(&mut ours);
        reference_expose_mirrors(&mut reference);
        assert_eq!(ours, reference, "{list:?}");
    }
}
