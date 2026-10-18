//! The fold of `Streams-<Name>` request headers into `Prisma-<Name>`.
#![cfg(test)]
use super::{REQUEST_ALIASES, allow_aliases, conflict, fold};
use axum::http::{HeaderMap, HeaderValue};

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
