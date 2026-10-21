//! `Streams-<Name>` aliases of the product surface's `Prisma-<Name>`
//! request headers (edge change #173).
//!
//! The Prisma Compute edge in front of `*.prisma.build` strips every
//! request header whose name starts with `Prisma-`, so a customer behind
//! it can never send `Prisma-Encryption-Key`. Each request header the
//! product surface reads is therefore also accepted as `Streams-<Name>`,
//! with the same value rules: the alias is folded into its `Prisma-` name
//! before the route runs, so every product handler reads one header
//! through the code it always had, and a browser preflight that allows a
//! `Prisma-<Name>` allows its alias. Both forms with byte-identical values
//! are one header; different values are refused 400 `conflicting_header`
//! before any handler runs.
//!
//! The edge strips `Prisma-*` RESPONSE headers too, so every response the
//! server sends carries each of its `Prisma-<Name>` headers also as
//! `Streams-<Name>`, with the same values, and an expose list that names a
//! `Prisma-<Name>` names its `Streams-<Name>` too (edge change #176). A
//! `Prisma-Streams-<X>` header is sent as `Streams-<X>` (`Streams-Origin`),
//! not with the prefix twice. That is done once, where every response
//! leaves the router (`stamp`).
#[cfg(test)]
mod tests;

use axum::extract::Request;
use axum::http::{HeaderMap, HeaderName, HeaderValue, StatusCode, header};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use axum::routing::MethodRouter;

/// `(alias, name)`: every request header the product surface reads.
pub(crate) const REQUEST_ALIASES: [(&str, &str); 3] = [
    ("streams-encryption-key", "prisma-encryption-key"),
    ("streams-routing-key", "prisma-routing-key"),
    ("streams-consumer-version", "prisma-consumer-version"),
];

/// `route` with every `Streams-<Name>` request header folded into its
/// `Prisma-<Name>` before the route's handler sees the request.
pub(crate) fn aliased<S>(route: MethodRouter<S>) -> MethodRouter<S>
where
    S: Clone + Send + Sync + 'static,
{
    route.layer(axum::middleware::from_fn(fold_request))
}

async fn fold_request(mut request: Request, next: Next) -> Response {
    if let Err((alias, name)) = fold(request.headers_mut()) {
        return conflict(alias, name);
    }
    let mut response = next.run(request).await;
    allow_aliases(response.headers_mut());
    response
}

/// Moves each alias's values to its name. An alias whose name is also
/// present must carry the same values in the same order; otherwise the
/// pair is returned, and the request it came from is refused.
fn fold(headers: &mut HeaderMap) -> Result<(), (&'static str, &'static str)> {
    for (alias, name) in REQUEST_ALIASES {
        let aliased: Vec<HeaderValue> = headers.get_all(alias).iter().cloned().collect();
        if aliased.is_empty() {
            continue;
        }
        let named = headers.get_all(name);
        if named.iter().next().is_none() {
            for value in aliased {
                headers.append(name, value);
            }
        } else if !named.iter().eq(aliased.iter()) {
            return Err((alias, name));
        }
        headers.remove(alias);
    }
    Ok(())
}

/// A preflight answer that allows `Prisma-<Name>` allows its alias too.
fn allow_aliases(headers: &mut HeaderMap) {
    let Some(allowed) = headers
        .get(header::ACCESS_CONTROL_ALLOW_HEADERS)
        .and_then(|value| value.to_str().ok())
    else {
        return;
    };
    let mut extended = allowed.to_string();
    for (alias, name) in REQUEST_ALIASES {
        if allowed
            .split(',')
            .any(|listed| listed.trim().eq_ignore_ascii_case(name))
        {
            extended.push_str(", ");
            extended.push_str(alias);
        }
    }
    if let Ok(value) = HeaderValue::from_str(&extended) {
        headers.insert(header::ACCESS_CONTROL_ALLOW_HEADERS, value);
    }
}

/// The product error shape (`crate::product::perr`), which a browser may
/// read: the refusal names the pair, never a value.
fn conflict(alias: &str, prisma: &str) -> Response {
    let body = format!(
        "{{\"error\":{{\"code\":\"conflicting_header\",\"message\":\"{alias} and {prisma} \
         carry different values; send one of them\",\"retryable\":false}}}}"
    );
    let headers = [
        (header::CONTENT_TYPE, "application/json"),
        (header::CACHE_CONTROL, "no-store"),
        (header::ACCESS_CONTROL_ALLOW_ORIGIN, "*"),
    ];
    (StatusCode::BAD_REQUEST, headers, body).into_response()
}

/// The headers every response leaves the router with: `nosniff`, the
/// server-origin marker (an answer without it never reached a server, see
/// `crate::http::router`), and the `Streams-` form of each `Prisma-` one.
/// It runs on every answer, refusals included, so the common names cost no
/// parsing or allocation (`twin`, `expose_mirrors`).
pub(crate) fn stamp(headers: &mut HeaderMap, origin_marker: &str) {
    headers.insert(
        header::X_CONTENT_TYPE_OPTIONS,
        HeaderValue::from_static("nosniff"),
    );
    if let Ok(marker) = HeaderValue::from_str(origin_marker) {
        headers.insert(HeaderName::from_static("prisma-streams-origin"), marker);
    }
    mirror(headers);
    expose_mirrors(headers);
}

/// The `Streams-` name of a lower-case `prisma-<rest>` header name:
/// `streams-<rest>`, or `<rest>` itself when it already starts with
/// `streams-` (`prisma-streams-origin` is `streams-origin`).
fn streams_name(header: &str) -> Option<String> {
    let rest = header.strip_prefix("prisma-")?;
    Some(if rest.starts_with("streams-") {
        rest.to_string()
    } else {
        format!("streams-{rest}")
    })
}

/// `streams_name` as a header name: the names the server sends are
/// spelled out, any other is derived.
fn twin(name: &HeaderName) -> Option<HeaderName> {
    let known = match name.as_str() {
        "prisma-streams-origin" => "streams-origin",
        "prisma-next-cursor" => "streams-next-cursor",
        "prisma-up-to-date" => "streams-up-to-date",
        "prisma-sealed" => "streams-sealed",
        "prisma-next-scan-cursor" => "streams-next-scan-cursor",
        "prisma-scan-complete" => "streams-scan-complete",
        "prisma-routing-key" => "streams-routing-key",
        "prisma-durable-cursor" => "streams-durable-cursor",
        "prisma-pending-from" => "streams-pending-from",
        "prisma-consumer-version" => "streams-consumer-version",
        "prisma-error-code" => "streams-error-code",
        other => return HeaderName::try_from(streams_name(other)?).ok(),
    };
    Some(HeaderName::from_static(known))
}

/// Each `prisma-<rest>` header is also sent under its `streams_name`, with
/// the same values in the same order (replacing any values there).
fn mirror(headers: &mut HeaderMap) {
    let twins: Vec<(HeaderName, HeaderName)> = headers
        .keys()
        .filter_map(|name| Some((name.clone(), twin(name)?)))
        .collect();
    for (name, twin) in twins {
        let values: Vec<HeaderValue> = headers.get_all(&name).iter().cloned().collect();
        headers.remove(&twin);
        for value in values {
            headers.append(twin.clone(), value);
        }
    }
}

/// The headers every product answer exposes to a browser
/// (`crate::product::with_product_cors`).
pub(crate) const PRODUCT_EXPOSE_HEADERS: &str = "content-type, retry-after, prisma-next-cursor, \
     prisma-up-to-date, prisma-sealed, prisma-next-scan-cursor, prisma-scan-complete, \
     prisma-routing-key, prisma-durable-cursor, prisma-pending-from, prisma-consumer-version";

/// `PRODUCT_EXPOSE_HEADERS` as `expose_mirrors` extends it, spelled out
/// because every product answer is stamped with it.
const PRODUCT_EXPOSE_MIRRORED: &str = "content-type, retry-after, prisma-next-cursor, \
     prisma-up-to-date, prisma-sealed, prisma-next-scan-cursor, prisma-scan-complete, \
     prisma-routing-key, prisma-durable-cursor, prisma-pending-from, prisma-consumer-version, \
     streams-next-cursor, streams-up-to-date, streams-sealed, streams-next-scan-cursor, \
     streams-scan-complete, streams-routing-key, streams-durable-cursor, \
     streams-pending-from, streams-consumer-version";

/// An expose list that names a `prisma-` header names its `streams_name`
/// too (`extended`); the product surface's own list is extended to the
/// list spelled out for it.
fn expose_mirrors(headers: &mut HeaderMap) {
    let Some(exposed) = headers
        .get(header::ACCESS_CONTROL_EXPOSE_HEADERS)
        .and_then(|value| value.to_str().ok())
    else {
        return;
    };
    let value = if exposed == PRODUCT_EXPOSE_HEADERS {
        HeaderValue::from_static(PRODUCT_EXPOSE_MIRRORED)
    } else {
        let Some(Ok(value)) = extended(exposed).map(HeaderValue::try_from) else {
            return;
        };
        value
    };
    headers.insert(header::ACCESS_CONTROL_EXPOSE_HEADERS, value);
}

/// `exposed` followed by the lower-case `streams_name` of each `prisma-`
/// name it lists whose twin it does not list (once per listing), or None
/// when there is nothing to add.
fn extended(exposed: &str) -> Option<String> {
    let listed: Vec<&str> = exposed.split(',').map(str::trim).collect();
    let mut extended = String::new();
    for name in &listed {
        let Some(rest) = strip_prefix_ignore_case(name, "prisma-") else {
            continue;
        };
        let prefix = if strip_prefix_ignore_case(rest, "streams-").is_some() {
            ""
        } else {
            "streams-"
        };
        let names_twin = |other: &&str| {
            strip_prefix_ignore_case(other, prefix).is_some_and(|r| r.eq_ignore_ascii_case(rest))
        };
        if listed.iter().any(names_twin) {
            continue;
        }
        if extended.is_empty() {
            extended.push_str(exposed);
        }
        extended.push_str(", ");
        extended.push_str(prefix);
        extended.extend(rest.chars().map(|c| c.to_ascii_lowercase()));
    }
    (!extended.is_empty()).then_some(extended)
}

/// `text` without `prefix`, compared ignoring ASCII case.
fn strip_prefix_ignore_case<'a>(text: &'a str, prefix: &str) -> Option<&'a str> {
    let head = text.get(..prefix.len())?;
    head.eq_ignore_ascii_case(prefix)
        .then_some(&text[prefix.len()..])
}
