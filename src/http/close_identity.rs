//! The raw surface's close identity: the seal operation id a raw close
//! carries (TLA-003), built from the request's own header text so that an
//! exact retry reproduces it. Only a close has one. A plain append computes
//! none: the two SHA-256 digests over its body and the hex encoding between
//! them were about 3% of an append's CPU in the old-vs-new head-to-head, for
//! a value nothing read.
use axum::http::HeaderMap;

use super::{create_request_hash, hdr};

/// The seven coordination headers, in the order the identity hashes them.
const COORDINATION: [&str; 7] = [
    "producer-id",
    "producer-epoch",
    "producer-seq",
    "stream-seq",
    "stream-timestamp",
    "content-type",
    "stream-key-version",
];

/// The close identity of a raw request, or `None` for a plain append.
pub(super) fn raw(
    close: bool,
    headers: &HeaderMap,
    content_type: &str,
    routing_key: &str,
    body: &[u8],
) -> Option<String> {
    if !close {
        return None;
    }
    let coordination = COORDINATION.map(|name| hdr(headers, name).unwrap_or_default());
    Some(crate::application::lifecycle::seal_op_id_semantic(
        &create_request_hash(content_type, None, None, true, body, None),
        routing_key,
        &coordination,
    ))
}

#[cfg(test)]
mod tests {
    use super::{COORDINATION, raw};
    use axum::http::HeaderMap;

    fn headers() -> HeaderMap {
        let mut headers = HeaderMap::new();
        for (name, value) in [
            ("producer-id", "writer-1"),
            ("producer-epoch", "3"),
            ("producer-seq", "7"),
            ("stream-timestamp", "1700000000000"),
            ("content-type", "application/json"),
            ("host", "example"),
        ] {
            headers.insert(name, value.parse().unwrap());
        }
        headers
    }

    #[test]
    fn a_plain_append_has_no_close_identity() {
        assert_eq!(
            raw(false, &headers(), "application/json", "k", b"[1]"),
            None
        );
    }

    /// The identity is the request hash of the body under its content type,
    /// the routing key, and the seven coordination headers' own text in
    /// order, an absent header as the empty string: what every raw close
    /// carried before a plain append stopped computing it.
    #[test]
    fn a_close_identity_hashes_the_coordination_headers_text_in_order() {
        let headers = headers();
        let want = crate::application::lifecycle::seal_op_id_semantic(
            &super::create_request_hash("application/json", None, None, true, b"[1]", None),
            "k",
            &[
                "writer-1".into(),
                "3".into(),
                "7".into(),
                String::new(),
                "1700000000000".into(),
                "application/json".into(),
                String::new(),
            ],
        );
        assert_eq!(COORDINATION[3], "stream-seq");
        assert_eq!(COORDINATION[6], "stream-key-version");
        assert_eq!(
            raw(true, &headers, "application/json", "k", b"[1]").as_deref(),
            Some(want.as_str())
        );
    }
}
