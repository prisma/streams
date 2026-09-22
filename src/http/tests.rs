//! The HTTP surface's unit tests.
#![cfg(test)]

/// interval_cursor_at is a pure function of (now, request cursor):
/// exact outputs for fixed instants, both arms and both fallbacks.
#[test]
fn interval_cursor_at_is_exact() {
    let now = 90_000_000 * 20_000; // interval 90_000_000 exactly
    assert_eq!(interval_cursor_at(now, None), "90000000");
    // request cursor at/above the current interval echoes r + 1:
    assert_eq!(interval_cursor_at(now, Some("90000000")), "90000001");
    assert_eq!(interval_cursor_at(now, Some("95000000")), "95000001");
    // long-past request cursor yields the current interval:
    assert_eq!(interval_cursor_at(now, Some("5")), "90000000");
    // unparseable request cursor likewise:
    assert_eq!(interval_cursor_at(now, Some("junk")), "90000000");
    // epoch boundary:
    assert_eq!(interval_cursor_at(0, None), "0");
    assert_eq!(interval_cursor_at(19_999, None), "0");
    assert_eq!(interval_cursor_at(20_000, None), "1");
}

// Round-19 fleet-contract: hierarchical names with characters that
// are structural in a URL must survive a relay intact.
#[test]
fn stream_names_encode_for_peer_paths() {
    assert_eq!(
        encode_stream_name_path("customers/acme/orders"),
        "customers/acme/orders",
        "the hierarchy separator must survive"
    );
    assert_eq!(encode_stream_name_path("a?b"), "a%3Fb");
    assert_eq!(encode_stream_name_path("a#b"), "a%23b");
    assert_eq!(encode_stream_name_path("a%b"), "a%25b");
    assert_eq!(encode_stream_name_path("a b"), "a%20b");
    // UTF-8 is encoded byte-wise.
    assert_eq!(encode_stream_name_path("é"), "%C3%A9");
}

/// Stream-TTL admits the canonical decimal grammar up to the service
/// ceiling (2^32 - 1 seconds) and nothing past it. Anything larger used
/// to be admitted and, from ~9.22e15 seconds up, wrapped the stream's
/// expiry into the past. `0` stays admitted on this surface: the
/// protocol gives it no meaning and the conformance suite never sends
/// it (the product surface refuses it as it always did).
#[test]
fn stream_ttl_refuses_windows_past_the_ceiling() {
    // The grammar, unchanged.
    assert_eq!(parse_ttl_strict("0"), Some(0));
    assert_eq!(parse_ttl_strict("3600"), Some(3600));
    assert_eq!(parse_ttl_strict(""), None);
    assert_eq!(parse_ttl_strict("00060"), None);
    assert_eq!(parse_ttl_strict("+60"), None);
    assert_eq!(parse_ttl_strict("60.5"), None);
    assert_eq!(parse_ttl_strict("1e3"), None);
    // The ceiling is the last admitted window.
    assert_eq!(parse_ttl_strict("4294967295"), Some(4_294_967_295));
    assert_eq!(parse_ttl_strict("4294967296"), None);
    // The review's two values, and one past u64 (always a parse error).
    assert_eq!(parse_ttl_strict("9223372036854776"), None);
    assert_eq!(parse_ttl_strict("18446744073709551615"), None);
    assert_eq!(parse_ttl_strict("18446744073709551616"), None);
}
use super::*;
