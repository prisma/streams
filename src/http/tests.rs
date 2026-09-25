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

/// An append response names the position a reader resumes at: the scalar
/// offset token while the stream's segments are not materialized, the
/// segment's epoch/segment token once they are, and the start before any
/// record. A closed refusal names its closing position the same way.
#[test]
fn append_responses_name_the_resume_position() {
    let header = |response: Response| {
        let value = &response.headers()["stream-next-offset"];
        value.to_str().unwrap().to_owned()
    };
    let appended = |seg_id, next_offset, materialized| {
        let outcome = crate::application::append::AppendOutcome {
            seg_id,
            materialized,
            next_offset,
            last_offset: next_offset.saturating_sub(1),
            duplicate: false,
            closed: false,
            producer: None,
            appended_records: 1,
            descriptor: crate::sse::feed::tests::test_desc("resume-position"),
        };
        header(render_append(Ok(outcome)))
    };
    let refused = |segment, next_offset, materialized| {
        let closed = crate::shard::AppendErr::Closed { next_offset };
        let failure =
            crate::application::append::AppendFailure::from_commit(segment, materialized, closed);
        header(render_append(Err(failure)))
    };
    for next in [0, 1, 42] {
        for token in [appended(0, next, false), refused(0, next, false)] {
            let scalar = crate::offsets::parse_scalar(&token).unwrap();
            assert_eq!(scalar, next, "unsplit position {token}");
        }
        for token in [appended(3, next, true), refused(3, next, true)] {
            let position = crate::offsets::parse(&token).unwrap();
            assert_eq!(position, (3, next), "{token}");
        }
    }
}

/// The raw surface's position tokens and fork-offset refusals, pinned as
/// bytes: clients store the tokens and read the refusal words, so no codec
/// refactor may move a byte of either.
#[test]
fn raw_position_tokens_and_fork_refusals_are_exact() {
    assert_eq!(tail_token(0), "00000000000000000000000000");
    assert_eq!(tail_token(42), "000000000000000000N0000000");
    assert_eq!(append_position(3, 6, true), "000000R0000000000030000000");
    assert_eq!(append_position(3, 42, false), "000000000000000000N0000000");
    assert_eq!(parse_fork_offset("-1"), Ok(0));
    assert_eq!(parse_fork_offset("000000000000000000N0000000"), Ok(42));
    assert_eq!(
        parse_fork_offset("0000000000000000_000000000000002a"),
        Ok(42)
    );
    assert_eq!(
        parse_fork_offset("000000R0000000000030000000"),
        Err("unsupported offset epoch: 3".to_string())
    );
    assert_eq!(
        parse_fork_offset("0"),
        Err("invalid offset length: 1".to_string())
    );
    assert_eq!(
        parse_fork_offset("0000000000000000000000000U"),
        Err("invalid base32 char: U".to_string())
    );
}
use super::*;
