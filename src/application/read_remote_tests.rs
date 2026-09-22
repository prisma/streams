use super::*;
use crate::application::read::ReadFailure;

/// The wire codes are the verdicts' own names: a coordinator decodes the
/// decision the owner made, never the public error vocabulary.
#[test]
fn the_page_route_refusal_names_the_owner_verdict() {
    for (failure, code) in [
        (ReadFailure::CursorBeyondTail, "cursor_beyond_tail"),
        (ReadFailure::ChangedIncarnation, "changed_incarnation"),
        (ReadFailure::Missing, "missing"),
        (ReadFailure::Gone, "gone"),
    ] {
        let refused = WireReadRefusal::of(&failure).expect("a stream verdict is typed");
        assert_eq!(
            serde_json::to_string(&WireRefusedPage { refused }).unwrap(),
            format!("{{\"refused\":\"{code}\"}}")
        );
        assert!(
            matches!(
                (ReadFailure::from(refused), &failure),
                (ReadFailure::CursorBeyondTail, ReadFailure::CursorBeyondTail)
                    | (
                        ReadFailure::ChangedIncarnation,
                        ReadFailure::ChangedIncarnation
                    )
                    | (ReadFailure::Missing, ReadFailure::Missing)
                    | (ReadFailure::Gone, ReadFailure::Gone)
            ),
            "{code} round-trips to the verdict it names"
        );
    }
    for failure in [
        ReadFailure::WrongKey,
        ReadFailure::InvalidCursor,
        ReadFailure::MissingKey,
        ReadFailure::KeylessLive,
        ReadFailure::AppliedFork,
        ReadFailure::Creating,
        ReadFailure::Storage("x".into()),
        ReadFailure::Resolve(crate::shard_directory::ResolveError::NotOwner {
            prefix: "0".into(),
            owner: "b".into(),
        }),
        ReadFailure::Remote(RemoteSpanError::Unauthorized),
    ] {
        assert!(
            WireReadRefusal::of(&failure).is_none(),
            "not a stream verdict: {failure:?}"
        );
    }
}

/// A typed body is the owner's decision whatever status carried it; an
/// answer without one (an older owner, a refusal decided before the read
/// service ran) keeps the meaning its status always had.
#[test]
fn a_typed_refusal_is_relayed_and_an_untyped_answer_keeps_its_transport_class() {
    let typed = [
        (
            409,
            &br#"{"refused":"cursor_beyond_tail"}"#[..],
            "CursorBeyondTail",
        ),
        (
            409,
            br#"{"refused":"changed_incarnation"}"#,
            "ChangedIncarnation",
        ),
        (404, br#"{"refused":"missing"}"#, "Missing"),
        (410, br#"{"refused":"gone"}"#, "Gone"),
        // The verdict wins over the status it travelled with.
        (
            500,
            br#"{"refused":"cursor_beyond_tail"}"#,
            "CursorBeyondTail",
        ),
    ];
    for (status, body, verdict) in typed {
        assert_eq!(format!("{:?}", peer_refusal(status, body)), verdict);
    }
    let untyped = [
        (401, &b""[..], "Remote(Unauthorized)"),
        (404, br#"{"error":{"code":"not_found"}}"#, "Missing"),
        (410, b"", "Gone"),
        (429, b"", "Remote(Retryable { status: 429, code: None })"),
        (
            503,
            br#"{"error":{"code":"creating"}}"#,
            "Remote(Retryable { status: 503, code: None })",
        ),
        // A bare 409 keeps its historical meaning; the envelope's code is
        // never read, so a public cursor_beyond_tail envelope is NOT the
        // typed verdict.
        (
            409,
            br#"{"error":{"code":"cursor_beyond_tail","message":"x"}}"#,
            "ChangedIncarnation",
        ),
        (
            409,
            br#"{"error":{"code":"target_mismatch"}}"#,
            "ChangedIncarnation",
        ),
        (409, b"", "ChangedIncarnation"),
        (
            500,
            b"",
            "Remote(InvalidResponse(\"read peer status 500\"))",
        ),
    ];
    for (status, body, class) in untyped {
        assert_eq!(format!("{:?}", peer_refusal(status, body)), class);
    }
}
