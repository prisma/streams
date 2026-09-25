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
        let refused = WireRefusedPage::of(&failure).expect("a stream verdict is typed");
        assert_eq!(
            serde_json::to_string(&refused).unwrap(),
            format!("{{\"refused\":\"{code}\"}}")
        );
        assert!(
            matches!(
                (refused.into_failure(), &failure),
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
    let replaced = ReadFailure::HistoryReplaced(crate::application::read::ReadPosition {
        segment: 2,
        after: 7,
    });
    let refused = WireRefusedPage::of(&replaced).expect("a replaced history is a verdict");
    assert_eq!(
        serde_json::to_string(&refused).unwrap(),
        r#"{"refused":"history_replaced","recover":{"segment":2,"after":7}}"#
    );
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
        (
            409,
            br#"{"refused":"history_replaced","recover":{"segment":2,"after":7}}"#,
            "HistoryReplaced(ReadPosition { segment: 2, after: 7 })",
        ),
        // A replaced history without its recovery position is malformed.
        (
            409,
            br#"{"refused":"history_replaced"}"#,
            "Remote(InvalidResponse(\"history_replaced without recover\"))",
        ),
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

/// A relayed page's continuation recovers at the page's own durable
/// position and fits its next position; an owner's page that breaks either
/// is malformed, whatever the other says.
#[test]
fn a_relayed_continuation_recovers_at_the_page_durable_position_and_fits_next() {
    use crate::application::read::{
        Continuation, ReadCommand, ReadMode, ReadPosition, ReadResultKind, ReadStart,
    };
    let descriptor = crate::sse::feed::tests::test_desc("relayed-continuation");
    let segment = descriptor.resolve_segment("").seg_id;
    let at = |after| ReadPosition { segment, after };
    let command = ReadCommand {
        descriptor: descriptor.clone(),
        key: None,
        start: ReadStart::Beginning,
        selector: None,
        mode: ReadMode::Replay,
        visibility: crate::shard::Deliver::Applied,
        max_bytes: 4096,
        tail_max_bytes: 4096,
        allow_remote: true,
        refresh: false,
    };
    // A page ending at `next` with durable position 1 whose continuation
    // recovers at `recover`.
    let page = |recover: u64, next: u64| WireReadPage {
        epoch: descriptor.stream_epoch.clone(),
        records: Vec::new(),
        next: at(next),
        durable: Some(at(1)),
        continuation: WireContinuation::Continues(Continuation::from_parts(
            [1; 16], recover, recover, [0; 16],
        )),
        pending_from: None,
        up_to_date: true,
        closed: false,
        kind: ReadResultKind::Data,
        segmented: false,
        identity: [0; 16],
        scan_from: 0,
        end: next,
    };
    let relayed = page(1, 3)
        .into_outcome(&command, segment)
        .unwrap_or_else(|error| panic!("a consistent continuation: {error:?}"));
    assert_eq!(
        relayed.continuation,
        Some(Continuation::from_parts([1; 16], 1, 1, [0; 16]))
    );
    for (recover, next, why) in [
        (0, 3, "it recovers below the page's durable position"),
        (1, 1, "it does not fit the page's next position"),
    ] {
        assert!(
            matches!(
                page(recover, next).into_outcome(&command, segment),
                Err(RemoteSpanError::InvalidResponse(_))
            ),
            "{why}"
        );
    }
}
