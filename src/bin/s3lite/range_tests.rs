#![cfg(test)]

use super::{AppState, Stats, get_object, put_object};
use axum::body::{Body, to_bytes};
use axum::http::{HeaderMap, StatusCode};
use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::AtomicU64;
use std::sync::{Arc, Mutex};
use std::time::Duration;

async fn populated(discard_substr: Option<&str>) -> Arc<AppState> {
    let state = Arc::new(AppState {
        latency: Duration::ZERO,
        discard_substr: discard_substr.map(str::to_string),
        objects: Mutex::new(BTreeMap::new()),
        uploads: Mutex::new(HashMap::new()),
        etag_counter: AtomicU64::new(1),
        upload_counter: AtomicU64::new(1),
        stats: Stats::default(),
    });
    assert_eq!(
        put_object(
            &state,
            "b/history/data.sst",
            &HeaderMap::new(),
            Body::from("abcdefg")
        )
        .await
        .status(),
        StatusCode::OK
    );
    state
}

#[tokio::test]
async fn ranged_head_of_discarded_body_uses_retained_metadata() {
    let state = populated(Some("history")).await;
    let mut headers = HeaderMap::new();
    headers.insert("range", "bytes=2-5".parse().unwrap());
    let response = get_object(&state, "b/history/data.sst", &headers, true);
    assert_eq!(response.status(), StatusCode::PARTIAL_CONTENT);
    assert_eq!(response.headers()["content-length"], "4");
    assert_eq!(response.headers()["content-range"], "bytes 2-5/7");
    assert!(
        to_bytes(response.into_body(), 1024)
            .await
            .unwrap()
            .is_empty()
    );
    let response = get_object(&state, "b/history/data.sst", &HeaderMap::new(), true);
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers()["content-length"], "7");
    assert_eq!(
        get_object(&state, "b/history/data.sst", &headers, false).status(),
        StatusCode::INTERNAL_SERVER_ERROR
    );
}

#[tokio::test]
async fn zero_suffix_follows_the_emulators_invalid_range_fallback() {
    let state = populated(None).await;
    let mut headers = HeaderMap::new();
    headers.insert("range", "bytes=-0".parse().unwrap());
    let response = get_object(&state, "b/history/data.sst", &headers, false);
    assert_eq!(response.status(), StatusCode::OK);
    assert!(!response.headers().contains_key("content-range"));
    assert_eq!(
        to_bytes(response.into_body(), 1024).await.unwrap(),
        "abcdefg"
    );
}

#[tokio::test]
async fn get_ranges_keep_the_exact_boundaries_and_full_response_fallback() {
    let state = populated(None).await;
    for (raw, expected_status, expected_body, content_range) in [
        ("bytes=0-0", 206, "a", Some("bytes 0-0/7")),
        ("bytes=2-5", 206, "cdef", Some("bytes 2-5/7")),
        ("bytes=5-", 206, "fg", Some("bytes 5-6/7")),
        ("bytes=-3", 206, "efg", Some("bytes 4-6/7")),
        (
            "bytes=-18446744073709551615",
            206,
            "abcdefg",
            Some("bytes 0-6/7"),
        ),
        (
            "bytes=2-18446744073709551615",
            206,
            "cdefg",
            Some("bytes 2-6/7"),
        ),
        ("bytes=7-", 200, "abcdefg", None),
        ("bytes=5-2", 200, "abcdefg", None),
        ("not-a-range", 200, "abcdefg", None),
    ] {
        let mut headers = HeaderMap::new();
        headers.insert("range", raw.parse().unwrap());
        let response = get_object(&state, "b/history/data.sst", &headers, false);
        assert_eq!(response.status().as_u16(), expected_status, "{raw}");
        assert_eq!(
            response.headers()["content-length"],
            expected_body.len().to_string(),
            "{raw}"
        );
        assert_eq!(
            response
                .headers()
                .get("content-range")
                .map(|h| h.to_str().unwrap()),
            content_range,
            "{raw}"
        );
        assert_eq!(
            to_bytes(response.into_body(), 1024).await.unwrap(),
            expected_body,
            "{raw}"
        );
    }
}

proptest::proptest! {
    #[test]
    fn parsed_ranges_are_nonempty_valid_slices(
        total in 0usize..4096,
        start in 0u64..8192,
        end in 0u64..8192,
        suffix in 0u64..8192,
    ) {
        let backing = vec![0u8; total];
        for raw in [format!("bytes={start}-{end}"), format!("bytes={start}-"), format!("bytes=-{suffix}")] {
            if let Some(range) = super::parse_range(&raw, total) {
                proptest::prop_assert!(!range.is_empty());
                proptest::prop_assert!(backing.get(range).is_some());
            }
        }
    }
}
