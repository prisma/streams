#![cfg(test)]

use super::tests::state;
use super::{AppState, handle};
use axum::body::{Body, to_bytes};
use axum::extract::State;
use axum::http::{HeaderMap, Method, Request, StatusCode};
use axum::response::Response;
use futures_util::FutureExt;
use std::sync::Arc;
use std::time::Duration;

async fn send(
    state: &Arc<AppState>,
    method: Method,
    uri: &str,
    headers: HeaderMap,
    body: Body,
) -> Response {
    let mut request = Request::builder()
        .method(method)
        .uri(uri)
        .body(body)
        .unwrap();
    *request.headers_mut() = headers;
    handle(State(state.clone()), request).await
}

async fn text(response: Response) -> String {
    String::from_utf8(
        to_bytes(response.into_body(), 64 * 1024)
            .await
            .unwrap()
            .to_vec(),
    )
    .unwrap()
}

#[tokio::test]
async fn conditional_object_writes_keep_the_original_cas_result() {
    let state = state(Duration::ZERO);
    let mut headers = HeaderMap::new();
    headers.insert("if-none-match", "*".parse().unwrap());
    let created = send(
        &state,
        Method::PUT,
        "/b/key",
        headers.clone(),
        Body::from("first"),
    )
    .await;
    assert_eq!(created.status(), StatusCode::OK);
    let first_etag = created.headers()["etag"].clone();
    let rejected = send(&state, Method::PUT, "/b/key", headers, Body::from("wrong")).await;
    assert_eq!(rejected.status(), StatusCode::PRECONDITION_FAILED);
    let mut headers = HeaderMap::new();
    headers.insert("if-match", "wrong".parse().unwrap());
    assert_eq!(
        send(
            &state,
            Method::PUT,
            "/b/key",
            headers.clone(),
            Body::from("wrong")
        )
        .await
        .status(),
        StatusCode::PRECONDITION_FAILED
    );
    headers.insert("if-match", first_etag.clone());
    let replaced = send(&state, Method::PUT, "/b/key", headers, Body::from("second")).await;
    assert_eq!(replaced.status(), StatusCode::OK);
    assert_ne!(replaced.headers()["etag"], first_etag);
    assert_eq!(
        text(
            send(
                &state,
                Method::GET,
                "/b/key",
                HeaderMap::new(),
                Body::empty()
            )
            .await
        )
        .await,
        "second"
    );
}

#[tokio::test]
async fn object_listing_and_escaped_batch_deletion_keep_their_keys() {
    let state = state(Duration::ZERO);
    for key in ["dir/a", "dir/b", "dir/c%26d"] {
        assert_eq!(
            send(
                &state,
                Method::PUT,
                &format!("/b/{key}"),
                HeaderMap::new(),
                Body::from("data")
            )
            .await
            .status(),
            StatusCode::OK
        );
    }
    let page = text(
        send(
            &state,
            Method::GET,
            "/b?prefix=dir%2F&max-keys=1",
            HeaderMap::new(),
            Body::empty(),
        )
        .await,
    )
    .await;
    assert!(page.contains("<Key>dir/a</Key>"));
    assert!(page.contains("<NextContinuationToken>dir/a</NextContinuationToken>"));
    assert!(!page.contains("<Key>dir/b</Key>"));
    let page = text(
        send(
            &state,
            Method::GET,
            "/b?prefix=dir%2F&continuation-token=dir%2Fa",
            HeaderMap::new(),
            Body::empty(),
        )
        .await,
    )
    .await;
    assert!(page.contains("<Key>dir/b</Key>"));
    assert!(page.contains("<Key>dir/c&amp;d</Key>"));
    let deleted = send(
        &state,
        Method::POST,
        "/b?delete",
        HeaderMap::new(),
        Body::from("<Delete><Object><Key>dir/c&amp;d</Key></Object></Delete>"),
    )
    .await;
    assert_eq!(deleted.status(), StatusCode::OK);
    assert_eq!(
        send(
            &state,
            Method::GET,
            "/b/dir/c%26d",
            HeaderMap::new(),
            Body::empty()
        )
        .await
        .status(),
        StatusCode::NOT_FOUND
    );
}

#[tokio::test]
async fn multipart_assembly_orders_parts_and_consumes_the_upload() {
    let state = state(Duration::ZERO);
    let created = text(
        send(
            &state,
            Method::POST,
            "/b/key?uploads",
            HeaderMap::new(),
            Body::empty(),
        )
        .await,
    )
    .await;
    assert!(created.contains("<UploadId>u1</UploadId>"));
    for (number, body) in [(2, "second"), (1, "first")] {
        let part = send(
            &state,
            Method::PUT,
            &format!("/b/key?uploadId=u1&partNumber={number}"),
            HeaderMap::new(),
            Body::from(body),
        )
        .await;
        assert_eq!(part.status(), StatusCode::OK);
        assert!(part.headers().contains_key("etag"));
    }
    let completed = send(
        &state,
        Method::POST,
        "/b/key?uploadId=u1",
        HeaderMap::new(),
        Body::empty(),
    )
    .await;
    assert_eq!(completed.status(), StatusCode::OK);
    assert!(
        text(completed)
            .await
            .contains("<CompleteMultipartUploadResult>")
    );
    assert_eq!(
        text(
            send(
                &state,
                Method::GET,
                "/b/key",
                HeaderMap::new(),
                Body::empty()
            )
            .await
        )
        .await,
        "firstsecond"
    );
    assert_eq!(
        send(
            &state,
            Method::POST,
            "/b/key?uploadId=u1",
            HeaderMap::new(),
            Body::empty()
        )
        .await
        .status(),
        StatusCode::NOT_FOUND
    );
}

#[tokio::test]
async fn multipart_abort_and_object_delete_remove_the_correct_state() {
    let state = state(Duration::ZERO);
    assert_eq!(
        send(
            &state,
            Method::POST,
            "/b/key?uploads",
            HeaderMap::new(),
            Body::empty()
        )
        .await
        .status(),
        StatusCode::OK
    );
    assert_eq!(
        send(
            &state,
            Method::DELETE,
            "/b/key?uploadId=u1",
            HeaderMap::new(),
            Body::empty()
        )
        .await
        .status(),
        StatusCode::NO_CONTENT
    );
    assert_eq!(
        send(
            &state,
            Method::PUT,
            "/b/key?uploadId=u1&partNumber=1",
            HeaderMap::new(),
            Body::from("part")
        )
        .await
        .status(),
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        send(
            &state,
            Method::PUT,
            "/b/key",
            HeaderMap::new(),
            Body::from("data")
        )
        .await
        .status(),
        StatusCode::OK
    );
    assert_eq!(
        send(
            &state,
            Method::DELETE,
            "/b/key",
            HeaderMap::new(),
            Body::empty()
        )
        .await
        .status(),
        StatusCode::NO_CONTENT
    );
    assert_eq!(
        send(
            &state,
            Method::HEAD,
            "/b/key",
            HeaderMap::new(),
            Body::empty()
        )
        .await
        .status(),
        StatusCode::NOT_FOUND
    );
}

#[tokio::test]
async fn a_failed_body_is_counted_without_publishing_an_object() {
    let state = state(Duration::ZERO);
    let failed = Body::from_stream(futures_util::stream::once(async {
        Err::<bytes::Bytes, _>(std::io::Error::other("injected body failure"))
    }));
    assert_eq!(
        send(
            &state,
            Method::PUT,
            "/b/shards/00/wal/1",
            HeaderMap::new(),
            failed
        )
        .await
        .status(),
        StatusCode::BAD_REQUEST
    );
    assert!(state.objects.lock().unwrap().is_empty());
    let costs = state.stats.detailed_snapshot();
    assert_eq!(
        costs["cells"]["shard/wal/put"],
        serde_json::json!({"4xx": 1})
    );
    assert_eq!(
        costs["total"],
        serde_json::json!({"class_a": 0, "class_b": 0, "free": 1})
    );
}

#[tokio::test]
async fn poisoned_storage_operations_cannot_acknowledge_success() {
    let state = state(Duration::ZERO);
    assert!(
        std::panic::catch_unwind(|| {
            let _held = state.objects.lock().unwrap();
            panic!("interrupt object update");
        })
        .is_err()
    );
    assert!(
        std::panic::AssertUnwindSafe(send(
            &state,
            Method::PUT,
            "/b/key",
            HeaderMap::new(),
            Body::from("data")
        ))
        .catch_unwind()
        .await
        .is_err()
    );
    assert!(
        std::panic::catch_unwind(|| {
            let _held = state.uploads.lock().unwrap();
            panic!("interrupt upload update");
        })
        .is_err()
    );
    assert!(
        std::panic::AssertUnwindSafe(send(
            &state,
            Method::POST,
            "/b/key?uploads",
            HeaderMap::new(),
            Body::empty()
        ))
        .catch_unwind()
        .await
        .is_err()
    );
}
