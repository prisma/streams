use super::*;

#[test]
fn r06a_wire_metadata_record_and_required_progress_limits() {
    let key = "\u{1}".repeat(u16::MAX as usize);
    let record = || WireRecord {
        off: 0,
        key: key.clone(),
        payload: "eA==".into(),
    };
    let wire = serde_json::to_vec(
        &serde_json::json!({"items": [record(), record()], "last":1,"end":3,"completed":false}),
    )
    .unwrap();
    assert!(wire.len() < max_wire_bytes());
    assert_eq!(scan_page(&wire, 10).unwrap().recs.len(), 2);
    let overflow = serde_json::to_vec(&serde_json::json!({"items": [record(), record(), record()], "last":2,"end":3,"completed":true})).unwrap();
    assert!(scan_page(&overflow, 10).is_err());
    let mut page = serde_json::json!({"items": [], "end":3, "completed":true});
    assert!(
        scan_page(&serde_json::to_vec(&page).unwrap(), 10).is_err(),
        "last is mandatory even when null"
    );
    page["last"] = serde_json::Value::Null;
    page["items"] = serde_json::json!(vec![
        serde_json::json!({"off":0,"rk":"","p":""});
        MAX_PAGE_RECORDS + 1
    ]);
    let error = scan_page(&serde_json::to_vec(&page).unwrap(), 10)
        .err()
        .unwrap();
    assert!(format!("{error:?}").contains("record limit"));
}

#[expect(
    clippy::disallowed_methods,
    reason = "r06a_actual_receiver_refuses_declared_wire_overflow_before_reading_body; the fixture spawns the one-shot server it releases and joins itself; a supervised spawn would tie the fixture's teardown to a supervisor it never builds"
)]
#[tokio::test]
async fn r06a_actual_receiver_refuses_declared_wire_overflow_before_reading_body() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (release_tx, release) = tokio::sync::oneshot::channel::<()>();
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut request = [0; 4096];
        let bytes = socket.read(&mut request).await.unwrap();
        assert!(
            bytes > 0,
            "the receiver must request the actual wire response"
        );
        socket
            .write_all(
                format!(
                    "HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\r\n",
                    max_wire_bytes() + 1
                )
                .as_bytes(),
            )
            .await
            .unwrap();
        release.await.unwrap();
    });
    let response = reqwest::get(format!("http://{addr}/")).await.unwrap();
    let result = tokio::time::timeout(
        std::time::Duration::from_secs(2),
        body(response.content_length(), response.bytes_stream()),
    )
    .await;
    assert!(
        result.unwrap().is_err(),
        "refuse before the held body arrives"
    );
    release_tx.send(()).unwrap();
    server.await.unwrap();
}
