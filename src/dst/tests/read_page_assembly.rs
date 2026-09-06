//! R06-A first-record allowances belong to a response, not to every segment.
use super::fixture_http::{engine_shutdown, http_rig};
use super::fixture_livefeed::split_and_await;
use super::fixture_requests::{PRISMA_KEY, hreq, preq};
use super::fixture_storage::{mem, skey};
use crate::application::read::{ReadCommand, ReadMode, ReadPosition, ReadStart};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn r06a_fork_page_keeps_the_child_record_withheld_after_ancestor_bytes() {
    let (state, addr) = http_rig(mem()).await;
    let payload = format!("\"{}\"", "f".repeat((6 << 10) - 2));
    let body = format!("[{payload}]");
    let content = [("content-type", "application/json")];
    assert_eq!(
        hreq(
            addr,
            "PUT",
            "/v1/stream/budget-parent",
            &content,
            body.as_bytes()
        )
        .await
        .0,
        201
    );
    let (_, headers, _) = hreq(addr, "GET", "/v1/stream/budget-parent", &[], b"").await;
    let boundary = headers.get("stream-next-offset").unwrap();
    assert_eq!(
        hreq(
            addr,
            "PUT",
            "/v1/stream/budget-child",
            &[
                ("content-type", "application/json"),
                ("stream-forked-from", "budget-parent"),
                ("stream-fork-offset", boundary)
            ],
            b""
        )
        .await
        .0,
        201
    );
    assert_eq!(
        hreq(
            addr,
            "POST",
            "/v1/stream/budget-child",
            &content,
            body.as_bytes()
        )
        .await
        .0,
        204
    );
    let desc = state
        .registry
        .get(&state.deployment.raw_adapter_sref("budget-child"))
        .await
        .unwrap()
        .unwrap();
    let command = |after| ReadCommand {
        descriptor: desc.clone(),
        key: Some(skey()),
        start: ReadStart::Position(ReadPosition { segment: 0, after }),
        selector: Some(String::new()),
        mode: ReadMode::Replay,
        visibility: crate::shard::Deliver::Durable,
        max_bytes: 8 << 10,
        tail_max_bytes: 8 << 10,
        allow_remote: true,
        refresh: false,
    };
    for after in [0, 1] {
        let page = state
            .read_service()
            .execute_read(command(after))
            .await
            .unwrap();
        assert_eq!(page.records.len(), 1);
        assert_eq!(page.records[0].off, after);
        assert_eq!(page.records[0].payload.as_ref(), payload.as_bytes());
        assert_eq!(page.next.after, after + 1);
        assert_eq!(page.up_to_date, after == 1);
    }
    engine_shutdown(&state).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn r06a_collection_scan_shares_one_budget_across_frozen_segments() {
    use crate::application::read_scan::ScanCommand;
    let (state, addr) = http_rig(mem()).await;
    let headers = [("prisma-encryption-key", PRISMA_KEY)];
    assert_eq!(
        preq(
            addr,
            "PUT",
            "/v1/streams/budget-scan",
            &headers,
            br#"{"format":{"kind":"json"}}"#
        )
        .await
        .0,
        201
    );
    let payload = format!("\"{}\"", "s".repeat((6 << 10) - 2));
    assert_eq!(
        preq(
            addr,
            "POST",
            "/v1/streams/budget-scan/records",
            &headers,
            payload.as_bytes()
        )
        .await
        .0,
        200
    );
    split_and_await(&state, "budget-scan", 0).await;
    assert_eq!(
        preq(
            addr,
            "POST",
            "/v1/streams/budget-scan/records",
            &headers,
            payload.as_bytes()
        )
        .await
        .0,
        200
    );
    let desc = state
        .registry
        .get(&state.deployment.raw_adapter_sref("budget-scan"))
        .await
        .unwrap()
        .unwrap();
    let mut cursor = None;
    for index in 0..2 {
        let page = state
            .read_service()
            .execute_scan(ScanCommand {
                descriptor: desc.clone(),
                key: skey(),
                cursor,
                max_bytes: 8 << 10,
                now_ms: crate::shard::now_ms(),
                lifetime_ms: 60_000,
            })
            .await
            .unwrap();
        assert_eq!(page.records.len(), 1);
        assert_eq!(page.records[0].payload.as_ref(), payload.as_bytes());
        assert_eq!(page.continuation.is_none(), index == 1);
        cursor = page.continuation;
    }
    engine_shutdown(&state).await;
}
