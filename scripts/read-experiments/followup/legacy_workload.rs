// Identical local workload injected into the reviewed and revised sources.
__IMPORTS__
use std::time::{Duration, Instant};
use std::sync::atomic::Ordering::Relaxed;

fn samples() -> usize { std::env::var("LOCAL_PERF_READS").ok().map(|s| s.parse().unwrap()).unwrap_or(128) }
fn emit(kind: &str, name: &str, latencies: Vec<u64>, mut metrics: serde_json::Value) {
    let raw_order = latencies.clone();
    let mut latencies = latencies;
    latencies.sort_unstable();
    let len = latencies.len();
    metrics["kind"] = kind.into();
    metrics["name"] = name.into();
    metrics["requests"] = len.into();
    metrics["p50_us"] = latencies[(len - 1) / 2].into();
    metrics["p95_us"] = latencies[((len - 1) * 95) / 100].into();
    metrics["p99_us"] = latencies[((len - 1) * 99) / 100].into();
    metrics["elapsed_sum_us"] = latencies.iter().sum::<u64>().into();
    metrics["latencies_us"] = serde_json::to_value(raw_order).unwrap();
    println!("LOCAL_PERF {}", metrics);
}

#[ignore = "outside-checkout bounded review measurement"]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn local_perf_append() {
    let store = FaultStore::uniform(mem(), 107, FaultPlan::new(0, 0, 0));
    let (state, addr) = http_rig(store).await;
    for size in [1024usize, 65536] {
        let body = vec![0x5au8; size];
        for product in [false, true] {
            let name = format!("local-perf-{}-{size}", if product { "product" } else { "raw" });
            let (create_path, append_path, key_header, create_body): (String, String, &str, &[u8]) = if product {
                (format!("/v1/streams/{name}"), format!("/v1/streams/{name}/records"),
                 "prisma-encryption-key", br#"{"format":{"kind":"bytes"}}"#)
            } else {
                (format!("/v1/stream/{name}"), format!("/v1/stream/{name}"),
                 "stream-encryption-key", b"")
            };
            let headers = [(key_header, PRISMA_KEY), ("content-type", "application/octet-stream")];
            let (status, _, response) = preq(addr, "PUT", &create_path, &headers, create_body).await;
            assert_eq!(status, 201, "{}", String::from_utf8_lossy(&response));
            for _ in 0..16 {
                let (status, _, response) = preq(addr, "POST", &append_path, &headers, &body).await;
                assert!(matches!(status, 200 | 204), "{status}: {}", String::from_utf8_lossy(&response));
            }
            let mut latencies = Vec::with_capacity(128);
            crate::local_perf_meter::begin(size);
            for _ in 0..128 {
                let started = Instant::now();
                let (status, _, response) = preq(addr, "POST", &append_path, &headers, &body).await;
                latencies.push(started.elapsed().as_micros() as u64);
                assert!(matches!(status, 200 | 204), "{status}: {}", String::from_utf8_lossy(&response));
            }
            let mut metrics = crate::local_perf_meter::finish();
            metrics["payload_bytes_per_request"] = size.into();
            metrics["payload_bytes_total"] = (size * 128).into();
            emit("append", &name, latencies, metrics);
            if product {
                // Seed an exact, complete 64KiB page independently of the
                // append measurements. maxBytes includes record framing.
                let replay_name = format!("{name}-replay");
                let replay_create = format!("/v1/streams/{replay_name}");
                let replay_append = format!("{replay_create}/records");
                let (status, _, response) = preq(addr, "PUT", &replay_create, &headers, create_body).await;
                assert_eq!(status, 201, "{}", String::from_utf8_lossy(&response));
                for _ in 0..65536 / size {
                    let (status, _, response) = preq(addr, "POST", &replay_append, &headers, &body).await;
                    assert!(matches!(status, 200 | 204), "{status}: {}", String::from_utf8_lossy(&response));
                }
                let read_path = format!("{replay_append}?maxBytes=131072");
                for _ in 0..16 {
                    let (status, _, response) = preq(addr, "GET", &read_path, &headers, b"").await;
                    assert_eq!(status, 200);
                    assert_eq!(response.len(), 65536);
                }
                let desc = state.registry.get(&state.deployment.raw_adapter_sref(&replay_name)).await.unwrap().unwrap();
                let epoch = desc.epoch_bytes().unwrap();
                let selector = crate::crypto::stream_hash("");
                let mut latencies = Vec::with_capacity(samples());
                crate::local_perf_meter::begin(65536);
                for _ in 0..samples() {
                    let started = Instant::now();
                    let (status, reply_headers, response) = preq(addr, "GET", &read_path, &headers, b"").await;
                    latencies.push(started.elapsed().as_micros() as u64);
                    assert_eq!(status, 200);
                    assert_eq!(response.len(), 65536);
                    assert!(response.iter().all(|byte| *byte == 0x5a));
                    let cursor = crate::product_cursor::KeyCursor::decode(
                        reply_headers.get("prisma-next-cursor").expect("complete read cursor"),
                        &desc.project_id, &skey(), &epoch, &selector).unwrap();
                    assert_eq!((cursor.seg_id, cursor.offset), (0, (65536 / size) as u64));
                    assert_eq!(reply_headers.get("prisma-up-to-date").map(String::as_str), Some("true"));
                }
                let mut metrics = crate::local_perf_meter::finish();
                metrics["payload_bytes_per_request"] = 65536usize.into();
                metrics["record_payload_bytes"] = size.into();
                emit("product-replay", &name, latencies, metrics);
            }
        }
    }
    engine_shutdown(&state).await;
}

