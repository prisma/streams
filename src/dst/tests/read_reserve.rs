//! Read reservation (tuning P1 item 2, the habanero 10M-streams failure): a
//! page read reserves a small initial hold and grows it as its page fetches
//! chunks, so cold reads of tiny streams waiting on the store do not each
//! hold a whole default page of the instance's read memory.

use super::fixture_cell::{Cell, CellSpec, open_cell, project};
use super::fixture_http::engine_shutdown;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use object_store::ObjectStore;
use object_store::path::Path as ObjPath;

const MIB: u64 = 1 << 20;

/// A memory store whose GETs wait `SLOW_GET` once `slow` is set: the cold
/// reads' descriptor and chunk GETs hold their reservations that long.
#[derive(Debug)]
struct SlowGets {
    inner: Arc<dyn ObjectStore>,
    slow: AtomicBool,
}

const SLOW_GET: Duration = Duration::from_millis(100);

impl std::fmt::Display for SlowGets {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("SlowGets")
    }
}

#[async_trait::async_trait]
impl ObjectStore for SlowGets {
    async fn put_opts(
        &self,
        location: &ObjPath,
        payload: object_store::PutPayload,
        opts: object_store::PutOptions,
    ) -> object_store::Result<object_store::PutResult> {
        self.inner.put_opts(location, payload, opts).await
    }
    async fn put_multipart_opts(
        &self,
        location: &ObjPath,
        opts: object_store::PutMultipartOptions,
    ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }
    async fn get_opts(
        &self,
        location: &ObjPath,
        options: object_store::GetOptions,
    ) -> object_store::Result<object_store::GetResult> {
        if self.slow.load(Ordering::Relaxed) {
            tokio::time::sleep(SLOW_GET).await;
        }
        self.inner.get_opts(location, options).await
    }
    fn delete_stream(
        &self,
        locations: futures_util::stream::BoxStream<'static, object_store::Result<ObjPath>>,
    ) -> futures_util::stream::BoxStream<'static, object_store::Result<ObjPath>> {
        self.inner.delete_stream(locations)
    }
    fn list(
        &self,
        prefix: Option<&ObjPath>,
    ) -> futures_util::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>>
    {
        self.inner.list(prefix)
    }
    async fn list_with_delimiter(
        &self,
        prefix: Option<&ObjPath>,
    ) -> object_store::Result<object_store::ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }
    async fn copy_opts(
        &self,
        from: &ObjPath,
        to: &ObjPath,
        options: object_store::CopyOptions,
    ) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, options).await
    }
}

/// Project 0's JSON stream `name` holding one 400-byte record.
async fn tiny_stream(cell: &Cell, name: &str) {
    let path = format!("/v1/streams/{name}");
    let created = cell
        .call(0, "PUT", &path, br#"{"format":{"kind":"json"}}"#)
        .await;
    assert_eq!(created.0, 201, "create {name}");
    let record = format!(r#"[{{"pad":"{}"}}]"#, "x".repeat(388));
    let appended = cell
        .call(
            0,
            "POST",
            &format!("{path}/records:batch"),
            record.as_bytes(),
        )
        .await;
    assert_eq!(appended.0, 200, "append {name}");
}

/// Every descriptor of `names` out of the hot cache: the next read of each
/// goes to the store for it.
fn cold(cell: &Cell, names: &[String]) {
    let registry = &cell.state.registry;
    for name in names {
        let sref = crate::tenant::TenantStreamRef::new(
            crate::tenant::ProjectId::new(&project(0)).unwrap(),
            crate::tenant::CanonicalStreamName::new(name).unwrap(),
        );
        registry.expire_cache_for_tests(&sref);
    }
    registry.sweep_cache();
}

/// 200 cold reads of 50 tiny streams at once, none naming `maxBytes`, on a
/// dedicated cell with 16 MiB of read memory and a store answering each
/// GET after 100 ms: every read is served its one record, none is refused
/// 503 `read_memory_busy`. Before: each read reserved the whole default
/// page (8 MiB), so two waited on the store at a time and the rest were
/// refused after their 2 s wait.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn two_hundred_cold_reads_of_tiny_streams_are_all_served() {
    let store = Arc::new(SlowGets {
        inner: Arc::new(object_store::memory::InMemory::new()),
        slow: AtomicBool::new(false),
    });
    let mut spec = CellSpec::open(1);
    spec.store = store.clone();
    let cell = open_cell(spec).await;
    cell.state.admission.set_read_memory_capacity(16 * MIB);
    let names: Vec<String> = (0..50).map(|i| format!("tiny{i:02}")).collect();
    for name in &names {
        tiny_stream(&cell, name).await;
    }
    cold(&cell, &names);
    store.slow.store(true, Ordering::Relaxed);
    let reads = (0..200).map(|i| {
        let path = format!("/v1/streams/{}/records", names[i % names.len()]);
        let cell = &cell;
        async move { cell.call(0, "GET", &path, b"").await }
    });
    let reads = futures_util::future::join_all(reads).await;
    let mut statuses = std::collections::BTreeMap::new();
    for (status, _, page) in reads {
        if status == 200 {
            let page: serde_json::Value = serde_json::from_slice(&page).unwrap();
            assert_eq!(page.as_array().map(Vec::len), Some(1), "one record");
        }
        *statuses.entry(status).or_insert(0) += 1;
    }
    assert_eq!(statuses, [(200u16, 200usize)].into(), "every read served");
    store.slow.store(false, Ordering::Relaxed);
    engine_shutdown(&cell.state).await;
}

/// Record `n`: a JSON object of 64 KiB and a few bytes.
fn padded(n: usize) -> String {
    format!(r#"{{"n":{n},"pad":"{}"}}"#, "x".repeat(64 * 1024))
}

/// Under read-memory pressure a read is served a shorter page, never
/// refused: with the instance's read memory at 512 KiB, a read without
/// `maxBytes` of a stream of 32 records of 64 KiB (2 MiB) answers 200 with
/// its first records in order, at least one and no more than the read
/// memory holds, its cursor past the last; reading on from each cursor
/// serves the rest in order, and nothing stays held. Before: the read
/// reserved 8 MiB (one read always fits) and was served whole, 2 MiB in a
/// read memory of 512 KiB.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_read_under_pressure_is_served_a_shorter_page_and_continues_from_its_cursor() {
    let cell = open_cell(CellSpec::open(1)).await;
    let created = cell
        .call(
            0,
            "PUT",
            "/v1/streams/two",
            br#"{"format":{"kind":"json"}}"#,
        )
        .await;
    assert_eq!(created.0, 201);
    let records: Vec<String> = (0..32).map(padded).collect();
    let batch = format!("[{}]", records.join(","));
    let appended = cell
        .call(0, "POST", "/v1/streams/two/records:batch", batch.as_bytes())
        .await;
    assert_eq!(appended.0, 200);
    let capacity = 512 * 1024;
    cell.state.admission.set_read_memory_capacity(capacity);
    let (mut next, mut pages, mut path) =
        (0usize, Vec::new(), "/v1/streams/two/records".to_string());
    while next < 32 && pages.len() < 32 {
        let (status, headers, body) = cell.call(0, "GET", &path, b"").await;
        assert_eq!(status, 200, "page {}", pages.len());
        let page: Vec<serde_json::Value> = serde_json::from_slice(&body).unwrap();
        let want: Vec<serde_json::Value> = (next..next + page.len())
            .map(|n| serde_json::from_str(&padded(n)).unwrap())
            .collect();
        assert_eq!(
            page,
            want,
            "page {} serves the next records in order",
            pages.len()
        );
        assert!(!page.is_empty(), "every page makes progress");
        assert!(
            body.len() as u64 <= capacity,
            "page {} of {} B in a {capacity} B read memory",
            pages.len(),
            body.len()
        );
        next += page.len();
        pages.push(page.len());
        let cursor = headers
            .get("streams-next-cursor")
            .expect("a cursor")
            .clone();
        path = format!("/v1/streams/two/records?cursor={cursor}");
    }
    assert_eq!(next, 32, "every record served once: pages {pages:?}");
    assert!(
        pages.len() >= 4,
        "shorter pages than the 2 MiB stream: {pages:?}"
    );
    assert_eq!(cell.state.admission.read_memory().0, 0, "nothing held");
    engine_shutdown(&cell.state).await;
}

/// Record `n` of 64 KiB that does not compress: a pseudo-random text pad.
fn noisy(n: usize) -> String {
    let mut x = (n as u64 + 1).wrapping_mul(0x9E37_79B9_7F4A_7C15);
    let pad: String = (0..64 * 1024)
        .map(|_| {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            char::from(b'a' + (x % 26) as u8)
        })
        .collect();
    format!(r#"{{"n":{n},"pad":"{pad}"}}"#)
}

/// Project 0's stream `name`, its records sealed into a segment object:
/// `records` incompressible records of 64 KiB appended in one batch, then
/// a requested seal, until the stream's every durable record is sealed.
async fn sealed_stream(cell: &Cell, name: &str, records: usize) {
    let path = format!("/v1/streams/{name}");
    let created = cell
        .call(0, "PUT", &path, br#"{"format":{"kind":"json"}}"#)
        .await;
    assert_eq!(created.0, 201);
    let batch: Vec<String> = (0..records).map(noisy).collect();
    let batch = format!("[{}]", batch.join(","));
    let appended = cell
        .call(
            0,
            "POST",
            &format!("{path}/records:batch"),
            batch.as_bytes(),
        )
        .await;
    assert_eq!(appended.0, 200);
    let sref = crate::tenant::TenantStreamRef::new(
        crate::tenant::ProjectId::new(&project(0)).unwrap(),
        crate::tenant::CanonicalStreamName::new(name).unwrap(),
    );
    let hash = cell
        .state
        .registry
        .get(&sref)
        .await
        .unwrap()
        .expect("live")
        .storage_hash();
    for engine in cell.state.shards.engines() {
        engine
            .seal_once(crate::shard::seal::Trigger::Requested)
            .await
            .expect("seal");
        super::fixture_storage::wait_all_sealed(&engine, &[hash]).await;
    }
}

/// A read's first sealed chunk is bounded by the read memory as admission
/// is (Fable 5.1's condition on #240): with 1 MiB of read memory, 16
/// concurrent reads without `maxBytes` of a stream whose first sealed
/// chunk is over 512 KiB (16 records of 64 KiB of random letters, about
/// 630 KB stored) wait for room for it and are then served or refused 503
/// `read_memory_busy`, never more: the read memory peaks at no more than 1
/// MiB and 512 KiB, every answer is 200 or that 503, and nothing stays
/// held. Before: every admitted read's first chunk was claimed whatever the
/// room, so four reads admitted at 256 KiB each held about 2.5 MB.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_reads_first_sealed_chunk_waits_for_the_read_memory_and_is_refused_as_admission_is() {
    let mut spec = CellSpec::open(1);
    // No tail ring: every read of the sealed records fetches their chunk.
    spec.cli = |c| c.tail_ring_bytes = 0;
    let cell = open_cell(spec).await;
    sealed_stream(&cell, "sealed", 16).await;
    let capacity = MIB;
    cell.state.admission.set_read_memory_capacity(capacity);
    cell.state.registry.sweep_cache();
    let peak = std::sync::atomic::AtomicU64::new(0);
    let done = AtomicBool::new(false);
    let sample = async {
        while !done.load(Ordering::Relaxed) {
            peak.fetch_max(cell.state.admission.read_memory().0, Ordering::Relaxed);
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    };
    let reads = async {
        let reads = (0..16).map(|_| cell.call(0, "GET", "/v1/streams/sealed/records", b""));
        let answers = futures_util::future::join_all(reads).await;
        done.store(true, Ordering::Relaxed);
        answers
    };
    let (answers, ()) = futures_util::future::join(reads, sample).await;
    let mut statuses = std::collections::BTreeMap::new();
    for (status, _, body) in &answers {
        let code = (*status == 503)
            .then(|| serde_json::from_slice::<serde_json::Value>(body).ok())
            .flatten()
            .and_then(|v| v["error"]["code"].as_str().map(str::to_string));
        *statuses.entry((*status, code)).or_insert(0usize) += 1;
    }
    let peak = peak.load(Ordering::Relaxed);
    let allowed: Vec<_> = [
        (200u16, None),
        (503u16, Some("read_memory_busy".to_string())),
    ]
    .into();
    assert!(
        statuses.keys().all(|k| allowed.contains(k)) && statuses.values().sum::<usize>() == 16,
        "the answers: {statuses:?}"
    );
    assert!(
        peak <= capacity + 512 * 1024,
        "the read memory peaked at {peak} B against {capacity} B and one 512 KiB chunk; \
         the answers {statuses:?}"
    );
    assert_eq!(cell.state.admission.read_memory().0, 0, "nothing held");
    engine_shutdown(&cell.state).await;
}
