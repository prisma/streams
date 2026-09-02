//! TraceStore's focused tests (PR 3.2.1: moved beside the subsystem
//! they pin; PR 4.1: split by behavior so no test file is over
//! budget). This module holds the shared fixtures; each behavior
//! module `use super::*`s them and reaches trace_store's private
//! internals through `super::super`.

use std::sync::Arc;

use object_store::path::Path as ObjPath;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, Result as OsResult,
};

mod basic;
mod cancellation;
mod concurrency;
mod delete_accounting;
mod streams;

pub(super) fn mem() -> Arc<dyn ObjectStore> {
    Arc::new(object_store::memory::InMemory::new())
}

// ---- delete_stream: pass-through with EXACTLY ONE delegated call ----

/// A scripted delete_stream store: counts invocations and consumed
/// inputs, returns a canned output stream. Used to prove the trace
/// layer neither fans the call out nor manufactures results.
#[derive(Debug)]
pub(super) struct DeleteSpy {
    inner: object_store::memory::InMemory,
    calls: std::sync::Arc<std::sync::atomic::AtomicU64>,
    consumed_inputs: std::sync::Arc<std::sync::atomic::AtomicU64>,
    // object_store::Error is not Clone, so scripted failures are
    // stored as their message and rebuilt at stream time.
    scripted_output: Vec<Result<ObjPath, String>>,
}

impl std::fmt::Display for DeleteSpy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "DeleteSpy({})", self.inner)
    }
}

#[async_trait::async_trait]
impl ObjectStore for DeleteSpy {
    async fn put_opts(
        &self,
        location: &ObjPath,
        payload: PutPayload,
        opts: PutOptions,
    ) -> OsResult<PutResult> {
        self.inner.put_opts(location, payload, opts).await
    }
    async fn put_multipart_opts(
        &self,
        location: &ObjPath,
        opts: PutMultipartOptions,
    ) -> OsResult<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }
    async fn get_opts(&self, location: &ObjPath, options: GetOptions) -> OsResult<GetResult> {
        self.inner.get_opts(location, options).await
    }
    fn list(
        &self,
        prefix: Option<&ObjPath>,
    ) -> futures_util::stream::BoxStream<'static, OsResult<ObjectMeta>> {
        self.inner.list(prefix)
    }
    async fn list_with_delimiter(&self, prefix: Option<&ObjPath>) -> OsResult<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }
    async fn copy_opts(&self, from: &ObjPath, to: &ObjPath, opts: CopyOptions) -> OsResult<()> {
        self.inner.copy_opts(from, to, opts).await
    }
    fn delete_stream(
        &self,
        locations: futures_util::stream::BoxStream<'static, OsResult<ObjPath>>,
    ) -> futures_util::stream::BoxStream<'static, OsResult<ObjPath>> {
        use futures_util::StreamExt;
        self.calls
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let consumed = self.consumed_inputs.clone();
        // Lazy pass-through, one pull of the input per output item:
        // each consumed Ok input counts and releases the next
        // scripted result (fewer scripted results than inputs = the
        // coalescing-store shape); an input error surfaces in place,
        // untouched; a dropped consumer stops driving the input.
        let scripted = self.scripted_output.clone().into_iter();
        futures_util::stream::unfold(
            (locations, scripted, consumed),
            |(mut locations, mut scripted, consumed)| async move {
                loop {
                    match locations.next().await {
                        Some(Ok(_)) => {
                            consumed.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                            if let Some(r) = scripted.next() {
                                let r = r.map_err(|msg| object_store::Error::Generic {
                                    store: "spy",
                                    source: msg.into(),
                                });
                                return Some((r, (locations, scripted, consumed)));
                            }
                            // Scripted output exhausted: drain on.
                        }
                        Some(Err(e)) => return Some((Err(e), (locations, scripted, consumed))),
                        None => return None,
                    }
                }
            },
        )
        .boxed()
    }
}

pub(super) fn spy(
    scripted: Vec<OsResult<ObjPath>>,
) -> (
    Arc<DeleteSpy>,
    std::sync::Arc<std::sync::atomic::AtomicU64>,
    std::sync::Arc<std::sync::atomic::AtomicU64>,
) {
    let calls = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
    let consumed = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
    (
        Arc::new(DeleteSpy {
            inner: object_store::memory::InMemory::new(),
            calls: calls.clone(),
            consumed_inputs: consumed.clone(),
            scripted_output: scripted
                .into_iter()
                .map(|r| r.map_err(|e| e.to_string()))
                .collect(),
        }),
        calls,
        consumed,
    )
}

pub(super) fn delete_err() -> object_store::Error {
    object_store::Error::Generic {
        store: "spy",
        source: "scripted delete failure".into(),
    }
}

// ---- PR 3.2: stream lifetime is distinct from observed outcome ----

/// A store whose list yields a scripted error FIRST, then delegates
/// to the real inner listing — the "stream keeps serving after an
/// error" case the ObjectStore trait explicitly allows.
#[derive(Debug)]
pub(super) struct ListSpy {
    inner: Arc<object_store::memory::InMemory>,
}

impl std::fmt::Display for ListSpy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ListSpy({})", self.inner)
    }
}

#[async_trait::async_trait]
impl ObjectStore for ListSpy {
    async fn put_opts(
        &self,
        location: &ObjPath,
        payload: PutPayload,
        opts: PutOptions,
    ) -> OsResult<PutResult> {
        self.inner.put_opts(location, payload, opts).await
    }
    async fn put_multipart_opts(
        &self,
        location: &ObjPath,
        opts: PutMultipartOptions,
    ) -> OsResult<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }
    async fn get_opts(&self, location: &ObjPath, options: GetOptions) -> OsResult<GetResult> {
        self.inner.get_opts(location, options).await
    }
    fn list(
        &self,
        prefix: Option<&ObjPath>,
    ) -> futures_util::stream::BoxStream<'static, OsResult<ObjectMeta>> {
        use futures_util::StreamExt;
        futures_util::stream::iter(vec![Err(object_store::Error::Generic {
            store: "list-spy",
            source: "scripted list failure".into(),
        })])
        .chain(self.inner.list(prefix))
        .boxed()
    }
    async fn list_with_delimiter(&self, prefix: Option<&ObjPath>) -> OsResult<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }
    async fn copy_opts(&self, from: &ObjPath, to: &ObjPath, opts: CopyOptions) -> OsResult<()> {
        self.inner.copy_opts(from, to, opts).await
    }
    fn delete_stream(
        &self,
        locations: futures_util::stream::BoxStream<'static, OsResult<ObjPath>>,
    ) -> futures_util::stream::BoxStream<'static, OsResult<ObjPath>> {
        self.inner.delete_stream(locations)
    }
}

pub(super) async fn list_spy_with_one_object() -> Arc<ListSpy> {
    let inner = Arc::new(object_store::memory::InMemory::new());
    inner
        .put_opts(
            &ObjPath::from("a/1.sst"),
            PutPayload::from(vec![1u8; 4]),
            PutOptions::default(),
        )
        .await
        .unwrap();
    Arc::new(ListSpy { inner })
}

// ---- PR 3.2.1: async cancellation is RAII-safe ------------------------

/// A store whose async operations never resolve — the fixture for
/// polling a traced future to `Pending` and then dropping it.
#[derive(Debug)]
pub(super) struct PendingStore;

impl std::fmt::Display for PendingStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "PendingStore")
    }
}

#[derive(Debug)]
pub(super) struct PendingUpload;

#[async_trait::async_trait]
impl MultipartUpload for PendingUpload {
    fn put_part(&mut self, _data: PutPayload) -> object_store::UploadPart {
        Box::pin(std::future::pending())
    }
    async fn complete(&mut self) -> OsResult<PutResult> {
        std::future::pending().await
    }
    async fn abort(&mut self) -> OsResult<()> {
        std::future::pending().await
    }
}

#[async_trait::async_trait]
impl ObjectStore for PendingStore {
    async fn put_opts(
        &self,
        _location: &ObjPath,
        _payload: PutPayload,
        _opts: PutOptions,
    ) -> OsResult<PutResult> {
        std::future::pending().await
    }
    async fn put_multipart_opts(
        &self,
        _location: &ObjPath,
        _opts: PutMultipartOptions,
    ) -> OsResult<Box<dyn MultipartUpload>> {
        Ok(Box::new(PendingUpload))
    }
    async fn get_opts(&self, _location: &ObjPath, _options: GetOptions) -> OsResult<GetResult> {
        std::future::pending().await
    }
    fn list(
        &self,
        _prefix: Option<&ObjPath>,
    ) -> futures_util::stream::BoxStream<'static, OsResult<ObjectMeta>> {
        use futures_util::StreamExt;
        futures_util::stream::pending().boxed()
    }
    async fn list_with_delimiter(&self, _prefix: Option<&ObjPath>) -> OsResult<ListResult> {
        std::future::pending().await
    }
    async fn copy_opts(&self, _from: &ObjPath, _to: &ObjPath, _opts: CopyOptions) -> OsResult<()> {
        std::future::pending().await
    }
    fn delete_stream(
        &self,
        _locations: futures_util::stream::BoxStream<'static, OsResult<ObjPath>>,
    ) -> futures_util::stream::BoxStream<'static, OsResult<ObjPath>> {
        use futures_util::StreamExt;
        futures_util::stream::pending().boxed()
    }
}
