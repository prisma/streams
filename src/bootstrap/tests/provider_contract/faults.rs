//! One-shot faults around the store under test. The wrapper sits above the
//! production client, so the client's own retries happen beneath it: a lost
//! reply here is a PUT the store applied whose outcome the caller never
//! learns, and a failed dispatch is a PUT the store never saw.
#![cfg(test)]

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering::SeqCst};
use std::sync::{Arc, Mutex};

use futures_util::stream::BoxStream;
use object_store::path::Path as ObjPath;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMode, PutMultipartOptions, PutOptions, PutPayload, PutResult,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum PutFault {
    /// The inner store performs the PUT; its reply is replaced by a
    /// transport error.
    LoseReply,
    /// The PUT fails before the inner store sees it.
    FailBeforeDispatch,
}

/// Which conditional PUT an armed fault applies to.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Conditional {
    Create,
    Update,
}

#[derive(Debug)]
struct Armed {
    fault: PutFault,
    mode: Conditional,
    path_contains: String,
}

impl Armed {
    fn matches(&self, location: &ObjPath, mode: &PutMode) -> bool {
        let mode_matches = match self.mode {
            Conditional::Create => matches!(mode, PutMode::Create),
            Conditional::Update => matches!(mode, PutMode::Update(_)),
        };
        mode_matches && location.as_ref().contains(&self.path_contains)
    }
}

#[derive(Debug)]
pub(super) struct FaultyStore {
    inner: Arc<dyn ObjectStore>,
    armed: Mutex<Option<Armed>>,
    strip_etags: AtomicBool,
    /// PUTs the inner store received.
    dispatched: AtomicU64,
    /// Armed faults that fired.
    fired: AtomicU64,
}

impl std::fmt::Display for FaultyStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "faulty({})", self.inner)
    }
}

impl FaultyStore {
    pub(super) fn new(inner: Arc<dyn ObjectStore>) -> Arc<Self> {
        Arc::new(FaultyStore {
            inner,
            armed: Mutex::new(None),
            strip_etags: AtomicBool::new(false),
            dispatched: AtomicU64::new(0),
            fired: AtomicU64::new(0),
        })
    }

    /// Apply `fault` to the next `mode` PUT whose path contains
    /// `path_contains`.
    pub(super) fn arm(&self, fault: PutFault, mode: Conditional, path_contains: &str) {
        *self.armed.lock().unwrap() = Some(Armed {
            fault,
            mode,
            path_contains: path_contains.into(),
        });
    }

    /// While on, every GET and HEAD answers without an ETag.
    pub(super) fn strip_etags(&self, on: bool) {
        self.strip_etags.store(on, SeqCst);
    }

    pub(super) fn dispatched(&self) -> u64 {
        self.dispatched.load(SeqCst)
    }

    pub(super) fn fired(&self) -> u64 {
        self.fired.load(SeqCst)
    }

    fn take(&self, location: &ObjPath, mode: &PutMode) -> Option<PutFault> {
        let mut armed = self.armed.lock().unwrap();
        if !armed.as_ref().is_some_and(|a| a.matches(location, mode)) {
            return None;
        }
        self.fired.fetch_add(1, SeqCst);
        armed.take().map(|a| a.fault)
    }
}

fn injected(location: &ObjPath, what: &str) -> object_store::Error {
    object_store::Error::Generic {
        store: "provider-contract",
        source: format!("{what} for {location}").into(),
    }
}

#[async_trait::async_trait]
impl ObjectStore for FaultyStore {
    async fn put_opts(
        &self,
        location: &ObjPath,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        let fault = self.take(location, &opts.mode);
        if fault == Some(PutFault::FailBeforeDispatch) {
            return Err(injected(location, "connection failed before dispatch"));
        }
        self.dispatched.fetch_add(1, SeqCst);
        let result = self.inner.put_opts(location, payload, opts).await;
        if fault == Some(PutFault::LoseReply) {
            return Err(injected(location, "reply lost after the store answered"));
        }
        result
    }

    async fn put_multipart_opts(
        &self,
        location: &ObjPath,
        opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(
        &self,
        location: &ObjPath,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        let mut result = self.inner.get_opts(location, options).await;
        if self.strip_etags.load(SeqCst)
            && let Ok(got) = &mut result
        {
            got.meta.e_tag = None;
        }
        result
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, object_store::Result<ObjPath>>,
    ) -> BoxStream<'static, object_store::Result<ObjPath>> {
        self.inner.delete_stream(locations)
    }

    fn list(
        &self,
        prefix: Option<&ObjPath>,
    ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&ObjPath>,
    ) -> object_store::Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &ObjPath,
        to: &ObjPath,
        options: CopyOptions,
    ) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, options).await
    }
}
