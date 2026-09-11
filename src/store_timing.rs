//! O14a diagnostic: per-operation object-store latency, recorded at the
//! object_store client boundary — the last point that is *ours* before the
//! platform's egress path and Tigris. Splits ack excursions three ways:
//!   - WAL PUT tail spikes alone            → provider (Tigris) tail latency
//!   - every op class spikes together AND the outbound in-flight gauge sits
//!     at the platform egress budget (~50)  → egress-slot exhaustion
//!   - acks spike with no store-side spike  → our scheduling/watermark path
//!
//! Admission is shared across the stores of one runtime. Diagnostic latency
//! and physical process-egress counters remain process-wide measurements.

use async_trait::async_trait;
use bytes::Bytes;
use futures_core::Stream;
use futures_util::{StreamExt, stream::BoxStream};
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, Result, UploadPart, path::Path,
};
use std::ops::Range;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Instant;

mod observations;
mod sentinels;
use observations::{OpGuard, record};
pub(crate) use observations::{SniffConnector, classify, heartbeat_summary, snapshot, stats};
pub(crate) use sentinels::spawn_sentinels;

/// Cumulative object-store GET count and bytes fetched.
///
/// R23-6: the deepest CHAOS-5 fact is that reading ~4 MiB of absorbable
/// data takes 21-42 s. Concurrency only overlapped those slow reads
/// while multiplying unmodelled memory, so the number that actually
/// explains the ceiling is READ AMPLIFICATION — bytes fetched from the
/// object store per useful frame byte returned. The absorber snapshots
/// these around its read phase; the delta is that gather's cost.
pub(crate) static GET_COUNT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
pub(crate) static GET_BYTES: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

mod resources;
pub(crate) use resources::StoreResources;

/// ObjectStore wrapper that times every operation. Sits *beneath*
/// PrefixStore so it sees final (fully-prefixed) paths.
#[derive(Debug)]
pub(crate) struct TimingStore<T: ObjectStore> {
    inner: T,
    resources: Arc<StoreResources>,
}

impl<T: ObjectStore> TimingStore<T> {
    pub(crate) fn new(inner: T, resources: Arc<StoreResources>) -> Self {
        TimingStore { inner, resources }
    }
}

impl<T: ObjectStore> std::fmt::Display for TimingStore<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "TimingStore({})", self.inner)
    }
}

#[async_trait]
impl<T: ObjectStore> ObjectStore for TimingStore<T> {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> Result<PutResult> {
        let _b = self
            .resources
            .bulk_permit(classify(location.as_ref()), payload.content_length() as u64)
            .await;
        let _p = self.resources.permit().await;
        let g = OpGuard::new(0, location);
        let r = self.inner.put_opts(location, payload, opts).await;
        g.finish(r.is_ok());
        r
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>> {
        let _p = self.resources.permit().await;
        let g = OpGuard::new(1, location);
        let class = classify(location.as_ref());
        match self.inner.put_multipart_opts(location, opts).await {
            Ok(up) => Ok(Box::new(TimedMpu {
                inner: up,
                guard: Some(g),
                class,
                resources: self.resources.clone(),
            })),
            Err(e) => {
                g.finish(false);
                Err(e)
            }
        }
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        // 0.14 routes the ext-method `head()` through get_opts(head: true).
        // Bulk gate: weight by the requested range when it is bounded, a
        // nominal SST otherwise; held for the call only (time to first
        // byte), never across body streaming — see BulkGate.
        let _b = if options.head {
            None
        } else {
            let w = match &options.range {
                Some(object_store::GetRange::Bounded(r)) => r.end.saturating_sub(r.start),
                _ => self.resources.nominal_get_bytes(),
            };
            self.resources
                .bulk_permit(classify(location.as_ref()), w)
                .await
        };
        let _p = self.resources.permit().await;
        let is_head = options.head;
        let g = OpGuard::new(if is_head { 3 } else { 2 }, location);
        let r = self.inner.get_opts(location, options).await;
        // GetResult still streams the body afterwards; timing to first byte
        // is what the egress path gates on, and it keeps the guard simple.
        // A 304 on a conditional GET is a successful revalidation (the
        // registry's TTL refresh), not an error.
        let ok = r.is_ok() || matches!(&r, Err(object_store::Error::NotModified { .. }));
        g.finish(ok);
        // R25-F: count ACTUAL transferred bytes by wrapping the payload
        // stream — the R23-6 version counted object METADATA size, so a
        // ranged read billed the whole object. GET_BYTES is now the
        // bytes that crossed the wire, whatever the range.
        if !is_head {
            return r.map(|res| {
                GET_COUNT.fetch_add(1, Ordering::Relaxed);
                let object_store::GetResult {
                    payload,
                    meta,
                    range,
                    attributes,
                    extensions,
                } = res;
                use futures_util::StreamExt;
                let counted = match payload {
                    object_store::GetResultPayload::Stream(st) => {
                        object_store::GetResultPayload::Stream(Box::pin(st.map(|chunk| {
                            if let Ok(b) = &chunk {
                                GET_BYTES.fetch_add(b.len() as u64, Ordering::Relaxed);
                            }
                            chunk
                        })))
                    }
                    other => other,
                };
                object_store::GetResult {
                    payload: counted,
                    meta,
                    range,
                    attributes,
                    extensions,
                }
            });
        }
        r
    }

    async fn get_ranges(&self, location: &Path, ranges: &[Range<u64>]) -> Result<Vec<Bytes>> {
        // Bulk gate: the buffers materialize inside this call, so the
        // exact requested byte total is the honest weight.
        let _b = self
            .resources
            .bulk_permit(
                classify(location.as_ref()),
                ranges.iter().map(|r| r.end.saturating_sub(r.start)).sum(),
            )
            .await;
        let _p = self.resources.permit().await;
        let g = OpGuard::new(2, location);
        let r = self.inner.get_ranges(location, ranges).await;
        g.finish(r.is_ok());
        // R23-6: range reads are how the absorber pulls frames out of the
        // object-store-backed LSM, so this is where amplification shows.
        if let Ok(parts) = &r {
            GET_COUNT.fetch_add(parts.len() as u64, Ordering::Relaxed);
            GET_BYTES.fetch_add(
                parts.iter().map(|b| b.len() as u64).sum::<u64>(),
                Ordering::Relaxed,
            );
        }
        r
    }

    // 0.14 made single-shot delete an ext method over delete_stream: each
    // yielded path is one completed delete; inter-yield gaps approximate
    // per-delete latency (deletes are never the ack path — class + gauge
    // matter here, exact durations don't).
    fn delete_stream(
        &self,
        locations: BoxStream<'static, Result<Path>>,
    ) -> BoxStream<'static, Result<Path>> {
        let s = stats();
        let now = s.inflight.fetch_add(1, Ordering::Relaxed) + 1;
        s.inflight_peak.fetch_max(now, Ordering::Relaxed);
        TimedDeleteStream {
            inner: self.inner.delete_stream(locations),
            last: Instant::now(),
            open: true,
        }
        .boxed()
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        let g = OpGuard::new(5, prefix.unwrap_or(&Path::default()));
        TimedStream {
            inner: self.inner.list(prefix),
            guard: Some(g),
        }
        .boxed()
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        let _p = self.resources.permit().await;
        let g = OpGuard::new(5, prefix.unwrap_or(&Path::default()));
        let r = self.inner.list_with_delimiter(prefix).await;
        g.finish(r.is_ok());
        r
    }

    async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> Result<()> {
        let _p = self.resources.permit().await;
        let g = OpGuard::new(6, from);
        let r = self.inner.copy_opts(from, to, options).await;
        g.finish(r.is_ok());
        r
    }
}

/// Times a list stream over its whole life (streams hold the outbound
/// connection until exhausted or dropped).
struct TimedStream<S> {
    inner: S,
    guard: Option<OpGuard>,
}

impl<S: Stream<Item = Result<ObjectMeta>> + Unpin> Stream for TimedStream<S> {
    type Item = Result<ObjectMeta>;
    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Result<ObjectMeta>>> {
        let poll = self.inner.poll_next_unpin(cx);
        if let std::task::Poll::Ready(None) = poll
            && let Some(g) = self.guard.take()
        {
            g.finish(true);
        }
        poll
    }
}

/// delete_stream wrapper: gauge held for the stream's life, one recorded
/// event per completed (yielded) delete.
struct TimedDeleteStream {
    inner: BoxStream<'static, Result<Path>>,
    last: Instant,
    open: bool,
}

impl Stream for TimedDeleteStream {
    type Item = Result<Path>;
    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Result<Path>>> {
        let poll = self.inner.poll_next_unpin(cx);
        match &poll {
            std::task::Poll::Ready(Some(item)) => {
                let start = self.last;
                self.last = Instant::now();
                let (p, ok) = match item {
                    Ok(p) => (p.as_ref().to_string(), true),
                    Err(_) => (String::new(), false),
                };
                record(4, classify(&p), start, &p, ok);
            }
            std::task::Poll::Ready(None) if self.open => {
                self.open = false;
                stats().inflight.fetch_sub(1, Ordering::Relaxed);
            }
            _ => {}
        }
        poll
    }
}

impl Drop for TimedDeleteStream {
    fn drop(&mut self) {
        if self.open {
            self.open = false;
            stats().inflight.fetch_sub(1, Ordering::Relaxed);
        }
    }
}

struct TimedMpu {
    resources: Arc<StoreResources>,
    inner: Box<dyn MultipartUpload>,
    guard: Option<OpGuard>,
    class: u8,
}

impl std::fmt::Debug for TimedMpu {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "TimedMpu({:?})", self.inner)
    }
}

#[async_trait]
impl MultipartUpload for TimedMpu {
    fn put_part(&mut self, data: PutPayload) -> UploadPart {
        // Bulk gate per part: WriteMultipart pipelines several parts per
        // upload, each one a fully-buffered payload — exactly the wave
        // the gate exists to flatten. Weight is acquired inside the
        // returned future so pipelined parts queue, not the caller.
        let bytes = data.content_length() as u64;
        let class = self.class;
        let inner = self.inner.put_part(data);
        if class != 2 {
            return inner;
        }
        let resources = self.resources.clone();
        Box::pin(async move {
            let _b = resources.bulk_permit(class, bytes).await;
            inner.await
        })
    }
    async fn complete(&mut self) -> Result<PutResult> {
        let r = self.inner.complete().await;
        if let Some(g) = self.guard.take() {
            g.finish(r.is_ok());
        }
        r
    }
    async fn abort(&mut self) -> Result<()> {
        let r = self.inner.abort().await;
        if let Some(g) = self.guard.take() {
            g.finish(false);
        }
        r
    }
}

#[cfg(test)]
mod tests;
