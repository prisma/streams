//! The server's S3 client: every request that carries a precondition is
//! sent exactly once (docs/PROVIDER-CONTRACT.md, finding F1).
//!
//! `object_store` 0.14.1 re-sends a failed request inside one call. For a
//! conditional PUT it re-sends after a 5xx, 429 or 408, after a connection
//! that closed before the whole reply arrived, and, for an update, after a
//! 409; the retry carries the original `If-None-Match: *` or `If-Match`.
//! When the first attempt had committed, the retry is refused, and the call
//! answers `AlreadyExists` or `Precondition` for the caller's own write.
//! Every conditional writer reads those two answers as "not written, another
//! writer won": the registry re-decides against its own descriptor
//! (applying a non-idempotent decision twice, or declining against itself)
//! and a create reports a lost race to itself. The retry policy is a
//! per-client [`RetryConfig`]; nothing per request turns it off.
//!
//! So the store holds two clients over one connection pool. The conditional
//! client never retries: its `AlreadyExists` and `Precondition` are the
//! provider's answer to the one request that carried the precondition, so
//! they mean "not written" exactly as ASM-OBJSTORE-CAS states. Every other
//! failure of a conditional write reaches the caller as an ordinary error,
//! which each caller already treats as possibly committed (the registry's
//! `AmbiguousCompletion`, SlateDB's put-id check). Everything else — reads,
//! lists, deletes, unconditional PUTs, multipart uploads — keeps the default
//! retries, because repeating them cannot turn a success into a refusal.

use async_trait::async_trait;
use futures_util::stream::BoxStream;
use object_store::aws::{AmazonS3, AmazonS3Builder};
use object_store::client::{HttpClient, HttpConnector};
use object_store::path::Path;
use object_store::{
    ClientOptions, CopyMode, CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload,
    ObjectMeta, ObjectStore, PutMode, PutMultipartOptions, PutOptions, PutPayload, PutResult,
    Result, RetryConfig,
};

/// The two S3 clients, split by whether a request carries a precondition.
#[derive(Debug)]
pub(super) struct S3Store {
    /// Requests that are safe to repeat, with the default retries.
    retried: AmazonS3,
    /// Conditional PUTs and copies, sent once.
    conditional: AmazonS3,
}

impl S3Store {
    /// Build both clients from one configured builder. They share one HTTP
    /// client, so the conditional writes (the WAL and manifest PUTs among
    /// them) use the pool the reads keep warm.
    pub(super) fn build(builder: AmazonS3Builder, options: ClientOptions) -> Result<Self> {
        // Records Tigris's Server-Timing (their internal ms) and
        // x-tigris-served-from per response → sp50/sp99 + served_from
        // in /v1/debug/store. wall − server = network path.
        let http = crate::store_timing::SniffConnector.connect(&options)?;
        let builder = builder
            .with_client_options(options)
            .with_http_connector(SharedHttp(http));
        Ok(S3Store {
            retried: builder.clone().build()?,
            conditional: builder
                .with_retry(RetryConfig {
                    max_retries: 0,
                    ..RetryConfig::default()
                })
                .build()?,
        })
    }
}

/// Hands every client the one HTTP client built from the server's options.
/// The server uses static credentials, so the S3 client is the only one a
/// builder connects.
#[derive(Debug)]
struct SharedHttp(HttpClient);

impl HttpConnector for SharedHttp {
    fn connect(&self, _: &ClientOptions) -> Result<HttpClient> {
        Ok(self.0.clone())
    }
}

impl std::fmt::Display for S3Store {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(&self.retried, f)
    }
}

#[async_trait]
impl ObjectStore for S3Store {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> Result<PutResult> {
        let client = match opts.mode {
            PutMode::Overwrite => &self.retried,
            PutMode::Create | PutMode::Update(_) => &self.conditional,
        };
        client.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>> {
        self.retried.put_multipart_opts(location, opts).await
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        self.retried.get_opts(location, options).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, Result<Path>>,
    ) -> BoxStream<'static, Result<Path>> {
        self.retried.delete_stream(locations)
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        self.retried.list(prefix)
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, Result<ObjectMeta>> {
        self.retried.list_with_offset(prefix, offset)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        self.retried.list_with_delimiter(prefix).await
    }

    async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> Result<()> {
        let client = match options.mode {
            CopyMode::Overwrite => &self.retried,
            CopyMode::Create => &self.conditional,
        };
        client.copy_opts(from, to, options).await
    }
}
