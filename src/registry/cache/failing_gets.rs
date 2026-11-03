//! A store whose GETs of one object fail on demand (tuning item 5's
//! tests): the registry's descriptor read under a store that fails it once,
//! or every time.
#![cfg(test)]
use std::sync::{Arc, Mutex};

use object_store::ObjectStore;
use object_store::path::Path as ObjPath;

/// GETs of the path ending in `suffix`: the next `failing` of them fail
/// with a generic store error, and `gets` counts every one asked for.
#[derive(Debug, Default)]
struct Rule {
    suffix: String,
    failing: u64,
    gets: u64,
}

/// An `ObjectStore` over `inner` whose armed path's GETs fail.
#[derive(Debug)]
pub(crate) struct FailingGets {
    pub(crate) inner: Arc<dyn ObjectStore>,
    rule: Mutex<Rule>,
}

impl FailingGets {
    pub(crate) fn over(inner: Arc<dyn ObjectStore>) -> Arc<Self> {
        Arc::new(Self {
            inner,
            rule: Mutex::default(),
        })
    }

    /// Fail the next `failing` GETs of the path ending in `suffix`
    /// (`u64::MAX`: every one), counting its GETs from zero.
    pub(crate) fn fail(&self, suffix: &str, failing: u64) {
        *self.rule.lock().unwrap() = Rule {
            suffix: suffix.to_string(),
            failing,
            gets: 0,
        };
    }

    /// The armed path's GETs since it was armed.
    pub(crate) fn gets(&self) -> u64 {
        self.rule.lock().unwrap().gets
    }

    /// Whether this GET of `location` fails, counting it.
    fn take(&self, location: &ObjPath) -> bool {
        let mut rule = self.rule.lock().unwrap();
        if rule.suffix.is_empty() || !location.as_ref().ends_with(rule.suffix.as_str()) {
            return false;
        }
        rule.gets += 1;
        let failing = rule.failing > 0;
        rule.failing = rule.failing.saturating_sub(1);
        failing
    }
}

/// The descriptor object's suffix for stream `name`: its name's hex.
pub(crate) fn descriptor_suffix(name: &str) -> String {
    let hex: String = name.bytes().map(|b| format!("{b:02x}")).collect();
    format!("/streams/{hex}.json")
}

impl std::fmt::Display for FailingGets {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("FailingGets")
    }
}

#[async_trait::async_trait]
impl ObjectStore for FailingGets {
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
        if self.take(location) {
            return Err(object_store::Error::Generic {
                store: "FailingGets",
                source: "injected store failure".into(),
            });
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
