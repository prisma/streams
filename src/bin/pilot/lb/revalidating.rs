//! The router re-reads the same few documents every poll (`Lb::poll_fleet`):
//! the desired count, the overrides, the URL map and every ordinal's
//! heartbeat every two seconds, the topology every minute. This store sends
//! each read of a whole document with the ETag of the copy it last returned,
//! so a document that has not changed answers 304 Not Modified, which the
//! object store does not bill, and the router reads that copy again (E7).
//! Every other operation passes through.
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use futures_util::StreamExt;
use object_store::path::Path;
use object_store::{
    Attributes, GetOptions, GetResult, GetResultPayload, ObjectMeta, ObjectStore, Result,
};

/// A whole document as last returned: what its answer carried, and its body.
#[derive(Clone, Debug)]
struct Held {
    meta: ObjectMeta,
    attributes: Attributes,
    body: Bytes,
}

impl Held {
    /// The answer this copy was read from, again.
    fn answer(self) -> GetResult {
        let body = self.body;
        GetResult {
            payload: GetResultPayload::Stream(
                futures_util::stream::once(async move { Ok(body) }).boxed(),
            ),
            range: 0..self.meta.size,
            meta: self.meta,
            attributes: self.attributes,
            extensions: Default::default(),
        }
    }
}

/// `inner`, re-reading an unchanged whole document from its last copy.
#[derive(Debug)]
pub(super) struct Revalidating {
    inner: Arc<dyn ObjectStore>,
    held: Mutex<HashMap<Path, Held>>,
}

impl Revalidating {
    pub(super) fn new(inner: Arc<dyn ObjectStore>) -> Self {
        Revalidating {
            inner,
            held: Mutex::default(),
        }
    }

    /// Holds a copy of `got`, a whole document's answer, when an ETag names
    /// it, and answers with that copy.
    async fn hold(&self, location: &Path, got: GetResult) -> Result<GetResult> {
        if got.meta.e_tag.is_none() {
            return Ok(got);
        }
        let (meta, attributes) = (got.meta.clone(), got.attributes.clone());
        let body = got.bytes().await?;
        let held = Held {
            meta,
            attributes,
            body,
        };
        if let Ok(mut copies) = self.held.lock() {
            copies.insert(location.clone(), held.clone());
        }
        Ok(held.answer())
    }
}

/// Whether `options` ask for the whole current document and nothing else.
fn whole(options: &GetOptions) -> bool {
    options.if_match.is_none()
        && options.if_none_match.is_none()
        && options.if_modified_since.is_none()
        && options.if_unmodified_since.is_none()
        && options.range.is_none()
        && options.version.is_none()
        && !options.head
}

impl std::fmt::Display for Revalidating {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Revalidating({})", self.inner)
    }
}

#[async_trait::async_trait]
impl ObjectStore for Revalidating {
    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        if !whole(&options) {
            return self.inner.get_opts(location, options).await;
        }
        let held = self
            .held
            .lock()
            .ok()
            .and_then(|copies| copies.get(location).cloned());
        let options = GetOptions {
            if_none_match: held.as_ref().and_then(|copy| copy.meta.e_tag.clone()),
            ..options
        };
        match (self.inner.get_opts(location, options).await, held) {
            (Ok(got), _) => self.hold(location, got).await,
            (Err(object_store::Error::NotModified { .. }), Some(copy)) => Ok(copy.answer()),
            (Err(error), _) => {
                if matches!(error, object_store::Error::NotFound { .. })
                    && let Ok(mut copies) = self.held.lock()
                {
                    copies.remove(location);
                }
                Err(error)
            }
        }
    }

    async fn put_opts(
        &self,
        location: &Path,
        payload: object_store::PutPayload,
        opts: object_store::PutOptions,
    ) -> Result<object_store::PutResult> {
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: object_store::PutMultipartOptions,
    ) -> Result<Box<dyn object_store::MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    fn delete_stream(
        &self,
        locations: futures_util::stream::BoxStream<'static, Result<Path>>,
    ) -> futures_util::stream::BoxStream<'static, Result<Path>> {
        self.inner.delete_stream(locations)
    }

    fn list(
        &self,
        prefix: Option<&Path>,
    ) -> futures_util::stream::BoxStream<'static, Result<ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<object_store::ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        options: object_store::CopyOptions,
    ) -> Result<()> {
        self.inner.copy_opts(from, to, options).await
    }
}

#[cfg(test)]
mod tests {
    use super::Revalidating;
    use object_store::{ObjectStore, ObjectStoreExt, memory::InMemory, path::Path};
    use std::sync::{Arc, Mutex};

    /// One GET as the store saw it: its path, whether it carried an ETag,
    /// and its answer.
    type Get = (String, bool, u16);

    /// An in-memory store recording each GET.
    #[derive(Debug, Default)]
    struct Recording {
        inner: InMemory,
        gets: Mutex<Vec<Get>>,
    }

    impl std::fmt::Display for Recording {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "recording")
        }
    }

    #[async_trait::async_trait]
    impl ObjectStore for Recording {
        async fn get_opts(
            &self,
            location: &Path,
            options: object_store::GetOptions,
        ) -> object_store::Result<object_store::GetResult> {
            let conditional = options.if_none_match.is_some();
            let got = self.inner.get_opts(location, options).await;
            let status = match &got {
                Ok(_) => 200,
                Err(object_store::Error::NotModified { .. }) => 304,
                Err(object_store::Error::NotFound { .. }) => 404,
                Err(_) => 500,
            };
            self.gets
                .lock()
                .unwrap()
                .push((location.to_string(), conditional, status));
            got
        }
        async fn put_opts(
            &self,
            location: &Path,
            payload: object_store::PutPayload,
            opts: object_store::PutOptions,
        ) -> object_store::Result<object_store::PutResult> {
            self.inner.put_opts(location, payload, opts).await
        }
        async fn put_multipart_opts(
            &self,
            location: &Path,
            opts: object_store::PutMultipartOptions,
        ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
            self.inner.put_multipart_opts(location, opts).await
        }
        fn delete_stream(
            &self,
            locations: futures_util::stream::BoxStream<'static, object_store::Result<Path>>,
        ) -> futures_util::stream::BoxStream<'static, object_store::Result<Path>> {
            self.inner.delete_stream(locations)
        }
        fn list(
            &self,
            prefix: Option<&Path>,
        ) -> futures_util::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>>
        {
            self.inner.list(prefix)
        }
        async fn list_with_delimiter(
            &self,
            prefix: Option<&Path>,
        ) -> object_store::Result<object_store::ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }
        async fn copy_opts(
            &self,
            from: &Path,
            to: &Path,
            options: object_store::CopyOptions,
        ) -> object_store::Result<()> {
            self.inner.copy_opts(from, to, options).await
        }
    }

    /// The body `store` answers for `path`, or the error's kind.
    async fn read(store: &Revalidating, path: &Path) -> Result<String, &'static str> {
        match store.get(path).await {
            Ok(got) => Ok(String::from_utf8(got.bytes().await.unwrap().to_vec()).unwrap()),
            Err(object_store::Error::NotFound { .. }) => Err("absent"),
            Err(_) => Err("failed"),
        }
    }

    /// A re-read of an unchanged document carries its ETag and answers the
    /// copy from the store's 304; a changed one answers its new body; a
    /// deleted one is absent, and its copy is dropped; a ranged read is
    /// passed through as asked.
    #[tokio::test]
    async fn an_unchanged_document_is_revalidated_and_its_copy_read_again() {
        let recording = Arc::new(Recording::default());
        let store = Revalidating::new(recording.clone());
        let path = Path::from("fleet/desired.json");
        let gets = || std::mem::take(&mut *recording.gets.lock().unwrap());
        let get = |conditional: bool, status: u16| (path.to_string(), conditional, status);
        recording.put(&path, "one".into()).await.unwrap();
        for (body, conditional, status) in [("one", false, 200), ("one", true, 304)] {
            assert_eq!(read(&store, &path).await, Ok(body.to_string()));
            assert_eq!(gets(), [get(conditional, status)]);
        }
        recording.put(&path, "two".into()).await.unwrap();
        for (body, status) in [("two", 200), ("two", 304)] {
            assert_eq!(read(&store, &path).await, Ok(body.to_string()));
            assert_eq!(gets(), [get(true, status)]);
        }
        assert_eq!(store.get_range(&path, 0..1).await.unwrap(), "t");
        assert_eq!(
            gets(),
            [get(false, 200)],
            "a ranged read is not revalidated"
        );
        recording.delete(&path).await.unwrap();
        for conditional in [true, false] {
            assert_eq!(read(&store, &path).await, Err("absent"));
            assert_eq!(gets(), [get(conditional, 404)]);
        }
    }

    /// The router's poll (`Lb::poll_fleet`), over the store its poller
    /// builds (`lb::poller_store`), of a fleet that has not changed since its
    /// last poll: the desired count, the URL map, each heartbeat and the
    /// topology answer 304; only the absent overrides document is read (a
    /// 404) again.
    #[tokio::test]
    async fn a_poll_of_an_unchanged_fleet_is_answered_by_304s() {
        let recording = Arc::new(Recording::default());
        let store = super::super::poller_store(recording.clone());
        let now = crate::now_ms();
        for (path, body) in [
            ("fleet/desired.json", r#"{"count":2}"#.to_string()),
            ("fleet/urls.json", "{}".to_string()),
            ("fleet/streams-1.json", format!(r#"{{"ts_ms":{now}}}"#)),
            ("fleet/streams-2.json", format!(r#"{{"ts_ms":{now}}}"#)),
            ("topology.json", r#"{"shards":["0","1"]}"#.to_string()),
        ] {
            recording.put(&Path::from(path), body.into()).await.unwrap();
        }
        let lb = crate::Lb {
            upstreams: std::sync::RwLock::new(vec!["http://127.0.0.1:9".into(); 2]),
            stats: vec![crate::UpStat::default(), crate::UpStat::default()],
            history: Mutex::default(),
            gen_stats: Mutex::new(serde_json::Value::Null),
            fleet: Mutex::new(crate::FleetView::default()),
            http: crate::RotatingClient::new(),
        };
        let mut polls = Vec::new();
        for _ in 0..2 {
            lb.poll_fleet(&store, &store, 2, true).await;
            let mut gets = std::mem::take(&mut *recording.gets.lock().unwrap());
            gets.sort();
            polls.push(gets);
        }
        let get =
            |path: &str, conditional: bool, status: u16| (path.to_string(), conditional, status);
        let first = [
            get("fleet/desired.json", false, 200),
            get("fleet/overrides.json", false, 404),
            get("fleet/streams-1.json", false, 200),
            get("fleet/streams-2.json", false, 200),
            get("fleet/urls.json", false, 200),
            get("topology.json", false, 200),
        ];
        let unchanged = [
            get("fleet/desired.json", true, 304),
            get("fleet/overrides.json", false, 404),
            get("fleet/streams-1.json", true, 304),
            get("fleet/streams-2.json", true, 304),
            get("fleet/urls.json", true, 304),
            get("topology.json", true, 304),
        ];
        assert_eq!(polls, [first, unchanged]);
        let view = lb.fleet.lock().unwrap().clone();
        assert_eq!((view.desired, view.active.len()), (2, 2));
        assert_eq!(view.topology, ["0", "1"]);
    }
}
