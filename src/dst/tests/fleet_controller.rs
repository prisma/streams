//! Entered fleet I/O cancellation and complete ownership-view publication.
use super::fixture_http::{HttpRigOptions, engine_shutdown, http_rig_build};
use super::fixture_runtime::RigRuntime;
use super::fixture_storage::mem;
use object_store::{ObjectStore, ObjectStoreExt, PutPayload, path::Path};
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};
use std::time::Duration;

#[derive(Debug)]
struct HeldDocument {
    inner: Arc<dyn ObjectStore>,
    path: &'static str,
    write: bool,
    entered: AtomicU64,
    gate: tokio::sync::Semaphore,
}
impl std::fmt::Display for HeldDocument {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "held-fleet-document")
    }
}
impl HeldDocument {
    async fn enter(&self, path: &Path, write: bool) {
        if path.as_ref() == self.path && write == self.write {
            self.entered.fetch_add(1, Ordering::SeqCst);
            let _ = self.gate.acquire().await;
        }
    }
}
#[async_trait::async_trait]
impl ObjectStore for HeldDocument {
    async fn put_opts(
        &self,
        path: &Path,
        body: PutPayload,
        opts: object_store::PutOptions,
    ) -> object_store::Result<object_store::PutResult> {
        self.enter(path, true).await;
        self.inner.put_opts(path, body, opts).await
    }
    async fn put_multipart_opts(
        &self,
        path: &Path,
        opts: object_store::PutMultipartOptions,
    ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
        self.inner.put_multipart_opts(path, opts).await
    }
    async fn get_opts(
        &self,
        path: &Path,
        opts: object_store::GetOptions,
    ) -> object_store::Result<object_store::GetResult> {
        self.enter(path, false).await;
        self.inner.get_opts(path, opts).await
    }
    fn delete_stream(
        &self,
        paths: futures_util::stream::BoxStream<'static, object_store::Result<Path>>,
    ) -> futures_util::stream::BoxStream<'static, object_store::Result<Path>> {
        self.inner.delete_stream(paths)
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
        opts: object_store::CopyOptions,
    ) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, opts).await
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn r09_fleet_cancels_entered_documents_without_partial_authority_or_lost_retry() {
    for (path, write) in [
        ("fleet/streams-1.json", true),
        ("fleet/overrides.json", false),
        ("fleet/desired.json", true),
    ] {
        let inner = mem();
        if path != "fleet/desired.json" {
            inner
                .put(
                    &Path::from("fleet/desired.json"),
                    PutPayload::from(
                        r#"{"count":1,"epoch":1,"reason":"source","computed_at_ms":0}"#,
                    ),
                )
                .await
                .unwrap();
        }
        inner
            .put(
                &Path::from("fleet/overrides.json"),
                PutPayload::from(r#"{"entries":{}}"#),
            )
            .await
            .unwrap();
        let store = Arc::new(HeldDocument {
            inner: inner.clone(),
            path,
            write,
            entered: AtomicU64::new(0),
            gate: tokio::sync::Semaphore::new(0),
        });
        let rig = http_rig_build(
            mem(),
            RigRuntime::first(),
            HttpRigOptions {
                fleet_store: Some(store.clone()),
                instance: Some("streams-1".into()),
                ..Default::default()
            },
        )
        .await;
        rig.state.ownership.set_view(
            vec!["prior-owner".into()],
            std::collections::HashMap::from([("00".into(), "prior-owner".into())]),
        );
        let prior = rig.state.ownership.view();
        assert!(crate::fleet::start_configured(
            rig.state.clone(),
            &rig.state.config,
            &rig.tasks
        ));
        tokio::time::timeout(Duration::from_secs(5), async {
            while store.entered.load(Ordering::SeqCst) == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("target storage operation must be entered");
        if path != "fleet/desired.json" {
            assert_eq!(
                rig.state.ownership.view(),
                prior,
                "unread overrides must not publish a new ring"
            );
        }
        let report = rig.tasks.shutdown(Duration::from_millis(300)).await;
        assert!(
            report.aborted.is_empty(),
            "active fleet I/O must cancel cooperatively: {report:?}"
        );
        assert!(
            report
                .outcomes
                .iter()
                .any(|(name, outcome)| *name == "fleet"
                    && *outcome == crate::tasks::TaskOutcome::Finished)
        );
        if path == "fleet/desired.json" {
            assert!(
                matches!(
                    inner.get(&Path::from(path)).await,
                    Err(object_store::Error::NotFound { .. })
                ),
                "held CAS cannot claim publication"
            );
        }
        store.gate.close();
        let retry = crate::tasks::TaskSupervisor::new();
        assert!(crate::fleet::start_configured(
            rig.state.clone(),
            &rig.state.config,
            &retry
        ));
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let desired = rig.state.fleet.read_desired_state().await.unwrap().0;
                if rig.state.ownership.ring_active() == vec!["streams-1".to_string()]
                    && desired.is_some()
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("fresh authoritative retry must complete after release");
        let desired = rig
            .state
            .fleet
            .read_desired_state()
            .await
            .unwrap()
            .0
            .unwrap();
        if path == "fleet/desired.json" {
            assert_eq!(desired.pending_events.len(), 1);
            assert_eq!(desired.pending_events[0].event_id, "desired/1");
        }
        assert!(
            retry
                .shutdown(Duration::from_millis(300))
                .await
                .aborted
                .is_empty()
        );
        engine_shutdown(&rig.state).await;
    }
}
