use super::*;
use std::sync::atomic::{AtomicU8, Ordering};

#[derive(Debug)]
struct BodyFault {
    inner: Arc<dyn ObjectStore>,
    mode: AtomicU8,
}
impl std::fmt::Display for BodyFault {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "fleet-body-fault")
    }
}
#[async_trait::async_trait]
impl ObjectStore for BodyFault {
    async fn put_opts(
        &self,
        path: &ObjPath,
        body: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<object_store::PutResult> {
        self.inner.put_opts(path, body, opts).await
    }
    async fn put_multipart_opts(
        &self,
        path: &ObjPath,
        opts: object_store::PutMultipartOptions,
    ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
        self.inner.put_multipart_opts(path, opts).await
    }
    async fn get_opts(
        &self,
        path: &ObjPath,
        opts: object_store::GetOptions,
    ) -> object_store::Result<object_store::GetResult> {
        let mut got = self.inner.get_opts(path, opts).await?;
        let chunk = match self.mode.load(Ordering::SeqCst) {
            1 => Err(object_store::Error::Generic {
                store: "fleet-test",
                source: "entered body failed".into(),
            }),
            2 => Ok(Bytes::from(vec![b' '; MAX_DOCUMENT_BYTES + 1])),
            _ => return Ok(got),
        };
        // Metadata cannot be trusted as the sole byte bound.
        got.meta.size = 0;
        got.payload = object_store::GetResultPayload::Stream(
            futures_util::stream::once(async move { chunk }).boxed(),
        );
        Ok(got)
    }
    fn delete_stream(
        &self,
        paths: futures_util::stream::BoxStream<'static, object_store::Result<ObjPath>>,
    ) -> futures_util::stream::BoxStream<'static, object_store::Result<ObjPath>> {
        self.inner.delete_stream(paths)
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
        opts: object_store::CopyOptions,
    ) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, opts).await
    }
}

#[tokio::test]
async fn r09_fleet_document_body_failures_and_sizes_preserve_authority() {
    let inner: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let fault = Arc::new(BodyFault {
        inner: inner.clone(),
        mode: AtomicU8::new(0),
    });
    let repository = FleetRepository::new(Some(fault.clone()));
    inner
        .put(
            &ObjPath::from(OVERRIDES_DOC),
            PutPayload::from(r#"{"entries":{"00":{"to":"streams-2","ms":1}}}"#),
        )
        .await
        .unwrap();
    let (prior, version) = repository.read_overrides().await.unwrap();
    assert_eq!(prior.entries["00"].to, "streams-2");
    for mode in [1, 2] {
        fault.mode.store(mode, Ordering::SeqCst);
        let error = repository
            .read_overrides()
            .await
            .err()
            .expect("body failure must not become an empty map");
        assert!(error.to_string().contains(if mode == 1 {
            "entered body failed"
        } else {
            "byte budget"
        }));
    }
    fault.mode.store(0, Ordering::SeqCst);
    let (after, after_version) = repository.read_overrides().await.unwrap();
    assert_eq!(after.entries["00"].to, "streams-2");
    assert_eq!(after_version, version);
    inner
        .put(&ObjPath::from(OVERRIDES_DOC), PutPayload::from("corrupt"))
        .await
        .unwrap();
    assert!(repository.read_overrides().await.is_err());
    inner
        .put(
            &ObjPath::from(DESIRED_DOC),
            PutPayload::from(format!(
                r#"{{"count":{},"epoch":1,"reason":"x","computed_at_ms":0}}"#,
                u64::MAX
            )),
        )
        .await
        .unwrap();
    assert!(
        repository.read_desired_state().await.is_err(),
        "one unbounded integer must not allocate an unbounded ring"
    );
    inner.delete(&ObjPath::from(OVERRIDES_DOC)).await.unwrap();
    let (missing, version) = repository.read_overrides().await.unwrap();
    assert!(missing.entries.is_empty());
    assert!(
        version.is_none(),
        "only actual absence supplies the empty bootstrap view"
    );
}

#[tokio::test(start_paused = true)]
async fn r09_fleet_document_deadlines_leave_cas_source_retryable() {
    use crate::dst::{FaultPlan, FaultStore, ObjClass, StoreOp};
    let inner: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let fault = FaultStore::uniform(inner.clone(), 9094, FaultPlan::CLEAN);
    let repository = FleetRepository::new(Some(fault.clone()));
    let before = r#"{"count":2,"epoch":1,"reason":"prior","computed_at_ms":0}"#;
    inner
        .put(&ObjPath::from(DESIRED_DOC), PutPayload::from(before))
        .await
        .unwrap();
    let (_, version) = repository.read_desired_state().await.unwrap();
    let entered = fault.hold_class(StoreOp::Get, ObjClass::Fleet, u64::MAX);
    let started = tokio::time::Instant::now();
    assert!(
        repository
            .read_desired_state()
            .await
            .unwrap_err()
            .to_string()
            .contains("timed out")
    );
    assert_eq!(entered.load(Ordering::SeqCst), 1, "actual GET entered");
    assert_eq!(started.elapsed(), DOCUMENT_DEADLINE);
    fault.release_hold();
    let replacement = br#"{"count":3,"epoch":2,"reason":"next","computed_at_ms":0}"#.to_vec();
    let entered = fault.hold_class(StoreOp::Put, ObjClass::Fleet, u64::MAX);
    assert!(
        !repository
            .replace_document(FleetDocument::Desired, replacement.clone(), version.clone())
            .await
    );
    assert_eq!(entered.load(Ordering::SeqCst), 1, "actual CAS PUT entered");
    assert_eq!(
        inner
            .get(&ObjPath::from(DESIRED_DOC))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap(),
        before.as_bytes()
    );
    fault.release_hold();
    assert!(
        repository
            .replace_document(FleetDocument::Desired, replacement, version)
            .await
    );
    assert_eq!(
        repository
            .read_desired_state()
            .await
            .unwrap()
            .0
            .unwrap()
            .count,
        3
    );
}

#[test]
fn r09_fleet_configuration_cannot_publish_an_unreadable_population() {
    for (max, min) in [(0, 1), (u64::MAX, 1), (1, 4097)] {
        let mut cli = crate::config::CliArgs::deterministic();
        cli.fleet_max = max;
        let mut config =
            crate::config::ServerConfig::load(cli, &crate::config::MapEnvironment::default());
        config.fleet.fleet_min = min;
        let error = config
            .validate()
            .err()
            .expect("invalid fleet bounds must fail before startup");
        assert!(error.to_string().contains("4096-member fleet work budget"));
    }
    let mut cli = crate::config::CliArgs::deterministic();
    cli.fleet_max = 4096;
    let config = crate::config::ServerConfig::load(cli, &crate::config::MapEnvironment::default());
    assert!(
        config.validate().is_ok(),
        "supported bounded configuration remains valid"
    );
}

#[tokio::test]
async fn r09_fleet_write_caps_preserve_readable_document_and_exact_cas_version() {
    let inner: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let repository = FleetRepository::new(Some(inner.clone()));
    let mut at_cap = Overrides::default();
    for index in 0..MAX_MEMBERS {
        at_cap.entries.insert(
            format!("{index:04x}"),
            super::super::OverrideEntry {
                to: "streams-1".into(),
                ms: 1,
            },
        );
    }
    let original = serde_json::to_vec(&at_cap).unwrap();
    assert!(
        repository
            .replace_document(FleetDocument::Overrides, original.clone(), None)
            .await
    );
    let (_, version) = repository.read_overrides().await.unwrap();
    at_cap.entries.insert(
        "beyond-cap".into(),
        super::super::OverrideEntry {
            to: "streams-2".into(),
            ms: 2,
        },
    );
    assert!(
        !repository
            .replace_document(
                FleetDocument::Overrides,
                serde_json::to_vec(&at_cap).unwrap(),
                version.clone()
            )
            .await
    );
    let (after, after_version) = repository.read_overrides().await.unwrap();
    assert_eq!(after.entries.len(), MAX_MEMBERS);
    assert_eq!(
        after_version, version,
        "rejected publication must not change the CAS source version"
    );
    assert_eq!(
        inner
            .get(&ObjPath::from(OVERRIDES_DOC))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap(),
        original
    );
    at_cap.entries.remove("beyond-cap");
    at_cap.entries.get_mut("0000").unwrap().to = "streams-2".into();
    assert!(
        repository
            .replace_document(
                FleetDocument::Overrides,
                serde_json::to_vec(&at_cap).unwrap(),
                version
            )
            .await,
        "same-version bounded retry must remain possible"
    );
    assert_eq!(
        repository.read_overrides().await.unwrap().0.entries["0000"].to,
        "streams-2"
    );
    let desired = Desired {
        count: MAX_MEMBERS as u64,
        epoch: 1,
        reason: "at cap".into(),
        computed_at_ms: 0,
        pending_events: vec![],
    };
    assert!(
        repository
            .replace_document(
                FleetDocument::Desired,
                serde_json::to_vec(&desired).unwrap(),
                None
            )
            .await
    );
    let (_, version) = repository.read_desired_state().await.unwrap();
    for invalid in [
        Desired {
            count: MAX_MEMBERS as u64 + 1,
            ..desired.clone()
        },
        Desired {
            epoch: u64::MAX,
            ..desired.clone()
        },
        Desired {
            pending_events: vec![crate::ops::OpsEvent::new("test", "fixed".into()); 65],
            ..desired.clone()
        },
    ] {
        assert!(
            !repository
                .replace_document(
                    FleetDocument::Desired,
                    serde_json::to_vec(&invalid).unwrap(),
                    version.clone()
                )
                .await
        );
        assert_eq!(repository.read_desired_state().await.unwrap().1, version);
    }
    let (_, version) = repository.read_overrides().await.unwrap();
    at_cap.pending_events = vec![crate::ops::OpsEvent::new("test", "fixed".into()); 65];
    for invalid in [
        serde_json::to_vec(&at_cap).unwrap(),
        b"malformed".to_vec(),
        vec![b' '; MAX_DOCUMENT_BYTES + 1],
    ] {
        assert!(
            !repository
                .replace_document(FleetDocument::Overrides, invalid, version.clone())
                .await
        );
        assert_eq!(repository.read_overrides().await.unwrap().1, version);
    }
}

#[tokio::test]
async fn r09_fleet_population_budget_includes_its_coordination_documents() {
    let inner: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let repository = FleetRepository::new(Some(inner.clone()));
    for path in [DESIRED_DOC, OVERRIDES_DOC, URLS_DOC] {
        inner
            .put(&ObjPath::from(path), PutPayload::from("{}"))
            .await
            .unwrap();
    }
    for index in 1..=MAX_MEMBERS {
        let heartbeat: Heartbeat = serde_json::from_value(serde_json::json!({"instance":format!("streams-{index}"),"ts_ms":0,"rps":0.0,"owned_shards":[],"draining":false})).unwrap();
        repository
            .publish_heartbeat(&heartbeat.instance, &heartbeat)
            .await
            .unwrap();
    }
    assert_eq!(
        repository.read_heartbeat_set().await.unwrap().len(),
        MAX_MEMBERS,
        "three owner documents must not consume configured member slots"
    );
    inner
        .put(&ObjPath::from("fleet/extra.json"), PutPayload::from("{}"))
        .await
        .unwrap();
    assert!(
        repository.read_heartbeat_set().await.is_err(),
        "provider work beyond the declared cap must still fail closed"
    );
}
