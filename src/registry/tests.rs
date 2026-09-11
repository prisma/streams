//! Registry fixtures: conditional writes, decode fail-closed, catalog
//! paging and recreate races against an in-memory store.
#![cfg(test)]
use super::*;
use object_store::ObjectStoreExt;

#[test]
fn r08_missing_conditional_token_fails_closed() {
    for token in [None, Some(String::new())] {
        let error = ConditionalUpdateToken::from_etag(token).unwrap_err();
        assert!(matches!(error, object_store::Error::Generic { .. }));
    }
    assert!(matches!(
        ConditionalUpdateToken::from_etag(Some("etag".into()))
            .unwrap()
            .mode(),
        PutMode::Update(_)
    ));
}

#[test]
fn r08_only_precondition_conflicts_are_retried() {
    let conflict = anyhow::Error::from(object_store::Error::Precondition {
        path: "descriptor".into(),
        source: "wording is irrelevant".into(),
    });
    assert!(retryable_cas_error(&conflict));
    for error in [
        anyhow::anyhow!("precondition conflict"),
        anyhow::Error::from(ConditionalUpdateToken::from_etag(None).unwrap_err()),
    ] {
        assert!(!retryable_cas_error(&error));
    }
}

#[tokio::test]
async fn r04_invalid_descriptors_cannot_reach_storage() {
    let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let registry = Registry::new(
        store.clone(),
        &crate::tenant::CellId::new("test-cell").unwrap(),
    );
    let valid = desc("validated", "00112233445566778899aabbccddeeff", false);
    let mut invalid = Vec::new();
    let mut d = valid.clone();
    d.stream_epoch = "00".into();
    invalid.push(d);
    let mut d = valid.clone();
    d.stream_epoch = "z".repeat(32);
    invalid.push(d);
    let mut d = valid.clone();
    d.segments = Some(crate::segmap::SegmentMap {
        version: 1,
        next_seg_id: 1,
        segments: vec![],
        pending: None,
    });
    invalid.push(d);
    let map = crate::segmap::SegmentMap::initial("", 1);
    let mut d = valid.clone();
    let mut m = map.clone();
    m.segments[0].lo = 1;
    d.segments = Some(m);
    invalid.push(d);
    let mut d = valid.clone();
    let mut m = map.clone();
    m.segments.push(m.segments[0].clone());
    d.segments = Some(m);
    invalid.push(d);
    let mut d = valid.clone();
    let mut m = map.clone();
    m.segments[0].predecessors.push(77);
    d.segments = Some(m);
    invalid.push(d);
    let mut d = valid.clone();
    d.sealed = true;
    d.sealing = Some(SealState {
        operation_id: "op".into(),
        claimed_ms: 1,
        claim_generation: 0,
        intent: SealIntent::Empty,
    });
    invalid.push(d);
    for (index, descriptor) in invalid.into_iter().enumerate() {
        assert!(
            StreamDesc::try_from(descriptor.clone()).is_err(),
            "invalid case {index}"
        );
        assert!(
            registry.create(descriptor).await.is_err(),
            "invalid case {index} reached create"
        );
        assert!(matches!(
            store.get(&desc_path("test-cell", &valid.sref())).await,
            Err(object_store::Error::NotFound { .. })
        ));
    }
}

#[test]
fn r04_valid_transition_and_sealed_predecessor_snapshots() {
    let mut dto = desc("validated", "00112233445566778899aabbccddeeff", false);
    let mut map = crate::segmap::SegmentMap::initial("", 1);
    map.pending = Some(crate::segmap::PendingTransition {
        kind: "split".into(),
        segs: vec![0],
        split_at: u64::MAX / 2,
        started_ms: 1,
        seal_gen: 0,
    });
    dto.segments = Some(map.clone());
    assert!(
        StreamDesc::try_from(dto.clone()).is_ok(),
        "pending split is a valid recovery state"
    );
    map.pending = None;
    let (a, b) = map.split(0, u64::MAX / 2, 7, [1; 16], [2; 16], 2).unwrap();
    dto.segments = Some(map.clone());
    assert!(StreamDesc::try_from(dto.clone()).is_ok());
    map.merge(a, b, 3, 4, [3; 16], 3).unwrap();
    dto.segments = Some(map.clone());
    assert!(StreamDesc::try_from(dto.clone()).is_ok());
    for segment in map.segments.iter_mut().filter(|segment| segment.is_live()) {
        segment.sealed_ms = Some(4);
        segment.sealed_next_offset = Some(5);
    }
    dto.segments = Some(map);
    dto.sealed = true;
    let descriptor = StreamDesc::try_from(dto).unwrap();
    assert!(matches!(descriptor.lifecycle(), Lifecycle::Sealed));
    assert_eq!(
        descriptor.epoch(),
        [
            0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0xaa, 0xbb, 0xcc, 0xdd,
            0xee, 0xff
        ]
    );
    assert!(descriptor.segment_route_by_id(999).is_none());
    assert!(descriptor.resolve_segment("key").sealed);
}

fn tp() -> crate::tenant::ProjectId {
    crate::tenant::ProjectId::new("proj-test").unwrap()
}
/// MULTITENANCY §19 "Identity": two projects owning the same name
/// share NOTHING — paths, route hashes, storage hashes, segment
/// identities all differ — and the workspace is not an input to
/// any of them (transfer changes none of these values).
#[test]
fn same_name_two_projects_share_no_identity() {
    let pa = crate::tenant::ProjectId::new("proj-a").unwrap();
    let pb = crate::tenant::ProjectId::new("proj-b").unwrap();
    let mk = |p: &crate::tenant::ProjectId| {
        let mut d = desc("orders", "00000000000000000000000000000001", false);
        d.project_id = p.clone();
        d
    };
    let (da, db) = (mk(&pa), mk(&pb));
    // Registry paths.
    assert_ne!(
        desc_path("cell", &da.sref()).to_string(),
        desc_path("cell", &db.sref()).to_string()
    );
    // Route, storage, and dynamic-segment identities.
    assert_ne!(
        crate::crypto::RouteHash::for_stream(&da.sref()),
        crate::crypto::RouteHash::for_stream(&db.sref())
    );
    assert_ne!(da.storage_hash(), db.storage_hash());
    assert_ne!(
        da.dynamic_segment_identity(3),
        db.dynamic_segment_identity(3)
    );
    // resolve_segment end to end: same key, disjoint physical
    // coordinates.
    let (ra, rb) = (da.resolve_segment("user-1"), db.resolve_segment("user-1"));
    assert_ne!(ra.identity, rb.identity);
    assert_ne!(ra.shard_route, rb.shard_route);
    // Same project + name + epoch = identical (the stable identity).
    assert_eq!(mk(&pa).storage_hash(), da.storage_hash());
    // And the catalog scan prefixes are disjoint by construction.
    assert_ne!(project_streams_prefix(&pa), project_streams_prefix(&pb));
    assert!(!project_streams_prefix(&pa).starts_with(&project_streams_prefix(&pb)));
}

fn ts(name: &str) -> crate::tenant::TenantStreamRef {
    crate::tenant::TenantStreamRef::new(
        tp(),
        crate::tenant::CanonicalStreamName::new(name).unwrap(),
    )
}

pub(super) fn desc(name: &str, epoch: &str, deleted: bool) -> PersistedDescriptor {
    PersistedDescriptor {
        seal_gen_counter: 0,
        account_id: None,
        project_id: tp(),
        name: name.into(),
        stream_epoch: epoch.into(),
        key_fingerprint: "fp".into(),
        created_ms: 1,
        expires_at_ms: None,
        deleted,
        content_type: "application/json".into(),
        ttl_secs: None,
        segments: None,
        sealed: false,
        watch_definitions: Vec::new(),
        watch_sig_key: None,
        parent_ref_pending: false,
        soft_deleted: false,
        logical_close_ms: None,
        forked_from: None,
        fork_children: Vec::new(),
        init: None,
        sealing: None,
        seal_op: None,
        layout_version: LAYOUT_VERSION,
    }
}

/// Pre-launch clean switch: a descriptor written by the previous
/// experimental layout (no layout_version, or any other value) is
/// REFUSED — never decoded, translated, or rewritten.
#[tokio::test]
async fn layout_gate_refuses_foreign_namespaces() {
    let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let reg = Registry::new(
        store.clone(),
        &crate::tenant::CellId::new("test-cell").unwrap(),
    );
    // Old-shape descriptor: valid JSON, no layout_version field.
    let old = serde_json::json!({
        "name": "legacy",
        "stream_epoch": "00000000000000000000000000000000",
        "key_fingerprint": "fp",
        "created_ms": 1,
        "profile": "queue",
        "content_type": "application/json",
    });
    put_raw(&store, "legacy", old.to_string().as_bytes()).await;
    let err = reg.get(&ts("legacy")).await.expect_err("gate must refuse");
    assert!(
        err.to_string().contains("unsupported_storage_layout"),
        "wrong refusal: {err}"
    );
    // A current-layout descriptor round-trips.
    let d = desc("fresh", "00000000000000000000000000000001", false);
    put_raw(&store, "fresh", &serde_json::to_vec(&d).unwrap()).await;
    assert!(reg.get(&ts("fresh")).await.unwrap().is_some());
}

async fn put_raw(store: &Arc<dyn ObjectStore>, name: &str, body: &[u8]) {
    store
        .put(
            &desc_path("test-cell", &ts(name)),
            object_store::PutPayload::from(body.to_vec()),
        )
        .await
        .unwrap();
}

/// Wrapper that counts get traffic and how it resolved, so the
/// conditional-refresh path is provable rather than assumed.
#[derive(Debug)]
struct CountingStore {
    inner: Arc<dyn ObjectStore>,
    gets: std::sync::atomic::AtomicU64,
    conditional: std::sync::atomic::AtomicU64,
    not_modified: std::sync::atomic::AtomicU64,
    puts: std::sync::atomic::AtomicU64,
    omit_etag: std::sync::atomic::AtomicBool,
    lose_put_reply: std::sync::atomic::AtomicBool,
}
impl std::fmt::Display for CountingStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "CountingStore")
    }
}
#[async_trait::async_trait]
impl ObjectStore for CountingStore {
    async fn put_opts(
        &self,
        location: &ObjPath,
        payload: object_store::PutPayload,
        opts: object_store::PutOptions,
    ) -> object_store::Result<object_store::PutResult> {
        use std::sync::atomic::Ordering::SeqCst;
        self.puts.fetch_add(1, SeqCst);
        let result = self.inner.put_opts(location, payload, opts).await?;
        if self.lose_put_reply.swap(false, SeqCst) {
            return Err(object_store::Error::Generic {
                store: "test",
                source: "arbitrary message after accepted PUT".into(),
            });
        }
        Ok(result)
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
        use std::sync::atomic::Ordering::Relaxed;
        self.gets.fetch_add(1, Relaxed);
        if options.if_none_match.is_some() {
            self.conditional.fetch_add(1, Relaxed);
        }
        let mut r = self.inner.get_opts(location, options).await;
        if self.omit_etag.load(Relaxed)
            && let Ok(result) = &mut r
        {
            result.meta.e_tag = None;
        }
        if matches!(&r, Err(object_store::Error::NotModified { .. })) {
            self.not_modified.fetch_add(1, Relaxed);
        }
        r
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

#[tokio::test]
async fn r08_mutation_preserves_conditional_metadata_and_classifies_ambiguous_completion() {
    use std::sync::atomic::Ordering::SeqCst;
    let inner: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let store = Arc::new(CountingStore {
        inner: inner.clone(),
        gets: Default::default(),
        conditional: Default::default(),
        not_modified: Default::default(),
        puts: Default::default(),
        omit_etag: Default::default(),
        lose_put_reply: Default::default(),
    });
    let reg = Registry::new(
        store.clone(),
        &crate::tenant::CellId::new("test-cell").unwrap(),
    );
    let epoch = "00000000000000000000000000000001";
    reg.create(desc("conditional", epoch, false)).await.unwrap();
    let decide = |current: &StreamDesc| {
        let mut next = current.to_persisted();
        next.seal_gen_counter += 1;
        Mutation::Write(next, ())
    };
    let puts = store.puts.load(SeqCst);
    store.omit_etag.store(true, SeqCst);
    assert!(matches!(
        reg.mutate_incarnation(&ts("conditional"), epoch, decide)
            .await,
        Err(MutationError::MissingConditionalToken(_))
    ));
    assert_eq!(
        store.puts.load(SeqCst),
        puts,
        "missing metadata cannot issue a PUT"
    );
    store.omit_etag.store(false, SeqCst);
    store.lose_put_reply.store(true, SeqCst);
    assert!(matches!(
        reg.mutate_incarnation(&ts("conditional"), epoch, decide)
            .await,
        Err(MutationError::AmbiguousCompletion(_))
    ));
    assert_eq!(
        store.puts.load(SeqCst),
        puts + 1,
        "an ambiguous write must not be retried"
    );
    reg.invalidate(&ts("conditional"));
    assert_eq!(
        reg.get(&ts("conditional"))
            .await
            .unwrap()
            .unwrap()
            .seal_gen_counter,
        1,
        "the lost reply's PUT actually landed"
    );
    put_raw(&inner, "conditional", b"malformed persisted state").await;
    assert!(matches!(
        reg.mutate_incarnation(&ts("conditional"), epoch, decide)
            .await,
        Err(MutationError::InvalidData(_))
    ));
    assert_eq!(
        store.puts.load(SeqCst),
        puts + 1,
        "corruption cannot replace data"
    );
    assert!(matches!(
        reg.mutate_incarnation(&ts("missing"), epoch, decide).await,
        Ok(MutationResult::Missing)
    ));
}

/// MUTATION CANARY for the typed incarnation API. A real
/// first-attempt CAS conflict — with the descriptor genuinely
/// changed underneath — must make `decide` re-run against the NEW
/// state, and only the winning attempt's verdict may escape. This
/// is the round-14 `release_fork_ref` bug, made structurally
/// impossible: `decide` is pure, so there is no out-parameter to
/// leak from the lost attempt.
///
/// Scenario: a soft-deleted source with one child C. Attempt 1
/// removes C, sees no children, decides to TOMBSTONE — and loses
/// the CAS to a concurrent install of child D. Attempt 2 removes C,
/// sees D remain, decides NOT to tombstone. The result must be
/// "removed, not tombstoned"; the source must survive for D.
#[tokio::test]
async fn typed_mutation_never_leaks_a_lost_attempts_decision() {
    let inner: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let conflict = Arc::new(ConflictOnceStore {
        inner: inner.clone(),
        armed: std::sync::atomic::AtomicBool::new(false),
        inject: std::sync::Mutex::new(None),
    });
    let reg = Registry::new(
        conflict.clone(),
        &crate::tenant::CellId::new("test-cell").unwrap(),
    );
    let mut src = desc("src", "00000000000000000000000000000001", false);
    src.soft_deleted = true;
    src.fork_children = vec!["C".into()];
    reg.create(src).await.unwrap();

    // The concurrent install that attempt 1 will lose to: the same
    // descriptor with child D added (and C still present, since
    // attempt 1 hasn't committed its removal).
    let mut installed = desc("src", "00000000000000000000000000000001", false);
    installed.soft_deleted = true;
    installed.fork_children = vec!["C".into(), "D".into()];
    *conflict.inject.lock().unwrap() = Some(serde_json::to_vec(&installed).unwrap());
    conflict
        .armed
        .store(true, std::sync::atomic::Ordering::SeqCst);

    let outcome = reg
        .mutate_incarnation(&ts("src"), "00000000000000000000000000000001", |x| {
            let mut next = x.to_persisted();
            let before = next.fork_children.len();
            next.fork_children.retain(|c| c != "C");
            let removed = next.fork_children.len() != before;
            let should_tombstone =
                next.fork_children.is_empty() && next.soft_deleted && !next.deleted;
            if should_tombstone {
                next.deleted = true;
                next.soft_deleted = false;
            }
            Mutation::Write(next, (removed, should_tombstone))
        })
        .await
        .unwrap();

    match outcome {
        MutationResult::Applied((removed, tombstoned)) => {
            assert!(removed, "C should have been removed");
            assert!(
                !tombstoned,
                "the lost attempt's tombstone decision leaked into the result"
            );
        }
        other => panic!("expected Applied, got {other:?}"),
    }
    let after = reg.get(&ts("src")).await.unwrap().unwrap();
    assert!(
        !after.deleted,
        "the source was tombstoned while D still forks it"
    );
    assert_eq!(after.fork_children, vec!["D".to_string()]);
}

/// Wraps a store to fail the FIRST `put_opts` with a precondition
/// error, after first writing an injected body directly to the
/// backend — simulating a concurrent writer that wins the CAS.
#[derive(Debug)]
struct ConflictOnceStore {
    inner: Arc<dyn ObjectStore>,
    armed: std::sync::atomic::AtomicBool,
    inject: std::sync::Mutex<Option<Vec<u8>>>,
}
impl std::fmt::Display for ConflictOnceStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ConflictOnceStore")
    }
}
#[async_trait::async_trait]
impl ObjectStore for ConflictOnceStore {
    async fn put_opts(
        &self,
        location: &ObjPath,
        payload: object_store::PutPayload,
        opts: object_store::PutOptions,
    ) -> object_store::Result<object_store::PutResult> {
        use std::sync::atomic::Ordering::SeqCst;
        if self.armed.swap(false, SeqCst) {
            // The concurrent winner lands first... (take the body
            // out of the lock BEFORE awaiting — a MutexGuard may
            // not cross an await point).
            let body = self.inject.lock().unwrap().take();
            if let Some(body) = body {
                self.inner
                    .put(location, object_store::PutPayload::from(body))
                    .await?;
            }
            // ...so our conditional put loses.
            return Err(object_store::Error::Precondition {
                path: location.to_string(),
                source: "conflict-once".into(),
            });
        }
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

/// A TTL refresh of an unchanged descriptor must be a conditional GET
/// answered 304 (uncharged on Tigris), never a billable body fetch —
/// and a genuinely changed descriptor must still come through.
#[tokio::test]
async fn ttl_refresh_of_unchanged_descriptor_is_a_free_304() {
    use std::sync::atomic::Ordering::Relaxed;
    let counting = Arc::new(CountingStore {
        inner: Arc::new(object_store::memory::InMemory::new()),
        gets: Default::default(),
        conditional: Default::default(),
        not_modified: Default::default(),
        puts: Default::default(),
        omit_etag: Default::default(),
        lose_put_reply: Default::default(),
    });
    let reg = Registry::new(
        counting.clone(),
        &crate::tenant::CellId::new("test-cell").unwrap(),
    );
    let (created, _) = reg
        .create(desc("s", "00000000000000000000000000000001", false))
        .await
        .unwrap();
    assert!(created);

    // Warm read: cache hit, no store traffic at all.
    assert_eq!(
        reg.get(&ts("s")).await.unwrap().unwrap().stream_epoch,
        "00000000000000000000000000000001"
    );
    assert_eq!(
        counting.gets.load(Relaxed),
        0,
        "warm read touched the store"
    );

    // TTL expiry on an unchanged descriptor: exactly one conditional
    // GET, answered 304, still serving the cached descriptor.
    reg.expire_for_tests(&ts("s"));
    assert_eq!(
        reg.get(&ts("s")).await.unwrap().unwrap().stream_epoch,
        "00000000000000000000000000000001"
    );
    assert_eq!(
        counting.conditional.load(Relaxed),
        1,
        "refresh was not conditional"
    );
    assert_eq!(
        counting.not_modified.load(Relaxed),
        1,
        "refresh paid for a body"
    );

    // The 304 renews the TTL: the next read is a cache hit again.
    let gets_now = counting.gets.load(Relaxed);
    assert_eq!(
        reg.get(&ts("s")).await.unwrap().unwrap().stream_epoch,
        "00000000000000000000000000000001"
    );
    assert_eq!(
        counting.gets.load(Relaxed),
        gets_now,
        "304 did not renew the TTL"
    );

    // A real change (delete tombstone) must come through on the next
    // refresh — the conditional path must never pin a stale view.
    reg.update(&ts("s"), |d| d.deleted = true).await.unwrap();
    reg.expire_for_tests(&ts("s"));
    // update() invalidates, so re-prime the cache then expire it.
    assert!(reg.get(&ts("s")).await.unwrap().unwrap().deleted);
    reg.expire_for_tests(&ts("s"));
    assert!(reg.get(&ts("s")).await.unwrap().unwrap().deleted);
}

#[tokio::test]
async fn catalog_provider_progress_survives_empty_filtered_pages_and_prefetch() {
    let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let reg = Registry::new(
        store,
        &crate::tenant::CellId::new("catalog-budget").unwrap(),
    );
    for n in 0..100 {
        reg.create(desc(
            &format!("stream-{n:03}"),
            "00000000000000000000000000000001",
            n < 90,
        ))
        .await
        .unwrap();
    }
    let first = reg.list_page(&tp(), None, 2).await.unwrap();
    assert!(first.streams.is_empty());
    assert!(first.next_after.is_some());
    assert!(!first.exhausted);
    let mut after = first.next_after;
    let mut names = Vec::new();
    loop {
        let page = reg.list_page(&tp(), after.as_deref(), 2).await.unwrap();
        names.extend(page.streams.iter().map(|d| d.name.clone()));
        if page.exhausted {
            break;
        }
        assert_ne!(page.next_after, after);
        after = page.next_after;
    }
    assert_eq!(
        names,
        (90..100)
            .map(|n| format!("stream-{n:03}"))
            .collect::<Vec<_>>()
    );
    assert!(reg.list_page_raw(&tp(), None, 0).await.is_err());
    let mut after = None;
    let mut all = Vec::new();
    loop {
        let page = reg.list_page_raw(&tp(), after.as_deref(), 3).await.unwrap();
        all.extend(page.streams.iter().map(|d| d.name.clone()));
        if page.exhausted {
            break;
        }
        after = page.next_after;
    }
    assert_eq!(
        all,
        (0..100)
            .map(|n| format!("stream-{n:03}"))
            .collect::<Vec<_>>()
    );
}

/// Round-22 item 7: the tombstone write carries the logical close
/// stamp durably, and the RAW catalog page — the reconciler's view
/// — returns tombstoned and expired descriptors that the customer
/// catalog hides.
#[tokio::test]
async fn tombstone_stamp_persists_and_raw_page_sees_terminals() {
    let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let reg = Registry::new(
        store.clone(),
        &crate::tenant::CellId::new("test-cell").unwrap(),
    );
    reg.create(desc("alive", "00000000000000000000000000000001", false))
        .await
        .unwrap();
    reg.create(desc("gone", "00000000000000000000000000000002", false))
        .await
        .unwrap();
    let mut ex = desc("expired", "00000000000000000000000000000003", false);
    ex.expires_at_ms = Some(1); // long past
    reg.create(ex).await.unwrap();
    // Tombstone with the stamp in the SAME write.
    reg.update(&ts("gone"), |d| {
        d.deleted = true;
        d.logical_close_ms = Some(1_786_000_000_000);
    })
    .await
    .unwrap();
    reg.invalidate(&ts("gone"));
    let got = reg.get(&ts("gone")).await.unwrap().unwrap();
    assert!(got.deleted);
    assert_eq!(
        got.logical_close_ms,
        Some(1_786_000_000_000),
        "the debt survives on the tombstone"
    );
    // Customer catalog: only the live stream.
    let visible = reg.list_page(&tp(), None, 10).await.unwrap();
    assert_eq!(visible.streams.len(), 1);
    assert_eq!(visible.streams[0].name, "alive");
    // Reconciler view: everything, terminals included.
    let raw = reg.list_page_raw(&tp(), None, 10).await.unwrap();
    assert_eq!(raw.streams.len(), 3, "raw page hides nothing");
    assert!(raw.streams.iter().any(|d| d.deleted));
    assert!(
        raw.streams
            .iter()
            .any(|d| d.expires_at_ms.is_some_and(|e| e < 1000)),
        "expired descriptor present"
    );
}

/// A corrupt descriptor must surface as an ERROR — treating it as
/// absent lets a create/recreate overwrite a live stream's identity.
#[tokio::test]
async fn corrupt_descriptor_fails_closed() {
    let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let reg = Registry::new(
        store.clone(),
        &crate::tenant::CellId::new("test-cell").unwrap(),
    );
    put_raw(&store, "s1", b"{ not json").await;
    assert!(
        reg.get(&ts("s1")).await.is_err(),
        "corrupt descriptor returned as absent/ok"
    );
    // update() must also refuse (was: Ok(None), i.e. missing).
    assert!(reg.update(&ts("s1"), |_| {}).await.is_err());
}

/// WP-03/PR 5 decode invariants: a descriptor whose stored
/// identities do not decode REFUSES at the boundary — never a
/// downstream `expect` panic, never a silent repair. Each case
/// starts from a VALID descriptor and corrupts one field, so the
/// refusal is attributable to that invariant alone.
#[tokio::test]
async fn corrupt_stored_identities_fail_closed_at_decode() {
    let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let reg = Registry::new(
        store.clone(),
        &crate::tenant::CellId::new("test-cell").unwrap(),
    );
    reg.create(desc("base", "00000000000000000000000000000001", false))
        .await
        .unwrap();
    let raw = store
        .get(&desc_path("test-cell", &ts("base")))
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    let valid: serde_json::Value = serde_json::from_slice(&raw).unwrap();

    type Corruption = (&'static str, Box<dyn Fn(&mut serde_json::Value)>);
    let cases: Vec<Corruption> = vec![
        (
            "fork source not canonical",
            Box::new(|d| {
                d["forked_from"] = serde_json::json!({
                    "source": "__ds/reserved",
                    "source_epoch": "00".repeat(16),
                    "fork_offset": 0,
                    "fork_sub": 0,
                    "fork_id": "f1",
                });
            }),
        ),
        (
            "fork source_epoch short",
            Box::new(|d| {
                d["forked_from"] = serde_json::json!({
                    "source": "ok-name",
                    "source_epoch": "0000",
                    "fork_offset": 0,
                    "fork_sub": 0,
                    "fork_id": "f1",
                });
            }),
        ),
        (
            "fork child not canonical",
            Box::new(|d| {
                d["fork_children"] = serde_json::json!(["bad//child"]);
            }),
        ),
    ];
    for (why, mutate) in cases {
        let mut c = valid.clone();
        mutate(&mut c);
        put_raw(&store, "base", c.to_string().as_bytes()).await;
        // A FRESH registry per case: the writer registry's 5s
        // descriptor cache would otherwise serve the pre-corruption
        // value and mask the decode check.
        let fresh = Registry::new(
            store.clone(),
            &crate::tenant::CellId::new("test-cell").unwrap(),
        );
        let err = fresh.get(&ts("base")).await;
        assert!(err.is_err(), "{why}: corrupt descriptor must refuse");
        let msg = format!("{}", err.err().unwrap());
        assert!(
            msg.contains("corruption"),
            "{why}: refusal must name corruption: {msg}"
        );
    }
    // And the untouched valid form still decodes (the checks refuse
    // corruption, not legitimate descriptors).
    put_raw(&store, "base", valid.to_string().as_bytes()).await;
    let fresh = Registry::new(
        store.clone(),
        &crate::tenant::CellId::new("test-cell").unwrap(),
    );
    assert!(fresh.get(&ts("base")).await.is_ok());
}

/// A corrupt topology must abort boot, never panic and NEVER be treated
/// as missing (re-initializing re-shards the whole keyspace).
#[tokio::test]
async fn corrupt_topology_fails_closed() {
    let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    store
        .put(
            &ObjPath::from(TOPOLOGY_PATH),
            object_store::PutPayload::from(b"garbage".to_vec()),
        )
        .await
        .unwrap();
    assert!(
        load_or_init_topology(
            &store,
            crate::config::validation::InitialShards::new(4).unwrap(),
            crate::protocol_pin::MAX_BODY_BYTES,
        )
        .await
        .is_err()
    );
    // The corrupt object must still be there — not replaced by a fresh
    // initialization.
    let raw = store
        .get(&ObjPath::from(TOPOLOGY_PATH))
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    assert_eq!(&raw[..], b"garbage");
}

/// Racing recreators of a dead incarnation: exactly one winner; the
/// loser observes the winner's descriptor instead of overwriting it.
#[tokio::test]
async fn recreate_race_has_one_winner() {
    let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let reg = Registry::new(
        store.clone(),
        &crate::tenant::CellId::new("test-cell").unwrap(),
    );
    let (created, _) = reg
        .create(desc("s", "00000000000000000000000000000006", true))
        .await
        .unwrap();
    assert!(created);

    let alive = |d: &StreamDesc| !d.deleted;
    let (won_a, got_a) = reg
        .recreate(
            &ts("s"),
            desc("s", "00000000000000000000000000000007", false),
            |d| !alive(d),
        )
        .await
        .unwrap();
    assert!(won_a, "first recreate must win");
    assert_eq!(got_a.stream_epoch, "00000000000000000000000000000007");

    // Second recreator raced and lost: descriptor is now alive, so the
    // predicate fails and it must observe epoch-a, not install epoch-b.
    let (won_b, got_b) = reg
        .recreate(
            &ts("s"),
            desc("s", "00000000000000000000000000000008", false),
            |d| !alive(d),
        )
        .await
        .unwrap();
    assert!(!won_b, "second recreate must lose");
    assert_eq!(got_b.stream_epoch, "00000000000000000000000000000007");

    reg.invalidate(&ts("s"));
    let stored = reg.get(&ts("s")).await.unwrap().unwrap();
    assert_eq!(
        stored.stream_epoch, "00000000000000000000000000000007",
        "loser overwrote the winner"
    );
}
