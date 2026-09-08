use super::*;
use crate::{
    crypto::{FrameCipher, FrameCompression, RouteHash, SegmentHash},
    history::{canonical_span, hist2_record_key},
    shard::{ShardConfig, record::CheckedFrame},
};
use bytes::Bytes;
use std::time::Duration;

fn descriptor(project: &str) -> StreamDesc {
    crate::registry::PersistedDescriptor {
        name: "canonical-cache".into(),
        account_id: None,
        project_id: ProjectId::new(project).unwrap(),
        stream_epoch: "11".repeat(16),
        seal_gen_counter: 0,
        key_fingerprint: String::new(),
        created_ms: 0,
        expires_at_ms: None,
        deleted: false,
        soft_deleted: false,
        logical_close_ms: None,
        forked_from: None,
        fork_children: vec![],
        init: None,
        sealing: None,
        seal_op: None,
        content_type: "application/octet-stream".into(),
        ttl_secs: None,
        segments: None,
        sealed: false,
        watch_definitions: vec![],
        watch_sig_key: None,
        parent_ref_pending: false,
        layout_version: crate::registry::LAYOUT_VERSION,
    }
    .try_into()
    .unwrap()
}
struct Rig {
    engine: Arc<ShardEngine>,
    part: Arc<Db>,
    desc: StreamDesc,
    cache: SpanCache,
}
impl Rig {
    async fn new() -> Self {
        let store = Arc::new(object_store::memory::InMemory::new());
        let db = Arc::new(
            Db::builder("span-test", store.clone())
                .with_settings(slatedb::config::Settings {
                    flush_interval: Some(Duration::from_millis(5)),
                    ..Default::default()
                })
                .build()
                .await
                .unwrap(),
        );
        let resources = Arc::new(crate::history::HistoryResources::new(
            &crate::config::HistoryConfig {
                canonical_span_cache: true,
                ..Default::default()
            },
            1 << 16,
        ));
        let (tx, _rx) = crate::history::absorber_channel();
        let engine = ShardEngine::start(
            "span-test".into(),
            db,
            store,
            ShardConfig {
                shared_history: Some(resources.clone()),
                ..Default::default()
            },
            tx,
            None,
            Default::default(),
        );
        let part = engine.history_partition().await.unwrap();
        Self {
            engine,
            part,
            desc: descriptor("project-a"),
            cache: resources.spans.clone(),
        }
    }
    fn scope_for(&self, desc: &StreamDesc) -> Arc<Scope> {
        self.cache
            .scope(
                desc,
                &self.engine,
                &self.part,
                &desc.epoch(),
                desc.segment_route_by_id(0).unwrap(),
                desc.storage_hash(),
            )
            .unwrap()
    }
    fn scope(&self) -> Arc<Scope> {
        self.scope_for(&self.desc)
    }
    fn frame(&self, off: u64, len: usize, rk: &str) -> CheckedFrame {
        let raw = Bytes::from(
            FrameCipher::new(
                &[7; 32],
                &self.desc.storage_hash(),
                FrameCompression::Disabled,
            )
            .encrypt(&self.desc.storage_hash(), off, 1, 1, rk, &vec![b'x'; len]),
        );
        let key = hist2_record_key(
            RouteHash(self.desc.segment_route_by_id(0).unwrap()),
            SegmentHash(self.desc.storage_hash()),
            off,
        );
        CheckedFrame::from_row(&key, &key[..33], raw).unwrap()
    }
    async fn store(&self, frame: &CheckedFrame) {
        let durable = self
            .part
            .put(
                hist2_record_key(
                    RouteHash(self.desc.segment_route_by_id(0).unwrap()),
                    SegmentHash(self.desc.storage_hash()),
                    frame.view().header.offset,
                ),
                frame.as_ref(),
            )
            .await
            .unwrap();
        self.part.flush().await.unwrap();
        durable.await_durable().await.unwrap();
    }
    async fn read(
        &self,
        scope: &Arc<Scope>,
        from: u64,
        to: u64,
        rk: &str,
        budget: usize,
    ) -> canonical_span::ResultPage {
        canonical_span::read(
            &self.part,
            RouteHash(self.desc.segment_route_by_id(0).unwrap()),
            SegmentHash(self.desc.storage_hash()),
            rk,
            crate::postings::Span {
                start: from,
                end: to,
                scan_bytes: 4096,
                matching_bytes: 4096,
            },
            budget,
            Some(scope),
            to,
        )
        .await
        .unwrap()
    }
    async fn close(self) {
        let completion = self.engine.shutdown_handle();
        self.engine.begin_close();
        completion.wait(Duration::from_secs(5)).await.unwrap();
    }
}
fn fill(scope: &Arc<Scope>, from: u64, to: u64) -> Capture {
    match scope.acquire(from, to, to, FILL as u64) {
        Access::Fill(f) => f,
        _ => panic!("expected new fill"),
    }
}
fn hit(scope: &Arc<Scope>, from: u64, to: u64) -> Arc<CipherSpan> {
    match scope.acquire(from, to, to, FILL as u64) {
        Access::Hit(f) => f,
        _ => panic!("expected complete proof"),
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn o5_complete_scan_reuses_misses_and_negative_space_but_authenticates_each_response() {
    let rig = Rig::new().await;
    let scope = rig.scope();
    for (off, rk) in [(1, "wanted"), (3, "miss")] {
        rig.store(&rig.frame(off, 32, rk)).await;
    }
    for _ in 0..2 {
        let page = rig.read(&scope, 0, 5, "wanted", 4096).await;
        assert_eq!(page.hits.iter().map(|h| h.0).collect::<Vec<_>>(), [1]);
        assert_eq!(page.last, Some(3));
        assert!(!page.truncated);
        let frame = &page.hits[0].1;
        assert_eq!(
            crate::crypto::decrypt_frame(&[7; 32], &rig.desc.storage_hash(), &frame.view(), frame)
                .unwrap(),
            vec![b'x'; 32]
        );
        assert!(
            crate::crypto::decrypt_frame(&[9; 32], &rig.desc.storage_hash(), &frame.view(), frame)
                .is_err()
        );
    }
    assert_eq!(
        hit(&scope, 0, 5).frames().len(),
        2,
        "filtered misses remain part of proof"
    );
    let miss = rig.read(&scope, 0, 5, "absent", 4096).await;
    assert!(miss.hits.is_empty());
    assert_eq!(miss.last, Some(3));
    assert!(!miss.truncated);
    let empty = rig.read(&scope, 8, 9, "wanted", 4096).await;
    assert!(empty.hits.is_empty());
    assert!(!empty.truncated);
    assert!(hit(&scope, 8, 9).frames().is_empty());
    assert!(
        matches!(scope.acquire(0, 5, 4, FILL as u64), Access::Bypass),
        "applied-only boundary cannot be cached"
    );
    assert!(
        matches!(scope.acquire(0, 4, 5, FILL as u64), Access::Fill(_)),
        "exact interval is required"
    );
    rig.close().await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn o5_partial_oversized_and_corrupt_scans_never_install_complete_proof() {
    let rig = Rig::new().await;
    let scope = rig.scope();
    rig.store(&rig.frame(0, 128, "wanted")).await;
    rig.store(&rig.frame(1, 128, "miss")).await;
    let page = rig.read(&scope, 0, 2, "wanted", 200).await;
    assert!(page.truncated);
    assert_eq!(page.last, Some(0));
    assert!(matches!(
        scope.acquire(0, 2, 2, FILL as u64),
        Access::Fill(_)
    ));
    rig.store(&rig.frame(3, FILL, "wanted")).await;
    assert!(!rig.read(&scope, 3, 4, "wanted", FILL * 2).await.truncated);
    assert!(matches!(
        scope.acquire(3, 4, 4, FILL as u64),
        Access::Fill(_)
    ));
    let key = hist2_record_key(RouteHash(scope.route), SegmentHash(scope.inc), 5);
    let durable = rig.part.put(key, b"bad frame").await.unwrap();
    rig.part.flush().await.unwrap();
    durable.await_durable().await.unwrap();
    let bad = canonical_span::read(
        &rig.part,
        RouteHash(scope.route),
        SegmentHash(scope.inc),
        "wanted",
        crate::postings::Span {
            start: 5,
            end: 6,
            scan_bytes: 1,
            matching_bytes: 1,
        },
        4096,
        Some(&scope),
        6,
    )
    .await;
    assert!(bad.is_err());
    assert!(matches!(
        scope.acquire(5, 6, 6, FILL as u64),
        Access::Fill(_)
    ));
    let warm = rig.read(&scope, 0, 2, "wanted", 4096).await;
    assert!(!warm.truncated);
    let restricted = rig.read(&scope, 0, 2, "wanted", 200).await;
    assert!(restricted.truncated);
    assert_eq!(restricted.last, Some(0));
    rig.close().await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn o5_fill_cancellation_and_invalidation_wake_waiters_and_fence_late_publication() {
    let rig = Rig::new().await;
    let scope = rig.scope();
    let baseline = rig.cache.reserved();
    let pending = fill(&scope, 0, 2);
    let Access::Wait(waiter) = scope.acquire(0, 2, 2, FILL as u64) else {
        panic!("identical fill must coalesce")
    };
    assert!(rig.cache.reserved() > baseline);
    drop(pending);
    tokio::time::timeout(Duration::from_millis(100), waiter.wait())
        .await
        .unwrap();
    assert_eq!(rig.cache.reserved(), baseline);
    let mut pending = fill(&scope, 0, 2);
    assert!(pending.push(&rig.frame(1, 32, "wanted")));
    let Access::Wait(waiter) = scope.acquire(0, 2, 2, FILL as u64) else {
        panic!()
    };
    rig.cache.invalidate_project(&rig.desc.project_id);
    tokio::time::timeout(Duration::from_millis(100), waiter.wait())
        .await
        .unwrap();
    pending.complete();
    assert!(matches!(
        scope.acquire(0, 2, 2, FILL as u64),
        Access::Bypass
    ));
    let fresh = rig.scope();
    assert!(matches!(
        fresh.acquire(0, 2, 2, FILL as u64),
        Access::Fill(_)
    ));
    drop(fresh);
    assert_eq!(rig.cache.reserved(), baseline);
    rig.close().await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn o5_eviction_charge_follows_last_ciphertext_slice_and_project_quota() {
    let rig = Rig::new().await;
    let scope = rig.scope();
    let baseline = rig.cache.reserved();
    let mut pending = fill(&scope, 0, 1);
    assert!(pending.push(&rig.frame(0, 8192, "wanted")));
    pending.complete();
    let owner = hit(&scope, 0, 1);
    let bytes = Bytes::from(owner.frames()[0].clone()).slice(0..1);
    let charged = rig.cache.reserved();
    assert!(charged >= baseline + 8192);
    rig.cache.invalidate_project(&rig.desc.project_id);
    drop(owner);
    assert_eq!(
        rig.cache.reserved(),
        charged,
        "eviction and partial slice must retain full charge"
    );
    drop(bytes);
    assert_eq!(rig.cache.reserved(), baseline);
    // Pending and retained scopes share ONE project allowance, including after
    // all cache entries of that project have been removed.
    let scope = rig.scope();
    let mut pending = Vec::new();
    for n in 0..10 {
        if let Access::Fill(f) = scope.acquire(n, n + 1, n + 1, FILL as u64) {
            pending.push(f)
        }
    }
    assert_eq!(pending.len(), 3);
    assert!(matches!(
        scope.acquire(20, 21, 21, FILL as u64),
        Access::Bypass
    ));
    let other = rig.scope_for(&descriptor("project-b"));
    assert!(matches!(
        other.acquire(0, 1, 1, FILL as u64),
        Access::Fill(_)
    ));
    drop(pending);
    rig.close().await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn o5_retirement_and_db_close_reject_pending_and_ready_admission() {
    let rig = Rig::new().await;
    let scope = rig.scope();
    let pending = fill(&scope, 0, 1);
    let Access::Wait(waiter) = scope.acquire(0, 1, 1, FILL as u64) else {
        panic!()
    };
    rig.engine.begin_close();
    pending.complete();
    tokio::time::timeout(Duration::from_millis(100), waiter.wait())
        .await
        .unwrap();
    assert!(matches!(
        scope.acquire(0, 1, 1, FILL as u64),
        Access::Bypass
    ));
    rig.close().await;
    let rig = Rig::new().await;
    let scope = rig.scope();
    let pending = fill(&scope, 0, 1);
    rig.part.close().await.unwrap();
    pending.complete();
    assert!(matches!(
        scope.acquire(0, 1, 1, FILL as u64),
        Access::Bypass
    ));
    rig.close().await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn o5_descriptor_opening_incarnation_and_runtime_are_part_of_admission() {
    let rig = Rig::new().await;
    let scope = rig.scope();
    let foreign = descriptor("foreign");
    for (desc, epoch, route, inc) in [
        (&foreign, rig.desc.epoch(), scope.route, scope.inc),
        (&rig.desc, [9; 16], scope.route, scope.inc),
        (&rig.desc, rig.desc.epoch(), [0; 16], scope.inc),
        (&rig.desc, rig.desc.epoch(), scope.route, [0; 16]),
    ] {
        assert!(
            rig.cache
                .scope(desc, &rig.engine, &rig.part, &epoch, route, inc)
                .is_none()
        );
    }
    assert!(
        rig.cache
            .scope(
                &rig.desc,
                &rig.engine,
                &rig.engine.db,
                &rig.desc.epoch(),
                scope.route,
                scope.inc
            )
            .is_none()
    );
    assert!(
        SpanCache::new(true)
            .scope(
                &rig.desc,
                &rig.engine,
                &rig.part,
                &rig.desc.epoch(),
                scope.route,
                scope.inc
            )
            .is_none()
    );
    let mut scopes = vec![scope];
    for i in 1..PROJECTS {
        scopes.push(rig.scope_for(&descriptor(&format!("project-{i}"))))
    }
    let extra = descriptor("overflow");
    assert!(
        rig.cache
            .scope(
                &extra,
                &rig.engine,
                &rig.part,
                &extra.epoch(),
                extra.segment_route_by_id(0).unwrap(),
                extra.storage_hash()
            )
            .is_none()
    );
    scopes.pop();
    assert!(
        rig.cache
            .scope(
                &extra,
                &rig.engine,
                &rig.part,
                &extra.epoch(),
                extra.segment_route_by_id(0).unwrap(),
                extra.storage_hash()
            )
            .is_some()
    );
    assert!(rig.cache.reserved() <= CAPACITY);
    drop(scopes);
    assert_eq!(rig.cache.reserved(), METADATA);
    rig.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn o5_global_fill_pressure_and_retained_project_quota_remain_bounded() {
    assert_eq!(SpanCache::new(false).capacity(), 0);
    assert!(!crate::config::HistoryConfig::default().canonical_span_cache);
    let rig = Rig::new().await;
    let mut pending = Vec::new();
    let mut scopes = Vec::new();
    for i in 0..16 {
        let desc = descriptor(&format!("pressure-{i}"));
        if let Some(scope) = rig.cache.scope(
            &desc,
            &rig.engine,
            &rig.part,
            &desc.epoch(),
            desc.segment_route_by_id(0).unwrap(),
            desc.storage_hash(),
        ) {
            for n in 0..3 {
                if let Access::Fill(f) = scope.acquire(n, n + 1, n + 1, FILL as u64) {
                    pending.push(f)
                }
            }
            scopes.push(scope);
        }
        assert!(rig.cache.reserved() <= CAPACITY);
    }
    assert!(
        (29..=31).contains(&pending.len()),
        "global allowance must stop many-project fills"
    );
    drop(pending);
    drop(scopes);
    assert_eq!(rig.cache.reserved(), METADATA);
    let scope = rig.scope();
    let mut fill = fill(&scope, 0, 1);
    assert!(fill.push(&rig.frame(0, 8192, "wanted")));
    fill.complete();
    let owner = hit(&scope, 0, 1);
    let raw = Bytes::from(owner.frames()[0].clone()).slice(0..1);
    rig.cache.invalidate_project(&rig.desc.project_id);
    drop(scope);
    drop(owner);
    let charged = rig.cache.reserved();
    assert!(charged > METADATA + 8192);
    let replacement = rig.scope();
    assert!(
        replacement.project.bytes.available_permits() < PROJECT_CAP - CONTROL - 8192,
        "retained data must prevent a fresh project quota"
    );
    drop(raw);
    assert_eq!(rig.cache.reserved(), METADATA + CONTROL);
    drop(replacement);
    assert_eq!(rig.cache.reserved(), METADATA);
    rig.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn o5_executor_cannot_install_a_foreign_scan_under_a_valid_scope() {
    let rig = Rig::new().await;
    let scope = rig.scope();
    rig.store(&rig.frame(1, 32, "wanted")).await;
    for (part, route, inc) in [
        (&rig.engine.db, scope.route, scope.inc),
        (&rig.part, [9; 16], scope.inc),
        (&rig.part, scope.route, [9; 16]),
    ] {
        let result = canonical_span::read(
            part,
            RouteHash(route),
            SegmentHash(inc),
            "wanted",
            crate::postings::Span {
                start: 0,
                end: 2,
                scan_bytes: 4096,
                matching_bytes: 4096,
            },
            4096,
            Some(&scope),
            2,
        )
        .await
        .unwrap();
        assert!(result.hits.is_empty());
        assert!(!result.truncated);
        assert!(
            matches!(scope.acquire(0, 2, 2, FILL as u64), Access::Fill(_)),
            "foreign empty scan cannot become a negative-space proof for this scope"
        );
    }
    let actual = rig.read(&scope, 0, 2, "wanted", 4096).await;
    assert_eq!(actual.hits.len(), 1);
    assert_eq!(hit(&scope, 0, 2).frames().len(), 1);
    rig.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn o5_four_concurrent_spans_preserve_a_reusable_hot_set() {
    use futures_util::{StreamExt, stream};
    let rig = Rig::new().await;
    let scope = rig.scope();
    for n in 0..16 {
        rig.store(&rig.frame(n, 1024, "wanted")).await;
    }
    for _ in 0..3 {
        let pages = stream::iter((0..16).map(|n| rig.read(&scope, n, n + 1, "wanted", 4096)))
            .buffered(4)
            .collect::<Vec<_>>()
            .await;
        for (n, page) in pages.iter().enumerate() {
            assert_eq!(page.hits.len(), 1);
            assert_eq!(page.hits[0].0, n as u64);
            assert!(!page.truncated);
        }
        for n in 0..16 {
            assert_eq!(
                hit(&scope, n, n + 1).frames().len(),
                1,
                "maximum fill credits must not churn a small admitted working set"
            );
        }
    }
    assert!(rig.cache.reserved() < METADATA + PROJECT_CAP);
    rig.close().await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn o5_an_underestimated_fill_abandons_caching_before_retaining_excess_bytes() {
    let rig = Rig::new().await;
    let scope = rig.scope();
    let frame = rig.frame(0, 8192, "wanted");
    rig.store(&frame).await;
    let baseline = rig.cache.reserved();
    let Access::Fill(mut fill) = scope.acquire(0, 1, 1, 1) else {
        panic!()
    };
    assert!(rig.cache.reserved() - baseline < 8192);
    assert!(!fill.push(&frame));
    drop(fill);
    assert_eq!(rig.cache.reserved(), baseline);
    let page = canonical_span::read(
        &rig.part,
        RouteHash(scope.route),
        SegmentHash(scope.inc),
        "wanted",
        crate::postings::Span {
            start: 0,
            end: 1,
            scan_bytes: 1,
            matching_bytes: 1,
        },
        16384,
        Some(&scope),
        1,
    )
    .await
    .unwrap();
    assert_eq!(page.hits[0].1, frame);
    assert!(!page.truncated);
    assert!(matches!(scope.acquire(0, 1, 1, 1), Access::Fill(_)));
    rig.close().await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn o5_a_closed_db_cannot_serve_an_already_ready_interval() {
    let rig = Rig::new().await;
    let scope = rig.scope();
    let mut fill = fill(&scope, 0, 1);
    assert!(fill.push(&rig.frame(0, 32, "wanted")));
    fill.complete();
    assert_eq!(hit(&scope, 0, 1).frames().len(), 1);
    rig.part.close().await.unwrap();
    assert!(matches!(scope.acquire(0, 1, 1, 4096), Access::Bypass));
    rig.close().await;
}
