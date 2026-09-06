//! Crash-resumable topology transitions. Registry intent, physical closes and
//! publication remain distinct durable phases, fenced to one incarnation.
use crate::registry::{Mutation, MutationResult, Registry, StreamDesc};
use crate::scaler3::{SEGMENT_MAP_REFRESHES, SEGMENT_MERGES, SEGMENT_SPLITS, Scaler};
use crate::shard::ShardEngine;
use std::sync::Arc;

#[derive(Clone)]
pub(crate) struct TopologyService {
    pub(crate) registry: Arc<Registry>,
    pub(crate) shards: crate::shard_directory::ShardDirectory,
    pub(crate) peer: crate::peer::PeerClient,
    pub(crate) scaler: Arc<Scaler>,
}

#[async_trait::async_trait]
impl crate::application::read::TopologyResume for TopologyService {
    async fn resume(&self, stream: &crate::tenant::TenantStreamRef) {
        let _ = resume(self, stream).await;
    }
}

/// Seal one segment identity through its committer: an empty close
/// append. Idempotent — re-closing a closed identity returns the same
/// frozen next offset via AppendErr::Closed.
/// Public seal of one segment identity (product lifecycle: collection
/// seal closes every live segment).
pub(crate) async fn seal_segment_identity(
    state: &TopologyService,
    desc: &StreamDesc,
    seg_id: u32,
    seal_gen: Option<u64>,
) -> Option<u64> {
    seal_identity(state, desc, seg_id, seal_gen).await
}

async fn seal_identity(
    state: &TopologyService,
    desc: &StreamDesc,
    seg_id: u32,
    seal_gen: Option<u64>,
) -> Option<u64> {
    let identity = desc.dynamic_segment_identity(seg_id);
    // The seal must reach the engine that OWNS this segment's appends —
    // hard-coding the parent route here sealed the wrong shard for any
    // child with a real route (review blocker 1).
    let route = desc.segment_route_by_id(seg_id)?;
    // Round-4 follow-up review, finding 3: the scaler resolution used
    // to be `.ok()`ed into None, so a wrong-owner response, an owner
    // convergence holdoff, an open failure and a capacity refusal were
    // indistinguishable in the logs — exactly when the segment lived
    // on ANOTHER instance. Resolve typed and name the category.
    // Round-4 follow-up review, finding 2: WRONG OWNER is not an
    // error — it is a routing fact. Relay the close to the segment's
    // owner over the fleet-internal channel instead of failing the
    // whole collection seal.
    match state
        .shards
        .resolve(&route, crate::shard_directory::Adoption::Internal)
        .await
    {
        Ok(engine) => close_segment_on_engine(&engine, identity, &route, seg_id, seal_gen).await,
        Err(crate::shard_directory::ResolveError::NotOwner { owner, .. }) => {
            relay_segment_close(state, desc, seg_id, seal_gen, owner).await
        }
        Err(error) => {
            tracing::error!(
                seg_id,
                ?route,
                ?error,
                "seal segment engine resolution failed"
            );
            None
        }
    }
}

/// The ONE segment-close primitive: submit an empty close append for
/// this exact segment identity to the LOCAL committer. Idempotent per
/// identity (a re-close of a closed segment answers its frozen next
/// offset via AppendErr::Closed). Shared by collection sealing,
/// split/merge transitions, and the fleet-internal segment-close
/// receiver.
pub(crate) async fn close_segment_on_engine(
    engine: &std::sync::Arc<ShardEngine>,
    identity: [u8; 16],
    route: &[u8; 16],
    seg_id: u32,
    seal_gen: Option<u64>,
) -> Option<u64> {
    let (tx, rx) = tokio::sync::oneshot::channel();
    let req = crate::shard::CloseReq {
        hash: identity,
        generation: seal_gen,
        resp: tx,
    };
    if let Err(_req) = engine.try_close(req) {
        tracing::error!(
            seg_id,
            ?route,
            "seal close never enqueued (committer queue full or closed)"
        );
        return None;
    }
    match rx.await {
        Ok(Ok(ack)) => Some(ack.next_offset),
        Ok(Err(crate::shard::AppendErr::Closed { next_offset })) => Some(next_offset),
        Ok(Err(other)) => {
            tracing::error!(seg_id, "seal close refused: {other:?}");
            None
        }
        Err(_) => {
            tracing::error!(
                seg_id,
                "seal close's committer answer was dropped (responder gone)"
            );
            None
        }
    }
}

/// Relay one segment close to the segment's OWNER instance over the
/// fleet-internal channel. The peer re-derives every target fact from
/// ITS OWN descriptor before submitting (the coordinator's descriptor
/// may be stale), so this carries only the coordinates plus the bound
/// target headers. One ownership redirect is followed; a SECOND means
/// ownership moved mid-relay — fail retryable rather than chase.
async fn relay_segment_close(
    state: &TopologyService,
    desc: &StreamDesc,
    seg_id: u32,
    seal_gen: Option<u64>,
    owner: String,
) -> Option<u64> {
    let Some(base) = state.peer.url_for(&owner) else {
        tracing::error!(
            seg_id,
            stream = %desc.name,
            "segment owner answered a redirect but no peer URL is configured"
        );
        return None;
    };
    let target = crate::application::read_remote::InternalTarget::of(desc, seg_id)?;
    let gen_q = seal_gen.map(|g| g.to_string()).unwrap_or_default();
    let name = crate::peer::encode_stream_name_path(&desc.name);
    let mk = |bearer: Option<&str>| {
        let mut req = crate::peer::client()
            .post(format!(
                "{base}/v1/internal/segment-close/{name}?seg_id={seg_id}&seal_gen={gen_q}"
            ))
            .timeout(std::time::Duration::from_secs(40));
        for (k, v) in target.headers() {
            req = req.header(k, v);
        }
        if let Some(t) = bearer {
            req = req.header("authorization", format!("Bearer {t}"));
        }
        req
    };
    match state.peer.send(mk).await {
        Ok(r) if r.status() == reqwest::StatusCode::OK => {
            #[derive(serde::Deserialize)]
            struct Ack {
                next_offset: u64,
            }
            match r.json::<Ack>().await {
                Ok(a) => Some(a.next_offset),
                Err(e) => {
                    tracing::error!(
                        seg_id,
                        owner = %owner,
                        "segment-close relay answer unparsable: {e}"
                    );
                    None
                }
            }
        }
        Ok(r) if r.status() == reqwest::StatusCode::CONFLICT => {
            tracing::error!(
                seg_id,
                owner = %owner,
                "segment-close relay was redirected again; ownership moved twice — \
                 refusing to chase (retry the seal)"
            );
            None
        }
        Ok(r) => {
            let status = r.status();
            let body = r.text().await.unwrap_or_default();
            tracing::error!(
                seg_id,
                owner = %owner,
                status = %status,
                "segment-close relay refused: {}",
                &body[..body.len().min(300)]
            );
            None
        }
        Err(e) => {
            tracing::error!(
                seg_id,
                owner = %owner,
                "segment-close relay to {base} failed: {e}"
            );
            None
        }
    }
}

/// Execute (or resume) one split end-to-end. Idempotent at every step.
/// Resolves the CURRENT incarnation and splits it — the entry point for
/// direct calls that just created or inspected the stream.
#[cfg(test)]
pub(crate) async fn execute_split(
    st: &TopologyService,
    sref: &crate::tenant::TenantStreamRef,
    seg_id: u32,
    split_at: u64,
) -> bool {
    let Ok(Some(d)) = st.registry.get(sref).await else {
        return false;
    };
    execute_split_fenced(st, sref, &d.stream_epoch, seg_id, split_at).await
}

/// The fenced form: the split decision was computed from ONE
/// incarnation's segment map, and a replacement created under the same
/// name can coincidentally satisfy every structural guard (a fresh
/// stream's segment 0 is live and spans the full range, so any split
/// point "fits"). The autonomous scaler always calls this with the
/// epoch of the descriptor its decision came from.
pub(crate) async fn execute_split_fenced(
    st: &TopologyService,
    sref: &crate::tenant::TenantStreamRef,
    expect_epoch: &str,
    seg_id: u32,
    split_at: u64,
) -> bool {
    // Phase A: persist the intent (materializing the implicit map).
    let ok = st
        .registry
        .mutate_incarnation(sref, expect_epoch, |current| {
            if current.deleted {
                return Mutation::Decline(false);
            }
            let mut d = current.to_persisted();
            let changed = {
                // Fork chains stay single-segment (audit P0): stitched fork
                // reads resolve each ancestor through its ONE empty-key
                // segment, so a post-fork split would make inherited data
                // unreadable. Both a fork and a stream that HAS forks are
                // pinned — enforced HERE, at the transition itself, so a
                // direct scaler call cannot bypass it.
                if d.forked_from.is_some() || !d.fork_children.is_empty() {
                    return Mutation::Decline(false);
                }
                // A sealing or sealed collection has a fixed topology. A
                // transition that started just before the seal could
                // otherwise publish a successor AFTER the seal took its
                // snapshot of live segments — a new writable child under a
                // collection that already reports Sealed.
                if d.sealed || d.sealing.is_some() {
                    return Mutation::Decline(false);
                }
                let map = d.segments.get_or_insert_with(|| {
                    crate::segmap::SegmentMap::initial("", crate::shard::now_ms())
                });
                if map.pending.is_some() {
                    return Mutation::Decline(false); // an in-flight transition owns the map
                }
                let Some(seg) = map.get(seg_id) else {
                    return Mutation::Decline(false);
                };
                if !seg.is_live() || split_at <= seg.lo || split_at >= seg.hi {
                    return Mutation::Decline(false);
                }
                map.pending = Some(crate::segmap::PendingTransition {
                    kind: "split".into(),
                    segs: vec![seg_id],
                    split_at,
                    started_ms: crate::shard::now_ms(),
                    seal_gen: 0, // patched below: needs the counter
                });
                map.version += 1;
                d.seal_gen_counter += 1;
                let g = d.seal_gen_counter;
                if let Some(p) = d.segments.as_mut().and_then(|m| m.pending.as_mut()) {
                    p.seal_gen = g;
                }
                true
            };
            Mutation::Write(d, changed)
        })
        .await
        .map(|result| matches!(result, MutationResult::Applied(true)))
        .unwrap_or(false);
    if !ok {
        return resume_incarnation(st, sref, Some(expect_epoch)).await; // maybe someone else's pending
    }
    resume_incarnation(st, sref, Some(expect_epoch)).await
}

/// Execute (or resume) one MERGE of two adjacent live segments.
/// Same two-phase discipline as split: persist the intent, seal both
/// parents, publish the child. Idempotent at every step.
#[cfg(test)]
pub(crate) async fn execute_merge(
    st: &TopologyService,
    sref: &crate::tenant::TenantStreamRef,
    a_id: u32,
    b_id: u32,
) -> bool {
    let Ok(Some(d)) = st.registry.get(sref).await else {
        return false;
    };
    execute_merge_fenced(st, sref, &d.stream_epoch, a_id, b_id).await
}

/// See [`execute_split_fenced`] — the same incarnation fence, for the
/// same reason.
pub(crate) async fn execute_merge_fenced(
    st: &TopologyService,
    sref: &crate::tenant::TenantStreamRef,
    expect_epoch: &str,
    a_id: u32,
    b_id: u32,
) -> bool {
    let ok = st
        .registry
        .mutate_incarnation(sref, expect_epoch, |current| {
            if current.deleted {
                return Mutation::Decline(false);
            }
            let mut d = current.to_persisted();
            let changed = {
                // A sealing or sealed collection has a fixed topology. A
                // transition that started just before the seal could
                // otherwise publish a successor AFTER the seal took its
                // snapshot of live segments — a new writable child under a
                // collection that already reports Sealed.
                if d.sealed || d.sealing.is_some() {
                    return Mutation::Decline(false);
                }
                let map = d.segments.get_or_insert_with(|| {
                    crate::segmap::SegmentMap::initial("", crate::shard::now_ms())
                });
                if map.pending.is_some() {
                    return Mutation::Decline(false);
                }
                let (Some(a), Some(b)) = (map.get(a_id), map.get(b_id)) else {
                    return Mutation::Decline(false);
                };
                let adjacent = a.hi == b.lo || b.hi == a.lo;
                if !a.is_live() || !b.is_live() || !adjacent {
                    return Mutation::Decline(false);
                }
                map.pending = Some(crate::segmap::PendingTransition {
                    kind: "merge".into(),
                    segs: vec![a_id, b_id],
                    split_at: 0,
                    started_ms: crate::shard::now_ms(),
                    seal_gen: 0, // patched below: needs the counter
                });
                map.version += 1;
                d.seal_gen_counter += 1;
                let g = d.seal_gen_counter;
                if let Some(p) = d.segments.as_mut().and_then(|m| m.pending.as_mut()) {
                    p.seal_gen = g;
                }
                true
            };
            Mutation::Write(d, changed)
        })
        .await
        .map(|result| matches!(result, MutationResult::Applied(true)))
        .unwrap_or(false);
    if !ok {
        return resume_incarnation(st, sref, Some(expect_epoch)).await;
    }
    resume_incarnation(st, sref, Some(expect_epoch)).await
}

/// Complete whatever transition the descriptor's `pending` records:
/// seal the parents (idempotent), then CAS the successor publication.
/// Safe to call from any instance at any time.
pub(crate) async fn resume(st: &TopologyService, sref: &crate::tenant::TenantStreamRef) -> bool {
    resume_incarnation(st, sref, None).await
}
/// Resume only the incarnation selected by an autonomous decision.
pub(crate) async fn resume_fenced(
    st: &TopologyService,
    sref: &crate::tenant::TenantStreamRef,
    epoch: &str,
) -> bool {
    resume_incarnation(st, sref, Some(epoch)).await
}

async fn resume_incarnation(
    st: &TopologyService,
    sref: &crate::tenant::TenantStreamRef,
    expected_epoch: Option<&str>,
) -> bool {
    st.registry.invalidate(sref);
    let Ok(Some(desc)) = st.registry.get(sref).await else {
        return false;
    };
    if expected_epoch.is_some_and(|epoch| desc.stream_epoch != epoch) {
        return false;
    }
    let Some(map) = &desc.segments else {
        return false;
    };
    let Some(p) = map.pending.clone() else {
        return false;
    };
    match (p.kind.as_str(), p.segs.len()) {
        ("split", 1) => {}
        ("merge", 2) => return resume_merge(st, &desc, p).await,
        _ => return false,
    }
    let seg_id = p.segs[0];
    let tg = (p.seal_gen > 0).then_some(p.seal_gen);
    let Some(frozen) = seal_identity(st, &desc, seg_id, tg).await else {
        return false;
    };
    // Deterministic failpoint for the seal-to-publication gap tests
    // (#108 registry): parks per PARENT STREAM NAME, so parallel gap
    // tests never wake on each other's transitions.
    #[cfg(test)]
    crate::failpoints::pause_scaler_before_publish(&desc.name).await;
    // Phase B: publish successors + clear the intent, with REAL routes
    // (review blocker 1: children on the parent's route add lineage but
    // zero capacity). The low child inherits the parent's route — its
    // predecessor data is already local; the high child gets a fresh
    // deterministic route (stable across CAS retries: derived from the
    // stream name and the child's seg id) that the ring spreads across
    // shard prefixes and owners.
    let prefixes = st.shards.prefixes().to_vec();
    // Phase B is fenced to the incarnation the pending transition was
    // READ from: mid-resume, the whole collection can be deleted and
    // recreated, and a name-scoped publication would stamp successors
    // onto the replacement.
    let published = st
        .registry
        .mutate_incarnation(&desc.sref(), &desc.stream_epoch, |current| {
            if current.deleted {
                return Mutation::Decline(false);
            }
            let mut d = current.to_persisted();
            let changed = {
                // Phase B is a SECOND durable step, so it re-checks the
                // lifecycle. Fencing only phase A left this race: publish
                // pending -> seal the parent -> pause -> the collection
                // seals (snapshotting the segments it can see) -> phase B
                // resumes and publishes live children UNDER a sealed
                // collection. Once sealed, no topology operation may create
                // another live segment.
                if d.sealed || d.sealing.is_some() {
                    return Mutation::Decline(false);
                }
                let pending_matches = d
                    .segments
                    .as_ref()
                    .is_some_and(|m| m.pending.as_ref() == Some(&p));
                if !pending_matches {
                    return Mutation::Decline(false); // someone else already completed it
                }
                let Some(low_route) = d.segment_route_by_id(seg_id) else {
                    return Mutation::Decline(false);
                };
                // The high child's route must land on a DIFFERENT shard
                // prefix than the parent whenever the topology has one —
                // otherwise the "split" keeps both children behind the same
                // serial committer and adds no capacity. Deterministic
                // salting (same sequence on every CAS retry) walks candidate
                // routes until the prefix differs; a single-shard topology
                // accepts the first candidate (capacity comes when shards
                // do).
                let child_id = d.segments.as_ref().expect("checked").next_seg_id + 1;
                let parent_prefix = crate::registry::shard_for_hash(&prefixes, &low_route);
                let mut high_route = [0u8; 16];
                for salt in 0u32..16 {
                    // Contract r1: route-child-v1 + project + name +
                    // child_segment_id + salt — the layout-4 domain-
                    // separated construction (was the "\0segroute\0"
                    // delimiter string).
                    high_route = crate::crypto::RouteHash::for_child(
                        &d.sref(),
                        child_id,
                        &salt.to_be_bytes(),
                    )
                    .0;
                    if prefixes.len() < 2
                        || crate::registry::shard_for_hash(&prefixes, &high_route) != parent_prefix
                    {
                        break;
                    }
                }
                let map = d.segments.as_mut().expect("checked above");
                match map.split(
                    seg_id,
                    p.split_at,
                    frozen,
                    low_route,
                    high_route,
                    crate::shard::now_ms(),
                ) {
                    Ok(_) => {
                        map.pending = None;
                        true
                    }
                    Err(_) => {
                        // Already split (idempotent completion): just clear.
                        map.pending = None;
                        map.version += 1;
                        true
                    }
                }
            };
            Mutation::Write(d, changed)
        })
        .await
        .map(|result| matches!(result, MutationResult::Applied(true)))
        .unwrap_or(false);
    if published {
        SEGMENT_SPLITS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        // Fresh sketches for the children start on first appends; the
        // parent's sketch is retired.
        st.scaler.retire_segments(&desc.sref(), &[seg_id]);
        st.registry.invalidate(&desc.sref());
        SEGMENT_MAP_REFRESHES.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }
    published
}

/// Merge completion: seal BOTH parents (idempotent), then publish the
/// merged child on the low parent's route. Crash-resumable from the
/// persisted pending intent; the seal-gap read semantics apply to both
/// parents automatically (pending.segs names them).
async fn resume_merge(
    st: &TopologyService,
    desc: &StreamDesc,
    p: crate::segmap::PendingTransition,
) -> bool {
    let (a_id, b_id) = (p.segs[0], p.segs[1]);
    let tg = (p.seal_gen > 0).then_some(p.seal_gen);
    let Some(fa) = seal_identity(st, desc, a_id, tg).await else {
        return false;
    };
    let Some(fb) = seal_identity(st, desc, b_id, tg).await else {
        return false;
    };
    #[cfg(test)]
    crate::failpoints::pause_scaler_before_publish(&desc.name).await;
    let published = st
        .registry
        .mutate_incarnation(&desc.sref(), &desc.stream_epoch, |current| {
            if current.deleted {
                return Mutation::Decline(false);
            }
            let mut d = current.to_persisted();
            let changed = {
                // Phase B is a SECOND durable step, so it re-checks the
                // lifecycle. Fencing only phase A left this race: publish
                // pending -> seal the parent -> pause -> the collection
                // seals (snapshotting the segments it can see) -> phase B
                // resumes and publishes live children UNDER a sealed
                // collection. Once sealed, no topology operation may create
                // another live segment.
                if d.sealed || d.sealing.is_some() {
                    return Mutation::Decline(false);
                }
                let pending_matches = d
                    .segments
                    .as_ref()
                    .is_some_and(|m| m.pending.as_ref() == Some(&p));
                if !pending_matches {
                    return Mutation::Decline(false);
                }
                let Some(child_route) = d.segment_route_by_id(a_id) else {
                    return Mutation::Decline(false);
                };
                let map = d.segments.as_mut().expect("checked");
                match map.merge(a_id, b_id, fa, fb, child_route, crate::shard::now_ms()) {
                    Ok(_) => {
                        map.pending = None;
                        true
                    }
                    Err(_) => {
                        // Already merged (idempotent completion): just clear.
                        map.pending = None;
                        map.version += 1;
                        true
                    }
                }
            };
            Mutation::Write(d, changed)
        })
        .await
        .map(|result| matches!(result, MutationResult::Applied(true)))
        .unwrap_or(false);
    if published {
        SEGMENT_MERGES.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        st.scaler.retire_segments(&desc.sref(), &[a_id, b_id]);
        st.registry.invalidate(&desc.sref());
        SEGMENT_MAP_REFRESHES.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }
    published
}
