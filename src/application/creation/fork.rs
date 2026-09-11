//! Resolve inherited source identity and validate a fork boundary before taking references.
use super::*;
pub(super) struct ForkCtx {
    pub(super) source: String,
    pub(super) source_desc: StreamDesc,
    pub(super) boundary: u64,
    pub(super) sub: u64,
    pub(super) materialize: Option<Bytes>,
}

pub(super) struct Preparation<'a> {
    pub sref: &'a crate::tenant::TenantStreamRef,
    pub key: &'a StreamKey,
    pub content_type: Option<String>,
    pub ttl_secs: Option<u64>,
    pub expires_at_ms: Option<i64>,
    pub fork: Option<&'a ForkCommand>,
}
pub(super) struct PreparedFork {
    pub context: Option<ForkCtx>,
    pub content_type: String,
    pub ttl_secs: Option<u64>,
}
#[expect(
    clippy::too_many_lines,
    reason = "prepare; fork preparation validates the source, its lifecycle, the boundary and the child's policy in the order a fork request must fail; splitting it would separate the checks from the order that decides which error a client sees"
)]
pub(super) async fn prepare(
    state: &Arc<CreationService>,
    input: Preparation<'_>,
) -> Result<PreparedFork, CreationError> {
    let Preparation {
        sref,
        key,
        content_type,
        mut ttl_secs,
        expires_at_ms,
        fork,
    } = input;
    let ct_hdr_present = content_type.is_some();
    let mut content_type = content_type.unwrap_or_else(|| "application/octet-stream".into());
    // ---- Fork creation (pinned DS protocol fork contract) ----------
    // Parsed before descriptor resolution: validation errors must beat
    // creation, and the fork identity participates in the idempotent
    // compare.
    let fork_src_hdr = fork.map(|f| f.source.clone());
    let fork_off_hdr = fork.and_then(|f| f.offset);
    let fork_sub_hdr = fork.and_then(|f| f.sub_offset);
    if fork_src_hdr.is_none() && (fork_off_hdr.is_some() || fork_sub_hdr.is_some()) {
        return Err(CreationError::new(
            CreationFailure::Invalid,
            "fork_headers",
            "Stream-Fork-Offset/Sub-Offset require Stream-Forked-From",
        ));
    }
    let fork_ctx: Option<ForkCtx> = if let Some(src_raw) = &fork_src_hdr {
        if src_raw.project_id() != sref.project_id() {
            return Err(CreationError::new(
                CreationFailure::Invalid,
                "fork_project_mismatch",
                "fork source and child must belong to the same project",
            ));
        }
        let src_name = src_raw.name().as_str().to_string();
        // Is THIS child already mid-initialization against this source?
        // If so, the source is being retained FOR IT — its reference is
        // already installed — and refusing a retained source would leave
        // the child permanently Initializing over data kept expressly to
        // serve it. Resolve the target's own state before demanding a
        // live source.
        // Ready OR still initializing: either way, if this exact child
        // already holds a reference on the source, the source is being
        // retained for it. Restricting this to initializing children
        // broke idempotence — a completed fork whose response was lost
        // could not be re-PUT once its source was retained, because the
        // soft-delete check fired first.
        let resuming_child = match state.registry.get(sref).await {
            Ok(Some(c)) if !c.deleted => c
                .forked_from
                .clone()
                .filter(|f| f.source == src_name && !f.fork_id.is_empty()),
            _ => None,
        };
        let src = match state.registry.get(src_raw).await {
            Ok(Some(d)) if desc_alive(&d) => d,
            // Retained for this very child: same incarnation, and the
            // reference this child installed is still on it.
            Ok(Some(d))
                if !d.deleted
                    && resuming_child.as_ref().is_some_and(|f| {
                        f.source_epoch == d.stream_epoch && d.fork_children.contains(&f.fork_id)
                    }) =>
            {
                d
            }
            Ok(Some(d)) if d.soft_deleted => {
                return Err(CreationError::new(
                    CreationFailure::Conflict,
                    "fork_source_gone",
                    "source is deleted (data retained for existing forks only)",
                ));
            }
            Ok(_) => {
                return Err(CreationError::new(
                    CreationFailure::Missing,
                    "not_found",
                    "fork source not found",
                ));
            }
            Err(e) => {
                return Err(CreationError::new(
                    CreationFailure::Storage,
                    "internal",
                    &e.to_string(),
                ));
            }
        };
        if src
            .segments
            .as_ref()
            .is_some_and(|m| m.segments.len() > 1 || m.pending.is_some())
        {
            return Err(CreationError::new(
                CreationFailure::Invalid,
                "fork_segmented_source",
                "forking a segmented collection is not supported",
            ));
        }
        // Content type: inherit when omitted; explicit mismatch is 409
        // BEFORE any reference is taken.
        if ct_hdr_present
            && crate::registry::media_type(&content_type)
                != crate::registry::media_type(&src.content_type)
        {
            return Err(CreationError::new(
                CreationFailure::Conflict,
                "fork_content_type_mismatch",
                "fork content type must match the source",
            ));
        }
        if !ct_hdr_present {
            content_type = src.content_type.clone();
        }
        if ttl_secs.is_none() && expires_at_ms.is_none() {
            ttl_secs = src.ttl_secs; // inherit source TTL
        }
        // Source key must accept the presented key (fork reads decrypt
        // the ancestor's records with it).
        if key.fingerprint(&src.epoch()) != src.key_fingerprint {
            return Err(CreationError::new(
                CreationFailure::WrongKey,
                "wrong_key",
                "key mismatch with source",
            ));
        }
        let Boundary {
            boundary,
            sub,
            materialize,
        } = validate_boundary(state, &src, key, fork_off_hdr, fork_sub_hdr).await?;
        Some(ForkCtx {
            source: src_name,
            source_desc: src,
            boundary,
            sub,
            materialize,
        })
    } else {
        None
    };
    Ok(PreparedFork {
        context: fork_ctx,
        content_type,
        ttl_secs,
    })
}

struct Boundary {
    boundary: u64,
    sub: u64,
    materialize: Option<Bytes>,
}
#[expect(
    clippy::too_many_lines,
    reason = "validate_boundary; the boundary walk reads the source record at the fork offset and certifies the materialized partial in one pass; splitting it would separate the partial from the record it is cut from"
)]
#[expect(
    clippy::unwrap_used,
    reason = "validate_boundary; a poisoned stream state may hold a half-advanced durable frontier; recovering it could validate a boundary against a length never made durable"
)]
#[expect(
    clippy::cast_possible_truncation,
    reason = "validate_boundary; the sub-offset was checked against the record's length, itself a usize; a checked conversion would only restate that bound"
)]
#[expect(
    clippy::excessive_nesting,
    reason = "validate_boundary; the walk nests the ceiling verdict inside the partial-record branch of the boundary check; flattening it would separate the verdict from the partial it certifies"
)]
async fn validate_boundary(
    state: &Arc<CreationService>,
    src: &StreamDesc,
    key: &StreamKey,
    fork_off_hdr: Option<u64>,
    fork_sub_hdr: Option<u64>,
) -> Result<Boundary, CreationError> {
    let src_key = key.clone();
    let (_, src_handle) = match state.reads.handle_of(src).await {
        Ok(v) => v,
        Err(m) => return Err(CreationError::new(CreationFailure::Storage, "internal", &m)),
    };
    let src_end = src_handle.state.lock().unwrap().durable.next;
    let base = match &fork_off_hdr {
        None => src_end,
        Some(offset) => *offset,
    };
    if base > src_end {
        return Err(CreationError::new(
            CreationFailure::Invalid,
            "fork_offset_beyond_end",
            "fork offset beyond the source's length",
        ));
    }
    let sub = fork_sub_hdr.unwrap_or(0);
    let mut boundary = base;
    let mut materialize: Option<Bytes> = None;
    if sub > 0 {
        if fork_off_hdr.is_none() {
            return Err(CreationError::new(
                CreationFailure::Invalid,
                "fork_headers",
                "a sub-offset requires an explicit Stream-Fork-Offset",
            ));
        }
        if src_end == 0 {
            return Err(CreationError::new(
                CreationFailure::Invalid,
                "fork_sub_offset_empty_source",
                "a sub-offset needs a record to split",
            ));
        }
        if src.is_json() {
            // Messages ARE records in this implementation: the
            // sub-offset advances the record boundary.
            boundary = base.saturating_add(sub);
            if boundary > src_end {
                return Err(CreationError::new(
                    CreationFailure::Invalid,
                    "fork_sub_offset_beyond_end",
                    "sub-offset overshoots the source",
                ));
            }
        } else {
            if base >= src_end {
                return Err(CreationError::new(
                    CreationFailure::Invalid,
                    "fork_sub_offset_beyond_end",
                    "no record at the fork offset",
                ));
            }
            // The record being split (the source may itself be a
            // fork — read through its chain).
            let rec = match state
                .reads
                .read_stitched(
                    src,
                    &src_key,
                    crate::application::read::ReadRange::bounded(base, base.saturating_add(1)),
                    64 << 20,
                )
                .await
            {
                Ok(out) => out,
                Err(m) => {
                    return Err(CreationError::new(CreationFailure::Storage, "internal", &m));
                }
            };
            let Some(rec) = rec.recs.iter().find(|r| r.off == base) else {
                return Err(CreationError::new(
                    CreationFailure::Storage,
                    "internal",
                    "source record unavailable for sub-offset validation",
                ));
            };
            let len = rec.payload.len() as u64;
            if sub > len {
                return Err(CreationError::new(
                    CreationFailure::Invalid,
                    "fork_sub_offset_beyond_end",
                    "sub-offset overshoots the record",
                ));
            }
            if sub == len {
                boundary = base + 1; // whole record inherited
            } else {
                boundary = base; // partial materializes at `base`
                let m = bytes::Bytes::copy_from_slice(&rec.payload[..sub as usize]);
                // Round-10e review: the materialized partial is a
                // CUSTOMER record this child will persist — it
                // must satisfy the per-record ceiling HERE, before
                // any fork lifecycle work (tail seed, source
                // reference) becomes durable. A source record
                // created under a different profile or an older,
                // larger ceiling must not smuggle an over-ring
                // record past release certification.
                if let Some(over) =
                    over_record_ceiling(state.admission.record_ceiling(), std::slice::from_ref(&m))
                {
                    return Err(CreationError::new(
                        CreationFailure::TooLarge,
                        "record_too_large",
                        &format!(
                            "the fork's materialized partial record of {over} bytes \
                 exceeds the per-record ceiling (MAX_RECORD_PAYLOAD_BYTES)"
                        ),
                    ));
                }
                materialize = Some(m);
            }
        }
    }
    Ok(Boundary {
        boundary,
        sub,
        materialize,
    })
}
