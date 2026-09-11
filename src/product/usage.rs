//! The customer usage API (docs/OBSERVABILITY-BILLING.md §10): the per-stream
//! and per-project usage answers, read from the rollup with point reads.
use super::*;

// ---- customer usage API (docs/OBSERVABILITY-BILLING.md §10) ----------

/// The rollup is the only source of usage answers: when it is absent or
/// fails, the request is retryable, never a client error.
fn usage_unavailable(error: &dyn std::fmt::Display) -> Response {
    perr(
        StatusCode::SERVICE_UNAVAILABLE,
        "usage_unavailable",
        &error.to_string(),
        None,
        true,
    )
}

/// GET /v1/streams/{name}/usage[?month=YYYY-MM] and .../usage/current.
/// Control-plane metadata: bearer-authorized, NO record key required,
/// answered from the rollup with a point read (never a ledger scan).
#[expect(
    clippy::unwrap_used,
    reason = "product_usage; the month was parsed once at admission, so parsing it again cannot fail; a fallible re-parse would turn an already accepted request into a spurious error"
)]
#[expect(
    clippy::too_many_lines,
    clippy::cast_sign_loss,
    clippy::cast_possible_truncation,
    reason = "product_usage; the usage answer derives byte-seconds, averages and month bounds from one rollup read, with clamped non-negative millisecond spans and byte averages that fit u64; splitting it or checking the casts would separate the figures from the read that dates them"
)]
pub(super) async fn product_usage(
    state: Arc<AppState>,
    sref: crate::tenant::TenantStreamRef,
    query: &str,
) -> Response {
    // R25-E: validate the query BEFORE availability checks — a
    // malformed request is the CLIENT's error whatever this instance's
    // billing posture, and a 503 for a typo'd parameter teaches callers
    // to retry requests that can never succeed.
    let q = match strict_query(query, &["month", "streamId"]) {
        Ok(q) => q,
        Err(r) => return r,
    };
    let Some(rollup) = state.rollup.get() else {
        return usage_unavailable(&"the usage rollup is not running on this instance");
    };
    let desc = match state.registry.get(&sref).await {
        Ok(Some(d)) => d,
        Ok(None) => {
            return perr(
                StatusCode::NOT_FOUND,
                "not_found",
                "stream not found",
                None,
                false,
            );
        }
        Err(e) => {
            return perr(
                StatusCode::INTERNAL_SERVER_ERROR,
                "internal",
                &e.to_string(),
                None,
                true,
            );
        }
    };
    let now = crate::shard::now_ms();
    let (cy, cm) = crate::billing::utc_year_month(now);
    let current = crate::billing::month_str(cy, cm);
    let month = q.get("month").cloned().unwrap_or_else(|| current.clone());
    if crate::billing::parse_month(&month).is_none() {
        return perr(
            StatusCode::BAD_REQUEST,
            "invalid_month",
            "month must be YYYY-MM",
            None,
            false,
        );
    }
    let mut id = crate::billing::identity_of_query(&state, &desc);
    // Historical incarnation lookup (round-21 dashboard gap): after a
    // delete/recreate, ?streamId= addresses a PRIOR incarnation's rows
    // directly — invoice history survives the live resource.
    if let Some(sid) = q.get("streamId").map(String::as_str) {
        id.stream_id = sid.to_string();
    }
    let row: crate::rollup::MonthRow = match rollup
        .month_row(&month, &id.account_id, &id.project_id, &id.stream_id)
        .await
    {
        Ok(row) => row.unwrap_or_default(),
        Err(error) => return usage_unavailable(&error),
    };
    let is_current = month == current;
    // Round-21 blocker 2: a retained-but-idle stream has no month row
    // yet for the CURRENT month — the durable segment index still knows
    // its gauge, so provisional storage never reads as zero.
    let (fallback_byte_ms, fallback_owned) = if is_current && row.segments.is_empty() {
        let states = match rollup
            .stream_segment_states(&id.account_id, &id.project_id, &id.stream_id)
            .await
        {
            Ok(states) => states,
            Err(error) => return usage_unavailable(&error),
        };
        let mstart = {
            let (y, m) = crate::billing::parse_month(&month).unwrap();
            crate::billing::month_start_ms(y, m)
        };
        let bms: u128 = states
            .iter()
            .map(|s| {
                let from = s.storage_accounted_through_ms.max(mstart);
                (now - from).max(0) as u128 * s.owned_frame_bytes_current as u128
            })
            .sum();
        let owned: u64 = states.iter().map(|s| s.owned_frame_bytes_current).sum();
        (bms, owned)
    } else {
        (0, 0)
    };
    let byte_ms = if is_current {
        row.storage_byte_ms_provisional(&month, now)
            .max(fallback_byte_ms)
    } else {
        row.storage_byte_ms()
    };
    let month_ms = {
        let (y, m) = crate::billing::parse_month(&month).unwrap();
        let (ny, nm) = crate::billing::next_month(y, m);
        (crate::billing::month_start_ms(ny, nm) - crate::billing::month_start_ms(y, m)) as u128
    };
    let avg_bytes = byte_ms / month_ms.max(1);
    let gb_month = byte_ms as f64 / month_ms as f64 / 1e9;
    let name_agg = match rollup
        .name_row(&month, &id.account_id, &id.project_id, &id.stream_name)
        .await
    {
        Ok(row) => row,
        Err(error) => return usage_unavailable(&error),
    };
    let status = if row.finalized_at_ms.is_some() {
        if row.corrections.is_empty() {
            "finalized"
        } else {
            "corrected"
        }
    } else {
        "provisional"
    };
    json_ok(&json!({
        "projectId": id.project_id,
        "streamId": id.stream_id,
        "streamName": id.stream_name,
        "month": month,
        "status": status,
        "ingestPayloadBytes": row.ingest_bytes(),
        "ingestRecords": row.ingest_records(),
        "readPayloadBytes": row.read_payload_bytes,
        "readRecords": row.read_records,
        "readOperations": row.read_operations,
        "queueOperations": row.queue_operations,
        "appendRequests": row.append_requests,
        "storageByteSeconds": (byte_ms / 1000).to_string(),
        "averageStoredBytes": avg_bytes as u64,
        "gbMonth": gb_month,
        "ownedStoredBytesNow": row.owned_bytes_now().max(fallback_owned),
        "updatedAt": row.updated_ms,
        "finalizedAt": row.finalized_at_ms,
        "corrections": row.corrections.len(),
        // Round-22 item 8: base + materialized corrections = what the
        // invoice will actually say, plus the audit trail itself.
        "effective": row.effective(),
        "correctionTotals": row.corr,
        "correctionList": row.corrections.iter().map(|c| serde_json::json!({
            "id": c.correction_id,
            "version": c.correction_version,
            "sourceEventId": c.source_event_id,
            "reason": c.reason,
            "createdAt": c.created_at_ms,
            "ingestPayloadBytesDelta": c.ingest_payload_bytes_delta,
            "ingestRecordsDelta": c.ingest_records_delta,
            "readPayloadBytesDelta": c.read_payload_bytes_delta,
            "readRecordsDelta": c.read_records_delta,
            "readOperationsDelta": c.read_operations_delta,
            "queueOperationsDelta": c.queue_operations_delta,
            "appendRequestsDelta": c.append_requests_delta,
            "storageByteMsDelta": c.storage_byte_ms_delta,
        })).collect::<Vec<_>>(),
        "nameAggregate": name_agg.as_ref().map(|a| serde_json::json!({
            "ingestPayloadBytes": a.ingest_bytes,
            "readPayloadBytes": a.read_payload_bytes,
            "storageByteSeconds": (a.storage_byte_ms.parse::<u128>().unwrap_or(0) / 1000).to_string(),
        })),
        "incarnations": name_agg.map(|a| a.incarnations).unwrap_or_default(),
        "metering": {
            "readFlushIntervalSeconds": crate::billing::READ_FLUSH_INTERVAL_MS / 1000,
            "possibleReadLossWindowSeconds": crate::billing::READ_FLUSH_INTERVAL_MS / 1000,
        }
    }))
}

/// GET /v1/projects/{project}/usage[?month=YYYY-MM] (round-22 doc
/// item D3): the project-level rollup answer — aggregate totals,
/// correction sums, and effective values. Bearer-authenticated like
/// every product control-plane read. Under the one-project-per-cell
/// deployment contract the {project} segment must match this cell's
/// configured project.
pub(crate) async fn project_usage(
    state: Arc<AppState>,
    authority: &crate::tenant::ProjectId,
    project: String,
    query: &str,
) -> Response {
    let q = match strict_query(query, &["month"]) {
        Ok(q) => q,
        Err(r) => return r,
    };

    // Stage 5d: the path must name the AUTHORITATIVE project for this
    // request — the verified principal's in enforce, the deployment
    // tenant otherwise. Grammar-invalid and foreign ids get the same
    // not-found answer (no grammar oracle), and the check precedes the
    // availability probe: a wrong path is the client's error whatever
    // this instance's rollup posture.
    let names_authority = crate::tenant::ProjectId::new(&project)
        .map(|p| p == *authority)
        .unwrap_or(false);
    if !names_authority {
        // Journaled (§10.4): a verified principal probing FOREIGN
        // project usage is the single most review-relevant denial
        // class, deliberately shaped as 404 on the wire.
        return crate::audit::tag_project(
            crate::audit::tag(
                perr(
                    StatusCode::NOT_FOUND,
                    "unknown_project",
                    "the path does not name this request's project",
                    None,
                    false,
                ),
                "unknown_project",
            ),
            authority,
        );
    }
    let Some(rollup) = state.rollup.get() else {
        return usage_unavailable(&"the usage rollup is not running on this instance");
    };
    let now = crate::shard::now_ms();
    let (cy, cm) = crate::billing::utc_year_month(now);
    let current = crate::billing::month_str(cy, cm);
    let month = q.get("month").cloned().unwrap_or_else(|| current.clone());
    if crate::billing::parse_month(&month).is_none() {
        return perr(
            StatusCode::BAD_REQUEST,
            "invalid_month",
            "month must be YYYY-MM",
            None,
            false,
        );
    }
    // Stage 7: rows land under the workspace-at-event; query and
    // report under the SAME resolution the meter used.
    let account = if state.auth.mode != crate::auth::AuthMode::Off {
        state
            .auth
            .workspace_for(authority)
            .map(|w| w.as_str().to_string())
            .unwrap_or_else(|| state.deployment.account_id().to_string())
    } else {
        state.deployment.account_id().to_string()
    };
    let agg = match rollup.project_row(&month, &account, &project).await {
        Ok(row) => row.unwrap_or_default(),
        Err(error) => return usage_unavailable(&error),
    };
    let byte_ms: u128 = agg.storage_byte_ms.parse().unwrap_or(0);
    json_ok(&json!({
        "accountId": account,
        "projectId": project,
        "month": month,
        "ingestPayloadBytes": agg.ingest_bytes,
        "ingestRecords": agg.ingest_records,
        "readPayloadBytes": agg.read_payload_bytes,
        "readRecords": agg.read_records,
        "readOperations": agg.read_operations,
        "queueOperations": agg.queue_operations,
        "appendRequests": agg.append_requests,
        "storageByteSeconds": (byte_ms / 1000).to_string(),
        "correctionTotals": agg.corr,
        "effective": {
            "ingestPayloadBytes": crate::rollup::eff_u64(agg.ingest_bytes, agg.corr.ingest_payload_bytes_delta),
            "ingestRecords": crate::rollup::eff_u64(agg.ingest_records, agg.corr.ingest_records_delta),
            "readPayloadBytes": crate::rollup::eff_u64(agg.read_payload_bytes, agg.corr.read_payload_bytes_delta),
            "readRecords": crate::rollup::eff_u64(agg.read_records, agg.corr.read_records_delta),
            "readOperations": crate::rollup::eff_u64(agg.read_operations, agg.corr.read_operations_delta),
            "queueOperations": crate::rollup::eff_u64(agg.queue_operations, agg.corr.queue_operations_delta),
            "appendRequests": crate::rollup::eff_u64(agg.append_requests, agg.corr.append_requests_delta),
            "storageByteSeconds": (crate::rollup::eff_u128(byte_ms, &agg.corr.storage_byte_ms_delta) / 1000).to_string(),
        },
    }))
}
