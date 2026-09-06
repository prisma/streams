//! Standards read parsing and rendering. Storage decisions and cursor progress
//! are returned by ReadService as typed data shared with the product adapter.
use super::*;
use crate::application::read::{
    ReadCommand, ReadFailure, ReadMode, ReadOutcome, ReadPosition, ReadResultKind, ReadStart,
};

pub(crate) fn read_failure_response(error: ReadFailure) -> Response {
    use ReadFailure as E;
    match error {
        E::Resolve(error) => resolve_response(error),
        E::Missing => err_resp(StatusCode::NOT_FOUND, "not_found", "stream not found"),
        E::Gone => err_resp(
            StatusCode::GONE,
            "gone",
            "stream deleted; live forks remain",
        ),
        E::Creating => creating_resp(),
        E::MissingKey => err_resp(
            StatusCode::BAD_REQUEST,
            "missing_key",
            "Stream-Encryption-Key required",
        ),
        E::WrongKey => err_resp(StatusCode::FORBIDDEN, "wrong_key", "key mismatch"),
        E::InvalidCursor => err_resp(
            StatusCode::BAD_REQUEST,
            "invalid_offset",
            "offset is outside this lineage's readable range",
        ),
        E::ChangedIncarnation => err_resp(
            StatusCode::CONFLICT,
            "target_mismatch",
            "stream incarnation changed",
        ),
        E::CursorBeyondTail => err_resp(
            StatusCode::CONFLICT,
            "cursor_beyond_tail",
            "cursor is ahead of the stream tail; resume from the durable cursor",
        ),
        E::KeylessLive => err_resp(
            StatusCode::BAD_REQUEST,
            "keyless_live",
            "live reads on a segmented stream require key=",
        ),
        E::AppliedFork => err_resp(
            StatusCode::BAD_REQUEST,
            "deliver_unsupported_fork",
            "deliver=applied is not supported on forked streams",
        ),
        E::Storage(message) => err_resp(StatusCode::INTERNAL_SERVER_ERROR, "internal", &message),
        E::Remote(error) => err_resp(
            StatusCode::SERVICE_UNAVAILABLE,
            "temporarily_unavailable",
            &format!("peer read failed: {error:?}"),
        ),
    }
}
fn resolve_response(error: crate::shard_directory::ResolveError) -> Response {
    // Ownership is an application failure with a typed routing hint; only this
    // edge converts it into a gateway header.
    use crate::shard_directory::ResolveError as E;
    match error {
        E::NotOwner { prefix, owner } => {
            let mut response = err_resp(
                StatusCode::CONFLICT,
                "not_ring_owner",
                &format!("shard {prefix} belongs to {owner}"),
            );
            if let Ok(owner) = axum::http::HeaderValue::from_str(&owner) {
                response.headers_mut().insert("streams-replay-to", owner);
            }
            response
        }
        E::Opening {
            code,
            retry_after_secs,
            ..
        } => {
            let mut response = err_resp(
                StatusCode::SERVICE_UNAVAILABLE,
                code,
                "shard is opening; retry",
            );
            response.headers_mut().insert(
                "retry-after",
                axum::http::HeaderValue::from_str(&retry_after_secs.to_string()).unwrap(),
            );
            response
        }
        E::OpenFailed { error, .. } => err_resp(
            StatusCode::INTERNAL_SERVER_ERROR,
            "shard_open_failed",
            &error,
        ),
    }
}
fn raw_start(
    desc: &StreamDesc,
    selector: Option<&str>,
    offset: Option<&str>,
) -> Result<ReadStart, ReadFailure> {
    match offset {
        None => Ok(ReadStart::Beginning),
        Some("now") => Ok(ReadStart::Now),
        Some(raw) => {
            let segmented = desc
                .segments
                .as_ref()
                .is_some_and(|m| m.segments.len() > 1 || m.pending.is_some());
            if segmented {
                let (segment, offset) =
                    crate::offsets::parse_ep(raw).map_err(|_| ReadFailure::InvalidCursor)?;
                Ok(ReadStart::Position(ReadPosition {
                    segment,
                    after: offset.scan_from(),
                }))
            } else {
                let offset = Offset::parse(raw).map_err(|_| ReadFailure::InvalidCursor)?;
                Ok(ReadStart::Position(ReadPosition {
                    segment: desc.resolve_segment(selector.unwrap_or("")).seg_id,
                    after: offset.scan_from(),
                }))
            }
        }
    }
}
pub(crate) async fn read_inner(
    state: Arc<AppState>,
    sref: crate::tenant::TenantStreamRef,
    params: ReadParams,
    headers: HeaderMap,
    head_only: bool,
    may_refresh: bool,
    surface: SseSurface,
) -> Response {
    let desc = match state.registry.get(&sref).await {
        Ok(Some(desc)) => desc,
        Ok(None) => return read_failure_response(ReadFailure::Missing),
        Err(error) => return read_failure_response(ReadFailure::Storage(error.to_string())),
    };
    if !head_only {
        state.creation_service().touch_ttl(&desc);
    }
    let live = match params.live.as_deref() {
        None => None,
        Some("long-poll" | "true") => Some("long-poll"),
        Some("sse") => Some("sse"),
        _ => return err_resp(StatusCode::BAD_REQUEST, "invalid_live", "invalid live mode"),
    };
    if live.is_some() && params.offset.is_none() {
        return err_resp(
            StatusCode::BAD_REQUEST,
            "missing_offset",
            "live reads require offset",
        );
    }
    let key = match raw_key(&headers, &state) {
        Some(raw) => match StreamKey::from_b64(raw) {
            Ok(key) => Some(key),
            Err(_) => return read_failure_response(ReadFailure::WrongKey),
        },
        None => None,
    };
    let start = if head_only
        && desc
            .segments
            .as_ref()
            .is_none_or(|map| map.segments.len() <= 1 && map.pending.is_none())
    {
        ReadStart::Beginning
    } else {
        match raw_start(&desc, params.key.as_deref(), params.offset.as_deref()) {
            Ok(start) => start,
            Err(error) => return read_failure_response(error),
        }
    };
    let command = ReadCommand {
        descriptor: desc,
        key: key.clone(),
        start,
        selector: params.key.clone(),
        mode: if head_only {
            ReadMode::Head
        } else if live == Some("long-poll") {
            ReadMode::LongPoll(
                params
                    .timeout
                    .as_deref()
                    .and_then(parse_duration)
                    .unwrap_or(Duration::from_secs(3))
                    .min(MAX_LONG_POLL),
            )
        } else {
            ReadMode::Replay
        },
        visibility: params.deliver,
        max_bytes: params.max_bytes.unwrap_or(MAX_READ_BYTES),
        tail_max_bytes: tail_max_bytes(&state.config.http),
        allow_remote: !params.no_fanout,
        refresh: may_refresh,
    };
    if let Err(error) = crate::application::read::ReadService::authorize_read(&command) {
        return read_failure_response(error);
    }
    if live == Some("sse") {
        return serve_read_sse(state, command, params, surface).await;
    }
    let out = match state.read_service().execute_read(command).await {
        Ok(out) => out,
        Err(error) => return read_failure_response(error),
    };
    if params.internal
        && headers
            .get("streams-internal-read-page")
            .and_then(|v| v.to_str().ok())
            == Some("1")
    {
        return axum::Json(crate::application::read_remote::WireReadPage::from_outcome(
            &out,
        ))
        .into_response();
    }
    render_raw_read(&state, &params, &headers, key.as_ref(), out)
}

pub(crate) fn read_payload(
    out: &ReadOutcome,
    frames: bool,
    key: Option<&StreamKey>,
    selector: Option<&str>,
    compress: bool,
) -> Bytes {
    let mut body = BytesMut::new();
    if out.descriptor.is_json() && !frames {
        body.extend_from_slice(b"[");
        for (index, record) in out.records.iter().enumerate() {
            if index > 0 {
                body.extend_from_slice(b",");
            }
            body.extend_from_slice(&record.payload);
        }
        body.extend_from_slice(b"]");
    } else if frames {
        if let Some(key) = key {
            let selector = selector.unwrap_or("");
            let subkey = derive_subkey(key, &out.descriptor.epoch(), selector, 0);
            for record in &out.records {
                body.extend_from_slice(&encrypt_frame(
                    &subkey,
                    &out.identity,
                    &FrameHeader {
                        offset: record.off,
                        ts_ms: 0,
                        key_version: 0,
                        routing_key: selector.to_string(),
                    },
                    &record.payload,
                    crate::crypto::FrameCompression::from_enabled(compress),
                ));
            }
        }
    } else {
        for record in &out.records {
            body.extend_from_slice(&record.payload);
            if out.segmented {
                body.extend_from_slice(b"\n");
            }
        }
    }
    body.freeze()
}
fn raw_position(position: ReadPosition, segmented: bool) -> String {
    let offset = Offset(position.after.checked_sub(1));
    if segmented {
        crate::offsets::encode_ep(position.segment, offset)
    } else {
        offset.encode()
    }
}
pub(crate) fn meter_read_outcome(state: &AppState, out: &ReadOutcome) {
    crate::billing::meter_read(
        state,
        &out.descriptor,
        out.records
            .iter()
            .map(|record| record.payload.len() as u64)
            .sum(),
        out.records.len() as u64,
    );
}
fn render_raw_read(
    state: &AppState,
    params: &ReadParams,
    headers: &HeaderMap,
    key: Option<&StreamKey>,
    out: ReadOutcome,
) -> Response {
    let etag = read_etag(&out.descriptor, out.scan_from, out.end, out.closed);
    if out.kind == ReadResultKind::Data
        && !out.segmented
        && hdr(headers, "if-none-match").is_some_and(|value| value == etag)
    {
        return Response::builder()
            .status(StatusCode::NOT_MODIFIED)
            .header("etag", etag)
            .body(Body::empty())
            .unwrap();
    }
    let empty = matches!(out.kind, ReadResultKind::Head | ReadResultKind::Timeout);
    let frames = params.format.as_deref() == Some("frames")
        && !out.segmented
        && out.descriptor.forked_from.is_none();
    let payload = if empty {
        Bytes::new()
    } else {
        read_payload(
            &out,
            frames,
            key,
            params.key.as_deref(),
            state.config.crypto.frame_compress,
        )
    };
    if !params.internal {
        meter_read_outcome(state, &out);
    }
    state
        .runtime
        .usage
        .counters(&crate::crypto::RouteHash::for_stream(&out.descriptor.sref()).0)
        .bytes_out
        .fetch_add(payload.len() as u64, std::sync::atomic::Ordering::Relaxed);
    let mut response = Response::builder()
        .status(if out.kind == ReadResultKind::Timeout {
            StatusCode::NO_CONTENT
        } else {
            StatusCode::OK
        })
        .header("stream-next-offset", raw_position(out.next, out.segmented));
    if out.kind != ReadResultKind::Timeout {
        response = response.header(
            header::CONTENT_TYPE,
            if frames {
                "application/x-durable-stream-frames"
            } else {
                &out.descriptor.content_type
            },
        );
    }
    if out.kind == ReadResultKind::Data && !out.segmented && out.descriptor.forked_from.is_none() {
        response = response.header("etag", etag);
    }
    if !matches!(out.kind, ReadResultKind::Head | ReadResultKind::Timeout) {
        response = response.header("cross-origin-resource-policy", "cross-origin");
    }
    if out.up_to_date {
        response = response.header("stream-up-to-date", "true");
    }
    if out.closed {
        response = response.header("stream-closed", "true");
    }
    if let Some(durable) = out.durable {
        response = response.header(
            "stream-durable-offset",
            raw_position(durable, out.segmented),
        );
    }
    if let Some(index) = out.pending_from {
        response = response.header("stream-pending-from", index.to_string());
    }
    if params
        .live
        .as_deref()
        .is_some_and(|value| value == "long-poll" || value == "true")
        && !out.segmented
    {
        response = response.header("stream-cursor", interval_cursor(params.cursor.as_deref()));
    }
    if params.live.is_some()
        || out.segmented
        || out.kind != ReadResultKind::Data
        || out.descriptor.forked_from.is_some()
    {
        response = response.header(header::CACHE_CONTROL, "no-store");
    }
    if out.kind == ReadResultKind::Head
        && let Some(expiry) = out.descriptor.expires_at_ms
        && out.descriptor.ttl_secs.is_some()
    {
        let remaining = ((expiry - now_ms()) as f64 / 1000.0).ceil() as i64;
        if remaining > 0 {
            response = response.header("stream-ttl", remaining.to_string());
        }
    }
    if debug_timing(&state.config.http) && out.kind == ReadResultKind::Data && !out.segmented {
        response = response.header(
            "streams-debug-wait",
            format!(
                "waited={} arm_us={} read_us={}",
                out.waited as u8, out.wait_micros, out.read_micros
            ),
        );
    }
    response.body(Body::from(payload)).unwrap()
}

pub(crate) async fn serve_read_sse(
    state: Arc<AppState>,
    command: ReadCommand,
    params: ReadParams,
    surface: SseSurface,
) -> Response {
    let failure = |error| {
        if surface == SseSurface::Product {
            crate::product::render_product_read_failure(error)
        } else {
            read_failure_response(error)
        }
    };
    if let Err(error) = crate::application::read::ReadService::authorize_read(&command) {
        return failure(error);
    }
    let desc = command.descriptor;
    let key = match command.key {
        Some(key) => key,
        None => return failure(ReadFailure::MissingKey),
    };
    let epoch = desc.epoch();
    let segmented = desc
        .segments
        .as_ref()
        .is_some_and(|m| m.segments.len() > 1 || m.pending.is_some());
    if !segmented {
        let route = desc.resolve_segment(command.selector.as_deref().unwrap_or(""));
        let engine = match state
            .shards
            .resolve(
                &route.shard_route,
                crate::shard_directory::Adoption::External,
            )
            .await
        {
            Ok(engine) => engine,
            Err(error) => return failure(ReadFailure::Resolve(error)),
        };
        let handle = match engine.stream_handle(route.identity).await {
            Ok(handle) => handle,
            Err(error) => return failure(ReadFailure::Storage(error.to_string())),
        };
        let start = match command.start {
            ReadStart::Beginning => StartPos::At(0),
            ReadStart::Now => StartPos::Now,
            ReadStart::Position(position)
                if position.segment
                    == desc
                        .resolve_segment(command.selector.as_deref().unwrap_or(""))
                        .seg_id =>
            {
                StartPos::At(position.after)
            }
            _ => return failure(ReadFailure::InvalidCursor),
        };
        return sse_response(
            state, desc, key, epoch, engine, handle, start, params, surface,
        )
        .await;
    }
    if command.selector.is_none() {
        return failure(ReadFailure::KeylessLive);
    }
    #[cfg(test)]
    crate::failpoints::pause_sse_before_lease_gate(&desc.name).await;
    if desc
        .segments
        .as_ref()
        .is_some_and(|map| map.pending.is_some())
    {
        let service = state.read_service();
        let sref = desc.sref();
        tokio::spawn(async move {
            service.topology.resume(&sref).await;
        });
        return err_resp(
            StatusCode::SERVICE_UNAVAILABLE,
            "segment_transition",
            "a segment transition is in flight; retry",
        );
    }
    let selector = command.selector;
    let source = match crate::sse::source::LineageSource::build(
        state.read_service(),
        desc.clone(),
        key.clone(),
        epoch,
        selector.clone(),
    )
    .await
    {
        Ok(source) => source,
        Err(error) => {
            use crate::sse::source::LineageBuildError as E;
            return match error {
                E::WrongOwner {
                    msg: _,
                    owner: Some(owner),
                } => {
                    crate::sse::auth::sse_stats::FEED_CUTOFF_WRONG_OWNER
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    crate::sse::auth::sse_stats::FEED_TOPOLOGY_DISCONNECTS
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    failure(ReadFailure::Resolve(
                        crate::shard_directory::ResolveError::NotOwner {
                            prefix: String::new(),
                            owner,
                        },
                    ))
                }
                E::WrongOwner { msg, .. } | E::Transient(msg) => failure(ReadFailure::Remote(
                    crate::application::read_remote::RemoteSpanError::Transport(msg),
                )),
                E::IncompatibleTopology(_) => failure(ReadFailure::InvalidCursor),
            };
        }
    };
    use crate::sse::feed::FeedSourceRead;
    let start = match command.start {
        ReadStart::Now => StartPos::Now,
        ReadStart::Beginning => StartPos::At(0),
        ReadStart::Position(position) => match source.logicalize(crate::sse::feed::WirePosition {
            seg_id: position.segment,
            local_after: position.after,
        }) {
            Some(position) => StartPos::At(position),
            None => return failure(ReadFailure::InvalidCursor),
        },
    };
    let slot = match sse_acquire(&state) {
        Ok(slot) => slot,
        Err(response) => return *response,
    };
    crate::sse::session::serve(
        state, desc, key, epoch, source, start, params, selector, surface, slot,
    )
    .await
}
