//! The SSE wire layer — the SOLE owner of protocol framing
//! (LIVE-FEED Stage 1). Data-event encoding is common across
//! surfaces (JSON arrays, text lines, base64 binary); only the
//! control vocabulary differs between the raw and product surfaces.
//! No feed or session code may format SSE bytes directly.

use crate::registry::StreamDesc;

/// The shared data event for one record payload. Formatted ONCE per
/// record regardless of subscriber count; sessions append their own
/// control frames.
pub(crate) fn sse_data_event(desc: &StreamDesc, payload: &[u8]) -> String {
    let mut ev = String::from("event: data\n");
    let mt = crate::registry::media_type(&desc.content_type);
    if mt == "application/json" {
        ev.push_str("data:[");
        ev.push_str(&String::from_utf8_lossy(payload));
        ev.push_str("]\n\n");
    } else if mt.starts_with("text/") {
        let text = String::from_utf8_lossy(payload);
        for line in text.split(['\r', '\n']) {
            ev.push_str("data:");
            ev.push_str(line);
            ev.push('\n');
        }
        ev.push('\n');
    } else {
        use base64::Engine;
        ev.push_str("data:");
        ev.push_str(&base64::engine::general_purpose::STANDARD.encode(payload));
        ev.push_str("\n\n");
    }
    ev
}

/// Raw-surface control with a pre-encoded (epoch) cursor token — the
/// lineage streamer's controls name segments, not scalar offsets.
pub(crate) fn sse_control_tok(
    next_tok: &str,
    cursor: Option<&str>,
    up_to_date: bool,
    closed: bool,
) -> String {
    let mut fields = vec![format!("\"streamNextOffset\":\"{next_tok}\"")];
    if !closed && let Some(c) = cursor {
        fields.push(format!(
            "\"streamCursor\":\"{}\"",
            crate::http::interval_cursor(Some(c))
        ));
    }
    if up_to_date {
        fields.push("\"upToDate\":true".to_string());
    }
    if closed {
        fields.push("\"streamClosed\":true".to_string());
    }
    format!("event: control\ndata:{{{}}}\n\n", fields.join(","))
}

/// Product control: signed key cursor + product field names.
pub(crate) fn sse_control_product(cursor_tok: &str, up_to_date: bool, sealed: bool) -> String {
    let mut fields = vec![format!("\"nextCursor\":\"{cursor_tok}\"")];
    if up_to_date {
        fields.push("\"upToDate\":true".to_string());
    }
    if sealed {
        fields.push("\"sealed\":true".to_string());
    }
    format!("event: control\ndata:{{{}}}\n\n", fields.join(","))
}

/// Raw scalar-offset control (the pinned single-segment surface).
/// Round-11.3: the ONE raw vocabulary — an epoch/segment offset token
/// with the exact field layout of the scalar control. For segment 0
/// the token is byte-identical to the scalar encoding
/// (`encode_ep(0, o) == Offset::encode(o)`), so unsplit raw
/// transcripts do not change; successor segments carry the
/// segment-aware token the legacy lineage streamer already proved.
pub(crate) fn sse_control_ep(
    seg_id: u32,
    next: u64,
    cursor: Option<&str>,
    up_to_date: bool,
    closed: bool,
) -> String {
    let tok = crate::offsets::encode_ep(
        seg_id,
        if next == 0 {
            crate::offsets::Offset::START
        } else {
            crate::offsets::Offset(Some(next - 1))
        },
    );
    let mut fields = vec![format!("\"streamNextOffset\":\"{tok}\"")];
    if !closed {
        fields.push(format!(
            "\"streamCursor\":\"{}\"",
            crate::http::interval_cursor(cursor)
        ));
    }
    if up_to_date {
        fields.push("\"upToDate\":true".to_string());
    }
    if closed {
        fields.push("\"streamClosed\":true".to_string());
    }
    format!("event: control\ndata:{{{}}}\n\n", fields.join(","))
}

pub(crate) fn sse_control(
    next: u64,
    cursor: Option<&str>,
    up_to_date: bool,
    closed: bool,
) -> String {
    let mut fields = vec![format!(
        "\"streamNextOffset\":\"{}\"",
        crate::http::tail_token(next)
    )];
    if !closed {
        fields.push(format!(
            "\"streamCursor\":\"{}\"",
            crate::http::interval_cursor(cursor)
        ));
    }
    if up_to_date {
        fields.push("\"upToDate\":true".to_string());
    }
    if closed {
        fields.push("\"streamClosed\":true".to_string());
    }
    format!("event: control\ndata:{{{}}}\n\n", fields.join(","))
}
