//! Product read cursors (TLA-018-F3): decodes a `cursor=` into the read
//! service's typed start and renders a page's positions back into signed
//! tokens. A durable position is a v2 key cursor; a `deliver=applied`
//! session position past the durable frontier is a v3 cursor carrying its
//! [`Continuation`]; a continuation the service refuses renders as the
//! resynchronisation answer with the durable recovery cursor.
use axum::http::StatusCode;
use axum::response::Response;

use super::{perr, render_product_read_failure};
use crate::application::read::{Continuation, ReadFailure, ReadPosition, ReadStart};
use crate::crypto::StreamKey;
use crate::product_cursor::{KeyCursor, ReadCursor, SessionCursor};

/// What a product read's cursors are bound to: the stream incarnation, its
/// project and key, and the routing key.
pub(super) struct CursorBinding<'a> {
    project: crate::tenant::ProjectId,
    epoch: [u8; 16],
    key: &'a StreamKey,
    routing_key: &'a str,
}

impl<'a> CursorBinding<'a> {
    pub(super) fn of(
        desc: &crate::registry::StreamDesc,
        epoch: [u8; 16],
        key: &'a StreamKey,
        routing_key: &'a str,
    ) -> Self {
        Self {
            project: desc.project_id.clone(),
            epoch,
            key,
            routing_key,
        }
    }

    fn position(&self, position: ReadPosition) -> KeyCursor {
        KeyCursor {
            epoch: self.epoch,
            key_hash: crate::crypto::stream_hash(self.routing_key),
            seg_id: position.segment,
            offset: position.after,
        }
    }

    /// A durable position (v2).
    pub(super) fn durable(&self, position: ReadPosition) -> String {
        self.position(position).encode(&self.project, self.key)
    }

    /// A session position: v3 while it continues a provisional suffix.
    pub(super) fn session(
        &self,
        position: ReadPosition,
        continuation: Option<Continuation>,
    ) -> String {
        let Some(continuation) = continuation else {
            return self.durable(position);
        };
        let (history, recover, from, digest) = continuation.to_parts();
        SessionCursor {
            position: self.position(position),
            history,
            recover,
            from,
            digest,
        }
        .encode(&self.project, self.key)
    }

    /// `cursor=`: absent/`beginning`, `now`, a durable position or a
    /// continuation. A v2 token never carries a continuation.
    pub(super) fn start(&self, cursor: Option<&str>) -> Result<ReadStart, ReadFailure> {
        let token = match cursor {
            None | Some("" | "beginning") => return Ok(ReadStart::Beginning),
            Some("now") => return Ok(ReadStart::Now),
            Some(token) => token,
        };
        let key_hash = crate::crypto::stream_hash(self.routing_key);
        let decoded = ReadCursor::decode(token, &self.project, self.key, &self.epoch, &key_hash)
            .map_err(|_| ReadFailure::InvalidCursor)?;
        Ok(match decoded {
            ReadCursor::Durable(cursor) => ReadStart::Position(ReadPosition {
                segment: cursor.seg_id,
                after: cursor.offset,
            }),
            ReadCursor::Session(cursor) => ReadStart::Continue(
                ReadPosition {
                    segment: cursor.position.seg_id,
                    after: cursor.position.offset,
                },
                Continuation::from_parts(
                    cursor.history,
                    cursor.recover,
                    cursor.from,
                    cursor.digest,
                ),
            ),
        })
    }

    /// A read refusal. A replaced history answers `409 cursor_beyond_tail`
    /// (the code every client already resumes from its durable cursor on)
    /// with the recovery cursor as `Prisma-Durable-Cursor` and in
    /// `details`, so a client that lost its own durable cursor still
    /// resynchronises exactly.
    #[expect(
        clippy::unwrap_used,
        reason = "CursorBinding::failure; a signed cursor is base64url text, always a valid header value; a fallible insert would drop the recovery position the client is owed"
    )]
    pub(super) fn failure(&self, error: ReadFailure) -> Response {
        let ReadFailure::HistoryReplaced(recover) = error else {
            return render_product_read_failure(error);
        };
        let cursor = self.durable(recover);
        let mut response = perr(
            StatusCode::CONFLICT,
            "cursor_beyond_tail",
            "the provisional records this cursor continues were replaced; resume from the durable cursor",
            Some(serde_json::Value::Object(serde_json::Map::from_iter([
                ("reason".to_string(), "history_replaced".into()),
                ("durableCursor".to_string(), cursor.clone().into()),
            ]))),
            false,
        );
        response
            .headers_mut()
            .insert("Prisma-Durable-Cursor", cursor.parse().unwrap());
        response
    }
}
