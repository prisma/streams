//! A tail subscription of the multiplexed wait: a records long-poll on one
//! stream, answered from the same read service and cursors as
//! `GET {collection}/records:long-poll` (WIRE-MATRIX §2.6).
//!
//! A tail at a durable position on a stream with one live segment (a
//! simple tail) is ready when the segment's durable tail has passed the
//! position, the stream closed or its engine retired: a lock and a compare,
//! no page read, so a re-issued wait costs its idle tails nothing. Any
//! other tail (no position yet, a segmented or forked stream) reads first
//! and then watches the stream's live handle, reading again when it moves.
//! A read that delivers records or moves the cursor is an answer; one that
//! does neither re-arms.
use std::sync::Arc;

use serde_json::value::RawValue;

use super::super::read_cursor::CursorBinding;
use super::super::{READ_MAX_BYTES_CAP, perr};
use super::AppState;
use super::Refusal;
use super::wire::TailSpec;
use crate::application::read::{ReadCommand, ReadMode, ReadOutcome, ReadPosition, ReadStart};
use crate::crypto::StreamKey;
use crate::registry::StreamDesc;
use crate::shard::{Deliver, ShardEngine, StreamHandle};
use axum::http::StatusCode;

/// The longest routing key, as the per-stream read takes.
const MAX_ROUTING_KEY: usize = 1024;

pub(super) struct Tail {
    desc: StreamDesc,
    key: StreamKey,
    epoch: [u8; 16],
    routing_key: String,
    start: ReadStart,
    simple: bool,
    /// Read at least once in this request: a non-simple tail arms only
    /// after a read placed its cursor.
    read_once: bool,
    wait: Wait,
}

/// How a tail waits while the request waits.
enum Wait {
    /// Read at once (no position yet, or not armed since its last read).
    Read,
    /// On the live handle.
    Armed(Armed),
    /// Its stream's live span is closed or sealed (a split between its
    /// fence and its successors' placement) or its shard is opening: read
    /// again at, never waiting on a closed handle no commit will wake.
    Poll(tokio::time::Instant),
    /// Its stream is not served here.
    Elsewhere,
}

/// How long a tail whose live span is not placed waits before it reads
/// again, as a pull with no local notification walks (`pull_wait`).
const UNPLACED_POLL: std::time::Duration = std::time::Duration::from_millis(50);

/// What a tail watches while the request waits.
struct Armed {
    engine: Arc<ShardEngine>,
    handle: Arc<StreamHandle>,
    /// The position the tail waits past: its cursor in the live segment,
    /// or the tail it saw when its cursor is in another segment.
    after: u64,
    /// The payload bytes before `after` when it was armed, the measure of
    /// what a woken read may render.
    payload: u64,
}

/// A rendered tail answer.
pub(super) struct TailAnswer {
    pub(super) records: Box<RawValue>,
    pub(super) cursor: String,
    pub(super) up_to_date: bool,
    pub(super) sealed: bool,
    pub(super) payload_bytes: u64,
    pub(super) record_count: u64,
    pub(super) descriptor: StreamDesc,
}

impl Tail {
    /// Resolve a tail as `product_read` does: the stream, its key, the
    /// routing key and the cursor. `Err` is the per-subscription answer.
    pub(super) async fn prepare(
        state: &Arc<AppState>,
        sref: &crate::tenant::TenantStreamRef,
        spec: TailSpec,
    ) -> Result<Self, Refusal> {
        let TailSpec {
            key,
            cursor,
            routing_key,
            applied,
        } = spec;
        if applied {
            return Err(Box::new(perr(
                StatusCode::BAD_REQUEST,
                "deliver_unsupported",
                "deliver=applied is served by the per-stream long-poll, not the multiplexed wait",
                None,
                false,
            )));
        }
        if routing_key.len() > MAX_ROUTING_KEY {
            return Err(Box::new(perr(
                StatusCode::BAD_REQUEST,
                "invalid_routing_key",
                "routing key exceeds 1,024 bytes",
                None,
                false,
            )));
        }
        let desc = super::live_descriptor(state, sref).await?;
        if !desc.is_json() {
            return Err(Box::new(super::unsupported_format()));
        }
        let (key, epoch) = super::stream_key(&key, &desc)?;
        let binding = CursorBinding::of(&desc, epoch, &key, &routing_key);
        let start = super::cursor_memo::start(&binding, cursor.as_deref(), |token| {
            super::cursor_memo::Decoded {
                project: desc.project_id.clone(),
                epoch,
                fingerprint: desc.key_fingerprint.clone(),
                routing_key: routing_key.clone(),
                token,
            }
        })
        .map_err(|error| Box::new(binding.failure(error)))?;
        if let Err(error) = state.creation_service().renew_ttl(&desc).await {
            return Err(Box::new(super::retry_unavailable(&error.to_string())));
        }
        let simple = desc.forked_from.is_none()
            && desc
                .segments
                .as_ref()
                .is_none_or(|m| m.segments.len() <= 1 && m.pending.is_none());
        Ok(Self {
            desc,
            key,
            epoch,
            routing_key,
            start,
            simple,
            read_once: false,
            wait: Wait::Read,
        })
    }

    /// Watch the stream's live handle at the cursor when the cursor is in
    /// the live segment (a simple tail at once, any other once a read
    /// placed it), else at the tail it sees now. A stream whose live span
    /// is closed or not placed (a split in progress, a shard opening) is
    /// read again after a short poll; one this instance does not serve is
    /// answered `not_owner`; a tail with no position is read at once.
    pub(super) async fn arm(&mut self, state: &Arc<AppState>) {
        self.wait = match live_handle(state, &self.desc, &self.routing_key).await {
            Placed::Here(engine, handle, segment) => self.armed(engine, handle, segment),
            Placed::Unplaced => Wait::Poll(tokio::time::Instant::now() + UNPLACED_POLL),
            Placed::Elsewhere => Wait::Elsewhere,
        };
    }

    fn armed(&self, engine: Arc<ShardEngine>, handle: Arc<StreamHandle>, segment: u32) -> Wait {
        let (tail, payload, closed) = handle.state.lock().map_or((0, 0, true), |state| {
            (
                state.durable.next,
                state.durable.logical,
                state.durable.closed,
            )
        });
        if closed || engine.is_closed() {
            return Wait::Poll(tokio::time::Instant::now() + UNPLACED_POLL);
        }
        let after = match self.start {
            ReadStart::Position(p)
                if (self.simple || self.read_once) && p.segment == segment && p.after <= tail =>
            {
                p.after
            }
            ReadStart::Position(_) if self.read_once => tail,
            _ => return Wait::Read,
        };
        Wait::Armed(Armed {
            engine,
            handle,
            after,
            payload: if after == tail { payload } else { 0 },
        })
    }

    /// The handle a waiting request listens on, if armed.
    pub(super) fn handle(&self) -> Option<&Arc<StreamHandle>> {
        match &self.wait {
            Wait::Armed(armed) => Some(&armed.handle),
            Wait::Read | Wait::Poll(_) | Wait::Elsewhere => None,
        }
    }

    /// When a polling tail reads again.
    pub(super) fn poll_at(&self) -> Option<tokio::time::Instant> {
        match self.wait {
            Wait::Poll(at) => Some(at),
            Wait::Read | Wait::Armed(_) | Wait::Elsewhere => None,
        }
    }

    /// Whether its stream is served elsewhere.
    pub(super) fn elsewhere(&self) -> bool {
        matches!(self.wait, Wait::Elsewhere)
    }

    /// Whether a read may answer now: not armed, a poll that is due, or an
    /// armed tail that moved past its position, closed (a seal, a split's
    /// fence) or lost its engine.
    pub(super) fn ready(&self) -> bool {
        let armed = match &self.wait {
            Wait::Read => return true,
            Wait::Poll(at) => return tokio::time::Instant::now() >= *at,
            Wait::Elsewhere => return false,
            Wait::Armed(armed) => armed,
        };
        if armed.engine.is_closed() {
            return true;
        }
        let Ok(state) = armed.handle.state.lock() else {
            return true;
        };
        state.durable.next > armed.after || state.durable.closed
    }

    /// What a woken read may render: the payload appended since it armed
    /// and its framing (as a woken long-poll measures, `read_request.rs`),
    /// or `None`, the whole share, for an unarmed tail.
    pub(super) fn appended(&self) -> Option<u64> {
        let Wait::Armed(armed) = &self.wait else {
            return None;
        };
        if armed.payload == 0 {
            return None;
        }
        let (tail, payload) = tail_payload(&armed.handle);
        Some(payload.saturating_sub(armed.payload) + 64 * tail.saturating_sub(armed.after) + 4096)
    }

    /// Read from the cursor, at most `max_bytes`: `Ok(None)` when the read
    /// neither delivered a record nor moved the cursor (the tail re-arms),
    /// `Err` the per-subscription failure.
    pub(super) async fn read(
        &mut self,
        state: &Arc<AppState>,
        max_bytes: usize,
    ) -> Result<Option<TailAnswer>, Refusal> {
        let budget = max_bytes.clamp(1, READ_MAX_BYTES_CAP);
        let command = ReadCommand {
            descriptor: self.desc.clone(),
            key: Some(self.key.clone()),
            start: self.start,
            selector: Some(self.routing_key.clone()),
            mode: ReadMode::Replay,
            visibility: Deliver::Durable,
            max_bytes: budget,
            tail_max_bytes: budget,
            allow_remote: false,
            refresh: true,
        };
        let binding = CursorBinding::of(&self.desc, self.epoch, &self.key, &self.routing_key);
        let outcome = state
            .read_service()
            .execute_read(command)
            .await
            .map_err(|error| Box::new(binding.failure(error)))?;
        let moved = !self.at(outcome.next);
        self.start = ReadStart::Position(outcome.next);
        self.read_once = true;
        if outcome.records.is_empty() && !moved && !(outcome.closed && outcome.up_to_date) {
            return Ok(None);
        }
        self.answer(&binding, &outcome).map(Some)
    }

    /// Whether the cursor already names `position`.
    fn at(&self, position: ReadPosition) -> bool {
        matches!(self.start, ReadStart::Position(p) if p == position)
    }

    /// The answer for `outcome`: its records as the JSON array a records
    /// page serves, and its cursors.
    fn answer(
        &self,
        binding: &CursorBinding<'_>,
        outcome: &ReadOutcome,
    ) -> Result<TailAnswer, Refusal> {
        let records = json_page(outcome).map_err(|message| Box::new(super::internal(&message)))?;
        Ok(TailAnswer {
            records,
            cursor: binding.session(outcome.next, outcome.continuation),
            up_to_date: outcome.up_to_date,
            sealed: outcome.closed,
            payload_bytes: outcome.records.iter().map(|r| r.payload.len() as u64).sum(),
            record_count: outcome.records.len() as u64,
            descriptor: outcome.descriptor.clone(),
        })
    }

    pub(super) fn descriptor(&self) -> &StreamDesc {
        &self.desc
    }
}

/// The records of a JSON stream's page as the JSON array a records page
/// serves (`read_payload`): each record's stored text, comma-separated.
fn json_page(outcome: &ReadOutcome) -> Result<Box<RawValue>, String> {
    let mut text = Vec::with_capacity(
        outcome
            .records
            .iter()
            .map(|r| r.payload.len() + 1)
            .sum::<usize>()
            + 2,
    );
    text.push(b'[');
    for (index, record) in outcome.records.iter().enumerate() {
        if index > 0 {
            text.push(b',');
        }
        text.extend_from_slice(&record.payload);
    }
    text.push(b']');
    let text = String::from_utf8(text).map_err(|e| e.to_string())?;
    RawValue::from_string(text).map_err(|e| e.to_string())
}

/// The durable tail of `handle` and the payload bytes before it; a
/// poisoned handle reads as empty, which arms nothing.
fn tail_payload(handle: &StreamHandle) -> (u64, u64) {
    handle
        .state
        .lock()
        .map_or((0, 0), |state| (state.durable.next, state.durable.logical))
}

/// Where a tail's live span is.
enum Placed {
    /// Served here: its engine, handle and segment.
    Here(Arc<ShardEngine>, Arc<StreamHandle>, u32),
    /// Not placed yet: the last span sealed before its successors are, or
    /// its shard not open.
    Unplaced,
    /// Owned by another instance.
    Elsewhere,
}

/// The engine, handle and segment a long-poll on `desc` waits on: a
/// fork's own, or the live last span of its lineage.
async fn live_handle(state: &Arc<AppState>, desc: &StreamDesc, routing_key: &str) -> Placed {
    if desc.forked_from.is_some() {
        return match state.read_service().handle_of(desc).await {
            Ok((engine, handle)) => Placed::Here(engine, handle, desc.resolve_segment("").seg_id),
            Err(_) => Placed::Unplaced,
        };
    }
    let topology = crate::application::read::ReadTopology::new(desc, Some(routing_key));
    let Some(span) = topology.spans.last() else {
        return Placed::Unplaced;
    };
    if span.sealed_next_offset.is_some() {
        return Placed::Unplaced;
    }
    let route = desc.segment_route(span);
    let engine = match state
        .shards
        .resolve(&route, crate::shard_directory::Adoption::External)
        .await
    {
        Ok(engine) => engine,
        Err(crate::shard_directory::ResolveError::NotOwner { .. }) => return Placed::Elsewhere,
        Err(_) => return Placed::Unplaced,
    };
    match engine
        .stream_handle(desc.dynamic_segment_identity(span.seg_id))
        .await
    {
        Ok(handle) => Placed::Here(engine, handle, span.seg_id),
        Err(_) => Placed::Unplaced,
    }
}
