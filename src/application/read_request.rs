//! Public replay/long-poll application operation. Protocol adapters decode their
//! own cursors and render these typed positions; neither surface invokes the
//! other surface's HTTP handler.
use super::{ReadPosition, ReadService, ReadTopology};
use crate::crypto::StreamKey;
use crate::registry::StreamDesc;
use crate::shard::{Deliver, StreamHandle};
use std::time::Duration;

#[derive(Clone, Copy, Debug)]
pub(crate) enum ReadStart {
    Beginning,
    Now,
    Position(ReadPosition),
}
#[derive(Clone, Copy, Debug)]
pub(crate) enum ReadMode {
    Replay,
    LongPoll(Duration),
    Head,
}
#[derive(Clone)]
pub(crate) struct ReadCommand {
    pub descriptor: StreamDesc,
    pub key: Option<StreamKey>,
    pub start: ReadStart,
    pub selector: Option<String>,
    pub mode: ReadMode,
    pub visibility: Deliver,
    pub max_bytes: usize,
    pub tail_max_bytes: usize,
    pub allow_remote: bool,
    pub refresh: bool,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) enum ReadResultKind {
    Data,
    Snapshot,
    Timeout,
    Head,
    Handoff,
}
pub(crate) struct ReadOutcome {
    pub descriptor: StreamDesc,
    pub records: Vec<super::PlainRec>,
    pub contiguous: Option<bytes::Bytes>,
    pub next: ReadPosition,
    pub durable: Option<ReadPosition>,
    pub pending_from: Option<usize>,
    pub up_to_date: bool,
    pub closed: bool,
    pub kind: ReadResultKind,
    pub segmented: bool,
    pub identity: [u8; 16],
    pub scan_from: u64,
    pub end: u64,
    pub waited: bool,
    pub wait_micros: u64,
    pub read_micros: u64,
}
#[derive(Debug)]
pub(crate) enum ReadFailure {
    Missing,
    Gone,
    Creating,
    MissingKey,
    WrongKey,
    InvalidCursor,
    ChangedIncarnation,
    CursorBeyondTail,
    KeylessLive,
    AppliedFork,
    Resolve(crate::shard_directory::ResolveError),
    Storage(String),
    Remote(crate::application::read_remote::RemoteSpanError),
}
impl std::fmt::Display for ReadFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::error::Error for ReadFailure {}

impl ReadOutcome {
    fn empty(
        command: &ReadCommand,
        position: ReadPosition,
        end: u64,
        durable: u64,
        closed: bool,
        kind: ReadResultKind,
        identity: [u8; 16],
    ) -> Self {
        let segmented = command
            .descriptor
            .segments
            .as_ref()
            .is_some_and(|map| map.segments.len() > 1 || map.pending.is_some());
        Self {
            descriptor: command.descriptor.clone(),
            contiguous: None,
            records: vec![],
            next: position,
            durable: (command.visibility == Deliver::Applied).then_some(ReadPosition {
                segment: position.segment,
                after: position.after.min(durable),
            }),
            pending_from: None,
            up_to_date: !matches!(kind, ReadResultKind::Handoff | ReadResultKind::Head)
                && !(segmented && kind == ReadResultKind::Timeout),
            closed,
            kind,
            segmented,
            identity,
            scan_from: position.after,
            end,
            waited: false,
            wait_micros: 0,
            read_micros: 0,
        }
    }
}

impl ReadService {
    /// Authenticate the immutable incarnation before any shard/history work.
    pub(crate) fn authorize_read(command: &ReadCommand) -> Result<(), ReadFailure> {
        let desc = &command.descriptor;
        if !crate::application::creation::desc_alive(desc) {
            return Err(if desc.soft_deleted {
                ReadFailure::Gone
            } else {
                ReadFailure::Missing
            });
        }
        if desc.init.is_some() {
            return Err(ReadFailure::Creating);
        }
        let segmented = desc
            .segments
            .as_ref()
            .is_some_and(|m| m.segments.len() > 1 || m.pending.is_some());
        if !matches!(command.mode, ReadMode::Head) || segmented || desc.forked_from.is_some() {
            let key = command.key.as_ref().ok_or(ReadFailure::MissingKey)?;
            if key.fingerprint(&desc.epoch()) != desc.key_fingerprint {
                return Err(ReadFailure::WrongKey);
            }
        }
        if command.visibility == Deliver::Applied && desc.forked_from.is_some() {
            return Err(ReadFailure::AppliedFork);
        }
        if segmented && matches!(command.mode, ReadMode::LongPoll(_)) && command.selector.is_none()
        {
            return Err(ReadFailure::KeylessLive);
        }
        Ok(())
    }

    /// Refresh is bounded to one attempt and must retain the exact project and
    /// incarnation. A replacement at the same name is never an implicit retry.
    async fn refreshed_read(&self, mut command: ReadCommand) -> Result<ReadOutcome, ReadFailure> {
        let original = command.descriptor;
        self.registry.invalidate(&original.sref());
        let fresh = self
            .registry
            .get(&original.sref())
            .await
            .map_err(|e| ReadFailure::Storage(e.to_string()))?
            .ok_or(ReadFailure::Missing)?;
        if fresh.epoch() != original.epoch() {
            return Err(ReadFailure::ChangedIncarnation);
        }
        command.descriptor = fresh;
        command.refresh = false;
        Box::pin(self.execute_read(command)).await
    }

    pub(crate) async fn execute_read(
        &self,
        mut command: ReadCommand,
    ) -> Result<ReadOutcome, ReadFailure> {
        Self::authorize_read(&command)?;
        command.max_bytes = command.max_bytes.clamp(1, 8 << 20);
        command.tail_max_bytes = command.tail_max_bytes.clamp(1, command.max_bytes);
        if command.descriptor.forked_from.is_some() {
            return self.execute_fork_read(command).await;
        }
        let desc = &command.descriptor;
        let segmented = desc
            .segments
            .as_ref()
            .is_some_and(|m| m.segments.len() > 1 || m.pending.is_some());
        let topology = ReadTopology::new(desc, command.selector.as_deref());
        let desc = &topology.descriptor;
        let spans = &topology.spans;
        if spans.is_empty() {
            return Err(ReadFailure::Storage("empty read lineage".into()));
        }
        let (mut index, mut start) = match command.position_in(&topology) {
            Ok(position) => position,
            Err(_) if command.refresh => return self.refreshed_read(command).await,
            Err(error) => return Err(error),
        };
        let entry_index = index;
        loop {
            let span = &spans[index];
            let identity = desc.dynamic_segment_identity(span.seg_id);
            let route = desc.segment_route(span);
            let engine = match self
                .shards
                .resolve(&route, crate::shard_directory::Adoption::External)
                .await
            {
                Ok(engine) => engine,
                Err(error) => {
                    if let crate::shard_directory::ResolveError::NotOwner { owner, .. } = &error {
                        if command.allow_remote && !matches!(command.mode, ReadMode::LongPoll(_)) {
                            return crate::application::read_remote::remote_read_page(
                                &self.peer,
                                owner,
                                &command,
                                span.seg_id,
                                start,
                            )
                            .await;
                        }
                        if !command.allow_remote && index > entry_index {
                            return Ok(ReadOutcome::empty(
                                &command,
                                ReadPosition {
                                    segment: span.seg_id,
                                    after: 0,
                                },
                                0,
                                0,
                                false,
                                ReadResultKind::Handoff,
                                identity,
                            ));
                        }
                    }
                    return Err(ReadFailure::Resolve(error));
                }
            };
            let handle = engine
                .stream_handle(identity)
                .await
                .map_err(|e| ReadFailure::Storage(e.to_string()))?;
            if let Some(key) = &command.key {
                self.keys.put(identity, key.clone(), desc.epoch());
            }
            let (mut end, mut closed, mut durable) = tail_state(&handle, command.visibility);
            let last = index + 1 == spans.len();
            let live_last = span.sealed_next_offset.is_none() && last;
            if closed && live_last && command.refresh {
                let _ = self.topology.schedule(desc);
                return self.refreshed_read(command).await;
            }
            let seal_gap = closed
                && live_last
                && desc
                    .segments
                    .as_ref()
                    .and_then(|m| m.pending.as_ref())
                    .is_some_and(|p| p.segs.contains(&span.seg_id));
            end = span.sealed_next_offset.unwrap_or(end);
            if start == u64::MAX {
                start = end;
            }
            if command.visibility == Deliver::Applied && start > end {
                return Err(ReadFailure::CursorBeyondTail);
            }
            if start >= end && !last {
                index += 1;
                start = 0;
                continue;
            }
            let position = ReadPosition {
                segment: span.seg_id,
                after: start,
            };
            if matches!(command.mode, ReadMode::Head) {
                return Ok(ReadOutcome::empty(
                    &command,
                    ReadPosition {
                        after: end,
                        ..position
                    },
                    end,
                    durable,
                    closed && last && !seal_gap,
                    ReadResultKind::Head,
                    identity,
                ));
            }
            if matches!(command.start, ReadStart::Now) && matches!(command.mode, ReadMode::Replay) {
                let mut out = ReadOutcome::empty(
                    &command,
                    position,
                    end,
                    durable,
                    closed && !seal_gap,
                    ReadResultKind::Snapshot,
                    identity,
                );
                out.up_to_date = !seal_gap;
                return Ok(out);
            }
            let wait_started = std::time::Instant::now();
            let mut waited = false;
            if let ReadMode::LongPoll(wait) = command.mode
                && last
                && start >= end
                && !closed
            {
                (end, closed, durable) = wait_tail(&handle, command.visibility, start, wait).await;
                waited = end > start;
                if closed && command.refresh {
                    let _ = self.topology.schedule(desc);
                    return self.refreshed_read(command).await;
                }
            }
            let wait_micros = wait_started.elapsed().as_micros() as u64;
            if matches!(command.mode, ReadMode::LongPoll(_)) && start >= end {
                let resume = if segmented {
                    position
                } else {
                    ReadPosition {
                        after: end,
                        ..position
                    }
                };
                let mut out = ReadOutcome::empty(
                    &command,
                    resume,
                    end,
                    durable,
                    closed && !seal_gap,
                    ReadResultKind::Timeout,
                    identity,
                );
                out.up_to_date = !segmented && !seal_gap;
                out.wait_micros = wait_micros;
                return Ok(out);
            }
            return ResolvedRead {
                command: &command,
                topology: &topology,
                index,
                start,
                engine,
                handle,
                closed,
                seal_gap,
                segmented,
                waited,
                wait_micros,
            }
            .execute()
            .await;
        }
    }

    async fn execute_fork_read(&self, command: ReadCommand) -> Result<ReadOutcome, ReadFailure> {
        let desc = &command.descriptor;
        let (_, handle) = self.handle_of(desc).await.map_err(ReadFailure::Storage)?;
        let key = command.key.as_ref().ok_or(ReadFailure::MissingKey)?;
        self.keys.put(handle.hash, key.clone(), desc.epoch());
        let (mut end, mut closed, mut durable) = tail_state(&handle, Deliver::Durable);
        let segment = desc.resolve_segment("").seg_id;
        let start = match command.start {
            ReadStart::Beginning => 0,
            ReadStart::Now => end,
            ReadStart::Position(p) if p.segment == segment => p.after,
            _ => return Err(ReadFailure::InvalidCursor),
        };
        let position = ReadPosition {
            segment,
            after: start,
        };
        if matches!(command.mode, ReadMode::Head) {
            return Ok(ReadOutcome::empty(
                &command,
                ReadPosition {
                    after: end,
                    ..position
                },
                end,
                durable,
                closed,
                ReadResultKind::Head,
                handle.hash,
            ));
        }
        if let ReadMode::LongPoll(wait) = command.mode
            && start >= end
            && !closed
        {
            (end, closed, durable) = wait_tail(&handle, Deliver::Durable, start, wait).await;
        }
        if matches!(command.mode, ReadMode::LongPoll(_)) && start >= end {
            return Ok(ReadOutcome::empty(
                &command,
                ReadPosition {
                    after: end,
                    ..position
                },
                end,
                durable,
                closed,
                ReadResultKind::Timeout,
                handle.hash,
            ));
        }
        let page = self
            .read_stitched(desc, key, start, command.max_bytes)
            .await
            .map_err(ReadFailure::Storage)?;
        let next = ReadPosition {
            segment,
            after: page.scanned_through(start),
        };
        let closed = handle.state.lock().unwrap().durable.closed;
        Ok(ReadOutcome {
            descriptor: desc.clone(),
            contiguous: page.contiguous,
            records: page.recs,
            next,
            durable: None,
            pending_from: None,
            up_to_date: page.completed,
            closed: closed && page.completed,
            kind: ReadResultKind::Data,
            segmented: false,
            identity: handle.hash,
            scan_from: start,
            end: page.end,
            waited: false,
            wait_micros: 0,
            read_micros: 0,
        })
    }
}

fn tail_state(handle: &StreamHandle, visibility: Deliver) -> (u64, bool, u64) {
    let state = handle.state.lock().unwrap();
    let durable = state.durable.next;
    (
        match visibility {
            Deliver::Durable => durable,
            Deliver::Applied => state.applied.next.max(durable),
        },
        state.durable.closed,
        durable,
    )
}
async fn wait_tail(
    handle: &StreamHandle,
    visibility: Deliver,
    start: u64,
    wait: Duration,
) -> (u64, bool, u64) {
    let deadline = tokio::time::Instant::now() + wait.min(Duration::from_secs(25));
    loop {
        let durable = handle.notify.notified();
        let applied = handle.applied_notify.notified();
        let state = tail_state(handle, visibility);
        if state.0 > start || state.1 {
            return state;
        }
        if visibility == Deliver::Applied {
            tokio::select! {_=durable=>{},_=applied=>{},_=tokio::time::sleep_until(deadline)=>return tail_state(handle,visibility)}
        } else {
            tokio::select! {_=durable=>{},_=tokio::time::sleep_until(deadline)=>return tail_state(handle,visibility)}
        }
    }
}

/// A resolved local physical span carries all inputs that may cross the async
/// history read. Completion derives scanned and durable cursors from one page.
struct ResolvedRead<'a> {
    command: &'a ReadCommand,
    topology: &'a ReadTopology,
    index: usize,
    start: u64,
    engine: std::sync::Arc<crate::shard::ShardEngine>,
    handle: std::sync::Arc<StreamHandle>,
    closed: bool,
    seal_gap: bool,
    segmented: bool,
    waited: bool,
    wait_micros: u64,
}
impl ResolvedRead<'_> {
    async fn execute(self) -> Result<ReadOutcome, ReadFailure> {
        let command = self.command;
        let topology = self.topology;
        let span = &topology.spans[self.index];
        let desc = &command.descriptor;
        let last = self.index + 1 == topology.spans.len();
        let identity = self.handle.hash;
        let Self {
            start,
            handle,
            engine,
            closed,
            seal_gap,
            segmented,
            waited,
            wait_micros,
            ..
        } = self;
        let key = command.key.as_ref().ok_or(ReadFailure::MissingKey)?;
        let read_started = std::time::Instant::now();
        let page = super::ReadPlan::segment(
            key,
            &desc.epoch(),
            &handle,
            &engine,
            start,
            command.selector.as_deref(),
            if waited {
                command.max_bytes.min(command.tail_max_bytes)
            } else {
                command.max_bytes
            },
            command.visibility,
        )
        .execute()
        .await
        .map_err(ReadFailure::Storage)?;
        let read_micros = read_started.elapsed().as_micros() as u64;
        let (next, mut durable_resume, drained) = topology
            .page_progress(span.seg_id, start, &page)
            .ok_or(ReadFailure::InvalidCursor)?;
        let floor = handle.state.lock().unwrap().durable.next;
        let hopped = next.segment != span.seg_id;
        if !hopped {
            durable_resume.after = next.after.min(floor);
        }
        let pending = page.recs.iter().position(|record| record.off >= floor);
        let complete = drained && last;
        Ok(ReadOutcome {
            descriptor: desc.clone(),
            contiguous: page.contiguous,
            records: page.recs,
            next,
            durable: (command.visibility == Deliver::Applied).then_some(durable_resume),
            pending_from: (command.visibility == Deliver::Applied)
                .then_some(pending)
                .flatten(),
            up_to_date: complete && !seal_gap,
            closed: complete && closed && !seal_gap,
            kind: ReadResultKind::Data,
            segmented,
            identity,
            scan_from: start,
            end: page.end,
            waited,
            wait_micros,
            read_micros,
        })
    }
}

impl ReadCommand {
    fn position_in(&self, topology: &ReadTopology) -> Result<(usize, u64), ReadFailure> {
        match self.start {
            ReadStart::Beginning => Ok((0, 0)),
            ReadStart::Now => Ok((topology.spans.len() - 1, u64::MAX)),
            ReadStart::Position(position) => topology
                .spans
                .iter()
                .position(|span| span.seg_id == position.segment)
                .map(|index| (index, position.after))
                .ok_or(ReadFailure::InvalidCursor),
        }
    }
}
