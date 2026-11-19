//! Bounded request-triggered maintenance, owned by the runtime supervisor.
//! Callers may discard their tickets; they never own a spawned worker.
use crate::tenant::TenantStreamRef;
use futures_util::{FutureExt, StreamExt, stream::FuturesUnordered};
use std::{
    collections::{HashMap, VecDeque},
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::sync::{Notify, watch};

pub(crate) const MAX_ACTIVE: usize = 8;
pub(crate) const MAX_QUEUED: usize = 64;
const JOB_TIMEOUT: Duration = Duration::from_secs(10);
/// The active slots orphaned stream-count seeds may hold at once; the rest
/// wait queued with their walk locks (Addendum 1 of the forward-wedge
/// ruling, 2026-11-19).
pub(crate) const SEED_SHARE: usize = 2;
/// A seed has no total deadline: each of its store steps has its own.
const SEED_TIMEOUT: Duration = Duration::from_secs(30 * 365 * 86_400);

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum Kind {
    Topology,
    Ttl,
    QueueDelete,
    StreamSeed,
}
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct Key {
    pub stream: TenantStreamRef,
    pub epoch: String,
    pub kind: Kind,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum WorkError {
    Overloaded,
    Stopped,
    TimedOut,
    Storage(String),
}
impl std::fmt::Display for WorkError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Overloaded => f.write_str("request maintenance queue is full; retry"),
            Self::Stopped => {
                f.write_str("request maintenance owner stopped; retry on a serving instance")
            }
            Self::TimedOut => f.write_str("request maintenance deadline expired; retry"),
            Self::Storage(message) => write!(f, "request maintenance storage failure: {message}"),
        }
    }
}
type ResultWatch = watch::Receiver<Option<Result<(), WorkError>>>;
pub(crate) struct Ticket(Option<(ResultWatch, tokio::time::Instant)>);
impl Ticket {
    pub(crate) fn complete() -> Self {
        Self(None)
    }
    #[expect(
        clippy::excessive_nesting,
        reason = "Ticket::wait; the wait nests the settled verdict inside the watch loop; flattening it would separate the verdict from the change that produced it"
    )]
    pub(crate) async fn wait(self) -> Result<(), WorkError> {
        let Some((mut result, deadline)) = self.0 else {
            return Ok(());
        };
        tokio::time::timeout_at(deadline, async {
            loop {
                if let Some(result) = result.borrow().clone() {
                    return result;
                }
                result.changed().await.map_err(|_| WorkError::Stopped)?;
            }
        })
        .await
        .map_err(|_| WorkError::TimedOut)?
    }
}
pub(crate) enum Action {
    Ttl(super::creation::TtlMutation),
    Topology {
        service: super::topology::TopologyService,
        stream: TenantStreamRef,
        epoch: String,
    },
    /// A deleting queue's cleanup (`queues::QueueDeletion`).
    QueueDelete(Box<super::queues::QueueDeletion>),
    /// A stream-count seed pass its create left unfinished (`quota::stream_seed`).
    StreamSeed(futures_util::future::BoxFuture<'static, ()>),
    #[cfg(test)]
    Held(futures_util::future::BoxFuture<'static, Result<(), WorkError>>),
}
impl Action {
    async fn run(self) -> Result<(), WorkError> {
        match self {
            Self::Ttl(mutation) => mutation.run().await,
            Self::Topology {
                service,
                stream,
                epoch,
            } => {
                // Failure/cancellation retains the registry's persisted pending
                // transition for the next reader or retained scaler hint.
                super::topology::resume_fenced(&service, &stream, &epoch).await;
                Ok(())
            }
            Self::QueueDelete(deletion) => deletion.run().await,
            Self::StreamSeed(pass) => {
                pass.await;
                Ok(())
            }
            #[cfg(test)]
            Self::Held(future) => future.await,
        }
    }
}
struct Job {
    key: Key,
    action: Action,
    result: watch::Sender<Option<Result<(), WorkError>>>,
    deadline: tokio::time::Instant,
}
#[derive(Default)]
struct State {
    started: bool,
    stopped: bool,
    pending: VecDeque<Job>,
    admitted: HashMap<Key, (ResultWatch, tokio::time::Instant)>,
    active: usize,
    /// Active stream-count seeds, at most `SEED_SHARE`.
    seeds: usize,
    rejected: u64,
}
impl State {
    /// The first pending job a slot may run: a seed only while seeds hold
    /// fewer than `SEED_SHARE` slots.
    fn next_job(&mut self) -> Option<Job> {
        let seeds_full = self.seeds >= SEED_SHARE;
        let at = self
            .pending
            .iter()
            .position(|job| !(seeds_full && job.key.kind == Kind::StreamSeed))?;
        let job = self.pending.remove(at)?;
        self.seeds += usize::from(job.key.kind == Kind::StreamSeed);
        Some(job)
    }
}
#[derive(Default)]
pub(crate) struct RequestWork {
    state: Mutex<State>,
    wake: Notify,
}
impl RequestWork {
    #[expect(
        clippy::unwrap_used,
        reason = "RequestWork::start; a poisoned work state may hold a half-admitted ticket; recovering it could run a request twice or never"
    )]
    pub(crate) fn start(
        self: &Arc<Self>,
        tasks: &crate::tasks::TaskSupervisor,
    ) -> Result<(), crate::tasks::SpawnRejected> {
        {
            let mut state = self.state.lock().unwrap();
            if state.stopped {
                return Err(crate::tasks::SpawnRejected::Stopped);
            }
            if state.started {
                return Ok(());
            }
            state.started = true;
        }
        let guard = StopGuard(self.clone());
        let worker = self.clone();
        tasks
            .spawn(
                "request-maintenance",
                crate::tasks::Policy::Critical,
                |cancel| async move {
                    let _guard = guard;
                    worker.run(cancel).await;
                    crate::tasks::TaskResult::Done
                },
            )
            .map(|_| ())
    }

    pub(crate) fn submit(&self, key: Key, action: Action) -> Result<Ticket, WorkError> {
        self.admit(key, action, tokio::time::Instant::now() + JOB_TIMEOUT)
    }

    /// Run `pass`, a stream-count seed its create left unfinished, under the
    /// seeds' share and with no total deadline; the share's slot is given up
    /// when the running pass ends or is dropped. Never panics: a poisoned state
    /// refuses it `Stopped`.
    pub(crate) fn submit_seed(
        self: &Arc<Self>,
        key: Key,
        pass: futures_util::future::BoxFuture<'static, ()>,
    ) -> Result<Ticket, WorkError> {
        let work = self.clone();
        // The slot exists once the pass runs: a pass refused or dropped while
        // queued never held one.
        let pass = async move {
            let _slot = SeedSlot(work);
            pass.await;
        };
        let Ok(mut state) = self.state.lock() else {
            return Err(WorkError::Stopped);
        };
        let deadline = tokio::time::Instant::now() + SEED_TIMEOUT;
        let action = Action::StreamSeed(Box::pin(pass));
        self.admit_into(&mut state, key, action, deadline)
    }

    #[expect(
        clippy::unwrap_used,
        reason = "RequestWork::admit; a poisoned work state may hold a half-admitted ticket; recovering it could run a request twice or never"
    )]
    fn admit(
        &self,
        key: Key,
        action: Action,
        deadline: tokio::time::Instant,
    ) -> Result<Ticket, WorkError> {
        let mut state = self.state.lock().unwrap();
        self.admit_into(&mut state, key, action, deadline)
    }

    /// `admit` under the state lock it holds.
    fn admit_into(
        &self,
        state: &mut State,
        key: Key,
        action: Action,
        deadline: tokio::time::Instant,
    ) -> Result<Ticket, WorkError> {
        if !state.started || state.stopped {
            return Err(WorkError::Stopped);
        }
        if let Some(existing) = state.admitted.get(&key) {
            return Ok(Ticket(Some(existing.clone())));
        }
        if state.pending.len() >= MAX_QUEUED {
            state.rejected += 1;
            return Err(WorkError::Overloaded);
        }
        let (result, watch) = watch::channel(None);
        state
            .admitted
            .insert(key.clone(), (watch.clone(), deadline));
        state.pending.push_back(Job {
            key,
            action,
            result,
            deadline,
        });
        self.wake.notify_one();
        Ok(Ticket(Some((watch, deadline))))
    }

    #[expect(
        clippy::let_underscore_must_use,
        reason = "RequestWork::run; the requester may have stopped waiting for its completion; a handled send would only restate that the ticket was abandoned"
    )]
    #[expect(
        clippy::unwrap_used,
        reason = "RequestWork::run; a poisoned work state may hold a half-admitted ticket; recovering it could run a request twice or never"
    )]
    #[expect(
        clippy::excessive_nesting,
        reason = "RequestWork::run; the runner nests the admission of pending jobs inside the state lock of each drain; flattening it would separate the admission from the state it reads"
    )]
    async fn run(&self, cancel: crate::tasks::Cancellation) {
        let mut active = FuturesUnordered::new();
        loop {
            {
                let mut state = self.state.lock().unwrap();
                while active.len() < MAX_ACTIVE {
                    let Some(job) = state.next_job() else {
                        break;
                    };
                    state.active += 1;
                    active.push(
                        async move {
                            let result = tokio::time::timeout_at(job.deadline, job.action.run())
                                .await
                                .unwrap_or(Err(WorkError::TimedOut));
                            (job.key, job.result, result)
                        }
                        .boxed(),
                    );
                }
            }
            tokio::select! {
                biased;
                _ = cancel.cancelled() => return,
                Some((key, completion, result)) = active.next(), if !active.is_empty() => {
                    let mut state = self.state.lock().unwrap();
                    state.active -= 1;
                    state.admitted.remove(&key);
                    let _ = completion.send(Some(result));
                }
                _ = self.wake.notified() => {}
            }
        }
    }

    #[cfg(test)]
    pub(crate) fn counts(&self) -> (usize, usize, usize, u64) {
        let state = self.state.lock().unwrap();
        (
            state.active,
            state.pending.len(),
            state.admitted.len(),
            state.rejected,
        )
    }
    #[cfg(test)]
    pub(crate) fn pending_ttl(&self) -> usize {
        self.state
            .lock()
            .unwrap()
            .admitted
            .keys()
            .filter(|key| key.kind == Kind::Ttl)
            .count()
    }
    #[cfg(test)]
    pub(crate) fn test_admit(
        &self,
        key: Key,
        action: Action,
        deadline: tokio::time::Instant,
    ) -> Result<Ticket, WorkError> {
        self.admit(key, action, deadline)
    }
    #[cfg(test)]
    pub(crate) fn test_keys(&self) -> std::collections::HashSet<Key> {
        self.state
            .lock()
            .unwrap()
            .admitted
            .keys()
            .cloned()
            .collect()
    }
}
/// A running seed's slot in the seeds' share (`State::next_job` takes it),
/// given up when the seed's pass ends or is dropped.
struct SeedSlot(Arc<RequestWork>);
impl Drop for SeedSlot {
    fn drop(&mut self) {
        if let Ok(mut state) = self.0.state.lock() {
            state.seeds = state.seeds.saturating_sub(1);
        }
    }
}
struct StopGuard(Arc<RequestWork>);
impl Drop for StopGuard {
    fn drop(&mut self) {
        // The run future has already dropped all active operations. Dropping
        // queued senders also wakes every coalesced observer with Stopped.
        let mut state = self
            .0
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        state.stopped = true;
        state.active = 0;
        state.pending.clear();
        state.admitted.clear();
    }
}

/// Addendum 1 of the forward-wedge ruling (2026-11-19): a stream-count seed
/// has no total deadline and holds at most `SEED_SHARE` of the runner's
/// slots; the three earlier kinds keep their 10 s.
#[cfg(test)]
mod seed_tests {
    use super::{Action, Key, Kind, RequestWork, WorkError};
    use futures_util::FutureExt;
    use std::{sync::Arc, time::Duration};
    use tokio::sync::oneshot;

    fn key(kind: Kind, name: &str) -> Key {
        let project = crate::tenant::ProjectId::new("seed-share").unwrap();
        Key {
            stream: project.stream_ref(name),
            epoch: "first".into(),
            kind,
        }
    }

    fn started() -> (Arc<RequestWork>, crate::tasks::TaskSupervisor) {
        let work = Arc::new(RequestWork::default());
        let tasks = crate::tasks::TaskSupervisor::new();
        work.start(&tasks).unwrap();
        (work, tasks)
    }

    /// A pass that ends once its sender sends.
    fn parked() -> (
        oneshot::Sender<()>,
        futures_util::future::BoxFuture<'static, ()>,
    ) {
        let (send, sent) = oneshot::channel::<()>();
        (send, async move { sent.await.unwrap_or(()) }.boxed())
    }

    /// Each earlier kind's job is cut, and its ticket answered, at 10 s;
    /// a seed that takes a minute runs to its end.
    #[tokio::test(start_paused = true, crate = "crate::test_rt")]
    async fn the_earlier_kinds_keep_10_s_and_a_seed_has_no_total_deadline() {
        let (work, tasks) = started();
        for kind in [Kind::Topology, Kind::Ttl, Kind::QueueDelete] {
            let at = tokio::time::Instant::now();
            let held = Action::Held(Box::pin(std::future::pending()));
            let waited = work.submit(key(kind, "held"), held).unwrap().wait().await;
            assert_eq!(
                (waited, at.elapsed()),
                (Err(WorkError::TimedOut), Duration::from_secs(10)),
                "{kind:?}"
            );
        }
        let minute = tokio::time::sleep(Duration::from_secs(60)).boxed();
        let ticket = work.submit_seed(key(Kind::StreamSeed, "slow"), minute);
        assert_eq!(ticket.unwrap().wait().await, Ok(()), "a seed of a minute");
        tasks.shutdown(Duration::from_secs(1)).await;
    }

    /// Two parked seeds hold two slots and a third waits queued; a TTL job
    /// still runs beside them, and the third seed starts when one ends.
    #[tokio::test(start_paused = true, crate = "crate::test_rt")]
    async fn seeds_hold_at_most_two_slots_and_the_rest_wait_their_turn() {
        let (work, tasks) = started();
        let mut sends = Vec::new();
        for n in 0..3 {
            let (send, pass) = parked();
            sends.push(send);
            drop(
                work.submit_seed(key(Kind::StreamSeed, &format!("s{n}")), pass)
                    .unwrap(),
            );
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
        let ttl = Action::Held(Box::pin(async { Ok(()) }));
        let ttl = work.submit(key(Kind::Ttl, "renew"), ttl).unwrap();
        assert_eq!(ttl.wait().await, Ok(()), "a TTL job beside two seeds");
        let (active, pending, _, _) = work.counts();
        assert_eq!((active, pending), (2, 1), "two seeds run, one waits");
        sends.remove(0).send(()).unwrap();
        tokio::time::sleep(Duration::from_millis(10)).await;
        let (active, pending, _, _) = work.counts();
        assert_eq!((active, pending), (2, 0), "the third seed started");
        drop(sends);
        tasks.shutdown(Duration::from_secs(1)).await;
    }
}
