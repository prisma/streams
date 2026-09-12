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

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum Kind {
    Topology,
    Ttl,
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
    rejected: u64,
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
                    let Some(job) = state.pending.pop_front() else {
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
