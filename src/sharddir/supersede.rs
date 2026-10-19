//! Supersession on a fleet-off cell (#179): a process that loses a shard
//! engine while a newer boot of its cell is registered never opens a shard
//! again.
//!
//! A deploy or a restart runs two processes of one cell side by side. The new
//! one opens the shard and fences the old one, whose engine closes; before
//! this module the old one reopened after its anti-flap holdoff and fenced
//! the new one, which reopened after its own and fenced it back: 7-9 s gaps
//! and `outcome_unknown` answers on each switch (the d4 field report). The
//! SlateDB writer epoch cannot say which process is newer, because a reopen
//! makes the old process the epoch's newest writer.
//!
//! The boot record decides instead. Each process writes `_cell/boot` (its
//! boot id) once, at boot, into its cell's data store; the boot the record
//! names is the cell's newest. The record is last-writer-wins and only boots
//! write it, so it needs no compare-and-swap and no clock:
//! - the named process never yields, so the two never both yield;
//! - an unnamed process yields at its first lost engine after the named one
//!   registered, so the two never both keep reopening;
//! - a stray older open that lands after the newest one's costs one extra
//!   round: the newest is named and re-takes, the older yields at its next
//!   lost engine.
//!
//! Yielding is the gate's own stop: every resolve that needs an open answers
//! `closing_outcome` (503 `shard_closing`, `Retry-After: 1`) with nothing
//! applied, an open already in flight is closed as it lands, and readiness
//! reports the supersession. The process's other residents keep serving until
//! they are fenced or the process stops. A record that cannot be read, or a
//! boot that could not register, decides nothing: the process behaves as it
//! did before (it reopens after its holdoff).
//!
//! If the newest process crashes after fencing an older one, the older one
//! never re-takes: a fleet-off cell has no liveness channel, and a timed
//! re-take would bring the ping-pong back. The platform restarts the crashed
//! process as a fresh boot, which registers and opens; a platform that keeps
//! the older process instead gets it back through the unready watchdog's exit
//! (`spawn_unready_watchdog`) and a fresh boot.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use object_store::{ObjectStore, ObjectStoreExt};

use super::OpenGate;

/// The cell's boot record, in the runtime's data store (under its path prefix).
const BOOT_RECORD: &str = "_cell/boot";
/// A read or write of the record that takes longer answers nothing. Below the
/// 3 s base holdoff, so a superseded process yields before it could reopen.
const RECORD_BOUND: Duration = Duration::from_secs(2);
/// Registration retries back off from the first to the last of these delays
/// and then give up: a registration that lands long after its boot could name
/// an older process over a newer one.
const REGISTER_RETRIES: [Duration; 4] = [
    Duration::from_secs(1),
    Duration::from_secs(2),
    Duration::from_secs(4),
    Duration::from_secs(8),
];
/// What readiness reports once this process yielded; `/health` and `/readyz`
/// answer it as `shard storage unavailable: superseded by a newer boot`.
pub(crate) const SUPERSEDED: &str = "superseded by a newer boot";

/// What the boot record says about this process.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Verdict {
    /// The record names this boot.
    Newest,
    /// The record names `newest`, another boot of the cell.
    Superseded { newest: String },
    /// The record could not be read in time, is absent or unreadable.
    Unknown,
}

/// The record's document.
#[derive(serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct BootRecord {
    boot_id: String,
    at_ms: i64,
}

/// One directory's supersession: whether it yielded, and the wake its checker
/// waits on. A directory owns exactly one (`ShardDirectory::new`).
pub(crate) struct Supersession {
    gate: OpenGate,
    superseded: AtomicBool,
    wake: tokio::sync::Notify,
}

impl Supersession {
    pub(crate) fn new(gate: OpenGate) -> Self {
        Supersession {
            gate,
            superseded: AtomicBool::new(false),
            wake: tokio::sync::Notify::new(),
        }
    }

    /// An engine of this directory died (a fence or a fatal fault): the
    /// checker reads the record once. A wake while it reads is kept.
    pub(crate) fn engine_died(&self) {
        self.wake.notify_one();
    }

    /// The readiness reason once this process yielded.
    pub(crate) fn unready_reason(&self) -> Option<String> {
        self.superseded
            .load(Ordering::SeqCst)
            .then(|| SUPERSEDED.to_string())
    }

    /// A newer boot owns the cell: open nothing again. The flag is set first,
    /// so readiness never reports ready for a gate that already refuses.
    pub(crate) fn yield_to_newer(&self) {
        self.superseded.store(true, Ordering::SeqCst);
        self.gate.stop();
    }

    /// Tests only: whether an engine death woke the checker within `within`
    /// (consuming that wake).
    #[cfg(test)]
    pub(crate) async fn woken_within(&self, within: Duration) -> bool {
        tokio::time::timeout(within, self.wake.notified())
            .await
            .is_ok()
    }
}

/// The verdict of one bounded read of the record (`None`: it timed out).
pub(crate) fn verdict(
    own: &str,
    read: Option<Result<bytes::Bytes, object_store::Error>>,
) -> Verdict {
    let Some(Ok(body)) = read else {
        return Verdict::Unknown;
    };
    match serde_json::from_slice::<BootRecord>(&body) {
        Ok(record) if record.boot_id == own => Verdict::Newest,
        Ok(record) => Verdict::Superseded {
            newest: record.boot_id,
        },
        Err(_) => Verdict::Unknown,
    }
}

/// `work` if it finishes within the record bound.
async fn bounded<T>(work: impl std::future::Future<Output = T>) -> Option<T> {
    tokio::time::timeout(RECORD_BOUND, work).await.ok()
}

fn record_path() -> object_store::path::Path {
    object_store::path::Path::from(BOOT_RECORD)
}

/// Fleet off (`fleet::start_configured`): register this boot as its cell's
/// newest and start the checker that yields when a newer boot registered.
/// Answers `false`: no fleet loop started.
pub(crate) fn start(
    directory: &crate::shard_directory::ShardDirectory,
    store: &Arc<dyn ObjectStore>,
    runtime: &crate::runtime::RuntimeCaps,
    tasks: &crate::tasks::TaskSupervisor,
) -> bool {
    let checker = Checker {
        supersession: directory.supersession(),
        store: store.clone(),
        own: runtime.identity.boot_id.clone(),
        at_ms: runtime.clock.now().ms(),
    };
    let spawned = tasks.spawn(
        "supersession",
        crate::tasks::Policy::Critical,
        move |cancel| checker.run(cancel),
    );
    if let Err(refused) = spawned {
        tracing::warn!("supersession checker not started ({refused:?}): the runtime is stopping");
    }
    false
}

/// The checker: registers once, then reads the record at each engine death.
struct Checker {
    supersession: Arc<Supersession>,
    store: Arc<dyn ObjectStore>,
    own: String,
    at_ms: i64,
}

impl Checker {
    async fn run(self, cancel: crate::tasks::Cancellation) -> crate::tasks::TaskResult {
        if self.register(&cancel).await {
            self.watch(&cancel).await;
        }
        cancel.cancelled().await;
        crate::tasks::TaskResult::Done
    }

    /// Writes the record naming this boot, retrying a failed write a few
    /// times. Whether it registered; a boot that did not never yields.
    async fn register(&self, cancel: &crate::tasks::Cancellation) -> bool {
        let record = BootRecord {
            boot_id: self.own.clone(),
            at_ms: self.at_ms,
        };
        let Ok(body) = serde_json::to_vec(&record) else {
            return false;
        };
        let body = bytes::Bytes::from(body);
        for retry in REGISTER_RETRIES.into_iter().map(Some).chain([None]) {
            match bounded(self.store.put(&record_path(), body.clone().into())).await {
                Some(Ok(_)) => {
                    tracing::info!(boot = %self.own, "registered as this cell's newest boot");
                    return true;
                }
                Some(Err(error)) => tracing::warn!("boot record not written: {error}"),
                None => tracing::warn!("boot record not written within {RECORD_BOUND:?}"),
            }
            let Some(delay) = retry else { break };
            if cancelled_within(cancel, delay).await {
                return false;
            }
        }
        tracing::error!(
            boot = %self.own,
            "boot record never written: this process does not yield to a newer boot"
        );
        false
    }

    /// Reads the record at each engine death until it names another boot,
    /// then yields; returns at the yield or the stop.
    async fn watch(&self, cancel: &crate::tasks::Cancellation) {
        loop {
            tokio::select! {
                () = cancel.cancelled() => return,
                () = self.supersession.wake.notified() => {}
            }
            match self.check().await {
                Verdict::Superseded { newest } => {
                    tracing::warn!(
                        boot = %self.own,
                        %newest,
                        "a newer boot of this cell is registered: this process opens no shard again"
                    );
                    self.supersession.yield_to_newer();
                    return;
                }
                Verdict::Newest => {}
                Verdict::Unknown => {
                    tracing::warn!("boot record unread: an engine death decides nothing");
                }
            }
        }
    }

    async fn check(&self) -> Verdict {
        let read = bounded(async {
            let found = self.store.get(&record_path()).await?;
            found.bytes().await
        });
        verdict(&self.own, read.await)
    }
}

/// Whether the stop came within `delay`.
async fn cancelled_within(cancel: &crate::tasks::Cancellation, delay: Duration) -> bool {
    tokio::time::timeout(delay, cancel.cancelled())
        .await
        .is_ok()
}

#[cfg(test)]
mod tests;
