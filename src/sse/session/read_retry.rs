//! The live phase's retry of a session's own failed read.
//!
//! A live drive whose read failed, or returned an empty partial page,
//! changed nothing a parked session waits on: the feed version is not
//! bumped (finding 6) and the source's advance notification already
//! fired for the records it could not read. Since round 11.1 no producer
//! heartbeat re-drives such a park, so without this bound the records
//! waited for the next append - and a sealed tail has none. Only a
//! session whose OWN drive failed owes itself this timer; a contended
//! session parks on the version watch alone. Under a persistent fault
//! every session that wins the drive permit arms its own timer, and the
//! permit serialises their reads, so a failing source costs at most one
//! read per such session per `CAP`.
use std::time::Duration;

pub(super) struct ReadRetry {
    /// The cursor whose read failed last and the wait that followed it:
    /// a failure at the same cursor means no progress since, so the wait
    /// doubles; a failure at a later cursor starts over.
    last: Option<(u64, Duration)>,
    /// A failed read owes the NEXT park one bounded wait. That park
    /// consumes it: any other wake re-drives (and re-arms on failure) or
    /// finds the session at the head, where no read is owed.
    owed: bool,
}

impl ReadRetry {
    /// No failed read owed: the session parks on its wakes alone.
    pub(super) const IDLE: Self = Self {
        last: None,
        owed: false,
    };
    const FIRST: Duration = Duration::from_millis(250);
    /// One read per failing session per 5 s bounds what a persistently
    /// failing store or peer costs.
    const CAP: Duration = Duration::from_secs(5);

    /// This session's own drive changed nothing, so no wake is owed to
    /// it: the next park must end on this session's own timer.
    pub(super) fn failed(&mut self) {
        self.owed = true;
    }

    /// The park's timer at `cursor`: the lease nap, shortened by the
    /// retry a failed read owes this park; a retry never postpones the
    /// lease deadline.
    pub(super) fn nap(&mut self, cursor: u64, lease_nap: Duration) -> Duration {
        if !std::mem::take(&mut self.owed) {
            return lease_nap;
        }
        let delay = match self.last {
            Some((at, waited)) if at == cursor => waited.saturating_mul(2).min(Self::CAP),
            Some(_) | None => Self::FIRST,
        };
        self.last = Some((cursor, delay));
        tracing::debug!(
            cursor,
            ?delay,
            "livefeed live read failed or made no progress; re-driving after a bounded park"
        );
        delay.min(lease_nap)
    }
}
