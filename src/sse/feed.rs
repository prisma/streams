//! LiveFeed — the one per-stream subscription engine (LIVE-FEED).
//! Replaces the direct reader and the LiveHub pump with a single
//! implementation whose only variable is retention and WHO reads:
//!
//! * SOLO (one subscriber): no background task. The session parks on
//!   the source's durable advance notify and drives its own reads —
//!   zero retained state, zero extra tasks for thousands of singleton
//!   feeds.
//! * SHARED (two or more): a single driver task owns reading; it
//!   formats each payload frame once into a bounded shared ring and
//!   every session consumes from it, waking ONCE per window on the
//!   version bump (scheduling parity with the legacy hub pump).
//!
//! Wakeups ride a `watch` generation plus the source's durable advance
//! notify — nothing can be lost between a session's state check and
//! its park.

use bytes::Bytes;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

/// Identity of a feed: stream incarnation + selector lane.
#[derive(Clone, PartialEq, Eq, Hash)]
pub(crate) struct FeedKey {
    pub(crate) identity: [u8; 16],
    pub(crate) selector: [u8; 16],
}

impl FeedKey {
    pub(crate) fn default_lane(identity: [u8; 16]) -> Self {
        Self {
            identity,
            selector: crate::crypto::stream_hash(""),
        }
    }
    pub(crate) fn keyed(identity: [u8; 16], rk: &str) -> Self {
        Self {
            identity,
            selector: crate::crypto::stream_hash(rk),
        }
    }
}

/// One prepared record. `data_event` is the SHAREABLE wire frame
/// (data + lane cursor control folded); sessions send it as-is.
pub(crate) struct PreparedRecord {
    pub(crate) offset: u64,
    pub(crate) data_event: Bytes,
    pub(crate) payload_len: u32,
    /// This frame carried THE terminal sealed control.
    pub(crate) sealed: bool,
}

pub(crate) struct PreparedBatch {
    pub(crate) scan_from: u64,
    pub(crate) scan_to: u64,
    pub(crate) records: Arc<[PreparedRecord]>,
    pub(crate) charge: usize,
}

#[async_trait::async_trait]
pub(crate) trait FeedSourceRead: Send + Sync {
    async fn read(
        &self,
        from: u64,
        max_bytes: usize,
    ) -> anyhow::Result<(Vec<crate::http::PlainRec>, bool)>;
    fn frontier(&self) -> u64;
    fn closed(&self) -> bool;
    fn desc(&self) -> &crate::registry::StreamDesc;
    /// Compose the SHAREABLE frame for one record: data event plus this
    /// lane's cursor control folded (identical for every subscriber of
    /// the feed). upToDate/sealed are lane-global facts supplied by the
    /// feed at publish time; per-session statuses stay separate.
    fn frame(&self, rec: &crate::http::PlainRec, up_to_date: bool, sealed: bool) -> Bytes;
    fn ctl_next(&self, rec: &crate::http::PlainRec) -> u64;
    fn advance_notify(&self) -> &tokio::sync::Notify;
}

fn charge_for(events: &[PreparedRecord]) -> usize {
    let ev: usize = events.iter().map(|r| r.data_event.len()).sum();
    ev + events.len() * 64 + 256
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Lifecycle {
    Active,
    Closed,
}

struct FeedState {
    head: u64,
    floor: u64,
    version: u64,
    batches: VecDeque<Arc<PreparedBatch>>,
    charge: usize,
    lifecycle: Lifecycle,
}

pub(crate) struct LiveFeed {
    key: FeedKey,
    src: std::sync::RwLock<Arc<dyn FeedSourceRead>>,
    st: Mutex<FeedState>,
    changed: tokio::sync::watch::Sender<u64>,
    driving: AtomicBool,
    subscribers: AtomicU64,
    retained_charge: AtomicUsize,
    source_reads: AtomicU64,
    ring_budget: usize,
    driver_abort: Mutex<Option<tokio::task::AbortHandle>>,
}

const MAX_DRIVER_BATCH_BYTES: usize = 256 * 1024;

impl LiveFeed {
    pub(crate) fn new(key: FeedKey, src: Arc<dyn FeedSourceRead>, ring_budget: usize) -> Arc<Self> {
        let head = src.frontier();
        let (changed, _) = tokio::sync::watch::channel(0u64);
        Arc::new(Self {
            key,
            src: std::sync::RwLock::new(src),
            st: Mutex::new(FeedState {
                head,
                floor: head,
                version: 0,
                batches: VecDeque::new(),
                charge: 0,
                lifecycle: Lifecycle::Active,
            }),
            changed,
            driving: AtomicBool::new(false),
            subscribers: AtomicU64::new(0),
            retained_charge: AtomicUsize::new(0),
            source_reads: AtomicU64::new(0),
            ring_budget,
            driver_abort: Mutex::new(None),
        })
    }

    fn current_source(&self) -> Arc<dyn FeedSourceRead> {
        let guard = self.src.read().unwrap();
        guard.clone()
    }

    pub(crate) fn subscriber_count(&self) -> u64 {
        self.subscribers.load(Ordering::SeqCst)
    }

    #[cfg(test)]
    pub(crate) fn source_read_count(&self) -> u64 {
        self.source_reads.load(Ordering::Relaxed)
    }

    #[cfg(test)]
    pub(crate) fn retained(&self) -> usize {
        self.retained_charge.load(Ordering::Relaxed)
    }

    /// Attach. Spawns the SHARED driver when the crowd reaches two;
    /// solo attachments never spawn anything.
    pub(crate) fn join(self: &Arc<Self>) -> (u64, tokio::sync::watch::Receiver<u64>) {
        let n = self.subscribers.fetch_add(1, Ordering::SeqCst) + 1;
        if n == 2 {
            self.spawn_shared_driver();
        }
        let rx = self.changed.subscribe();
        let head = self.st.lock().unwrap().head;
        (head, rx)
    }

    pub(crate) fn leave(&self) {
        let prev = self.subscribers.fetch_sub(1, Ordering::SeqCst);
        // Crowd dropped below two: the shared driver is no longer
        // needed (a remaining solo session self-drives off the source
        // notify directly).
        if prev <= 2 {
            if let Some(h) = self.driver_abort.lock().unwrap().take() {
                h.abort();
            }
        }
    }

    /// SHARED-mode driver: the only reader while fanned out. Woken by
    /// the current source's durable advance; aborted when the crowd
    /// drops back below two.
    fn spawn_shared_driver(self: &Arc<Self>) {
        let this = Arc::clone(self);
        let handle = tokio::spawn(async move {
            loop {
                if this.subscriber_count() < 2 {
                    break;
                }
                // Drive whatever is durable right now (no-op when the
                // previous pass already drained to the frontier).
                let _ = this.drive_once().await;
                let src = this.current_source();
                let n = src.advance_notify().notified();
                let repoll = async {
                    loop {
                        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
                    }
                };
                tokio::select! {
                    _ = n => {}
                    _ = repoll => {}
                }
            }
        });
        *self.driver_abort.lock().unwrap() = Some(handle.abort_handle());
    }

    /// Owned future parking on the CURRENT source's durable advance —
    /// the solo session's wake (no driver task exists in solo mode).
    pub(crate) fn park_advance(
        &self,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>> {
        let src = self.current_source();
        Box::pin(async move {
            let n = src.advance_notify().notified();
            n.await;
        })
    }

    /// Consume retained records at/after `cursor`. Lagged = below floor
    /// → disconnect-and-resume per the lag contract.
    pub(crate) fn take_visible(&self, cursor: u64) -> Take {
        let st = self.st.lock().unwrap();
        if cursor < st.floor {
            return Take::Lagged { floor: st.floor };
        }
        let mut out = Vec::new();
        let mut next = cursor;
        for b in &st.batches {
            if b.scan_to <= cursor {
                continue;
            }
            for r in b.records.iter().filter(|r| r.offset >= cursor) {
                out.push((r.offset, r.data_event.clone(), r.payload_len, r.sealed));
                next = next.max(r.offset + 1);
            }
        }
        if next > cursor {
            Take::Records { records: out, next }
        } else {
            Take::AtHead
        }
    }

    pub(crate) async fn drive_once(&self) -> Option<DriveOutcome> {
        if self
            .driving
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            return None;
        }
        let out = self.drive_under_permit().await;
        // Release BEFORE any socket write by any consumer of the result.
        self.driving.store(false, Ordering::SeqCst);
        Some(out)
    }

    #[cfg(test)]
    pub(crate) fn hold_permit_for_test(&self) -> PermitGuard<'_> {
        self.driving.store(true, Ordering::SeqCst);
        PermitGuard(self)
    }

    async fn drive_under_permit(&self) -> DriveOutcome {
        let src = self.current_source();
        let head = self.st.lock().unwrap().head;
        if head >= src.frontier() {
            if src.closed() && st_lifecycle_active(&self.st) {
                let mut st = self.st.lock().unwrap();
                if st.lifecycle == Lifecycle::Active {
                    st.lifecycle = Lifecycle::Closed;
                    st.version += 1;
                    let _ = self.changed.send(st.version);
                }
                return DriveOutcome::Closed;
            }
            return DriveOutcome::Idle;
        }
        self.source_reads.fetch_add(1, Ordering::Relaxed);
        let outcome = self.read_and_publish(&src, head).await;
        let ver = {
            let mut st = self.st.lock().unwrap();
            st.version += 1;
            st.version
        };
        let _ = self.changed.send(ver);
        outcome
    }

    async fn read_and_publish(&self, src: &Arc<dyn FeedSourceRead>, head: u64) -> DriveOutcome {
        let (recs, _) = match src.read(head, MAX_DRIVER_BATCH_BYTES).await {
            Ok(x) => x,
            Err(_) => return DriveOutcome::SourceFailed,
        };
        let scan_from = head;
        // Lane-global flag facts captured ONCE per batch: a scan whose
        // LAST record reaches the durable end of an OPEN stream marks
        // its final frame upToDate; a CLOSED stream marks it sealed.
        let end_after = src.frontier();
        let closed_now = src.closed();
        let n = recs.len();
        let mut prepared: Vec<PreparedRecord> = Vec::with_capacity(n);
        let mut last = scan_from;
        for (i, r) in recs.iter().enumerate() {
            last = last.max(r.off + 1);
            let flagged = i + 1 == n && r.off + 1 >= end_after;
            let sealed_i = flagged && closed_now;
            prepared.push(PreparedRecord {
                offset: r.off,
                data_event: src.frame(r, flagged, sealed_i),
                payload_len: r.payload.len() as u32,
                sealed: sealed_i,
            });
        }
        let solo = self.subscribers.load(Ordering::Relaxed) <= 1;
        let mut st = self.st.lock().unwrap();
        st.head = st.head.max(last);
        if solo {
            st.floor = st.head;
            return DriveOutcome::Solo(prepared);
        }
        let batch_charge = charge_for(&prepared);
        st.charge += batch_charge;
        st.batches.push_back(Arc::new(PreparedBatch {
            scan_from,
            scan_to: last,
            charge: batch_charge,
            records: prepared.into(),
        }));
        while st.charge > self.ring_budget {
            match st.batches.pop_front() {
                Some(b) => {
                    st.charge -= b.charge;
                    st.floor = st.floor.max(b.scan_to);
                }
                None => break,
            }
        }
        self.retained_charge.store(st.charge, Ordering::Relaxed);
        DriveOutcome::Published
    }
}

fn st_lifecycle_active(st: &Mutex<FeedState>) -> bool {
    st.lock().unwrap().lifecycle == Lifecycle::Active
}

#[cfg(test)]
pub(crate) struct PermitGuard<'a>(&'a LiveFeed);
#[cfg(test)]
impl Drop for PermitGuard<'_> {
    fn drop(&mut self) {
        self.0.driving.store(false, Ordering::SeqCst);
    }
}

#[derive(Debug)]
pub(crate) enum Take {
    Records {
        records: Vec<(u64, Bytes, u32, bool)>,
        next: u64,
    },
    AtHead,
    Lagged {
        floor: u64,
    },
}

pub(crate) enum DriveOutcome {
    /// Zero retention: these records belong to the driving session.
    Solo(Vec<PreparedRecord>),
    Published,
    Idle,
    Closed,
    SourceFailed,
}
