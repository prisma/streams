//! LiveFeed — the one per-stream subscription engine (LIVE-FEED).
//! Replaces the direct reader and the LiveHub pump with a single
//! implementation whose only variable is retention:
//!
//! * NO pump task. One session at a time holds the driver permit
//!   (`drive_once`), reads one bounded source batch, formats each
//!   payload event once, publishes to the shared ring (or hands the
//!   batch straight back in solo mode), and releases the permit
//!   BEFORE any socket write.
//! * Retention: zero while a single subscriber is attached; a bounded
//!   shared ring once two or more are.
//! * Wakeups ride a `watch` generation plus the source's durable
//!   advance notify — an append committing between a session's check
//!   and its park can never be missed.
//!
//! The feed tracks the LIVE TAIL of its lane; a subscriber connecting
//! behind it performs private durable catch-up (lag contract,
//! docs/LIVE-FEED.md).

use bytes::Bytes;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

/// Identity of a feed: the stream INCARNATION plus the selector lane.
/// Delete/recreate mints a new epoch and therefore a new feed. Raw and
/// product subscribers share the default-key data lane (`""`); a keyed
/// product subscriber forms its own lane.
#[derive(Clone, PartialEq, Eq, Hash)]
pub(crate) struct FeedKey {
    pub(crate) identity: [u8; 16],
    pub(crate) selector: [u8; 16],
}

impl FeedKey {
    /// The default-key lane — shared by raw SSE and unfiltered product
    /// SSE (the raw singular route IS the default-key stream).
    pub(crate) fn default_lane(identity: [u8; 16]) -> Self {
        Self {
            identity,
            selector: crate::crypto::stream_hash(""),
        }
    }
    /// A keyed product lane for one routing key.
    pub(crate) fn keyed(identity: [u8; 16], rk: &str) -> Self {
        Self {
            identity,
            selector: crate::crypto::stream_hash(rk),
        }
    }
}

/// One prepared record: the DATA event formatted ONCE per lane.
/// Sessions compose their own surface control around it (one chunk on
/// the wire after a small local concat).
pub(crate) struct PreparedRecord {
    pub(crate) offset: u64,
    pub(crate) data_event: Bytes,
    pub(crate) payload_len: u32,
}

pub(crate) struct PreparedBatch {
    pub(crate) scan_from: u64,
    pub(crate) scan_to: u64,
    pub(crate) records: Arc<[PreparedRecord]>,
    pub(crate) charge: usize,
}

/// Where reads come from. Implementations cover single-segment
/// (default + keyed lanes), forks (stitched), and lineages (segment
/// traversal with refresh).
#[async_trait::async_trait]
pub(crate) trait FeedSourceRead: Send + Sync {
    /// Read up to `max_bytes` from the CURRENT segment position.
    /// Returns records in order plus whether THIS call reached the end
    /// of everything this source currently serves.
    async fn read(
        &self,
        from: u64,
        max_bytes: usize,
    ) -> anyhow::Result<(Vec<crate::http::PlainRec>, bool)>;
    /// Durable frontier (next offset) of the current position.
    fn frontier(&self) -> u64;
    /// Whether the served sequence is CLOSED for appends right now.
    fn closed(&self) -> bool;
    fn desc(&self) -> &crate::registry::StreamDesc;
    fn prepare_data(&self, rec: &crate::http::PlainRec) -> Bytes;
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
    src: Arc<dyn FeedSourceRead>,
    st: Mutex<FeedState>,
    changed: tokio::sync::watch::Sender<u64>,
    driving: AtomicBool,
    subscribers: AtomicU64,
    retained_charge: AtomicUsize,
    source_reads: AtomicU64,
    ring_budget: usize,
}

const MAX_DRIVER_BATCH_BYTES: usize = 256 * 1024;

impl LiveFeed {
    pub(crate) fn new(key: FeedKey, src: Arc<dyn FeedSourceRead>, ring_budget: usize) -> Arc<Self> {
        let head = src.frontier();
        let (changed, _) = tokio::sync::watch::channel(0u64);
        Arc::new(Self {
            key,
            src,
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
        })
    }

    pub(crate) fn join(&self) -> (u64, tokio::sync::watch::Receiver<u64>) {
        self.subscribers.fetch_add(1, Ordering::SeqCst);
        let rx = self.changed.subscribe();
        let head = self.st.lock().unwrap().head;
        (head, rx)
    }

    pub(crate) fn leave(&self) {
        self.subscribers.fetch_sub(1, Ordering::SeqCst);
    }

    pub(crate) fn subscriber_count(&self) -> u64 {
        self.subscribers.load(Ordering::SeqCst)
    }

    pub(crate) fn source_read_count(&self) -> u64 {
        self.source_reads.load(Ordering::Relaxed)
    }

    pub(crate) fn retained(&self) -> usize {
        self.retained_charge.load(Ordering::Relaxed)
    }

    /// Consume retained records at/after `cursor`. `Lagged` = below
    /// floor → disconnect-and-resume per the lag contract.
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
                out.push((r.offset, r.data_event.clone(), r.payload_len));
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

    #[cfg(test)]
    pub(crate) fn permit_held(&self) -> bool {
        self.driving.load(Ordering::SeqCst)
    }

    async fn drive_under_permit(&self) -> DriveOutcome {
        let head = self.st.lock().unwrap().head;
        if head >= self.src.frontier() {
            if self.src.closed() {
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
        let outcome = self.read_and_publish(head).await;
        let ver = {
            let mut st = self.st.lock().unwrap();
            st.version += 1;
            st.version
        };
        let _ = self.changed.send(ver);
        outcome
    }

    async fn read_and_publish(&self, head: u64) -> DriveOutcome {
        let (recs, _) = match self.src.read(head, MAX_DRIVER_BATCH_BYTES).await {
            Ok(x) => x,
            Err(_) => return DriveOutcome::SourceFailed,
        };
        let scan_from = head;
        let mut prepared: Vec<PreparedRecord> = Vec::with_capacity(recs.len());
        let mut last = scan_from;
        for r in &recs {
            last = last.max(r.off + 1);
            prepared.push(PreparedRecord {
                offset: r.off,
                data_event: self.src.prepare_data(r),
                payload_len: r.payload.len() as u32,
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
        records: Vec<(u64, Bytes, u32)>,
        next: u64,
    },
    AtHead,
    Lagged {
        floor: u64,
    },
}

pub(crate) enum DriveOutcome {
    /// Zero retention: the batch belongs to the driving session.
    Solo(Vec<PreparedRecord>),
    Published,
    Idle,
    Closed,
    SourceFailed,
}

impl std::fmt::Debug for DriveOutcome {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Solo(_) => "Solo(..)",
            Self::Published => "Published",
            Self::Idle => "Idle",
            Self::Closed => "Closed",
            Self::SourceFailed => "SourceFailed",
        })
    }
}
