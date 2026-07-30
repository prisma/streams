//! Interval metrics as an internal stream (`__metrics__`), following the old
//! implementation's pattern: the node appends one JSON record per interval
//! with per-stream counters — which is also the multi-tenant billing feed
//! (stream = tenant boundary; appends/bytes in, reads/bytes out).

use std::collections::HashMap;
use std::sync::Mutex;

#[derive(Default, Clone, serde::Serialize)]
pub struct PerStream {
    pub appends: u64,
    pub append_bytes: u64,
    pub reads: u64,
    pub read_bytes: u64,
    pub queue_ops: u64,
}

#[derive(Default)]
pub struct Metrics {
    counters: Mutex<HashMap<String, PerStream>>,
    /// Collection is OFF until a flusher exists: without one, drain()
    /// never runs and the per-stream map grows with total cardinality
    /// forever (static-audit memory finding).
    enabled: std::sync::atomic::AtomicBool,
}

impl Metrics {
    pub fn enable(&self) {
        self.enabled.store(true, std::sync::atomic::Ordering::Relaxed);
    }
    fn on(&self) -> bool {
        self.enabled.load(std::sync::atomic::Ordering::Relaxed)
    }
    pub fn len(&self) -> usize {
        self.counters.lock().unwrap().len()
    }
    pub fn append(&self, stream: &str, bytes: u64) {
        if !self.on() {
            return;
        }
        let mut m = self.counters.lock().unwrap();
        let e = m.entry(stream.to_string()).or_default();
        e.appends += 1;
        e.append_bytes += bytes;
    }
    pub fn read(&self, stream: &str, bytes: u64) {
        if !self.on() {
            return;
        }
        let mut m = self.counters.lock().unwrap();
        let e = m.entry(stream.to_string()).or_default();
        e.reads += 1;
        e.read_bytes += bytes;
    }
    pub fn queue(&self, stream: &str) {
        if !self.on() {
            return;
        }
        let mut m = self.counters.lock().unwrap();
        m.entry(stream.to_string()).or_default().queue_ops += 1;
    }
    pub fn drain(&self) -> HashMap<String, PerStream> {
        std::mem::take(&mut *self.counters.lock().unwrap())
    }
}
