//! Local review measurement instrumentation; never part of the shipped crate.
use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering::Relaxed};
static ACTIVE: AtomicBool = AtomicBool::new(false);
static CALLS: AtomicU64 = AtomicU64::new(0);
static BYTES: AtomicU64 = AtomicU64::new(0);
static PAYLOAD_ALLOCS: AtomicU64 = AtomicU64::new(0);
static PAYLOAD_BYTES: AtomicUsize = AtomicUsize::new(0);
static GETS: AtomicU64 = AtomicU64::new(0);
static GET_ATTEMPTS: AtomicU64 = AtomicU64::new(0);
static GET_BYTES: AtomicU64 = AtomicU64::new(0);
static BRIDGE_POINTER: AtomicUsize = AtomicUsize::new(0);
static BRIDGES: AtomicU64 = AtomicU64::new(0);
static BRIDGE_COPIES: AtomicU64 = AtomicU64::new(0);
static BRIDGE_COPY_BYTES: AtomicU64 = AtomicU64::new(0);
struct CountedSystem;
#[global_allocator]
static ALLOCATOR: CountedSystem = CountedSystem;
fn allocation(size: usize) {
    if ACTIVE.load(Relaxed) {
        CALLS.fetch_add(1, Relaxed);
        BYTES.fetch_add(size as u64, Relaxed);
        let payload = PAYLOAD_BYTES.load(Relaxed);
        if payload != 0 && size >= payload && size <= payload * 2 + 512 {
            PAYLOAD_ALLOCS.fetch_add(1, Relaxed);
        }
    }
}
unsafe impl GlobalAlloc for CountedSystem {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        allocation(layout.size());
        unsafe { System.alloc(layout) }
    }
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        allocation(layout.size());
        unsafe { System.alloc_zeroed(layout) }
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        allocation(size);
        unsafe { System.realloc(ptr, layout, size) }
    }
}
pub(crate) fn get_attempt() {
    if ACTIVE.load(Relaxed) { GET_ATTEMPTS.fetch_add(1, Relaxed); }
}
pub(crate) fn get(bytes: u64) {
    if ACTIVE.load(Relaxed) {
        GETS.fetch_add(1, Relaxed);
        GET_BYTES.fetch_add(bytes, Relaxed);
    }
}
// The workload has one in-flight append. Only its exact payload size is
// eligible; background watch/maintenance appends cannot claim its probe.
pub(crate) fn bridge_start(pointer: usize, len: usize) {
    if ACTIVE.load(Relaxed) && len == PAYLOAD_BYTES.load(Relaxed) {
        assert_eq!(BRIDGE_POINTER.swap(pointer, Relaxed), 0);
    }
}
pub(crate) fn bridge_end(pointer: usize, len: usize) {
    if ACTIVE.load(Relaxed) && len == PAYLOAD_BYTES.load(Relaxed) {
        let before = BRIDGE_POINTER.swap(0, Relaxed);
        if before != 0 {
            BRIDGES.fetch_add(1, Relaxed);
            if pointer != before {
                BRIDGE_COPIES.fetch_add(1, Relaxed);
                BRIDGE_COPY_BYTES.fetch_add(len as u64, Relaxed);
            }
        }
    }
}
pub(crate) fn begin(payload_bytes: usize) {
    assert!(!ACTIVE.swap(false, Relaxed));
    for counter in [&CALLS, &BYTES, &PAYLOAD_ALLOCS, &GETS, &GET_BYTES, &GET_ATTEMPTS, &BRIDGES, &BRIDGE_COPIES, &BRIDGE_COPY_BYTES] {
        counter.store(0, Relaxed);
    }
    PAYLOAD_BYTES.store(payload_bytes, Relaxed);
    ACTIVE.store(true, Relaxed);
}
pub(crate) fn finish() -> serde_json::Value {
    ACTIVE.store(false, Relaxed);
    serde_json::json!({
        "allocation_calls": CALLS.load(Relaxed),
        "allocated_bytes": BYTES.load(Relaxed),
        "payload_capacity_allocations": PAYLOAD_ALLOCS.load(Relaxed),
        "object_get_attempts": GET_ATTEMPTS.load(Relaxed),
        "successful_object_gets": GETS.load(Relaxed),
        "object_get_range_bytes": GET_BYTES.load(Relaxed),
        "product_payload_bridges": BRIDGES.load(Relaxed),
        "product_bridge_copies": BRIDGE_COPIES.load(Relaxed),
        "product_bridge_copy_bytes": BRIDGE_COPY_BYTES.load(Relaxed),
    })
}
