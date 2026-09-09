//! Synchronous, thread-local allocator diagnostic. Only the explicit region is
//! counted; frames and input setup precede it. Requested layout bytes are not RSS.
use std::{alloc::{GlobalAlloc, Layout}, cell::Cell};
#[derive(Clone, Copy, Default, serde::Serialize)]
pub(crate) struct Stats {
    calls: u64, reallocs: u64, requested_bytes: u64, realloc_moved_bytes: u64,
    explicit_aggregate_copy_bytes: u64, live_bytes: i64, peak_live_bytes: i64,
}
thread_local! {
    static ACTIVE: Cell<bool> = const { Cell::new(false) };
    static STATS: Cell<Stats> = const { Cell::new(Stats { calls: 0, reallocs: 0, requested_bytes: 0, realloc_moved_bytes: 0, explicit_aggregate_copy_bytes: 0, live_bytes: 0, peak_live_bytes: 0 }) };
}
struct Meter;
#[global_allocator]
static ALLOCATOR: Meter = Meter;
fn update(f: impl FnOnce(&mut Stats)) {
    if ACTIVE.try_with(Cell::get).unwrap_or(false) {
        let _ = STATS.try_with(|cell| { let mut stats = cell.get(); f(&mut stats); cell.set(stats); });
    }
}
unsafe impl GlobalAlloc for Meter {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let pointer = unsafe { BACKEND.alloc(layout) };
        if !pointer.is_null() { update(|s| { s.calls += 1; s.requested_bytes += layout.size() as u64; s.live_bytes += layout.size() as i64; s.peak_live_bytes = s.peak_live_bytes.max(s.live_bytes); }); }
        pointer
    }
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let pointer = unsafe { BACKEND.alloc_zeroed(layout) };
        if !pointer.is_null() { update(|s| { s.calls += 1; s.requested_bytes += layout.size() as u64; s.live_bytes += layout.size() as i64; s.peak_live_bytes = s.peak_live_bytes.max(s.live_bytes); }); }
        pointer
    }
    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        update(|s| s.live_bytes -= layout.size() as i64);
        unsafe { BACKEND.dealloc(pointer, layout) }
    }
    unsafe fn realloc(&self, pointer: *mut u8, old: Layout, size: usize) -> *mut u8 {
        let next = unsafe { BACKEND.realloc(pointer, old, size) };
        if !next.is_null() { update(|s| {
            s.calls += 1; s.reallocs += 1; s.requested_bytes += size as u64;
            if next != pointer { s.realloc_moved_bytes += old.size().min(size) as u64; s.peak_live_bytes = s.peak_live_bytes.max(s.live_bytes + size as i64); }
            s.live_bytes += size as i64 - old.size() as i64; s.peak_live_bytes = s.peak_live_bytes.max(s.live_bytes);
        }); }
        next
    }
}
pub(crate) fn aggregate_copy(bytes: usize) { update(|s| s.explicit_aggregate_copy_bytes += bytes as u64); }
pub(crate) fn measure<F: FnOnce() -> T, T>(f: F) -> (T, Stats) {
    ACTIVE.with(|a| assert!(!a.get()));
    STATS.with(|s| s.set(Stats::default()));
    ACTIVE.with(|a| a.set(true));
    let result = f();
    ACTIVE.with(|a| a.set(false));
    (result, STATS.with(Cell::get))
}
// Replaced with the production allocator in the separately labelled build.
const BACKEND: std::alloc::System = std::alloc::System;
