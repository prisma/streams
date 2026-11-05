//! The lib test binary's allocator: the system allocator, counting the
//! bytes each thread holds and their peak, so a test can measure the most a
//! synchronous step holds at once (tuning item 3c's feed parse). Only the
//! test build has it; the binary keeps mimalloc (`src/main.rs`).

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;

/// `System`, counting this thread's live bytes.
struct Counting;

thread_local! {
    /// Bytes this thread allocated less those it freed (a block freed on
    /// another thread counts there).
    static LIVE: Cell<isize> = const { Cell::new(0) };
    /// The most `LIVE` reached since `peak_bytes` last reset it.
    static PEAK: Cell<isize> = const { Cell::new(0) };
}

/// Moves this thread's live count by `delta`, raising its peak.
fn count(delta: isize) {
    // A thread past its locals' teardown counts nothing.
    let Ok(now) = LIVE.try_with(|live| {
        let now = live.get() + delta;
        live.set(now);
        now
    }) else {
        return;
    };
    PEAK.with(|peak| peak.set(peak.get().max(now)));
}

/// The bytes a layout asks for, as a count.
fn bytes(layout: Layout) -> isize {
    isize::try_from(layout.size()).unwrap_or(isize::MAX)
}

// SAFETY: every method forwards its arguments unchanged to `System`, which
// upholds `GlobalAlloc`'s contract; the counting touches only thread-local
// integers, which neither allocate nor have destructors.
unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        // SAFETY: the caller's layout, as `GlobalAlloc::alloc` requires.
        let block = unsafe { System.alloc(layout) };
        if !block.is_null() {
            count(bytes(layout));
        }
        block
    }

    unsafe fn dealloc(&self, block: *mut u8, layout: Layout) {
        // SAFETY: a block this allocator returned, with its layout.
        unsafe { System.dealloc(block, layout) };
        count(-bytes(layout));
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        // SAFETY: the caller's layout, as `GlobalAlloc::alloc_zeroed` requires.
        let block = unsafe { System.alloc_zeroed(layout) };
        if !block.is_null() {
            count(bytes(layout));
        }
        block
    }

    unsafe fn realloc(&self, block: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        // SAFETY: a block this allocator returned, its layout and a size
        // the caller vouches for, as `GlobalAlloc::realloc` requires.
        let moved = unsafe { System.realloc(block, layout, new_size) };
        if !moved.is_null() {
            count(isize::try_from(new_size).unwrap_or(isize::MAX) - bytes(layout));
        }
        moved
    }
}

#[global_allocator]
static ALLOCATOR: Counting = Counting;

/// The bytes this thread holds (see `LIVE`).
pub(crate) fn live_bytes() -> isize {
    LIVE.with(Cell::get)
}

/// `step`'s value, and the most bytes this thread held at once while it
/// ran beyond what it held when it began (what it returns included).
pub(crate) fn peak_bytes<T>(step: impl FnOnce() -> T) -> (T, usize) {
    let start = LIVE.with(Cell::get);
    PEAK.with(|peak| peak.set(start));
    let value = step();
    let peak = PEAK.with(Cell::get) - start;
    (value, usize::try_from(peak).unwrap_or(0))
}

#[cfg(test)]
mod tests {
    use super::peak_bytes;

    /// A step that holds 1 MiB and then 2 MiB at once peaks at 3 MiB, and
    /// one that frees all it allocated still reports its peak.
    #[test]
    fn a_step_reports_the_most_it_held_at_once() {
        let (kept, peak) = peak_bytes(|| {
            let first = vec![1u8; 1 << 20];
            let second = vec![2u8; 2 << 20];
            std::hint::black_box((&first, &second));
            first.len() + second.len()
        });
        assert_eq!(kept, 3 << 20);
        assert!((3 << 20..(3 << 20) + 4096).contains(&peak), "{peak}");
    }
}
