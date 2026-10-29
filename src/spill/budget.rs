//! Layout 6's memory budget lines for the index and read structures
//! (REVIEW-6A F15: every new structure has a line under the 500 MB shed
//! line). Each is a process-wide bound:
//!
//! | Structure | Bound | Enforced by |
//! |---|---:|---|
//! | Seal directories (all shards) | `PROCESS_DIRECTORY_BYTES` | a fold of the largest directory once the sum passes it, and of any shard past `DIRECTORY_FOLD_BYTES` |
//! | Index cache (run-index footers, runs) | `cache::INDEX_CACHE_BYTES` | LRU eviction |
//! | Chunk cache (whole sealed chunks) | `cache::CHUNK_CACHE_BYTES` | LRU eviction |
//! | Merges (input objects and output) | `MERGE_BUFFER_BYTES` | one merge at a time per process, inputs within half of it |
//! | Consolidations in flight | `CONSOLIDATION_BYTES` | a process-wide byte semaphore |
//!
//! Their sum, `INDEX_AND_READ_BYTES`, is 120 MiB; with the fixed caches
//! (144 MiB) and the boot check's 100 MiB of headroom it stays under the
//! shed line (`config::tests`).

/// A shard folds once its directory holds this much, whatever the period.
pub(crate) const DIRECTORY_FOLD_BYTES: u64 = 4 << 20;
/// The process's directories together: past it, the largest folds.
pub(crate) const PROCESS_DIRECTORY_BYTES: u64 = 32 << 20;
/// One merge's memory: its input objects (at most half) and its output.
pub(crate) const MERGE_BUFFER_BYTES: u64 = 32 << 20;
/// Pages held by consolidations in flight, process-wide.
pub(crate) const CONSOLIDATION_BYTES: u64 = 8 << 20;

/// Every budget line above, summed.
#[cfg(test)]
pub(crate) const INDEX_AND_READ_BYTES: u64 = PROCESS_DIRECTORY_BYTES
    + super::cache::INDEX_CACHE_BYTES
    + super::cache::CHUNK_CACHE_BYTES
    + MERGE_BUFFER_BYTES
    + CONSOLIDATION_BYTES;

/// One merge at a time in the process.
pub(crate) fn merge_permit() -> &'static tokio::sync::Semaphore {
    static ONE: tokio::sync::Semaphore = tokio::sync::Semaphore::const_new(1);
    &ONE
}

/// Consolidation bytes in flight, in KiB permits.
pub(crate) fn consolidation_permits() -> &'static tokio::sync::Semaphore {
    static KIB: tokio::sync::Semaphore =
        tokio::sync::Semaphore::const_new((CONSOLIDATION_BYTES >> 10) as usize);
    &KIB
}
