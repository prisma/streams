//! A failed source read owes the feed one typed verdict (item 87): a
//! cutoff retires it once, a transient failure changes nothing.
#![cfg(test)]
use super::feed_with;
use crate::sse::feed::{DriveOutcome, FeedMemoryBudget, SourceCutoff};
use std::sync::Arc;
use std::sync::atomic::Ordering;

/// Item 87 pin: a fatal read retires the lifecycle with its typed reason
/// and bumps the version exactly once, at the transition, so parked
/// sessions wake to disconnect; nothing is delivered.
#[tokio::test]
async fn a_fatal_source_read_retires_the_feed_once() {
    let budget = Arc::new(FeedMemoryBudget::new_for_test(1 << 20));
    let (feed, src) = feed_with(4, 8, 1 << 20, &budget);
    let (_cursor, woken, _generation, _swapped) = feed.subscribe_locked();
    *src.cut_reads.lock().unwrap() = Some(SourceCutoff::WrongOwner);
    let v0 = feed.version();
    assert!(matches!(
        feed.drive_once().await,
        Some(DriveOutcome::IncarnationClosed(SourceCutoff::WrongOwner))
    ));
    assert_eq!(feed.lifecycle_for_test(), "Gone");
    assert_eq!(feed.version(), v0 + 1, "the cutoff bumps exactly once");
    assert!(
        woken.has_changed().unwrap(),
        "parked sessions wake to disconnect"
    );
    assert_eq!(feed.head(), 0, "a cutoff delivers nothing");
}

/// Item 87 (red on eb742c42: the retried cause was dropped): a transient
/// failure is counted and retried with its cause logged, once per drive.
#[tokio::test]
async fn a_retryable_source_read_logs_its_cause_once_per_drive() {
    let log = crate::sse::test_log::ErrorLog::capture();
    let budget = Arc::new(FeedMemoryBudget::new_for_test(1 << 20));
    let (feed, src) = feed_with(4, 8, 1 << 20, &budget);
    feed.subscribe_locked();
    src.fail_reads.store(true, Ordering::Relaxed);
    assert!(matches!(
        feed.drive_once().await,
        Some(DriveOutcome::SourceFailed)
    ));
    assert_eq!(
        log.causes(),
        ["injected source failure"],
        "one cause per failed drive"
    );
}
