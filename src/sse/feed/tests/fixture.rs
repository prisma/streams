use super::*;

pub(super) async fn hold_until_release(
    started: &tokio::sync::Notify,
    release: &tokio::sync::Notify,
) {
    // "Started" promises that an immediate release cannot be
    // lost, including a release from another runtime worker.
    let released = release.notified();
    tokio::pin!(released);
    released.as_mut().enable();
    started.notify_waiters();
    released.await;
}

#[test]
fn fake_source_release_cannot_race_started_signal() {
    use std::future::Future;
    use std::task::{Context, Wake, Waker};

    struct ReleaseOnStart {
        source: Arc<FakeSource>,
        next_source: bool,
    }
    impl Wake for ReleaseOnStart {
        fn wake(self: Arc<Self>) {
            self.wake_by_ref();
        }
        fn wake_by_ref(self: &Arc<Self>) {
            if self.next_source {
                self.source.next_release.notify_waiters();
            } else {
                self.source.read_release.notify_waiters();
            }
        }
    }

    // Release synchronously from the started waiter's wake. This
    // exercises the exact notification/registration interleaving
    // without sleeps, scheduling luck or an unbounded hanging test.
    for next_source in [true, false] {
        let source = Arc::new(FakeSource::new(1, 8));
        source
            .next_source_block
            .store(next_source, Ordering::Relaxed);
        source.block_reads.store(!next_source, Ordering::Relaxed);
        let started = if next_source {
            source.next_started.notified()
        } else {
            source.read_started.notified()
        };
        let mut started = std::pin::pin!(started);
        let release = Waker::from(Arc::new(ReleaseOnStart {
            source: source.clone(),
            next_source,
        }));
        assert!(
            started
                .as_mut()
                .poll(&mut Context::from_waker(&release))
                .is_pending()
        );
        let work = async {
            if next_source {
                assert!(matches!(
                    source.next_source().await.unwrap(),
                    SourceTransition::GenuineClose
                ));
            } else {
                let batch = source.read_batch(0, 1024).await.unwrap();
                assert_eq!(batch.records.len(), 1);
                assert_eq!(batch.scan_to, 1);
            }
        };
        let mut work = std::pin::pin!(work);
        assert!(
            work.as_mut()
                .poll(&mut Context::from_waker(Waker::noop()))
                .is_ready(),
            "release must survive the started signal (next_source={next_source})"
        );
    }
}
