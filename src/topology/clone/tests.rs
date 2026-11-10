#![cfg(test)]
//! The skew a clock refusal names, and the waits a seeded clone takes for it.

use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use super::{SKEW_BUDGET, seeded, skew_wait, tick};

fn refusal(last: i64, next: i64) -> slatedb::Error {
    slatedb::Error::invalid(format!(
        "invalid clock tick, must be monotonic. last_tick=`{last}`, next_tick=`{next}`"
    ))
}

/// A clock refusal names its two ticks; the wait is twice the gap plus 1 ms, and nothing else is
/// a skew: no gap, a backward pair, another invalid error, another kind.
#[test]
fn a_clock_refusal_waits_twice_its_gap() {
    assert_eq!(tick("a last_tick=`42`, b", "last_tick=`"), Some(42));
    assert_eq!(tick("last_tick=`4", "last_tick=`"), None);
    assert_eq!(
        skew_wait(&refusal(1_389, 1_388)),
        Some(Duration::from_millis(3))
    );
    assert_eq!(
        skew_wait(&refusal(1_500, 1_000)),
        Some(Duration::from_millis(1_001))
    );
    assert_eq!(skew_wait(&refusal(1_000, 1_000)), None);
    assert_eq!(skew_wait(&refusal(1_000, 1_389)), None);
    assert_eq!(
        skew_wait(&slatedb::Error::invalid("a bad argument".into())),
        None
    );
    assert_eq!(skew_wait(&slatedb::Error::unavailable("down".into())), None);
}

/// A clone refused for a skew is built again once its wait passes, while the waits stay within
/// the budget; any other error, and a skew past the budget, is the clone's answer at once.
#[tokio::test(crate = "crate::test_rt")]
async fn a_skewed_clone_is_built_again_within_the_budget() {
    let store = object_store::memory::InMemory::new();
    let attempts = AtomicU32::new(0);
    let twice = seeded(&store, "p", |_| {
        let attempt = attempts.fetch_add(1, Ordering::SeqCst);
        async move {
            match attempt {
                0 | 1 => Err(refusal(1_010, 1_000)),
                _ => Ok(()),
            }
        }
    });
    assert_eq!(twice.await, Ok(()));
    assert_eq!(attempts.load(Ordering::SeqCst), 3);

    let over = u64::try_from(SKEW_BUDGET.as_millis()).unwrap();
    let over = i64::try_from(over).unwrap();
    let attempts = AtomicU32::new(0);
    let refused = seeded(&store, "p", |_| {
        attempts.fetch_add(1, Ordering::SeqCst);
        async move { Err(refusal(1_000 + over, 1_000)) }
    })
    .await;
    assert!(refused.unwrap_err().starts_with("clone p: "));
    assert_eq!(attempts.load(Ordering::SeqCst), 1, "a skew past the budget");

    let attempts = AtomicU32::new(0);
    let budget = seeded(&store, "p", |_| {
        attempts.fetch_add(1, Ordering::SeqCst);
        async move { Err(refusal(1_200, 1_000)) }
    })
    .await;
    assert!(budget.is_err());
    assert_eq!(
        attempts.load(Ordering::SeqCst),
        3,
        "401 ms twice fits the 1 s budget, a third does not"
    );

    let attempts = AtomicU32::new(0);
    let other = seeded(&store, "p", |_| {
        attempts.fetch_add(1, Ordering::SeqCst);
        async move { Err(slatedb::Error::unavailable("down".into())) }
    })
    .await;
    assert!(other.is_err());
    assert_eq!(
        attempts.load(Ordering::SeqCst),
        1,
        "no retry for another error"
    );
}
