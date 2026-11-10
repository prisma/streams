//! Kani proof for the frontier's step: KANI-110 (S3b-DESIGN §4.2, §11.3).
//! Over any page offset, send time, retention and clock, and any setting
//! of the page's three bits in its range row, `at_frontier` takes exactly
//! one arm, and:
//! - a scheduled offset (its `skip` bit) is passed, resolved exactly when
//!   its `sbits` bit is clear;
//! - a cancelled offset is passed, never leased (I4);
//! - an untaken message is leased only before its retention end, and
//!   enters the gap from it on, whatever the arithmetic's range (I3).
use super::{AtFrontier, at_frontier};
use crate::native_queue::range::RangeRow;

#[kani::proof]
fn kani_110_the_frontier_takes_one_arm_and_never_leases_a_passed_offset() {
    let offset: u64 = kani::any();
    let ts: i64 = kani::any();
    let retention_ms: i64 = kani::any();
    let t: i64 = kani::any();
    let (skip, cancel, sbits): (bool, bool, bool) = (kani::any(), kani::any(), kani::any());
    let mut range = RangeRow::default();
    if skip {
        range.skip.set(offset);
    }
    if cancel {
        range.cancel.set(offset);
    }
    if sbits {
        range.sbits.set(offset);
    }
    let expired = ts.saturating_add(retention_ms) <= t;
    match at_frontier(&range, (offset, ts), retention_ms, t) {
        AtFrontier::Scheduled { resolved } => {
            kani::cover!(resolved, "a resolved scheduled offset is passed");
            kani::cover!(!resolved, "an unresolved scheduled offset is passed");
            assert!(skip, "only a skip bit passes an offset as scheduled");
            assert!(
                resolved == !sbits,
                "resolved exactly when the sbits bit is clear"
            );
        }
        AtFrontier::Cancelled => {
            kani::cover!(true, "a cancelled offset is passed");
            assert!(
                !skip && cancel,
                "only a cancel bit, and no skip bit, passes it as cancelled"
            );
        }
        AtFrontier::Expired => {
            kani::cover!(true, "an untaken message enters the gap");
            assert!(!skip && !cancel, "only an unmarked offset enters the gap");
            assert!(
                expired,
                "a message enters the gap only from its retention end"
            );
        }
        AtFrontier::Live => {
            kani::cover!(true, "an untaken message is leased");
            assert!(!skip && !cancel, "a marked offset is never leased");
            assert!(
                !expired,
                "a message is leased only before its retention end"
            );
        }
    }
}
