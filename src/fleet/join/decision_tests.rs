#![cfg(test)]
//! What this boot's own committed write tells its member note (`note_decision`, Addendum 63).

use super::{Write, note_decision};

/// Addendum 63 (TLA-049's undecided joiner): a committed write that lists this boot with its own
/// boot id (a join, an in-place restart, a stamp) notes it a member at once, so a foreign write
/// before its next pass is repaired by C18 and C18b; its own release clears the note; a write not
/// committed, or one that does not list it so, notes nothing.
#[test]
fn a_committed_write_that_lists_this_boot_notes_it_a_member_at_once() {
    use crate::fleet::standing::Standing;
    let decided = |written: bool, why: &Write| {
        let standing = Standing::default();
        note_decision(&standing, written, why);
        standing.decided_member()
    };
    let joined = Write::Joined { uncovered: vec![] };
    let listing = [&joined, &Write::InPlace, &Write::Stamped];
    assert_eq!(
        listing.map(|why| decided(true, why)),
        [true; 3],
        "committed"
    );
    assert_eq!(
        listing.map(|why| decided(false, why)),
        [false; 3],
        "not committed"
    );
    let others = [
        &Write::Unlisted,
        &Write::Listed,
        &Write::Pinned(vec![], None),
    ];
    assert_eq!(others.map(|why| decided(true, why)), [false; 3]);
    let standing = Standing::default();
    standing.note_member();
    note_decision(&standing, true, &Write::Released);
    assert!(!standing.decided_member(), "its own release clears it");
}
