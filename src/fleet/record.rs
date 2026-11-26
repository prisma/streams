//! The membership of record (§22, GATE-DECISION-s22-addendum-2026-11-19 C1-C4, C17; the join's
//! R1, GATE-DECISION-tla049-r1-r3-2026-11-20 §2.2).
//!
//! The overrides document's `members` names the ring of record, each member with the boot id that
//! joined (R1a stamps it). A view's ring is computed from the one document read that gives the
//! view its entries (C1, C2): **the listed members, less those the reader holds evidence against**
//! (R1). Evidence is a held beat **of the listed boot id** that is stale, progress-stale, draining
//! or withdrawn (`planning::member`). No beat held, a beat of another boot, a GET that answered
//! absent: none is evidence, and the member is in the ring. So the ring is a function of one
//! document and the held beats, with no read order in it. This instance is in its own ring only
//! when the document lists its name with its own boot id (R1b, C4); until then (an unlisted
//! joiner, or a restart whose R1a CAS has not landed, C13) its publications leave it out. A boot
//! that has read itself so listed (`Standing::decided_member`) also counts a listing of its name
//! with no boot id, whatever the count (C18b, the join ruling, Addendum 35; Addendum 41 B item 8),
//! for the passes until its C18 stamp lands; never a listing with another boot id.
//!
//! A listed member with no boot id (the step 4 interim wrote members so, Addendum 14) is judged
//! by name, as today's rule judged it, until it stamps its own boot id on its next pass
//! (`join::pass`). No listed member surviving the evidence, the listed members within the count
//! are the ring unfiltered (C3's fallback, today's bootstrap); a document listing nobody gives no
//! ring (`awaits_view`) until a join commits. A document without `members` (an older cell's, C17)
//! gives today's rule until a member of today's ring lists it.
//!
//! The interim's add half is gone (the join, Addendum 14: one writer per half): only a join CAS
//! adds a member. Its removal half, a listed name beyond the count leaving at the next pass of a
//! member of the ring, stays until the leave lands (scale-in keeps today's class).

use super::Member;

/// The ordinal names within `count`: `streams-1`..=`streams-count`, one at least.
pub(super) fn ordinal(count: u64) -> Vec<String> {
    (1..=count.max(1))
        .map(|index| format!("streams-{index}"))
        .collect()
}

/// `name`'s place in the ordinal order: its index, then (for a name outside the pattern) after
/// every ordinal, by name. Every ring is in this order (the join ruling, Addendum 23 §3).
// mt-lint: allow(name-param-shared-core): a fleet instance name's ring order; no tenant or stream identity
pub(super) fn ordinal_key(name: &str) -> (u64, &str) {
    let index = name
        .strip_prefix("streams-")
        .and_then(|index| index.parse::<u64>().ok());
    (index.unwrap_or(u64::MAX), name)
}

/// Whether the fleet instance `instance` is an ordinal within `count` (C5, C8, C11).
pub(super) fn within_count(instance: &str, count: u64) -> bool {
    instance
        .strip_prefix("streams-")
        .and_then(|index| index.parse::<u64>().ok())
        .is_some_and(|index| (1..=count.max(1)).contains(&index))
}

/// The members of `ring` within `count`, those a fair share divides by (Addendum 41 B item 2): a
/// leaver beyond the count stays in the ring until it releases, and counting it would understate
/// the share, refusing a return or a count move the cell needs.
pub(super) fn sharing(ring: &[String], count: u64) -> Vec<String> {
    ring.iter()
        .filter(|member| within_count(member, count))
        .cloned()
        .collect()
}

/// A boot id as the planner compares it: the first eight bytes of its `crypto::hash16`, an
/// algorithm the code fixes (the join ruling §7). Never written or sent; both sides of every
/// comparison are hashed in the comparing process. Two boot ids that collide in 64 bits would let
/// one boot's beat count as evidence against the other's listing: accepted at that width.
pub(super) fn boot_of(boot_id: &str) -> u64 {
    let digest = crate::crypto::hash16(boot_id.as_bytes());
    let mut first = [0u8; 8];
    first.copy_from_slice(&digest[..8]);
    u64::from_le_bytes(first)
}

/// One pass's candidates (C3) and its held beats (R1): every beat the pass read with its boot id
/// and whether it qualifies, the reader kept, with the count the pass read and the reader's own
/// name and boot. The ring is made from them and a document.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(super) struct Candidates {
    count: u64,
    /// The qualified names, sorted, without duplicates.
    names: Vec<String>,
    /// Every held beat by name, sorted: its boot and whether it qualifies.
    held: Vec<(String, (u64, bool))>,
    /// The reader, and its boot once known (`with_boot`).
    me: String,
    my_boot: Option<u64>,
    /// Whether a pass of this boot has read it a member (C18b, `with_decided`).
    decided: bool,
    /// Every held beat's own time and whether it states its end (draining or withdrawn), by name,
    /// sorted (`with_beats`): what Addendum 46's guard watches.
    beats: Vec<(String, (i64, bool))>,
    /// The listed members this boot's watch of their beats has settled as gone (`with_settled`,
    /// `Standing::settled`), sorted.
    settled: Vec<String>,
}

impl Candidates {
    /// Candidates qualified by name alone, with no beats held (unit tests and today's rule).
    pub(super) fn new(count: u64, mut names: Vec<String>) -> Self {
        names.sort();
        names.dedup();
        Self {
            count,
            names,
            ..Self::default()
        }
    }

    /// A pass's candidates from its held beats (`planning::active_members`): `me` is kept when the
    /// listing missed its beat.
    pub(super) fn held(count: u64, me: &str, mut held: Vec<(String, (u64, bool))>) -> Self {
        held.sort_by(|a, b| a.0.cmp(&b.0));
        let qualified = held.iter().filter(|(_, (_, ok))| *ok).map(|(name, _)| name);
        let missed = held
            .iter()
            .all(|(name, _)| name != me)
            .then(|| me.to_string());
        let mut candidates = Self::new(count, qualified.cloned().chain(missed).collect());
        candidates.held = held;
        candidates.me = me.to_string();
        candidates
    }

    /// The same, the reader's own boot id known (R1b).
    pub(super) fn with_boot(mut self, boot_id: &str) -> Self {
        self.my_boot = Some(boot_of(boot_id));
        self
    }

    /// The same, with each held beat's own time and whether it states its end
    /// (`planning::active_members`).
    pub(super) fn with_beats(mut self, mut beats: Vec<(String, (i64, bool))>) -> Self {
        beats.sort_by(|a, b| a.0.cmp(&b.0));
        self.beats = beats;
        self
    }

    /// Each held beat that does not qualify: its name, boot, own time and whether it states its
    /// end. The beats `with_beats` gave no time are left out.
    pub(super) fn unqualified(&self) -> impl Iterator<Item = (&str, u64, i64, bool)> {
        self.held
            .iter()
            .filter(|(_, (_, ok))| !ok)
            .filter_map(|(name, (boot, _))| {
                let at = self.beats.binary_search_by(|beat| beat.0.cmp(name)).ok()?;
                let (ts_ms, stated) = self.beats[at].1;
                Some((name.as_str(), *boot, ts_ms, stated))
            })
    }

    /// The same, `settled` the members `Standing::settled` found gone.
    pub(super) fn with_settled(mut self, mut settled: Vec<String>) -> Self {
        settled.sort();
        self.settled = settled;
        self
    }

    /// The same, knowing whether a pass of this boot has read it listed with its own boot id
    /// (`Standing::decided_member`, C18b).
    pub(super) fn with_decided(mut self, decided: bool) -> Self {
        self.decided = decided;
        self
    }

    /// The count the pass read.
    pub(super) fn count(&self) -> u64 {
        self.count
    }

    /// Whether the fleet instance `instance` is a candidate.
    pub(super) fn contains(&self, instance: &str) -> bool {
        self.names
            .binary_search_by(|held| held.as_str().cmp(instance))
            .is_ok()
    }

    /// The held beat of the fleet instance `instance`: its boot and whether it qualifies.
    pub(super) fn beat_of(&self, instance: &str) -> Option<(u64, bool)> {
        let at = self
            .held
            .binary_search_by(|held| held.0.as_str().cmp(instance));
        at.ok().map(|at| self.held[at].1)
    }

    /// Whether the pass read this instance's own beat of this boot (R3's third clause).
    pub(super) fn read_own_beat(&self) -> bool {
        self.my_boot
            .is_some_and(|boot| self.beat_of(&self.me).is_some_and(|(held, _)| held == boot))
    }

    /// Today's ring (the rule before §22): the ordinal names within the count that qualify, else
    /// all of them unfiltered.
    pub(super) fn today(&self) -> Vec<String> {
        let ordinal = ordinal(self.count);
        let active: Vec<String> = ordinal
            .iter()
            .filter(|name| self.contains(name))
            .cloned()
            .collect();
        if active.is_empty() { ordinal } else { active }
    }

    /// The ring of a document listing `members` (C1, C3 under R1): the listed members this reader
    /// holds no evidence against; none, the listed members within the count unfiltered. A
    /// document without `members` gives [`Candidates::today`] (C17). Always in the ordinal order,
    /// whatever order the document lists: `ring_pick` breaks an equal score by position, so two
    /// orders of one set could name two owners for one leaf (the join ruling, Addendum 23 §3).
    pub(super) fn ring(&self, members: Option<&[Member]>) -> Vec<String> {
        let Some(listed) = members else {
            return self.today();
        };
        let mut kept: Vec<&str> = listed
            .iter()
            .filter(|member| self.in_ring(member))
            .map(|member| member.name.as_str())
            .collect();
        if kept.is_empty() {
            let within = listed
                .iter()
                .filter(|member| within_count(&member.name, self.count));
            kept = within.map(|member| member.name.as_str()).collect();
        }
        kept.sort_by_key(|name| ordinal_key(name));
        kept.into_iter().map(str::to_string).collect()
    }

    /// A joiner's coverage ring (GATE-DECISION-s22-join-2026-11-20 Addenda 45 and 46): `listed`
    /// less each member this reader holds settled evidence against, a held beat of its listed boot
    /// that does not qualify and that `settled` names. Never C3's unfiltered fallback, so it can be
    /// empty: a joiner behind listed members that are all gone has nothing to cover. A member with
    /// no beat of its listed boot held, or unstamped, has no evidence against it and stays.
    pub(super) fn evidence_ring(&self, listed: &[Member]) -> Vec<String> {
        let gone = |member: &&Member| {
            let boot = member.boot_id.as_deref().map(boot_of);
            let held = self.beat_of(&member.name);
            let against = matches!((boot, held), (Some(boot), Some((held, false))) if boot == held);
            against && self.settled.binary_search(&member.name).is_ok()
        };
        let mut kept: Vec<&str> = listed
            .iter()
            .filter(|member| !gone(member))
            .map(|member| member.name.as_str())
            .collect();
        kept.sort_by_key(|name| ordinal_key(name));
        kept.into_iter().map(str::to_string).collect()
    }

    /// Whether `member` is in this reader's ring (R1, R1b).
    fn in_ring(&self, member: &Member) -> bool {
        let listed = member.boot_id.as_deref().map(boot_of);
        if member.name == self.me {
            // Never by the listing alone: an unstamped listing of its own name is another boot's
            // until it stamps it (the join ruling §2), unless this boot already read itself a
            // member (C18b), whatever the count (Addendum 41 B item 8): C18 stamps it back in place.
            let unstamped = listed.is_none() && self.decided;
            let mine = (listed.is_some() && listed == self.my_boot) || unstamped;
            // A member never judges its own liveness, but leaves its own ring when its own beat of
            // this boot withdrew, drains or stopped progressing: today's rule for itself.
            let own = self.my_boot.and_then(|boot| {
                let held = self.beat_of(&self.me);
                held.filter(|(held, _)| *held == boot)
            });
            return mine && own.is_none_or(|(_, qualifies)| qualifies);
        }
        match (listed, self.beat_of(&member.name)) {
            (Some(boot), Some((held, qualifies))) if boot == held => qualifies,
            (Some(_), _) => true,
            (None, beat) => beat.is_some_and(|(_, qualifies)| qualifies),
        }
    }

    /// Whether the document lists this reader with its own boot id (R1b): it is a member.
    pub(super) fn is_member(&self, members: Option<&[Member]>) -> bool {
        let mine = |member: &&Member| member.name == self.me;
        members
            .and_then(|listed| listed.iter().find(mine))
            .is_some_and(|member| member.boot_id.as_deref().map(boot_of) == self.my_boot)
    }

    /// The reader's name.
    pub(super) fn me(&self) -> &str {
        &self.me
    }
}

#[cfg(test)]
mod tests {
    use super::{Candidates, ordinal};
    use crate::fleet::Member;

    fn names(list: &[&str]) -> Vec<String> {
        list.iter().map(|name| (*name).to_string()).collect()
    }

    /// `name`'s beat of boot `boot` as `planning::active_members` holds it.
    fn beat(name: &str, boot: &str, qualifies: bool) -> (String, (u64, bool)) {
        (name.to_string(), (super::boot_of(boot), qualifies))
    }

    fn member(name: &str, boot: Option<&str>) -> Member {
        Member {
            name: name.to_string(),
            boot_id: boot.map(str::to_string),
        }
    }

    /// **R1, the ring from one document and the held beats** (GATE-DECISION-tla049-r1-r3 §2.2):
    /// a listed member leaves only on a held beat of its listed boot that does not qualify; no
    /// beat, or a beat of another boot, is no evidence. The model's `RingRec` with `RingRule`
    /// "evidence" and M1's incarnations.
    #[test]
    fn a_listed_member_leaves_a_ring_only_on_evidence_of_its_listed_boot() {
        let held = vec![
            beat("streams-1", "a1", true),
            beat("streams-2", "b-old", false),
            beat("streams-3", "c1", false),
        ];
        let me = Candidates::held(2, "streams-1", held).with_boot("a1");
        let listed = [
            member("streams-1", Some("a1")),
            member("streams-2", Some("b-new")),
            member("streams-3", Some("c1")),
            member("streams-4", Some("d1")),
        ];
        assert_eq!(
            me.ring(Some(&listed)),
            names(&["streams-1", "streams-2", "streams-4"])
        );
    }

    /// R1b and C13: this instance is in its own ring only when it is listed with its own boot id;
    /// a restart whose R1a CAS has not landed, or a joiner, leaves itself out.
    #[test]
    fn an_instance_is_in_its_own_ring_only_listed_with_its_own_boot() {
        let me = Candidates::held(2, "streams-1", vec![beat("streams-1", "a2", true)]);
        let me = me.with_boot("a2");
        let theirs = [
            member("streams-1", Some("a1")),
            member("streams-2", Some("b1")),
        ];
        let mine = [
            member("streams-1", Some("a2")),
            member("streams-2", Some("b1")),
        ];
        assert_eq!(me.ring(Some(&theirs)), names(&["streams-2"]));
        assert!(!me.is_member(Some(&theirs)));
        assert_eq!(me.ring(Some(&mine)), names(&["streams-1", "streams-2"]));
        assert!(me.is_member(Some(&mine)));
        assert!(me.read_own_beat());
    }

    /// The upgrade from the interim (step 4 listed no boot ids): a listed member without one is
    /// judged by name, today's rule, until it stamps its own; this instance unstamped is neither
    /// in its own ring nor a member (C13 decides it, `join::plan`; the join ruling §2).
    #[test]
    fn a_listed_member_without_a_boot_id_is_judged_by_name() {
        let held = vec![
            beat("streams-1", "a1", true),
            beat("streams-2", "b1", false),
            beat("streams-3", "c1", true),
        ];
        let me = Candidates::held(3, "streams-1", held).with_boot("a1");
        let listed = [
            member("streams-1", None),
            member("streams-2", None),
            member("streams-3", None),
        ];
        assert_eq!(me.ring(Some(&listed)), names(&["streams-3"]));
        assert!(!me.is_member(Some(&listed)));
    }

    /// **C18b** (the join ruling, Addendum 35; Addendum 41 B item 8): a boot that has read itself
    /// a member counts a listing of its own name with no boot id as membership in its own ring
    /// until its C18 stamp lands, whatever the count (a member beyond it has not released); an
    /// undecided boot does not (R1b), and a listing with another boot id is never its own
    /// (supersession).
    #[test]
    fn a_decided_member_counts_its_own_unstamped_listing_whatever_the_count() {
        let held = |me: &str| {
            let beats = vec![beat(me, "a1", true), beat("streams-2", "b1", true)];
            Candidates::held(3, me, beats).with_boot("a1")
        };
        let unstamped = [member("streams-1", None), member("streams-2", Some("b1"))];
        let theirs = [
            member("streams-1", Some("a0")),
            member("streams-2", Some("b1")),
        ];
        let beyond = [member("streams-4", None), member("streams-2", Some("b1"))];
        let decided = held("streams-1").with_decided(true);
        let got = (
            decided.ring(Some(&unstamped)),
            held("streams-1").ring(Some(&unstamped)),
            decided.ring(Some(&theirs)),
            held("streams-4").with_decided(true).ring(Some(&beyond)),
        );
        let want = (
            names(&["streams-1", "streams-2"]),
            names(&["streams-2"]),
            names(&["streams-2"]),
            names(&["streams-2", "streams-4"]),
        );
        assert_eq!(got, want);
        assert!(
            !decided.is_member(Some(&unstamped)),
            "C18b is the ring's, not R1b's member"
        );
    }

    /// A pass's candidates keep the reader when its listing missed the reader's beat, and the reader
    /// leaves its own ring only on its own beat of this boot that does not qualify, never on a beat
    /// of another boot (#183's survivors: `held`'s `!=` and `in_ring`'s `==` on the boot).
    #[test]
    fn the_reader_is_a_candidate_without_its_beat_and_judged_only_by_this_boots() {
        let peer_only = Candidates::held(2, "streams-1", vec![beat("streams-2", "b1", true)]);
        assert!(
            peer_only.contains("streams-1"),
            "the listing missed its beat"
        );
        let listed = [
            member("streams-1", Some("a1")),
            member("streams-2", Some("b1")),
        ];
        let ring = |own: (String, (u64, bool))| {
            let held = vec![own, beat("streams-2", "b1", true)];
            Candidates::held(2, "streams-1", held)
                .with_boot("a1")
                .ring(Some(&listed))
        };
        assert_eq!(
            (
                ring(beat("streams-1", "a0", false)),
                ring(beat("streams-1", "a1", false))
            ),
            (names(&["streams-1", "streams-2"]), names(&["streams-2"])),
            "(an old boot's stopped beat, this boot's)"
        );
    }

    /// R3's third clause reads this boot's own beat and nothing else: not a beat of another boot,
    /// not a pass that missed its beat, not before the boot is known; and the reader is the name it
    /// was read for (#183's survivors: `read_own_beat` as `true`, `me` as another name).
    #[test]
    fn only_a_beat_of_this_boot_is_the_readers_own() {
        let reader = |held: Vec<(String, (u64, bool))>| Candidates::held(2, "streams-1", held);
        let peer = beat("streams-2", "b1", true);
        let got = (
            reader(vec![beat("streams-1", "a1", true)]).read_own_beat(),
            reader(vec![beat("streams-1", "a0", true)])
                .with_boot("a1")
                .read_own_beat(),
            reader(vec![peer.clone()]).with_boot("a1").read_own_beat(),
            reader(vec![beat("streams-1", "a1", false), peer])
                .with_boot("a1")
                .read_own_beat(),
        );
        assert_eq!(
            got,
            (false, false, false, true),
            "(no boot, old boot, missed, own)"
        );
        assert_eq!(reader(Vec::new()).me(), "streams-1");
    }

    /// **A ring is in the ordinal order, whatever order the document lists** (the join ruling,
    /// Addendum 23 §3). `streams-112789` and `streams-349192` score equal for the leaf `0`, so
    /// `ring_pick` takes whichever is listed first: the two listings of one set give one ring and
    /// the same owner for every leaf, the tie included.
    #[test]
    fn a_ring_is_in_ordinal_order_so_a_tie_has_one_owner() {
        let (low, high) = ("streams-112789", "streams-349192");
        let held = vec![beat(low, "l1", true), beat(high, "h1", true)];
        let me = Candidates::held(400_000, "streams-9", held).with_boot("z");
        let ascending = [member(low, Some("l1")), member(high, Some("h1"))];
        let descending = [member(high, Some("h1")), member(low, Some("l1"))];
        let (up, down) = (me.ring(Some(&ascending)), me.ring(Some(&descending)));
        assert_eq!((&up, &down), (&names(&[low, high]), &names(&[low, high])));
        for leaf in ["", "0", "1", "00", "01", "10", "11"] {
            let pick = |ring: &[String]| ring[crate::ownership::ring_pick(leaf, ring)].clone();
            assert_eq!(pick(&up), pick(&down), "one owner for {leaf:?}");
        }
        let raw = |list: &[&str]| {
            let ring = names(list);
            ring[crate::ownership::ring_pick("0", &ring)].clone()
        };
        assert_ne!(raw(&[low, high]), raw(&[high, low]), "the leaf 0 is a tie");
    }

    /// C3's fallback and C17: every listed member judged dead gives the listed members within the
    /// count unfiltered; a document listing nobody gives no ring; one without `members` gives
    /// today's rule.
    #[test]
    fn the_ring_falls_back_as_c3_and_c17_rule() {
        let held = vec![
            beat("streams-2", "b1", false),
            beat("streams-3", "c1", false),
        ];
        let me = Candidates::held(2, "streams-9", held).with_boot("z");
        let listed = [
            member("streams-2", Some("b1")),
            member("streams-3", Some("c1")),
        ];
        assert_eq!(me.ring(Some(&listed)), names(&["streams-2"]));
        assert_eq!(me.ring(Some(&[])), Vec::<String>::new());
        assert_eq!(me.ring(None), ordinal(2));
    }

    /// C17 (§2.1): a document is read back byte-identical by an instance that does not write it:
    /// an older one without `members`, a listed one whose boot ids are absent, and one whose boot
    /// ids a later version stamped. Every other writer of the document re-encodes what it read.
    #[test]
    fn a_document_reads_back_byte_identical_with_or_without_members_and_boot_ids() {
        let documents = [
            r#"{"entries":{}}"#,
            r#"{"entries":{},"members":[{"name":"streams-1"},{"name":"streams-2"}]}"#,
            r#"{"entries":{},"members":[{"name":"streams-1","boot_id":"b1"},{"name":"streams-2"}]}"#,
        ];
        for document in documents {
            let read: crate::fleet::Overrides = serde_json::from_str(document).unwrap();
            assert_eq!(serde_json::to_string(&read).unwrap(), document);
        }
        let older: crate::fleet::Overrides = serde_json::from_str(documents[0]).unwrap();
        assert_eq!(older.members, None);
    }

    /// Boot ids compare by value; ordinals within the count by index.
    #[test]
    fn boot_ids_and_ordinals_compare_as_written() {
        assert_eq!(super::boot_of("b1"), super::boot_of("b1"));
        assert_ne!(super::boot_of("b1"), super::boot_of("b2"));
        let within = ["streams-1", "streams-2", "streams-3", "streams-x", "other"]
            .map(|name| super::within_count(name, 2));
        assert_eq!(within, [true, true, false, false, false]);
    }

    /// Today's ring: the qualified ordinals within the count, in ordinal order; none qualifying,
    /// every ordinal within the count.
    /// The members a fair share divides by (Addendum 41 B item 2): the ring's within the count,
    /// in the ring's order.
    #[test]
    fn a_share_divides_by_the_rings_members_within_the_count() {
        let ring = names(&["streams-3", "streams-1", "streams-4", "streams-2"]);
        assert_eq!(super::sharing(&ring, 2), names(&["streams-1", "streams-2"]));
        assert_eq!(
            super::sharing(&ring, 3),
            names(&["streams-3", "streams-1", "streams-2"])
        );
    }

    #[test]
    fn todays_ring_is_the_qualified_ordinals_within_the_count() {
        let candidates = Candidates::new(3, names(&["streams-3", "streams-1", "streams-9"]));
        assert_eq!(candidates.today(), names(&["streams-1", "streams-3"]));
        assert_eq!(Candidates::new(2, Vec::new()).today(), ordinal(2));
    }

    /// Every production call of the ownership view's two writers, as `(method, file, fn)`: the
    /// crate's sources outside `src/dst`, `src/bin`, test-only files and `cfg(test)` items. Calls
    /// inside a macro's tokens are not seen (none is today).
    fn view_writers() -> Vec<(String, String, String)> {
        let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
        let (mut dirs, mut files) = (vec![root.clone()], Vec::new());
        while let Some(dir) = dirs.pop() {
            for path in std::fs::read_dir(&dir)
                .unwrap()
                .map(|entry| entry.unwrap().path())
            {
                sort_entry(&root, path, &mut dirs, &mut files);
            }
        }
        let mut writers = Writers::default();
        for (relative, path) in files {
            let ast = syn::parse_file(&std::fs::read_to_string(&path).unwrap()).unwrap();
            if !test_gated(&ast.attrs) {
                writers.file = relative;
                syn::visit::Visit::visit_file(&mut writers, &ast);
            }
        }
        writers.found.sort();
        writers.found
    }

    /// One entry of the source walk: a directory to walk (but `dst` and `bin`), or a production
    /// `.rs` file (not a `*tests` file) with its path relative to `src`.
    fn sort_entry(
        root: &std::path::Path,
        path: std::path::PathBuf,
        dirs: &mut Vec<std::path::PathBuf>,
        files: &mut Vec<(String, std::path::PathBuf)>,
    ) {
        let relative = path
            .strip_prefix(root)
            .unwrap()
            .to_string_lossy()
            .replace('\\', "/");
        let stem = path
            .file_stem()
            .unwrap_or_default()
            .to_string_lossy()
            .to_string();
        if path.is_dir() && relative != "dst" && relative != "bin" {
            dirs.push(path);
        } else if path.extension().is_some_and(|x| x == "rs") && !stem.ends_with("tests") {
            files.push((relative, path));
        }
    }

    /// `cfg(test)`, `#[test]` or `#[tokio::test]`-like: a doc comment never gates.
    fn test_gated(attrs: &[syn::Attribute]) -> bool {
        use quote::ToTokens as _;
        let gates = |attr: &syn::Attribute| {
            let text = attr.to_token_stream().to_string();
            !attr.path().is_ident("doc") && text.contains("test")
        };
        attrs.iter().any(gates)
    }

    #[derive(Default)]
    struct Writers {
        file: String,
        within: Vec<String>,
        found: Vec<(String, String, String)>,
    }

    impl<'ast> syn::visit::Visit<'ast> for Writers {
        fn visit_item_mod(&mut self, item: &'ast syn::ItemMod) {
            if !test_gated(&item.attrs) {
                syn::visit::visit_item_mod(self, item);
            }
        }
        fn visit_item_fn(&mut self, item: &'ast syn::ItemFn) {
            if !test_gated(&item.attrs) {
                self.within.push(item.sig.ident.to_string());
                syn::visit::visit_item_fn(self, item);
                self.within.pop();
            }
        }
        fn visit_impl_item_fn(&mut self, item: &'ast syn::ImplItemFn) {
            if !test_gated(&item.attrs) {
                self.within.push(item.sig.ident.to_string());
                syn::visit::visit_impl_item_fn(self, item);
                self.within.pop();
            }
        }
        fn visit_expr_method_call(&mut self, call: &'ast syn::ExprMethodCall) {
            let method = call.method.to_string();
            if method == "set_view" || method == "set_override" {
                let within = self.within.last().cloned().unwrap_or_default();
                self.found.push((method, self.file.clone(), within));
            }
            syn::visit::visit_expr_method_call(self, call);
        }
    }

    /// §22 C2 (GATE-DECISION-s22-3b-step4-2026-11-21 §2.2 and Addendum 1): the ownership view is
    /// published whole by exactly five calls, the tick and four that take their ring from
    /// `Standing::ring_of` (`installed` serves a split's birth and the lag move's evacuation, each
    /// publishing the document it wrote; a co-location writes no entry, GATE-DECISION-s22-join-
    /// 2026-11-20 Addendum 13); and no production call sets one entry beside it. A sixth fails here
    /// first.
    #[test]
    fn the_ownership_view_has_exactly_its_known_production_writers() {
        let expected = [
            ("set_view", "fleet/handoff.rs", "mirror"),
            ("set_view", "fleet/view.rs", "installed"),
            ("set_view", "fleet/view.rs", "published"),
            ("set_view", "fleet/view.rs", "published_here"),
            ("set_view", "topology/revalidate.rs", "refreshed"),
        ];
        let expected: Vec<(String, String, String)> = expected
            .iter()
            .map(|(m, f, w)| ((*m).to_string(), (*f).to_string(), (*w).to_string()))
            .collect();
        assert_eq!(view_writers(), expected);
    }

    /// A joiner's coverage ring (Addenda 45 and 46): the listed members less those with settled
    /// evidence against their listed boot; never the unfiltered fallback, so it can be empty. A
    /// stale beat the watch has not settled stays, as do a member with no beat of its listed boot
    /// held (C14's corner), an unstamped listing and a member whose beat qualifies.
    #[test]
    fn a_joiners_coverage_ring_drops_only_settled_evidence() {
        let listed = vec![
            member("streams-1", Some("a1")),
            member("streams-2", Some("b1")),
            member("streams-3", Some("c1")),
            member("streams-4", None),
        ];
        let held = vec![
            beat("streams-1", "a1", false),
            beat("streams-2", "b1", false),
            beat("streams-3", "other", false),
            beat("streams-4", "d1", false),
        ];
        let reader = |settled: &[&str]| {
            Candidates::held(4, "streams-5", held.clone()).with_settled(names(settled))
        };
        let all = names(&["streams-1", "streams-2", "streams-3", "streams-4"]);
        assert_eq!(reader(&[]).evidence_ring(&listed), all, "nothing settled");
        let settled = reader(&["streams-1", "streams-3", "streams-4"]).evidence_ring(&listed);
        assert_eq!(settled, names(&["streams-2", "streams-3", "streams-4"]));
        let two = &listed[..2];
        let gone = reader(&["streams-1", "streams-2"]).evidence_ring(two);
        assert_eq!(
            gone,
            Vec::<String>::new(),
            "every listed member gone: empty, no fallback"
        );
        assert_eq!(gone, reader(&[]).evidence_ring(&[]));
    }

    /// The beats the watch reads (`Standing::settled`, Addendum 46): each held beat that does not
    /// qualify, with its boot, its own time and whether it states its end (withdrawn or draining,
    /// either alone), as `planning::active_members` holds them; a qualifying beat is not one, and
    /// a beat given no time is left out.
    #[test]
    fn the_unqualified_beats_carry_their_boot_time_and_stated_end() {
        const NOW: i64 = 1_000_000;
        let stale = NOW - crate::fleet::planning::RING_LIVENESS_MS;
        let heartbeat = |name: &str, ts: i64, end: &str| {
            let document = format!(
                r#"{{"instance":"{name}","ts_ms":{ts},"rps":0.0,"owned_shards":[],"boot_id":"{name}-b"{end}}}"#
            );
            let heartbeat: crate::fleet::Heartbeat = serde_json::from_str(&document).unwrap();
            (name.to_string(), heartbeat.candidacy(NOW))
        };
        let candidacies = [
            heartbeat("streams-1", NOW, r#","draining":false"#),
            heartbeat(
                "streams-2",
                NOW - 10,
                r#","draining":false,"withdrawn":"stopping""#,
            ),
            heartbeat("streams-3", NOW - 20, r#","draining":true"#),
            heartbeat("streams-4", stale, r#","draining":false"#),
            heartbeat("streams-5", NOW, r#","draining":false"#),
        ];
        let me = crate::fleet::planning::active_members(5, "streams-1", &candidacies.into());
        let boot = |name: &str| super::boot_of(&format!("{name}-b"));
        let unqualified: Vec<_> = me.unqualified().collect();
        let expected = [
            ("streams-2", boot("streams-2"), NOW - 10, true),
            ("streams-3", boot("streams-3"), NOW - 20, true),
            ("streams-4", boot("streams-4"), stale, false),
        ];
        assert_eq!(unqualified, expected);
        let held = vec![beat("streams-2", "b1", false)];
        let untimed = Candidates::held(2, "streams-1", held);
        assert_eq!(
            untimed.unqualified().count(),
            0,
            "no time, no beat to watch"
        );
    }
}
