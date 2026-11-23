//! The join (§22 step 5: GATE-DECISION-s22-addendum-2026-11-19 C5-C8, C13; GATE-DECISION-tla049-
//! r1-r3-2026-11-20 R1a, R2, R3): membership changes by CAS of the overrides document, inside the
//! fleet tick's publication, so a view's ring and its entries always come from one document.
//!
//! Each pass of the tick, under the revalidation lock and between its read of the document and its
//! publication (`view::published`), [`pass`] writes at most one CAS:
//! - **a member** (listed with its own boot id, R1b) **pins** (C7): an entry naming itself for each
//!   leaf the document gives it that the ring with every unlisted ordinal within the count would
//!   give another. It needs no beat from any joiner. In the same CAS it drops the listed names
//!   beyond the count: the interim's removal half, today's class at scale-in, until the leave
//!   (step 6) replaces it (Addendum 14). The model's `Pin`.
//! - **a listed instance without a boot id** (the step 4 interim's write) stamps its own: the
//!   upgrade, one CAS. A listed instance with another boot id (a restart, C13) replaces it when its
//!   previous beat is young (R1a, in place) and otherwise removes its listing and then joins; both
//!   only under R3. The model's `Decide`.
//! - **a joiner** (unlisted, its ordinal within the count, R3 met) joins when nothing is uncovered
//!   (R2): no leaf's owner over the document's ring differs from its owner over the ring with the
//!   joiner. One CAS adds it with its boot id, against the version the coverage was computed
//!   from; the view with itself is published with no await after the CAS's answer (C5). Past
//!   [`JOIN_PHASE`] from its first pass that found something uncovered, it joins anyway: a WARN
//!   names each uncovered leaf and its owner, counted `joins_unpinned` (E5). The model's
//!   `JoinStep` and the join fallback.
//! - **an older cell's document** (no `members`, C17) is listed with today's ordinal set by a
//!   member of today's ring.
//! - **a member found unstamped** (C18, the join ruling, Addendum 33): a boot that has read itself
//!   listed with its own boot id, and then reads its listing unstamped (a write that dropped its
//!   boot id, or one that dropped `members` and was relisted by another), stamps its own boot id
//!   back in place. It never unlists itself on C13's boot-time verdict after its decision. Not
//!   covered: an undecided boot (C13), a listing with another boot id (R1a), a name removed
//!   outright (a joiner), a member beyond the count.
//!
//! A committed CAS answers the document as written at the version the store gave, so the pass
//! reads the document once (C4's read-once); a lost CAS, or one whose answer was lost, is followed
//! by one read that finds what is stored.

use std::collections::HashMap;
use std::time::Duration;

use object_store::UpdateVersion;

use super::record::{Candidates, ordinal, ordinal_key, within_count};
use super::{AppState, FleetDocument, FleetRepository, Member, OverrideEntry, Overrides};

/// How long a joiner waits for its holders' pins before it joins anyway (§3 of the ruling):
/// `PROGRESS_DEADLINE`, 139 s. A member that progresses pins within a pass; one that stops leaves
/// the ring within the deadline.
pub(crate) const JOIN_PHASE: Duration =
    Duration::from_millis(super::planning::PROGRESS_DEADLINE_MS);

/// A pin's stamp (C7): no return window and never a recent override, so nothing waits on it. An
/// entry stamped 0 is a pin; `validate_overrides` accepts it as any entry (the join ruling §5).
pub(super) const PIN_MS: i64 = 0;

/// The clock skew between two members' hosts that C13's young bound allows (the join ruling §4).
/// The tree bounds no skew elsewhere for beat ages (the watch caps' 30 s is another clock's), so
/// the fleet judges beat ages across hosts' clocks with this allowance (NEXT-WORK records it).
const SKEW_ALLOWANCE: Duration = Duration::from_secs(2);

/// Passes a listed member's beats may be live with its entry unstamped before it is logged and
/// counted (the join ruling §2): one to read its previous beat, one to read its own first beat
/// back, and one more for a pass abandoned at its deadline.
pub(super) const UNSTAMPED_PASSES: u32 = 3;

/// The leaves' owners map of a document: prefix to the instance its entry names.
type Entries = HashMap<String, OverrideEntry>;

/// The effective owner of `prefix` over `ring` and `entries`: an entry wins only while it names a
/// ring member (`OwnershipService::effective_owner`'s rule over a given ring).
pub(super) fn owner(prefix: &str, ring: &[String], entries: &Entries) -> Option<String> {
    let named = entries.get(prefix).map(|entry| &entry.to);
    if let Some(to) = named.filter(|to| ring.contains(to)) {
        return Some(to.clone());
    }
    (!ring.is_empty()).then(|| ring[crate::ownership::ring_pick(prefix, ring)].clone())
}

/// R2: the leaves whose owner over `ring` with `joiner` differs from their owner over `ring`.
/// An entry naming the joiner is uncovered until C6(c) drops it.
pub(super) fn uncovered(
    leaves: &[String],
    ring: &[String],
    joiner: &str,
    entries: &Entries,
) -> Vec<String> {
    let mut with: Vec<String> = ring.to_vec();
    with.push(joiner.to_string());
    with.sort_by(|a, b| ordinal_key(a).cmp(&ordinal_key(b)));
    let differs = |leaf: &&String| owner(leaf, ring, entries) != owner(leaf, &with, entries);
    leaves.iter().filter(differs).cloned().collect()
}

/// C7: the leaves `me` pins, owners from the document (its `ring` and `entries`): each that the
/// document gives `me` and `prospective` (the ring with every unlisted ordinal within the count)
/// would give another, with no entry naming `me` already.
pub(super) fn pins(
    leaves: &[String],
    ring: &[String],
    prospective: &[String],
    me: &str,
    entries: &Entries,
) -> Vec<String> {
    let mine = |leaf: &&String| owner(leaf, ring, entries).as_deref() == Some(me);
    let pinned = |leaf: &&String| {
        entries
            .get(leaf.as_str())
            .is_some_and(|entry| entry.to == me)
    };
    let moves = |leaf: &&String| owner(leaf, prospective, entries).as_deref() != Some(me);
    let pins = leaves.iter().filter(mine).filter(|leaf| !pinned(leaf));
    pins.filter(moves).cloned().collect()
}

/// C13's young test, on the instance's previous beat read once before its first beat: in place
/// iff it is not draining or withdrawn and its age is under 12 s: `RING_LIVENESS` (30 s) less the
/// document deadline (10 s), the first beat's wait and one beat period (2 s each), one reader's
/// tick (2 s: a reader learns the new listing at its next publication and judges the old beat's
/// age on its own clock until then) and [`SKEW_ALLOWANCE`] (2 s). No beat, or an unreadable one: a
/// joiner (the join ruling §4).
pub(super) fn in_place(previous: Option<&super::Heartbeat>, now_ms: i64) -> bool {
    let Some(beat) = previous else {
        return false;
    };
    let margin = super::repository::DOCUMENT_DEADLINE
        + 2 * super::heartbeat::PERIOD
        + super::planning::TICK_PERIOD
        + SKEW_ALLOWANCE;
    let margin_ms = i64::try_from(margin.as_millis()).unwrap_or(i64::MAX);
    let age_at_first_beat = (now_ms - beat.ts_ms).saturating_add(margin_ms);
    !beat.draining
        && beat.withdrawn.is_none()
        && age_at_first_beat < super::planning::RING_LIVENESS_MS
}

/// What this pass's facts let it do (the pure part of [`pass`]).
pub(super) struct Facts<'a> {
    pub(super) candidates: &'a Candidates,
    /// The installed topology's leaves.
    pub(super) leaves: &'a [String],
    /// R3's first two clauses: no drain and no orderly stop begun, not withdrawn.
    pub(super) may_join: bool,
    /// C13's verdict on the previous beat, once the heartbeat task read it.
    pub(super) young: Option<bool>,
    /// C18: a pass of this boot already read it listed with its own boot id.
    pub(super) member: bool,
    /// Whether [`JOIN_PHASE`] has run out since the first pass that found something uncovered.
    pub(super) phase_over: bool,
}

/// The one write a pass makes, and what it is (for the INFO line and the counters).
#[derive(Debug, PartialEq, Eq)]
pub(super) enum Write {
    /// C17: an older cell's document listed with today's ordinal set.
    Listed,
    /// The upgrade: this instance stamps its boot id on its unstamped listing.
    Stamped,
    /// C18: a member found unstamped stamps its own boot id back in place.
    Restamped,
    /// R1a: a restart in place replaces its listed boot id.
    InPlace,
    /// C13: a restart that is not young removes its listing (it joins on a later pass).
    Unlisted,
    /// C7 pins and the interim's removal half: the leaves pinned, the names unlisted.
    Pinned(Vec<String>, Vec<String>),
    /// C5: the join; `uncovered` is non-empty only for the fallback past [`JOIN_PHASE`].
    Joined { uncovered: Vec<String> },
}

/// The document this pass writes, and what it is; `None` when it writes nothing. `uncovered_now`
/// reports, for a joiner, whether anything was uncovered (the caller's [`JOIN_PHASE`] clock).
pub(super) fn plan(facts: &Facts<'_>, document: &Overrides, boot_id: &str) -> Planned {
    let c = facts.candidates;
    let me = c.me();
    let Some(listed) = document.members.as_deref() else {
        let lists = c.today().iter().any(|name| name == me);
        let first = ordinal_members(c.count(), me, boot_id);
        let next = lists.then(|| with_members(document, first));
        return Planned::write(next, Write::Listed);
    };
    let mine = listed.iter().find(|member| member.name == me);
    match mine.map(|member| member.boot_id.as_deref()) {
        // An unstamped listing of its own name is another boot's (the join ruling §2): never
        // membership by the listing alone.
        Some(None) if facts.member && within_count(me, c.count()) => {
            let next = restamped(document, me, Some(boot_id));
            Planned::write(Some(next), Write::Restamped)
        }
        Some(None) => Planned::of_restart(facts, document, boot_id, Write::Stamped),
        Some(Some(_)) if c.is_member(Some(listed)) => Planned::of_member(facts, document, listed),
        Some(Some(_)) => Planned::of_restart(facts, document, boot_id, Write::InPlace),
        None => Planned::of_joiner(facts, document, listed, boot_id),
    }
}

/// A pass's plan: the document to write and why, and whether a joiner found anything uncovered.
#[derive(Default)]
pub(super) struct Planned {
    pub(super) next: Option<(Overrides, Write)>,
    pub(super) uncovered_now: bool,
}

impl Planned {
    fn write(next: Option<Overrides>, why: Write) -> Self {
        Self {
            next: next.map(|next| (next, why)),
            uncovered_now: false,
        }
    }

    /// A member's pass: its pins (C7) and the interim's removal half, in one CAS if either.
    fn of_member(facts: &Facts<'_>, document: &Overrides, listed: &[Member]) -> Self {
        let c = facts.candidates;
        let ring = c.ring(Some(listed));
        let mut prospective = ring.clone();
        let unlisted = ordinal(c.count()).into_iter();
        prospective
            .extend(unlisted.filter(|name| listed.iter().all(|member| &member.name != name)));
        prospective.sort_by(|a, b| ordinal_key(a).cmp(&ordinal_key(b)));
        let pinned = pins(facts.leaves, &ring, &prospective, c.me(), &document.entries);
        let beyond: Vec<String> = listed
            .iter()
            .filter(|member| !within_count(&member.name, c.count()))
            .map(|member| member.name.clone())
            .collect();
        if pinned.is_empty() && beyond.is_empty() {
            return Self::default();
        }
        let mut next = document.clone();
        for leaf in &pinned {
            let entry = OverrideEntry {
                to: c.me().to_string(),
                ms: PIN_MS,
            };
            next.entries.insert(leaf.clone(), entry);
        }
        let kept = listed
            .iter()
            .filter(|member| !beyond.contains(&member.name));
        next.members = Some(kept.cloned().collect());
        Self::write(Some(next), Write::Pinned(pinned, beyond))
    }

    /// A boot listed with another boot id, or unstamped (C13, R1a): in place when young and R3's
    /// first two clauses hold (`in_place`: `Write::InPlace`, or `Write::Stamped` for an unstamped
    /// listing); its own beat cannot be among them, since this boot writes none before its
    /// decision (the join ruling, Addendum 2). Else its listing removed; nothing while undecided.
    fn of_restart(facts: &Facts<'_>, document: &Overrides, boot_id: &str, in_place: Write) -> Self {
        let me = facts.candidates.me();
        match facts.young {
            Some(true) if facts.may_join => {
                Self::write(Some(restamped(document, me, Some(boot_id))), in_place)
            }
            Some(false) => Self::write(Some(restamped(document, me, None)), Write::Unlisted),
            _ => Self::default(),
        }
    }

    /// A joiner's pass (C5, R2, R3): the join when nothing is uncovered, or past [`JOIN_PHASE`].
    fn of_joiner(
        facts: &Facts<'_>,
        document: &Overrides,
        listed: &[Member],
        boot_id: &str,
    ) -> Self {
        let c = facts.candidates;
        if !r3(facts) || !within_count(c.me(), c.count()) {
            return Self::default();
        }
        let ring = c.ring(Some(listed));
        let uncovered = uncovered(facts.leaves, &ring, c.me(), &document.entries);
        if !uncovered.is_empty() && !facts.phase_over {
            return Self {
                next: None,
                uncovered_now: true,
            };
        }
        let mut next = document.clone();
        let joiner = Member {
            name: c.me().to_string(),
            boot_id: Some(boot_id.to_string()),
        };
        // In the ordinal order, so the document reads as every ring is built (Addendum 23 §3).
        let members = next.members.get_or_insert_with(Vec::new);
        let key = ordinal_key(c.me());
        let at = members.partition_point(|member| ordinal_key(&member.name) <= key);
        members.insert(at, joiner);
        let uncovered_now = !uncovered.is_empty();
        Self {
            next: Some((next, Write::Joined { uncovered })),
            uncovered_now,
        }
    }
}

/// R3: no drain and no stop begun, not withdrawn, and the pass read this instance's own beat of
/// this boot.
fn r3(facts: &Facts<'_>) -> bool {
    facts.may_join && facts.candidates.read_own_beat()
}

/// C17's first listing: today's ordinal set, this writer stamped with its own boot id (it is a
/// member of today's ring, and must not then decide itself out of the record it wrote, C13), every
/// other name unstamped until its own process stamps it.
fn ordinal_members(count: u64, me: &str, boot_id: &str) -> Vec<Member> {
    let entry = |name: String| {
        let boot_id = (name == me).then(|| boot_id.to_string());
        Member { name, boot_id }
    };
    ordinal(count).into_iter().map(entry).collect()
}

fn with_members(document: &Overrides, members: Vec<Member>) -> Overrides {
    let mut next = document.clone();
    next.members = Some(members);
    next
}

/// `document` with `me`'s listing given `boot_id`, or removed when `None`.
fn restamped(document: &Overrides, me: &str, boot_id: Option<&str>) -> Overrides {
    let mut next = document.clone();
    let members = next.members.get_or_insert_with(Vec::new);
    match boot_id {
        Some(boot) => members
            .iter_mut()
            .filter(|member| member.name == me)
            .for_each(|member| member.boot_id = Some(boot.to_string())),
        None => members.retain(|member| member.name != me),
    }
    next
}

/// The pass's membership step, under the revalidation lock the tick holds: the document `read` at
/// `version`, as this pass's view must be published from (see the module).
pub(super) async fn pass(
    state: &AppState,
    candidates: &Candidates,
    read: (Overrides, Option<UpdateVersion>),
) -> (Overrides, Option<UpdateVersion>) {
    let standing = state.fleet.standing();
    standing.next_join_pass();
    if candidates.is_member(read.0.members.as_deref()) {
        standing.note_member();
    }
    let leaves = state.shards.prefixes().to_vec();
    let facts = Facts {
        candidates,
        leaves: &leaves,
        may_join: !standing.drain_begun() && state.withdrawal().is_none(),
        young: standing.boot_young(),
        member: standing.decided_member(),
        phase_over: standing
            .join_phase_since()
            .is_some_and(|since| since.elapsed() >= JOIN_PHASE),
    };
    let planned = plan(&facts, &read.0, &state.runtime.identity.boot_id);
    unstamped(state, candidates, &read.0);
    standing
        .note_uncovered(planned.uncovered_now && within_count(candidates.me(), candidates.count()));
    // Released on what this pass read, not on what it wrote: a decision written now is read back,
    // and the beat released, at the next pass.
    released(state, candidates, &read.0);
    match planned.next {
        Some((next, why)) => committed(state, read, next, &why).await,
        None => read,
    }
}

/// Release this boot's beats once a document this pass read shows its decision (the join ruling,
/// Addendum 2: landed and read back): its name not listed, or listed with its own boot id. A
/// listing of another boot, or an unstamped one, keeps the first beat held.
fn released(state: &AppState, candidates: &Candidates, document: &Overrides) {
    if may_beat(candidates, document) {
        state.fleet.standing().allow_beats();
    }
}

/// Whether `document` shows this boot's decision (see [`released`]).
pub(super) fn may_beat(candidates: &Candidates, document: &Overrides) -> bool {
    let me = candidates.me();
    let listed = document
        .members
        .iter()
        .flatten()
        .any(|member| member.name == me);
    !listed || candidates.is_member(document.members.as_deref())
}

/// The listed peers whose entry is still unstamped while their beats are live: each past
/// [`UNSTAMPED_PASSES`] such passes is logged at WARN and counted (the join ruling §2).
fn unstamped(state: &AppState, candidates: &Candidates, document: &Overrides) {
    let live_unstamped: Vec<String> = document
        .members
        .iter()
        .flatten()
        .filter(|member| member.boot_id.is_none() && member.name != candidates.me())
        .filter(|member| {
            candidates
                .beat_of(&member.name)
                .is_some_and(|(_, live)| live)
        })
        .map(|member| member.name.clone())
        .collect();
    for name in state.fleet.standing().note_unstamped(live_unstamped) {
        let why = "fleet member listed unstamped while its beats are live";
        tracing::warn!(member = %name, passes = UNSTAMPED_PASSES, "{why}");
    }
}

/// The CAS of `next` over `read`: the document as written at the version the store answered, or,
/// when the CAS was lost or its answer was, one read of what is stored (C4).
async fn committed(
    state: &AppState,
    read: (Overrides, Option<UpdateVersion>),
    next: Overrides,
    why: &Write,
) -> (Overrides, Option<UpdateVersion>) {
    let Ok(body) = serde_json::to_vec(&next) else {
        return read;
    };
    let written = state
        .fleet
        .replaced_document(FleetDocument::Overrides, body, read.1.clone())
        .await;
    reported(state, &read.0, written.is_some(), why);
    answered(&state.fleet, read, next, written).await
}

/// What a CAS of `next` over `read` leaves, as the store answered it (`written`): `next` at the
/// version the store answered, with no read of it; or, when the CAS was lost or its answer carried
/// neither an ETag nor a version, one read of what is stored, else `read` (C4).
async fn answered(
    fleet: &FleetRepository,
    read: (Overrides, Option<UpdateVersion>),
    next: Overrides,
    written: Option<UpdateVersion>,
) -> (Overrides, Option<UpdateVersion>) {
    match written {
        Some(written) if written.e_tag.is_some() || written.version.is_some() => {
            (next, Some(written))
        }
        _ => fleet.read_overrides().await.unwrap_or(read),
    }
}

/// The INFO line for `why`, the joiner's `fleet joined` line, and the fallback's WARN and count
/// (E5's `joins_unpinned`).
fn reported(state: &AppState, read: &Overrides, written: bool, why: &Write) {
    tracing::info!(written, ?why, "fleet membership");
    let Write::Joined { uncovered } = why else {
        return;
    };
    if !written {
        return;
    }
    joined(state, uncovered.len());
    if uncovered.is_empty() {
        return;
    }
    for leaf in uncovered {
        let to = read.entries.get(leaf).map(|entry| entry.to.as_str());
        tracing::warn!(shard = %leaf, ?to, "fleet join past JOIN_PHASE: a leaf was left uncovered");
    }
    state.fleet.standing().note_join_unpinned();
}

/// The joiner's line a leg reads (bench/ramp/join_report.py, package 6's reader of the `fleet
/// joined` line), once its join CAS is written: `rise_ms`, the time from the count rise to the
/// join on the store's clock (see [`since_rise_ms`]); `passes`, this boot's membership passes to
/// the join, the join's included; `uncovered`, the leaves the join left unpinned past
/// `JOIN_PHASE`. The leg reads the new member's first served leaf from its first `shard open's
/// indexes loaded` line after this one.
fn joined(state: &AppState, uncovered: usize) {
    let standing = state.fleet.standing();
    let passes = standing.note_joined();
    let rise = standing.doc_times().desired_computed_ms;
    let rise_ms = since_rise_ms(rise, crate::shard::now_ms());
    tracing::info!(rise_ms, passes, uncovered, "fleet joined");
}

/// Milliseconds from desired.json's `computed_at_ms` to `now_ms`, or -1 when no desired document
/// was read. desired.json is written only when the count changes, so a joiner within the count
/// reads the rise that admitted it (or a later change, which then bounds it from below).
fn since_rise_ms(computed_at_ms: Option<i64>, now_ms: i64) -> i64 {
    computed_at_ms.map_or(-1, |at| now_ms.saturating_sub(at))
}

#[cfg(test)]
mod tests;
