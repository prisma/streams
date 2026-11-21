//! The membership of record (§22, GATE-DECISION-s22-addendum-2026-11-19 C1-C4, C17) and the
//! interim rule that keeps it until the join and the leave land
//! (GATE-DECISION-meas-full-forward-wedge-2026-11-19, Addendum 14).
//!
//! The overrides document's `members` names the ring of record. A view's ring is the listed
//! members that qualify by the reader's beats (`planning::member`: live, progressing, not withdrawn,
//! not draining), in the document's order, computed from the one document read that gives the
//! view its entries (C1, C2). No listed member qualifying, the listed members within the count are
//! the ring unfiltered (C3's fallback, today's bootstrap).
//!
//! **The interim** (Addendum 14): until the join lands, the tick keeps `members` equal to the
//! ordinal set within the count it read (`streams-1`..=`streams-count`), by one CAS of the overrides
//! document from [`listed`], its one write path, written only by an instance that today's rule puts
//! in its own ring (C17). Every reader then filters the listed members by its own beats, so its
//! ring is exactly the set today's rule computes, at every tick and on every member. A document that
//! does not list this pass's ordinal set (an older cell's, C17, or one written for a count another
//! member read) gives today's rule. The join replaces the add half; removal stays today's rule until
//! the leave lands.

use object_store::UpdateVersion;

use super::{AppState, FleetDocument, Member, Overrides};

/// The ordinal names within `count`: `streams-1`..=`streams-count`, one at least.
pub(super) fn ordinal(count: u64) -> Vec<String> {
    (1..=count.max(1))
        .map(|index| format!("streams-{index}"))
        .collect()
}

/// Whether `members` names exactly `names`, in order; boot ids are not read (the interim, §2.1).
fn names_exactly(members: &[Member], names: &[String]) -> bool {
    let mut listed: Vec<&String> = members.iter().map(|member| &member.name).collect();
    let mut wanted: Vec<&String> = names.iter().collect();
    listed.sort();
    wanted.sort();
    listed == wanted
}

/// The members a document must list for `want` (the ordinal set within the count), or `None` when
/// it already names exactly that set. Names are compared as a set, and an entry whose name stays
/// keeps its boot id exactly as read (GATE-DECISION-s22-join-2026-11-20 §1 item 3), so an interim
/// writer and a join writer in one cell never undo each other's stamps.
fn relisted(members: Option<&[Member]>, want: Vec<String>) -> Option<Vec<Member>> {
    let read = members.unwrap_or_default();
    if members.is_some() && names_exactly(read, &want) {
        return None;
    }
    let kept = |name: String| {
        let stamped = read.iter().find(|member| member.name == name).cloned();
        stamped.unwrap_or(Member {
            name,
            boot_id: None,
        })
    };
    Some(want.into_iter().map(kept).collect())
}

/// One pass's candidates (C3): every instance whose beat qualifies on the reader's clock, the
/// reader kept, with the count the pass read. The ring is made from them and a document.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(super) struct Candidates {
    count: u64,
    /// Sorted, without duplicates.
    names: Vec<String>,
}

impl Candidates {
    pub(super) fn new(count: u64, mut names: Vec<String>) -> Self {
        names.sort();
        names.dedup();
        Self { count, names }
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

    /// The ring of a document listing `members` (C1, C3): the listed members that are candidates,
    /// in the listed order, else the listed members within the count unfiltered. In the interim a
    /// document that does not list this pass's ordinal set gives [`Candidates::today`] (C17,
    /// Addendum 14); the join's commit removes that arm.
    pub(super) fn ring(&self, members: Option<&[Member]>) -> Vec<String> {
        match members {
            Some(listed) if names_exactly(listed, &ordinal(self.count)) => {
                // The listed names in ordinal order, then those that are candidates.
                let is_listed = |name: &String| listed.iter().any(|member| &member.name == name);
                let names: Vec<String> =
                    ordinal(self.count).into_iter().filter(is_listed).collect();
                let active: Vec<String> = names
                    .iter()
                    .filter(|name| self.contains(name))
                    .cloned()
                    .collect();
                if active.is_empty() { names } else { active }
            }
            _ => self.today(),
        }
    }
}

/// The interim's one write path (Addendum 14), under the revalidation lock the tick holds: the
/// document `read` at `version` as this pass's view must be published from. A document that already
/// names the ordinal set within `candidates`' count (as a set, boot ids unread), or an instance
/// today's rule leaves out of its own ring (C17: only a member of today's ring writes), is
/// returned as read, unwritten. Otherwise one CAS lists that set ([`relisted`]: an entry whose
/// name stays keeps its boot id as read, a new one has none), the entries and outbox kept: a committed CAS answers the document as written at the version the store gave, so
/// the pass reads the document once, as before. A lost CAS, or a CAS whose answer was lost (C4), is
/// followed by one read that finds what is stored; an unreadable document leaves the one read,
/// whose ring is then today's rule.
pub(super) async fn listed(
    state: &AppState,
    me: &str,
    candidates: &Candidates,
    read: (Overrides, Option<UpdateVersion>),
) -> (Overrides, Option<UpdateVersion>) {
    let (document, version) = &read;
    let writes = candidates.today().iter().any(|name| name == me);
    if !writes {
        return read;
    }
    let want = ordinal(candidates.count());
    let Some(members) = relisted(document.members.as_deref(), want) else {
        return read;
    };
    let mut next = document.clone();
    next.members = Some(members);
    let Ok(body) = serde_json::to_vec(&next) else {
        return read;
    };
    let written = state
        .fleet
        .replaced_document(FleetDocument::Overrides, body, version.clone())
        .await;
    tracing::info!(
        written = written.is_some(),
        count = candidates.count(),
        "fleet members listed"
    );
    match written {
        // The document as written, at the version the store answered: no read of it, so the pass
        // reads the document once, as before step 4.
        Some(written) if written.e_tag.is_some() || written.version.is_some() => {
            (next, Some(written))
        }
        _ => state.fleet.read_overrides().await.unwrap_or(read),
    }
}

#[cfg(test)]
mod tests {
    use super::{Candidates, ordinal};
    use crate::fleet::Member;

    fn names(list: &[&str]) -> Vec<String> {
        list.iter().map(|name| (*name).to_string()).collect()
    }

    /// `list` as members of record with no boot ids, as the interim writes them.
    fn listed(list: &[String]) -> Vec<Member> {
        let entry = |name: &String| Member {
            name: name.clone(),
            boot_id: None,
        };
        list.iter().map(entry).collect()
    }

    /// The interim's invariant (Addendum 14, condition 1): over a document listing the pass's
    /// ordinal set, the ring is today's, for every candidate set, the fallback included.
    #[test]
    fn a_document_listing_the_ordinal_set_gives_todays_ring() {
        let cases: [(u64, &[&str]); 6] = [
            (1, &["streams-1"]),
            (2, &["streams-1", "streams-2"]),
            (2, &["streams-2"]),
            (3, &["streams-1", "streams-3", "streams-4"]),
            (2, &[]),
            (1, &["streams-2", "streams-3"]),
        ];
        for (count, qualified) in cases {
            let candidates = Candidates::new(count, names(qualified));
            let members = listed(&ordinal(count));
            assert_eq!(
                candidates.ring(Some(&members)),
                candidates.today(),
                "{count} {qualified:?}"
            );
        }
    }

    /// C17 and the interim: a document without `members`, or listing another count's ordinal
    /// set, gives today's rule.
    #[test]
    fn a_document_without_the_ordinal_set_gives_todays_rule() {
        let candidates = Candidates::new(2, names(&["streams-1", "streams-2", "streams-3"]));
        let today = names(&["streams-1", "streams-2"]);
        assert_eq!(candidates.ring(None), today);
        assert_eq!(candidates.ring(Some(&listed(&ordinal(3)))), today);
        assert_eq!(
            candidates.ring(Some(&listed(&names(&["streams-2"])))),
            today
        );
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

    fn stamped(name: &str, boot_id: Option<&str>) -> Member {
        Member {
            name: name.to_string(),
            boot_id: boot_id.map(str::to_string),
        }
    }

    /// GATE-DECISION-s22-join-2026-11-20 §1 item 3: a document whose names already are the
    /// ordinal set, in any order and with a stamped entry, is not written by the interim writer,
    /// so it stays byte-identical as stored.
    #[test]
    fn the_interim_writer_leaves_a_stamped_document_with_the_ordinal_names_alone() {
        let document = r#"{"entries":{},"members":[{"name":"streams-2"},{"name":"streams-1","boot_id":"b1"}]}"#;
        let read: crate::fleet::Overrides = serde_json::from_str(document).unwrap();
        assert_eq!(super::relisted(read.members.as_deref(), ordinal(2)), None);
        assert_eq!(serde_json::to_string(&read).unwrap(), document);
    }

    /// Item 3: a change of the name set keeps every stamped entry whose name stays, exactly as
    /// read; a new name has no boot id, and a name beyond the count goes.
    #[test]
    fn a_names_change_keeps_each_stamped_boot_id() {
        let one = [stamped("streams-1", Some("b1"))];
        let grown = vec![stamped("streams-1", Some("b1")), stamped("streams-2", None)];
        assert_eq!(super::relisted(Some(&one), ordinal(2)), Some(grown));
        let three = [
            stamped("streams-1", Some("b1")),
            stamped("streams-2", Some("b2")),
            stamped("streams-3", None),
        ];
        let shrunk = vec![
            stamped("streams-1", Some("b1")),
            stamped("streams-2", Some("b2")),
        ];
        assert_eq!(super::relisted(Some(&three), ordinal(2)), Some(shrunk));
        let fresh = vec![stamped("streams-1", None)];
        assert_eq!(super::relisted(None, ordinal(1)), Some(fresh));
    }

    /// §2.1: the interim reads names only; a boot id changes no ring.
    #[test]
    fn a_boot_id_changes_no_ring() {
        let candidates = Candidates::new(2, names(&["streams-1", "streams-2"]));
        let mut stamped = listed(&ordinal(2));
        stamped[0].boot_id = Some("b1".into());
        assert_eq!(
            candidates.ring(Some(&stamped)),
            candidates.ring(Some(&listed(&ordinal(2))))
        );
    }

    /// Today's ring: the qualified ordinals within the count, in ordinal order; none qualifying,
    /// every ordinal within the count.
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
    /// published whole by exactly four calls, the tick and three that take their ring from
    /// `Standing::ring_of`; and one entry is set beside the published ring by exactly the
    /// interim's three, which the join replaces with whole publications. A fifth of either fails
    /// here first.
    #[test]
    fn the_ownership_view_has_exactly_its_known_production_writers() {
        let expected = [
            ("set_override", "fleet/colocate.rs", "colocate"),
            ("set_override", "fleet/moves.rs", "evacuate"),
            ("set_override", "topology/split/birth.rs", "born"),
            ("set_view", "fleet/handoff.rs", "mirror"),
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
}
