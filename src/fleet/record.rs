//! The membership of record (§22, GATE-DECISION-s22-addendum-2026-11-19 C1-C4, C17) and the
//! interim rule that keeps it until the join and the leave land
//! (GATE-DECISION-meas-full-forward-wedge-2026-11-19, Addendum 14).
//!
//! The overrides document's `members` names the ring of record. Once the join lands, a view's ring
//! is the listed members that qualify by the reader's beats (`planning::member`: live, progressing,
//! not withdrawn, not draining), computed from the one document read that gives the view its
//! entries (C1, C2), and no listed member qualifying, the listed members unfiltered (C3).
//!
//! **The interim** (Addendum 14): until the join lands, the tick keeps `members` equal to the
//! ordinal set within the count it read (`streams-1`..=`streams-count`), by one CAS of the overrides
//! document from [`listed`], its one write path, written only by an instance that today's rule puts
//! in its own ring (C17). Over a document listing that set, the listed members a reader's beats
//! qualify, in ordinal order, else all of them, are exactly today's rule
//! ([`Candidates::today`]); over any other document the interim's ring is today's rule too. So in
//! the interim every ring is `today()` over the pass's candidates, whatever document it is paired
//! with, and no code computes a ring from `members` (PR #173's survivor `record.rs:105:29`, that
//! arm replaced by `today()`, was equivalent and the arm is removed). The join brings its own C1
//! arm.
//! The join replaces the add half; removal stays today's rule until the leave lands.

use object_store::UpdateVersion;

use super::{FleetDocument, FleetRepository, Member, Overrides};

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
    fleet: &FleetRepository,
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
    let written = fleet
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
        _ => fleet.read_overrides().await.unwrap_or(read),
    }
}

#[cfg(test)]
mod tests {
    use super::{Candidates, ordinal};
    use crate::fleet::Member;

    fn names(list: &[&str]) -> Vec<String> {
        list.iter().map(|name| (*name).to_string()).collect()
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

    /// A store over memory that counts the overrides document's reads and, with `bare`, answers a
    /// write with neither an ETag nor a version (no store this crate runs on does; the seam for
    /// `listed`'s guard).
    #[derive(Debug)]
    struct Answers {
        inner: std::sync::Arc<dyn object_store::ObjectStore>,
        bare: bool,
        reads: std::sync::atomic::AtomicU64,
    }

    impl std::fmt::Display for Answers {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "answers")
        }
    }

    #[async_trait::async_trait]
    impl object_store::ObjectStore for Answers {
        async fn put_opts(
            &self,
            path: &object_store::path::Path,
            body: object_store::PutPayload,
            opts: object_store::PutOptions,
        ) -> object_store::Result<object_store::PutResult> {
            let mut written = self.inner.put_opts(path, body, opts).await?;
            if self.bare {
                (written.e_tag, written.version) = (None, None);
            }
            Ok(written)
        }
        async fn put_multipart_opts(
            &self,
            path: &object_store::path::Path,
            opts: object_store::PutMultipartOptions,
        ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
            self.inner.put_multipart_opts(path, opts).await
        }
        async fn get_opts(
            &self,
            path: &object_store::path::Path,
            opts: object_store::GetOptions,
        ) -> object_store::Result<object_store::GetResult> {
            if path.as_ref() == "fleet/overrides.json" {
                self.reads.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            }
            self.inner.get_opts(path, opts).await
        }
        fn delete_stream(
            &self,
            paths: futures_util::stream::BoxStream<
                'static,
                object_store::Result<object_store::path::Path>,
            >,
        ) -> futures_util::stream::BoxStream<'static, object_store::Result<object_store::path::Path>>
        {
            self.inner.delete_stream(paths)
        }
        fn list(
            &self,
            prefix: Option<&object_store::path::Path>,
        ) -> futures_util::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>>
        {
            self.inner.list(prefix)
        }
        async fn list_with_delimiter(
            &self,
            prefix: Option<&object_store::path::Path>,
        ) -> object_store::Result<object_store::ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }
        async fn copy_opts(
            &self,
            from: &object_store::path::Path,
            to: &object_store::path::Path,
            opts: object_store::CopyOptions,
        ) -> object_store::Result<()> {
            self.inner.copy_opts(from, to, opts).await
        }
    }

    /// `listed` on a fresh cell of one over a store answering `bare`: the version it answers, the
    /// stored document's ETag, and the overrides reads it made.
    async fn listed_over(bare: bool) -> (Option<String>, Option<String>, u64) {
        use object_store::ObjectStoreExt as _;
        let inner: std::sync::Arc<dyn object_store::ObjectStore> =
            std::sync::Arc::new(object_store::memory::InMemory::new());
        let answers = std::sync::Arc::new(Answers {
            inner: inner.clone(),
            bare,
            reads: std::sync::atomic::AtomicU64::new(0),
        });
        let store: std::sync::Arc<dyn object_store::ObjectStore> = answers.clone();
        let fleet = crate::fleet::FleetRepository::new(Some(store));
        let candidates = Candidates::new(1, names(&["streams-1"]));
        let read = (crate::fleet::Overrides::default(), None);
        let (written, version) = super::listed(&fleet, "streams-1", &candidates, read).await;
        assert_eq!(written.members.map(|listed| listed.len()), Some(1));
        let path = object_store::path::Path::from("fleet/overrides.json");
        let stored = inner.head(&path).await.unwrap().e_tag;
        let reads = answers.reads.load(std::sync::atomic::Ordering::SeqCst);
        (version.and_then(|version| version.e_tag), stored, reads)
    }

    /// A committed write is published at the version the store answered, with no read of the
    /// document (PR #173's survivors `record.rs:163`, the guard as `false` and its `||` as `&&`:
    /// the memory store answers an ETag and no version).
    #[tokio::test(crate = "crate::test_rt")]
    async fn a_committed_members_write_answers_its_version_without_a_read() {
        let (answered, stored, reads) = listed_over(false).await;
        assert!(stored.is_some());
        assert_eq!((answered, reads), (stored, 0));
    }

    /// A write the store answered with neither an ETag nor a version is read back, so the next CAS
    /// is anchored on the stored version (the survivor guard as `true` would answer none).
    #[tokio::test(crate = "crate::test_rt")]
    async fn a_members_write_answered_without_a_version_is_read_back() {
        let (answered, stored, reads) = listed_over(true).await;
        assert!(stored.is_some());
        assert_eq!((answered, reads), (stored, 1));
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
    /// interim's two (a co-location writes no entry, GATE-DECISION-s22-join-2026-11-20 Addendum 13),
    /// which the join replaces with whole publications. A fifth or a third fails here first.
    #[test]
    fn the_ownership_view_has_exactly_its_known_production_writers() {
        let expected = [
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
