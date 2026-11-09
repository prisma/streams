//! The member reads: whom the tick reads (the ordinals the desired count it
//! reads names, the members read before, and itself), that a member whose
//! document has not changed answers 304 and is read again, that absent
//! members never cost more than a listing, that the members no count names
//! and the router reports are listed once every `LIST_EVERY` reads, and that
//! a read over a ceiling, of a corrupt member or past its deadline fails
//! instead of reading as an absent member.
#![cfg(test)]
use super::{Heartbeat, LIST_EVERY, MAX_OBJECTS, Members};
use object_store::{ObjectStore, ObjectStoreExt, PutPayload, memory::InMemory, path::Path};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

/// One GET as the store saw it: its path, whether it carried an ETag, and
/// its answer.
type Get = (String, bool, u16);

/// An in-memory store that records each GET and each listing's prefix, and
/// whose GETs never answer while `hang` is set.
#[derive(Debug, Default)]
struct Recording {
    inner: InMemory,
    gets: Mutex<Vec<Get>>,
    lists: Mutex<Vec<String>>,
    hang: AtomicBool,
}

impl std::fmt::Display for Recording {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "recording-fleet")
    }
}

#[async_trait::async_trait]
impl ObjectStore for Recording {
    async fn put_opts(
        &self,
        path: &Path,
        body: PutPayload,
        opts: object_store::PutOptions,
    ) -> object_store::Result<object_store::PutResult> {
        self.inner.put_opts(path, body, opts).await
    }
    async fn put_multipart_opts(
        &self,
        path: &Path,
        opts: object_store::PutMultipartOptions,
    ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
        self.inner.put_multipart_opts(path, opts).await
    }
    async fn get_opts(
        &self,
        path: &Path,
        opts: object_store::GetOptions,
    ) -> object_store::Result<object_store::GetResult> {
        if self.hang.load(Ordering::SeqCst) {
            std::future::pending::<()>().await;
        }
        let conditional = opts.if_none_match.is_some();
        let got = self.inner.get_opts(path, opts).await;
        let status = match &got {
            Ok(_) => 200,
            Err(object_store::Error::NotModified { .. }) => 304,
            Err(object_store::Error::NotFound { .. }) => 404,
            Err(_) => 500,
        };
        self.gets
            .lock()
            .unwrap()
            .push((path.to_string(), conditional, status));
        got
    }
    fn delete_stream(
        &self,
        paths: futures_util::stream::BoxStream<'static, object_store::Result<Path>>,
    ) -> futures_util::stream::BoxStream<'static, object_store::Result<Path>> {
        self.inner.delete_stream(paths)
    }
    fn list(
        &self,
        prefix: Option<&Path>,
    ) -> futures_util::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>>
    {
        let listed = prefix.map(ToString::to_string).unwrap_or_default();
        self.lists.lock().unwrap().push(listed);
        self.inner.list(prefix)
    }
    async fn list_with_delimiter(
        &self,
        prefix: Option<&Path>,
    ) -> object_store::Result<object_store::ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }
    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        opts: object_store::CopyOptions,
    ) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, opts).await
    }
}

impl Recording {
    /// The GETs since the last call but the drain marker's, in path order,
    /// and the listings: the reads the tests of listing, probing and
    /// ceilings describe (the cadence's test reads the marker too).
    fn asked(&self) -> (Vec<Get>, Vec<String>) {
        let (mut gets, lists) = self.asked_all();
        gets.retain(|(path, ..)| path != DRAINS);
        (gets, lists)
    }

    /// Every GET since the last call, in path order, and the listings.
    fn asked_all(&self) -> (Vec<Get>, Vec<String>) {
        let mut gets = std::mem::take(&mut *self.gets.lock().unwrap());
        gets.sort();
        (gets, std::mem::take(&mut *self.lists.lock().unwrap()))
    }

    async fn write(&self, path: &str, body: String) {
        self.inner
            .put(&Path::from(path), PutPayload::from(body))
            .await
            .unwrap();
    }

    /// The desired document, counting `count` members.
    async fn desired(&self, count: u64) {
        let body = format!(r#"{{"count":{count},"epoch":1,"reason":"t","computed_at_ms":0}}"#);
        self.write(DESIRED, body).await;
    }

    async fn beat(&self, instance: &str, ts_ms: i64) {
        let body = format!(
            r#"{{"instance":"{instance}","ts_ms":{ts_ms},"rps":0.0,"owned_shards":[],"draining":false}}"#
        );
        self.write(&format!("fleet/{instance}.json"), body).await;
    }
}

const DESIRED: &str = "fleet/desired.json";
const DRAINS: &str = "fleet/drains.json";

/// `instance`'s heartbeat document.
fn hb(instance: &str) -> String {
    format!("fleet/{instance}.json")
}

fn get(path: &str, conditional: bool, status: u16) -> Get {
    (path.to_string(), conditional, status)
}

/// Each router report's number, in the order read.
fn numbers(reports: &[serde_json::Value]) -> Vec<i64> {
    reports
        .iter()
        .map(|report| report["n"].as_i64().unwrap())
        .collect()
}

fn names(beats: &[Heartbeat]) -> Vec<(&str, i64)> {
    beats
        .iter()
        .map(|beat| (beat.instance.as_str(), beat.ts_ms))
        .collect()
}

/// The tick reads the ordinals the desired count names and itself by name;
/// its first read lists, so it asks for no member that is absent, and its
/// own first beat, landing after that listing, is read at the next read.
/// Each copy it holds is revalidated, so an unchanged member answers 304 and
/// reads as before; an ordinal that boots is read at the next read, and a
/// document that disappears is absent from it on.
#[tokio::test(crate = "crate::test_rt")]
async fn the_tick_names_the_counted_ordinals_and_itself_and_revalidates_each_copy() {
    let store = Recording::default();
    let members = Members::reading_every_member();
    members.join("streams-9");
    store.desired(3).await;
    for instance in ["streams-1", "streams-2"] {
        store.beat(instance, 1).await;
    }
    let beats = members.heartbeats(&store).await.unwrap();
    assert_eq!(names(&beats), [("streams-1", 1), ("streams-2", 1)]);
    let first = vec![
        get(DESIRED, false, 200),
        get(&hb("streams-1"), false, 200),
        get(&hb("streams-2"), false, 200),
    ];
    assert_eq!(store.asked(), (first, vec!["fleet".to_string()]));

    store.beat("streams-9", 1).await;
    let present = [("streams-1", 1), ("streams-2", 1), ("streams-9", 1)];
    let beats = members.heartbeats(&store).await.unwrap();
    assert_eq!(names(&beats), present, "its own beat is named");
    let own = vec![
        get(DESIRED, true, 304),
        get(&hb("streams-1"), true, 304),
        get(&hb("streams-2"), true, 304),
        get(&hb("streams-3"), false, 404),
        get(&hb("streams-9"), false, 200),
    ];
    assert_eq!(store.asked(), (own, vec![]));

    let beats = members.heartbeats(&store).await.unwrap();
    assert_eq!(
        names(&beats),
        present,
        "an unchanged member reads as its copy"
    );
    let unchanged = vec![
        get(DESIRED, true, 304),
        get(&hb("streams-1"), true, 304),
        get(&hb("streams-2"), true, 304),
        get(&hb("streams-3"), false, 404),
        get(&hb("streams-9"), true, 304),
    ];
    assert_eq!(store.asked(), (unchanged, vec![]));

    store.beat("streams-2", 2).await;
    store.beat("streams-3", 2).await;
    let beats = members.heartbeats(&store).await.unwrap();
    assert_eq!(
        names(&beats),
        [
            ("streams-1", 1),
            ("streams-2", 2),
            ("streams-3", 2),
            ("streams-9", 1)
        ]
    );
    let changed = vec![
        get(DESIRED, true, 304),
        get(&hb("streams-1"), true, 304),
        get(&hb("streams-2"), true, 200),
        get(&hb("streams-3"), false, 200),
        get(&hb("streams-9"), true, 304),
    ];
    assert_eq!(store.asked(), (changed, vec![]));

    store
        .inner
        .delete(&Path::from(hb("streams-2")))
        .await
        .unwrap();
    for conditional in [true, false] {
        let beats = members.heartbeats(&store).await.unwrap();
        assert_eq!(
            names(&beats),
            [("streams-1", 1), ("streams-3", 2), ("streams-9", 1)]
        );
        let (gets, lists) = store.asked();
        assert!(lists.is_empty());
        assert!(gets.contains(&get(&hb("streams-2"), conditional, 404)));
    }
}

/// The repository the tick reads through names the instance that joined it
/// (`FleetRepository::join`, as its heartbeat starts): an instance no count
/// names whose first beat lands after the first listing is read by the next
/// read, not by the next listing.
#[tokio::test(crate = "crate::test_rt")]
async fn the_repository_reads_the_instance_that_joined_it() {
    let store = Arc::new(Recording::default());
    let shared: Arc<dyn ObjectStore> = store.clone();
    let repository = super::super::FleetRepository::new(Some(shared));
    repository.join("streams-9");
    store.desired(1).await;
    store.beat("streams-1", 1).await;
    let beats = repository.read_heartbeat_set().await.unwrap();
    assert_eq!(names(&beats), [("streams-1", 1)]);
    assert_eq!(store.asked().1, ["fleet"]);

    store.beat("streams-9", 1).await;
    let beats = repository.read_heartbeat_set().await.unwrap();
    assert_eq!(
        names(&beats),
        [("streams-1", 1), ("streams-9", 1)],
        "the joined instance is read by the read after its first beat (a read between the every-member reads keeps streams-1's copy)"
    );
    let joined = vec![get(DESIRED, true, 304), get(&hb("streams-9"), false, 200)];
    assert_eq!(store.asked(), (joined, vec![]));
}

/// The tick's read is the beats the topology policy reads from memory; a peek is not.
#[tokio::test(crate = "crate::test_rt")]
async fn the_ticks_read_is_cached_for_the_topology_policy_and_a_peek_is_not() {
    let store = Arc::new(Recording::default());
    let shared: Arc<dyn ObjectStore> = store.clone();
    let repository = super::super::FleetRepository::new(Some(shared));
    store.desired(2).await;
    store.beat("streams-1", 1).await;
    assert!(repository.standing().cached_beats().is_none());
    repository.read_heartbeat_set().await.unwrap();
    let (_, cached) = repository.standing().cached_beats().unwrap();
    assert_eq!(names(&cached), [("streams-1", 1)]);
    store.beat("streams-2", 1).await;
    assert_eq!(repository.peek_heartbeat_set().await.unwrap().len(), 2);
    let (_, cached) = repository.standing().cached_beats().unwrap();
    assert_eq!(
        names(&cached),
        [("streams-1", 1)],
        "a peek leaves the cache"
    );
    repository.read_heartbeat_set().await.unwrap();
    let (_, cached) = repository.standing().cached_beats().unwrap();
    assert_eq!(names(&cached), [("streams-1", 1), ("streams-2", 1)]);
}

/// The reads of desired.json and the overrides leave their times for the topology policy.
#[tokio::test(crate = "crate::test_rt")]
async fn the_fleet_documents_reads_leave_their_times_for_the_topology_policy() {
    let store = Arc::new(Recording::default());
    let shared: Arc<dyn ObjectStore> = store.clone();
    let repository = super::super::FleetRepository::new(Some(shared.clone()));
    repository.read_desired_state().await.unwrap();
    repository.read_overrides().await.unwrap();
    let times = repository.standing().doc_times();
    assert_eq!(
        (times.desired_computed_ms, times.youngest_override_ms),
        (None, None)
    );
    assert!(times.age_ms.is_some(), "both absent documents were read");
    let desired = r#"{"count":1,"reason":"seed","epoch":1,"computed_at_ms":1234}"#;
    let overrides =
        r#"{"entries":{"0":{"to":"streams-2","ms":50},"1":{"to":"streams-3","ms":70}}}"#;
    for (path, body) in [
        ("fleet/desired.json", desired),
        ("fleet/overrides.json", overrides),
    ] {
        shared
            .put(&object_store::path::Path::from(path), body.into())
            .await
            .unwrap();
    }
    repository.read_desired_state().await.unwrap();
    repository.read_overrides().await.unwrap();
    let times = repository.standing().doc_times();
    assert_eq!(
        (times.desired_computed_ms, times.youngest_override_ms),
        (Some(1_234), Some(70))
    );
}

/// The count a read finds names its members in that same read: a rise is
/// read at once, never a pass later. A smaller count, or a document that does
/// not validate (no count, zero, beyond the 4096-member budget), names only
/// the first ordinal, and the members read before are still read.
#[tokio::test(crate = "crate::test_rt")]
async fn the_count_a_read_finds_names_its_members_in_that_read() {
    let store = Recording::default();
    let members = Members::reading_every_member();
    store.beat("streams-1", 1).await;
    let beats = members.heartbeats(&store).await.unwrap();
    assert_eq!(names(&beats), [("streams-1", 1)]);
    let absent = vec![get(DESIRED, false, 404), get(&hb("streams-1"), false, 200)];
    assert_eq!(store.asked(), (absent, vec!["fleet".to_string()]));

    store.desired(3).await;
    for instance in ["streams-2", "streams-3"] {
        store.beat(instance, 1).await;
    }
    let three = [("streams-1", 1), ("streams-2", 1), ("streams-3", 1)];
    let beats = members.heartbeats(&store).await.unwrap();
    assert_eq!(
        names(&beats),
        three,
        "the read that finds the rise reads it"
    );
    let rise = vec![
        get(DESIRED, false, 200),
        get(&hb("streams-1"), true, 304),
        get(&hb("streams-2"), false, 200),
        get(&hb("streams-3"), false, 200),
    ];
    assert_eq!(store.asked(), (rise, vec![]));

    for count in [Some(1), Some(0), Some(4097), None] {
        match count {
            Some(count) => store.desired(count).await,
            None => store.write(DESIRED, "{}".into()).await,
        }
        let beats = members.heartbeats(&store).await.unwrap();
        assert_eq!(names(&beats), three, "count {count:?}");
        let smaller = vec![
            get(DESIRED, true, 200),
            get(&hb("streams-1"), true, 304),
            get(&hb("streams-2"), true, 304),
            get(&hb("streams-3"), true, 304),
        ];
        assert_eq!(store.asked(), (smaller, vec![]), "count {count:?}");
    }
}

/// A count far beyond the members that exist (4,096 with four booted) costs
/// one listing per read, never 4,092 billed 404s: up to nine absent
/// ordinals are asked for by name, and from ten on the read lists instead.
/// No count read before is kept as a high-water mark, and a count that falls
/// back to the members that exist is read by name again, without a listing.
#[tokio::test(crate = "crate::test_rt")]
async fn absent_members_never_cost_more_than_a_listing() {
    let store = Recording::default();
    let members = Members::reading_every_member();
    members.join("streams-1");
    store.desired(4096).await;
    for ordinal in 1..=4 {
        store.beat(&format!("streams-{ordinal}"), 1).await;
    }
    let revalidated = |desired: u16| {
        let mut gets = vec![get(DESIRED, true, desired)];
        gets.extend((1..=4).map(|ordinal| get(&hb(&format!("streams-{ordinal}")), true, 304)));
        gets
    };
    assert_eq!(members.heartbeats(&store).await.unwrap().len(), 4);
    store.asked();
    for _ in 0..2 {
        assert_eq!(members.heartbeats(&store).await.unwrap().len(), 4);
        assert_eq!(store.asked(), (revalidated(304), vec!["fleet".to_string()]));
    }

    store.desired(13).await;
    assert_eq!(members.heartbeats(&store).await.unwrap().len(), 4);
    let mut probed = revalidated(200);
    probed.extend((5..=13).map(|ordinal| get(&hb(&format!("streams-{ordinal}")), false, 404)));
    probed.sort();
    assert_eq!(store.asked(), (probed, vec![]), "nine absent are asked for");

    store.desired(14).await;
    assert_eq!(members.heartbeats(&store).await.unwrap().len(), 4);
    let listed = (revalidated(200), vec!["fleet".to_string()]);
    assert_eq!(store.asked(), listed, "ten absent are listed");

    store.desired(4).await;
    assert_eq!(members.heartbeats(&store).await.unwrap().len(), 4);
    let fallen = (revalidated(200), vec![]);
    assert_eq!(
        store.asked(),
        fallen,
        "a fallen count asks for no absent ordinal"
    );
}

/// A member no count names (above the count, or not an ordinal) is found by
/// the listing on the `LIST_EVERY`th read after the last, and is read at
/// every read from then on; a listing reads no coordination document and no
/// object that is not JSON as a member.
#[tokio::test(crate = "crate::test_rt")]
async fn a_member_no_count_names_is_found_by_the_next_listing() {
    let store = Recording::default();
    let members = Members::reading_every_member();
    members.join("streams-1");
    store.desired(1).await;
    for path in ["fleet/overrides.json", "fleet/urls.json"] {
        store.write(path, "{}".into()).await;
    }
    store.write("fleet/notes.txt", "notes".into()).await;
    store.beat("streams-1", 1).await;
    members.heartbeats(&store).await.unwrap();
    assert_eq!(store.asked().1, ["fleet"]);

    store.beat("streams-7", 1).await;
    store.beat("streams", 1).await;
    for _ in 1..LIST_EVERY {
        let beats = members.heartbeats(&store).await.unwrap();
        assert_eq!(names(&beats), [("streams-1", 1)]);
        let between = vec![get(DESIRED, true, 304), get(&hb("streams-1"), true, 304)];
        assert_eq!(store.asked(), (between, vec![]));
    }

    let found = [("streams-1", 1), ("streams-7", 1), ("streams", 1)];
    let beats = members.heartbeats(&store).await.unwrap();
    assert_eq!(names(&beats), found);
    let listed = vec![
        get(DESIRED, true, 304),
        get(&hb("streams-1"), true, 304),
        get(&hb("streams-7"), false, 200),
        get(&hb("streams"), false, 200),
    ];
    assert_eq!(store.asked(), (listed, vec!["fleet".to_string()]));
    let beats = members.heartbeats(&store).await.unwrap();
    assert_eq!(names(&beats), found);
    let held = vec![
        get(DESIRED, true, 304),
        get(&hb("streams-1"), true, 304),
        get(&hb("streams-7"), true, 304),
        get(&hb("streams"), true, 304),
    ];
    assert_eq!(store.asked(), (held, vec![]));
}

/// The router reports are listed on the first read and on every
/// `LIST_EVERY`th after it; the reads between revalidate the reports the
/// listing found, so a new report waits for the next listing and a vanished
/// one is dropped at once.
#[tokio::test(crate = "crate::test_rt")]
async fn router_reports_are_listed_once_every_list_every_reads() {
    let store = Recording::default();
    let members = Members::reading_every_member();
    store
        .write("routers/router-1.json", r#"{"n":1}"#.into())
        .await;
    let reports = members.router_reports(&store).await.unwrap();
    assert_eq!(numbers(&reports), [1]);
    let first = vec![get("routers/router-1.json", false, 200)];
    assert_eq!(store.asked(), (first, vec!["routers".to_string()]));

    store
        .write("routers/router-2.json", r#"{"n":2}"#.into())
        .await;
    for _ in 1..LIST_EVERY {
        let reports = members.router_reports(&store).await.unwrap();
        assert_eq!(numbers(&reports), [1]);
        let between = vec![get("routers/router-1.json", true, 304)];
        assert_eq!(store.asked(), (between, vec![]));
    }

    let reports = members.router_reports(&store).await.unwrap();
    assert_eq!(numbers(&reports), [1, 2]);
    let listed = vec![
        get("routers/router-1.json", true, 304),
        get("routers/router-2.json", false, 200),
    ];
    assert_eq!(store.asked(), (listed, vec!["routers".to_string()]));

    store
        .inner
        .delete(&Path::from("routers/router-1.json"))
        .await
        .unwrap();
    let vanished = vec![
        get("routers/router-1.json", true, 404),
        get("routers/router-2.json", true, 304),
    ];
    let dropped = vec![get("routers/router-2.json", true, 304)];
    for gets in [vanished, dropped] {
        let reports = members.router_reports(&store).await.unwrap();
        assert_eq!(numbers(&reports), [2]);
        assert_eq!(store.asked(), (gets, vec![]));
    }
}

/// A member at the document ceiling is read and one byte over it fails the
/// read; a population at the byte ceiling is read and one member more fails
/// it; a corrupt member fails it too. None reads as an absent member, and the
/// copies held before survive a failed read.
#[tokio::test(crate = "crate::test_rt")]
async fn ceilings_and_corruption_fail_the_read_instead_of_dropping_a_member() {
    let store = Recording::default();
    let members = Members::reading_every_member();
    store.desired(65).await;
    let padded = |instance: &str, bytes: usize| {
        let doc = format!(
            r#"{{"instance":"{instance}","ts_ms":1,"rps":0.0,"owned_shards":[],"draining":false}}"#
        );
        format!("{doc}{}", " ".repeat(bytes - doc.len()))
    };
    for ordinal in 1..=64 {
        let instance = format!("streams-{ordinal}");
        let body = padded(&instance, super::MAX_OBJECT_BYTES);
        store.write(&format!("fleet/{instance}.json"), body).await;
    }
    assert_eq!(members.heartbeats(&store).await.unwrap().len(), 64);
    store.beat("streams-65", 1).await;
    let error = members.heartbeats(&store).await.unwrap_err();
    assert!(error.to_string().contains("byte budget"), "{error}");
    store
        .inner
        .delete(&Path::from("fleet/streams-65.json"))
        .await
        .unwrap();

    let body = padded("streams-2", super::MAX_OBJECT_BYTES + 1);
    store.write("fleet/streams-2.json", body).await;
    let error = members.heartbeats(&store).await.unwrap_err();
    assert!(error.to_string().contains("too large"), "{error}");
    store.write("fleet/streams-2.json", "corrupt".into()).await;
    let error = members.heartbeats(&store).await.unwrap_err();
    assert!(
        error.to_string().contains("fleet/streams-2.json"),
        "{error}"
    );

    store.asked();
    let body = padded("streams-2", super::MAX_OBJECT_BYTES);
    store.write("fleet/streams-2.json", body).await;
    assert_eq!(members.heartbeats(&store).await.unwrap().len(), 64);
    let (gets, _) = store.asked();
    assert!(
        gets.contains(&get("fleet/streams-1.json", true, 304)),
        "the copies held before the failed reads are revalidated"
    );
}

/// A read asks for no more documents than the heartbeat namespace holds
/// (its members and the three coordination documents): a read that would
/// name one document more lists the namespace instead and reads what is
/// there, at every read while that holds; only a listing of one object more
/// fails, and the read after the namespace is back within its bound succeeds.
#[tokio::test(crate = "crate::test_rt")]
async fn a_read_asks_for_no_more_documents_than_the_namespace_holds() {
    let store = Recording::default();
    let members = Members::reading_every_member();
    store.desired(1).await;
    for index in 0..MAX_OBJECTS - 1 {
        store.beat(&format!("peer-{index:04}"), 1).await;
    }
    let beats = members.heartbeats(&store).await.unwrap();
    assert_eq!(beats.len(), MAX_OBJECTS - 1);
    assert_eq!(store.asked().1, ["fleet"], "the first read lists");
    let beats = members.heartbeats(&store).await.unwrap();
    assert_eq!(beats.len(), MAX_OBJECTS - 1);
    let (gets, lists) = store.asked();
    assert!(
        lists.is_empty(),
        "naming the namespace's last object reads by name"
    );
    assert!(gets.contains(&get(&hb("streams-1"), false, 404)));
    store.desired(2).await;
    for _ in 0..2 {
        let beats = members.heartbeats(&store).await.unwrap();
        assert_eq!(beats.len(), MAX_OBJECTS - 1);
        let (gets, lists) = store.asked();
        assert_eq!(lists, ["fleet"], "naming one document more lists instead");
        assert!(
            !gets
                .iter()
                .any(|(path, ..)| path.starts_with("fleet/streams-"))
        );
    }
    store.beat("streams-1", 1).await;
    for members in [&members, &Members::reading_every_member()] {
        let error = members.heartbeats(&store).await.unwrap_err();
        assert!(error.to_string().contains("4100 objects"), "{error}");
    }
    store
        .inner
        .delete(&Path::from(hb("peer-0000")))
        .await
        .unwrap();
    let beats = members.heartbeats(&store).await.unwrap();
    assert_eq!(beats.len(), MAX_OBJECTS - 1);
    assert_eq!(store.asked().1, ["fleet", "fleet", "fleet"]);
}

/// A namespace that grows past its bound between listings, by members no
/// count names, fails no read before the next listing is due: those reads
/// GET the documents the last listing found. The due listing fails, and so
/// does every read after it, each listing again, until the namespace is
/// back within its bound.
#[tokio::test(crate = "crate::test_rt")]
async fn a_namespace_over_its_bound_fails_every_read_from_its_next_listing() {
    let store = Recording::default();
    let members = Members::reading_every_member();
    store.desired(1).await;
    store.beat("streams-1", 1).await;
    for index in 0..MAX_OBJECTS - 3 {
        store.beat(&format!("peer-{index:04}"), 1).await;
    }
    let beats = members.heartbeats(&store).await.unwrap();
    assert_eq!(beats.len(), MAX_OBJECTS - 2);
    assert_eq!(store.asked().1, ["fleet"], "the first read lists");

    store.beat("late-1", 1).await;
    store.beat("late-2", 1).await;
    for _ in 1..LIST_EVERY {
        let beats = members.heartbeats(&store).await.unwrap();
        assert_eq!(beats.len(), MAX_OBJECTS - 2);
        let (gets, lists) = store.asked();
        assert!(lists.is_empty(), "a read before the due listing names");
        assert!(!gets.iter().any(|(path, ..)| path.contains("late-")));
    }
    for _ in 0..2 {
        let error = members.heartbeats(&store).await.unwrap_err();
        assert!(error.to_string().contains("4100 objects"), "{error}");
        assert_eq!(store.asked().1, ["fleet"], "every read from then on lists");
    }

    store.inner.delete(&Path::from(hb("late-2"))).await.unwrap();
    let beats = members.heartbeats(&store).await.unwrap();
    assert_eq!(beats.len(), MAX_OBJECTS - 1);
    assert_eq!(store.asked().1, ["fleet"]);
}

/// A member read the store never answers fails at the document deadline, so
/// the pass that asked keeps its prior view.
#[tokio::test(start_paused = true, crate = "crate::test_rt")]
async fn a_read_the_store_never_answers_fails_at_the_document_deadline() {
    let store = Arc::new(Recording::default());
    let members = Members::reading_every_member();
    store.beat("streams-1", 1).await;
    store.write("routers/router-1.json", "{}".into()).await;
    store.hang.store(true, Ordering::SeqCst);
    let started = tokio::time::Instant::now();
    let error = members.heartbeats(store.as_ref()).await.unwrap_err();
    assert!(error.to_string().contains("timed out"), "{error}");
    assert_eq!(started.elapsed(), super::DOCUMENT_DEADLINE);
    let error = members.router_reports(store.as_ref()).await.unwrap_err();
    assert!(error.to_string().contains("timed out"), "{error}");
}

/// Each read's GETs by kind: (the desired document, the drain marker, this
/// runtime's own beat, every other member's beat).
fn by_kind(gets: &[Get], own: &str) -> (usize, usize, usize, usize) {
    let count = |path: &str| gets.iter().filter(|(asked, ..)| asked == path).count();
    let (desired, drains, mine) = (count(DESIRED), count(DRAINS), count(&hb(own)));
    (desired, drains, mine, gets.len() - desired - drains - mine)
}

impl Recording {
    /// The drain marker, naming `entries` (instance, boot).
    async fn drains(&self, entries: &[(&str, &str)]) {
        let entries: Vec<String> = entries
            .iter()
            .map(|(instance, boot)| {
                format!(r#"{{"instance":"{instance}","boot_id":"{boot}","at_ms":1}}"#)
            })
            .collect();
        self.write(DRAINS, format!(r#"{{"entries":[{}]}}"#, entries.join(",")))
            .await;
    }

    /// `instance`'s beat under `boot`, draining or not.
    async fn beat_of(&self, instance: &str, ts_ms: i64, boot: &str, draining: bool) {
        let body = format!(
            r#"{{"instance":"{instance}","ts_ms":{ts_ms},"rps":0.0,"owned_shards":[],"draining":{draining},"boot_id":"{boot}"}}"#
        );
        self.write(&hb(instance), body).await;
    }
}

/// `instance`'s beat in `beats`: (`ts_ms`, draining).
fn beat_in(beats: &[Heartbeat], instance: &str) -> Option<(i64, bool)> {
    beats
        .iter()
        .find(|beat| beat.instance == instance)
        .map(|beat| (beat.ts_ms, beat.draining))
}

/// The peers' heartbeats are read on one read in three (item 8): liveness is
/// judged at 30 s and every scale decision sustains 20 s, so a peer read
/// every 6 s at the tick's period loses nothing. Over six reads (12 s) a
/// fleet of eight peers costs 16 peer GETs, not 48; the desired document,
/// the drain marker (an unchanged copy: 304, unbilled) and this runtime's own
/// beat are read at every read. A peer's newer beat is read at the next peer
/// read, and a counted ordinal not yet held is read by the read that finds
/// it, whichever read that is.
#[tokio::test(crate = "crate::test_rt")]
async fn peers_are_read_on_one_read_in_three_and_the_rest_at_every_read() {
    let store = Recording::default();
    let members = Members::default();
    members.join("streams-1");
    store.desired(9).await;
    store.drains(&[]).await;
    for ordinal in 1..=9 {
        store.beat(&format!("streams-{ordinal}"), 1).await;
    }
    let mut counts = Vec::new();
    for read in 1..=6 {
        if read == 2 {
            store.beat("streams-5", 2).await;
        }
        let beats = members.heartbeats(&store).await.unwrap();
        let fresh = beat_in(&beats, "streams-5").map(|(ts_ms, _)| ts_ms);
        counts.push((by_kind(&store.asked_all().0, "streams-1"), fresh));
    }
    assert_eq!(
        counts,
        [
            ((1, 1, 1, 8), Some(1)),
            ((1, 1, 1, 0), Some(1)),
            ((1, 1, 1, 0), Some(1)),
            ((1, 1, 1, 8), Some(2)),
            ((1, 1, 1, 0), Some(2)),
            ((1, 1, 1, 0), Some(2)),
        ],
        "(desired, marker, own, peers) GETs and streams-5's beat, per read"
    );

    store.desired(10).await;
    store.beat("streams-10", 1).await;
    let beats = members.heartbeats(&store).await.unwrap();
    assert_eq!(beats.len(), 10, "the rise's ordinal is read at once");
    let gets = store.asked_all().0;
    assert_eq!(by_kind(&gets, "streams-1"), (1, 1, 1, 9), "a peer read");
    store.desired(11).await;
    store.beat("streams-11", 1).await;
    let beats = members.heartbeats(&store).await.unwrap();
    assert_eq!(beats.len(), 11);
    assert_eq!(
        store.asked_all().0,
        vec![
            get(DESIRED, true, 200),
            get(DRAINS, true, 304),
            get(&hb("streams-1"), true, 304),
            get(&hb("streams-11"), false, 200),
        ],
        "between peer reads: the desired document, the marker, its own beat and the new ordinal"
    );
}

/// A planned drain's peers read its draining beat within a tick (the
/// drain's bound counts one tick period): a runtime the drain marker names
/// is read at every read, between the every-member reads too, until a
/// draining beat of the named boot is held, and from then on with the other
/// peers.
#[tokio::test(crate = "crate::test_rt")]
async fn a_runtime_the_drain_marker_names_is_read_at_every_read_until_its_draining_beat() {
    let store = Recording::default();
    let members = Members::default();
    members.join("streams-1");
    store.desired(3).await;
    store.drains(&[]).await;
    store.beat_of("streams-1", 1, "a", false).await;
    store.beat_of("streams-2", 1, "b", false).await;
    store.beat_of("streams-3", 1, "c", false).await;
    members.heartbeats(&store).await.unwrap();
    store.asked_all();

    store.beat_of("streams-2", 2, "b", true).await;
    store.drains(&[("streams-2", "b")]).await;
    let beats = members.heartbeats(&store).await.unwrap();
    assert_eq!(
        beat_in(&beats, "streams-2"),
        Some((2, true)),
        "the read after the announcement holds the draining beat"
    );
    assert_eq!(
        store.asked_all().0,
        vec![
            get(DESIRED, true, 304),
            get(DRAINS, true, 200),
            get(&hb("streams-1"), true, 304),
            get(&hb("streams-2"), true, 200),
        ],
        "a read between the every-member reads, which reads the announced runtime"
    );
    members.heartbeats(&store).await.unwrap();
    assert_eq!(
        by_kind(&store.asked_all().0, "streams-1"),
        (1, 1, 1, 0),
        "once its draining beat is held, it waits for the every-member read"
    );

    store.drains(&[("streams-3", "c")]).await;
    let beats = members.heartbeats(&store).await.unwrap();
    assert_eq!(by_kind(&store.asked_all().0, "streams-1"), (1, 1, 1, 2));
    assert_eq!(beat_in(&beats, "streams-3"), Some((1, false)));
    store.beat_of("streams-3", 2, "c", true).await;
    let beats = members.heartbeats(&store).await.unwrap();
    assert_eq!(
        store.asked_all().0,
        vec![
            get(DESIRED, true, 304),
            get(DRAINS, true, 304),
            get(&hb("streams-1"), true, 304),
            get(&hb("streams-3"), true, 200),
        ],
        "an announced runtime whose draining beat has not landed is read again at the next read"
    );
    assert_eq!(beat_in(&beats, "streams-3"), Some((2, true)));
}

/// An entry names a boot: one whose instance now beats under another boot
/// is stale, so it reads nothing extra and never makes a live member read
/// as draining; whether a member drains is only its own beat's word.
#[tokio::test(crate = "crate::test_rt")]
async fn a_stale_drain_marker_entry_never_makes_a_live_member_read_as_draining() {
    let store = Recording::default();
    let members = Members::default();
    members.join("streams-1");
    store.desired(2).await;
    store.drains(&[("streams-2", "old-boot")]).await;
    store.beat_of("streams-1", 1, "a", false).await;
    store.beat_of("streams-2", 1, "new-boot", false).await;
    for read in 1..=3 {
        let beats = members.heartbeats(&store).await.unwrap();
        assert_eq!(
            beat_in(&beats, "streams-2"),
            Some((1, false)),
            "read {read}"
        );
        let peers = by_kind(&store.asked_all().0, "streams-1").3;
        assert_eq!(
            peers,
            usize::from(read == 1),
            "read {read}: only the every-member read"
        );
    }
}
