//! The fleet tick's member reads, without a listing at every pass (E7). The
//! tick used to list `fleet/` and `routers/` at every pass: two Class A
//! requests every two seconds on every server, each priced as ten GETs
//! (`docs/TIGRIS-404-COST.md` §3), and 89% of an idle server's LISTs.
//!
//! One read GETs, by name: the desired document, then the heartbeats of the
//! ordinals its count names (`streams-1..=count`), of every member a read
//! found before, and of this runtime (`Members::join`). An ordinal within the
//! count that boots is read by the first pass that starts after its first
//! beat lands, as when the tick listed, and so is an ordinal a count rise
//! names, in the pass that reads the rise. A member no count names (above the
//! count, or not an ordinal) is found by a listing of `fleet/` on the first
//! read and on every `LIST_EVERY`th read after it, and is read at every read
//! from then on. An absent ordinal within the count answers a billed 404 at
//! every read; when more than `MAX_PROBES` of them are absent the read lists
//! instead, so the members that do not exist never cost more than the
//! listing the tick made before. A read that would name more documents than
//! the namespace may hold (`MAX_OBJECTS`) lists it instead too, so a read
//! fails on that bound only when its listing finds the namespace over it.
//! Router reports carry names the servers are not configured with
//! (`ROUTER_NAME`): a listing on the same cadence discovers them, and the
//! reads between GET the reports it found.
//!
//! Every GET of a document read before carries the ETag of that copy: one
//! that has not changed answers 304 Not Modified, which the store does not
//! bill, and the copy is read again. Each reader judges a heartbeat's age on
//! its own clock, so a member that stops beating ages out of the ring
//! exactly as before. A drain's poll and the operator view still list the
//! heartbeats (`read_population`): they are rare, and they must see every
//! beat in the namespace.
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Mutex;

use bytes::Bytes;
use futures_util::{StreamExt, TryStreamExt};
use object_store::{GetOptions, ObjectStore, path::Path as ObjPath};

use super::{
    DESIRED_DOC, DOCUMENT_DEADLINE, Desired, Heartbeat, MAX_DOCUMENT_BYTES, MAX_MEMBERS,
    MAX_OBJECT_BYTES, MAX_TOTAL_BYTES, OVERRIDES_DOC, URLS_DOC, validate_desired,
};

/// A listing rediscovers a population on this many reads: once a minute at
/// the tick's period.
const LIST_EVERY: u32 = 30;
/// The absent ordinals one read asks for by name. Each answers a billed 404
/// and a listing is priced as ten GETs, so from ten on a listing finds the
/// members that exist at no greater cost.
const MAX_PROBES: usize = 9;
/// The heartbeat namespace's objects: its members and the three
/// coordination documents beside them. A listing past it fails, and a read
/// that would name more by name lists instead.
const MAX_OBJECTS: usize = MAX_MEMBERS + 3;
/// Member documents read at once.
const READS_AT_ONCE: usize = 8;

/// A document as last read: its body, and the ETag naming that body.
#[derive(Clone)]
struct Held {
    e_tag: Option<String>,
    body: Bytes,
}

/// The documents one read found present, by path.
type Copies = BTreeMap<ObjPath, Held>;

/// Whom the tick reads, and the copies it revalidates.
#[derive(Default)]
pub(super) struct Members {
    /// This runtime's own heartbeat, once it has joined.
    own: Mutex<Option<ObjPath>>,
    /// The desired document as last read: its count names the ordinals.
    desired: Mutex<Option<Held>>,
    heartbeats: Mutex<Found>,
    routers: Mutex<Found>,
}

/// What a population's reads found, and when it was last listed.
#[derive(Default)]
struct Found {
    copies: Copies,
    /// Reads since the last listing; `None` until a listing has succeeded.
    since_listing: Option<u32>,
}

impl Found {
    /// Whether the next read lists: the first does, and so does every
    /// `LIST_EVERY`th after the last listing.
    fn listing_due(&self) -> bool {
        self.since_listing
            .is_none_or(|reads| reads + 1 >= LIST_EVERY)
    }

    /// A read found `copies`, by a listing when `listed`.
    fn found(&mut self, copies: Copies, listed: bool) {
        self.copies = copies;
        self.since_listing = if listed {
            Some(0)
        } else {
            self.since_listing.map(|reads| reads + 1)
        };
    }
}

impl Members {
    /// The tick reads `instance`'s heartbeat, this runtime's own, at every
    /// read.
    pub(super) fn join(&self, instance: &str) {
        if let Ok(mut own) = self.own.lock() {
            *own = Some(heartbeat_path(instance));
        }
    }

    /// Every member heartbeat present, in path order.
    pub(super) async fn heartbeats(
        &self,
        store: &dyn ObjectStore,
    ) -> anyhow::Result<Vec<Heartbeat>> {
        let own = self.own.lock().map(|own| own.clone()).unwrap_or_default();
        let desired = self
            .desired
            .lock()
            .map(|desired| desired.clone())
            .unwrap_or_default();
        let (held, due) = snapshot(&self.heartbeats);
        let (desired, read, listed) = within_deadline(async {
            let desired = read_one(
                store,
                ObjPath::from(DESIRED_DOC),
                desired,
                MAX_DOCUMENT_BYTES,
            )
            .await?
            .map(|(_, copy)| copy);
            let named = named(counted(desired.as_ref()), own);
            let absent = named.iter().filter(|path| !held.contains_key(*path));
            let probes = absent.count();
            let by_name: BTreeSet<ObjPath> = held.keys().cloned().chain(named).collect();
            let listed = due || probes > MAX_PROBES || by_name.len() > MAX_OBJECTS;
            let paths = if listed {
                list(store, "fleet", MAX_OBJECTS, is_heartbeat).await?
            } else {
                by_name
            };
            Ok((desired, read_each(store, paths, &held).await?, listed))
        })
        .await?;
        let beats = parsed(&read)?;
        if let Ok(mut held) = self.desired.lock() {
            *held = desired;
        }
        found(&self.heartbeats, read, listed);
        Ok(beats)
    }

    /// Every router report present, in path order: those a listing
    /// discovered, listed again on every `LIST_EVERY`th read.
    pub(super) async fn router_reports(
        &self,
        store: &dyn ObjectStore,
    ) -> anyhow::Result<Vec<serde_json::Value>> {
        let (held, listed) = snapshot(&self.routers);
        let read = within_deadline(async {
            let paths = if listed {
                list(store, "routers", MAX_MEMBERS, |_| true).await?
            } else {
                held.keys().cloned().collect()
            };
            read_each(store, paths, &held).await
        })
        .await?;
        let reports = parsed(&read)?;
        found(&self.routers, read, listed);
        Ok(reports)
    }
}

/// The copies a population holds, and whether its next read lists.
fn snapshot(population: &Mutex<Found>) -> (Copies, bool) {
    population
        .lock()
        .map(|found| (found.copies.clone(), found.listing_due()))
        .unwrap_or_else(|_| (Copies::new(), true))
}

/// A population's read found `copies`, by a listing when `listed`.
fn found(population: &Mutex<Found>, copies: Copies, listed: bool) {
    if let Ok(mut population) = population.lock() {
        population.found(copies, listed);
    }
}

/// `instance`'s heartbeat document.
fn heartbeat_path(instance: &str) -> ObjPath {
    ObjPath::from(format!("fleet/{instance}.json"))
}

/// The ordinals the desired document counts: its count when it is present
/// and valid, else the first, which every ring holds. The tick refuses an
/// invalid document itself before it publishes a view
/// (`read_desired_state`), so naming fewer members then changes nothing.
fn counted(desired: Option<&Held>) -> u64 {
    desired
        .and_then(|copy| serde_json::from_slice::<Desired>(&copy.body).ok())
        .filter(|doc| validate_desired(doc).is_ok())
        .map_or(1, |doc| doc.count)
}

/// The heartbeats a read names: `streams-1..=count`, and `own`.
fn named(count: u64, own: Option<ObjPath>) -> BTreeSet<ObjPath> {
    (1..=count)
        .map(|ordinal| heartbeat_path(&format!("streams-{ordinal}")))
        .chain(own)
        .collect()
}

/// Whether a document under `fleet/` is a member's heartbeat: a JSON
/// document other than the three coordination documents.
fn is_heartbeat(path: &ObjPath) -> bool {
    let path = path.as_ref();
    path.ends_with(".json") && ![DESIRED_DOC, OVERRIDES_DOC, URLS_DOC].contains(&path)
}

async fn within_deadline<T>(read: impl Future<Output = anyhow::Result<T>>) -> anyhow::Result<T> {
    tokio::time::timeout(DOCUMENT_DEADLINE, read)
        .await
        .map_err(|_| anyhow::anyhow!("fleet population read timed out"))?
}

/// The documents under `prefix` that `keep` names, each within the member
/// ceiling; a listing of more than `max_objects` objects fails.
async fn list(
    store: &dyn ObjectStore,
    prefix: &str,
    max_objects: usize,
    keep: fn(&ObjPath) -> bool,
) -> anyhow::Result<BTreeSet<ObjPath>> {
    let (mut paths, mut listed) = (BTreeSet::new(), 0usize);
    let mut listing = store.list(Some(&ObjPath::from(prefix)));
    while let Some(meta) = listing.try_next().await? {
        listed += 1;
        anyhow::ensure!(
            listed <= max_objects,
            "fleet population exceeds {max_objects} objects"
        );
        if keep(&meta.location) {
            anyhow::ensure!(
                meta.size <= MAX_OBJECT_BYTES as u64,
                "fleet document too large: {}",
                meta.location
            );
            paths.insert(meta.location);
        }
    }
    Ok(paths)
}

/// Each of `paths` present (one absent is left out), revalidating each copy
/// `held` names. An incomplete read is an error: the controller then keeps
/// its prior ownership view. `paths` is a listing's, or names no more than
/// `MAX_OBJECTS` documents.
async fn read_each(
    store: &dyn ObjectStore,
    paths: BTreeSet<ObjPath>,
    held: &Copies,
) -> anyhow::Result<Copies> {
    let mut reads = futures_util::stream::iter(paths)
        .map(|path| {
            let copy = held.get(&path).cloned();
            read_one(store, path, copy, MAX_OBJECT_BYTES)
        })
        .buffered(READS_AT_ONCE);
    let (mut read, mut bytes) = (Copies::new(), 0usize);
    while let Some(found) = reads.try_next().await? {
        let Some((path, copy)) = found else {
            continue;
        };
        bytes += copy.body.len();
        anyhow::ensure!(
            bytes <= MAX_TOTAL_BYTES,
            "fleet population exceeds byte budget"
        );
        read.insert(path, copy);
    }
    Ok(read)
}

/// One document of at most `max_bytes`: `held` again when the store answers
/// that it has not changed, `None` when it is absent.
async fn read_one(
    store: &dyn ObjectStore,
    path: ObjPath,
    held: Option<Held>,
    max_bytes: usize,
) -> anyhow::Result<Option<(ObjPath, Held)>> {
    let options = GetOptions {
        if_none_match: held.as_ref().and_then(|copy| copy.e_tag.clone()),
        ..GetOptions::default()
    };
    let got = match store.get_opts(&path, options).await {
        Ok(got) => got,
        Err(object_store::Error::NotModified { .. }) => {
            let copy = held.ok_or_else(|| anyhow::anyhow!("fleet document {path} never read"))?;
            return Ok(Some((path, copy)));
        }
        Err(object_store::Error::NotFound { .. }) => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    anyhow::ensure!(
        got.meta.size <= max_bytes as u64,
        "fleet document too large: {path}"
    );
    let e_tag = got.meta.e_tag.clone();
    let mut chunks = got.into_stream();
    let mut body = Vec::new();
    while let Some(chunk) = chunks.try_next().await? {
        anyhow::ensure!(
            body.len().saturating_add(chunk.len()) <= max_bytes,
            "fleet document exceeds byte budget: {path}"
        );
        body.extend_from_slice(&chunk);
    }
    let body = Bytes::from(body);
    Ok(Some((path, Held { e_tag, body })))
}

/// Every copy's document; one that does not parse fails the read, so a
/// corrupt member never reads as an absent one.
fn parsed<T: serde::de::DeserializeOwned>(read: &Copies) -> anyhow::Result<Vec<T>> {
    read.iter()
        .map(|(path, copy)| {
            serde_json::from_slice(&copy.body)
                .map_err(|error| anyhow::anyhow!("invalid fleet member {path}: {error}"))
        })
        .collect()
}

#[cfg(test)]
mod tests;
