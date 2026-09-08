//! Experimental canonical interval reuse, carved from the history block budget.
//! A hit proves a complete immutable physical scan, including negative space;
//! it is never an authorization or AEAD authentication result.
use crate::{registry::StreamDesc, shard::ShardEngine, tenant::ProjectId};
use slatedb::Db;
use std::sync::{
    Arc, Mutex, Weak,
    atomic::{AtomicU64, Ordering},
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, watch};
mod capture;
pub(crate) use capture::{Capture, CipherSpan};

pub(crate) const CAPACITY: usize = 2 * 1024 * 1024;
const METADATA: usize = 64 * 1024;
const PROJECT_CAP: usize = 256 * 1024;
const FILL: usize = 64 * 1024;
const CONTROL: usize = 512;
const ENTRIES: usize = 256;
const PROJECTS: usize = 32;

#[derive(Clone)]
pub(crate) struct SpanCache(Option<Arc<Inner>>);
struct Inner {
    state: Mutex<State>,
    bytes: Arc<Semaphore>,
}
struct State {
    entries: Box<[Option<Entry>]>,
    projects: Box<[Weak<Project>]>,
    clock: u64,
}
struct Project {
    id: ProjectId,
    generation: AtomicU64,
    bytes: Arc<Semaphore>,
}
/// Holding the Project as well as its permits prevents quota recreation while
/// an evicted slow reader still owns bytes from the old allowance.
struct Lease {
    _project: Arc<Project>,
    permits: Mutex<(OwnedSemaphorePermit, OwnedSemaphorePermit)>,
}
impl Lease {
    fn reserve(inner: &Inner, project: &Arc<Project>, bytes: usize) -> Option<Arc<Self>> {
        let global = inner
            .bytes
            .clone()
            .try_acquire_many_owned(bytes as u32)
            .ok()?;
        let tenant = project
            .bytes
            .clone()
            .try_acquire_many_owned(bytes as u32)
            .ok()?;
        Some(Arc::new(Self {
            _project: project.clone(),
            permits: Mutex::new((global, tenant)),
        }))
    }
    fn shrink(&self, bytes: usize) {
        let mut permits = self.permits.lock().unwrap_or_else(|e| e.into_inner());
        let excess = permits
            .0
            .num_permits()
            .checked_sub(bytes)
            .expect("reserved before allocation");
        drop(permits.0.split(excess));
        drop(permits.1.split(excess));
    }
}

/// Descriptor-bound physical scope. Weak owners retain opening identities, not
/// an engine or its DB lifetime. The cache backreference is weak to avoid cycles.
pub(crate) struct Scope {
    inner: Weak<Inner>,
    engine: Weak<ShardEngine>,
    part: Weak<Db>,
    project: Arc<Project>,
    generation: u64,
    route: [u8; 16],
    inc: [u8; 16],
    _charge: Arc<Lease>,
}
impl Scope {
    fn live(&self) -> bool {
        self.project.generation.load(Ordering::Acquire) == self.generation
            && self.engine.upgrade().is_some_and(|e| !e.is_closed())
            && self.part.upgrade().is_some_and(|part| {
                // A retained status receiver would also pin its manifest after
                // DB drop. Borrow a temporary subscription only while checking.
                part.subscribe().borrow().close_reason.is_none()
            })
    }
    pub(crate) fn matches_read(&self, part: &Arc<Db>, route: [u8; 16], inc: [u8; 16]) -> bool {
        self.part.as_ptr() == Arc::as_ptr(part) && self.route == route && self.inc == inc
    }
    fn same(&self, other: &Self) -> bool {
        self.engine.ptr_eq(&other.engine)
            && self.part.ptr_eq(&other.part)
            && Arc::ptr_eq(&self.project, &other.project)
            && self.generation == other.generation
            && self.route == other.route
            && self.inc == other.inc
    }
    pub(crate) fn acquire(self: &Arc<Self>, from: u64, to: u64, absorbed: u64) -> Access {
        let Some(inner) = self.inner.upgrade() else {
            return Access::Bypass;
        };
        if from >= to || to > absorbed {
            return Access::Bypass;
        }
        let mut state = inner.state.lock().unwrap_or_else(|e| e.into_inner());
        if !self.live() {
            return Access::Bypass;
        }
        // Closing an underlying DB also fences admission, even if retirement
        // did not originate in the engine. Reclaim its bounded stale slots.
        state.entries.iter_mut().for_each(|slot| {
            if slot.as_ref().is_some_and(|e| !e.scope.live()) {
                *slot = None;
            }
        });
        state.clock = state.clock.wrapping_add(1);
        let clock = state.clock;
        if let Some(entry) = state
            .entries
            .iter_mut()
            .flatten()
            .find(|e| e.from == from && e.to == to && e.scope.same(self))
        {
            entry.used = clock;
            return match &entry.value {
                Value::Ready(span) => Access::Hit(span.clone()),
                Value::Pending(signal) => Access::Wait(Waiter(signal.clone())),
            };
        }
        // Evict only this project's ready entries for a tenant limit. Global
        // pressure can evict other ready entries; held aliases retain permits.
        let data = loop {
            if let Some(lease) = Lease::reserve(&inner, &self.project, FILL - CONTROL) {
                break lease;
            }
            let tenant_full = self.project.bytes.available_permits() < FILL;
            if !state.evict(if tenant_full {
                Some(&self.project)
            } else {
                None
            }) {
                return Access::Bypass;
            }
        };
        let Some(control) = Lease::reserve(&inner, &self.project, CONTROL) else {
            return Access::Bypass;
        };
        if state.entries.iter().all(Option::is_some) {
            state.evict(None);
        }
        let Some(slot) = state.entries.iter_mut().find(|e| e.is_none()) else {
            return Access::Bypass;
        };
        let (done, _) = watch::channel(false);
        let signal = Arc::new(Signal {
            done,
            _charge: control,
        });
        *slot = Some(Entry {
            scope: self.clone(),
            from,
            to,
            used: clock,
            value: Value::Pending(signal.clone()),
        });
        Access::Fill(Capture::new(
            inner.clone(),
            self.clone(),
            signal,
            from,
            to,
            data,
        ))
    }
}

struct Entry {
    scope: Arc<Scope>,
    from: u64,
    to: u64,
    used: u64,
    value: Value,
}
enum Value {
    Pending(Arc<Signal>),
    Ready(Arc<CipherSpan>),
}
struct Signal {
    done: watch::Sender<bool>,
    _charge: Arc<Lease>,
}
impl Drop for Entry {
    fn drop(&mut self) {
        if let Value::Pending(signal) = &self.value {
            signal.done.send_replace(true);
        }
    }
}
impl State {
    fn evict(&mut self, project: Option<&Arc<Project>>) -> bool {
        let index = self
            .entries
            .iter()
            .enumerate()
            .filter_map(|(i, e)| {
                let e = e.as_ref()?;
                (matches!(e.value, Value::Ready(_))
                    && project.is_none_or(|p| Arc::ptr_eq(p, &e.scope.project)))
                .then_some((i, e.used))
            })
            .min_by_key(|(_, used)| *used)
            .map(|(i, _)| i);
        if let Some(i) = index {
            self.entries[i] = None;
            true
        } else {
            false
        }
    }
}
pub(crate) enum Access {
    Bypass,
    Hit(Arc<CipherSpan>),
    Fill(Capture),
    Wait(Waiter),
}
pub(crate) struct Waiter(Arc<Signal>);
impl Waiter {
    pub(crate) async fn wait(self) {
        let mut rx = self.0.done.subscribe();
        if !*rx.borrow_and_update() {
            let _ = rx.changed().await;
        }
    }
}
impl SpanCache {
    pub(crate) fn new(enabled: bool) -> Self {
        if !enabled {
            return Self(None);
        }
        // Fixed slots, keys, project controls and allocation headers have a
        // conservative allowance independent of traffic/cardinality.
        assert!(
            ENTRIES * std::mem::size_of::<Option<Entry>>()
                + PROJECTS
                    * (std::mem::size_of::<Weak<Project>>() + std::mem::size_of::<Project>() + 512)
                + 4096
                <= METADATA
        );
        Self(Some(Arc::new(Inner {
            state: Mutex::new(State {
                entries: (0..ENTRIES).map(|_| None).collect(),
                projects: (0..PROJECTS).map(|_| Weak::new()).collect(),
                clock: 0,
            }),
            bytes: Arc::new(Semaphore::new(CAPACITY - METADATA)),
        })))
    }
    pub(crate) fn capacity(&self) -> usize {
        if self.0.is_some() { CAPACITY } else { 0 }
    }
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn scope(
        &self,
        desc: &StreamDesc,
        engine: &Arc<ShardEngine>,
        part: &Arc<Db>,
        epoch: &[u8; 16],
        route: [u8; 16],
        inc: [u8; 16],
    ) -> Option<Arc<Scope>> {
        let inner = self.0.as_ref()?;
        let bound = desc.epoch() == *epoch
            && match &desc.segments {
                Some(map) => map.segments.iter().any(|s| {
                    desc.dynamic_segment_identity(s.seg_id) == inc && desc.segment_route(s) == route
                }),
                None => desc.storage_hash() == inc && desc.segment_route_by_id(0) == Some(route),
            };
        if !bound
            || desc.deleted
            || !engine
                .history_resources
                .spans
                .0
                .as_ref()
                .is_some_and(|i| Arc::ptr_eq(i, inner))
            || !engine
                .history_partition_if_open()
                .is_some_and(|p| Arc::ptr_eq(&p, part))
        {
            return None;
        }
        let mut state = inner.state.lock().unwrap_or_else(|e| e.into_inner());
        let project = match state
            .projects
            .iter()
            .filter_map(Weak::upgrade)
            .find(|p| p.id == desc.project_id)
        {
            Some(project) => project,
            None => {
                let slot = state.projects.iter_mut().find(|p| p.strong_count() == 0)?;
                let project = Arc::new(Project {
                    id: desc.project_id.clone(),
                    generation: AtomicU64::new(0),
                    bytes: Arc::new(Semaphore::new(PROJECT_CAP)),
                });
                *slot = Arc::downgrade(&project);
                project
            }
        };
        let charge = Lease::reserve(inner, &project, CONTROL)?;
        let scope = Arc::new(Scope {
            inner: Arc::downgrade(inner),
            engine: Arc::downgrade(engine),
            part: Arc::downgrade(part),
            generation: project.generation.load(Ordering::Acquire),
            project,
            route,
            inc,
            _charge: charge,
        });
        scope.live().then_some(scope)
    }
    /// Current deletion/policy callers invalidate all physical proofs of the
    /// project. Future canonical retention must call this BEFORE removing rows.
    pub(crate) fn invalidate_project(&self, id: &ProjectId) {
        let Some(inner) = &self.0 else { return };
        let mut state = inner.state.lock().unwrap_or_else(|e| e.into_inner());
        for p in state
            .projects
            .iter()
            .filter_map(Weak::upgrade)
            .filter(|p| &p.id == id)
        {
            p.generation.fetch_add(1, Ordering::AcqRel);
        }
        for slot in state.entries.iter_mut() {
            if slot.as_ref().is_some_and(|e| &e.scope.project.id == id) {
                *slot = None;
            }
        }
    }
    pub(crate) fn retire(&self, engine: &ShardEngine) {
        let Some(inner) = &self.0 else { return };
        let mut state = inner.state.lock().unwrap_or_else(|e| e.into_inner());
        for slot in state.entries.iter_mut() {
            if slot
                .as_ref()
                .is_some_and(|e| std::ptr::eq(e.scope.engine.as_ptr(), engine))
            {
                *slot = None;
            }
        }
    }
    #[cfg(test)]
    pub(crate) fn reserved(&self) -> usize {
        self.0
            .as_ref()
            .map_or(0, |i| CAPACITY - i.bytes.available_permits())
    }
}
#[cfg(test)]
mod tests;
