//! A split's children opened ahead of its publish (F5 shape C, GATE-DECISION-f5-pause-log-reads-
//! 2026-11-17, addendum "part 2 shape"). Once the parent's `'T'` is durable the split registers
//! each child here, with the log path its home names, and starts the child's open through the
//! directory's gate (`ShardDirectory::open_ahead`). The fenced opener (`fence::open_held`) opens a
//! prefix that is no leaf only while it is registered here: it opens the registered path, then
//! waits, signalled, until the installed topology names the prefix a leaf, and only then hands the
//! engine to the gate to publish. A registration is a guard the split holds: dropping it withdraws
//! the entry, whatever ends the split, and a withdrawn open closes its engine and fails.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, PoisonError};

/// Whether this instance may still serve a registered child once the topology names it a leaf
/// (the ring may have given it to another instance since the split registered it).
type Ours = Box<dyn Fn() -> bool + Send + Sync>;

/// The children registered ahead of their publish, by prefix, and the wake of the opens waiting
/// on them (a registration withdrawn, or a topology installed).
#[derive(Default)]
pub(crate) struct Ahead {
    entries: Mutex<HashMap<String, Entry>>,
    changed: tokio::sync::Notify,
}

/// One registered child: its log's path and whether it is still this instance's to serve.
struct Entry {
    path: String,
    ours: Ours,
}

/// Where a registered child's open stands once the opener looks again.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Standing {
    /// The installed topology names it a leaf, and it is this instance's to serve.
    Leaf,
    /// Not a leaf yet, and still registered: wait.
    Waiting,
    /// The registration is gone before the install, or the ring gave the child away.
    Withdrawn,
}

impl Ahead {
    /// Registers `prefix` with its log `path`, served here while `ours` holds; withdrawn when the
    /// returned guard drops.
    pub(crate) fn register(self: &Arc<Self>, prefix: &str, path: String, ours: Ours) -> Registered {
        self.entries()
            .insert(prefix.to_string(), Entry { path, ours });
        Registered {
            ahead: self.clone(),
            prefix: prefix.to_string(),
        }
    }

    /// The registered log path of `prefix`, if it is registered.
    pub(crate) fn path(&self, prefix: &str) -> Option<String> {
        self.entries().get(prefix).map(|entry| entry.path.clone())
    }

    /// Where `prefix`'s open stands, `leaf` saying whether the installed topology names it one. A
    /// leaf whose registration the split already dropped (after its install) is the split's child
    /// as any other leaf is.
    pub(crate) fn standing(&self, prefix: &str, leaf: bool) -> Standing {
        let entries = self.entries();
        match (leaf, entries.get(prefix)) {
            (true, Some(entry)) if !(entry.ours)() => Standing::Withdrawn,
            (true, _) => Standing::Leaf,
            (false, Some(_)) => Standing::Waiting,
            (false, None) => Standing::Withdrawn,
        }
    }

    /// The next change (a withdrawal or an install). Enable it before looking, so a change between
    /// the look and the wait is not lost.
    pub(crate) fn changed(&self) -> tokio::sync::futures::Notified<'_> {
        self.changed.notified()
    }

    /// A topology was installed: every waiting open looks again.
    pub(crate) fn installed(&self) {
        self.changed.notify_waiters();
    }

    fn entries(&self) -> std::sync::MutexGuard<'_, HashMap<String, Entry>> {
        self.entries.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// A child registered ahead of its publish; dropping it withdraws the registration.
pub(crate) struct Registered {
    ahead: Arc<Ahead>,
    prefix: String,
}

impl Drop for Registered {
    fn drop(&mut self) {
        self.ahead.entries().remove(&self.prefix);
        self.ahead.changed.notify_waiters();
    }
}

#[cfg(test)]
mod tests;
