//! Task-scoped backing-allocation oracle; observes the last Bytes owner.
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
#[derive(Clone, Default)]
pub(crate) struct Probe(Arc<AtomicUsize>);
tokio::task_local! { static CURRENT: Probe; }
impl Probe {
    pub(crate) fn live(&self) -> usize {
        self.0.load(Ordering::SeqCst)
    }
    pub(crate) async fn scope<F: std::future::Future>(&self, future: F) -> F::Output {
        CURRENT.scope(self.clone(), future).await
    }
}
pub(crate) struct Charge {
    capacity: usize,
    probe: Option<Probe>,
}
impl Drop for Charge {
    fn drop(&mut self) {
        if let Some(probe) = &self.probe {
            probe.0.fetch_sub(self.capacity, Ordering::SeqCst);
        }
    }
}
pub(crate) fn charge(capacity: usize) -> Charge {
    let probe = CURRENT.try_with(Clone::clone).ok();
    if let Some(probe) = &probe {
        probe.0.fetch_add(capacity, Ordering::SeqCst);
    }
    Charge { capacity, probe }
}
