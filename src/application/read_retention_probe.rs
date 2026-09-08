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
struct Owner {
    bytes: Vec<u8>,
    probe: Option<Probe>,
}
impl AsRef<[u8]> for Owner {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}
impl Drop for Owner {
    fn drop(&mut self) {
        if let Some(p) = &self.probe {
            p.0.fetch_sub(self.bytes.capacity(), Ordering::SeqCst);
        }
    }
}
pub(crate) fn track(bytes: Vec<u8>) -> bytes::Bytes {
    let probe = CURRENT.try_with(Clone::clone).ok();
    if let Some(p) = &probe {
        p.0.fetch_add(bytes.capacity(), Ordering::SeqCst);
    }
    bytes::Bytes::from_owner(Owner { bytes, probe })
}
