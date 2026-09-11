//! The per-project retention entry's admission mirror.
use super::*;

impl ProjectRetention {
    #[expect(
        clippy::let_underscore_must_use,
        reason = "ProjectRetention::bind_admission; the admission entry is bound once per project and a second bind names the same entry; a handled result would only restate the first bind"
    )]
    pub(crate) fn bind_admission(&self, adm: Arc<crate::quota::ProjectAdmission>) {
        let _ = self.admission.set(adm);
    }
    pub(super) fn mirror_add(&self, bytes: u64) {
        if let Some(a) = self.admission.get() {
            a.retained_sse_add(bytes);
        }
    }
    pub(super) fn mirror_sub(&self, bytes: u64) {
        if let Some(a) = self.admission.get() {
            a.retained_sse_sub(bytes);
        }
    }
}
