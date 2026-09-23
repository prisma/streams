//! The catalog-walk test failpoint. It lives beside the registry, compiled
//! only under `cfg(test)`, so production `list_page` neither locks nor
//! consults a set nothing in production ever arms.

use super::Registry;

impl Registry {
    /// SR3-2 test failpoint: fail the next catalog page walk for this
    /// project (drives the fail-closed seed path).
    // mt-lint: allow(name-param-shared-core): test failpoint arming, no identity derived
    pub(crate) fn fail_next_list(&self, project: &str) {
        self.fail_next_list
            .lock()
            .unwrap()
            .insert(project.to_string());
    }

    pub(super) fn take_fail_next_list(&self, project: &crate::tenant::ProjectId) -> bool {
        self.fail_next_list.lock().unwrap().remove(project.as_str())
    }
}
