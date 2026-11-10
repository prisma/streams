//! A merge's co-location (S6-DESIGN §7.2 step 0): a merge's two halves must be one instance's,
//! and a pair the fleet split across two instances (a count-balance move, say) is moved onto the
//! merge's actor first. One CAS of the overrides document names both halves for the actor: the
//! one it already serves and the one it takes. With both overridden, count balance moves neither
//! (it moves only a shard without an override), and the merge, once the move's cooldown has
//! passed, finds both here. After the merge the halves are no leaves; the next write of the
//! document by count balance or a co-location drops their entries (`stale`).

use super::{FleetDocument, FleetRepository};
use crate::ownership::OwnershipService;

/// Move `halves` to `to` in one CAS of the overrides document, the entries that name no leaf of
/// the topology stored in `store` dropped (`stale`), and mirror the move here at once. Whether it
/// committed: a lost CAS or an unreadable document moves nothing, and the caller asks again on its
/// next pass.
pub(crate) async fn colocate(
    fleet: &FleetRepository,
    ownership: &OwnershipService,
    store: &dyn object_store::ObjectStore,
    halves: &[String; 2],
    to: &str,
) -> bool {
    let Ok((overrides, version)) = fleet.read_overrides().await else {
        return false;
    };
    let overrides = super::stale::pruned(overrides, store).await;
    let now_ms = crate::shard::now_ms();
    let next = halves.iter().fold(overrides, |doc, half| {
        super::count_balance::moved_document(doc, half, to, now_ms)
    });
    let Ok(body) = serde_json::to_vec(&next) else {
        return false;
    };
    if !fleet
        .replace_document(FleetDocument::Overrides, body, version)
        .await
    {
        return false;
    }
    for half in halves {
        ownership.set_override(half, to);
    }
    true
}
