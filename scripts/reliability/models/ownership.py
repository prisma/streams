"""Bounded ownership/receipt protocol; no production implementation imports."""
from dataclasses import dataclass, replace


@dataclass(frozen=True)
class Write:
    # 0 absent, 1 admitted, 2 written, 3 published, 4 claimed, 5 published effects.
    phase: int = 0
    owner: int = -1
    durable: bool = False
    stored_incarnation: int = -1
    copies: int = 0
    # Retry: 0 absent, 1 attached, 2 claimed, 3 delivered, 4 rejected.
    retry: int = 0
    # Captured authority distinguishes attached effects from canonical recovery.
    retry_owner: int = -1
    acknowledged: bool = False


@dataclass(frozen=True)
class State:
    owner: int = 0
    retired: bool = False
    incarnation: int = 0
    writes: tuple[Write, Write] = (Write(), Write())


def update(state, index, **changes):
    writes = list(state.writes)
    writes[index] = replace(writes[index], **changes)
    return replace(state, writes=tuple(writes))


def transitions(state, mutation):
    if not state.retired:
        yield "retire owner 0", replace(state, retired=True, owner=-1)
    if state.owner == -1:
        yield "recover/acquire owner 1", replace(state, owner=1)
    if state.incarnation == 0 and state.writes[0].phase:
        yield "delete/recreate same name as incarnation 1", replace(state, incarnation=1)
    for index, write in enumerate(state.writes):
        label = f"write {index}: "
        live_owner = write.owner == state.owner and state.owner >= 0
        if write.phase == 0 and index == state.incarnation and state.owner >= 0:
            yield label + "admit", update(state, index, phase=1, owner=state.owner)
        if write.phase == 1:
            # An already accepted storage operation can complete after retirement.
            identity = state.incarnation if mutation == "name_only_identity" else index
            yield label + "storage accepts applied batch", update(
                state, index, phase=2, stored_incarnation=identity, copies=1
            )
        if write.phase >= 2 and not write.durable:
            yield label + "remote durability (response may be lost)", update(state, index, durable=True)
        if write.phase == 2 and (live_owner or mutation == "publish_after_retirement"):
            yield label + "publish applied/register barrier", update(state, index, phase=3)
        if write.phase == 3 and (live_owner or mutation == "claim_after_retirement"):
            if write.durable or mutation == "ack_applied":
                yield label + "claim completion", update(
                    state, index, phase=4,
                    retry=2 if mutation == "attached_retry_skips_effects" and write.retry == 1 else write.retry
                )
        if write.phase == 4:
            yield label + "publish durable effects", update(
                state, index, phase=5, retry=2 if write.retry == 1 else write.retry
            )
        if write.phase == 5 and not write.acknowledged:
            yield label + "deliver original receipt", update(state, index, acknowledged=True)
        if write.retry == 0 and write.phase >= 2 and state.owner >= 0:
            if live_owner and write.phase == 3:
                retry = 2 if mutation == "retry_skips_barrier" else 1
                yield label + "retry attaches pending", update(state, index, retry=retry, retry_owner=state.owner)
            elif write.durable and (not live_owner or write.phase == 5):
                # New owner reloads canonical producer result; open-empty dispatch
                # gate admits a duplicate only after earlier effects completed.
                copies = 2 if mutation == "retry_duplicates_record" else write.copies
                yield label + "retry canonical durable result", update(
                    state, index, retry=2, retry_owner=state.owner, copies=copies
                )
            elif not live_owner:
                yield label + "retry rejected while outcome unknown", update(state, index, retry=4)
        if write.retry == 2:
            yield label + "deliver retry receipt", update(state, index, retry=3)


def invariant(state):
    for index, write in enumerate(state.writes):
        if (write.acknowledged or write.retry == 3) and not write.durable:
            return "receipt_requires_remote_durability"
        if write.retry == 3 and write.retry_owner == write.owner and write.phase < 5:
            return "attached_retry_requires_published_effects"
        if write.copies and write.stored_incarnation != index:
            return "storage_identity_includes_incarnation"
        if write.copies > 1:
            return "retry_preserves_exactly_one_record"
    return None


def edge_invariant(before, label, after):
    if label.endswith("publish applied/register barrier"):
        index = int(label.split()[1][:-1])
        if before.writes[index].owner != before.owner or before.owner < 0:
            return "applied_publication_requires_live_authority"
    if label.endswith("claim completion"):
        index = int(label.split()[1][:-1])
        if before.writes[index].owner != before.owner or before.owner < 0:
            return "completion_claim_requires_live_authority"
    return None


def witnesses(before, label, state):
    found = set()
    for index, write in enumerate(state.writes):
        if label == f"write {index}: deliver original receipt" and write.owner == 0 and state.retired:
            found.add("durable_receipt_survives_retirement")
        if write.retry == 3 and write.acknowledged:
            found.add("retry_and_original_share_one_record")
        if label == f"write {index}: publish durable effects" and before.writes[index].retry == 1:
            if write.retry == 2:
                found.add("attached_retry_released_after_effects")
        if label == f"write {index}: deliver retry receipt":
            if write.retry_owner != write.owner and write.phase == 2:
                found.add("canonical_retry_survives_unpublished_old_owner")
        if write.durable and not write.acknowledged and state.owner >= 0 and write.owner != state.owner:
            found.add("ambiguous_commit_survives_owner_change")
        if index == 0 and label == "write 0: storage accepts applied batch" and state.incarnation == 1:
            found.add("old_operation_survives_recreation")
    if all(write.acknowledged for write in state.writes):
        found.add("both_incarnations_complete")
    return found


INITIAL = State()
REQUIRED = {
    "durable_receipt_survives_retirement", "retry_and_original_share_one_record",
    "ambiguous_commit_survives_owner_change", "old_operation_survives_recreation",
    "both_incarnations_complete", "attached_retry_released_after_effects",
    "canonical_retry_survives_unpublished_old_owner",
}
MUTATIONS = {
    "attached_retry_skips_effects": "attached_retry_requires_published_effects",
    "ack_applied": "receipt_requires_remote_durability",
    "retry_skips_barrier": "receipt_requires_remote_durability",
    "claim_after_retirement": "completion_claim_requires_live_authority",
    "publish_after_retirement": "applied_publication_requires_live_authority",
    "name_only_identity": "storage_identity_includes_incarnation",
    "retry_duplicates_record": "retry_preserves_exactly_one_record",
}
BOUNDS = {"owners": 2, "stream_names": 1, "incarnations": 2, "writes": 2, "retries_per_write": 1}
