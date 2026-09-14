"""Two-row absorption/compaction model with durable roots and cold recovery."""
from dataclasses import dataclass, replace

# Physical objects O0, O1, H0, H1, C; C contains both rows. Bits are objects,
# not offsets, SlateDB sequence numbers, or absorbed boundaries.
ORIGINAL = 0b00011
HISTORY = (0b00100, 0b01000)
COMPACTED = 0b10000


@dataclass(frozen=True)
class State:
    objects: int = ORIGINAL
    history_roots: tuple[int, int] = (0, 0)
    absorbed: int = 0
    trim_safe: int = 0
    original_rows: int = ORIGINAL
    # Volatile knowledge/pending operations disappear on every crash.
    staged: int = 0
    flushed: int = 0
    pending_boundary: int = 0
    # 0 running, 1 down, 2 recovering. Recovery itself can crash.
    mode: int = 0
    crashes: int = 0
    live: bool = True
    # 0 not created, 1 active, 2 released; creation/release each at most once.
    fork: int = 0
    checkpoint: int = 0
    checkpoint_roots: int = 0


def retained(state):
    return state.live or state.fork == 1


def current_roots(state):
    roots = state.original_rows
    for root in state.history_roots:
        roots |= root
    return roots if retained(state) else 0


def required_roots(state):
    return current_roots(state) | state.checkpoint_roots


def transitions(state, mutation):
    if state.crashes < 2 and state.mode != 1:
        yield "crash during recovery" if state.mode == 2 else "crash", replace(
            state, mode=1, crashes=state.crashes + 1, staged=0, flushed=0, pending_boundary=0
        )
    if state.mode == 1:
        yield "begin cold recovery", replace(state, mode=2)
    if state.mode == 2:
        observed = sum(1 << row for row, root in enumerate(state.history_roots) if root)
        yield "finish cold recovery from durable roots", replace(state, mode=0, flushed=observed)
    if state.mode != 0:
        return
    if state.live:
        if state.fork == 0:
            yield "take durable fork reference", replace(state, fork=1)
        if state.checkpoint == 0:
            yield "take durable checkpoint root", replace(
                state, checkpoint=1, checkpoint_roots=current_roots(state)
            )
        yield "delete source descriptor", replace(state, live=False)
    if state.fork == 1:
        yield "release fork reference", replace(state, fork=2)
    if state.checkpoint == 1:
        yield "release checkpoint root", replace(state, checkpoint=2, checkpoint_roots=0)
    if retained(state):
        for row in range(2):
            bit, obj = 1 << row, HISTORY[row]
            if not state.history_roots[row] and not state.staged & bit:
                yield f"stage history row {row}", replace(state, staged=state.staged | bit)
            if state.staged & bit and not state.objects & obj:
                yield f"persist history object {row} (reply may be lost)", replace(
                    state, objects=state.objects | obj
                )
            if state.staged & bit and not state.history_roots[row]:
                if state.objects & obj or mutation == "manifest_before_data":
                    roots = list(state.history_roots)
                    roots[row] = obj
                    yield f"persist history manifest reference {row}", replace(
                        state, history_roots=tuple(roots), staged=state.staged & ~bit
                    )
            if state.history_roots[row] and not state.flushed & bit:
                yield f"observe history flush {row}", replace(state, flushed=state.flushed | bit)
        boundary = state.absorbed + 1
        if boundary <= 2 and not state.pending_boundary:
            prefix = (1 << boundary) - 1
            if state.flushed & prefix == prefix or mutation == "publish_before_history":
                yield f"stage shard absorbed boundary {boundary}", replace(
                    state, pending_boundary=boundary
                )
        if state.pending_boundary:
            # Tail, trim safety and original-row tombstones commit atomically.
            # Only the PREVIOUS absorbed boundary is eligible for trimming.
            previous = state.absorbed
            yield "persist shard boundary and lagged trim batch (reply may be lost)", replace(
                state, absorbed=state.pending_boundary, trim_safe=previous,
                original_rows=state.original_rows & ~((1 << previous) - 1), pending_boundary=0
            )
        if all(state.history_roots) and not state.objects & COMPACTED:
            yield "persist compacted history object", replace(state, objects=state.objects | COMPACTED)
        if state.objects & COMPACTED and all(state.history_roots):
            if state.history_roots != (COMPACTED, COMPACTED):
                yield "atomically replace history manifest roots", replace(
                    state, history_roots=(COMPACTED, COMPACTED)
                )
    protected = required_roots(state)
    if mutation == "gc_ignores_checkpoint":
        protected = current_roots(state)
    elif mutation == "gc_ignores_fork" and not state.live:
        protected = state.checkpoint_roots
    for obj in (1, 2, 4, 8, 16):
        if state.objects & obj and not protected & obj:
            yield f"physically delete object {obj}", replace(state, objects=state.objects & ~obj)


def invariant(state):
    if required_roots(state) & ~state.objects:
        return "every_retained_root_references_present_objects"
    if retained(state):
        for row in range(state.absorbed):
            if not state.history_roots[row] or not state.objects & state.history_roots[row]:
                return "absorbed_boundary_requires_durable_history_and_reference"
        for row in range(2):
            original = state.original_rows & (1 << row) & state.objects
            history = state.history_roots[row] & state.objects
            if not original and not history:
                return "acknowledged_rows_remain_recoverable"
    if state.trim_safe > state.absorbed:
        return "trim_stays_below_durable_boundary"
    return None


def edge_invariant(before, label, after):
    return None


def witnesses(before, label, state):
    found = set()
    if label == "crash during recovery":
        found.add("crash_during_recovery")
    if label == "finish cold recovery from durable roots" and not state.objects & 1 and retained(state):
        found.add("cold_recovery_after_physical_original_deletion")
    if label == "finish cold recovery from durable roots" and state.objects & 12 and not any(state.history_roots):
        found.add("cold_recovery_with_unreferenced_durable_history")
    if state.checkpoint_roots & 1 and not state.original_rows & 1:
        found.add("checkpoint_keeps_obsolete_original_object")
    if not state.live and state.fork == 1:
        found.add("fork_keeps_deleted_source_recoverable")
    if label.startswith("physically delete") and int(label.split()[-1]) in HISTORY:
        if state.history_roots == (COMPACTED, COMPACTED) and retained(state):
            found.add("compaction_physically_reclaims_old_history")
    return found


INITIAL = State()
REQUIRED = {
    "crash_during_recovery", "cold_recovery_after_physical_original_deletion",
    "cold_recovery_with_unreferenced_durable_history", "checkpoint_keeps_obsolete_original_object",
    "fork_keeps_deleted_source_recoverable", "compaction_physically_reclaims_old_history",
}
MUTATIONS = {
    "manifest_before_data": "every_retained_root_references_present_objects",
    "publish_before_history": "absorbed_boundary_requires_durable_history_and_reference",
    "gc_ignores_checkpoint": "every_retained_root_references_present_objects",
    "gc_ignores_fork": "every_retained_root_references_present_objects",
}
BOUNDS = {"rows": 2, "objects": 5, "crashes_including_recovery": 2, "forks": 1, "checkpoints": 1}
