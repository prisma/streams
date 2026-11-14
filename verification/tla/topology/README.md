# Topology models (group `topology`): TLA-045, TLA-048 (S4, S6)

These two models are a shard split and a merge of two sibling leaves, each with its recovery. Each
result is a bounded model check of the recorded instance under the recorded assumptions. The models
do not prove that the Rust code refines them, and they do not verify SlateDB or the object store.
They are conditional design checks: the writer epoch's fence rests on ASM-SLATEDB-FENCE, and every
topology and intent write is a CAS (ASM-OBJSTORE-CAS, unestablished).

Each model's header comment maps it to the code action by action, and lists the abstractions the
code must keep for the result to apply. The manifest entries name the checks and their expected
verdicts. Every configuration runs with TLC's deadlock check off, because a finished operation
stops. A negative control is a configuration that sets one of the model's operator constants to
`FALSE` (each named in the model's `CONSTANTS` block, `TRUE` in the baseline). It checks only the
property it must break.

## TLA-045: `TopologyReconfiguration.tla`

The split arm of S4-DESIGN §12: `split::split` and `Started::finish`, the actor and its lease, the
open path's fence and claim, and the reclaim of a published operation.

| Model | Code |
|---|---|
| `Begin` | the home CAS (`next_home`), then the intent (`intent.rs`, `store.rs`) |
| `Park`, `ParkLate` | the committer parks after the barrier's group (`shard/transaction/barrier.rs`) |
| `Checkpoint` | the parent's `topo-<op_id>` checkpoint |
| `Clone` | both children's seeded clones (`topology/clone.rs`), resuming into a path that holds one |
| `Retire` | the parent's `'T'`, fenced by a newer writer epoch; an attempted `'T'` counts as written (Addendum 6) |
| `Publish`, `Reclaim`, `ReclaimPublished` | the topology CAS and the barrier's release; ending a published operation's intent (`reclaim.rs`) |
| `Open`, `FenceStep`, `CloseEngine` | an open bumps the writer epoch, then re-reads `'T'` and the intent (`fence.rs`) |
| `Crash`, `Renew`, `LeaseLapse` | the actor's crash and its lease (`actor.rs`); the lease does not fence |
| `ServeUnderT`, `ClaimUnfenced`, `DecideStale`, `RollBackAnyway`, `EndAfterT` | the moves the negative controls enable |

Properties: `AckedReadable`, `NoDualServe`, `RetiredNeverServes`, `ChildrenExact`, `HomesUnique`,
`NoTEndedUnpublished`; liveness `ForwardAfterRetire` and `OperationCompletes` under `Fairness`.
Checks: 2 baselines (safety; liveness), 11 negative controls and 5 witnesses.

## TLA-048: `TopologyMerge.tla`

The merge of S6-DESIGN §7.2-§7.4: `merge::merge` and `Started::finish`, `recover::merge`, the open
path's fence, the policy's co-location (`policy_loop.rs`, `fleet/colocate.rs`), and the fleet's eager
move-in and beat opens (`fleet.rs`).

| Model | Code |
|---|---|
| `Colocate`, `AskForM`, `FleetAdd`, `Settle`, `Balance` | the policy's co-location of the pair, its ask for an instance, and the fleet adding one and moving load onto it |
| `Begin`, `StillOwned`, `Abort`, `Park`, `Checkpoint`, `Clone` | the actor's pause, both barriers and checkpoints, and the union clone into a fresh home |
| `Retire(0, …)`, `Retire(1, …)` | `'T'` into S0 and then S1; a fenced write leaves the intent live (`Unfinished`) |
| `Publish`, `Reclaim`, `ReclaimPublished` | the topology CAS, the terminal CAS, and the heartbeat's reclaim of a published intent |
| `RequestOpen`, `EagerOpen` | a client request at the effective owner; the tick's eager move-in, and the lapsed open of a live intent's sources |
| `ClaimFence`, `ClaimCAS`, `Act`, `Abandon` | the claimant (`recover::merge`): the claim, the other source's maintenance open, and the decision |
| `Crash`, `Restart`, `Partition`, `Heal`, `Renew`, `LeaseLapse` | the instances' failures and the actor's lease |

Properties: `AckedReadable`, `NoDualServe`, `RetiredNeverServes`, `NoRollbackWithT`,
`NeverInconsistent`, `NoTEndedUnpublished`; liveness `MergeHappens` and `OperationCompletes` under weak fairness for the
steps the code retries. One configuration (`assume_strong_claim_fairness`) assumes strong fairness
on the claim. Checks: 13 baselines (local and cross-instance pairs, the landed open path,
co-location, liveness), 17 negative controls and 7 witnesses.
