# GH Archive Per-Index Memory Sweep Conclusion

Raw sweep data is in:

- `/Users/sorenschmidt/code/streams/experiments/reports/per-index-memory-sweep-2026-03-31.md`

## Setup

The GH Archive demo was run on macOS with `--onlyindex` so each run enabled just one isolated index path:

- exact: one exact secondary index only
- col: one `.col` companion field only
- fts: one `.fts` field only
- agg: the `events` rollup only

Selectors were driven to:

- exact: first real L0 span (`16` uploaded segments target)
- companion families: `8` uploaded segments target

## Result

No isolated selector reproduced the earlier multi-gigabyte indexed workload.

Peak RSS by family:

- exact: average `622,469,120` bytes, max `963,149,824` at `exact:ghArchiveId`
- agg: `589,316,096` bytes at `agg:events`
- fts: average `466,557,806` bytes, max `533,725,184` at `fts:message`
- col: average `441,592,491` bytes, max `462,143,488` at `col:eventTime`

Top isolated selectors overall:

1. `exact:ghArchiveId` -> `963,149,824`
2. `exact:orgLogin` -> `713,850,880`
3. `exact:actorLogin` -> `614,318,080`
4. `exact:repoOwner` -> `603,504,640`
5. `agg:events` -> `589,316,096`
6. `exact:repoName` -> `587,988,992`
7. `exact:isBot` -> `569,507,840`
8. `exact:eventType` -> `558,563,328`
9. `exact:public` -> `549,158,912`
10. `fts:message` -> `533,725,184`

The heaviest non-exact companion selector was:

- `agg:events` -> `589,316,096`

The heaviest pure FTS selector was:

- `fts:message` -> `533,725,184`

The heaviest column selector was:

- `col:eventTime` -> `462,143,488`

## Interpretation

The isolated runs point to three conclusions.

1. No single GH Archive index path explains the original `4-5 GB` macOS indexed run on its own.
2. Exact indexing can be materially heavier than companion families for very high-cardinality fields, especially `ghArchiveId`, but even that stayed below `1 GB`.
3. Every isolated run still showed retained RSS after recovery that stayed close to the peak, which is consistent with the earlier Bun/macOS retention theory.

The earlier full indexed run is therefore most consistent with:

- combined pressure from several index families being active at once
- plus macOS/Bun resident-footprint retention after those transient allocations

The per-index sweep does not support a theory that one obviously broken single index path alone is responsible for the entire blow-up.

## Prioritization

If we focus optimization work by likely payoff, the order should be:

1. multi-family overlap and retained-footprint behavior, not just single-index micro-optimizations
2. exact indexing for very high-cardinality identifiers such as `ghArchiveId`
3. aggregate rollup building
4. FTS text fields, with `message` ahead of `title` and `body`

If we want the next experiment, the best one is a small matrix of combined selectors that match the real heavy mix:

- `exact:ghArchiveId + agg:events`
- `fts:message + agg:events`
- `fts:message + col:eventTime`
- `exact:ghArchiveId + fts:message + agg:events`

That should tell us whether the remaining blow-up is mostly additive or whether there is a nonlinear interaction between families.
