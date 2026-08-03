# Historical documents

These describe designs or product shapes that the pre-launch hard
cutover REMOVED. They are kept for provenance only — nothing here
describes the shipping product. Do not build against them.

- `PROFILES.md` — the old `queue` / `state` stream *profiles*. The
  product surface replaces them with first-class **consumer groups**
  (`/v1/streams/{name}/consumers/...`, pull/settle/leases/DLQ) and
  **watches** (`/v1/streams/{name}/watches/...`). There are no
  profiles on the product route.
