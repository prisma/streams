# Secret custody for a shared cell

Every secret a public-beta shared cell, its interim token issuer and its
watch depend on: who holds it, where it is stored, how it rotates, and
what a leak or a loss means. RUNBOOK.md's "Shared cells" section links
here. The production workspace and the operator's machine are not named
yet (NEXT-WORK "Public beta" item 8); "the operator" below is whoever the
owner names for them.

## Where secrets are stored

- **Compute environment (encrypted).** The cell, the issuer and the
  watch are three Composer apps in three Compute projects (owner answer
  I1 (a) of 2026-10-12), so none sees another's secrets. Each secret is
  an encrypted project environment variable, bound by `envSecret` in
  `deploy/composer/app.ts` (cell), `deploy/composer-issuer/app.ts`
  (issuer) and `deploy/cell-watch/module.ts` (watch).
- **The operator's machine.** `deploy/composer/set-secrets` is the only
  way a value reaches the platform: it reads one mode-0600 file per name
  from a directory (`--from DIR`) and never takes a value on a command
  line. Those files are the operator's copy. The Management API token it
  uses is itself a file (`PRISMA_TOKEN_FILE`) and can set or replace any
  of these secrets: keep it like the most powerful of them.
- **The control bucket** holds the cells' state (projects, credential
  hashes, the allow-list); **the feeds bucket** holds the bundle the cell
  reads. They are data the secrets below protect, not secrets themselves
  (`deploy/cell-admin/README.md`, "The state in buckets").

A changed value reaches a running app only by a redeploy. A redeploy of
the cell is a restart of its one server: every project on it is down
until it is back. Plan each cell rotation as a short outage.

## The cell's secrets

| Secret (platform name) | Held by, stored | Rotation | If it leaks | If it is lost |
|---|---|---|---|---|
| `USAGE_STREAM_KEY` (`STREAMS_USAGE_STREAM_KEY`) | Cell environment; the operator's file | **None at this tip.** The usage, ops and audit ledgers are streams whose descriptors pin this key's fingerprint, so a cell started with another key is refused as a wrong key on its own ledgers. Changing it needs a fresh `PATH_PREFIX` or a ledger migration that does not exist | With the data bucket's key: the usage, ops and audit ledgers (projects, stream names, usage). Never record content | The ledgers cannot be read or appended; under `BILLING_MODE=required` the cell cannot run on its existing data. Keep a backup of the file |
| `STREAMS_CURSOR_KEY` (same name) | Cell environment; the operator's file | Set a new value and redeploy. Outstanding stream-listing cursors answer `400 invalid_cursor`; clients start the listing again | A client could forge a listing cursor, but only for a project its own credential may list (cursors are project-bound): it can only skip within its own listing | Set a new one, as for rotation |
| `AUTH_TOKEN` (`STREAMS_OPERATOR_TOKEN`) | Cell environment, and the watch's (the same value); the operator's file | Set the new value for both apps and redeploy both; the watch's row 20 notifies until both carry it | `/operator*` and `/v1/debug/*`: operational metadata and per-stream usage counters with stream names; the mutating debug routes (`absorb-pause`, `history-stall`) can degrade the cell. No record content; under enforce it opens neither the customer API nor the raw surface (`raw_surface_authorization`, `src/http.rs`) | Set a new one, as for rotation |
| `SLATE_S3_ACCESS_KEY_ID`, `SLATE_S3_SECRET_ACCESS_KEY` (`STREAMS_DATA_S3_*`) | Cell environment; the operator's file | Make a new key pair for the data bucket, set it, redeploy the cell, then revoke the old pair | Read every tenant's ciphertext and metadata (names, routing keys, sizes, the usage rollup), and **write or delete any object: all data can be destroyed, with no backup** (BETA-DATA-POSTURE.md) | Make a new pair, as for rotation |
| `FEEDS_S3_ACCESS_KEY_ID`, `FEEDS_S3_SECRET_ACCESS_KEY` (`STREAMS_FEEDS_S3_*`), the cell's copy | Cell environment; read-only on the feeds bucket | As for the data bucket's key | The bundle: project ids, quotas, credential grants and public keys. It holds no secret or secret hash (`deploy/cell-admin/feeds.ts`) | Make a new pair. The wrapper deletes the feed files after 120 s without a successful poll and the cell refuses every request about 7 min after its last good poll (RUNBOOK "Publishing") |

`FEEDS_S3_KEY` is the bundle's object key in the feeds bucket, a path,
not a secret.

## The issuer's secrets

All are in the issuer's environment (owner answer I2 of 2026-10-12: the
signing key lives there, and there is no backup of it).

| Secret | Held by, stored | Rotation | If it leaks | If it is lost |
|---|---|---|---|---|
| `STREAMS_ISSUER_SIGNING_KEY` | Issuer environment, set from a file the operator shreds afterwards | The rotation drill in `deploy/cell-admin/README.md` ("Rotating the issuer's key"); `deploy/issuer/drills.test.ts` runs it | Tokens for any project on the cell: its streams can be listed, sealed and deleted and new ones created; reading or appending to an existing stream still needs that stream's key (the descriptor pins its fingerprint, `src/application/append.rs`, `src/application/read.rs`). Run the compromise drill (same README) | Make a new key and run the rotation drill; customer credentials are unaffected |
| Control-bucket and feeds-bucket writer keys (`STREAMS_CONTROL_S3_*`, `STREAMS_FEEDS_S3_*`) | Issuer environment; the operator's `--s3-credentials` file | `/v1/buckets/{id}/keys`: a new pair to both holders, then revoke the old (the compromise drill's step 0) | As powerful as the signing key: whoever holds them can publish a bundle with a key of their own | Make a new pair |
| `STREAMS_ISSUER_PEPPER` | Issuer environment; the operator's pepper file (`cell-admin credential --pepper-file`) | Add a newer version first (`v<N>:...`, `deploy/cell-admin/secrets.ts`) and redeploy; the issuer re-hashes each credential when it is next used (`deploy/issuer/rehash.ts`); drop the old version once every credential reports the newest | With the control bucket's state, guesses at credential secrets could be checked offline; the secrets are 256 random bits, so this is not a practical attack | No credential secret can be verified: every customer needs a new credential. Keep a backup of the file |
| `STREAMS_ISSUER_SESSION_KEY` | Issuer environment | Set a new value and redeploy; every browser signs in again | Forged sessions that create and delete projects of allow-listed workspaces | Set a new one, as for rotation |
| `STREAMS_ISSUER_OAUTH_CLIENT_SECRET` | Issuer environment | Rotated at Prisma's authorization server, then set and redeploy (the real client is not registered yet, NEXT-WORK item 3, C1) | The issuer's identity at the authorization server | Ask for a new one |
| `STREAMS_ISSUER_METRICS_TOKEN` | Issuer environment and the watch's | Set the new value for both and redeploy both | The issuer's counters | Set a new one |

## The watch's secrets

| Secret | Held by, stored | Rotation | If it leaks | If it is lost |
|---|---|---|---|---|
| `ALERT_SINK_URL`, `ALERT_SINK_KEY` | Watch environment; the paging tool | Make new ones in the paging tool, set them, redeploy the watch | Anyone can send alerts to the on-call | Make new ones |
| `HEARTBEAT_URL` | Watch environment; the paging tool | As above | Anyone can beat the heartbeat and hide a dead watch | Make a new one |
| `CANARY_CREDENTIAL` | Watch environment; made on the operator's machine (RUNBOOK "The canary") | Make a new one with `cell-admin credential --hidden`, set it, redeploy the watch, then revoke the old one with `cell-admin revoke --project sp_canary_<cell id> --credential <old id>` and publish the bundle (the issuer's credential API skips hidden credentials; `cell-admin revoke` takes them, never the catalog-read one) | Reads and writes of the canary project only, within its 5 requests a second and 2 streams; false canary pages | Make a new one |
| `PAGER_ROUTING_KEY`, `PAGER_HEARTBEAT_TOKEN` (scratch and cert only) | The paging stand-in's environment (`streams-pager-standin`, RUNBOOK "A scratch cell's paging stand-in"); the operator's files; the watch holds copies as `ALERT_SINK_KEY` and in `HEARTBEAT_URL` | Make new ones, set them with `set-secrets <stage> --app pager-standin` and on the watch, redeploy both | Anyone can send the stand-in alerts, beat its heartbeat, or read its listing (`GET /changes`) | Make new ones |
| `STREAMS_OPERATOR_TOKEN`, `STREAMS_ISSUER_METRICS_TOKEN` | Copies of the cell's and the issuer's (above) | With the original | As above | As above |

## Open gaps

- `USAGE_STREAM_KEY` has no rotation path.
- The issuer's signing key and the cell's secrets have no backup beyond
  the operator's files, and the operator's machine is not named yet.
