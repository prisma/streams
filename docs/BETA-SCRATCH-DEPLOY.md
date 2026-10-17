# A public-beta scratch deployment, command by command

The exact sequence that puts a shared cell, its interim issuer, its watch
and the paging stand-in on the `scratch` stage of four Compute projects,
from one CI-green commit, with the repository's tools only. It is a
rehearsal shape (Q5 field approval, owner decisions D1/D2): evidence stays
on the operator's machine, the budget is the owner's, and everything is
destroyed at the end. Production differs where marked. The tools' own
headers are the reference for every flag: `scripts/cut-release.sh`,
`deploy/composer/{provision-cell.py,set-secrets,deploy-release.sh,
deploy-watch.ts,watch-release.ts,cell-check}` and
`deploy/cell-admin/README.md`. `docs/GUIDE-COMPOSER.md` is older and is not
this path.

Rules that hold throughout (AGENTS.md §1): a secret lives only in a file of
mode 0600 outside the checkout and reaches the platform through
`set-secrets`; it is never an argument, an environment variable of the
deploy shell (`deploy-release.sh` refuses that) or a log line. Every tool
reads the Management API token from the file `PRISMA_TOKEN_FILE` names.
One heavy leg at a time on the machine (the cut builds the musl server).

## 0. Inputs

```bash
export PRISMA_TOKEN_FILE=<the Management API token file, mode 0600>
export SOAK_HOME=<the directory of the artifact bucket's files>   # artifact-endpoint.txt, artifact-bucket.txt, binid.txt, binsec.txt
WS=<workspace id, wksp_...>; REG=eu-central-1
S=<a directory outside the checkout>; CELL=<cell id, e.g. beta-scratch-1>; C=<full commit sha>
REL=${STREAMS_RELEASES:-$HOME/.streams-releases}
mkdir -p -m 700 $S/cells $S/secrets/issuer $S/secrets/watch $S/secrets/pager $S/cut-a $S/cut-b $S/cut-c
```

1. `scripts/dev/ci-status.sh $C` prints `CI_GREEN` (`cut-release.sh`
   refuses otherwise).
2. The token works and the workspace's plan does not stop services: a
   header file, written by builtins only, keeps the token off every
   command line:
   ```bash
   (umask 077; printf 'Authorization: Bearer %s\n' "$(<$PRISMA_TOKEN_FILE)" > $S/auth.h)
   curl -sS -H @$S/auth.h https://api.prisma.io/v1/workspaces/$WS/subscription
   curl -sS -H @$S/auth.h https://api.prisma.io/v1/projects | jq -r '.data[].name'
   ```
   None of `streams-cell`, `streams-issuer`, `streams-cell-watch`,
   `streams-pager-standin` may exist already unless it is this
   deployment's: Composer adopts a project by name, the oldest first.

## 1. Projects, Branches, buckets

Each app is its own project (owner answer I1 (a)). A project's first
Branch becomes its production Branch, so each gets that Branch (`main`)
before its `scratch` Branch; `provision-cell.py` does both, and refuses
with what to ask Platform for if the platform answers otherwise.

```bash
P=deploy/composer/provision-cell.py
python3 $P production --app streams-cell --region $REG --cells $S/cells --no-buckets
python3 $P scratch --app streams-cell --region $REG --cells $S/cells --secrets $S/secrets --cell-id $CELL
for app in streams-issuer streams-cell-watch streams-pager-standin; do
  python3 $P scratch --app $app --region $REG --cells $S/cells --no-buckets
done
ledger_id() { jq -r ".$2.id" $S/cells/$1.provision.json; }
CP=$(ledger_id scratch project) CB=$(ledger_id scratch branch)           # the cell
IP=$(ledger_id scratch.streams-issuer project) IB=$(ledger_id scratch.streams-issuer branch)
WP=$(ledger_id scratch.streams-cell-watch project) WB=$(ledger_id scratch.streams-cell-watch branch)
GP=$(ledger_id scratch.streams-pager-standin project) GB=$(ledger_id scratch.streams-pager-standin branch)
```

The cell's run makes the buckets `streams-cell-scratch-{data,feeds,control}`,
four keys (their pairs under `$S/secrets` and `$S/secrets/issuer`,
cell-admin's `$S/secrets/cell-admin-s3.json`), `$S/cells/scratch.env` and
the bucket half of `$S/cells/scratch.issuer.env`.

## 2. Keys and the remaining secrets

Each value is written by a builtin or a tool straight into its file:

```bash
(umask 077
 openssl genpkey -algorithm ed25519 -out $S/sign.pem && openssl pkey -in $S/sign.pem -pubout -out $S/sign.pub
 openssl rand -hex 32 > $S/secrets/STREAMS_OPERATOR_TOKEN          # space-free
 openssl rand -base64 32 > $S/secrets/STREAMS_USAGE_STREAM_KEY
 openssl rand -base64 32 > $S/secrets/STREAMS_CURSOR_KEY
 cp $S/sign.pem $S/secrets/issuer/STREAMS_ISSUER_SIGNING_KEY
 printf 'v1:%s\n' "$(openssl rand -base64 32)" > $S/secrets/issuer/STREAMS_ISSUER_PEPPER
 openssl rand -base64 32 > $S/secrets/issuer/STREAMS_ISSUER_SESSION_KEY
 openssl rand -hex 32 > $S/secrets/issuer/STREAMS_ISSUER_METRICS_TOKEN
 printf 'dummy-until-C1\n' > $S/secrets/issuer/STREAMS_ISSUER_OAUTH_CLIENT_SECRET
 openssl rand -hex 24 > $S/secrets/pager/PAGER_ROUTING_KEY
 openssl rand -hex 24 > $S/secrets/pager/PAGER_HEARTBEAT_TOKEN
 cp $S/secrets/STREAMS_OPERATOR_TOKEN $S/secrets/watch/)
```

"Sign in with Prisma" needs a registered OAuth client (Console ask C1),
which does not exist: the issuer boots with the dummy client above, its
exchange, `/healthz`, metrics and the canary work, and the beta page and
its project and workspace management cannot. Until C1, projects and
credentials come from `cell-admin` (section 7).

## 3. The cell's state

```bash
CA="bun deploy/cell-admin/cell-admin.ts"
STATE=s3://$(jq -r .bucket $S/secrets/cell-admin-s3.json)/cells/$CELL
$CA init --cell-id $CELL --deployment-project <the cell's system project id> --account <the account sink> \
  --public-key $S/sign.pub --issuer https://auth.prisma.io --cell-env $S/cells/scratch.env \
  --state $STATE --s3-credentials $S/secrets/cell-admin-s3.json
```

It publishes the first bundle (without it the cell answers 503
`feeds_unpublished`) and places the canary project `sp_canary_$CELL`.
Optionally add `KEEP_AWAKE_UNTIL_MS=<epoch ms>` to `$S/cells/scratch.env`
as a cost cap (scratch only; production refuses it).

## 4. Set the cell's, the issuer's and the stand-in's secrets

```bash
SS=deploy/composer/set-secrets
$SS scratch --project $CP --branch $CB --from $S/secrets
$SS scratch --project $IP --branch $IB --app issuer --from $S/secrets/issuer
$SS scratch --project $GP --branch $GB --app pager-standin --from $S/secrets/pager
```

## 5. The three cuts (the origins problem)

A release is immutable and carries the issuer's and the watch's settings,
but the issuer's `STREAMS_CELL_ORIGIN` and the watch's two origins are
service domains known only after a first deploy. So the same commit is cut
up to three times, each a new tag (each cut builds the server, about 15
minutes, alone on the machine):

| cut | `--cells` holds | deploys |
|---|---|---|
| A | `scratch.env` only | the cell alone: its domain |
| B | `scratch.env`, `scratch.issuer.env` (the cell's origin), `scratch.watch.env` with `ISSUER_ORIGIN=none` | the cell again and the issuer; the stand-in; the watch watching the cell only |
| C | as B, with the issuer's domain in `scratch.watch.env` | the watch alone, with the issuer rows and the canary |

Cut C is needed for rows 19, 21, 22, 27 and 28; B alone leaves them off.
If the platform's service domains turn out to be predictable or stable
across deploys (cloud check (h)), later rehearsals can start at B.

**Cut A and the cell.**

```bash
cp $S/cells/scratch.env $S/cut-a/
scripts/cut-release.sh $C $C-a --cells $S/cut-a
xxd -s 18 -l 2 -p $REL/$C-a/deploy/composer/streams/streams-slate     # 3e00: x86_64
DR=deploy/composer/deploy-release.sh
$DR scratch $REL/$C-a --project $CP --branch $CB --workspace $WS --state $STATE \
  --s3-credentials $S/secrets/cell-admin-s3.json                     # exit 3: deployed, not checked
```

Read the cell's domain from the Console or the deploy's output:
`CELL_ORIGIN=https://<cell domain>`. Then make the canary token cell-check
needs and run the same command again with the cell's address: the deploy
is a no-op (same release, same nonce) and cell-check runs.

```bash
$CA credential --project sp_canary_$CELL --hidden --name cellcheck \
  --scopes "streams.create streams.records.append streams.records.read streams.usage.read streams.lifecycle.manage" \
  --pepper-file $S/secrets/issuer/STREAMS_ISSUER_PEPPER --out $S/cellcheck.cred --state $STATE --s3-credentials $S/secrets/cell-admin-s3.json
$CA token --credential <the credential id it printed> --key-file $S/sign.pem --out $S/canary.token \
  --state $STATE --s3-credentials $S/secrets/cell-admin-s3.json        # at most 24 h: mint again before each check
$DR scratch $REL/$C-a --project $CP --branch $CB --workspace $WS --state $STATE \
  --s3-credentials $S/secrets/cell-admin-s3.json --cell-url $CELL_ORIGIN \
  --bearer-file $S/secrets/STREAMS_OPERATOR_TOKEN --canary-token-file $S/canary.token
$CA canary --key-file $S/sign.pem --cell-url $CELL_ORIGIN               # the cell serves the issuer's key
```

**Cut B, the issuer, the stand-in and the watch.** Write the two settings
files (space-free `KEY=VALUE`; the issuer's every non-optional envParam of
`deploy/composer-issuer/app.ts`, the watch's exactly the six of
`deploy/cell-watch/module.ts`):

```bash
cat >> $S/cells/scratch.issuer.env <<EOF
STREAMS_CELL_ID=$CELL
STREAMS_ISSUER_ID=https://auth.prisma.io
STREAMS_CELL_ORIGIN=$CELL_ORIGIN
STREAMS_ISSUER_ORIGIN=https://issuer.invalid
STREAMS_ISSUER_OAUTH_CLIENT_ID=dummy-until-C1
EOF
cat > $S/cells/scratch.watch.env <<EOF
STREAMS_CELL_ORIGIN=$CELL_ORIGIN
STREAMS_CELL_ID=$CELL
ALERT_SINK_KIND=events-v2
EXPECTED_GIT=$C
NOTIFY_ONLY=1
ISSUER_ORIGIN=none
EOF
cp $S/cells/scratch.env $S/cells/scratch.issuer.env $S/cells/scratch.watch.env $S/cut-b/
scripts/cut-release.sh $C $C-b --cells $S/cut-b
$CA token --credential <cellcheck's id> --key-file $S/sign.pem --out $S/canary.token --state $STATE --s3-credentials $S/secrets/cell-admin-s3.json
$DR scratch $REL/$C-b --project $CP --branch $CB --workspace $WS --state $STATE \
  --s3-credentials $S/secrets/cell-admin-s3.json --cell-url $CELL_ORIGIN \
  --bearer-file $S/secrets/STREAMS_OPERATOR_TOKEN --canary-token-file $S/canary.token \
  --issuer-release-dir $REL/$C-b-issuer --issuer-project $IP --issuer-branch $IB
$DR scratch $REL/$C-b-watch --app pager-standin --project $GP --branch $GB --workspace $WS
```

`STREAMS_ISSUER_ORIGIN` only feeds the sign-in callback, so a placeholder
serves until C1; the issuer's `/healthz` is its check. Read the two new domains (`ISSUER_ORIGIN`,
`PAGER=https://<stand-in domain>`), give the watch its sink and heartbeat,
and deploy it:

```bash
(umask 077
 printf '%s/v2/enqueue\n' "$PAGER" > $S/secrets/watch/ALERT_SINK_URL
 cp $S/secrets/pager/PAGER_ROUTING_KEY $S/secrets/watch/ALERT_SINK_KEY
 printf '%s/heartbeat/%s\n' "$PAGER" "$(<$S/secrets/pager/PAGER_HEARTBEAT_TOKEN)" > $S/secrets/watch/HEARTBEAT_URL
 printf 'none\n' > $S/secrets/watch/STREAMS_ISSUER_METRICS_TOKEN
 printf 'none\n' > $S/secrets/watch/CANARY_CREDENTIAL)
$SS scratch --project $WP --branch $WB --app cell-watch --from $S/secrets/watch
$DR scratch $REL/$C-b-watch --app cell-watch --project $WP --branch $WB --workspace $WS
(umask 077; printf 'Authorization: Bearer %s\n' "$(<$S/secrets/pager/PAGER_ROUTING_KEY)" > $S/pager-auth.h)
curl -sS -H @$S/pager-auth.h $PAGER/changes | jq '.beats'               # grows every 15 s
```

Production instead pages through the real paging service,
PagerDuty Events v2 (RUNBOOK §7.5); `deploy-release.sh` refuses the
stand-in there.

**Cut C, the watch's issuer rows.** The watch's credential on the canary
project (RUNBOOK, Shared cells, "The canary"), then a cut whose watch
settings name the issuer:

```bash
rm $S/secrets/watch/CANARY_CREDENTIAL                                   # the `none` of cut B: credential writes a new file
$CA credential --project sp_canary_$CELL --hidden --name watch \
  --scopes "streams.create streams.records.append streams.records.read" \
  --pepper-file $S/secrets/issuer/STREAMS_ISSUER_PEPPER --out $S/secrets/watch/CANARY_CREDENTIAL \
  --state $STATE --s3-credentials $S/secrets/cell-admin-s3.json
cp $S/secrets/issuer/STREAMS_ISSUER_METRICS_TOKEN $S/secrets/watch/
sed -i '' "s|^ISSUER_ORIGIN=none$|ISSUER_ORIGIN=$ISSUER_ORIGIN|" $S/cells/scratch.watch.env
cp $S/cells/scratch.env $S/cells/scratch.issuer.env $S/cells/scratch.watch.env $S/cut-c/
scripts/cut-release.sh $C $C-c --cells $S/cut-c
$SS scratch --project $WP --branch $WB --app cell-watch --from $S/secrets/watch
$DR scratch $REL/$C-c-watch --app cell-watch --project $WP --branch $WB --workspace $WS
```

Only the watch redeploys; the cell and the issuer keep running cut B.

## 6. The watch's drills against the stand-in

RUNBOOK's drills are local (`drills.e2e.test.ts`); on the scratch cell,
stop the watch's service (its heartbeat: the stand-in opens `heartbeat`
3 minutes after its last beat) or make a rule happen, and read
`GET /changes` on the stand-in. Nobody is called.

## 7. Customers while "Sign in with Prisma" waits on C1

```bash
$CA allow --workspace <W> --state $STATE --s3-credentials $S/secrets/cell-admin-s3.json
$CA admit --project <p> --workspace <W> --state $STATE --s3-credentials $S/secrets/cell-admin-s3.json
$CA credential --project <p> --pepper-file $S/secrets/issuer/STREAMS_ISSUER_PEPPER --out $S/<p>.cred \
  --state $STATE --s3-credentials $S/secrets/cell-admin-s3.json
```

A client then uses the SDK's `connect({ credential, issuerUrl })`
(`sdk/README.md`), which exchanges the credential at the issuer. The
remote-capable harnesses are `bench/k2cost/tenants.ts` and
`bench/lifecycle/lifecycle.py`; `scripts/platform-e2e*.mjs` start their
own local cells and cannot target a deployed one.

## 8. Destroy

```bash
$DR scratch $REL/$C-b --project $CP --branch $CB --workspace $WS --destroy   # the cell's scratch stage
python3 $P scratch --app streams-cell --cells $S/cells --teardown            # its buckets and keys
```

The issuer's, the watch's and the stand-in's scratch Branches, and all
four projects, are deleted by hand (`DELETE /v1/branches/{id}`, then
`DELETE /v1/projects/{id}`, with `-H @$S/auth.h`): no tool removes a
production Branch or a project. Then remove `$S` and the release
directories.

## Unverified on the platform

Every tool above has run against offline fakes only. Not yet seen on
Compute: whether a fresh project already owns its default Branch (the
tools handle both), musl boot and the CA bundle, adoption of a
pre-created project by name, a service domain's stability across deploys,
two Composer apps deployed from one release root (the watch and the
stand-in), Branch-scoped secrets being invisible to other Branches, an
unchanged redeploy being a no-op, and the platform storing an empty
variable value (`set-secrets --blank`).
