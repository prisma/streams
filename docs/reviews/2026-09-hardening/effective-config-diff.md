# Effective configuration: v0.2.0-rc.4 vs HEAD, per deployment family

The external review made this a deploy prerequisite, and the owner adopted
it: before the hardened binary is deployed, compare the configuration it will
run with against the last release, for every deployed configuration family,
using the real argv and env. This page is the result for the owner to review.

- Old: `v0.2.0-rc.4` (685ea035).
- New: HEAD of this change. The servers were built from a5068053, whose Rust
  source equals 2ffff86c (origin/slate) plus `examples/effective_config.rs`,
  which is not part of the server binary.
- Tool: `scripts/effective-config/`. Raw evidence, including the generated
  report, every family's flattened configuration on both sides, the boot logs
  and the owner's annotation template:
  `evidence/effective-config/2026-09-24/`. Placeholders only; no value
  anywhere is a real secret.

## Summary

- **No family changes verdict.** HEAD's `validate()` accepts all 12
  families. The real rc.4 and HEAD binaries both boot every family against a
  local s3lite (Leg C).
- **Four effective fields change, the same four in every family.**
  `http.h1_header_timeout` is new (120 s). Three v1 absorber options lose
  their clap defaults and become "not set". All four were intended. The
  absorber options did nothing at rc.4 either. Details are in
  [Changed effective fields](#changed-effective-fields).
- **No name is read by only one side.** Every variable a family sets is
  honoured by both, by neither (it belongs to the supervisor, the platform or
  another service in the project), or has no effect at the value the family
  gives it.
- **Validation notices:** none is new in any family. One rc.4 warning is gone
  at HEAD (the absorb-budget floor). HEAD adds one warning only in the
  merge-trap probe: an rc.4-era `ABSORB_PASS_BYTES` still held by a Compute
  project.
- **Derived values** read from both running binaries (memory budgets,
  compactor profile, SSE caps; 29 keys) are identical wherever they could be
  read.
- **Deploy is still blocked** until the owner decides the items in
  [What the owner must decide before deploy](#what-the-owner-must-decide-before-deploy).
  The hard gap is the platform export (D2/P6). The families come from the
  deploy scripts. A Compute project holds every variable ever set in it, and
  only an export shows that set.

## How it was produced

Four sides from three revisions. Each revision is built from its own checkout
(two temporary git worktrees, removed afterwards, and this checkout) into its
own target directory. Every artifact is copied and hashed immediately after
its build, and later steps run only those copies.

| Side | What it is | Built from |
| --- | --- | --- |
| new | `examples/effective_config` | HEAD: the public facade (`CliArgs::try_parse_from` → `ServerConfig::load` → `{:#?}` → `validate()`) |
| old model | An injected dumper (`scripts/effective-config/old/*.rs.in`) | A temporary worktree of 6bcaff69, the first revision that holds rc.4's configuration as one value. It was never committed there. |
| rc.4 | `streams-slate` | A clean temporary worktree of `v0.2.0-rc.4` |
| HEAD | `streams-slate`, `s3lite` | HEAD |

The old-model dumper is tied back to rc.4 by an equivalence gate:

- **E1 PASS.** rc.4's clap `Args` block (`src/main.rs:72`, 522 lines) is
  byte-identical to 6bcaff69's (`src/bootstrap.rs:23`), apart from
  `pub(crate)`.
- **E2 PASS.** rc.4 reads 72 environment names in production code. They are
  6bcaff69's 70 `ENV_KNOBS`, plus `TOKIO_WORKERS` (the dumper transcribes it
  verbatim and anchor-checks it at both revisions), plus `DST_DRAIN_TRACE`
  (read only under `#[cfg(test)]`). All six generic env helpers resolve.
- **E3 needs your sign-off.** It is a 70-row table, one row per knob, putting
  rc.4's read expression beside 6bcaff69's overlay. For 51 knobs the tool
  checked the defaults by evaluating them: rc.4's literal against the
  old-model dumper's executed default, and all 51 are equal. The other 19
  have no single evaluable default: raw strings, predicates, one name feeding
  two fields, and the old `cfg!(test)` budget fork. They are left to the
  reviewer (`equivalence/e3-transcription.md`).

Every dumper and server runs under `env -i` with one family's variables. Each
variable a family sets is probed on both sides twice: once with the variable
removed, once with its value perturbed (a `1` appended to a number, an `x` to
a string). The results are classified by what the probe shows, not by reading
source.

All controls passed in the run that produced the evidence:

| Control | What it shows |
| --- | --- |
| K1 | Output is deterministic. |
| K2 | The flattener is sensitive to a nested knob on both sides. |
| K3 | Argv reaches both sides. |
| K4 | An ambient `FLEET_MIN=9` in the tool's own environment is not seen on either side. |
| K5 | Pins the shipped, non-test absorber defaults on the old side (64 MiB, 2 gathers). |
| K6 | Reproduces HEAD's exact refusal text. |
| K7 | A non-placeholder `AUTH_TOKEN`, URL userinfo and an unlisted `*_KEY` name are each refused before anything runs. |
| K8 | The name classes come out right. |
| K9 | Leaf coverage: old 156, new 155; `billing.mode_env`, `billing.rollup_env` and `http.h1_header_timeout` are declared in the rename map. |
| K10 | rc.4 and HEAD both refuse `SWEEP_MAINT_RESIDENT=0`, each with its own exact text. |
| K11 | HEAD boots a family if and only if `validate()` accepts it. |
| K12 | Defaults boot on both sides. |
| K13 | Artifact identity: the old and new dumpers are different binaries, the rc.4 and HEAD servers are different binaries, and only the old output carries `billing.mode_env`. |

To reproduce: the evidence run took 6 minutes on this machine with warm
target directories (5 of them in the builds, 25 s for all 28 boots); the
first cold build of the three target directories took about 10 minutes and
3.3 GiB of disk.

```sh
T=$TMPDIR/ecfg-trees; W=.ecfg-work; OUT=$W/evidence   # W: a dot-directory, skipped by the source gate
python3 scripts/effective-config/effective_config.py build --work $W --trees $T --binaries
python3 scripts/effective-config/effective_config.py check-families
for c in equivalence compare controls boot; do python3 scripts/effective-config/effective_config.py $c --work $W --out $OUT; done
python3 scripts/effective-config/effective_config.py drift --out $OUT
cp $W/build.json $OUT/ && python3 scripts/effective-config/effective_config.py report --work $W --out $OUT
python3 scripts/effective-config/effective_config.py clean --work $W --trees $T   # removes both worktrees and every target dir
```

## Families

In a Compute deployment the server's environment is the supervisor's
environment, and Compute merges environment variables across the whole
project (RUNBOOK §7.3). So each Compute family is modelled in four layers:

- the other roles' `--env` first (generator, load balancer, other fleet
  ordinals);
- then the server's own `--env`, including the compute-1g profile, in script
  order, where the last setter wins;
- then the variables the supervisor adds (`APP_BINARY_SHA256`);
- and the assumed platform `PORT`.

For the Compute families the argv is the supervisor's
`--listen 0.0.0.0:$PORT`; the two local families use their scripts' own argv.
`check-families` confirms that every `--env` name in each source script is
either set by its family or explicitly omitted, with a reason.

| Family | Source | rc.4 | HEAD `validate()` | HEAD boot |
| --- | --- | --- | --- | --- |
| defaults | binary defaults (`SLATE_S3_ENDPOINT` only) | BOOTED | accepted | BOOTED |
| fleet-server-1 | `bench/fleet/deploy-fleet.sh` (ordinal 1, `ROLLUP=1`, `KEEP_AWAKE=1`) | BOOTED | accepted | BOOTED |
| fleet-server-n | `bench/fleet/deploy-fleet.sh` (ordinals 2-4; ordinal 1's `ROLLUP=1` arrives through the project merge) | BOOTED | accepted | BOOTED |
| region-server | `bench/soak/deploy-region.sh` server + generator | BOOTED | accepted | BOOTED |
| region-server-scale | the same with `SCALE_KNOBS=1` | BOOTED | accepted | BOOTED |
| mt-tenants-off | `bench/soak/mt-tenants.sh` stage 0 (fresh project, auth off) | BOOTED | accepted | BOOTED |
| mt-tenants-enforce | `bench/soak/mt-tenants.sh` enforce redeploy (stage-0 env merged) | BOOTED | accepted | BOOTED |
| wc-ladder | `bench/soak/wc-ladder.sh` | BOOTED | accepted | BOOTED |
| wc-ladder-diet | the same with `WC_DIET=1` | BOOTED | accepted | BOOTED |
| fra-ab-server | `scripts/bench-fra-ab.sh` server + generator | BOOTED | accepted | BOOTED |
| livefeed-canary | `bench/canary/livefeed-canary.mjs` (release posture, local) | BOOTED | accepted | BOOTED |
| platform-e2e | `scripts/platform-e2e.mjs` cell A (release posture, local) | BOOTED | accepted | BOOTED |

Also run, but not families:

- K10's `SWEEP_MAINT_RESIDENT=0`: REFUSED with exit 1 on both sides.
- The merge-trap probe (region-server plus `ABSORB_PASS_BYTES=67108864`):
  BOOTED on both sides.

`bench/docker/harness/cluster-deploy.sh` is excluded: it is marked HISTORICAL
and refuses to run without an opt-out.

## Changed effective fields

These are identical in all 12 families.

| Field | rc.4 → HEAD | Caused by | Intended? | Effect |
| --- | --- | --- | --- | --- |
| `http.h1_header_timeout` | absent → `120s` | 71345c03, edge record #12 | Yes | A new request-head and idle keep-alive deadline on every HTTP/1.1 connection. No family sets `SSE_H1_HEADER_TIMEOUT_MS`, so every deployment gets 120 s. Any client or edge that keeps an idle connection pooled for longer races the close. The platform edge's upstream idle timeout has not been measured. |
| `cli.absorb_pass_bytes` | `268435456` → `None` | 99d5c098 (2026-09-13, before the program, so there is no edge record) | Yes | None. The option is accepted but ignored at HEAD. At rc.4 its value was stored in `AbsorberConfig` and never read. |
| `cli.absorb_concurrency` | `6` → `None` | 99d5c098 | Yes | None; the same as above. |
| `cli.absorb_small_bytes` | `1048576` → `None` | 99d5c098 | Yes | None; the same as above. |

Reader moves checked in every family:

- `billing.mode_env` ↔ `cli.billing_mode` and `billing.rollup_env` ↔
  `cli.rollup` (46d4b7df, edge record #49): the values are equal in all 12
  families, counting an unset old value as the clap default.
- The other resolution commits named in the review leave every family's
  resolved values unchanged: fdc31b07 (`--compactor-poll-ms` on argv; the
  families set it through env or not at all), 07db91a7 (scaler cooldown
  saturation), a342ab6f (the tokio worker floor), a8c99c23 (unadmittable
  limits refused) and 8f590ec5 (`SSE_H1_MAX_BUF` validation). They change
  how a value is used or which values are refused, and no family is refused.
  Those rules are the edge records.

## Names

- **OLD_ONLY and NEW_ONLY: none in any family.**
- **NO_EFFECT_AT_VALUE** (read by both sides, but the family's value equals
  the unset default):
  - `STREAMS_DEBUG_EXIT=0` in the fleet families.
  - `STREAMS_DEBUG_TIMING=0` in the region families.
- **NEITHER:** every name in this class is one of these (the full per-family
  lists are in the evidence `report.md`, for R3):
  - supervisor names: `SERVER_BINARY_S3_KEY`, `BIN_S3_*`, `KEEP_AWAKE`,
    `RESOLV_OVERRIDE`, `FEEDS_S3_KEY`;
  - `PORT`;
  - generator or load-balancer names that reach the server through the
    project merge: `BENCH_*`, `S3_*`, `STREAMS`, `STREAM_KEY`,
    `PILOT_MODE`, `UPSTREAMS`, `LB_URL`, `TOKENS_S3_KEY` and others.

  None of them looks like a misspelled server knob. The static name sets are
  emitted as a cross-check, and no name the probe found honoured is missing
  from its side's static set.
- **PROCESS** (read outside the configuration graph on both sides, so the
  probe cannot see them):
  - `RUST_LOG` and `MIMALLOC_*`;
  - the proxy variables that reqwest honours (`HTTP_PROXY`, `HTTPS_PROXY`,
    `ALL_PROXY`, `NO_PROXY`);
  - `SSL_CERT_FILE` and `SSL_CERT_DIR`. `cargo tree --target all` shows
    `rustls-native-certs` linked through `rustls-platform-verifier` →
    reqwest 0.13 → object_store, the platform verifier used on Linux. It is
    in both lockfiles.

  No family sets any of these. A platform export would show whether Compute
  does.

## Validation notices (Leg C boot logs)

| Notice | rc.4 | HEAD | Families | Cause and intent |
| --- | --- | --- | --- | --- |
| `ABSORB_GLOBAL_BUDGET_BYTES raised 67108864 -> 100859904: the budget must cover one worst-case frame build` | WARN | gone | defaults, platform-e2e (the families without the compute-1g profile) | b612fbff (R10 refactor, 2026-09-06) dropped only the warning. The floor still applies: `absorbBudgetBytes` is 100859904 on both sides, and HEAD states it in the INFO "memory budget" line. Intended. |
| `ABSORB_PASS_BYTES are deprecated compatibility options and are ignored by the v2 gather planner; …` | none | WARN | merge-trap probe only | 99d5c098 (`IgnoredAbsorberOptions`). A project that still holds the rc.4-era variable boots and logs this one line. Intended. |

Notices both binaries log, unchanged. These are pre-existing, so they are
listed for the owner's awareness, not as changes:

- `FLEET_AUTH_MODE=static: the shared bridge token is a NAMED legacy posture;
  the release posture requires workload identity`: in every Compute family and
  in defaults.
- `STREAMS_AUTH_MODE=enforce without USAGE_STREAM_KEY: the _audit_events
  denial journal is DISABLED`: in mt-tenants-enforce, wc-ladder and
  wc-ladder-diet.
- `INITIAL_SHARDS=8 < 4×FLEET_MAX=3 … use >= 16`: in livefeed-canary.
- `CERTIFICATION MODE: sealed publication delayed ms=1500`: in
  livefeed-canary.
- `memory profile certified: compute-1g (all DB families)`, with an identical
  profile: in every family that includes the profile.

Other warnings in the boot logs name no configuration variable. They come
from running one fresh process against s3lite: missing auth feed files that
the supervisor would materialize from `FEEDS_S3_KEY`, ops-stream drains to
peers that do not exist, and shard closes. They are listed in the evidence
report for completeness. The budget summary lines (tokio workers,
descriptors, feed retention, memory budget) are identical on both sides in
all 13 boots that bound.

Derived values (C11) were read from `/v1/debug/absorb` `config` and
`/v1/debug/load` (compactor profile, shed line, SSE caps, pressure model)
while each family was booted: 29 keys per boot, identical on both sides.
They could not be read for platform-e2e, which sets no `AUTH_TOKEN`; its
debug surface returns 401 on both sides.

## Script drift since rc.4

- `ABSORB_PASS_BYTES=67108864` was removed from 13 scripts. Five of them are
  the Compute deployers: deploy-fleet, deploy-region, mt-tenants, wc-ladder
  and bench-fra-ab.
- No other `--env` name changed in those five scripts.
- `deploy/profiles/compute-1g.env` is unchanged since rc.4 (24 knobs).

## What this does not show

- **What a Compute project actually holds.** The families are derived from
  the scripts, with their `${X:-default}` fallbacks. Two things are unknown
  until a platform export: whether a project still carries leftovers such as
  `ABSORB_PASS_BYTES`, and what the platform injects.
- **In-place upgrade of an rc.4-era namespace.** A fresh in-memory s3lite
  cannot trigger persisted-state refusals: the stored
  `MAX_REQUEST_BODY_BYTES`, or the stored topology versus `INITIAL_SHARDS`.
  "HEAD BOOTED" means it binds with this configuration. It does not mean it
  boots on the existing projects. That remains the Compute platform
  validation prerequisite.
- **Platform-specific values.** Leg C ran on darwin (nofile hard limit
  effectively unlimited); Compute is x86_64 linux-musl, so the
  descriptor-derived notices and the SSE clamp there are not shown here.
- **Consumer semantics.** A value used differently under the same path is
  out of scope; those are the edge records. Examples: 07db91a7's cooldown
  saturation, a342ab6f's worker floor, a8c99c23's limit refusals.
- **Hermeticity of the local families.** The canary and platform-e2e spread
  the invoking shell's `process.env` into the server. They are modelled
  hermetically here.

## What the owner must decide before deploy

D-numbers are the decision list of the design plan (planning round 20,
effective-config-diff); R-numbers are the review classes, also the `review`
field of each row in `annotations.template.json`. PASS needs every row
decided `accept` and the platform export below.

1. **Platform export (D2, P6: blocking).** For every Compute project that will
   receive the HEAD binary, someone with platform access exports the project's
   env and pipes it through `effective_config.py redact --family <project>
   --note "<who, when>"`. The redaction happens in memory, so only
   placeholders are written. Then rerun `compare`, `boot` and `report`. Who
   does this, and for which projects? Until then the result covers the
   scripts, not the projects.
2. **The four changed fields (R1).** Accept `http.h1_header_timeout = 120 s`
   for every family. That means either knowing the platform edge's upstream
   idle timeout is under 120 s, or setting `SSE_H1_HEADER_TIMEOUT_MS` in the
   deploy scripts (edge record #12's risk). Accept the three inert absorber
   option rows.
3. **The notices (R4).** Accept the gone budget-floor warning and the new
   `IgnoredAbsorberOptions` warning. Decide whether to `--unset-env
   ABSORB_PASS_BYTES` in projects the export shows still hold it. It is
   harmless, but it logs a warning at every boot.
4. **E3 (R5, D8).** Sign off the 70-row transcription table: 51 knobs whose
   defaults were checked by evaluation, 19 left for review. Decide whether you
   review it alone or countersign an independent reviewer's pass.
5. **Old-side model (D1).** Accept 6bcaff69 plus E1/E2/E3 as rc.4's
   configuration model. The alternative is an rc.4-exact dumper, which needs
   new transcriptions. Leg C already supplies rc.4's refusal truth from the
   real binary.
6. **Family scope (D3).** Confirm that these 12 are the deployed shapes.
   None of the Compute families sets `STREAMS_RELEASE_POSTURE=1`; only the
   local canary and platform-e2e exercise the release posture. If production
   runs the release posture, a release-shaped Compute family is needed. So is
   a decision on the static fleet-auth bridge, which HEAD and rc.4 both warn
   about in every Compute family.
7. **Found in passing.** None of these differs between rc.4 and HEAD. Each is
   a separate item if you want it:
   - The project merge gives fleet ordinals 2-4 ordinal 1's `ROLLUP=1`, if
     RUNBOOK §7.3 holds. The export will confirm it.
   - wc-ladder's own `ADMIT_RSS_SHED_MB=600` and `SLATEDB_RT_THREADS=2` are
     dead: the profile that follows them wins (500 and 4).
   - `bench-fra-ab.sh` re-tightens `MANIFEST_POLL_MS=1000`, against
     RUNBOOK.md:103.
   - mt-tenants-enforce and wc-ladder run enforce without
     `USAGE_STREAM_KEY`, so the audit denial journal is off.
   - `--compactor-max-concurrent` given on argv never reaches the compactor
     (only the env overlay feeds `engine.compactor_max_concurrent`; the same
     class as item 32), and PATH_PREFIX is split between argv (stores) and
     env (the billing read spool) (D11). The Compute families set both
     through env only.
8. **Pass rule and bookkeeping (D6, D7, D9).**
   - Every row in `annotations.template.json` has `decision: null`: 45 rows
     for R1-R7, with agent proposals from `proposed.json`. Rows that are
     identical across families are collapsed.
   - The evidence is committed here, placeholders only.
   - `scripts/quality.sh` now runs the tool's unit tests. Keep that hook?
