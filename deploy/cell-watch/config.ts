// The cell watch's inputs (public-beta alerting triage B4): the names its
// Composer service binds, each under its own platform name, and the one
// reading of them into the watch's configuration. The entry hands over the
// service's input and nothing else from the process environment, so the
// watch reads none of the variables its Compute project would merge in
// (deploy/README.md: "Compute env vars are project-scoped and merged"). A
// refusal names the input, never its value.
//
//   secrets   STREAMS_OPERATOR_TOKEN  the cell's deployment bearer (the
//                                     value the operator sets for the cell)
//             ALERT_SINK_URL          where every event is POSTed
//             ALERT_SINK_KEY          the Events v2 routing key, or the
//                                     webhook's bearer
//             HEARTBEAT_URL           the dead-man's GET
//             STREAMS_ISSUER_METRICS_TOKEN  the issuer's /v1/admin/metrics
//                                     bearer
//             CANARY_CREDENTIAL       the canary project's hidden customer
//                                     credential (rows 19 and 22, canary.ts)
//   settings  STREAMS_CELL_ORIGIN     the cell's stable origin
//             STREAMS_CELL_ID         the alert keys' prefix
//             ALERT_SINK_KIND         events-v2 or webhook (sinks.ts)
//             EXPECTED_GIT            the release's commit (row 5)
//             NOTIFY_ONLY             1 demotes every PAGE (calibration, B6)
//             ISSUER_ORIGIN           the issuer's origin (rows 21, 22, 27, 28)
//
// Every input is required: a deploy whose platform lacks a secret fails its
// preflight, and one whose shell lacks a setting fails the service's input
// schema, naming it (`prisma dev` too: "ALERT_SINK_KIND must be a string
// (was missing)"). An optional input says it is absent with the value
// `none`, so a row that does not run is a written choice, never a
// forgotten variable: ISSUER_ORIGIN (no issuer GET and no canary, and
// then STREAMS_ISSUER_METRICS_TOKEN and CANARY_CREDENTIAL are none too:
// before the issuer exists the canary has nothing to exchange its
// credential at), EXPECTED_GIT (row 5 off) and, for a webhook only,
// ALERT_SINK_KEY (no bearer). The rules' prices,
// budget and G keep their defaults (rules.ts, the owner's S3 answer): no
// input names them.
import type { SinkConfig } from "./sinks";
import { rulesConfig } from "./rules";
import type { WatchConfig } from "./watch";

export const SECRET_INPUTS = [
  "STREAMS_OPERATOR_TOKEN",
  "ALERT_SINK_URL",
  "ALERT_SINK_KEY",
  "HEARTBEAT_URL",
  "STREAMS_ISSUER_METRICS_TOKEN",
  "CANARY_CREDENTIAL",
] as const;

export const PARAM_INPUTS = [
  "STREAMS_CELL_ORIGIN",
  "STREAMS_CELL_ID",
  "ALERT_SINK_KIND",
  "EXPECTED_GIT",
  "NOTIFY_ONLY",
  "ISSUER_ORIGIN",
] as const;

/// Every name the watch reads.
export const INPUTS = [...SECRET_INPUTS, ...PARAM_INPUTS] as const;

export type InputName = (typeof INPUTS)[number];
export type Inputs = Readonly<Partial<Record<InputName, string>>>;

/// An optional input's value when it is absent.
export const NONE = "none";

/// A refusal of the inputs; its message names an input and quotes no value.
export class InputError extends Error {}

const LOCAL_HOSTS = ["localhost", "127.0.0.1", "[::1]"];
const ID = /^[A-Za-z0-9_-]{1,128}$/;
const COMMIT = /^[0-9a-f]{7,40}$/;
/// A credential as cell-admin's `credential` writes it and the issuer's
/// exchange takes it: `prisma_streams_<credential id>.<secret>`.
const CREDENTIAL = /^prisma_streams_strcred_[0-9a-f]{32}\.[A-Za-z0-9_-]{43}$/;
const SINK_KINDS: readonly SinkConfig["kind"][] = ["events-v2", "webhook"];

function given(inputs: Inputs, name: InputName): string {
  const v = inputs[name];
  if (v === undefined || v === "") throw new InputError(`${name} is required`);
  return v;
}

function required(inputs: Inputs, name: InputName): string {
  const v = given(inputs, name);
  if (v === NONE) throw new InputError(`${name} is required; none is for an optional input`);
  return v;
}

/// The value, or null for none.
function optional(inputs: Inputs, name: InputName): string | null {
  const v = given(inputs, name);
  return v === NONE ? null : v;
}

/// https, or http to this machine (`prisma dev` and the drills).
function url(name: InputName, raw: string): URL {
  let u: URL;
  try {
    u = new URL(raw);
  } catch {
    throw new InputError(`${name} is not a URL`);
  }
  if (u.protocol !== "https:" && !(u.protocol === "http:" && LOCAL_HOSTS.includes(u.hostname))) {
    throw new InputError(`${name} must be https (http only to this machine)`);
  }
  return u;
}

function origin(name: InputName, raw: string): string {
  const u = url(name, raw);
  if (u.pathname !== "/" || u.search !== "" || u.hash !== "") {
    throw new InputError(`${name} must be an origin, without a path`);
  }
  return u.origin;
}

function sink(inputs: Inputs): SinkConfig {
  const kind = required(inputs, "ALERT_SINK_KIND");
  if (!(SINK_KINDS as readonly string[]).includes(kind)) throw new InputError("ALERT_SINK_KIND is events-v2 or webhook");
  const key = optional(inputs, "ALERT_SINK_KEY");
  if (kind === "events-v2" && key === null) {
    throw new InputError("ALERT_SINK_KEY is the events-v2 routing key; none is for a webhook only");
  }
  const target = required(inputs, "ALERT_SINK_URL");
  url("ALERT_SINK_URL", target);
  return { kind: kind as SinkConfig["kind"], url: target, key: key ?? "" };
}

/// The issuer's origin, metrics bearer and the canary's credential, all
/// null when ISSUER_ORIGIN is none.
function issuer(inputs: Inputs): { origin: string | null; token: string | null; canary: string | null } {
  const raw = optional(inputs, "ISSUER_ORIGIN");
  const token = optional(inputs, "STREAMS_ISSUER_METRICS_TOKEN");
  const canary = optional(inputs, "CANARY_CREDENTIAL");
  if (raw === null) {
    if (token !== null) throw new InputError("STREAMS_ISSUER_METRICS_TOKEN is set but ISSUER_ORIGIN is none");
    if (canary !== null) throw new InputError("CANARY_CREDENTIAL is set but ISSUER_ORIGIN is none");
    return { origin: null, token: null, canary: null };
  }
  if (token === null) throw new InputError("STREAMS_ISSUER_METRICS_TOKEN is required when ISSUER_ORIGIN is set");
  if (canary === null) throw new InputError("CANARY_CREDENTIAL is required when ISSUER_ORIGIN is set");
  if (!CREDENTIAL.test(canary)) {
    throw new InputError("CANARY_CREDENTIAL is not a credential (prisma_streams_<credential id>.<secret>)");
  }
  return { origin: origin("ISSUER_ORIGIN", raw), token, canary };
}

/// The watch's configuration from `inputs`, or a refusal naming the input.
export function readWatchConfig(inputs: Inputs): WatchConfig {
  const cellId = required(inputs, "STREAMS_CELL_ID");
  if (!ID.test(cellId)) throw new InputError(`STREAMS_CELL_ID is not a valid id (${ID.source})`);
  const cellOrigin = origin("STREAMS_CELL_ORIGIN", required(inputs, "STREAMS_CELL_ORIGIN"));
  const operatorToken = required(inputs, "STREAMS_OPERATOR_TOKEN");
  const heartbeatUrl = required(inputs, "HEARTBEAT_URL");
  url("HEARTBEAT_URL", heartbeatUrl);
  const notifyOnly = required(inputs, "NOTIFY_ONLY");
  if (notifyOnly !== "0" && notifyOnly !== "1") throw new InputError("NOTIFY_ONLY is 0 or 1");
  const expectedGit = optional(inputs, "EXPECTED_GIT");
  if (expectedGit !== null && !COMMIT.test(expectedGit)) {
    throw new InputError("EXPECTED_GIT is a commit (7 to 40 lowercase hex digits) or none");
  }
  const sinkConfig = sink(inputs);
  const iss = issuer(inputs);
  return {
    rules: rulesConfig({
      cellId,
      cellOrigin,
      expectedGit: expectedGit ?? "",
      notifyOnly: notifyOnly === "1",
      issuer: iss.origin !== null,
    }),
    poll: {
      cellOrigin,
      operatorToken,
      issuerOrigin: iss.origin,
      issuerMetricsToken: iss.token,
      canary: iss.canary === null ? null : { credential: iss.canary, cellId },
    },
    sink: sinkConfig,
    heartbeatUrl,
  };
}
