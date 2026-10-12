// The shared cell's child environment (public-beta deploy-q5 triage B2):
// what the Composer entry hands streams-slate, built by env.ts from named
// sources only. Run with `bun test ./deploy/composer`.
import { describe, expect, test } from "bun:test";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { parseProfiles } from "../../cell-admin/profile";
import { buildChildEnv, CHILD_NAMES, type ChildEnvInput, PASS_THROUGH, STAGES } from "./env";

const DEPLOY = join(import.meta.dir, "..", "..");
const read = (path: string) => readFileSync(join(DEPLOY, path), "utf8");
const PROFILES = ["profiles/compute-1g.env", "profiles/shared-cell.env"].map((path) => ({ path, text: read(path) }));

const SCRATCH_LINES = [
  "CELL_STAGE=scratch",
  "CELL_ID=cell_e1",
  "PROJECT_ID=proj_deploy",
  "ACCOUNT_ID=acct_sink",
  "STREAMS_AUTH_ISSUER=https://auth.prisma.io",
  "PATH_PREFIX=cells/cell_e1",
  "REGION=eu-central-1",
  "SLATE_S3_ENDPOINT=https://data.example",
  "SLATE_S3_BUCKET=cell-data",
  "SLATE_S3_REGION=auto",
  "FEEDS_S3_ENDPOINT=https://feeds.example",
  "FEEDS_S3_BUCKET=cell-feeds",
  "FEEDS_S3_REGION=auto",
  "FEEDS_S3_KEY=cells/cell_e1/feeds-bundle.json",
];
const cellsText = (lines: string[] = SCRATCH_LINES) => `# the scratch cell\n${lines.join("\n")}\n`;

/// The seven platform names the entry reads, with test values.
const SECRETS = {
  STREAMS_OPERATOR_TOKEN: "operator-token-e1",
  STREAMS_USAGE_STREAM_KEY: "usage-key-e1",
  STREAMS_CURSOR_KEY: "cursor-key-e1",
  STREAMS_DATA_S3_ACCESS_KEY_ID: "data-key-id-e1",
  STREAMS_DATA_S3_SECRET_ACCESS_KEY: "data-secret-e1",
  STREAMS_FEEDS_S3_ACCESS_KEY_ID: "feeds-key-id-e1",
  STREAMS_FEEDS_S3_SECRET_ACCESS_KEY: "feeds-secret-e1",
};
/// What else a process environment can carry: a campaign wrapper's names,
/// and names the cell sets from elsewhere, with other values.
const LACED = {
  MODE: "gen",
  BILLING_METER: "off",
  BINARY_URL: "https://bin.example/streams-slate",
  SERVER_BINARY_S3_KEY: "bin/streams-old",
  SLATE_S3_BUCKET: "laced-bucket",
  AUTH_TOKEN: "laced-token",
  STREAMS_AUTH_MODE: "off",
  APP_BINARY_SHA256: "f".repeat(64),
  KEEP_AWAKE_UNTIL_MS: "1",
  CELL_STAGE: "production",
};
const PLATFORM = { ...LACED, ...SECRETS, PATH: "/usr/local/bin:/usr/bin:/bin", HOME: "/home/app" };
const SHA = "ab".repeat(32);

function input(over: Partial<ChildEnvInput> = {}): ChildEnvInput {
  return {
    profiles: PROFILES,
    cellEnv: { path: "cells/scratch.env", text: cellsText() },
    stage: "scratch",
    platformEnv: PLATFORM,
    binarySha256: SHA,
    port: "8080",
    ...over,
  };
}

function built(over: Partial<ChildEnvInput> = {}) {
  const r = buildChildEnv(input(over));
  if ("refused" in r) throw new Error(`refused ${r.refused}: ${r.message}`);
  return r;
}

const refusal = (over: Partial<ChildEnvInput>) => {
  const r = buildChildEnv(input(over));
  return "refused" in r ? { refused: r.refused, message: r.message } : r;
};

/// The profiles with `name` removed from both files and, unless undefined,
/// set to `value` in the last.
function profilesWith(name: string, value: string | undefined) {
  const drop = (text: string) => text.split("\n").filter((l) => !l.startsWith(`${name}=`)).join("\n");
  const [a, b] = PROFILES;
  return [
    { path: a.path, text: drop(a.text) },
    { path: b.path, text: drop(b.text) + (value === undefined ? "" : `\n${name}=${value}\n`) },
  ];
}

describe("the child environment", () => {
  test("is the profiles (later wins), the cells file, the seven secrets, the binary's sha256 and the runtime pass-through, and nothing else", () => {
    const layered = parseProfiles(PROFILES);
    layered.delete("KEEP_AWAKE");
    const cells = Object.fromEntries(SCRATCH_LINES.map((l) => l.split("=") as [string, string]));
    const { env } = built();
    expect(env).toEqual({
      ...Object.fromEntries(layered),
      ...cells,
      AUTH_TOKEN: "operator-token-e1",
      USAGE_STREAM_KEY: "usage-key-e1",
      STREAMS_CURSOR_KEY: "cursor-key-e1",
      SLATE_S3_ACCESS_KEY_ID: "data-key-id-e1",
      SLATE_S3_SECRET_ACCESS_KEY: "data-secret-e1",
      FEEDS_S3_ACCESS_KEY_ID: "feeds-key-id-e1",
      FEEDS_S3_SECRET_ACCESS_KEY: "feeds-secret-e1",
      APP_BINARY_SHA256: SHA,
      PATH: "/usr/local/bin:/usr/bin:/bin",
      HOME: "/home/app",
    });
    // shared-cell.env's value over compute-1g.env's, and the profile's
    // auth mode, the cells file's bucket, the platform's operator token,
    // the entry's sha256 and the cells file's stage over the laced ones.
    expect(
      ["SSE_FEED_PROJECT_BYTES", "STREAMS_AUTH_MODE", "SLATE_S3_BUCKET", "AUTH_TOKEN", "APP_BINARY_SHA256", "CELL_STAGE"].map(
        (n) => env[n],
      ),
    ).toEqual(["8388608", "enforce", "cell-data", "operator-token-e1", SHA, "scratch"]);
    for (const name of ["MODE", "BILLING_METER", "BINARY_URL", "SERVER_BINARY_S3_KEY"]) expect(env).not.toHaveProperty(name);
  });

  test("never carries KEEP_AWAKE or KEEP_AWAKE_UNTIL_MS: the entry holds the keep-awake guard itself", () => {
    const always = built();
    expect([always.env.KEEP_AWAKE, always.env.KEEP_AWAKE_UNTIL_MS, always.keepAwake]).toEqual([
      undefined,
      undefined,
      { hold: true, untilMs: null },
    ]);
    const bounded = built({ cellEnv: { path: "cells/scratch.env", text: cellsText([...SCRATCH_LINES, "KEEP_AWAKE_UNTIL_MS=1767225600000"]) } });
    expect([bounded.env.KEEP_AWAKE, bounded.env.KEEP_AWAKE_UNTIL_MS, bounded.keepAwake]).toEqual([
      undefined,
      undefined,
      { hold: true, untilMs: 1767225600000 },
    ]);
  });

  test("passes PATH, HOME, SSL_CERT_FILE and SSL_CERT_DIR through when present, and no other runtime name", () => {
    expect([...PASS_THROUGH]).toEqual(["PATH", "HOME", "SSL_CERT_FILE", "SSL_CERT_DIR"]);
    const certs = { SSL_CERT_FILE: "/etc/ssl/certs/ca-certificates.crt", SSL_CERT_DIR: "/etc/ssl/certs" };
    const { env } = built({ platformEnv: { ...SECRETS, ...certs, TMPDIR: "/tmp", LANG: "C.UTF-8" } });
    expect([env.SSL_CERT_FILE, env.SSL_CERT_DIR, "PATH" in env, "HOME" in env, "TMPDIR" in env, "LANG" in env]).toEqual([
      certs.SSL_CERT_FILE,
      certs.SSL_CERT_DIR,
      false,
      false,
      false,
      false,
    ]);
  });

  test("states every name it sets beyond the profiles in one anchored literal the effective-config parser reads", () => {
    // scripts/effective-config/effective_config.py mjs_block_names: from the
    // line holding the anchor, one KEY: line per name, until a line that is
    // "};" or "});".
    const lines = read("composer/streams/env.ts").split("\n");
    const start = lines.findIndex((l) => l.includes("export const CHILD_NAMES"));
    const names: string[] = [];
    for (const line of lines.slice(start + 1)) {
      if (/^\s*\}\)?;\s*$/.test(line)) break;
      const m = /^\s*([A-Z_][A-Z0-9_]*):/.exec(line);
      if (m) names.push(m[1]);
    }
    expect([start > 0, names]).toEqual([true, Object.keys(CHILD_NAMES)]);
  });
});

describe("refusals", () => {
  // Every row of profile.test.ts's posture table (cell-admin's own check,
  // cellSizing), refused here before anything starts.
  const breaks: [string, string | undefined, string][] = [
    ["STREAMS_AUTH_MODE", "shadow", "a shared cell needs STREAMS_AUTH_MODE=enforce"],
    ["STREAMS_RELEASE_POSTURE", undefined, "a shared cell needs STREAMS_RELEASE_POSTURE=1"],
    ["BILLING_MODE", "off", "a shared cell needs BILLING_MODE=required"],
    ["ROLLUP", "0", "a shared cell needs ROLLUP=1"],
    ["INITIAL_SHARDS", "4", "a shared cell needs INITIAL_SHARDS=1"],
    ["FLEET_PREFIX", "fleet/", "profile sets FLEET_PREFIX: a single-server shared cell runs without it"],
    ["FLEET_INTERNAL_TOKEN", "x".repeat(32), "profile sets FLEET_INTERNAL_TOKEN"],
    ["WORKLOAD_TOKEN_FILE", "/tmp/w", "profile sets WORKLOAD_TOKEN_FILE"],
    ["KEEP_AWAKE", undefined, "a shared cell needs KEEP_AWAKE=1"],
    ["KEEP_AWAKE", "0", "a shared cell needs KEEP_AWAKE=1"],
    ["KEEP_AWAKE_UNTIL_MS", "1767225600000", "profile sets KEEP_AWAKE_UNTIL_MS: a shared cell is kept awake for good"],
    ["PROJECT_SHARE_K", "1", "a shared cell needs k >= 2"],
    ["PROJECT_SHARE_K", undefined, "profile does not set PROJECT_SHARE_K"],
    ["SSE_FEED_PROJECT_BYTES", "33554432", "needs SSE_FEED_TOTAL_BYTES / k = 8388608"],
    ["PROJECT_MEMORY_PRESSURE_BYTES", "43690667", "3 projects at their lines would fill the instance read memory (131072000)"],
    ["PROJECT_MEMORY_PRESSURE_BYTES", "33554433", "8 projects at their memory lines (268435464) would pass the shard line MAX_UNABSORBED_BYTES_PER_SHARD=268435456"],
    ["MAX_UNABSORBED_BYTES_PER_SHARD", "268435455", "8 projects at their memory lines (268435456) would pass the shard line MAX_UNABSORBED_BYTES_PER_SHARD=268435455"],
    ["MAX_UNABSORBED_BYTES_PER_SHARD", undefined, "profile does not set MAX_UNABSORBED_BYTES_PER_SHARD"],
    ["CELL_ENVELOPE_REQUESTS_PER_SEC", undefined, "profile does not set CELL_ENVELOPE_REQUESTS_PER_SEC"],
    ["MAX_UNABSORBED_BYTES_PER_INSTANCE", undefined, "profile does not set MAX_UNABSORBED_BYTES_PER_INSTANCE"],
    ["ADMIT_MAX_INFLIGHT", "5e2", "ADMIT_MAX_INFLIGHT=5e2 is not a positive integer"],
    ["PROJECT_STORED_BYTES_DEFAULT", undefined, "profile does not set PROJECT_STORED_BYTES_DEFAULT"],
    ["PROJECT_STORED_BYTES_DEFAULT", "0", "profile PROJECT_STORED_BYTES_DEFAULT=0 is not a positive integer"],
  ];
  test.each(breaks)("a profile with %s=%s fails cell-admin's posture check", (name, value, message) => {
    const r = refusal({ profiles: profilesWith(name, value) });
    expect(["refused" in r && r.refused, "message" in r && r.message.includes(message)]).toEqual(["posture", true]);
  });

  test("the posture table mirrors profile.test.ts's row for row", () => {
    const rows = (path: string) => {
      const text = read(path);
      const block = text.slice(text.indexOf("const breaks"), text.indexOf("\n  ];", text.indexOf("const breaks")));
      return block.split("\n").filter((l) => l.trimStart().startsWith('["')).map((l) => l.trim());
    };
    const mine = rows("composer/streams/env.test.ts");
    expect([mine.length, mine]).toEqual([23, rows("cell-admin/profile.test.ts")]);
  });

  test("a profile line that is not a space-free KEY=VALUE", () => {
    const [a, b] = PROFILES;
    expect(refusal({ profiles: [a, { path: b.path, text: `${b.text}ROLLUP = 1\n` }] })).toEqual({
      refused: "profile",
      message: `profiles/shared-cell.env:${b.text.split("\n").length}: not a space-free KEY=VALUE line`,
    });
  });

  test("a profile that sets a name the child environment takes from elsewhere", () => {
    const withLine = (line: string) => [PROFILES[0], { path: PROFILES[1].path, text: `${PROFILES[1].text}${line}\n` }];
    expect(
      ["AUTH_TOKEN=t", "CELL_ID=cell_x", "APP_BINARY_SHA256=" + SHA, "SSL_CERT_FILE=/x"].map((l) => refusal({ profiles: withLine(l) })),
    ).toEqual([
      { refused: "duplicate_target", message: "the profiles set AUTH_TOKEN, which the cell takes from the platform's STREAMS_OPERATOR_TOKEN" },
      { refused: "duplicate_target", message: "the profiles set CELL_ID, which the cell takes from its cells file" },
      { refused: "duplicate_target", message: "the profiles set APP_BINARY_SHA256, which the cell takes from the binary it ships" },
      { refused: "duplicate_target", message: "the profiles set SSL_CERT_FILE, which the cell takes from its runtime" },
    ]);
  });

  test("a cells-file key that names a profile key", () => {
    const cellEnv = { path: "cells/scratch.env", text: cellsText([...SCRATCH_LINES, "MAX_REQUEST_BODY_BYTES=33554432"]) };
    expect(refusal({ cellEnv })).toEqual({
      refused: "profile_key",
      message: "cells/scratch.env sets MAX_REQUEST_BODY_BYTES, which the profiles set",
    });
  });

  test("an unknown cells-file key, a retired name included", () => {
    const with_ = (line: string) => refusal({ cellEnv: { path: "cells/scratch.env", text: cellsText([...SCRATCH_LINES, line]) } });
    expect([with_("BINARY_URL=https://bin.example"), with_("BIN_S3_BUCKET=artifacts")]).toEqual([
      { refused: "unknown_key", message: "cells/scratch.env sets BINARY_URL, which is not a cells-file key" },
      { refused: "unknown_key", message: "cells/scratch.env sets BIN_S3_BUCKET, which is not a cells-file key" },
    ]);
  });

  test("a cells file that misses a per-cell key, or sets one empty or twice", () => {
    const without = SCRATCH_LINES.filter((l) => !l.startsWith("REGION=") && !l.startsWith("FEEDS_S3_KEY="));
    expect(refusal({ cellEnv: { path: "cells/scratch.env", text: cellsText(without) } })).toEqual({
      refused: "missing",
      message: "cells/scratch.env does not set REGION, FEEDS_S3_KEY",
    });
    const empty = SCRATCH_LINES.map((l) => (l.startsWith("CELL_ID=") ? "CELL_ID=" : l));
    expect(refusal({ cellEnv: { path: "cells/scratch.env", text: cellsText(empty) } })).toEqual({
      refused: "missing",
      message: "cells/scratch.env does not set CELL_ID",
    });
    expect(refusal({ cellEnv: { path: "cells/scratch.env", text: cellsText([...SCRATCH_LINES, "CELL_ID=cell_e2"]) } })).toEqual({
      refused: "cells_file",
      message: "cells/scratch.env sets CELL_ID twice",
    });
    expect(refusal({ cellEnv: { path: "cells/scratch.env", text: "CELL_ID = cell_e1\n" } })).toEqual({
      refused: "cells_file",
      message: "cells/scratch.env:1: not a space-free KEY=VALUE line",
    });
  });

  test("a stage other than scratch, cert or production, or a cells file naming another", () => {
    expect(refusal({ stage: "dev" })).toEqual({ refused: "stage", message: "CELL_STAGE dev is not scratch, cert or production" });
    expect(refusal({ stage: "cert", cellEnv: { path: "cells/cert.env", text: cellsText() } })).toEqual({
      refused: "stage",
      message: "cells/cert.env names CELL_STAGE scratch, not cert",
    });
  });

  test("KEEP_AWAKE_UNTIL_MS anywhere but a scratch cell's file, or not a time", () => {
    const at = (stage: string, value: string) => {
      const lines = SCRATCH_LINES.map((l) => (l === "CELL_STAGE=scratch" ? `CELL_STAGE=${stage}` : l));
      return refusal({ stage, cellEnv: { path: `cells/${stage}.env`, text: cellsText([...lines, `KEEP_AWAKE_UNTIL_MS=${value}`]) } });
    };
    expect([at("cert", "1767225600000"), at("production", "1767225600000"), at("scratch", "soon")]).toEqual([
      {
        refused: "keep_awake",
        message: "cells/cert.env sets KEEP_AWAKE_UNTIL_MS: only a scratch cell's keep-awake may end; a cert cell is kept awake for good",
      },
      {
        refused: "keep_awake",
        message: "cells/production.env sets KEEP_AWAKE_UNTIL_MS: only a scratch cell's keep-awake may end; a production cell is kept awake for good",
      },
      { refused: "keep_awake", message: "cells/scratch.env: KEEP_AWAKE_UNTIL_MS=soon is not a time in epoch milliseconds" },
    ]);
  });

  test("a missing, empty or not space-free secret, named by its platform name and never by its value", () => {
    const { STREAMS_CURSOR_KEY: _, ...noCursor } = PLATFORM;
    const results = [
      refusal({ platformEnv: noCursor }),
      refusal({ platformEnv: { ...PLATFORM, STREAMS_DATA_S3_SECRET_ACCESS_KEY: "", STREAMS_OPERATOR_TOKEN: "" } }),
      refusal({ platformEnv: { ...PLATFORM, STREAMS_USAGE_STREAM_KEY: "usage key\n" } }),
    ];
    expect(results).toEqual([
      { refused: "missing", message: "the platform provides no STREAMS_CURSOR_KEY" },
      { refused: "missing", message: "the platform provides no STREAMS_OPERATOR_TOKEN, STREAMS_DATA_S3_SECRET_ACCESS_KEY" },
      { refused: "not_space_free", message: "the platform's STREAMS_USAGE_STREAM_KEY is not space-free" },
    ]);
    for (const value of Object.values(SECRETS)) {
      expect(results.some((r) => "message" in r && r.message.includes(value))).toBe(false);
    }
  });

  test("a binary sha256 that is not 64 lowercase hex digits, or a PORT that is not a port", () => {
    expect([refusal({ binarySha256: "AB".repeat(32) }), refusal({ port: "http" }), refusal({ port: "65536" })]).toEqual([
      { refused: "input", message: `the binary's sha256 ${"AB".repeat(32)} is not 64 lowercase hex digits` },
      { refused: "input", message: "PORT http is not a TCP port" },
      { refused: "input", message: "PORT 65536 is not a TCP port" },
    ]);
  });
});

describe("the app's bindings", () => {
  test("bind exactly the platform names the child environment reads, CELL_STAGE as the stage and CELL_DEPLOY_NONCE as the deploy's nonce", () => {
    const app = read("composer/app.ts");
    const bound = [...app.matchAll(/^\s*(\w+): envSecret\("([A-Z0-9_]+)"\),$/gm)].map((m) => [m[1], m[2]]);
    const platform = Object.values(CHILD_NAMES).flatMap((s) => (s.from === "platform" ? [s.name] : []));
    expect(bound.map(([, name]) => name)).toEqual(platform);
    expect([...app.matchAll(/^\s*(\w+): envParam\("([A-Z0-9_]+)"\),$/gm)].map((m) => [m[1], m[2]])).toEqual([
      ["cellStage", "CELL_STAGE"],
      ["deployNonce", "CELL_DEPLOY_NONCE"],
    ]);
    // Each bound field is a secretString() of the service's input schema,
    // the stage is one of env.ts's three, and the nonce is a non-empty
    // string no child-environment name carries.
    const service = read("composer/streams/service.ts");
    const fields = [...service.matchAll(/^\s*(\w+): secretString\(\),$/gm)].map((m) => m[1]);
    expect(fields).toEqual(bound.map(([field]) => field));
    expect(service).toContain(`cellStage: type.enumerated(${STAGES.map((s) => JSON.stringify(s)).join(", ")}),`);
    expect([service.includes('deployNonce: "string > 0",'), "CELL_DEPLOY_NONCE" in CHILD_NAMES]).toEqual([true, false]);
  });
});
