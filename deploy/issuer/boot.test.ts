// The issuer's inputs and its boot refusals (public-beta issuer triage
// B2.2, B2.3). Run with `bun test ./deploy/issuer`.
import { afterEach, describe, expect, test } from "bun:test";
import { generateKeyPairSync } from "node:crypto";
import { readFileSync } from "node:fs";
import { cleanup, keyFile, publicKeyFile, readStateText, run, writeStateText } from "../cell-admin/test-rig";
import { boot } from "./boot";
import { INPUTS, readConfig, TOKEN_TTL_DEFAULT_SECS } from "./config";
import { CELL, envWith, fakeS3, issuerCell } from "./rig";

afterEach(cleanup);

/// The refusal `fn` throws, or "" when it does not.
async function refusal(fn: () => unknown): Promise<string> {
  try {
    await fn();
    return "";
  } catch (e) {
    return (e as Error).message;
  }
}

describe("the issuer's inputs", () => {
  test("every required input is named when it is missing or empty", async () => {
    const cell = await issuerCell();
    const required = [
      "STREAMS_CELL_ID", "STREAMS_ISSUER_ID", "STREAMS_CELL_ORIGIN", "STREAMS_ISSUER_SIGNING_KEY",
      "STREAMS_ISSUER_PEPPER", "STREAMS_ISSUER_SESSION_KEY", "STREAMS_ISSUER_OAUTH_CLIENT_ID",
      "STREAMS_ISSUER_METRICS_TOKEN", "STREAMS_CONTROL_S3_ENDPOINT", "STREAMS_CONTROL_S3_BUCKET",
      "STREAMS_CONTROL_S3_REGION", "STREAMS_CONTROL_S3_ACCESS_KEY_ID", "STREAMS_CONTROL_S3_SECRET_ACCESS_KEY",
      "STREAMS_FEEDS_S3_ENDPOINT", "STREAMS_FEEDS_S3_BUCKET", "STREAMS_FEEDS_S3_REGION",
      "STREAMS_FEEDS_S3_ACCESS_KEY_ID", "STREAMS_FEEDS_S3_SECRET_ACCESS_KEY",
    ];
    for (const name of required) {
      expect(await refusal(() => readConfig(envWith(cell, { [name]: undefined })))).toBe(`${name} is required`);
      expect(await refusal(() => readConfig(envWith(cell, { [name]: "" })))).toBe(`${name} is required`);
    }
    // Every name the entry may read is one of INPUTS, and nothing else.
    expect([...INPUTS].sort()).toEqual([...required, "STREAMS_ISSUER_OAUTH_CLIENT_SECRET",
      "STREAMS_ISSUER_TOKEN_TTL_SECS", "PORT", "KEEP_AWAKE"].sort());
  });

  test("the defaults: a 60-minute token (I5), awake (I10), port 8080", async () => {
    const cell = await issuerCell();
    const cfg = readConfig(envWith(cell, { KEEP_AWAKE: undefined }));
    expect([cfg.tokenTtlSecs, TOKEN_TTL_DEFAULT_SECS, cfg.keepAwake, cfg.port]).toEqual([3600, 3600, true, 8080]);
    expect(cfg.cellOrigin).toBe("https://cell-sc1.example.com");
    expect(readConfig(envWith(cell, { STREAMS_ISSUER_TOKEN_TTL_SECS: "900" })).tokenTtlSecs).toBe(900);
  });

  test("a malformed input is refused by name, its value never quoted", async () => {
    const cell = await issuerCell();
    const ec = generateKeyPairSync("ec", { namedCurve: "P-256" }).privateKey.export({ type: "pkcs8", format: "pem" });
    const rsa1024 = generateKeyPairSync("rsa", { modulusLength: 1024 }).privateKey.export({ type: "pkcs8", format: "pem" });
    const cases: [Record<string, string>, string][] = [
      [{ STREAMS_CELL_ID: "cell sc1" }, "STREAMS_CELL_ID is not a valid id (^[A-Za-z0-9_-]{1,128}$)"],
      [{ STREAMS_CELL_ORIGIN: "http://cell.example.com" }, "STREAMS_CELL_ORIGIN must be an https origin"],
      [{ STREAMS_CELL_ORIGIN: "https://cell.example.com/v1" }, "STREAMS_CELL_ORIGIN must be an origin, without a path"],
      [{ STREAMS_CELL_ORIGIN: "cell.example.com" }, "STREAMS_CELL_ORIGIN is not a URL"],
      [{ STREAMS_ISSUER_SIGNING_KEY: "not a key" }, "STREAMS_ISSUER_SIGNING_KEY does not hold a PEM private key"],
      [{ STREAMS_ISSUER_SIGNING_KEY: ec.toString() }, "STREAMS_ISSUER_SIGNING_KEY holds a ec key; use RSA or Ed25519"],
      [{ STREAMS_ISSUER_SIGNING_KEY: rsa1024.toString() }, "STREAMS_ISSUER_SIGNING_KEY holds an RSA key shorter than 2048 bits"],
      [{ STREAMS_ISSUER_PEPPER: "v1:c2hvcnQ=" }, "STREAMS_ISSUER_PEPPER: pepper v1 is shorter than 32 bytes"],
      [{ STREAMS_ISSUER_PEPPER: "pepper" }, "STREAMS_ISSUER_PEPPER: entry 1 is not v<N>:<base64>"],
      [{ STREAMS_ISSUER_SESSION_KEY: "c2hvcnQ=" }, "STREAMS_ISSUER_SESSION_KEY must be 32 bytes in base64"],
      [{ STREAMS_ISSUER_METRICS_TOKEN: "short" }, "STREAMS_ISSUER_METRICS_TOKEN is shorter than 32 characters"],
      [{ STREAMS_ISSUER_TOKEN_TTL_SECS: "299" }, "STREAMS_ISSUER_TOKEN_TTL_SECS is not a whole number in 300..86400"],
      [{ STREAMS_ISSUER_TOKEN_TTL_SECS: "86401" }, "STREAMS_ISSUER_TOKEN_TTL_SECS is not a whole number in 300..86400"],
      [{ PORT: "http" }, "PORT is not a whole number in 0..65535"],
      [{ KEEP_AWAKE: "yes" }, "KEEP_AWAKE is 0 or 1"],
    ];
    for (const [patch, message] of cases) {
      expect(await refusal(() => readConfig(envWith(cell, patch)))).toBe(message);
    }
    // http to this machine is a development origin.
    expect(readConfig(envWith(cell, { STREAMS_CELL_ORIGIN: "http://127.0.0.1:8787" })).cellOrigin).toBe(
      "http://127.0.0.1:8787",
    );
  });
});

describe("the issuer's boot", () => {
  test("boots on its own cell, under its issuer, with a key the cell's keys feed holds", async () => {
    const cell = await issuerCell();
    const booted = await boot(readConfig(cell.env));
    expect([booted.state.cell.cell_id, booted.state.cell.issuer, booted.state.feed_version]).toEqual([
      CELL, "https://auth.prisma.io", 1,
    ]);
  });

  test("refuses a bucket that ignores conditional writes", async () => {
    const cell = await issuerCell();
    fakeS3().honourPreconditions = false;
    expect(await refusal(() => boot(readConfig(cell.env)))).toContain(
      "does not honour conditional writes (If-None-Match: * over an existing object answered 200, not 412)",
    );
  });

  test("refuses another cell's id, and a state that holds another cell", async () => {
    const cell = await issuerCell();
    expect(await refusal(() => boot(readConfig(envWith(cell, { STREAMS_CELL_ID: "cell_other" }))))).toBe(
      `s3://${cell.env.STREAMS_CONTROL_S3_BUCKET}/cells/cell_other/cell-state.json does not exist: run cell-admin init first`,
    );
    const bucket = cell.env.STREAMS_CONTROL_S3_BUCKET;
    fakeS3().put(bucket, "cells/cell_other/cell-state.json", fakeS3().text(bucket, `cells/${CELL}/cell-state.json`)!);
    expect(await refusal(() => boot(readConfig(envWith(cell, { STREAMS_CELL_ID: "cell_other" }))))).toBe(
      `s3://${bucket}/cells/cell_other/cell-state.json holds cell ${CELL}, not cell_other`,
    );
  });

  test("refuses a state behind its published bundle (a stale restore), as cell-admin does", async () => {
    const cell = await issuerCell();
    expect((await run("admit", "--state", cell.state, "--project", "sp_a", "--workspace", "ws_a")).code).toBe(0);
    const older = readStateText(cell.state);
    expect((await run("admit", "--state", cell.state, "--project", "sp_b", "--workspace", "ws_b")).code).toBe(0);
    writeStateText(cell.state, older);
    expect(await refusal(() => boot(readConfig(cell.env)))).toBe(
      `s3://${cell.env.STREAMS_CONTROL_S3_BUCKET}/cells/${CELL}/cell-state.json is at feed_version 2 but its bundle ` +
        "is at 3: the state was restored from a stale copy; recover the newest state before any change",
    );
  });

  test("refuses an issuer id other than the state's", async () => {
    const cell = await issuerCell();
    expect(await refusal(() => boot(readConfig(envWith(cell, { STREAMS_ISSUER_ID: "https://issuer.example.com" })))))
      .toBe(`cell ${CELL} names issuer https://auth.prisma.io, not STREAMS_ISSUER_ID https://issuer.example.com`);
  });

  test("refuses a signing key the cell's keys feed does not hold, and boots once it is added", async () => {
    const cell = await issuerCell();
    const other = keyFile(cell.root, "ed25519", 0o600, "next.pem");
    const env = envWith(cell, { STREAMS_ISSUER_SIGNING_KEY: readFileSync(other, "utf8") });
    const kid = readConfig(env).signingKey.record.kid;
    const message = await refusal(() => boot(readConfig(env)));
    expect(message).toBe(
      `the signing key (kid ${kid}) is not a key of cell ${CELL}: publish its public half with cell-admin keys add`,
    );
    expect(message).not.toContain("PRIVATE KEY");
    expect((await run("keys", "add", "--state", cell.state, "--public-key", publicKeyFile(other, "next.pub"))).code)
      .toBe(0);
    expect((await boot(readConfig(env))).state.keys.map((k) => k.kid)).toContain(kid);
  });
});
