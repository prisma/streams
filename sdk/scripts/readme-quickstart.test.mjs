// The README opens with the beta's install line and a quickstart whose code
// typechecks against the package's own source under its own compiler
// options, so the first thing a beta user copies compiles.
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import test from "node:test";
import { fileURLToPath } from "node:url";
import ts from "typescript";

const sdkDir = join(dirname(fileURLToPath(import.meta.url)), "..");
const readme = readFileSync(join(sdkDir, "README.md"), "utf8");

// The names the quickstart leaves to the reader: the environment and the
// durable producer-state store the README tells them to bring.
const PRELUDE = `declare const process: { env: Record<string, string | undefined> };
declare const myStateStore: import("@prisma/streams").ProducerStateStore;
`;

function section(title) {
  const start = readme.indexOf(`\n## ${title}\n`);
  assert.ok(start >= 0, `README has a "## ${title}" section`);
  const end = readme.indexOf("\n## ", start + 1);
  return readme.slice(start, end < 0 ? undefined : end);
}

function diagnostics(code) {
  const { compilerOptions } = JSON.parse(readFileSync(join(sdkDir, "tsconfig.json"), "utf8"));
  const { options, errors } = ts.convertCompilerOptionsFromJson(
    { ...compilerOptions, baseUrl: ".", paths: { "@prisma/streams": ["src/index.ts"] }, types: [] },
    sdkDir,
  );
  assert.deepEqual(errors, []);
  const file = join(sdkDir, "readme-quickstart.ts");
  const host = ts.createCompilerHost(options);
  const { getSourceFile, fileExists, readFile } = host;
  host.getSourceFile = (name, language, ...rest) =>
    name === file ? ts.createSourceFile(name, code, language) : getSourceFile.call(host, name, language, ...rest);
  host.fileExists = name => name === file || fileExists.call(host, name);
  host.readFile = name => (name === file ? code : readFile.call(host, name));
  const program = ts.createProgram([file], options, host);
  return ts.getPreEmitDiagnostics(program).map(d =>
    ts.flattenDiagnosticMessageText(d.messageText, "\n"));
}

test("the README installs the beta by its dist-tag", () => {
  assert.match(section("Install"), /npm install @prisma\/streams@beta/);
});

test("the quickstart starts from a Console credential kept whole in PRISMA_STREAMS_CREDENTIAL", () => {
  const quickstart = section("Quickstart");
  assert.match(quickstart, /Streams \(beta\)/);
  assert.match(quickstart, /PRISMA_STREAMS_CREDENTIAL/);
  assert.match(quickstart, /generateEncryptionKey\(\)/);
});

test("the quickstart's code typechecks against the package", () => {
  const blocks = [...section("Quickstart").matchAll(/```ts\n([\s\S]*?)```/g)].map(m => m[1]);
  assert.equal(blocks.length, 1, "the quickstart has one ts block");
  assert.match(blocks[0], /StreamsClient\.connect\(/);
  assert.deepEqual(diagnostics(PRELUDE + blocks[0]), []);
});
