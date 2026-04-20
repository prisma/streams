import { mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { basename, dirname, join, resolve } from "node:path";
import { tmpdir } from "node:os";
import { spawnSync } from "node:child_process";
import { localPackageBunEngine } from "./package-contract.mjs";

const repoRoot = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const TABLE_KEY_POSTS = "8c646d3dd6bc68f4";

function run(cmd, args, cwd) {
  const result = spawnSync(cmd, args, {
    cwd,
    stdio: "pipe",
    encoding: "utf8",
    env: process.env,
  });
  if (result.status !== 0) {
    if (result.stdout) process.stdout.write(result.stdout);
    if (result.stderr) process.stderr.write(result.stderr);
    throw new Error(`${cmd} ${args.join(" ")} failed with code ${result.status}`);
  }
  return result.stdout.trim();
}

const tmpRoot = mkdtempSync(join(tmpdir(), "prisma-streams-node-e2e-"));
const localServerName = basename(tmpRoot);

try {
  run("node", ["scripts/build-npm-packages.mjs"], repoRoot);

  const packDir = join(tmpRoot, "pack");
  const consumerDir = join(tmpRoot, "consumer");
  mkdirSync(packDir, { recursive: true });
  mkdirSync(consumerDir, { recursive: true });

  const localPackageDir = join(repoRoot, "dist", "npm", "streams-local");
  const localPackageManifest = JSON.parse(readFileSync(join(localPackageDir, "package.json"), "utf8"));
  if ("@durable-streams/client" in (localPackageManifest.dependencies ?? {})) {
    throw new Error("@prisma/streams-local should not publish @durable-streams/client");
  }
  if (localPackageManifest.engines?.bun !== localPackageBunEngine) {
    throw new Error(`@prisma/streams-local should publish bun ${localPackageBunEngine}, got ${localPackageManifest.engines?.bun}`);
  }
  const tarballPath = run("bun", ["pm", "pack", "--destination", packDir, "--quiet"], localPackageDir)
    .split(/\r?\n/)
    .filter(Boolean)
    .at(-1);
  if (!tarballPath) throw new Error("bun pm pack did not produce a tarball path");

  writeFileSync(
    join(consumerDir, "package.json"),
    JSON.stringify(
      {
        name: "prisma-streams-node-consumer-smoke",
        private: true,
        type: "module",
      },
      null,
      2
    )
  );

  run("npm", ["install", "--no-package-lock", tarballPath], consumerDir);

  writeFileSync(
    join(consumerDir, "consumer.mjs"),
    `
import { startLocalDurableStreamsServer } from "@prisma/streams-local";

const server = await startLocalDurableStreamsServer({
  name: "${localServerName}",
  port: 0,
  hostname: "127.0.0.1",
});

const baseUrl = server.exports.http.url;
const stream = "state";
const schemaStream = "schema-reopen";
const schemaUpdate = {
  schema: {
    type: "object",
    additionalProperties: false,
    required: ["repo"],
    properties: {
      repo: { type: "string" },
    },
  },
};

async function fetchJson(url, init) {
  const res = await fetch(url, init);
  const text = await res.text();
  return { status: res.status, body: text ? JSON.parse(text) : null };
}

async function ensureSchemaInstalled(baseUrl, stream, update) {
  const current = await fetchJson(\`\${baseUrl}/v1/stream/\${encodeURIComponent(stream)}/_schema\`, { method: "GET" });
  if (current.status !== 200) throw new Error(\`schema get failed: \${current.status}\`);

  const currentSchema = current.body?.schemas?.["1"] ?? null;
  const alreadyMatches =
    current.body?.currentVersion === 1 &&
    JSON.stringify(currentSchema) === JSON.stringify(update.schema) &&
    JSON.stringify(current.body?.routingKey ?? null) === JSON.stringify(update.routingKey ?? null) &&
    JSON.stringify(current.body?.search ?? null) === JSON.stringify(update.search ?? null);

  if (alreadyMatches) return;

  const install = await fetchJson(\`\${baseUrl}/v1/stream/\${encodeURIComponent(stream)}/_schema\`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(update),
  });
  if (install.status !== 200) throw new Error(\`schema install failed: \${install.status}\`);
}

try {
  const serverDetails = await fetchJson(\`\${baseUrl}/v1/server/_details\`, { method: "GET" });
  if (serverDetails.status !== 200) throw new Error(\`/v1/server/_details failed: \${serverDetails.status}\`);
  if (serverDetails.body?.auto_tune?.preset_mb !== 1024) {
    throw new Error(\`expected local preset 1024, got \${JSON.stringify(serverDetails.body?.auto_tune)}\`);
  }

  {
    const res = await fetch(\`\${baseUrl}/v1/stream/\${encodeURIComponent(stream)}\`, {
      method: "PUT",
      headers: { "content-type": "application/json" },
    });
    if (res.status !== 201 && res.status !== 200) throw new Error(\`PUT failed: \${res.status}\`);
  }

  {
    const profile = await fetchJson(\`\${baseUrl}/v1/stream/\${encodeURIComponent(stream)}/_profile\`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({
        apiVersion: "durable.streams/profile/v1",
        profile: {
          kind: "state-protocol",
          touch: {
            enabled: true,
            onMissingBefore: "coarse",
          },
        },
      }),
    });
    if (profile.status !== 200) throw new Error(\`profile install failed: \${profile.status}\`);
  }

  {
    const routingStream = "routing";
    const res = await fetch(\`\${baseUrl}/v1/stream/\${encodeURIComponent(routingStream)}\`, {
      method: "PUT",
      headers: { "content-type": "application/json" },
    });
    if (res.status !== 201 && res.status !== 200) throw new Error(\`routing PUT failed: \${res.status}\`);

    const schema = await fetchJson(\`\${baseUrl}/v1/stream/\${encodeURIComponent(routingStream)}/_schema\`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({
        routingKey: { jsonPointer: "/repo", required: false },
      }),
    });
    if (schema.status !== 200) throw new Error(\`routing schema install failed: \${schema.status}\`);

    const append = await fetch(\`\${baseUrl}/v1/stream/\${encodeURIComponent(routingStream)}\`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify([{ repo: "beta/repo" }, { repo: "alpha/repo" }, { repo: "beta/repo" }]),
    });
    if (append.status !== 204) throw new Error(\`routing append failed: \${append.status}\`);

    const routingKeys = await fetchJson(\`\${baseUrl}/v1/stream/\${encodeURIComponent(routingStream)}/_routing_keys?limit=10\`, {
      method: "GET",
    });
    if (routingKeys.status !== 200) throw new Error(\`routing keys failed: \${routingKeys.status}\`);
    if (JSON.stringify(routingKeys.body?.keys) !== JSON.stringify(["alpha/repo", "beta/repo"])) {
      throw new Error(\`unexpected routing keys: \${JSON.stringify(routingKeys.body)}\`);
    }
  }

  {
    const res = await fetch(\`\${baseUrl}/v1/stream/\${encodeURIComponent(schemaStream)}\`, {
      method: "PUT",
      headers: { "content-type": "application/json" },
    });
    if (res.status !== 201 && res.status !== 200) throw new Error(\`schema stream PUT failed: \${res.status}\`);

    await ensureSchemaInstalled(baseUrl, schemaStream, schemaUpdate);

    const append = await fetch(\`\${baseUrl}/v1/stream/\${encodeURIComponent(schemaStream)}\`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify([{ repo: "alpha/repo" }]),
    });
    if (append.status !== 204) throw new Error(\`schema stream append failed: \${append.status}\`);
  }

  const activate = await fetchJson(\`\${baseUrl}/v1/stream/\${encodeURIComponent(stream)}/touch/templates/activate\`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({
      templates: [
        {
          entity: "posts",
          fields: [{ name: "title", encoding: "string" }],
        },
      ],
      inactivityTtlMs: 60_000,
    }),
  });
  if (activate.status !== 200) throw new Error(\`touch/templates/activate failed: \${activate.status}\`);
  const templateId = String(activate.body?.activated?.[0]?.templateId ?? "");
  if (!/^[0-9a-f]{16}$/.test(templateId)) {
    throw new Error(\`touch/templates/activate did not return a templateId: \${JSON.stringify(activate.body)}\`);
  }

  const meta0 = await fetchJson(\`\${baseUrl}/v1/stream/\${encodeURIComponent(stream)}/touch/meta\`, { method: "GET" });
  if (meta0.status !== 200) throw new Error(\`touch/meta failed: \${meta0.status}\`);
  const cursor = String(meta0.body?.cursor ?? "");
  if (!cursor) throw new Error("touch/meta missing cursor");

  const waitPromise = fetchJson(\`\${baseUrl}/v1/stream/\${encodeURIComponent(stream)}/touch/wait\`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({
      cursor,
      keys: ["${TABLE_KEY_POSTS}"],
      templateIdsUsed: [templateId],
      interestMode: "coarse",
      timeoutMs: 2000,
    }),
  });

  await new Promise((resolve) => setTimeout(resolve, 50));

  {
    const append = await fetch(\`\${baseUrl}/v1/stream/\${encodeURIComponent(stream)}\`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({
        type: "posts",
        key: "post:1",
        value: { id: "post:1", title: "hello" },
        old_value: null,
        headers: {
          operation: "insert",
          timestamp: new Date().toISOString(),
        },
      }),
    });
    if (append.status !== 204) throw new Error(\`append failed: \${append.status}\`);
  }

  const wait = await waitPromise;

  if (wait.status !== 200) throw new Error(\`touch/wait failed: \${wait.status}\`);
  if (wait.body?.touched !== true) throw new Error(\`touch/wait did not report touched=true: \${JSON.stringify(wait.body)}\`);

  console.log(JSON.stringify({ ok: true, url: baseUrl }));
} finally {
  await server.close();
}
`
  );

  run("node", ["consumer.mjs"], consumerDir);

  writeFileSync(
    join(consumerDir, "consumer-reopen.mjs"),
    `
import { startLocalDurableStreamsServer } from "@prisma/streams-local";

const server = await startLocalDurableStreamsServer({
  name: "${localServerName}",
  port: 0,
  hostname: "127.0.0.1",
});

const baseUrl = server.exports.http.url;
const schemaStream = "schema-reopen";
const schemaUpdate = {
  schema: {
    type: "object",
    additionalProperties: false,
    required: ["repo"],
    properties: {
      repo: { type: "string" },
    },
  },
};

async function fetchJson(url, init) {
  const res = await fetch(url, init);
  const text = await res.text();
  return { status: res.status, body: text ? JSON.parse(text) : null };
}

async function ensureSchemaInstalled(baseUrl, stream, update) {
  const current = await fetchJson(\`\${baseUrl}/v1/stream/\${encodeURIComponent(stream)}/_schema\`, { method: "GET" });
  if (current.status !== 200) throw new Error(\`schema get failed: \${current.status}\`);

  const currentSchema = current.body?.schemas?.["1"] ?? null;
  const alreadyMatches =
    current.body?.currentVersion === 1 &&
    JSON.stringify(currentSchema) === JSON.stringify(update.schema) &&
    JSON.stringify(current.body?.routingKey ?? null) === JSON.stringify(update.routingKey ?? null) &&
    JSON.stringify(current.body?.search ?? null) === JSON.stringify(update.search ?? null);

  if (alreadyMatches) return;

  const install = await fetchJson(\`\${baseUrl}/v1/stream/\${encodeURIComponent(stream)}/_schema\`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(update),
  });
  if (install.status !== 200) throw new Error(\`schema install failed: \${install.status}\`);
}

try {
  await ensureSchemaInstalled(baseUrl, schemaStream, schemaUpdate);

  const append = await fetch(\`\${baseUrl}/v1/stream/\${encodeURIComponent(schemaStream)}\`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify([{ repo: "beta/repo" }]),
  });
  if (append.status !== 204) throw new Error(\`schema stream reopen append failed: \${append.status}\`);

  const read = await fetchJson(\`\${baseUrl}/v1/stream/\${encodeURIComponent(schemaStream)}?offset=-1&format=json\`, {
    method: "GET",
  });
  if (read.status !== 200) throw new Error(\`schema stream reopen read failed: \${read.status}\`);
  if (JSON.stringify(read.body) !== JSON.stringify([{ repo: "alpha/repo" }, { repo: "beta/repo" }])) {
    throw new Error(\`unexpected schema stream reopen read: \${JSON.stringify(read.body)}\`);
  }

  console.log(JSON.stringify({ ok: true, reopen: true, url: baseUrl }));
} finally {
  await server.close();
}
`
  );

  run("node", ["consumer-reopen.mjs"], consumerDir);
} finally {
  rmSync(tmpRoot, { recursive: true, force: true });
}
