import { mkdirSync, mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { dirname, join, resolve } from "node:path";
import { tmpdir } from "node:os";
import { spawnSync } from "node:child_process";

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

const tmpRoot = mkdtempSync(join(tmpdir(), "prisma-streams-bun-local-e2e-"));

try {
  run("node", ["scripts/build-npm-packages.mjs"], repoRoot);

  const packDir = join(tmpRoot, "pack");
  const consumerDir = join(tmpRoot, "consumer");
  mkdirSync(packDir, { recursive: true });
  mkdirSync(consumerDir, { recursive: true });

  const localPackageDir = join(repoRoot, "dist", "npm", "streams-local");
  const packOutput = run("npm", ["pack", "--pack-destination", packDir], localPackageDir);
  const tarballName = packOutput.split(/\r?\n/).filter(Boolean).at(-1);
  if (!tarballName) throw new Error("npm pack did not produce a tarball name");
  const tarballPath = join(packDir, tarballName);

  writeFileSync(
    join(consumerDir, "package.json"),
    JSON.stringify(
      {
        name: "prisma-streams-bun-consumer-smoke",
        private: true,
        type: "module",
      },
      null,
      2
    )
  );

  run("bun", ["add", tarballPath], consumerDir);

  writeFileSync(
    join(consumerDir, "consumer.mjs"),
    `
import { startLocalDurableStreamsServer } from "@prisma/streams-local";

const server = await startLocalDurableStreamsServer({
  name: "bun-e2e",
  port: 0,
  hostname: "127.0.0.1",
});

const baseUrl = server.exports.http.url;
const stream = "state";

async function fetchJson(url, init) {
  const res = await fetch(url, init);
  const text = await res.text();
  return { status: res.status, body: text ? JSON.parse(text) : null };
}

try {
  {
    const res = await fetch(\`\${baseUrl}/v1/stream/\${encodeURIComponent(stream)}\`, {
      method: "PUT",
      headers: { "content-type": "application/json" },
    });
    if (res.status !== 201 && res.status !== 200) throw new Error(\`PUT failed: \${res.status}\`);
  }

  {
    const schema = await fetchJson(\`\${baseUrl}/v1/stream/\${encodeURIComponent(stream)}/_schema\`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({
        interpreter: {
          apiVersion: "durable.streams/stream-interpreter/v1",
          format: "durable.streams/state-protocol/v1",
          touch: {
            enabled: true,
            onMissingBefore: "coarse",
          },
        },
      }),
    });
    if (schema.status !== 200) throw new Error(\`schema install failed: \${schema.status}\`);
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
        oldValue: null,
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

  run("bun", ["consumer.mjs"], consumerDir);
} finally {
  rmSync(tmpRoot, { recursive: true, force: true });
}
