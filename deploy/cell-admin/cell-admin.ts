#!/usr/bin/env bun
// cell-admin: the operator tool for a single-server shared cell (shared-cells
// PLAN step 9). It owns the cell's feed bundle and admits, lists and
// offboards projects; README.md in this directory is the operator's guide.
//
//   bun deploy/cell-admin/cell-admin.ts <command> [flags]
//
// Exit codes: 0 done, 1 refused or failed, 2 usage, 3 not yet (publish the
// bundle or wait, then run the same command again).
import { parseFlags } from "./args";
import { admit, bundle, init, type Io, list, sizingCheck, token } from "./commands";
import { AdminError } from "./errors";
import { canary, credential, exportCell, revokeOne } from "./credentials";
import { keysAdd, keysRetire } from "./keys";
import { offboard } from "./offboard";
import { allow, disallow, gate, quota, resume, suspend } from "./owner";
import { reprofile } from "./reprofile";

const USAGE = `usage: cell-admin <command> --state DIR|s3://BUCKET/cells/CELL [--s3-credentials FILE] [flags]
  init      --cell-id C --deployment-project P --account A (--key-file K | --public-key FILE)
            [--profile F]... [--issuer URL] [--workspace-cap N] [--max-projects N] [--denylist FILE]
            [--cell-env FILE]
  admit     --project P --workspace W [--quota field=value]... [--scopes "s ..."] [--prefix p]...
            [--profile F]...
  check-sizing --profile F [--profile F]...
  reprofile --recorded-profile F [--recorded-profile F]... --profile F [--profile F]...
  token     --credential C --key-file K --out FILE [--ttl SECS] [--sub NAME]
  offboard  --project P [--cell-url URL --key-file K] [--max-walks N] [--settle-secs S] [--wait-secs S]
  list      [--json]
  bundle
  suspend   --project P
  resume    --project P
  quota     --project P --quota field=value...
  allow     --workspace W [--max-projects N] [--requests-per-sec R] [--note TEXT]
  disallow  --workspace W
  gate      --open | --close | --n-max N
  credential --project P --pepper-file F --out FILE [--scopes "s ..."] [--prefix p]... [--name N] [--hidden]
  revoke    --project P --credential C
  canary    --key-file K --cell-url URL [--wait-secs S]
  export    --out FILE
  keys add  --public-key FILE [--retire-others]
  keys retire --kid K`;

const COMMANDS = {
  init: {
    run: init,
    spec: {
      single: [
        "state", "s3-credentials", "cell-id", "deployment-project", "account", "key-file", "public-key", "issuer",
        "workspace-cap", "max-projects", "denylist", "cell-env",
      ],
      multi: ["profile"],
      required: ["state", "cell-id", "deployment-project", "account"],
    },
  },
  admit: {
    run: admit,
    spec: {
      single: ["state", "s3-credentials", "project", "workspace", "scopes"],
      multi: ["quota", "weight", "prefix", "profile"],
      required: ["state", "project", "workspace"],
    },
  },
  "check-sizing": {
    run: sizingCheck,
    spec: { single: ["state", "s3-credentials"], multi: ["profile"], required: ["state", "profile"] },
  },
  reprofile: {
    run: reprofile,
    spec: {
      single: ["state", "s3-credentials"],
      multi: ["recorded-profile", "profile"],
      required: ["state", "recorded-profile", "profile"],
    },
  },
  token: {
    run: token,
    spec: {
      single: ["state", "s3-credentials", "credential", "key-file", "out", "ttl", "sub"],
      required: ["state", "credential", "key-file", "out"],
    },
  },
  offboard: {
    run: offboard,
    spec: {
      single: ["state", "s3-credentials", "project", "cell-url", "key-file", "max-walks", "settle-secs", "wait-secs"],
      required: ["state", "project"],
    },
  },
  list: { run: list, spec: { single: ["state", "s3-credentials"], bool: ["json"], required: ["state"] } },
  bundle: { run: bundle, spec: { single: ["state", "s3-credentials"], required: ["state"] } },
  suspend: { run: suspend, spec: { single: ["state", "s3-credentials", "project"], required: ["state", "project"] } },
  resume: { run: resume, spec: { single: ["state", "s3-credentials", "project"], required: ["state", "project"] } },
  credential: {
    run: credential,
    spec: {
      single: ["state", "s3-credentials", "project", "pepper-file", "out", "scopes", "name"],
      multi: ["prefix"],
      bool: ["hidden"],
      required: ["state", "project", "pepper-file", "out"],
    },
  },
  revoke: {
    run: revokeOne,
    spec: { single: ["state", "s3-credentials", "project", "credential"], required: ["state", "project", "credential"] },
  },
  export: { run: exportCell, spec: { single: ["state", "s3-credentials", "out"], required: ["state", "out"] } },
  canary: {
    run: canary,
    spec: {
      single: ["state", "s3-credentials", "key-file", "cell-url", "wait-secs"],
      required: ["state", "key-file", "cell-url"],
    },
  },
  "keys add": {
    run: keysAdd,
    spec: { single: ["state", "s3-credentials", "public-key"], bool: ["retire-others"], required: ["state", "public-key"] },
  },
  "keys retire": { run: keysRetire, spec: { single: ["state", "s3-credentials", "kid"], required: ["state", "kid"] } },
  allow: {
    run: allow,
    spec: {
      single: ["state", "s3-credentials", "workspace", "max-projects", "requests-per-sec", "note"],
      required: ["state", "workspace"],
    },
  },
  disallow: { run: disallow, spec: { single: ["state", "s3-credentials", "workspace"], required: ["state", "workspace"] } },
  gate: { run: gate, spec: { single: ["state", "s3-credentials", "n-max"], bool: ["open", "close"], required: ["state"] } },
  quota: {
    run: quota,
    spec: { single: ["state", "s3-credentials", "project"], multi: ["quota"], required: ["state", "project"] },
  },
} as const;

/// Run one command; never throws. Messages go to `io`, the code is returned.
export async function main(argv: readonly string[], io: Io & { err: (line: string) => void }): Promise<number> {
  // `keys` takes a subcommand: `keys add`, `keys retire`.
  const [name, ...rest] = argv[0] === "keys" ? [`keys ${argv[1]}`, ...argv.slice(2)] : argv;
  const command = COMMANDS[name as keyof typeof COMMANDS];
  if (!command) {
    io.err(USAGE);
    return 2;
  }
  try {
    await command.run(parseFlags(rest, command.spec), io);
    return 0;
  } catch (e) {
    if (e instanceof AdminError) {
      io.err(`cell-admin ${name}: ${e.message}`);
      if (e.code === 2) io.err(USAGE);
      return e.code;
    }
    io.err(`cell-admin ${name}: unexpected failure: ${(e as Error).message}`);
    return 1;
  }
}

if (import.meta.main) {
  const code = await main(process.argv.slice(2), {
    out: (line) => console.log(line),
    err: (line) => console.error(line),
  });
  process.exit(code);
}
