#!/usr/bin/env bun
// cell-admin: the operator tool for a single-server shared cell (shared-cells
// PLAN step 9). It owns the cell's feed bundle and admits and lists
// projects; README.md in this directory is the operator's guide.
//
//   bun deploy/cell-admin/cell-admin.ts <command> [flags]
//
// Exit codes: 0 done, 1 refused or failed, 2 usage, 3 not yet (publish the
// bundle or wait, then run the same command again).
import { parseFlags } from "./args";
import { admit, bundle, init, type Io, list, token } from "./commands";
import { AdminError } from "./errors";

const USAGE = `usage: cell-admin <command> --state DIR [flags]
  init      --cell-id C --deployment-project P --account A --key-file K
            [--profile F]... [--issuer URL] [--workspace-cap N] [--max-projects N] [--denylist FILE]
  admit     --project P --workspace W [--quota field=value]... [--scopes "s ..."] [--prefix p]...
  token     --credential C --key-file K --out FILE [--ttl SECS] [--sub NAME]
  list      [--json]
  bundle`;

const COMMANDS = {
  init: {
    run: init,
    spec: {
      single: ["state", "cell-id", "deployment-project", "account", "key-file", "issuer", "workspace-cap", "max-projects", "denylist"],
      multi: ["profile"],
      required: ["state", "cell-id", "deployment-project", "account", "key-file"],
    },
  },
  admit: {
    run: admit,
    spec: {
      single: ["state", "project", "workspace", "scopes"],
      multi: ["quota", "prefix"],
      required: ["state", "project", "workspace"],
    },
  },
  token: {
    run: token,
    spec: {
      single: ["state", "credential", "key-file", "out", "ttl", "sub"],
      required: ["state", "credential", "key-file", "out"],
    },
  },
  list: { run: list, spec: { single: ["state"], bool: ["json"], required: ["state"] } },
  bundle: { run: bundle, spec: { single: ["state"], required: ["state"] } },
} as const;

/// Run one command; never throws. Messages go to `io`, the code is returned.
export async function main(argv: readonly string[], io: Io & { err: (line: string) => void }): Promise<number> {
  const [name, ...rest] = argv;
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
