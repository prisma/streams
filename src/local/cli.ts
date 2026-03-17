import { existsSync, rmSync } from "node:fs";
import { join } from "node:path";
import { parseLocalProcessOptions, waitForPidExit, isPidAlive } from "./common";
import { runLocalServerProcess } from "./process";
import { getDataDir, getDbPath, getStatus, readServerDump, scanServers } from "./state";
import { normalizeServerName } from "./paths";
import { dsError } from "../util/ds_error.ts";

type ParsedArgs = {
  command: string;
  name?: string;
  hostname?: string;
  port?: number;
};

function parseArgs(argv: string[]): ParsedArgs {
  const [command = "status", ...rest] = argv;
  return { command, ...parseLocalProcessOptions(rest) };
}

async function cmdStart(args: ParsedArgs): Promise<void> {
  await runLocalServerProcess(
    {
      name: args.name,
      hostname: args.hostname,
      port: args.port,
    },
    {
      onReady: (exportsPayload) => {
        // eslint-disable-next-line no-console
        console.log(JSON.stringify(exportsPayload, null, 2));
      },
    }
  );
}

async function cmdStatus(args: ParsedArgs): Promise<void> {
  if (args.name) {
    const status = await getStatus(args.name);
    // eslint-disable-next-line no-console
    console.log(JSON.stringify(status, null, 2));
    return;
  }

  const names = scanServers();
  const statuses = await Promise.all(names.map((n) => getStatus(n)));
  // eslint-disable-next-line no-console
  console.log(JSON.stringify(statuses, null, 2));
}

async function cmdStop(args: ParsedArgs): Promise<void> {
  const name = normalizeServerName(args.name);
  const dump = readServerDump(name);
  if (!dump) {
    // eslint-disable-next-line no-console
    console.log(JSON.stringify({ name, stopped: false, reason: "not_found" }, null, 2));
    return;
  }

  if (!isPidAlive(dump.pid)) {
    // eslint-disable-next-line no-console
    console.log(JSON.stringify({ name, stopped: false, reason: "pid_not_running", pid: dump.pid }, null, 2));
    return;
  }

  process.kill(dump.pid, "SIGTERM");
  const exited = await waitForPidExit(dump.pid);
  // eslint-disable-next-line no-console
  console.log(JSON.stringify({ name, stopped: exited, pid: dump.pid }, null, 2));
}

async function cmdReset(args: ParsedArgs): Promise<void> {
  const name = normalizeServerName(args.name);
  const status = await getStatus(name);
  if (status.running || status.pidAlive) {
    throw dsError(`server is running: ${name}`);
  }

  const dataDir = getDataDir(name);
  const dbPath = getDbPath(name);
  const candidates = [dbPath, `${dbPath}-wal`, `${dbPath}-shm`, `${dbPath}-journal`, join(dataDir, "cache")];
  for (const path of candidates) {
    if (!existsSync(path)) continue;
    rmSync(path, { recursive: true, force: true });
  }

  // eslint-disable-next-line no-console
  console.log(JSON.stringify({ name, reset: true, sqlite: dbPath }, null, 2));
}

const args = parseArgs(process.argv.slice(2));

switch (args.command) {
  case "start":
    await cmdStart(args);
    break;
  case "status":
    await cmdStatus(args);
    break;
  case "stop":
    await cmdStop(args);
    break;
  case "reset":
    await cmdReset(args);
    break;
  default:
    throw dsError(`unknown command: ${args.command}`);
}
