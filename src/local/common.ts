import { dsError } from "../util/ds_error.ts";

export type ParsedOption = {
  found: boolean;
  value?: string;
};

export type ParsedLocalProcessOptions = {
  name?: string;
  hostname?: string;
  port?: number;
};

type ParseLocalProcessArgsOptions = {
  allowPositionalName?: boolean;
  defaultNameWhenMissing?: string;
  defaultHostnameWhenMissing?: string;
  defaultPortWhenMissing?: number;
};

export function readOptionValue(args: string[], flag: string): ParsedOption {
  const idx = args.findIndex((arg) => arg === flag || arg.startsWith(`${flag}=`));
  if (idx === -1) return { found: false };
  const raw = args[idx];
  if (raw.includes("=")) return { found: true, value: raw.split("=", 2)[1] };
  const next = args[idx + 1];
  if (!next || next.startsWith("--")) return { found: true };
  return { found: true, value: next };
}

function readFirstPositionalArg(args: string[]): string | undefined {
  for (let i = 0; i < args.length; i += 1) {
    const arg = args[i]!;
    if (arg === "--name" || arg === "--hostname" || arg === "--port") {
      i += 1;
      continue;
    }
    if (arg.startsWith("--name=") || arg.startsWith("--hostname=") || arg.startsWith("--port=")) {
      continue;
    }
    if (!arg.startsWith("--")) return arg;
  }
  return undefined;
}

export function parseLocalProcessOptions(
  args: string[],
  opts: ParseLocalProcessArgsOptions = {}
): ParsedLocalProcessOptions {
  const out: ParsedLocalProcessOptions = {};

  const name = readOptionValue(args, "--name");
  if (name.found) out.name = name.value ?? "default";
  else if (opts.allowPositionalName) {
    out.name = readFirstPositionalArg(args) ?? opts.defaultNameWhenMissing;
  } else if (opts.defaultNameWhenMissing != null) {
    out.name = opts.defaultNameWhenMissing;
  }

  const hostname = readOptionValue(args, "--hostname");
  if (hostname.found) out.hostname = hostname.value ?? "127.0.0.1";
  else if (opts.defaultHostnameWhenMissing != null) out.hostname = opts.defaultHostnameWhenMissing;

  const port = readOptionValue(args, "--port");
  if (port.found) out.port = parsePortValue(port.value ?? "0");
  else if (opts.defaultPortWhenMissing != null) out.port = opts.defaultPortWhenMissing;

  return out;
}

export function parsePortValue(raw: string, flag = "--port"): number {
  const port = Number(raw);
  if (!Number.isFinite(port) || port < 0) {
    throw dsError(`invalid ${flag}: ${raw}`);
  }
  return Math.floor(port);
}

export function isPidAlive(pid: number): boolean {
  if (!Number.isInteger(pid) || pid <= 0) return false;
  try {
    process.kill(pid, 0);
    return true;
  } catch (err: any) {
    if (err?.code === "EPERM") return true;
    return false;
  }
}

export async function waitForPidExit(pid: number, timeoutMs = 5_000): Promise<boolean> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (!isPidAlive(pid)) return true;
    await new Promise((resolve) => setTimeout(resolve, 100));
  }
  return !isPidAlive(pid);
}
