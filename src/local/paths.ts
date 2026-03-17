import envPaths from "env-paths";
import { join, resolve } from "node:path";
import { dsError } from "../util/ds_error.ts";

export function normalizeServerName(name: string | undefined): string {
  const trimmed = (name ?? "default").trim();
  if (trimmed.length === 0) return "default";
  if (!/^[A-Za-z0-9._-]+$/.test(trimmed)) {
    throw dsError(`invalid server name: ${name}`);
  }
  return trimmed;
}

export function getDurableStreamsDataRoot(): string {
  const override = process.env.DS_LOCAL_DATA_ROOT;
  if (override && override.trim().length > 0) return resolve(override);
  const root = envPaths("prisma-dev").data;
  return join(root, "durable-streams");
}

export function getServerDataDir(name: string): string {
  return join(getDurableStreamsDataRoot(), normalizeServerName(name));
}
