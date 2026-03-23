export type HostRuntime = "bun" | "node";

export function detectHostRuntime(): HostRuntime {
  return typeof (globalThis as any).Bun !== "undefined" || Boolean(process.versions?.bun) ? "bun" : "node";
}
