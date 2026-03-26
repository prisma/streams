import { Result } from "better-result";
import { applyLensChainResult } from "../lens/lens";
import type { SchemaRegistry } from "./registry";
import { SchemaRegistryStore } from "./registry";

export function schemaVersionForOffset(reg: SchemaRegistry, offset: bigint): number {
  if (!reg.boundaries || reg.boundaries.length === 0) return 0;
  const off = Number(offset);
  let version = 0;
  for (const boundary of reg.boundaries) {
    if (boundary.offset <= off) version = boundary.version;
    else break;
  }
  return version;
}

export function decodeJsonPayloadResult(
  registry: SchemaRegistryStore,
  stream: string,
  offset: bigint,
  payload: Uint8Array
): Result<any, { status: 400 | 500; message: string }> {
  const regRes = registry.getRegistryResult(stream);
  if (Result.isError(regRes)) return Result.err({ status: 500, message: regRes.error.message });
  const reg = regRes.value;
  try {
    const text = new TextDecoder().decode(payload);
    let value: any = JSON.parse(text);
    if (reg.currentVersion > 0) {
      const version = schemaVersionForOffset(reg, offset);
      if (version < reg.currentVersion) {
        const chainRes = registry.getLensChainResult(reg, version, reg.currentVersion);
        if (Result.isError(chainRes)) return Result.err({ status: 500, message: chainRes.error.message });
        const transformedRes = applyLensChainResult(chainRes.value, value);
        if (Result.isError(transformedRes)) return Result.err({ status: 400, message: transformedRes.error.message });
        value = transformedRes.value;
      }
    }
    return Result.ok(value);
  } catch (e: unknown) {
    return Result.err({ status: 400, message: String((e as any)?.message ?? e) });
  }
}
