import { xxh3BigInt, xxh3Hex } from "../runtime/hash.ts";
export type TemplateEncoding = "string" | "int64" | "bool" | "datetime" | "bytes";

function utf8(s: string): Uint8Array {
  return new TextEncoder().encode(s);
}

function encodeU64Be(v: bigint): Uint8Array {
  const out = new Uint8Array(8);
  let x = v;
  for (let i = 7; i >= 0; i--) {
    out[i] = Number(x & 0xffn);
    x >>= 8n;
  }
  return out;
}

function xxh3Low32(bytes: Uint8Array): number {
  const h = xxh3BigInt(bytes);
  return Number(h & 0xffffffffn) >>> 0;
}

export function canonicalizeTemplateFields(fields: Array<{ name: string; encoding: TemplateEncoding }>): Array<{ name: string; encoding: TemplateEncoding }> {
  const out = [...fields].map((f) => ({ name: f.name, encoding: f.encoding }));
  out.sort((a, b) => (a.name < b.name ? -1 : a.name > b.name ? 1 : 0));
  return out;
}

export function templateIdFor(entity: string, fieldNamesSorted: string[]): string {
  const parts: Uint8Array[] = [utf8("tpl\0"), utf8(entity), utf8("\0")];
  for (let i = 0; i < fieldNamesSorted.length; i++) {
    if (i > 0) parts.push(utf8("\0"));
    parts.push(utf8(fieldNamesSorted[i]));
  }
  return xxh3Hex(concat(parts));
}

export function tableKeyFor(entity: string): string {
  return xxh3Hex(concat([utf8("tbl\0"), utf8(entity)]));
}

export function tableKeyIdFor(entity: string): number {
  return xxh3Low32(concat([utf8("tbl\0"), utf8(entity)]));
}

export function templateKeyFor(templateIdHex16: string): string {
  const tplBytes = encodeU64Be(BigInt(`0x${templateIdHex16}`));
  return xxh3Hex(concat([utf8("tpl\0"), tplBytes]));
}

export function templateKeyIdFor(templateIdHex16: string): number {
  const tplBytes = encodeU64Be(BigInt(`0x${templateIdHex16}`));
  return xxh3Low32(concat([utf8("tpl\0"), tplBytes]));
}

export function membershipKeyFor(templateIdHex16: string, encodedArgs: string[]): string {
  const tplBytes = encodeU64Be(BigInt(`0x${templateIdHex16}`));
  const parts: Uint8Array[] = [utf8("mem\0"), tplBytes];
  for (const a of encodedArgs) {
    parts.push(utf8("\0"));
    parts.push(utf8(a));
  }
  return xxh3Hex(concat(parts));
}

export function membershipKeyIdFor(templateIdHex16: string, encodedArgs: string[]): number {
  const tplBytes = encodeU64Be(BigInt(`0x${templateIdHex16}`));
  const parts: Uint8Array[] = [utf8("mem\0"), tplBytes];
  for (const a of encodedArgs) {
    parts.push(utf8("\0"));
    parts.push(utf8(a));
  }
  return xxh3Low32(concat(parts));
}

export function watchKeyFor(templateIdHex16: string, encodedArgs: string[]): string {
  const tplBytes = encodeU64Be(BigInt(`0x${templateIdHex16}`));
  const parts: Uint8Array[] = [utf8("key\0"), tplBytes];
  for (const a of encodedArgs) {
    parts.push(utf8("\0"));
    parts.push(utf8(a));
  }
  return xxh3Hex(concat(parts));
}

export function watchKeyIdFor(templateIdHex16: string, encodedArgs: string[]): number {
  const tplBytes = encodeU64Be(BigInt(`0x${templateIdHex16}`));
  const parts: Uint8Array[] = [utf8("key\0"), tplBytes];
  for (const a of encodedArgs) {
    parts.push(utf8("\0"));
    parts.push(utf8(a));
  }
  return xxh3Low32(concat(parts));
}

export function encodeTemplateArg(value: unknown, encoding: TemplateEncoding): string | null {
  if (value === null || value === undefined) return null;
  switch (encoding) {
    case "string": {
      if (typeof value === "string") return value;
      if (typeof value === "number" && Number.isFinite(value)) return String(value);
      if (typeof value === "boolean") return value ? "true" : "false";
      return null;
    }
    case "int64": {
      if (typeof value === "bigint") return value.toString();
      if (typeof value === "number" && Number.isFinite(value) && Number.isInteger(value)) return String(value);
      if (typeof value === "string" && /^-?(0|[1-9][0-9]*)$/.test(value.trim())) return value.trim();
      return null;
    }
    case "bool": {
      if (typeof value !== "boolean") return null;
      return value ? "1" : "0";
    }
    case "datetime": {
      if (typeof value !== "string") return null;
      const d = new Date(value);
      if (!Number.isFinite(d.getTime())) return null;
      return d.toISOString();
    }
    case "bytes": {
      if (typeof value !== "string") return null;
      return value;
    }
  }
}

function concat(parts: Uint8Array[]): Uint8Array {
  let total = 0;
  for (const p of parts) total += p.byteLength;
  const out = new Uint8Array(total);
  let off = 0;
  for (const p of parts) {
    out.set(p, off);
    off += p.byteLength;
  }
  return out;
}
