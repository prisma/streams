import { Buffer } from "node:buffer";
import { createHash } from "node:crypto";
import { Result } from "better-result";
import type { GitObjectFormat, GitObjectType, GitOid } from "./types";

export type GitObject = {
  type: GitObjectType;
  oid: GitOid;
  size: number;
  body: Uint8Array;
  framed: Uint8Array;
};

export type GitTreeEntryInput = {
  mode: "100644" | "100755" | "120000" | "40000";
  name: string;
  oid: GitOid;
};

export type GitPerson = {
  name: string;
  email: string;
  timestampSeconds: number;
  timezone: string;
};

export type GitCommitInput = {
  tree: GitOid;
  parents?: GitOid[];
  author: GitPerson;
  committer?: GitPerson;
  message: string;
};

export type GitObjectError = {
  message: string;
};

function algorithm(format: GitObjectFormat): "sha1" | "sha256" {
  return format === "sha256" ? "sha256" : "sha1";
}

function oidByteLength(format: GitObjectFormat): number {
  return format === "sha256" ? 32 : 20;
}

function oidPattern(format: GitObjectFormat): RegExp {
  return format === "sha256" ? /^[0-9a-f]{64}$/ : /^[0-9a-f]{40}$/;
}

function asBytes(bytes: Uint8Array | string): Uint8Array {
  if (typeof bytes !== "string") return bytes;
  return new TextEncoder().encode(bytes);
}

export function frameGitObject(type: GitObjectType, body: Uint8Array): Uint8Array {
  const header = new TextEncoder().encode(`${type} ${body.byteLength}\0`);
  const framed = new Uint8Array(header.byteLength + body.byteLength);
  framed.set(header, 0);
  framed.set(body, header.byteLength);
  return framed;
}

export function hashGitObject(type: GitObjectType, body: Uint8Array, format: GitObjectFormat = "sha1"): GitOid {
  return createHash(algorithm(format)).update(frameGitObject(type, body)).digest("hex");
}

export function buildGitObject(type: GitObjectType, body: Uint8Array | string, format: GitObjectFormat = "sha1"): GitObject {
  const raw = asBytes(body);
  return {
    type,
    oid: hashGitObject(type, raw, format),
    size: raw.byteLength,
    body: raw,
    framed: frameGitObject(type, raw),
  };
}

export function writeGitBlob(bytes: Uint8Array | string, format: GitObjectFormat = "sha1"): GitObject {
  return buildGitObject("blob", bytes, format);
}

function validateOidResult(oid: string, format: GitObjectFormat): Result<void, GitObjectError> {
  if (!oidPattern(format).test(oid)) return Result.err({ message: `invalid ${format} object id` });
  return Result.ok(undefined);
}

function validateTreeEntryResult(entry: GitTreeEntryInput, format: GitObjectFormat): Result<void, GitObjectError> {
  if (!/^[^/\0]+$/.test(entry.name)) return Result.err({ message: "tree entry name must be one path segment" });
  return validateOidResult(entry.oid, format);
}

export function encodeGitTreeResult(entries: GitTreeEntryInput[], format: GitObjectFormat = "sha1"): Result<Uint8Array, GitObjectError> {
  const sorted = entries.slice().sort((a, b) => Buffer.compare(Buffer.from(a.name), Buffer.from(b.name)));
  const parts: Buffer[] = [];
  for (const entry of sorted) {
    const entryRes = validateTreeEntryResult(entry, format);
    if (Result.isError(entryRes)) return entryRes;
    parts.push(Buffer.from(`${entry.mode} ${entry.name}\0`));
    const oidBytes = Buffer.from(entry.oid, "hex");
    if (oidBytes.byteLength !== oidByteLength(format)) return Result.err({ message: `invalid ${format} object id byte length` });
    parts.push(oidBytes);
  }
  return Result.ok(Buffer.concat(parts));
}

export function writeGitTreeResult(entries: GitTreeEntryInput[], format: GitObjectFormat = "sha1"): Result<GitObject, GitObjectError> {
  const bodyRes = encodeGitTreeResult(entries, format);
  if (Result.isError(bodyRes)) return bodyRes;
  return Result.ok(buildGitObject("tree", bodyRes.value, format));
}

function formatPersonResult(person: GitPerson): Result<string, GitObjectError> {
  if (person.name.trim() === "") return Result.err({ message: "git person name is required" });
  if (person.email.trim() === "" || person.email.includes(">") || person.email.includes("<")) {
    return Result.err({ message: "git person email is invalid" });
  }
  if (!Number.isFinite(person.timestampSeconds)) return Result.err({ message: "git person timestamp is invalid" });
  if (!/^[+-][0-9]{4}$/.test(person.timezone)) return Result.err({ message: "git person timezone must look like +0000" });
  return Result.ok(`${person.name} <${person.email}> ${Math.floor(person.timestampSeconds)} ${person.timezone}`);
}

export function encodeGitCommitResult(input: GitCommitInput, format: GitObjectFormat = "sha1"): Result<Uint8Array, GitObjectError> {
  const treeRes = validateOidResult(input.tree, format);
  if (Result.isError(treeRes)) return treeRes;
  const authorRes = formatPersonResult(input.author);
  if (Result.isError(authorRes)) return authorRes;
  const committerRes = formatPersonResult(input.committer ?? input.author);
  if (Result.isError(committerRes)) return committerRes;
  const lines = [`tree ${input.tree}`];
  for (const parent of input.parents ?? []) {
    const parentRes = validateOidResult(parent, format);
    if (Result.isError(parentRes)) return parentRes;
    lines.push(`parent ${parent}`);
  }
  lines.push(`author ${authorRes.value}`);
  lines.push(`committer ${committerRes.value}`);
  lines.push("");
  lines.push(input.message.endsWith("\n") ? input.message.slice(0, -1) : input.message);
  lines.push("");
  return Result.ok(new TextEncoder().encode(lines.join("\n")));
}

export function writeGitCommitResult(input: GitCommitInput, format: GitObjectFormat = "sha1"): Result<GitObject, GitObjectError> {
  const bodyRes = encodeGitCommitResult(input, format);
  if (Result.isError(bodyRes)) return bodyRes;
  return Result.ok(buildGitObject("commit", bodyRes.value, format));
}
