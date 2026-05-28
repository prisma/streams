import { Buffer } from "node:buffer";
import { Result } from "better-result";
import type { GitObjectFormat, GitOid } from "./types";

export type GitTreeEntryType = "file" | "dir" | "symlink";

export type GitTreeEntry = {
  name: string;
  mode: "100644" | "100755" | "120000" | "40000" | string;
  type: GitTreeEntryType;
  oid: GitOid;
};

export type GitCommitBody = {
  tree: GitOid;
  parents: GitOid[];
  message: string;
};

export type GitTreeParseError = {
  message: string;
};

const TEXT_DECODER = new TextDecoder();

function oidByteLength(format: GitObjectFormat): number {
  return format === "sha256" ? 32 : 20;
}

function oidPattern(format: GitObjectFormat): RegExp {
  return format === "sha256" ? /^[0-9a-f]{64}$/ : /^[0-9a-f]{40}$/;
}

function entryType(mode: string): GitTreeEntryType {
  if (mode === "40000") return "dir";
  if (mode === "120000") return "symlink";
  return "file";
}

export function parseGitCommitBodyResult(body: Uint8Array, format: GitObjectFormat): Result<GitCommitBody, GitTreeParseError> {
  const text = TEXT_DECODER.decode(body);
  const splitAt = text.indexOf("\n\n");
  if (splitAt < 0) return Result.err({ message: "git commit is missing message separator" });
  const header = text.slice(0, splitAt);
  const message = text.slice(splitAt + 2);
  let tree: string | null = null;
  const parents: string[] = [];
  for (const line of header.split("\n")) {
    if (line.startsWith("tree ")) tree = line.slice("tree ".length);
    else if (line.startsWith("parent ")) parents.push(line.slice("parent ".length));
  }
  if (!tree || !oidPattern(format).test(tree)) return Result.err({ message: "git commit tree oid is invalid" });
  for (const parent of parents) {
    if (!oidPattern(format).test(parent)) return Result.err({ message: "git commit parent oid is invalid" });
  }
  return Result.ok({ tree, parents, message });
}

export function parseGitTreeBodyResult(body: Uint8Array, format: GitObjectFormat): Result<GitTreeEntry[], GitTreeParseError> {
  const oidBytes = oidByteLength(format);
  const entries: GitTreeEntry[] = [];
  let offset = 0;
  while (offset < body.byteLength) {
    const modeEnd = body.indexOf(0x20, offset);
    if (modeEnd < 0) return Result.err({ message: "git tree entry is missing mode separator" });
    const mode = TEXT_DECODER.decode(body.slice(offset, modeEnd));
    const nameStart = modeEnd + 1;
    const nameEnd = body.indexOf(0, nameStart);
    if (nameEnd < 0) return Result.err({ message: "git tree entry is missing name terminator" });
    const name = TEXT_DECODER.decode(body.slice(nameStart, nameEnd));
    if (name === "" || name.includes("/")) return Result.err({ message: "git tree entry name is invalid" });
    const oidStart = nameEnd + 1;
    const oidEnd = oidStart + oidBytes;
    if (oidEnd > body.byteLength) return Result.err({ message: "git tree entry oid is truncated" });
    const oid = Buffer.from(body.slice(oidStart, oidEnd)).toString("hex");
    entries.push({ name, mode, type: entryType(mode), oid });
    offset = oidEnd;
  }
  return Result.ok(entries);
}
