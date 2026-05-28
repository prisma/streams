import type { GitObjectFormat, GitOid } from "./types";
import { streamHash16Hex } from "../util/stream_paths";

export type GitArtifactPointer = {
  uri: string;
  format: GitObjectFormat;
  objectCount: number;
  bytes: number;
};

export type GitLooseObjectPointer = {
  oid: GitOid;
  uri: string;
  size: number;
};

export function gitLooseObjectPath(oid: GitOid): string {
  return `objects/${oid.slice(0, 2)}/${oid.slice(2)}`;
}

export function gitObjectArtifactPrefix(repoStream: string, format: GitObjectFormat): string {
  return `streams/${streamHash16Hex(repoStream)}/git/${format}`;
}

export function gitLooseObjectKey(repoStream: string, format: GitObjectFormat, oid: GitOid): string {
  return `${gitObjectArtifactPrefix(repoStream, format)}/${gitLooseObjectPath(oid)}`;
}

export function gitPackPath(packId: string): string {
  return `packs/${packId}.pack`;
}

export function gitPackIndexPath(packId: string): string {
  return `packs/${packId}.idx`;
}
