import { createHash } from "node:crypto";

export function streamHash16Hex(stream: string): string {
  const full = createHash("sha256").update(stream).digest();
  return full.subarray(0, 16).toString("hex"); // 32 hex chars
}

export function pad16(n: number): string {
  const s = String(n);
  return s.padStart(16, "0");
}

export function segmentObjectKey(streamHash: string, segmentIndex: number): string {
  return `streams/${streamHash}/segments/${pad16(segmentIndex)}.bin`;
}

export function manifestObjectKey(streamHash: string): string {
  return `streams/${streamHash}/manifest.json`;
}

export function schemaObjectKey(streamHash: string): string {
  return `streams/${streamHash}/schema-registry.json`;
}

export function indexRunObjectKey(streamHash: string, runId: string): string {
  return `streams/${streamHash}/index/${runId}.idx`;
}

export function secondaryIndexRunObjectKey(streamHash: string, indexName: string, runId: string): string {
  return `streams/${streamHash}/secondary-index/${encodeURIComponent(indexName)}/${runId}.idx`;
}

export function searchColSegmentObjectKey(streamHash: string, segmentIndex: number, objectId: string): string {
  return `streams/${streamHash}/col/segments/${pad16(segmentIndex)}-${objectId}.col`;
}

export function searchFtsSegmentObjectKey(streamHash: string, segmentIndex: number, objectId: string): string {
  return `streams/${streamHash}/fts/segments/${pad16(segmentIndex)}-${objectId}.fts`;
}

export function searchAggSegmentObjectKey(streamHash: string, segmentIndex: number, objectId: string): string {
  return `streams/${streamHash}/agg/segments/${pad16(segmentIndex)}-${objectId}.agg`;
}

export function localSegmentPath(rootDir: string, streamHash: string, segmentIndex: number): string {
  return `${rootDir}/local/streams/${streamHash}/segments/${pad16(segmentIndex)}.bin`;
}
