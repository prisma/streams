export type PutResult = { etag: string };

export type GetRange = { start: number; end?: number }; // end is inclusive; omit for EOF
export type GetOptions = { range?: GetRange };

export interface ObjectStore {
  put(key: string, data: Uint8Array, opts?: { contentType?: string; contentLength?: number }): Promise<PutResult>;
  putFile?(key: string, path: string, size: number, opts?: { contentType?: string }): Promise<PutResult>;
  get(key: string, opts?: GetOptions): Promise<Uint8Array | null>;
  head(key: string): Promise<{ etag: string; size: number } | null>;
  delete(key: string): Promise<void>;
  list(prefix: string): Promise<string[]>;
}
