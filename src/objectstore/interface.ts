export type PutResult = { etag: string };
export type GetFileResult = { size: number };

export type GetRange = { start: number; end?: number }; // end is inclusive; omit for EOF
export type GetOptions = { range?: GetRange };
export type PutOptions = { contentType?: string; contentLength?: number; signal?: AbortSignal };
export type PutFileOptions = { contentType?: string; signal?: AbortSignal };

export interface ObjectStore {
  put(key: string, data: Uint8Array, opts?: PutOptions): Promise<PutResult>;
  putFile?(key: string, path: string, size: number, opts?: PutFileOptions): Promise<PutResult>;
  get(key: string, opts?: GetOptions): Promise<Uint8Array | null>;
  getFile?(key: string, path: string, opts?: GetOptions): Promise<GetFileResult | null>;
  head(key: string): Promise<{ etag: string; size: number } | null>;
  delete(key: string): Promise<void>;
  list(prefix: string): Promise<string[]>;
}
