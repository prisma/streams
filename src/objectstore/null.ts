import type { GetFileResult, GetOptions, ObjectStore, PutFileOptions, PutOptions, PutResult } from "./interface";
import { dsError } from "../util/ds_error.ts";

function disabled(op: string, key?: string): never {
  throw dsError(`object store disabled in local mode (${op}${key ? `: ${key}` : ""})`);
}

export class NullObjectStore implements ObjectStore {
  async put(key: string, _data: Uint8Array, _opts?: PutOptions): Promise<PutResult> {
    return disabled("put", key);
  }

  async putFile(key: string, _path: string, _size: number, _opts?: PutFileOptions): Promise<PutResult> {
    return disabled("putFile", key);
  }

  async get(key: string, _opts?: GetOptions): Promise<Uint8Array | null> {
    return disabled("get", key);
  }

  async getFile(key: string, _path: string, _opts?: GetOptions): Promise<GetFileResult | null> {
    return disabled("getFile", key);
  }

  async head(key: string): Promise<{ etag: string; size: number } | null> {
    return disabled("head", key);
  }

  async delete(key: string): Promise<void> {
    return disabled("delete", key);
  }

  async list(prefix: string): Promise<string[]> {
    return disabled("list", prefix);
  }
}
