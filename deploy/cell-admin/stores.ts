// The store `--state` names: a local directory, or
// `s3://<bucket>/cells/<cell id>` with the bucket keys in the file
// `--s3-credentials` names.
import { resolve } from "node:path";
import type { Flags } from "./args";
import { AdminError } from "./errors";
import { readS3Credentials, S3Store } from "./s3store";
import { DirStore, type StateStore } from "./store";

/// What the operator does with a bundle the store just wrote: a directory's
/// is uploaded to the cell's FEEDS key by hand; an s3 state's bundle is
/// the object the cell's wrapper polls.
export const publishNote = (store: StateStore) =>
  store.bundlePath.startsWith("s3://") ? "the cell's wrapper polls it there" : "publish it to the cell";

/// `create` is for `init` only: a directory store makes its directory.
export async function openStore(f: Flags, opts: { create?: boolean } = {}): Promise<StateStore> {
  const where = f.must("state");
  if (where.startsWith("s3://")) {
    const creds = f.get("s3-credentials");
    if (creds === undefined) throw new AdminError(`--state ${where} needs --s3-credentials FILE`, 2);
    return S3Store.open(where, readS3Credentials(resolve(creds)));
  }
  if (f.has("s3-credentials")) throw new AdminError("--s3-credentials goes with an s3:// --state only", 2);
  return new DirStore(resolve(where), opts.create ?? false);
}
