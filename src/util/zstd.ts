import * as zlib from "node:zlib";
import { dsError } from "./ds_error.ts";

type ZstdOperation = (bytes: Uint8Array) => Uint8Array | Buffer;

function getZstdOperation(name: "zstdCompressSync" | "zstdDecompressSync") {
  const operation = (zlib as Record<string, unknown>)[name];

  if (typeof operation !== "function") {
    throw dsError(
      `${name} is not available in this runtime. Prisma Streams local requires node:zlib zstd support.`,
    );
  }

  return operation as ZstdOperation;
}

export function zstdCompressSync(bytes: Uint8Array) {
  return getZstdOperation("zstdCompressSync")(bytes);
}

export function zstdDecompressSync(bytes: Uint8Array) {
  return getZstdOperation("zstdDecompressSync")(bytes);
}
