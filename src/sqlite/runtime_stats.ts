import { createRequire } from "node:module";
import { detectHostRuntime } from "../runtime/host_runtime.ts";
import { getSqliteAdapterRuntimeCounts } from "./adapter.ts";
import type { SqliteRuntimeMemoryStats } from "../runtime_memory.ts";

const SQLITE_STATUS_MEMORY_USED = 0;
const SQLITE_STATUS_PAGECACHE_USED = 1;
const SQLITE_STATUS_PAGECACHE_OVERFLOW = 2;
const SQLITE_STATUS_MALLOC_COUNT = 9;

type SqliteStatus64Fn = (op: number, currentPtr: number, highwaterPtr: number, resetFlag: number) => number;
type BunFfiModule = {
  dlopen: (
    path: string,
    symbols: {
      sqlite3_status64: {
        args: ["i32", "ptr", "ptr", "i32"];
        returns: "i32";
      };
    }
  ) => { symbols: { sqlite3_status64: SqliteStatus64Fn }; close: () => void };
  ptr: (value: TypedArray) => number;
};

type TypedArray = BigInt64Array | BigUint64Array | Int32Array | Uint32Array;

type SqliteLibraryHandle = {
  sqlite3_status64: SqliteStatus64Fn;
  close: () => void;
};

type CachedSqliteRuntimeStats = {
  at_ms: number;
  value: SqliteRuntimeMemoryStats;
};

let ffiModule: BunFfiModule | null | undefined;
let sqliteLibrary: SqliteLibraryHandle | null | undefined;
let cachedRuntimeStats: CachedSqliteRuntimeStats | null = null;
const require = createRequire(import.meta.url);

function sqliteLibraryNames(): string[] {
  if (process.platform === "darwin") {
    return ["libsqlite3.dylib", "/usr/lib/libsqlite3.dylib"];
  }
  if (process.platform === "linux") {
    return ["libsqlite3.so.0", "libsqlite3.so", "/usr/lib/x86_64-linux-gnu/libsqlite3.so.0", "/lib/x86_64-linux-gnu/libsqlite3.so.0"];
  }
  return ["libsqlite3.so", "libsqlite3.dylib"];
}

function loadBunFfi(): BunFfiModule | null {
  if (ffiModule !== undefined) return ffiModule;
  if (detectHostRuntime() !== "bun") {
    ffiModule = null;
    return ffiModule;
  }
  try {
    ffiModule = require("bun:ffi") as BunFfiModule;
  } catch {
    ffiModule = null;
  }
  return ffiModule;
}

function loadSqliteLibrary(): SqliteLibraryHandle | null {
  if (sqliteLibrary !== undefined) return sqliteLibrary;
  const ffi = loadBunFfi();
  if (!ffi) {
    sqliteLibrary = null;
    return sqliteLibrary;
  }
  for (const name of sqliteLibraryNames()) {
    try {
      const lib = ffi.dlopen(name, {
        sqlite3_status64: {
          args: ["i32", "ptr", "ptr", "i32"],
          returns: "i32",
        },
      });
      sqliteLibrary = {
        sqlite3_status64: lib.symbols.sqlite3_status64,
        close: () => lib.close(),
      };
      return sqliteLibrary;
    } catch {
      // Try the next library name.
    }
  }
  sqliteLibrary = null;
  return sqliteLibrary;
}

function readStatus64(
  lib: SqliteLibraryHandle,
  ffi: BunFfiModule,
  op: number
): { current: number; highwater: number } | null {
  const current = new BigInt64Array(1);
  const highwater = new BigInt64Array(1);
  try {
    const rc = lib.sqlite3_status64(op, ffi.ptr(current), ffi.ptr(highwater), 0);
    if (rc !== 0) return null;
    return {
      current: Number(current[0]),
      highwater: Number(highwater[0]),
    };
  } catch {
    return null;
  }
}

export function readSqliteRuntimeMemoryStats(ttlMs = 1_000): SqliteRuntimeMemoryStats {
  const now = Date.now();
  if (cachedRuntimeStats && now - cachedRuntimeStats.at_ms < Math.max(0, ttlMs)) {
    return cachedRuntimeStats.value;
  }

  const adapterCounts = getSqliteAdapterRuntimeCounts();
  const unavailable: SqliteRuntimeMemoryStats = {
    available: false,
    source: "unavailable",
    memory_used_bytes: 0,
    memory_highwater_bytes: 0,
    pagecache_used_slots: 0,
    pagecache_used_slots_highwater: 0,
    pagecache_overflow_bytes: 0,
    pagecache_overflow_highwater_bytes: 0,
    malloc_count: 0,
    malloc_count_highwater: 0,
    open_connections: adapterCounts.open_connections,
    prepared_statements: adapterCounts.prepared_statements,
  };

  const ffi = loadBunFfi();
  const lib = loadSqliteLibrary();
  if (!ffi || !lib) {
    cachedRuntimeStats = { at_ms: now, value: unavailable };
    return unavailable;
  }

  const memoryUsed = readStatus64(lib, ffi, SQLITE_STATUS_MEMORY_USED);
  const pagecacheUsed = readStatus64(lib, ffi, SQLITE_STATUS_PAGECACHE_USED);
  const pagecacheOverflow = readStatus64(lib, ffi, SQLITE_STATUS_PAGECACHE_OVERFLOW);
  const mallocCount = readStatus64(lib, ffi, SQLITE_STATUS_MALLOC_COUNT);

  const stats: SqliteRuntimeMemoryStats = {
    available: memoryUsed != null,
    source: memoryUsed != null ? "sqlite3_status64" : "unavailable",
    memory_used_bytes: Math.max(0, Math.floor(memoryUsed?.current ?? 0)),
    memory_highwater_bytes: Math.max(0, Math.floor(memoryUsed?.highwater ?? 0)),
    pagecache_used_slots: Math.max(0, Math.floor(pagecacheUsed?.current ?? 0)),
    pagecache_used_slots_highwater: Math.max(0, Math.floor(pagecacheUsed?.highwater ?? 0)),
    pagecache_overflow_bytes: Math.max(0, Math.floor(pagecacheOverflow?.current ?? 0)),
    pagecache_overflow_highwater_bytes: Math.max(0, Math.floor(pagecacheOverflow?.highwater ?? 0)),
    malloc_count: Math.max(0, Math.floor(mallocCount?.current ?? 0)),
    malloc_count_highwater: Math.max(0, Math.floor(mallocCount?.highwater ?? 0)),
    open_connections: adapterCounts.open_connections,
    prepared_statements: adapterCounts.prepared_statements,
  };
  cachedRuntimeStats = { at_ms: now, value: stats };
  return stats;
}
