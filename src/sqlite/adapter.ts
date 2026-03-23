import { createRequire } from "node:module";
import type { HostRuntime } from "../runtime/host_runtime.ts";
import { detectHostRuntime } from "../runtime/host_runtime.ts";
import { dsError } from "../util/ds_error.ts";
export interface SqliteStatement {
  get(...params: any[]): any;
  all(...params: any[]): any[];
  run(...params: any[]): { changes: number | bigint; lastInsertRowid: number | bigint };
  iterate(...params: any[]): Iterable<any>;
  finalize?(): void;
}

export interface SqliteDatabase {
  exec(sql: string): void;
  query(sql: string): SqliteStatement;
  transaction<T>(fn: () => T): () => T;
  close(): void;
}

class BunStatementAdapter implements SqliteStatement {
  private readonly stmt: any;

  constructor(stmt: any) {
    this.stmt = stmt;
  }

  get(...params: any[]): any {
    return this.stmt.get(...params);
  }

  all(...params: any[]): any[] {
    return this.stmt.all(...params);
  }

  run(...params: any[]): { changes: number | bigint; lastInsertRowid: number | bigint } {
    return this.stmt.run(...params);
  }

  iterate(...params: any[]): Iterable<any> {
    return this.stmt.iterate(...params);
  }

  finalize(): void {
    if (typeof this.stmt.finalize === "function") this.stmt.finalize();
  }
}

class BunDatabaseAdapter implements SqliteDatabase {
  private readonly db: any;

  constructor(db: any) {
    this.db = db;
  }

  exec(sql: string): void {
    this.db.exec(sql);
  }

  query(sql: string): SqliteStatement {
    return new BunStatementAdapter(this.db.query(sql));
  }

  transaction<T>(fn: () => T): () => T {
    return this.db.transaction(fn);
  }

  close(): void {
    this.db.close();
  }
}

class NodeStatementAdapter implements SqliteStatement {
  private readonly stmt: any;

  constructor(stmt: any) {
    this.stmt = stmt;
  }

  get(...params: any[]): any {
    return this.stmt.get(...params);
  }

  all(...params: any[]): any[] {
    return this.stmt.all(...params);
  }

  run(...params: any[]): { changes: number | bigint; lastInsertRowid: number | bigint } {
    return this.stmt.run(...params);
  }

  iterate(...params: any[]): Iterable<any> {
    return this.stmt.iterate(...params);
  }

  finalize(): void {
    if (typeof this.stmt.finalize === "function") this.stmt.finalize();
  }
}

class NodeDatabaseAdapter implements SqliteDatabase {
  private txDepth = 0;
  private txCounter = 0;
  private readonly db: any;

  constructor(db: any) {
    this.db = db;
  }

  exec(sql: string): void {
    this.db.exec(sql);
  }

  query(sql: string): SqliteStatement {
    const stmt = this.db.prepare(sql);
    if (typeof stmt?.setReadBigInts === "function") stmt.setReadBigInts(true);
    return new NodeStatementAdapter(stmt);
  }

  transaction<T>(fn: () => T): () => T {
    return () => {
      const nested = this.txDepth > 0;
      const savepoint = `ds_tx_${++this.txCounter}`;
      this.txDepth += 1;
      try {
        if (nested) this.db.exec(`SAVEPOINT ${savepoint};`);
        else this.db.exec("BEGIN;");
        const out = fn();
        if (nested) this.db.exec(`RELEASE SAVEPOINT ${savepoint};`);
        else this.db.exec("COMMIT;");
        return out;
      } catch (err) {
        try {
          if (nested) {
            this.db.exec(`ROLLBACK TO SAVEPOINT ${savepoint};`);
            this.db.exec(`RELEASE SAVEPOINT ${savepoint};`);
          } else {
            this.db.exec("ROLLBACK;");
          }
        } catch {
          // Ignore secondary rollback failures.
        }
        throw err;
      } finally {
        this.txDepth = Math.max(0, this.txDepth - 1);
      }
    };
  }

  close(): void {
    this.db.close();
  }
}

let openImpl: ((path: string) => SqliteDatabase) | null = null;
let openImplRuntime: HostRuntime | null = null;
let runtimeOverride: HostRuntime | null = null;
const require = createRequire(import.meta.url);

function selectedRuntime(): HostRuntime {
  return runtimeOverride ?? detectHostRuntime();
}

function buildOpenImpl(runtime: HostRuntime): (path: string) => SqliteDatabase {
  if (runtime === "bun") {
    const { Database } = require("bun:sqlite") as { Database: new (path: string) => any };
    return (path: string) => new BunDatabaseAdapter(new Database(path));
  }
  const { DatabaseSync } = require("node:sqlite") as { DatabaseSync: new (path: string) => any };
  return (path: string) => new NodeDatabaseAdapter(new DatabaseSync(path));
}

export function setSqliteRuntimeOverride(runtime: HostRuntime | null): void {
  runtimeOverride = runtime;
  if (runtimeOverride && openImplRuntime && runtimeOverride !== openImplRuntime) {
    openImpl = null;
    openImplRuntime = null;
  }
}

export function openSqliteDatabase(path: string): SqliteDatabase {
  const runtime = selectedRuntime();
  if (!openImpl || openImplRuntime !== runtime) {
    openImpl = buildOpenImpl(runtime);
    openImplRuntime = runtime;
  }
  if (!openImpl) throw dsError("sqlite adapter not initialized");
  return openImpl(path);
}
