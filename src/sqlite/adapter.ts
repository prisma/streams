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
  prepare(sql: string): SqliteStatement;
  transaction<T>(fn: () => T): () => T;
  close(): void;
}

type SqliteAdapterRuntimeCounts = {
  open_connections: number;
  prepared_statements: number;
};

const sqliteAdapterRuntimeCounts: SqliteAdapterRuntimeCounts = {
  open_connections: 0,
  prepared_statements: 0,
};

function incrementSqliteConnection(): void {
  sqliteAdapterRuntimeCounts.open_connections += 1;
}

function decrementSqliteConnection(): void {
  sqliteAdapterRuntimeCounts.open_connections = Math.max(0, sqliteAdapterRuntimeCounts.open_connections - 1);
}

function incrementPreparedStatement(): void {
  sqliteAdapterRuntimeCounts.prepared_statements += 1;
}

function decrementPreparedStatement(): void {
  sqliteAdapterRuntimeCounts.prepared_statements = Math.max(0, sqliteAdapterRuntimeCounts.prepared_statements - 1);
}

export function getSqliteAdapterRuntimeCounts(): SqliteAdapterRuntimeCounts {
  return { ...sqliteAdapterRuntimeCounts };
}

class BunStatementAdapter implements SqliteStatement {
  private readonly stmt: any;
  private readonly onFinalize?: () => void;
  private finalized = false;

  constructor(stmt: any, onFinalize?: () => void) {
    this.stmt = stmt;
    this.onFinalize = onFinalize;
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
    if (this.finalized) return;
    this.finalized = true;
    try {
      if (typeof this.stmt.finalize === "function") this.stmt.finalize();
    } finally {
      this.onFinalize?.();
    }
  }
}

class BunDatabaseAdapter implements SqliteDatabase {
  private readonly db: any;
  private preparedStatementCount = 0;
  private closed = false;
  private readonly statementCache = new Map<string, BunStatementAdapter>();

  constructor(db: any) {
    this.db = db;
    incrementSqliteConnection();
  }

  private trackStatement(): () => void {
    let released = false;
    this.preparedStatementCount += 1;
    incrementPreparedStatement();
    return () => {
      if (released) return;
      released = true;
      this.preparedStatementCount = Math.max(0, this.preparedStatementCount - 1);
      decrementPreparedStatement();
    };
  }

  exec(sql: string): void {
    this.db.exec(sql);
  }

  prepare(sql: string): SqliteStatement {
    return new BunStatementAdapter(this.db.query(sql), this.trackStatement());
  }

  query(sql: string): SqliteStatement {
    const cached = this.statementCache.get(sql);
    if (cached) return cached;
    let adapter: BunStatementAdapter;
    const release = this.trackStatement();
    adapter = new BunStatementAdapter(this.db.query(sql), () => {
      this.statementCache.delete(sql);
      release();
    });
    this.statementCache.set(sql, adapter);
    return adapter;
  }

  transaction<T>(fn: () => T): () => T {
    return this.db.transaction(fn);
  }

  close(): void {
    if (this.closed) return;
    this.closed = true;
    const cachedStatements = Array.from(this.statementCache.values());
    this.statementCache.clear();
    for (const stmt of cachedStatements) {
      try {
        stmt.finalize?.();
      } catch {
        // Ignore finalizer failures during shutdown.
      }
    }
    while (this.preparedStatementCount > 0) {
      this.preparedStatementCount -= 1;
      decrementPreparedStatement();
    }
    try {
      this.db.close();
    } finally {
      decrementSqliteConnection();
    }
  }
}

class NodeStatementAdapter implements SqliteStatement {
  private readonly stmt: any;
  private readonly onFinalize?: () => void;
  private finalized = false;

  constructor(stmt: any, onFinalize?: () => void) {
    this.stmt = stmt;
    this.onFinalize = onFinalize;
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
    if (this.finalized) return;
    this.finalized = true;
    try {
      if (typeof this.stmt.finalize === "function") this.stmt.finalize();
    } finally {
      this.onFinalize?.();
    }
  }
}

class NodeDatabaseAdapter implements SqliteDatabase {
  private txDepth = 0;
  private txCounter = 0;
  private readonly db: any;
  private preparedStatementCount = 0;
  private closed = false;
  private readonly statementCache = new Map<string, NodeStatementAdapter>();

  constructor(db: any) {
    this.db = db;
    incrementSqliteConnection();
  }

  private trackStatement(): () => void {
    let released = false;
    this.preparedStatementCount += 1;
    incrementPreparedStatement();
    return () => {
      if (released) return;
      released = true;
      this.preparedStatementCount = Math.max(0, this.preparedStatementCount - 1);
      decrementPreparedStatement();
    };
  }

  exec(sql: string): void {
    this.db.exec(sql);
  }

  prepare(sql: string): SqliteStatement {
    const stmt = this.db.prepare(sql);
    if (typeof stmt?.setReadBigInts === "function") stmt.setReadBigInts(true);
    return new NodeStatementAdapter(stmt, this.trackStatement());
  }

  query(sql: string): SqliteStatement {
    const cached = this.statementCache.get(sql);
    if (cached) return cached;
    const stmt = this.db.prepare(sql);
    if (typeof stmt?.setReadBigInts === "function") stmt.setReadBigInts(true);
    let adapter: NodeStatementAdapter;
    const release = this.trackStatement();
    adapter = new NodeStatementAdapter(stmt, () => {
      this.statementCache.delete(sql);
      release();
    });
    this.statementCache.set(sql, adapter);
    return adapter;
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
    if (this.closed) return;
    this.closed = true;
    const cachedStatements = Array.from(this.statementCache.values());
    this.statementCache.clear();
    for (const stmt of cachedStatements) {
      try {
        stmt.finalize?.();
      } catch {
        // Ignore finalizer failures during shutdown.
      }
    }
    while (this.preparedStatementCount > 0) {
      this.preparedStatementCount -= 1;
      decrementPreparedStatement();
    }
    try {
      this.db.close();
    } finally {
      decrementSqliteConnection();
    }
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
