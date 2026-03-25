import type { SqliteDurableStore } from "../db/db";
import { canonicalizeTemplateFields, templateIdFor, type TemplateEncoding } from "./live_keys";

export type TemplateFieldSpec = { name: string; encoding: TemplateEncoding };
export type TemplateDecl = { entity: string; fields: TemplateFieldSpec[] };

export type ActivatedTemplate = {
  templateId: string;
  state: "active";
  activeFromTouchOffset: string;
};

export type DeniedTemplate = {
  templateId: string;
  reason: "rate_limited" | "invalid";
};

export type LiveTemplateRow = {
  stream: string;
  template_id: string;
  entity: string;
  fields_json: string;
  encodings_json: string;
  state: string;
  created_at_ms: bigint;
  last_seen_at_ms: bigint;
  inactivity_ttl_ms: bigint;
  active_from_source_offset: bigint;
  retired_at_ms: bigint | null;
  retired_reason: string | null;
};

export type TemplateLifecycleEvent =
  | {
      type: "live.template_activated";
      ts: string;
      stream: string;
      templateId: string;
      entity: string;
      fields: string[];
      encodings: TemplateEncoding[];
      reason: "declared" | "heartbeat";
      activeFromTouchOffset: string;
      inactivityTtlMs: number;
    }
  | {
      type: "live.template_retired";
      ts: string;
      stream: string;
      templateId: string;
      entity: string;
      fields: string[];
      encodings: TemplateEncoding[];
      lastSeenAt: string;
      inactiveForMs: number;
      reason: "inactivity";
    }
  | {
      type: "live.template_evicted";
      ts: string;
      stream: string;
      templateId: string;
      reason: "cap_exceeded";
      cap: number;
    };

type RateState = { tokens: number; lastRefillMs: number };

function nowIso(ms: number): string {
  return new Date(ms).toISOString();
}

function parseTemplateRow(row: any): LiveTemplateRow {
  return {
    stream: String(row.stream),
    template_id: String(row.template_id),
    entity: String(row.entity),
    fields_json: String(row.fields_json),
    encodings_json: String(row.encodings_json),
    state: String(row.state),
    created_at_ms: typeof row.created_at_ms === "bigint" ? row.created_at_ms : BigInt(row.created_at_ms),
    last_seen_at_ms: typeof row.last_seen_at_ms === "bigint" ? row.last_seen_at_ms : BigInt(row.last_seen_at_ms),
    inactivity_ttl_ms: typeof row.inactivity_ttl_ms === "bigint" ? row.inactivity_ttl_ms : BigInt(row.inactivity_ttl_ms),
    active_from_source_offset:
      typeof row.active_from_source_offset === "bigint" ? row.active_from_source_offset : BigInt(row.active_from_source_offset),
    retired_at_ms: row.retired_at_ms == null ? null : typeof row.retired_at_ms === "bigint" ? row.retired_at_ms : BigInt(row.retired_at_ms),
    retired_reason: row.retired_reason == null ? null : String(row.retired_reason),
  };
}

export class LiveTemplateRegistry {
  private readonly db: SqliteDurableStore;

  // In-memory last-seen tracking to avoid sqlite writes on every wait call.
  private readonly lastSeenMem = new Map<string, { lastSeenMs: number; lastPersistMs: number }>();
  private readonly dirtyLastSeen = new Set<string>();

  private readonly rate = new Map<string, RateState>();

  constructor(db: SqliteDurableStore) {
    this.db = db;
  }

  private key(stream: string, templateId: string): string {
    return `${stream}\n${templateId}`;
  }

  private allowActivation(stream: string, nowMs: number, limitPerMinute: number): boolean {
    if (limitPerMinute <= 0) return true;
    const ratePerMs = limitPerMinute / 60_000;
    const st = this.rate.get(stream) ?? { tokens: limitPerMinute, lastRefillMs: nowMs };
    const elapsed = Math.max(0, nowMs - st.lastRefillMs);
    st.tokens = Math.min(limitPerMinute, st.tokens + elapsed * ratePerMs);
    st.lastRefillMs = nowMs;
    if (st.tokens < 1) {
      this.rate.set(stream, st);
      return false;
    }
    st.tokens -= 1;
    this.rate.set(stream, st);
    return true;
  }

  getActiveTemplateCount(stream: string): number {
    try {
      const row = this.db.db
        .query(`SELECT COUNT(*) as cnt FROM live_templates WHERE stream=? AND state='active';`)
        .get(stream) as any;
      return Number(row?.cnt ?? 0);
    } catch {
      return 0;
    }
  }

  listActiveTemplates(stream: string): Array<{ templateId: string; entity: string; fields: string[]; encodings: TemplateEncoding[]; lastSeenAtMs: number }> {
    try {
      const rows = this.db.db
        .query(
          `SELECT template_id, entity, fields_json, encodings_json, last_seen_at_ms
           FROM live_templates
           WHERE stream=? AND state='active'
           ORDER BY entity ASC, template_id ASC;`
        )
        .all(stream) as any[];
      const out: Array<{ templateId: string; entity: string; fields: string[]; encodings: TemplateEncoding[]; lastSeenAtMs: number }> = [];
      for (const row of rows) {
        const templateId = String(row.template_id);
        const entity = String(row.entity);
        const fields = JSON.parse(String(row.fields_json));
        const encodings = JSON.parse(String(row.encodings_json));
        if (!Array.isArray(fields) || !Array.isArray(encodings) || fields.length !== encodings.length) continue;
        const lastSeenAtMs = Number(row.last_seen_at_ms);
        out.push({ templateId, entity, fields: fields.map(String), encodings: encodings.map(String) as any, lastSeenAtMs });
      }
      return out;
    } catch {
      return [];
    }
  }

  /**
   * Activate templates (idempotent). Returns lifecycle events to be emitted.
   *
   * `baseStreamNextOffset` is used to set `active_from_source_offset` so we do
   * not backfill fine touches for history when a template is activated while
   * touch processing is behind.
   */
  activate(args: {
    stream: string;
    activeFromTouchOffset: string;
    baseStreamNextOffset: bigint;
    templates: TemplateDecl[];
    inactivityTtlMs: number;
    limits: {
      maxActiveTemplatesPerStream: number;
      maxActiveTemplatesPerEntity: number;
      activationRateLimitPerMinute: number;
    };
    nowMs: number;
  }): { activated: ActivatedTemplate[]; denied: DeniedTemplate[]; lifecycle: TemplateLifecycleEvent[] } {
    const { stream, templates, inactivityTtlMs, nowMs } = args;
    const { maxActiveTemplatesPerStream, maxActiveTemplatesPerEntity, activationRateLimitPerMinute } = args.limits;

    const activated: ActivatedTemplate[] = [];
    const denied: DeniedTemplate[] = [];
    const lifecycle: TemplateLifecycleEvent[] = [];

    const protectedIds = new Set<string>();

    for (const t of templates) {
      const entity = typeof t?.entity === "string" ? t.entity.trim() : "";
      if (entity === "") {
        denied.push({ templateId: "0000000000000000", reason: "invalid" });
        continue;
      }
      if (!Array.isArray(t.fields) || t.fields.length === 0 || t.fields.length > 3) {
        denied.push({ templateId: "0000000000000000", reason: "invalid" });
        continue;
      }

      const rawFields: Array<{ name: string; encoding: TemplateEncoding }> = [];
      for (const f of t.fields) {
        const name = typeof (f as any)?.name === "string" ? String((f as any).name).trim() : "";
        const encoding = (f as any)?.encoding as TemplateEncoding;
        if (name === "") continue;
        if (encoding !== "string" && encoding !== "int64" && encoding !== "bool" && encoding !== "datetime" && encoding !== "bytes") continue;
        rawFields.push({ name, encoding });
      }
      if (rawFields.length !== t.fields.length) {
        denied.push({ templateId: "0000000000000000", reason: "invalid" });
        continue;
      }
      {
        const seen = new Set<string>();
        let ok = true;
        for (const f of rawFields) {
          if (seen.has(f.name)) ok = false;
          seen.add(f.name);
        }
        if (!ok) {
          denied.push({ templateId: "0000000000000000", reason: "invalid" });
          continue;
        }
      }

      const fields = canonicalizeTemplateFields(rawFields);
      const fieldNames = fields.map((f) => f.name);
      const encodings = fields.map((f) => f.encoding);

      const templateId = templateIdFor(entity, fieldNames);

      const existing = this.db.db
        .query(
          `SELECT stream, template_id, entity, fields_json, encodings_json, state, created_at_ms, last_seen_at_ms,
                  inactivity_ttl_ms, active_from_source_offset, retired_at_ms, retired_reason
           FROM live_templates
           WHERE stream=? AND template_id=? LIMIT 1;`
        )
        .get(stream, templateId) as any;

      const alreadyActive = existing && String(existing.state) === "active";
      const needsToken = !alreadyActive;

      if (needsToken && !this.allowActivation(stream, nowMs, activationRateLimitPerMinute)) {
        denied.push({ templateId, reason: "rate_limited" });
        continue;
      }

      if (existing) {
        const row = parseTemplateRow(existing);
        if (row.entity !== entity) {
          denied.push({ templateId, reason: "invalid" });
          continue;
        }
        let storedFields: any;
        let storedEnc: any;
        try {
          storedFields = JSON.parse(row.fields_json);
          storedEnc = JSON.parse(row.encodings_json);
        } catch {
          denied.push({ templateId, reason: "invalid" });
          continue;
        }
        if (!Array.isArray(storedFields) || !Array.isArray(storedEnc) || storedFields.length !== storedEnc.length) {
          denied.push({ templateId, reason: "invalid" });
          continue;
        }
        const sf = storedFields.map(String);
        const se = storedEnc.map(String);
        if (sf.join("\0") !== fieldNames.join("\0")) {
          denied.push({ templateId, reason: "invalid" });
          continue;
        }
        if (se.join("\0") !== encodings.join("\0")) {
          denied.push({ templateId, reason: "invalid" });
          continue;
        }

        if (row.state === "active") {
          this.db.db
            .query(
              `UPDATE live_templates
               SET last_seen_at_ms=?, inactivity_ttl_ms=?
               WHERE stream=? AND template_id=?;`
            )
            .run(nowMs, inactivityTtlMs, stream, templateId);
        } else {
          this.db.db
            .query(
              `UPDATE live_templates
               SET state='active',
                   last_seen_at_ms=?,
                   inactivity_ttl_ms=?,
                   active_from_source_offset=?,
                   retired_at_ms=NULL,
                   retired_reason=NULL
               WHERE stream=? AND template_id=?;`
            )
            .run(nowMs, inactivityTtlMs, args.baseStreamNextOffset, stream, templateId);
        }
      } else {
        this.db.db
          .query(
            `INSERT INTO live_templates(
               stream, template_id, entity, fields_json, encodings_json,
               state, created_at_ms, last_seen_at_ms, inactivity_ttl_ms, active_from_source_offset,
               retired_at_ms, retired_reason
             ) VALUES(?, ?, ?, ?, ?, 'active', ?, ?, ?, ?, NULL, NULL);`
          )
          .run(stream, templateId, entity, JSON.stringify(fieldNames), JSON.stringify(encodings), nowMs, nowMs, inactivityTtlMs, args.baseStreamNextOffset);
      }

      protectedIds.add(templateId);
      activated.push({ templateId, state: "active", activeFromTouchOffset: args.activeFromTouchOffset });
      lifecycle.push({
        type: "live.template_activated",
        ts: nowIso(nowMs),
        stream,
        templateId,
        entity,
        fields: fieldNames,
        encodings,
        reason: "declared",
        activeFromTouchOffset: args.activeFromTouchOffset,
        inactivityTtlMs,
      });
      this.markSeen(stream, templateId, nowMs);
    }

    // Enforce caps with LRU eviction.
    const evicted = this.evictToCaps(stream, nowMs, { maxActiveTemplatesPerStream, maxActiveTemplatesPerEntity }, protectedIds);
    for (const e of evicted) lifecycle.push(e);

    return { activated, denied, lifecycle };
  }

  heartbeat(stream: string, templateIdsUsed: string[], nowMs: number): void {
    for (const id of templateIdsUsed) {
      const templateId = typeof id === "string" ? id.trim() : "";
      if (!/^[0-9a-f]{16}$/.test(templateId)) continue;
      this.markSeen(stream, templateId, nowMs);
    }
  }

  flushLastSeen(nowMs: number, persistIntervalMs: number): void {
    if (this.dirtyLastSeen.size === 0) return;

    const stmt = this.db.db.query(
      `UPDATE live_templates
       SET last_seen_at_ms=?
       WHERE stream=? AND template_id=? AND state='active';`
    );
    try {
      for (const k of this.dirtyLastSeen) {
        const item = this.lastSeenMem.get(k);
        if (!item) {
          this.dirtyLastSeen.delete(k);
          continue;
        }
        if (nowMs - item.lastPersistMs < persistIntervalMs) continue;
        const [stream, templateId] = k.split("\n");
        stmt.run(item.lastSeenMs, stream, templateId);
        item.lastPersistMs = nowMs;
        this.dirtyLastSeen.delete(k);
      }
    } finally {
      try {
        stmt.finalize?.();
      } catch {
        // ignore
      }
    }
  }

  gcRetireExpired(stream: string, nowMs: number): { retired: TemplateLifecycleEvent[] } {
    const expired: any[] = [];
    try {
      const rows = this.db.db
        .query(
          `SELECT template_id, entity, fields_json, encodings_json, last_seen_at_ms, inactivity_ttl_ms
           FROM live_templates
           WHERE stream=? AND state='active' AND (last_seen_at_ms + inactivity_ttl_ms) < ?
           ORDER BY last_seen_at_ms ASC
           LIMIT 1000;`
        )
        .all(stream, nowMs) as any[];
      expired.push(...rows);
    } catch {
      return { retired: [] };
    }
    if (expired.length === 0) return { retired: [] };

    // If a client is heartbeating frequently but last-seen persistence is
    // configured with a longer interval than the inactivity TTL, DB state can
    // look expired even though in-memory last-seen is fresh. Prefer in-memory
    // last-seen and opportunistically refresh DB to avoid incorrect retirement.
    const effectiveExpired: any[] = [];
    const refreshLastSeen = this.db.db.query(
      `UPDATE live_templates
       SET last_seen_at_ms=?
       WHERE stream=? AND template_id=? AND state='active';`
    );
    try {
      for (const row of expired) {
        const templateId = String(row.template_id);
        const dbLastSeenAtMs = Number(row.last_seen_at_ms);
        const ttlMs = Number(row.inactivity_ttl_ms);
        const mem = this.lastSeenMem.get(this.key(stream, templateId));
        const memLastSeen = mem ? mem.lastSeenMs : 0;
        const lastSeenAtMs = Math.max(dbLastSeenAtMs, memLastSeen);
        if (lastSeenAtMs + ttlMs >= nowMs) {
          // Not expired when considering in-memory last-seen. Refresh DB so it
          // doesn't get re-selected on the next GC tick.
          if (mem && memLastSeen > dbLastSeenAtMs) {
            refreshLastSeen.run(memLastSeen, stream, templateId);
            mem.lastPersistMs = nowMs;
            this.dirtyLastSeen.delete(this.key(stream, templateId));
          }
          continue;
        }
        effectiveExpired.push(row);
      }
    } finally {
      try {
        refreshLastSeen.finalize?.();
      } catch {
        // ignore
      }
    }

    if (effectiveExpired.length === 0) return { retired: [] };

    const retired: TemplateLifecycleEvent[] = [];
    const update = this.db.db.query(
      `UPDATE live_templates
       SET state='retired', retired_reason='inactivity', retired_at_ms=?
       WHERE stream=? AND template_id=? AND state='active';`
    );
    try {
      for (const row of effectiveExpired) {
        const templateId = String(row.template_id);
        const entity = String(row.entity);
        let fields: string[] = [];
        let encodings: TemplateEncoding[] = [];
        try {
          const f = JSON.parse(String(row.fields_json));
          const e = JSON.parse(String(row.encodings_json));
          if (Array.isArray(f)) fields = f.map(String);
          if (Array.isArray(e)) encodings = e.map(String) as any;
        } catch {
          // ignore
        }
        const dbLastSeenAtMs = Number(row.last_seen_at_ms);
        const mem = this.lastSeenMem.get(this.key(stream, templateId));
        const memLastSeen = mem ? mem.lastSeenMs : 0;
        const lastSeenAtMs = Math.max(dbLastSeenAtMs, memLastSeen);
        const inactiveForMs = Math.max(0, nowMs - lastSeenAtMs);
        update.run(nowMs, stream, templateId);
        retired.push({
          type: "live.template_retired",
          ts: nowIso(nowMs),
          stream,
          templateId,
          entity,
          fields,
          encodings,
          lastSeenAt: nowIso(lastSeenAtMs),
          inactiveForMs,
          reason: "inactivity",
        });
        this.lastSeenMem.delete(this.key(stream, templateId));
        this.dirtyLastSeen.delete(this.key(stream, templateId));
      }
    } finally {
      try {
        update.finalize?.();
      } catch {
        // ignore
      }
    }

    return { retired };
  }

  private markSeen(stream: string, templateId: string, nowMs: number): void {
    const k = this.key(stream, templateId);
    const item = this.lastSeenMem.get(k) ?? { lastSeenMs: 0, lastPersistMs: 0 };
    if (nowMs > item.lastSeenMs) item.lastSeenMs = nowMs;
    this.lastSeenMem.set(k, item);
    this.dirtyLastSeen.add(k);
  }

  private evictToCaps(
    stream: string,
    nowMs: number,
    caps: { maxActiveTemplatesPerStream: number; maxActiveTemplatesPerEntity: number },
    protectedIds: Set<string>
  ): TemplateLifecycleEvent[] {
    const out: TemplateLifecycleEvent[] = [];
    const { maxActiveTemplatesPerStream, maxActiveTemplatesPerEntity } = caps;

    // Per-entity cap.
    let entities: Array<{ entity: string; cnt: number }> = [];
    try {
      const rows = this.db.db
        .query(
          `SELECT entity, COUNT(*) as cnt
           FROM live_templates
           WHERE stream=? AND state='active'
           GROUP BY entity;`
        )
        .all(stream) as any[];
      entities = rows.map((r) => ({ entity: String(r.entity), cnt: Number(r.cnt) }));
    } catch {
      // ignore
    }

    for (const e of entities) {
      if (e.cnt <= maxActiveTemplatesPerEntity) continue;
      const extra = e.cnt - maxActiveTemplatesPerEntity;
      const evicted = this.evictLru(stream, nowMs, extra, { entity: e.entity, cap: maxActiveTemplatesPerEntity }, protectedIds);
      out.push(...evicted);
    }

    // Per-stream cap.
    let activeCount = 0;
    try {
      const row = this.db.db.query(`SELECT COUNT(*) as cnt FROM live_templates WHERE stream=? AND state='active';`).get(stream) as any;
      activeCount = Number(row?.cnt ?? 0);
    } catch {
      activeCount = 0;
    }
    if (activeCount > maxActiveTemplatesPerStream) {
      const extra = activeCount - maxActiveTemplatesPerStream;
      const evicted = this.evictLru(stream, nowMs, extra, { cap: maxActiveTemplatesPerStream }, protectedIds);
      out.push(...evicted);
    }

    return out;
  }

  private evictLru(
    stream: string,
    nowMs: number,
    count: number,
    scope: { entity?: string; cap: number },
    protectedIds: Set<string>
  ): TemplateLifecycleEvent[] {
    if (count <= 0) return [];
    const out: TemplateLifecycleEvent[] = [];

    const pick = (excludeProtected: boolean): string[] => {
      const params: any[] = [stream];
      let where = `stream=? AND state='active'`;
      if (scope.entity) {
        where += ` AND entity=?`;
        params.push(scope.entity);
      }
      if (excludeProtected && protectedIds.size > 0) {
        const placeholders = Array.from(protectedIds).map(() => "?").join(", ");
        where += ` AND template_id NOT IN (${placeholders})`;
        params.push(...Array.from(protectedIds));
      }
      const q = `SELECT template_id FROM live_templates WHERE ${where} ORDER BY last_seen_at_ms ASC, template_id ASC LIMIT ?;`;
      params.push(count);
      try {
        const rows = this.db.db.query(q).all(...params) as any[];
        return rows.map((r) => String(r.template_id));
      } catch {
        return [];
      }
    };

    let ids = pick(true);
    if (ids.length < count) {
      // Evict protected templates only if we have to.
      const extra = pick(false);
      const merged: string[] = [];
      const seen = new Set<string>();
      for (const id of [...ids, ...extra]) {
        if (seen.has(id)) continue;
        seen.add(id);
        merged.push(id);
        if (merged.length >= count) break;
      }
      ids = merged;
    }
    if (ids.length === 0) return [];

    const update = this.db.db.query(
      `UPDATE live_templates
       SET state='retired', retired_reason='cap_exceeded', retired_at_ms=?
       WHERE stream=? AND template_id=? AND state='active';`
    );
    try {
      for (const id of ids) {
        update.run(nowMs, stream, id);
        out.push({
          type: "live.template_evicted",
          ts: nowIso(nowMs),
          stream,
          templateId: id,
          reason: "cap_exceeded",
          cap: scope.cap,
        });
        this.lastSeenMem.delete(this.key(stream, id));
        this.dirtyLastSeen.delete(this.key(stream, id));
      }
    } finally {
      try {
        update.finalize?.();
      } catch {
        // ignore
      }
    }

    return out;
  }
}
