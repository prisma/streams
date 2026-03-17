import type { Config } from "./config";
import type { SqliteDurableStore } from "./db/db";

export class ExpirySweeper {
  private readonly cfg: Config;
  private readonly db: SqliteDurableStore;
  private timer: any | null = null;
  private running = false;

  constructor(cfg: Config, db: SqliteDurableStore) {
    this.cfg = cfg;
    this.db = db;
  }

  start(): void {
    if (this.timer || this.cfg.expirySweepIntervalMs <= 0) return;
    this.timer = setInterval(() => {
      void this.tick();
    }, this.cfg.expirySweepIntervalMs);
  }

  stop(): void {
    if (this.timer) clearInterval(this.timer);
    this.timer = null;
  }

  private async tick(): Promise<void> {
    if (this.running) return;
    this.running = true;
    try {
      const expired = this.db.listExpiredStreams(this.cfg.expirySweepBatchLimit);
      if (expired.length === 0) return;
      for (const stream of expired) {
        try {
          this.db.deleteStream(stream);
        } catch {
          // ignore deletion errors
        }
      }
    } finally {
      this.running = false;
    }
  }
}
