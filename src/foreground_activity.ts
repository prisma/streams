import { yieldToEventLoop } from "./util/yield";

export class ForegroundActivityTracker {
  private active = 0;
  private lastActivityAt = 0;
  private readonly idleResolvers = new Set<() => void>();

  enter(): () => void {
    this.lastActivityAt = Date.now();
    this.active += 1;
    let released = false;
    return () => {
      if (released) return;
      released = true;
      this.lastActivityAt = Date.now();
      this.active = Math.max(0, this.active - 1);
      if (this.active !== 0) return;
      const resolvers = Array.from(this.idleResolvers);
      this.idleResolvers.clear();
      for (const resolve of resolvers) resolve();
    };
  }

  isActive(): boolean {
    return this.active > 0;
  }

  getActive(): number {
    return this.active;
  }

  wasActiveWithin(windowMs: number): boolean {
    return Date.now() - this.lastActivityAt <= windowMs;
  }

  async yieldBackgroundWork(maxPauseMs = 5): Promise<void> {
    if (this.active === 0) {
      await yieldToEventLoop();
      return;
    }
    let idleResolve!: () => void;
    const idlePromise = new Promise<void>((resolve) => {
      idleResolve = resolve;
      this.idleResolvers.add(resolve);
    });
    try {
      await Promise.race([
        idlePromise,
        new Promise<void>((resolve) => setTimeout(resolve, maxPauseMs)),
      ]);
    } finally {
      this.idleResolvers.delete(idleResolve);
    }
  }
}
