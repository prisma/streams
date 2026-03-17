import { existsSync, readdirSync, unlinkSync } from "node:fs";
import { join } from "node:path";

export function cleanupTempSegments(rootDir: string): void {
  const base = join(rootDir, "local");
  if (!existsSync(base)) return;
  const walk = (dir: string) => {
    for (const entry of readdirSync(dir, { withFileTypes: true })) {
      const full = join(dir, entry.name);
      if (entry.isDirectory()) {
        walk(full);
      } else if (entry.isFile() && entry.name.endsWith(".tmp")) {
        try {
          unlinkSync(full);
        } catch {
          // ignore
        }
      }
    }
  };
  walk(base);
}
