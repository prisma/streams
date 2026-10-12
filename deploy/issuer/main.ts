// streams-issuer: the interim token issuer of the public beta's shared
// cell (owner decision 9 of 2026-10-11; public-beta issuer triage B2), run
// on its environment (config.ts's INPUTS; run.ts says what it does):
//
//   bun deploy/issuer/main.ts
//
// Bundled into one file, it holds @prisma/compute's keep-awake guard at
// the version this directory's package.json pins exactly and bun.lock
// resolves when the build inlines it; run with `bun --no-install`, a build
// that left the guard out refuses to boot (exit 1) instead of Bun's
// auto-install fetching whatever npm serves as the newest @prisma/compute.
// On Compute the issuer starts from its Composer entry instead
// (deploy/composer-issuer/issuer/entry.ts, B6), whose bundle carries the
// guard.
import { runIssuer } from "./run";

await runIssuer(process.env, async () => {
  const { KeepAwakeGuard } = await import("@prisma/compute");
  return new KeepAwakeGuard();
});
