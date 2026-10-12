// The interim issuer's process (public-beta issuer triage B2), started by
// one of two entries: main.ts, which hands it its environment (`bun
// deploy/issuer/main.ts`, and the bundle the tests run), and the issuer's
// Composer entry (deploy/composer-issuer/issuer/entry.ts, B6), which hands
// it the Compute service's input under the same names.
//
// It reads exactly config.ts's INPUTS from what it is handed and deletes
// the secrets among them from the process environment once parsed
// (hygiene, not a control). It refuses to start (exit 1, a message that
// quotes ids only) on any input or boot refusal (boot.ts). Then it serves
// server.ts's routes on PORT, revalidates the cell's state every 5 s,
// ticks the audit every 10 s, writes noted re-hashes 10 minutes after the
// first (rehash.ts) and drains deleted projects (offboarding.ts, looking
// every 15 s); SIGTERM or SIGINT stops the timers, writes the noted
// re-hashes (at most STOP_SWEEP_MS) and the open audit period, and exits 0
// (a drain cut short resumes at the next boot).
import { AdminError } from "../cell-admin/errors";
import { boot } from "./boot";
import { StateCache } from "./cache";
import { type Env, INPUTS, type IssuerConfig, readConfig, SECRET_INPUTS } from "./config";
import { refuse } from "./respond";
import { Issuer } from "./server";

/// A request body larger than this is cut off by the server before any
/// route reads it; the exchange takes at most 4 KiB.
const MAX_REQUEST_BODY_BYTES = 64 * 1024;
/// How long the stop waits for the re-hash sweep: the notes live only in
/// memory, but the stop must not outlast the platform's grace.
const STOP_SWEEP_MS = 5_000;

/// Takes Compute's keep-awake guard (@prisma/compute's KeepAwakeGuard),
/// or throws when the build has none. Each entry brings its own: main.ts
/// loads the package, the Composer entry has it bundled.
export type HoldKeepAwake = () => Promise<unknown>;

function fail(message: string): never {
  console.error(`streams-issuer: ${message}`);
  process.exit(1);
}

/// Compute's keep-awake guard, held for as long as the process runs
/// (owner question I10, recommended answer: awake; the same guard the
/// cell's wrapper holds, deploy/app-server/index.ts). Outside Compute it
/// is a no-op.
let guard: unknown;

async function keepAwake(cfg: IssuerConfig, hold: HoldKeepAwake): Promise<void> {
  if (!cfg.keepAwake) return;
  try {
    guard = await hold();
  } catch {
    fail("KEEP_AWAKE=1 needs @prisma/compute's KeepAwakeGuard in the build (set KEEP_AWAKE=0 to run without it)");
  }
}

/// Run the issuer on `env`'s INPUTS until SIGTERM or SIGINT.
export async function runIssuer(env: Env, hold: HoldKeepAwake): Promise<void> {
  let cfg: IssuerConfig;
  try {
    cfg = readConfig(Object.fromEntries(INPUTS.map((name) => [name, env[name]])));
  } catch (e) {
    fail(e instanceof AdminError ? e.message : "the inputs could not be read");
  }
  for (const name of SECRET_INPUTS) delete process.env[name];
  const booted = await boot(cfg).catch((e) => fail(e instanceof AdminError ? e.message : `boot failed: ${(e as Error).message}`));
  await keepAwake(cfg, hold);
  const cache = new StateCache(booted.store, booted);
  const issuer = new Issuer(cfg, { store: booted.store, cache });
  const stops = [cache.start(), issuer.audit.start(), issuer.rehasher.start(), issuer.offboarding.start()];
  const server = Bun.serve({
    port: cfg.port,
    hostname: "0.0.0.0",
    maxRequestBodySize: MAX_REQUEST_BODY_BYTES,
    fetch: async (req) => {
      try {
        return await issuer.fetch(req);
      } catch (e) {
        console.error(`streams-issuer: ${req.method} ${new URL(req.url).pathname} failed: ${(e as Error).message}`);
        return refuse(500, "internal", "the issuer failed to answer; retry");
      }
    },
  });
  console.log(
    `streams-issuer: cell ${cfg.cellId}, issuer ${cfg.issuerId}, kid ${cfg.signingKey.record.kid}, ` +
      `state at feed_version ${booted.state.feed_version}, listening on :${server.port}`,
  );
  let stopping = false;
  const stop = async () => {
    if (stopping) return;
    stopping = true;
    for (const s of stops) s();
    const swept = issuer.rehasher.sweep().then(
      (n) => n > 0 && console.log(`streams-issuer: re-hashed ${n} credential(s) at the stop`),
      (e) => console.error(`streams-issuer: re-hashing at the stop failed: ${(e as Error).message}`),
    );
    await Promise.race([swept, new Promise((r) => setTimeout(r, STOP_SWEEP_MS))]);
    await issuer.audit.flush();
    server.stop();
    process.exit(0);
  };
  process.on("SIGTERM", stop);
  process.on("SIGINT", stop);
}
