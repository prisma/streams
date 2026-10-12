// streams-issuer: the interim token issuer of the public beta's shared
// cell (owner decision 9 of 2026-10-11; public-beta issuer triage B2). It
// runs on Compute as one self-contained ESM file that Composer builds from
// this entry (B6), cell-admin's library and the contract schemas inlined;
// the build also inlines @prisma/compute, whose keep-awake guard it holds.
//
//   bun deploy/issuer/main.ts     (its inputs in the environment, config.ts)
//
// It reads exactly config.ts's INPUTS from the environment and deletes the
// secrets among them once parsed (hygiene, not a control). It refuses to
// start (exit 1, a message that quotes ids only) on any input or boot
// refusal (boot.ts). Then it serves server.ts's routes on PORT,
// revalidates the cell's state every 5 s, ticks the audit every 10 s,
// sweeps re-hashes daily and drains deleted projects (offboarding.ts,
// looking every 15 s); SIGTERM or SIGINT stops the timers, writes the
// open audit period and exits 0 (a drain cut short resumes at the next
// boot).
import { AdminError } from "../cell-admin/errors";
import { boot } from "./boot";
import { StateCache } from "./cache";
import { INPUTS, type IssuerConfig, readConfig, SECRET_INPUTS } from "./config";
import { refuse } from "./respond";
import { Issuer } from "./server";

/// A request body larger than this is cut off by the server before any
/// route reads it; the exchange takes at most 4 KiB.
const MAX_REQUEST_BODY_BYTES = 64 * 1024;

function fail(message: string): never {
  console.error(`streams-issuer: ${message}`);
  process.exit(1);
}

/// Compute's keep-awake guard, held for as long as the process runs
/// (owner question I10, recommended answer: awake; the same guard the
/// cell's wrapper holds, deploy/app-server/index.ts). Outside Compute it
/// is a no-op.
let guard: unknown;

async function keepAwake(cfg: IssuerConfig): Promise<void> {
  if (!cfg.keepAwake) return;
  try {
    const { KeepAwakeGuard } = await import("@prisma/compute");
    guard = new KeepAwakeGuard();
  } catch {
    fail("KEEP_AWAKE=1 needs @prisma/compute's KeepAwakeGuard in the build (set KEEP_AWAKE=0 to run without it)");
  }
}

async function start(): Promise<void> {
  let cfg: IssuerConfig;
  try {
    cfg = readConfig(Object.fromEntries(INPUTS.map((name) => [name, process.env[name]])));
  } catch (e) {
    fail(e instanceof AdminError ? e.message : "the inputs could not be read");
  }
  for (const name of SECRET_INPUTS) delete process.env[name];
  const booted = await boot(cfg).catch((e) => fail(e instanceof AdminError ? e.message : `boot failed: ${(e as Error).message}`));
  await keepAwake(cfg);
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
    await issuer.audit.flush();
    server.stop();
    process.exit(0);
  };
  process.on("SIGTERM", stop);
  process.on("SIGINT", stop);
}

await start();
