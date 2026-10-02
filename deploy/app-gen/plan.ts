// A phase plan for a generator that takes its configuration as arguments
// (bench/k2cost/k2gen.ts), for the K2 cost field runs
// (bench/k2cost/field/gen.sh).
//
// index.ts hands a supervised binary no arguments, so a plan arrives as
// GEN_PLAN_JSON: a list of stages run in order, each a list of argument
// vectors whose invocations run concurrently; a stage ends when all of its
// invocations have. In a vector, "@HEADERS" stands for the file
// TOKENS_S3_KEY was downloaded to (header lines, re-read by k2gen), and a
// vector ["idle", "SECS"] only waits. Every invocation gets its own
// --out/--ledger under /tmp/k2plan. A stage with an invocation that exits
// other than 0 (or 3, a failed --expect) stops the plan.
//
// $PORT serves the plan's state and output, which holds counts, latencies
// and arguments, never a credential:
//   GET /                   stages, exit codes, timings, the instance shape
//   GET /windows?since=N    window lines N.. of every invocation as JSONL,
//                           each tagged with its invocation index "inv"
//   GET /ledgers            every finished invocation's ledger
// Like every generator here, a finished plan holds (policyFor("app-gen")):
// a restarted generator would run its load again. It does not hold the
// instance awake, though: the keep-awake guard is released RELEASE_GRACE_MS
// after the plan ends (time for observe.py to fetch the ledgers), so a
// finished generator sleeps once nothing polls it. And a plan never starts
// after GEN_START_BY_MS (gen.py: the latest start that still ends inside the
// guard's KEEP_AWAKE_UNTIL_MS), so a wake that cold-boots a finished
// generator cannot run its load a second time.
import { existsSync, mkdirSync, readFileSync } from "node:fs";

type Inv = { inv: number; stage: number; argv: string[]; rc?: number | null; error?: string; startMs?: number; endMs?: number };
const DIR = "/tmp/k2plan";
const OK_CODES = new Set([0, 3]);
const RELEASE_GRACE_MS = 120_000;

export async function runPlan(
  bin: string, planJson: string, env: Record<string, string | undefined>, shape: unknown,
  guard: { release(): void } | null = null,
): Promise<never> {
  const stages: string[][][] = JSON.parse(planJson);
  if (!Array.isArray(stages) || !stages.every((s) => Array.isArray(s) && s.every((v) => Array.isArray(v) && v.length > 0))) {
    throw new Error("GEN_PLAN_JSON must be a list of stages, each a list of argument vectors");
  }
  mkdirSync(DIR, { recursive: true });
  const invs: Inv[] = [];
  stages.forEach((s, stage) => s.forEach((argv) => invs.push({ inv: invs.length, stage, argv })));
  const startBy = Number(env.GEN_START_BY_MS ?? 0);
  const keepAwake = {
    held: guard !== null, untilMs: Number(env.KEEP_AWAKE_UNTIL_MS ?? 0) || null,
    releaseAtMs: null as number | null, releasedMs: null as number | null,
  };
  const release = (why: string) => {
    if (keepAwake.releasedMs !== null) return;
    guard?.release();
    keepAwake.releasedMs = Date.now();
    console.log(`plan: keep-awake guard released (${why})`);
  };
  const state = {
    startedMs: Date.now(), endedMs: null as number | null, aborted: false, refused: null as string | null,
    startByMs: startBy || null, keepAwake, invs, shape,
  };
  const lines: string[] = [];
  const offsets = new Map<number, number>();
  const collect = () => {
    for (const i of invs) {
      const f = `${DIR}/inv-${i.inv}.jsonl`;
      if (!existsSync(f)) continue;
      const text = readFileSync(f, "utf8");
      const from = offsets.get(i.inv) ?? 0;
      const end = text.lastIndexOf("\n") + 1;
      if (end <= from) continue;
      for (const l of text.slice(from, end).split("\n")) {
        if (l) lines.push(`{"inv":${i.inv},${l.slice(1)}`);
      }
      offsets.set(i.inv, end);
    }
  };
  setInterval(collect, 2000);
  Bun.serve({
    port: Number(env.PORT ?? 8080),
    hostname: "0.0.0.0",
    fetch(req) {
      const url = new URL(req.url);
      if (url.pathname === "/windows") {
        collect();
        const since = Math.max(0, Number(url.searchParams.get("since") ?? 0) || 0);
        const page = lines.slice(since, since + 5000);
        return new Response(page.map((l) => l + "\n").join(""), {
          headers: { "content-type": "application/x-ndjson", "x-next": String(since + page.length), "cache-control": "no-store" },
        });
      }
      if (url.pathname === "/ledgers") {
        const out = invs.filter((i) => existsSync(`${DIR}/inv-${i.inv}.ledger.json`))
          .map((i) => ({ inv: i.inv, ledger: JSON.parse(readFileSync(`${DIR}/inv-${i.inv}.ledger.json`, "utf8")) }));
        return Response.json(out, { headers: { "cache-control": "no-store" } });
      }
      return Response.json({ ...state, windows: lines.length }, { headers: { "cache-control": "no-store" } });
    },
  });

  const running = new Set<ReturnType<typeof Bun.spawn>>();
  for (const sig of ["SIGTERM", "SIGINT"] as const) {
    process.on(sig, async () => {
      console.error(`plan: ${sig}; stopping ${running.size} invocation(s)`);
      for (const p of running) p.kill(sig);
      await Promise.race([Promise.all([...running].map((p) => p.exited)), Bun.sleep(20_000)]);
      process.exit(0);
    });
  }
  const headers = env.BENCH_TOKENS_FILE ?? "";
  const runOne = async (i: Inv) => {
    i.startMs = Date.now();
    if (i.argv[0] === "idle") {
      await Bun.sleep(Number(i.argv[1] ?? 0) * 1000);
      i.rc = 0;
    } else {
      const argv = i.argv.map((a) => (a === "@HEADERS" ? headers : a));
      argv.push("--out", `${DIR}/inv-${i.inv}.jsonl`, "--ledger", `${DIR}/inv-${i.inv}.ledger.json`);
      console.log(`plan: stage ${i.stage} inv ${i.inv}: ${argv.join(" ")}`);
      try {
        const p = Bun.spawn([bin, ...argv], { env, stdout: "inherit", stderr: "inherit" });
        running.add(p);
        i.rc = await p.exited;
        running.delete(p);
      } catch (e) {
        i.rc = null;
        i.error = String(e);
      }
    }
    i.endMs = Date.now();
    console.log(`plan: inv ${i.inv} ended rc=${i.rc}${i.error ? ` error=${i.error}` : ""}`);
  };
  if (startBy && Date.now() > startBy) {
    state.refused = `start deadline ${new Date(startBy).toISOString()} passed: not running the load again`;
    state.aborted = true;
    console.log(`plan: ${state.refused}`);
  }
  for (let s = 0; s < stages.length && !state.aborted; s++) {
    const mine = invs.filter((i) => i.stage === s);
    await Promise.all(mine.map(runOne));
    if (mine.some((i) => i.rc === null || i.rc === undefined || !OK_CODES.has(i.rc))) state.aborted = true;
  }
  collect();
  state.endedMs = Date.now();
  console.log(`plan: ${state.refused ? "refused" : state.aborted ? "stopped at a failed stage" : "complete"}; holding :${env.PORT ?? 8080}`);
  if (state.refused) release("plan refused");
  keepAwake.releaseAtMs = Date.now() + (state.refused ? 0 : RELEASE_GRACE_MS);
  setTimeout(() => release("plan ended"), Math.max(0, keepAwake.releaseAtMs - Date.now()));
  return new Promise<never>(() => {});
}
