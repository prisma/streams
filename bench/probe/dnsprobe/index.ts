// Differential DNS probe for the Tigris cross-region routing question
// (docs/SOAK-REGIONS.md, the eu-central-1 → ord1 window).
//
// t3.storage.dev is geo-DNS on NS1 (measured 2026-07-27): the answer
// depends on where the *resolver* appears to be, ECS is honored when
// sent, and the TTL is 60 s. Compute microVMs get a per-node DC-local
// forwarder in resolv.conf (172.16.x.x — observed .11.178 and .2.58 on
// consecutive boots), with Cloudflare 1.1.1.1 believed upstream of it;
// 1.1.1.1 never sends ECS, so at that hop NS1 steers on Cloudflare's
// egress location, which is usually right and occasionally is not.
//
// This probe answers three questions from inside a Compute microVM:
//
//   1. Can /etc/resolv.conf be overridden at all? (boot-time write test,
//      env-gated via RESOLV_OVERRIDE)
//   2. Do different resolvers steer t3.storage.dev differently from this
//      instance, and does the system path follow the override?
//   3. When the next mis-steering window happens, which resolver was
//      wrong? Every tick resolves via the system path, explicit
//      1.1.1.1, Vultr's recursor, Google, and NS1's authoritative
//      DIRECTLY — the authority sees the instance's own IP, making it
//      the ground truth for this location. A tick where a resolver's
//      answer set is disjoint from the authority's is a mis-steer.
//
// Serves its state as JSON on $PORT. No secrets, no bucket access.

import { promises as dnsp, Resolver as CbResolver } from "node:dns";
import { readFileSync, writeFileSync } from "node:fs";
import { KeepAwakeGuard } from "@prisma/compute";

if (process.env.KEEP_AWAKE === "1") new KeepAwakeGuard();

const TARGET = process.env.PROBE_TARGET ?? "t3.storage.dev";
const TICK_MS = Number(process.env.PROBE_TICK_MS ?? 30_000);
const RING = 240; // ~2h of ticks at 30s

// NS1 authoritatives for storage.dev (dig NS storage.dev). Rendezvous on
// the first that answers; re-resolved hourly in case the set changes.
const AUTHORITY_NAMES = [
  "dns1.p05.nsone.net",
  "dns2.p05.nsone.net",
  "dns3.p05.nsone.net",
  "dns4.p05.nsone.net",
];

type TickResult = {
  ok: boolean;
  ms: number;
  answers: string[];
  error?: string;
};

type Tick = {
  ts: string;
  results: Record<string, TickResult>;
  /// Resolvers whose answer set is disjoint from the authority's.
  missteered: string[];
};

const state = {
  boot: {
    startedAt: new Date().toISOString(),
    resolvConfOriginal: "" as string,
    resolvConfAfter: "" as string,
    overrideRequested: process.env.RESOLV_OVERRIDE ?? null,
    overrideWriteError: null as string | null,
    authorityIp: null as string | null,
    authorityError: null as string | null,
  },
  ticks: 0,
  disagreementTicks: 0,
  perResolver: {} as Record<string, { ok: number; err: number }>,
  ring: [] as Tick[],
  /// Only ticks with a mis-steer, kept separately so a rare event is
  /// never aged out of the main ring before someone looks.
  missteers: [] as Tick[],
};

function readResolvConf(): string {
  try {
    return readFileSync("/etc/resolv.conf", "utf8");
  } catch (e) {
    return `<unreadable: ${e}>`;
  }
}

// Boot: record the platform default, then attempt the override if asked.
state.boot.resolvConfOriginal = readResolvConf();
if (process.env.RESOLV_OVERRIDE) {
  // "\n" arrives literally through the deploy CLI's --env; unescape it.
  const conf = process.env.RESOLV_OVERRIDE.replace(/\\n/g, "\n");
  try {
    writeFileSync("/etc/resolv.conf", conf + "\n");
  } catch (e) {
    state.boot.overrideWriteError = String(e);
  }
  state.boot.resolvConfAfter = readResolvConf();
} else {
  state.boot.resolvConfAfter = state.boot.resolvConfOriginal;
}

/// resolve4 against one explicit server, with our own timeout — Bun's
/// node:dns options support is partial, so measure and bound ourselves.
function resolveVia(server: string, name: string, timeoutMs = 4000): Promise<TickResult> {
  const t0 = performance.now();
  return new Promise((done) => {
    let settled = false;
    const finish = (r: Omit<TickResult, "ms">) => {
      if (settled) return;
      settled = true;
      done({ ...r, ms: Math.round(performance.now() - t0) });
    };
    const timer = setTimeout(() => finish({ ok: false, answers: [], error: "timeout" }), timeoutMs);
    try {
      const r = new CbResolver();
      r.setServers([server]);
      r.resolve4(name, (err, addrs) => {
        clearTimeout(timer);
        if (err) finish({ ok: false, answers: [], error: String(err.code ?? err) });
        else finish({ ok: true, answers: (addrs ?? []).slice().sort() });
      });
    } catch (e) {
      clearTimeout(timer);
      finish({ ok: false, answers: [], error: String(e) });
    }
  });
}

/// System-path lookup (getaddrinfo / resolv.conf), same shape.
async function systemLookup(name: string): Promise<TickResult> {
  const t0 = performance.now();
  try {
    const res = await dnsp.lookup(name, { all: true, family: 4 });
    return {
      ok: true,
      ms: Math.round(performance.now() - t0),
      answers: res.map((r) => r.address).sort(),
    };
  } catch (e) {
    return { ok: false, ms: Math.round(performance.now() - t0), answers: [], error: String(e) };
  }
}

async function refreshAuthority() {
  for (const n of AUTHORITY_NAMES) {
    const via = await resolveVia("8.8.8.8", n);
    if (via.ok && via.answers.length) {
      state.boot.authorityIp = via.answers[0];
      state.boot.authorityError = null;
      return;
    }
    state.boot.authorityError = via.error ?? "no answer";
  }
}

function disjoint(a: string[], b: string[]): boolean {
  return a.length > 0 && b.length > 0 && !a.some((x) => b.includes(x));
}

async function tick() {
  const resolvers: Record<string, Promise<TickResult>> = {
    system: systemLookup(TARGET),
    cloudflare: resolveVia("1.1.1.1", TARGET),
    vultr: resolveVia("108.61.10.10", TARGET),
    google: resolveVia("8.8.8.8", TARGET),
  };
  // The platform's own DC-local forwarder (found in the microVM's
  // original resolv.conf: 172.16.11.178). Queried explicitly so the
  // default path stays observable even if the system path is overridden
  // — and so a forwarder-only fault is distinguishable from a
  // Cloudflare-upstream fault.
  const fwd = state.boot.resolvConfOriginal.match(/nameserver\s+(\S+)/)?.[1];
  if (fwd && !["108.61.10.10", "8.8.8.8", "1.1.1.1"].includes(fwd)) {
    resolvers.platform = resolveVia(fwd, TARGET);
  }
  if (state.boot.authorityIp) {
    resolvers.authority = resolveVia(state.boot.authorityIp, TARGET);
  }
  const entries = await Promise.all(
    Object.entries(resolvers).map(async ([k, p]) => [k, await p] as const),
  );
  const results = Object.fromEntries(entries);

  const truth = results.authority?.ok ? results.authority.answers : [];
  const missteered = entries
    .filter(([k, r]) => k !== "authority" && r.ok && disjoint(r.answers, truth))
    .map(([k]) => k);

  const t: Tick = { ts: new Date().toISOString(), results, missteered };
  state.ticks += 1;
  for (const [k, r] of entries) {
    const c = (state.perResolver[k] ??= { ok: 0, err: 0 });
    r.ok ? c.ok++ : c.err++;
  }
  if (missteered.length) {
    state.disagreementTicks += 1;
    state.missteers.push(t);
    if (state.missteers.length > RING) state.missteers.shift();
    console.error(`MISSTEER ${t.ts}: ${missteered.join(",")} vs authority=${truth.join(",")}`);
  }
  state.ring.push(t);
  if (state.ring.length > RING) state.ring.shift();
}

await refreshAuthority();
setInterval(refreshAuthority, 3600_000);
await tick();
setInterval(() => void tick().catch(() => {}), TICK_MS);

const port = Number(process.env.PORT ?? 8080);
Bun.serve({
  port,
  fetch() {
    return new Response(JSON.stringify(state, null, 2), {
      headers: { "content-type": "application/json" },
    });
  },
});
console.error(`dnsprobe serving on :${port}; target=${TARGET} tick=${TICK_MS}ms`);
