// Minimal reproduction: Prisma Compute edge vs Server-Sent Events.
//
// ONE file, two roles:
//   bun edge-repro.ts server           — trivial SSE origin (deploy this on Compute)
//   bun edge-repro.ts probe <baseUrl>  — client that demonstrates the problem
//
// The origin does nothing except stream heartbeats every 5 s, so any
// connection that goes silent for 15 s is dead by definition — no
// application logic can be blamed. The server also exposes /stats so
// client-visible state can be compared with origin-visible truth
// (that comparison is how the problem was found: clients believed
// ~1,000 subscriptions were parked while the origin saw ~60).
//
// No dependencies. Knobs via env: PORT, BURST, PARK_SECS, LOAD_RPS.

const mode = process.argv[2];

// ---------------------------------------------------------------- server
if (mode === "server") {
  const port = Number(process.env.PORT ?? 8080);
  let seq = 0;
  const open = new Map<number, { openedAt: number; writes: number }>();
  let accepted = 0;

  Bun.serve({
    port,
    idleTimeout: 0, // never idle-kill: heartbeats flow every 5 s anyway
    fetch(req) {
      const url = new URL(req.url);
      if (url.pathname === "/ping") return new Response("ok\n");
      if (url.pathname === "/echo")
        return req.arrayBuffer().then((b) => new Response(`${b.byteLength}\n`));
      if (url.pathname === "/probe-report") {
        const conns = ((globalThis as any).__probe ?? []) as any[];
        const now = Date.now();
        return Response.json({
          n: conns.length,
          live15s: conns.filter((c) => c.lastByteAt && now - c.lastByteAt < 15000).length,
          silent: conns.filter((c) => c.status === "200" && (!c.lastByteAt || now - c.lastByteAt >= 15000)).length,
          statuses: conns.map((c) => `${c.status}:${c.bytes}b:${c.lastByteAt ? Math.round((now - c.lastByteAt) / 1000) : "-"}s`),
        });
      }
      if (url.pathname === "/stats")
        return Response.json({
          openSse: open.size,
          acceptedSse: accepted,
          oldestOpenSecs: open.size
            ? Math.round((Date.now() - Math.min(...[...open.values()].map((c) => c.openedAt))) / 1000)
            : 0,
        });
      if (url.pathname === "/sse-pad") {
        // Padded open stream: hello + N KiB of SSE comment padding,
        // then heartbeats. Bisects the edge's flush threshold — if
        // padding >= threshold arrives immediately while smaller
        // padding arrives never, the edge flushes on buffer size.
        const kb = Number(url.searchParams.get("kb") ?? 0);
        const id = ++seq;
        let timer: ReturnType<typeof setInterval>;
        const stream = new ReadableStream({
          start(controller) {
            controller.enqueue(`event: hello\ndata: {"conn":${id},"padKb":${kb}}\n\n`);
            if (kb > 0) controller.enqueue(`: ${"p".repeat(kb * 1024)}\n\n`);
            timer = setInterval(() => {
              try {
                controller.enqueue(`event: hb\ndata: {"conn":${id}}\n\n`);
              } catch {
                clearInterval(timer);
              }
            }, 5000);
          },
          cancel() {
            clearInterval(timer);
          },
        });
        return new Response(stream, {
          headers: { "content-type": "text/event-stream", "cache-control": "no-store" },
        });
      }
      if (url.pathname === "/sse-once") {
        // Streaming response that ENDS immediately: if this arrives at
        // the client while /sse never does, the edge is buffering
        // streaming responses until completion (fatal for SSE).
        const stream = new ReadableStream({
          start(controller) {
            controller.enqueue(`event: hello\ndata: {"once":true}\n\n`);
            controller.close();
          },
        });
        return new Response(stream, {
          headers: { "content-type": "text/event-stream", "cache-control": "no-store" },
        });
      }
      if (url.pathname === "/sse") {
        const id = ++seq;
        accepted++;
        let timer: ReturnType<typeof setInterval>;
        const stream = new ReadableStream({
          start(controller) {
            open.set(id, { openedAt: Date.now(), writes: 0 });
            controller.enqueue(`event: hello\ndata: {"conn":${id}}\n\n`);
            timer = setInterval(() => {
              const c = open.get(id);
              if (!c) return;
              c.writes++;
              try {
                controller.enqueue(`event: hb\ndata: {"conn":${id},"n":${c.writes}}\n\n`);
              } catch {
                clearInterval(timer);
                open.delete(id);
              }
            }, 5000);
          },
          cancel() {
            clearInterval(timer);
            open.delete(id);
          },
        });
        return new Response(stream, {
          headers: {
            "content-type": "text/event-stream",
            "cache-control": "no-store",
            // Standard proxy-buffering opt-out (nginx & friends). The
            // /sse-pad and /sse-once endpoints deliberately OMIT it so
            // the buffered behavior stays reproducible side by side.
            "x-accel-buffering": "no",
          },
        });
      }
      return new Response("not found\n", { status: 404 });
    },
  });
  console.log(`edge-repro origin listening on :${port}`);

  // In-region streams probe (finding the hairpin path): when
  // PROBE_TARGET/PROBE_TOKEN/PROBE_KEY are set, park PROBE_N SSE
  // subscriptions against a Prisma Streams server and track per-conn
  // byte flow; /probe-report exposes live-vs-silent so an outside
  // observer can compare in-region vs out-of-region client behavior.
  const target = process.env.PROBE_TARGET;
  if (target) {
    const token = process.env.PROBE_TOKEN!;
    const pkey = process.env.PROBE_KEY!;
    const n = Number(process.env.PROBE_N ?? 16);
    const stream = process.env.PROBE_STREAM ?? "wcL2g-s";
    const conns: { id: number; status: string; bytes: number; lastByteAt: number }[] = [];
    (globalThis as any).__probe = conns;
    for (let i = 0; i < n; i++) {
      const c = { id: i, status: "connecting", bytes: 0, lastByteAt: 0 };
      conns.push(c);
      (async () => {
        try {
          const resp = await fetch(
            `${target}/v1/streams/${stream}${40 + i}/records:sse?cursor=now`,
            {
              headers: {
                authorization: `Bearer ${token}`,
                "prisma-encryption-key": pkey,
                accept: "text/event-stream",
              },
            },
          );
          c.status = String(resp.status);
          const reader = resp.body?.getReader();
          if (!reader) return;
          for (;;) {
            const { done, value } = await reader.read();
            if (done) { c.status = "eof"; break; }
            c.bytes += value?.byteLength ?? 0;
            c.lastByteAt = Date.now();
          }
        } catch (e) {
          c.status = `err:${e}`;
        }
      })();
      await Bun.sleep(300);
    }
  }
}

// ---------------------------------------------------------------- probe
if (mode === "probe") {
  const base = process.argv[3];
  if (!base) {
    console.error("usage: bun edge-repro.ts probe <baseUrl>");
    process.exit(1);
  }
  const BURST = Number(process.env.BURST ?? 100);
  const PARK_SECS = Number(process.env.PARK_SECS ?? 120);
  const LOAD_RPS = Number(process.env.LOAD_RPS ?? 200);

  type Conn = {
    id: number;
    status: number | "timeout" | "error";
    ttfbMs?: number;
    lastByteAt?: number;
    bytes: number;
    reader?: ReadableStreamDefaultReader<Uint8Array>;
  };
  const conns: Conn[] = [];

  const stats = async () => {
    try {
      const r = await fetch(`${base}/stats`, { signal: AbortSignal.timeout(8000) });
      return await r.json();
    } catch (e) {
      return { error: String(e) };
    }
  };

  // Open one SSE conn; resolve once headers arrive (or fail).
  const openSse = async (id: number, timeoutMs = 8000): Promise<Conn> => {
    const c: Conn = { id, status: "error", bytes: 0 };
    const t0 = performance.now();
    try {
      // Connect-phase timeout ONLY: AbortSignal.timeout() would abort
      // the parked BODY at the deadline too (self-inflicted zombies —
      // caught by the localhost baseline run).
      const ctl = new AbortController();
      const connectTimer = setTimeout(() => ctl.abort("connect-timeout"), timeoutMs);
      const resp = await fetch(`${base}/sse`, {
        headers: { accept: "text/event-stream" },
        signal: ctl.signal,
      });
      clearTimeout(connectTimer);
      c.status = resp.status;
      c.ttfbMs = Math.round(performance.now() - t0);
      if (resp.status === 200 && resp.body) {
        c.reader = resp.body.getReader();
        // Background read loop: track byte flow per conn.
        (async () => {
          try {
            for (;;) {
              const { done, value } = await c.reader!.read();
              if (done) break;
              c.bytes += value?.byteLength ?? 0;
              c.lastByteAt = Date.now();
            }
          } catch {}
        })();
      }
    } catch (e) {
      c.status =
        String(e).includes("connect-timeout") || String(e).includes("TimeoutError")
          ? "timeout"
          : "error";
    }
    return c;
  };

  const liveCount = () => {
    // Heartbeats are every 5 s: silent for >15 s = dead, whatever TCP thinks.
    const now = Date.now();
    return conns.filter((c) => c.status === 200 && c.lastByteAt && now - c.lastByteAt < 15000).length;
  };

  const p = (s: string) => console.log(s);

  p(`# edge-repro probe against ${base}`);
  p(`server before: ${JSON.stringify(await stats())}`);

  // -- Phase A: sequential connects (baseline the edge cannot blame on bursts)
  let seqOk = 0;
  for (let i = 0; i < 15; i++) {
    const c = await openSse(1000 + i);
    if (c.status === 200) seqOk++;
    conns.push(c);
  }
  p(`\nPhase A  sequential 15 connects: ok=${seqOk} fail=${15 - seqOk}`);

  // -- Phase B: concurrent burst
  const burst = await Promise.all(
    Array.from({ length: BURST }, (_, i) => openSse(2000 + i)),
  );
  conns.push(...burst);
  const okB = burst.filter((c) => c.status === 200).length;
  const toB = burst.filter((c) => c.status === "timeout").length;
  p(`Phase B  burst ${BURST} connects: ok=${okB} timeout=${toB} error=${BURST - okB - toB}`);
  p(`server after connects: ${JSON.stringify(await stats())}  (client holds ${conns.filter((c) => c.status === 200).length})`);

  // -- Phase C: park and watch heartbeat liveness vs server truth
  p(`\nPhase C  parking ${PARK_SECS}s (heartbeats every 5 s; silent >15 s = zombie)`);
  const parkEnd = Date.now() + PARK_SECS * 1000;
  while (Date.now() < parkEnd) {
    await Bun.sleep(15000);
    const s = await stats();
    p(`  t+${Math.round((PARK_SECS * 1000 - (parkEnd - Date.now())) / 1000)}s  client-live=${liveCount()}  client-held=${conns.filter((c) => c.status === 200).length}  server=${JSON.stringify(s)}`);
  }

  // -- Phase D: request load while the fleet is parked
  p(`\nPhase D  ${LOAD_RPS} rps POST /echo for 60 s while parked`);
  let loadOk = 0, loadErr = 0;
  const body = "x".repeat(1024);
  const dEnd = Date.now() + 60_000;
  const worker = async () => {
    while (Date.now() < dEnd) {
      try {
        const r = await fetch(`${base}/echo`, { method: "POST", body, signal: AbortSignal.timeout(10000) });
        r.ok ? loadOk++ : loadErr++;
        await r.arrayBuffer();
      } catch {
        loadErr++;
      }
      await Bun.sleep(1000 / (LOAD_RPS / 32));
    }
  };
  const workers = Array.from({ length: 32 }, worker);
  // Mid-load: try ONE fresh SSE connect and one plain GET, timed.
  await Bun.sleep(20000);
  const freshSse = await openSse(9999, 15000);
  const t0 = performance.now();
  let pingMs = -1;
  try {
    await fetch(`${base}/ping`, { signal: AbortSignal.timeout(10000) });
    pingMs = Math.round(performance.now() - t0);
  } catch {}
  await Promise.all(workers);
  p(`  load: ok=${loadOk} err=${loadErr}`);
  p(`  mid-load fresh SSE: status=${freshSse.status} ttfb=${freshSse.ttfbMs ?? "-"}ms bytesIn15s=${freshSse.bytes}`);
  p(`  mid-load plain GET /ping: ${pingMs}ms`);
  p(`  post-load client-live=${liveCount()}  server=${JSON.stringify(await stats())}`);

  // -- Verdict
  const held = conns.filter((c) => c.status === 200).length;
  const live = liveCount();
  p(`\n# VERDICT`);
  p(`connect success: sequential ${seqOk}/15, burst ${okB}/${BURST}`);
  p(`held-by-client ${held} vs receiving-heartbeats ${live} (zombies: ${held - live})`);
  p(`healthy expectation: burst ~= sequential success rate, zombies = 0,`);
  p(`fresh SSE mid-load gets headers+hello immediately, server openSse == client-live.`);
  process.exit(0);
}

if (!mode) console.error("usage: bun edge-repro.ts server | probe <baseUrl>");
