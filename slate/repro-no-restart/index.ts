// Minimal reproduction: process death leaves the service unbound and
// never restarted. No dependencies, no state, ~30 lines.
const start = Date.now();
const port = Number(process.env.PORT ?? 8080);

// Leg 4: the production services that zombied all ran with the
// keep-awake guard active. Hypothesis: the platform holds the VM for a
// guarded instance even after the app inside dies -> permanent husk.
if (process.env.KEEP_AWAKE === "1") {
  const { KeepAwakeGuard } = await import("@prisma/compute");
  new KeepAwakeGuard();
  console.log("keep-awake guard active");
}

// CRASHLOOP=1: exit immediately on boot. Models any app that cannot
// stay up — a wrapper whose child binary fails to start, or an instance
// that re-OOMs seconds after every restart because it rejoins a hot
// fleet. This is the leg that reproduces the zombie.
if (process.env.CRASHLOOP === "1") {
  console.log("crashloop mode: exiting now");
  process.exit(1);
}

const server = Bun.serve({
  port,
  async fetch(req) {
    const url = new URL(req.url);
    if (url.pathname === "/health") {
      return new Response(
        `ok pid=${process.pid} uptime_s=${((Date.now() - start) / 1000).toFixed(1)}`,
      );
    }
    if (url.pathname === "/crash") {
      // Deterministic hard exit shortly after responding — models any
      // fatal crash (panic, uncaught error, abort).
      setTimeout(() => process.exit(1), 100);
      return new Response("exiting with code 1 in 100ms\n");
    }
    if (url.pathname === "/oom") {
      // Allocate until the platform's memory limit kills the process —
      // models a real OOM (exit signature we see in production: the
      // instance dies hard under memory pressure).
      setTimeout(() => {
        const hog: Uint8Array[] = [];
        // eslint-disable-next-line no-constant-condition
        while (true) hog.push(new Uint8Array(64 * 1024 * 1024).fill(1));
      }, 100);
      return new Response("allocating until OOM kill\n");
    }
    if (url.pathname === "/wedge") {
      // Stop serving WITHOUT exiting — models a process that is alive
      // but unresponsive (guest thrashing near the memory limit, a
      // wrapper whose child died, a deadlocked runtime). The process
      // keeps running; the listener goes away.
      setTimeout(() => server.stop(true), 100);
      return new Response("closing listener in 100ms; process stays alive\n");
    }
    return new Response(
      "no-restart repro. endpoints: /health, /crash (exit 1), /oom (alloc to death), /wedge (stop serving, stay alive)\n",
    );
  },
});
console.log(`repro app listening on :${port}, pid ${process.pid}`);
