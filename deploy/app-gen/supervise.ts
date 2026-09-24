// Run the downloaded binary and, if it dies, make the death visible.
//
// Compute keeps reporting a version as "running" after its process exits;
// the only symptom a caller sees is the domain answering 404/503, which is
// indistinguishable from a cold start, a wrong-arch binary, or a crash
// loop. Diagnosing one of these cost a soak window (2026-07-26: a missing
// BENCH_SHAPE made a clap arg fail to parse, so the binary exited before
// binding $PORT).
//
// So: capture stderr, and when the child exits, bind $PORT ourselves and
// serve the exit code plus the tail of its stderr. A dead service then
// explains itself over HTTP instead of looking like a platform fault.
//
// Item 39: that holds only for a death at or near boot. A child that was
// READY (it accepted on $PORT and had been up for READY_UPTIME_MS) failed at
// runtime (an OOM kill, or item 38: a critical loop's exit makes
// streams-slate stop and exit 1), and holding its port would leave a 500 the
// platform never replaces. A caller that serves on $PORT therefore passes
// `{ onDeathAfterReady: "exit" }`: the wrapper then exits with the child's
// own code (a graceful 0 stays 0, a signal is 128 + its number) and Compute
// reprovisions the instance. A child that never accepted, or died within its
// first READY_UPTIME_MS, failed its boot (arguments, environment, arch, a
// store it cannot open); restarting it would crash-loop into a silent
// platform zombie, so that death is still held and served, as it always was.
//
// Which app passes which policy is `policyFor`, and every caller must pass
// one: there is no default, because a silently defaulted "hold" would bring
// back the unreplaced 500 that item 39 removed.
//
// Known gap (predates item 39): the wrapper does not forward SIGTERM/SIGINT
// to the child. A signal to the wrapper ends it at once and leaves the child
// running; the child's graceful stop runs only when the platform signals
// the child itself.

const TAIL_BYTES = 16 * 1024;
/// How often the wrapper checks whether the child accepts on $PORT yet.
const READY_PROBE_MS = 250;
/// How long a child that accepted on $PORT must have been up before its
/// death counts as a runtime death rather than a boot failure (item 39).
/// Counted from the spawn, not from the first accept (owner decision
/// D4(c)), on the monotonic clock: a wall-clock step at boot cannot move a
/// death across the line.
const READY_UPTIME_MS = 60_000;

export type DeathPolicy = {
  /// "exit": a ready child's death ends the wrapper too, so the platform
  /// replaces the instance. "hold": every death is served as a diagnostic
  /// (a workload that runs to completion).
  onDeathAfterReady: "exit" | "hold";
  /// Tests only: overrides READY_UPTIME_MS.
  readyUptimeMs?: number;
};

/// The wrapper apps under deploy/, each with its own index.ts.
export type App = "app-server" | "app-lb" | "app-gen";

/// Item 39's policy per app, from the environment the app was deployed with
/// (owner decisions D5 and skeptic C2). The stream server and the pilot
/// router (app-lb with PILOT_MODE unset or "lb") serve until they die, so a
/// death after ready ends the wrapper and Compute replaces the instance. A
/// load generator holds every death: app-gen (awsbench), and app-lb running
/// the pilot's generator or benchmark (PILOT_MODE=gen or bench), because a
/// restarted generator would re-ramp load mid-campaign and lose the stderr
/// tail that explains its failure.
export function policyFor(app: App, env: Record<string, string | undefined>): DeathPolicy {
  switch (app) {
    case "app-server":
      return { onDeathAfterReady: "exit" };
    case "app-lb":
      return { onDeathAfterReady: (env.PILOT_MODE ?? "lb") === "lb" ? "exit" : "hold" };
    case "app-gen":
      return { onDeathAfterReady: "hold" };
  }
  throw new Error(`policyFor: unknown wrapper app ${JSON.stringify(app)}`);
}

export async function superviseBinary(
  bin: string,
  argv: string[],
  env: Record<string, string | undefined>,
  policy: DeathPolicy,
): Promise<never> {
  const death = policy?.onDeathAfterReady;
  if (death !== "exit" && death !== "hold") {
    throw new Error(
      `superviseBinary: a death policy is required (policyFor), got ${JSON.stringify(policy)}`,
    );
  }
  const port = process.env.PORT ?? "8080";
  const started = performance.now();
  const proc = Bun.spawn([bin, ...argv], {
    env,
    stdout: "inherit",
    stderr: "pipe",
  });

  // Tee stderr: still goes to the platform log, and we keep the tail.
  let tail = "";
  const pump = (async () => {
    const dec = new TextDecoder();
    for await (const chunk of proc.stderr as ReadableStream<Uint8Array>) {
      const s = dec.decode(chunk, { stream: true });
      process.stderr.write(s);
      tail = (tail + s).slice(-TAIL_BYTES);
    }
  })();

  // Whether the child ever accepted a connection on $PORT.
  let accepted = false;
  let exited = false;
  const probe = (async () => {
    while (!accepted && !exited) {
      accepted = await accepts(Number(port));
      if (!accepted) await Bun.sleep(READY_PROBE_MS);
    }
  })();

  const code = await proc.exited;
  exited = true;
  const uptime = Math.round(performance.now() - started);
  await pump.catch(() => {});
  await probe.catch(() => {});

  const ready = accepted && uptime >= (policy.readyUptimeMs ?? READY_UPTIME_MS);
  if (ready && policy.onDeathAfterReady === "exit") {
    console.error(
      `binary exited with code ${code} after serving on :${port} (up ${uptime} ms); exiting ${code} so the platform replaces this instance`,
    );
    process.exit(code);
  }

  const body = JSON.stringify(
    {
      error: "binary_exited",
      binary: bin,
      argv,
      exitCode: code,
      // A binary that never bound the port almost always failed argument
      // or environment validation; say so rather than making the reader
      // guess from an empty log.
      hint:
        code !== 0
          ? "non-zero exit: check required env vars and that the binary is x86_64"
          : "clean exit: the workload finished",
      stderrTail: tail.slice(-8192),
    },
    null,
    2,
  );

  console.error(`binary exited with code ${code}; serving diagnostic on :${port}`);
  Bun.serve({
    port: Number(port),
    fetch: () =>
      new Response(body, {
        status: 500,
        headers: { "content-type": "application/json" },
      }),
  });

  // Never resolve: keep the diagnostic reachable for the operator.
  return new Promise<never>(() => {});
}

/// One TCP connect to the child's port on this host.
async function accepts(port: number): Promise<boolean> {
  try {
    const socket = await Bun.connect({
      hostname: "127.0.0.1",
      port,
      socket: { data() {} },
    });
    socket.end();
    return true;
  } catch {
    return false;
  }
}
