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

const TAIL_BYTES = 16 * 1024;

export async function superviseBinary(
  bin: string,
  argv: string[] = [],
  env: Record<string, string | undefined> = process.env,
): Promise<never> {
  const port = process.env.PORT ?? "8080";
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

  const code = await proc.exited;
  await pump.catch(() => {});

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
