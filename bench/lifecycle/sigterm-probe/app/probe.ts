// sigterm-probe (deploy-q5 triage B6; design §3.4 leg 1): what a Compute
// instance's stop does to the process the platform started. It serves 200 on
// $PORT, starts one child in its own process group, and logs a SIGTERM and
// then once a second until it is killed; it never forwards a signal and never
// exits by itself. Its log answers D10's three questions: does anything
// arrive (a SIGTERM line or none), at the process or its whole group (the
// child logs only a signal the platform sent it), and how many seconds pass
// before the kill (the last tick). bench/lifecycle/lifecycle.py reads it.
//
// The main process holds the instance awake for its whole life, as the
// cell's entry does with @prisma/compute's KeepAwakeGuard (its
// scale-to-zero control: "+" written to the control file, whose descriptor
// stays open): the cell is kept awake (edge #140), so its stop facts must
// come from a probe Compute's scale-to-zero never suspends. The boot line's
// keep_awake says whether it is held: held, unavailable (no control file,
// i.e. not on Compute) or failed (the write was refused).
import { spawn } from "node:child_process";
import { constants, openSync, readFileSync, writeSync } from "node:fs";
import { createServer } from "node:http";

const role = process.argv[2] === "child" ? "child" : "main";
const read = (path: string | URL): string | null => {
  try {
    return readFileSync(path, "utf8").replaceAll("\0", " ").trim();
  } catch {
    return null;
  }
};
const say = (event: string, fields: string) =>
  console.log(`sigterm-probe ${role} ${event} t=${new Date().toISOString()}${fields}`);
const ids = () => ` pid=${process.pid} ppid=${process.ppid} pgid=${read("/proc/self/stat")?.split(") ")[1]?.split(" ")[2] ?? "?"}`;

let termAt = 0;
process.on("SIGTERM", () => {
  if (termAt) return;
  termAt = Date.now();
  say("SIGTERM", ` pid=${process.pid}`);
  setInterval(() => say("alive", ` after=${Math.round((Date.now() - termAt) / 1000)}s`), 1000);
});

/// The control file Compute's runtime reads (scale-to-zero-control.js in
/// @prisma/compute); the variable exists for the tests only.
const KEEP_AWAKE_CONTROL = process.env.SIGTERM_PROBE_KEEP_AWAKE_CONTROL ?? "/uk/libukp/scale_to_zero_disable";
function holdAwake(): "held" | "unavailable" | "failed" {
  let fd: number;
  try {
    fd = openSync(KEEP_AWAKE_CONTROL, constants.O_WRONLY);
  } catch {
    return "unavailable";
  }
  try {
    writeSync(fd, "+"); // the descriptor is never closed: held until the kill
    return "held";
  } catch {
    return "failed";
  }
}

if (role === "main") {
  const keepAwake = holdAwake();
  const port = Number(process.env.PORT ?? 3000);
  // The tests bind 127.0.0.1 (SIGTERM_PROBE_HOST exists for them only): on
  // macOS another socket can take 127.0.0.1 on a port held at the wildcard.
  createServer((_, res) => res.end("sigterm-probe\n")).listen(port, process.env.SIGTERM_PROBE_HOST ?? "0.0.0.0");
  spawn(process.execPath, [import.meta.filename, "child"], { stdio: "inherit" });
  const nonce = read(new URL("./nonce.txt", import.meta.url));
  const runtime = process.versions.bun ? `bun-${process.versions.bun}` : `node-${process.versions.node}`;
  say("boot", `${ids()} runtime=${runtime} port=${port} nonce=${nonce === null ? "-" : JSON.stringify(nonce)} keep_awake=${keepAwake} pid1=${JSON.stringify(read("/proc/1/cmdline") ?? "?")}`);
} else {
  say("boot", ids());
  setInterval(() => {}, 1 << 30);
}
