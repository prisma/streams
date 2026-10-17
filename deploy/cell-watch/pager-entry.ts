// The paging stand-in's Compute entry (pager-app.ts), bundled into
// pager-entry.js when a release is cut (pager-service.ts says how): its
// service's input, each secret read out of its box, to pagerOptions, then
// the stand-in on the service's port on every interface, with Compute's
// keep-awake guard held for the process's life (its heartbeat window and
// uptime check are timers, which stop when an idle service sleeps). An
// input it cannot read or refuses exits 1 with a line naming the input,
// never a value. The boot line names the checks it runs, never the key,
// the token or a URL.
import { KeepAwakeGuard } from "@prisma/compute";
import { PAGER_INPUTS, Pager, pagerOptions } from "./pager.ts";
import service from "./pager-service.ts";

const held: KeepAwakeGuard[] = [];

function refuse(reason: string): never {
  console.error(`pager-standin: refused: ${reason}`);
  process.exit(1);
}

let options: ReturnType<typeof pagerOptions>;
try {
  const input: Readonly<Record<string, string | { expose(): string } | undefined>> = service.input();
  const out: Record<string, string> = {};
  for (const name of PAGER_INPUTS) {
    const v = input[name];
    if (v !== undefined) out[name] = typeof v === "string" ? v : v.expose();
  }
  options = pagerOptions(out);
} catch (e) {
  refuse((e as Error).message);
}
held.push(new KeepAwakeGuard());
new Pager({ ...options, port: service.port(), hostname: "0.0.0.0" });
console.log(`pager-standin: events and heartbeat on :${service.port()}, uptime check ${options.uptimeOrigin === null ? "off" : "on"}`);
