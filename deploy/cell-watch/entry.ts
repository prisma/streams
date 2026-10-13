// The cell watch's Compute entry (public-beta alerting triage B4), bundled
// into entry.js when a release is cut (service.ts says how). Composer's
// runtime has left the service's input document in the process
// environment beside the secrets' platform variables; the entry hands the
// watch each input field under its own name, a secret through its box's
// one reader, and the service's port, and reads nothing else from the
// process environment. It holds Compute's keep-awake guard for the
// process's life, as the cell's wrapper does (no KEEP_AWAKE_UNTIL_MS): a
// Compute service with no inbound traffic and no guard sleeps, and its
// timers stop. Outside Compute the guard is a no-op (triage N4).
//
// An input that cannot be read (a secret's platform variable unset or
// empty, a document that does not parse or validate) or that the watch
// refuses is a boot refusal: exit 1 with a line naming the input, never a
// value. The heartbeat then never comes, and the dead-man (row 2) pages.
import { existsSync } from "node:fs";
import { KeepAwakeGuard } from "@prisma/compute";
import { INPUTS, InputError, type Inputs } from "./config.ts";
import { runWatch } from "./run.ts";
import service from "./service.ts";

/// Compute's scale-to-zero control, which the guard writes `+` to.
const KEEP_AWAKE_CONTROL = "/uk/libukp/scale_to_zero_disable";

/// The guard, held for the process's life.
const held: KeepAwakeGuard[] = [];

function refuse(reason: string): never {
  console.error(`cell-watch: refused: ${reason}`);
  process.exit(1);
}

/// The service's input, each secret read out of its box.
function inputs(): Inputs {
  let input: Readonly<Record<string, string | { expose(): string } | undefined>>;
  try {
    input = service.input();
  } catch (e) {
    refuse(`the service's input: ${(e as Error).message}`);
  }
  const out: Record<string, string> = {};
  for (const name of INPUTS) {
    const v = input[name];
    if (v !== undefined) out[name] = typeof v === "string" ? v : v.expose();
  }
  return out;
}

try {
  runWatch(inputs(), service.port(), () => {
    held.push(new KeepAwakeGuard());
    return existsSync(KEEP_AWAKE_CONTROL) ? "held" : "unavailable";
  });
} catch (e) {
  refuse(e instanceof InputError ? e.message : `the watch did not start: ${(e as Error).message}`);
}
