// The interim issuer's Compute entry (public-beta issuer triage B6),
// bundled into entry.js when a release is cut (service.ts). Composer's
// runtime has left the service's input document in the process
// environment (COMPOSER_INPUT), beside the secrets' platform variables;
// the entry hands the issuer (deploy/issuer/run.ts) each input field under
// its own name, a secret through its box's one reader, and Compute's PORT,
// and nothing else from the process environment. KEEP_AWAKE is no input,
// so the issuer holds Compute's keep-awake guard (owner answer I10),
// bundled here. An input that cannot be read (a secret's platform variable
// unset or empty, a document that does not parse or validate) is a boot
// refusal like the issuer's own: exit 1, with Composer's message, which
// names variables and never their values.
import { KeepAwakeGuard } from "@prisma/compute";
import { runIssuer } from "../../issuer/run.ts";
import service from "./service.ts";

/// The service's input, each secret read out of its box (a setting left
/// unset at the deploy is absent from it).
function inputs(): Record<string, string> {
  let input: Readonly<Record<string, string | { expose(): string } | undefined>>;
  try {
    input = service.input();
  } catch (e) {
    console.error(`streams-issuer: the service's input: ${(e as Error).message}`);
    process.exit(1);
  }
  const out: Record<string, string> = {};
  for (const [name, v] of Object.entries(input)) if (v !== undefined) out[name] = typeof v === "string" ? v : v.expose();
  return out;
}

await runIssuer({ ...inputs(), PORT: process.env.PORT }, async () => new KeepAwakeGuard());
