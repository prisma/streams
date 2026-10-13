// The cell watch's Composer app (public-beta alerting triage B4, D3): the
// watch's module and nothing else, an app of its own so that it deploys
// into a Compute project of its own (owner answer I1 (a) of 2026-10-12;
// Composer names the project after this root module, or `--name`). The
// project holds the watch's inputs and no other secret: not the cell's
// bucket keys or usage key, not the issuer's signing key, pepper or
// session key, though the watch is the one process with a channel to a
// third party (the pager).
//
// `prisma deploy app.ts` (and `prisma dev app.ts`) runs from this
// directory, the app's Composer project root: package.json, bun.lock,
// prisma.config.ts, node_modules installed with `bun install
// --frozen-lockfile`, and entry.js built (service.ts says how). The release
// run that deploys the cell deploys this app too, with the settings
// exported in its shell (module.ts).
import { module } from "@prisma/composer";
import cellWatch from "./module.ts";

export default module("streams-cell-watch", ({ provision }) => {
  provision(cellWatch);
});
