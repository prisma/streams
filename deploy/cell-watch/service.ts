// The cell watch's Compute service (public-beta alerting triage B4): a node
// build of one file, entry.js beside this one, which a release cuts by
// bundling entry.ts with everything it imports (the watch, Composer's
// runtime and @prisma/compute's keep-awake guard), with this directory's
// packages installed (`bun install --frozen-lockfile`):
//
//   bun build deploy/cell-watch/entry.ts --target=bun --format=esm \
//     --outfile deploy/cell-watch/entry.js
//
// Compute runs the build with no node_modules beside it (deploy-q5's D0,
// finding 2), and Composer copies only that file. `prisma dev` and
// `prisma deploy` need it built first.
//
// No Composer port and no dependency: the watch reaches the cell and the
// issuer at their stable origins, with the bearers it holds as its own
// inputs (triage D2). Each input field is named as its platform variable
// (module.ts binds them; config.ts reads them): a secret as secretString(),
// a setting as a string config.ts validates at boot. None is optional, so a
// setting missing from the deploy's shell fails the deploy by its name
// instead of turning a row off; `none` is how an optional input says it
// is absent (config.ts).
import { compute } from "@prisma/composer-prisma-cloud";
import { secretString } from "@prisma/composer/arktype";
import node from "@prisma/composer/node";
import { type } from "arktype";

export default compute({
  name: "cellwatch",
  deps: {},
  input: type({
    STREAMS_OPERATOR_TOKEN: secretString(),
    ALERT_SINK_URL: secretString(),
    ALERT_SINK_KEY: secretString(),
    HEARTBEAT_URL: secretString(),
    STREAMS_ISSUER_METRICS_TOKEN: secretString(),
    STREAMS_CELL_ORIGIN: "string",
    STREAMS_CELL_ID: "string",
    ALERT_SINK_KIND: "string",
    EXPECTED_GIT: "string",
    NOTIFY_ONLY: "string",
    ISSUER_ORIGIN: "string",
  }),
  build: node({ module: import.meta.url, entry: "entry.js" }),
});
