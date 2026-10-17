// The paging stand-in's Compute service (pager-app.ts): a node build of one
// file, pager-entry.js beside this one, which a release cuts by bundling
// pager-entry.ts with everything it imports:
//
//   bun build deploy/cell-watch/pager-entry.ts --target=bun --format=esm \
//     --outfile deploy/cell-watch/pager-entry.js
//
// Its inputs are named as their platform variables (pager.ts's
// PAGER_INPUTS): the two secrets as secretString(), the cell's origin (or
// none) as a string; none is optional.
import { compute } from "@prisma/composer-prisma-cloud";
import { secretString } from "@prisma/composer/arktype";
import node from "@prisma/composer/node";
import { type } from "arktype";

export default compute({
  name: "pagerstandin",
  deps: {},
  input: type({
    PAGER_ROUTING_KEY: secretString(),
    PAGER_HEARTBEAT_TOKEN: secretString(),
    STREAMS_CELL_ORIGIN: "string",
  }),
  build: node({ module: import.meta.url, entry: "pager-entry.js" }),
});
