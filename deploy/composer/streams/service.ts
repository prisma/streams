// The shared cell's Compute service (public-beta deploy-q5 triage B2, as
// D0's local findings correct it): a node build of deploy/ (dir "../.."
// from this file) whose entry is entry.ts bundled into one file at cut
// time (`bun build deploy/composer/streams/entry.ts --target=bun
// --format=esm --outfile deploy/composer/streams/entry.js`; D0 finding 2:
// Compute runs the build's tree with no node_modules, so a TypeScript
// entry with package imports never links there). The release's binary,
// release.json, cells/ and profiles/ ship inside that tree.
//
// No expose: Composer mints no port for the cell, and nothing reaches it
// through one (triage D15, owner answer I1 (a)). Names are letters and
// digits only (D0 finding 3). `prisma deploy` loads this file with Node's
// type stripping, so it imports only Composer and arktype: env.ts's graph
// (cell-admin's errors.ts) has parameter properties, which strip-only mode
// refuses. env.test.ts holds the stages equal to env.ts's STAGES.
import { compute } from "@prisma/composer-prisma-cloud";
import { secretString } from "@prisma/composer/arktype";
import node from "@prisma/composer/node";
import { type } from "arktype";

export default compute({
  name: "streamscell",
  deps: {},
  input: type({
    operatorToken: secretString(),
    usageStreamKey: secretString(),
    cursorKey: secretString(),
    dataS3AccessKeyId: secretString(),
    dataS3SecretAccessKey: secretString(),
    feedsS3AccessKeyId: secretString(),
    feedsS3SecretAccessKey: secretString(),
    cellStage: type.enumerated("scratch", "cert", "production"),
    deployNonce: "string > 0",
  }),
  build: node({ module: import.meta.url, dir: "../..", entry: "composer/streams/entry.js" }),
});
