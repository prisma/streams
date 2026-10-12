// The shared cell's Composer app (public-beta deploy-q5 triage B2): the
// cell's service and nothing else. The interim issuer and cell-watch are
// apps of their own, in their own projects (owner answer I1 (a) of
// 2026-10-12), so no secret of theirs is visible to the cell's process
// and none of the cell's to theirs.
//
// `prisma deploy deploy/composer/app.ts` runs from a release directory's
// root, which holds this directory's package.json, bun.lock,
// prisma.config.ts and node_modules (D0 finding 1: the Composer project
// root must sit outside the service's build dir, deploy/), never from
// this directory; cut-release and deploy-release.sh (triage B5) do that.
//
// The binding: the seven secrets by their platform names, which the
// platform must hold in the stage's scope before the deploy (set-secrets,
// never the deploy shell's environment; triage D10), the stage, which
// names the release's cells/<stage>.env, and the deploy's nonce: the
// release's tag, or `<tag>.r<secs>` for deploy-release.sh --restart, an
// input literal that makes a restart of the same release a new version
// (a deploy of an unchanged input is a no-op, D0 finding 8; lead decision
// deploy-q5 #6 (2)). The entry logs the nonce and nothing else reads it.
// env.test.ts holds these names equal to env.ts's.
import { module } from "@prisma/composer";
import { envParam, envSecret } from "@prisma/composer-prisma-cloud";
import cell from "./streams/service.ts";

export default module("streams-cell", ({ provision }) => {
  provision(cell, {
    id: "streamscell",
    input: {
      operatorToken: envSecret("STREAMS_OPERATOR_TOKEN"),
      usageStreamKey: envSecret("STREAMS_USAGE_STREAM_KEY"),
      cursorKey: envSecret("STREAMS_CURSOR_KEY"),
      dataS3AccessKeyId: envSecret("STREAMS_DATA_S3_ACCESS_KEY_ID"),
      dataS3SecretAccessKey: envSecret("STREAMS_DATA_S3_SECRET_ACCESS_KEY"),
      feedsS3AccessKeyId: envSecret("STREAMS_FEEDS_S3_ACCESS_KEY_ID"),
      feedsS3SecretAccessKey: envSecret("STREAMS_FEEDS_S3_SECRET_ACCESS_KEY"),
      cellStage: envParam("CELL_STAGE"),
      deployNonce: envParam("CELL_DEPLOY_NONCE"),
    },
  });
});
