// The paging tool's stand-in as a Composer app of its own (pager.ts says
// what it does): for a scratch or cert cell that has no paging service,
// the https endpoints the watch's ALERT_SINK_URL and HEARTBEAT_URL need,
// with no third-party service, in a Compute project of its own apart from
// the watch's (a stand-in inside the watch's process would die with it,
// and its dead-man would never page). Production pages through the real
// paging service, PagerDuty Events v2 (RUNBOOK §7.5): deploy-release.sh
// refuses this app there.
//
// It runs from the cell watch's release root (watch-release.ts), sharing
// its package.json, bun.lock and prisma.config.ts:
// `deploy/composer/deploy-release.sh <scratch|cert> <tag>-watch --app
// pager-standin` deploys it with the stage's STREAMS_CELL_ORIGIN (the
// uptime check's target, row 1) as its one setting; its two secrets reach
// the platform through `set-secrets --app pager-standin`.
import { module } from "@prisma/composer";
import { envParam, envSecret } from "@prisma/composer-prisma-cloud";
import pager from "./pager-service.ts";

export default module("streams-pager-standin", ({ provision }) => {
  provision(pager, {
    id: "pager",
    input: {
      PAGER_ROUTING_KEY: envSecret("PAGER_ROUTING_KEY"),
      PAGER_HEARTBEAT_TOKEN: envSecret("PAGER_HEARTBEAT_TOKEN"),
      STREAMS_CELL_ORIGIN: envParam("STREAMS_CELL_ORIGIN"),
    },
  });
});
