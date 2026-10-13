// The cell watch as a Composer module (public-beta alerting triage B4): its
// one service, bound to its inputs by their platform names. app.ts mounts
// it as an app of its own; the owner's veto line on Q5 (I1 of 2026-10-12
// chose the build default, its own app) moves it into the cell's or the
// issuer's app with one import and one `provision(cellWatch)` there.
//
// The secrets are envSecrets, which the platform must hold in the stage's
// scope before the deploy (set-secrets, never the deploy shell). The
// settings are envParams, which the deploy reads from the shell that runs
// it and writes into the service's input document as literals (deploy-q5's
// D0, finding 4), so no secret is ever one; a setting unset there is left
// out of the input document, which the service's schema then refuses by
// its name. An optional input is `none` when absent (config.ts).
// composer.test.ts holds these lines equal to config.ts's names and
// service.ts's schema.
import { module } from "@prisma/composer";
import { envParam, envSecret } from "@prisma/composer-prisma-cloud";
import watch from "./service.ts";

export default module("cellwatch", ({ provision }) => {
  provision(watch, {
    id: "watch",
    input: {
      STREAMS_OPERATOR_TOKEN: envSecret("STREAMS_OPERATOR_TOKEN"),
      ALERT_SINK_URL: envSecret("ALERT_SINK_URL"),
      ALERT_SINK_KEY: envSecret("ALERT_SINK_KEY"),
      HEARTBEAT_URL: envSecret("HEARTBEAT_URL"),
      STREAMS_ISSUER_METRICS_TOKEN: envSecret("STREAMS_ISSUER_METRICS_TOKEN"),
      CANARY_CREDENTIAL: envSecret("CANARY_CREDENTIAL"),
      STREAMS_CELL_ORIGIN: envParam("STREAMS_CELL_ORIGIN"),
      STREAMS_CELL_ID: envParam("STREAMS_CELL_ID"),
      ALERT_SINK_KIND: envParam("ALERT_SINK_KIND"),
      EXPECTED_GIT: envParam("EXPECTED_GIT"),
      NOTIFY_ONLY: envParam("NOTIFY_ONLY"),
      ISSUER_ORIGIN: envParam("ISSUER_ORIGIN"),
    },
  });
});
