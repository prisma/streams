// The interim issuer's Composer app (public-beta issuer triage B6): the
// `issuer` service and nothing else, an app of its own so that it deploys
// into a Compute project of its own (owner answer I1 (a) of 2026-10-12;
// Composer names the project after the module): no secret of the cell's
// is visible to the issuer's process, and none of the issuer's to the
// cell's.
//
// `prisma deploy app.ts` (and `prisma dev app.ts`) runs from the issuer's
// release directory, which holds this directory's package.json, bun.lock,
// prisma.config.ts, app.ts and issuer/service.ts, node_modules installed
// with `bun install --frozen-lockfile`, and issuer/entry.js, the entry
// bundled from the repository (issuer/service.ts says how). The release
// run that deploys the cell deploys this app too.
//
// The binding: each of the issuer's inputs (deploy/issuer/config.ts's
// INPUTS) under its own name. Its secrets and the two bucket key pairs are
// envSecrets, which the platform must hold in the stage's scope before
// the deploy (set-secrets, never the deploy shell's environment). Every
// other input is an envParam, which `prisma deploy` reads from its own
// environment and writes into the deploy's input document as a literal
// (D0 finding 4), so a secret is never one; an envParam left unset is
// absent, and the issuer takes its default:
//   - STREAMS_CELL_ORIGIN and STREAMS_ISSUER_ORIGIN are the beta's two
//     stable hostnames (owner answer X4 (a)): the exchange's `endpoint`,
//     and where the sign-in's callback is registered;
//   - STREAMS_ISSUER_TOKEN_TTL_SECS unset is 3,600 s (owner answer I5);
//   - STREAMS_ISSUER_OAUTH_AUTHORITY names the mock identity provider for
//     `prisma dev` (the issuer refuses any origin but this machine's);
//     a cloud deploy leaves it unset.
// No input is a generatedParam (triage D3: Composer keeps one as config,
// not as a secret). The OAuth client secret is bound like every secret,
// so the issuer deploys with a confidential client (Console ask C1); a
// public client would drop its line here and in service.ts.
// deploy/issuer/composer.test.ts holds these lines equal to config.ts's
// INPUTS and to service.ts's input schema.
import { module } from "@prisma/composer";
import { envParam, envSecret } from "@prisma/composer-prisma-cloud";
import issuer from "./issuer/service.ts";

export default module("streams-issuer", ({ provision }) => {
  provision(issuer, {
    id: "issuer",
    input: {
      STREAMS_CELL_ID: envParam("STREAMS_CELL_ID"),
      STREAMS_ISSUER_ID: envParam("STREAMS_ISSUER_ID"),
      STREAMS_CELL_ORIGIN: envParam("STREAMS_CELL_ORIGIN"),
      STREAMS_ISSUER_ORIGIN: envParam("STREAMS_ISSUER_ORIGIN"),
      STREAMS_ISSUER_OAUTH_CLIENT_ID: envParam("STREAMS_ISSUER_OAUTH_CLIENT_ID"),
      STREAMS_ISSUER_OAUTH_AUTHORITY: envParam("STREAMS_ISSUER_OAUTH_AUTHORITY"),
      STREAMS_ISSUER_TOKEN_TTL_SECS: envParam("STREAMS_ISSUER_TOKEN_TTL_SECS"),
      STREAMS_CONTROL_S3_ENDPOINT: envParam("STREAMS_CONTROL_S3_ENDPOINT"),
      STREAMS_CONTROL_S3_BUCKET: envParam("STREAMS_CONTROL_S3_BUCKET"),
      STREAMS_CONTROL_S3_REGION: envParam("STREAMS_CONTROL_S3_REGION"),
      STREAMS_FEEDS_S3_ENDPOINT: envParam("STREAMS_FEEDS_S3_ENDPOINT"),
      STREAMS_FEEDS_S3_BUCKET: envParam("STREAMS_FEEDS_S3_BUCKET"),
      STREAMS_FEEDS_S3_REGION: envParam("STREAMS_FEEDS_S3_REGION"),
      STREAMS_ISSUER_SIGNING_KEY: envSecret("STREAMS_ISSUER_SIGNING_KEY"),
      STREAMS_ISSUER_PEPPER: envSecret("STREAMS_ISSUER_PEPPER"),
      STREAMS_ISSUER_SESSION_KEY: envSecret("STREAMS_ISSUER_SESSION_KEY"),
      STREAMS_ISSUER_OAUTH_CLIENT_SECRET: envSecret("STREAMS_ISSUER_OAUTH_CLIENT_SECRET"),
      STREAMS_ISSUER_METRICS_TOKEN: envSecret("STREAMS_ISSUER_METRICS_TOKEN"),
      STREAMS_CONTROL_S3_ACCESS_KEY_ID: envSecret("STREAMS_CONTROL_S3_ACCESS_KEY_ID"),
      STREAMS_CONTROL_S3_SECRET_ACCESS_KEY: envSecret("STREAMS_CONTROL_S3_SECRET_ACCESS_KEY"),
      STREAMS_FEEDS_S3_ACCESS_KEY_ID: envSecret("STREAMS_FEEDS_S3_ACCESS_KEY_ID"),
      STREAMS_FEEDS_S3_SECRET_ACCESS_KEY: envSecret("STREAMS_FEEDS_S3_SECRET_ACCESS_KEY"),
    },
  });
});
