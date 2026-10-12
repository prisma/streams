// The interim issuer's Compute service (public-beta issuer triage B6.1): a
// node build of one file, entry.js beside this one, which a release cuts
// by bundling entry.ts with everything it imports (the issuer, cell-admin's
// library, the contract schemas, the profiles, Composer's runtime and
// @prisma/compute's keep-awake guard):
//
//   bun build deploy/composer-issuer/issuer/entry.ts --target=bun \
//     --format=esm --outfile <release>/issuer/entry.js
//
// (with deploy/composer-issuer's packages installed; D0 finding 2: Compute
// runs the build with no node_modules). Composer copies only that file.
//
// The service declares no Composer port: its one HTTP port is the one
// Compute routes to every service, served at the issuer's stable hostname.
// Each input field is named as its platform variable (app.ts binds them);
// a setting is a string the issuer's own config.ts validates at boot, and
// one config.ts defaults may be absent. `prisma deploy` loads this file
// with Node's type stripping, so it imports only Composer and arktype.
import { compute } from "@prisma/composer-prisma-cloud";
import { secretString } from "@prisma/composer/arktype";
import node from "@prisma/composer/node";
import { type } from "arktype";

export default compute({
  name: "issuer",
  deps: {},
  input: type({
    STREAMS_CELL_ID: "string",
    STREAMS_ISSUER_ID: "string",
    STREAMS_CELL_ORIGIN: "string",
    STREAMS_ISSUER_ORIGIN: "string",
    STREAMS_ISSUER_OAUTH_CLIENT_ID: "string",
    "STREAMS_ISSUER_OAUTH_AUTHORITY?": "string",
    "STREAMS_ISSUER_TOKEN_TTL_SECS?": "string",
    STREAMS_CONTROL_S3_ENDPOINT: "string",
    STREAMS_CONTROL_S3_BUCKET: "string",
    STREAMS_CONTROL_S3_REGION: "string",
    STREAMS_FEEDS_S3_ENDPOINT: "string",
    STREAMS_FEEDS_S3_BUCKET: "string",
    STREAMS_FEEDS_S3_REGION: "string",
    STREAMS_ISSUER_SIGNING_KEY: secretString(),
    STREAMS_ISSUER_PEPPER: secretString(),
    STREAMS_ISSUER_SESSION_KEY: secretString(),
    STREAMS_ISSUER_OAUTH_CLIENT_SECRET: secretString(),
    STREAMS_ISSUER_METRICS_TOKEN: secretString(),
    STREAMS_CONTROL_S3_ACCESS_KEY_ID: secretString(),
    STREAMS_CONTROL_S3_SECRET_ACCESS_KEY: secretString(),
    STREAMS_FEEDS_S3_ACCESS_KEY_ID: secretString(),
    STREAMS_FEEDS_S3_SECRET_ACCESS_KEY: secretString(),
  }),
  build: node({ module: import.meta.url, entry: "entry.js" }),
});
