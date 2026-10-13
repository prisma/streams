// Destroy one named stage of the shared cell's Composer app (public-beta
// deploy-q5 triage B5; D0 finding 3: the prisma CLI has no destroy, so
// this is Composer's own `destroy` of @prisma/composer/control). A release
// carries it at its root, beside prisma.config.ts and node_modules, and
// `deploy-release.sh <stage> <release-dir> --destroy` runs it there with
// PRISMA_SERVICE_TOKEN and PRISMA_WORKSPACE_ID in its environment:
//
//   bun destroy.ts <stage>
//
// Composer removes the stage's services and then its Branch. The cell's
// buckets are outside Composer (design §2.4 (b)) and stay;
// `provision-cell.py <stage> --teardown` removes a scratch or cert
// stage's. The default stage (production) is never destroyed: this
// refuses it, as deploy-release.sh does.
import { destroy } from "@prisma/composer/control";
import config from "./prisma.config.ts";

const stage = process.argv[2];
if (!stage || stage === "production" || process.argv.length !== 3) {
  console.error("usage: bun destroy.ts <scratch|cert>: the default stage (production) is never destroyed");
  process.exit(2);
}
const result = await destroy({
  config: { value: config.composer, file: "./prisma.config.ts" },
  entry: "deploy/composer/app.ts",
  target: { kind: "stage", stage },
  cwd: process.cwd(),
  onEvent: (event) => console.log(`destroy: ${JSON.stringify(event)}`),
});
if (!result.ok) {
  console.error(`destroy: stage ${stage} was not destroyed: ${result.failure.code}: ${result.failure.message}`);
  process.exit(1);
}
console.log(`destroy: stage ${stage} of the cell's app is destroyed`);
