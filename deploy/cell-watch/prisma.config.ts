// The cell watch app's Composer configuration, the cell's and the issuer's
// shape (deploy-q5's D0, finding 3), read by `prisma deploy app.ts` and
// `prisma dev app.ts` from this directory. The Prisma Cloud target takes
// its workspace and region from PRISMA_WORKSPACE_ID and PRISMA_REGION in the
// release run's environment: no region is written here, because a project
// cannot be re-homed and the production region is the owner's (H1).
import { defineConfig as composer } from "@prisma/composer/config";
import { nodeBuild } from "@prisma/composer/node/control";
import { prismaCloud, prismaState } from "@prisma/composer-prisma-cloud/control";
import { definePrismaConfig } from "prisma/config";

export default definePrismaConfig({
  composer: composer({ extensions: [prismaCloud(), nodeBuild()], state: prismaState() }),
});
