// The cell app's Composer configuration, read by `prisma deploy` from the
// release directory's root (app.ts says why there). The Prisma Cloud
// target takes its workspace and region from PRISMA_WORKSPACE_ID and
// PRISMA_REGION, which deploy-release.sh sets for the stage it deploys
// (triage B5): no region is written here, because a project cannot be
// re-homed and the production region is the owner's (H1).
import { defineConfig as composer } from "@prisma/composer/config";
import { nodeBuild } from "@prisma/composer/node/control";
import { prismaCloud, prismaState } from "@prisma/composer-prisma-cloud/control";
import { definePrismaConfig } from "prisma/config";

export default definePrismaConfig({
  composer: composer({ extensions: [prismaCloud(), nodeBuild()], state: prismaState() }),
});
