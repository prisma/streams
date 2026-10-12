// The issuer app's Composer configuration, the cell app's exactly
// (deploy/composer/prisma.config.ts), read by `prisma deploy` from the
// issuer's release directory (app.ts says what it holds). The Prisma Cloud
// target takes its workspace and region from PRISMA_WORKSPACE_ID and
// PRISMA_REGION, which the release run sets as it does for the cell: no
// region is written here (H1).
import { defineConfig as composer } from "@prisma/composer/config";
import { nodeBuild } from "@prisma/composer/node/control";
import { prismaCloud, prismaState } from "@prisma/composer-prisma-cloud/control";
import { definePrismaConfig } from "prisma/config";

export default definePrismaConfig({
  composer: composer({ extensions: [prismaCloud(), nodeBuild()], state: prismaState() }),
});
