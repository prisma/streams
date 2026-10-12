// The probe as a Composer service: a directory build whose tree is app/
// (the entry and, at deploy time, the nonce file lifecycle.py writes so that
// each redeploy is a new artifact and therefore a new Compute version).
import { compute } from "@prisma/composer-prisma-cloud";
import node from "@prisma/composer/node";

export default compute({
  name: "sigtermprobe",
  deps: {},
  build: node({ module: import.meta.url, dir: "./app", entry: "probe.ts" }),
});
