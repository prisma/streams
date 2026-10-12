// The probe's Composer app. lifecycle.py deploys it with
// `prisma deploy app.ts --name <scratch app> --stage <probe stage>`, so it
// runs in its own Branch of the scratch project and leaves the cell's alone.
import { module } from "@prisma/composer";
import probe from "./service.ts";

export default module("streams-sigterm-probe", ({ provision }) => {
  provision(probe, { id: "sigtermprobe" });
});
