import { ensureComputeArgv } from "./entry";

process.argv = ensureComputeArgv(process.argv);
await import("../server");
