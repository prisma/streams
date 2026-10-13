// The cell watch's start (public-beta alerting triage B4), from its inputs
// as the entry hands them over: read them (config.ts; an InputError is the
// caller's refusal), check the deployment bearer with one GET of
// /operator/data.json (a 401 or 403 is the same refusal: a watch whose
// STREAMS_OPERATOR_TOKEN the cell does not take would run blind, so it
// does not start and the dead-man, row 2, pages; a cell that does not
// answer is row 3's and the watch starts), take the keep-awake guard,
// answer `ok` on the service's port, then run a cycle every 15 s for the
// process's life (watch.ts). The boot line names the cell, the sink's kind
// and which optional rows run, never a URL or a key.
import { InputError, type Inputs, readWatchConfig } from "./config";
import { bearerRefusal } from "./poll";
import { CellWatch, serveOk } from "./watch";

export interface Running {
  port: number;
  /// Ends the loop once the cycle in flight has, then the port.
  stop: () => Promise<void>;
}

/// `keepAwake` takes Compute's keep-awake guard, held until the process
/// ends, and says how it went (`held`, or `unavailable` off Compute).
export async function runWatch(
  inputs: Inputs,
  port: number,
  keepAwake: () => string,
  log: (line: string) => void = console.log,
): Promise<Running> {
  const cfg = readWatchConfig(inputs);
  const refused = await bearerRefusal(cfg.poll);
  if (refused !== null) {
    throw new InputError(`STREAMS_OPERATOR_TOKEN is refused by the cell (/operator/data.json answered ${refused})`);
  }
  const awake = keepAwake();
  const server = serveOk(port);
  const bound = server.port ?? port;
  log(
    `cell-watch: cell ${cfg.rules.cellId}, sink ${cfg.sink.kind}, notify_only ${cfg.rules.notifyOnly}, ` +
      `issuer ${cfg.rules.issuer}, expected_git ${cfg.rules.expectedGit !== ""}, keep_awake ${awake}, ` +
      `listening on :${bound}`,
  );
  const stopLoop = new CellWatch(cfg, { log }).start();
  return {
    port: bound,
    stop: async () => {
      await stopLoop();
      server.stop(true);
    },
  };
}
