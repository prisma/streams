import { parseLocalProcessOptions } from "./common";
import { runLocalServerProcess } from "./process";

const opts = parseLocalProcessOptions(process.argv.slice(2), {
  allowPositionalName: true,
  defaultNameWhenMissing: "default",
  defaultHostnameWhenMissing: "127.0.0.1",
  defaultPortWhenMissing: 0,
});

await runLocalServerProcess(
  {
    name: opts.name,
    hostname: opts.hostname,
    port: opts.port,
  },
  {
    onReady: (exportsPayload) => {
      if (typeof process.send === "function") {
        process.send({ type: "ready", exports: exportsPayload });
      } else {
        // eslint-disable-next-line no-console
        console.log(JSON.stringify(exportsPayload));
      }
    },
    closeOnDisconnect: true,
  }
);
