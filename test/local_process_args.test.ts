import { expect, test } from "bun:test";
import { parseLocalProcessOptions } from "../src/local/common";

test("parseLocalProcessOptions leaves CLI defaults unset when flags are absent", () => {
  expect(parseLocalProcessOptions([])).toEqual({});
});

test("parseLocalProcessOptions applies explicit flag fallbacks", () => {
  expect(parseLocalProcessOptions(["--name", "--hostname", "--port"])).toEqual({
    name: "default",
    hostname: "127.0.0.1",
    port: 0,
  });
});

test("parseLocalProcessOptions supports daemon positional name and missing-value defaults", () => {
  expect(
    parseLocalProcessOptions(["server-a", "--hostname", "0.0.0.0"], {
      allowPositionalName: true,
      defaultNameWhenMissing: "default",
      defaultHostnameWhenMissing: "127.0.0.1",
      defaultPortWhenMissing: 0,
    })
  ).toEqual({
    name: "server-a",
    hostname: "0.0.0.0",
    port: 0,
  });
});

test("parseLocalProcessOptions skips option values when searching for a positional name", () => {
  expect(
    parseLocalProcessOptions(["--hostname", "0.0.0.0", "server-b"], {
      allowPositionalName: true,
      defaultNameWhenMissing: "default",
    })
  ).toEqual({
    name: "server-b",
    hostname: "0.0.0.0",
  });
});
