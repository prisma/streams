// A harness's request headers cross the Prisma Compute edge, which strips
// every request header named Prisma-* (edge change #173): HeaderSource
// sends the key, routing key and consumer version under their Streams-
// names, whichever name a header file or a default gives them, the file's
// value in force. Run with `bun test ./bench/k2cost`.
import { afterAll, expect, test } from "bun:test";
import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { HeaderSource } from "./common.ts";

const dir = mkdtempSync(join(tmpdir(), "k2c-headers-"));
afterAll(() => rmSync(dir, { recursive: true, force: true }));

function file(text: string): string {
  const path = join(dir, `h${Math.random().toString(36).slice(2)}.txt`);
  writeFileSync(path, text, { mode: 0o600 });
  return path;
}

test("a header file's Prisma- names are sent under their Streams- names, its values in force", () => {
  const src = new HeaderSource(
    file("authorization: Bearer t\nPrisma-Encryption-Key: fk\nprisma-routing-key: r\nprisma-consumer-version: v\n"),
    { "streams-encryption-key": "default" },
  );
  expect(src.get()).toEqual({
    authorization: "Bearer t",
    "streams-encryption-key": "fk",
    "streams-routing-key": "r",
    "streams-consumer-version": "v",
  });
  expect(src.key()).toBe("fk");
});

test("a default under a Prisma- name is sent under its Streams- name; a Streams- name in one source wins over its Prisma- name", () => {
  expect(new HeaderSource(undefined, { "prisma-encryption-key": "dk" }).get()).toEqual({ "streams-encryption-key": "dk" });
  const both = new HeaderSource(file("streams-encryption-key: s\nprisma-encryption-key: p\n"), {});
  expect([both.get(), both.key()]).toEqual([{ "streams-encryption-key": "s" }, "s"]);
});
