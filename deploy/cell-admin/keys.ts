// The cell's signing keys, changed only by the owner (public-beta issuer
// triage B1.7; design §7.3 and §7.4): `keys add` publishes a new key's
// public half beside the others (the rotation drill's overlap generation),
// or alone with --retire-others (the compromise drill); `keys retire`
// omits a kid, which retires it at the cell for good (keys contract r5).
// A retired kid is remembered and never published again: the cell refuses
// a keys snapshot in which a retired kid returns, and would go dark.
import { resolve } from "node:path";
import type { Flags } from "./args";
import type { Io } from "./commands";
import { AdminError } from "./errors";
import { mutate } from "./publish";
import { bumpFeed } from "./rules";
import { type CellState, journal } from "./state";
import { openStore, publishNote } from "./stores";
import { readPublicKey } from "./tokens";

function report(io: Io, store: { bundlePath: string }, state: CellState, note: string): void {
  io.out(`keys of cell ${state.cell.cell_id}: ${state.keys.map((k) => `${k.kid} (${k.alg})`).join(", ")}`);
  io.out(`bundle: ${store.bundlePath} (feed_version ${state.feed_version}); ${note}`);
}

export async function keysAdd(f: Flags, io: Io): Promise<void> {
  const store = await openStore(f);
  const key = readPublicKey(resolve(f.must("public-key")));
  const retireOthers = f.has("retire-others");
  await store.exclusive(async () => {
    const { state } = await mutate(store, (s) => {
      if (s.keys.some((k) => k.kid === key.kid)) {
        throw new AdminError(`key ${key.kid} is already a key of cell ${s.cell.cell_id}`);
      }
      if (s.retired_kids.includes(key.kid)) {
        throw new AdminError(`key ${key.kid} was retired: a retired kid never returns to a cell; make a new key`);
      }
      const retired = retireOthers ? s.keys.map((k) => k.kid) : [];
      s.keys = [...(retireOthers ? [] : s.keys), key];
      s.retired_kids.push(...retired);
      bumpFeed(s);
      journal(s, "keys add", `${key.kid}${retired.length ? ` retired=${retired.join(",")}` : ""}`);
    });
    report(io, store, state, publishNote(store));
  });
}

export async function keysRetire(f: Flags, io: Io): Promise<void> {
  const store = await openStore(f);
  const kid = f.must("kid");
  await store.exclusive(async () => {
    const { state } = await mutate(store, (s) => {
      if (!s.keys.some((k) => k.kid === kid)) throw new AdminError(`key ${kid} is not a key of cell ${s.cell.cell_id}`);
      if (s.keys.length === 1) {
        throw new AdminError(`key ${kid} is the cell's last key: add its successor first (keys add)`);
      }
      s.keys = s.keys.filter((k) => k.kid !== kid);
      s.retired_kids.push(kid);
      bumpFeed(s);
      journal(s, "keys retire", kid);
    });
    report(io, store, state, publishNote(store));
  });
}
