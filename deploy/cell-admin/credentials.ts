// Customer credentials with a secret, made by the owner (public-beta
// issuer triage B1.8): `credential` adds one to a project and writes its
// secret, once, to a new file only its owner can read; the state keeps the
// secret's peppered hash (secrets.ts), exactly as the issuer stores the
// credentials it creates, so the issuer's exchange serves both.
import { existsSync } from "node:fs";
import { resolve } from "node:path";
import type { Flags } from "./args";
import type { Io } from "./commands";
import { AdminError } from "./errors";
import { mutate } from "./publish";
import { addCredential, isCanary, newCredentialId, prefixesOf, scopesOf } from "./rules";
import { newSecret, presentedSecret, readPepperFile, secretFields } from "./secrets";
import { journal, writeDurable } from "./state";
import { openStore, publishNote } from "./stores";

export async function credential(f: Flags, io: Io): Promise<void> {
  const store = await openStore(f);
  const project = f.must("project");
  const out = resolve(f.must("out"));
  const hidden = f.has("hidden");
  if (hidden !== isCanary(project)) {
    throw new AdminError(
      hidden ? "--hidden is for the canary's credentials only" : "the canary's credentials are hidden: pass --hidden",
    );
  }
  if (existsSync(out)) throw new AdminError(`--out ${out} exists: a secret is written once, to a new file`);
  const peppers = readPepperFile(resolve(f.must("pepper-file")));
  const scopes = scopesOf(f.get("scopes"));
  const stream_prefixes = prefixesOf(f.all("prefix"));
  const secret = newSecret();
  const fields = secretFields(peppers, secret);
  await store.exclusive(async () => {
    const { result: id, state } = await mutate(store, (s) => {
      const id = newCredentialId(s);
      const display_name = f.get("name");
      addCredential(s, project, {
        scopes,
        stream_prefixes,
        credential: { id, ...fields, ...(display_name ? { display_name } : {}), ...(hidden ? { hidden } : {}) },
      });
      journal(s, "credential", `${project} credential=${id}${hidden ? " hidden" : ""}`);
      return id;
    });
    writeDurable(out, `${presentedSecret(id, secret)}\n`);
    io.out(`credential ${id} for ${project} (${scopes}), hint ${fields.hint}`);
    io.out(`its secret is in ${out} (mode 0600) and nowhere else: hand the file over, never paste it`);
    io.out(`bundle: ${store.bundlePath} (feed_version ${state.feed_version}); ${publishNote(store)}`);
  });
}
