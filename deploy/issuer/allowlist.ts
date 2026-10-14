// The beta's allow-list as the issuer reads it (public-beta issuer triage
// B3.1, B4.2; design §3.2): `beta/allowlist.json` in the control bucket,
// which the owner keeps with `cell-admin allow` and `disallow`. Signing in,
// `/auth/session` and the management calls that give a workspace something
// new (a project create, a credential create, a rotation) check it, so
// taking a workspace off the list stops those within ALLOWLIST_MAX_AGE_MS;
// a workspace off it that still holds a project keeps sign-in, reads,
// revoke and delete (the owner's call 16, the reduced mode: signin.ts,
// management.ts). It fails closed: while the bucket holds no list, no
// workspace is on it.
import type { AllowEntry, Allowlist, StateStore } from "../cell-admin/store";

/// How long a read of the allow-list is taken as current.
export const ALLOWLIST_MAX_AGE_MS = 5_000;

export class AllowlistReader {
  private last: { at: number; doc: Allowlist | null } | null = null;

  constructor(
    private readonly store: StateStore,
    private readonly now: () => number,
    private readonly log: (line: string) => void,
  ) {}

  /// The allow-list's entry for `workspace`: undefined when it has none
  /// (or there is no allow-list), null when the list cannot be read.
  async entryOf(workspace: string): Promise<AllowEntry | undefined | null> {
    const now = this.now();
    if (this.last === null || now - this.last.at > ALLOWLIST_MAX_AGE_MS) {
      try {
        this.last = { at: now, doc: (await this.store.readAllowlist())?.doc ?? null };
      } catch (e) {
        this.log(`reading the allow-list failed: ${(e as Error).message}`);
        this.last = null;
        return null;
      }
    }
    return this.last.doc?.entries.find((e) => e.workspace_id === workspace);
  }
}
