// The interim issuer's boot (public-beta issuer triage B2.3): it opens the
// cell's state in the control bucket and refuses to run, with a message
// that quotes ids only, unless
//   - both buckets honour conditional writes (S3Store.open's self-test:
//     without them every compare-and-swap would be last-writer-wins);
//   - the state is the state of STREAMS_CELL_ID (S3Store.load);
//   - the state is not behind the bundle the cell serves (cell-admin's
//     loadChecked): a state restored from a stale copy would mint for
//     credentials revoked or rotated in the lost tail, while a running
//     issuer refuses such a state at revalidation (cache.ts) and ages to
//     503, so a redeploy must not bring it up healthy;
//   - the state's issuer is STREAMS_ISSUER_ID, the `iss` every token
//     carries and the cell compares exactly (src/auth.rs, WrongIssuer);
//   - the cell's keys feed holds the signing key's public half under its
//     kid (cellSigningKey's rule): a token under any other key is refused
//     by the cell, so the issuer would mint only dead tokens.
// The signing key's type (Ed25519, or RSA of at least 2,048 bits) and a
// non-empty pepper list are refused earlier, by readConfig.
import { AdminError } from "../cell-admin/errors";
import { loadChecked } from "../cell-admin/publish";
import { S3Store } from "../cell-admin/s3store";
import type { CellState } from "../cell-admin/state";
import type { StateStore } from "../cell-admin/store";
import type { IssuerConfig } from "./config";

export interface Booted {
  store: StateStore;
  state: CellState;
  etag: string;
}

/// The refusals a loaded state can cause, at boot and whenever the state
/// changes: null when the issuer may mint from `state`.
export function stateMismatch(cfg: IssuerConfig, state: CellState): string | null {
  if (state.cell.issuer !== cfg.issuerId) {
    return `cell ${state.cell.cell_id} names issuer ${state.cell.issuer}, not STREAMS_ISSUER_ID ${cfg.issuerId}`;
  }
  const { kid, pem } = cfg.signingKey.record;
  if (!state.keys.some((k) => k.kid === kid && k.pem === pem)) {
    return `the signing key (kid ${kid}) is not a key of cell ${state.cell.cell_id}: publish its public half with cell-admin keys add`;
  }
  return null;
}

/// Open the cell's store and its state, or refuse.
export async function boot(cfg: IssuerConfig): Promise<Booted> {
  const store = await S3Store.open(`s3://${cfg.control.bucket}/cells/${cfg.cellId}`, {
    control: cfg.control,
    feeds: cfg.feeds,
  });
  const { state, etag } = await loadChecked(store);
  const refused = stateMismatch(cfg, state);
  if (refused !== null) throw new AdminError(refused);
  return { store, state, etag };
}
