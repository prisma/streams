// The shared-cell profile the issuer admits against (public-beta issuer
// triage B1.5, B3.2; skeptic S5a): deploy/profiles' two files, layered as
// `cell-admin init` layers them by default, compiled into the issuer's
// bundle as text. A create compares it with the digest `init` recorded
// (checkSizing): an issuer built from another profile than its cell's
// places nothing.
import { parseProfiles } from "../cell-admin/profile";
import compute1g from "../profiles/compute-1g.env" with { type: "text" };
import sharedCell from "../profiles/shared-cell.env" with { type: "text" };

export const BUNDLED_PROFILE: Map<string, string> = parseProfiles([
  { path: "deploy/profiles/compute-1g.env (compiled in)", text: compute1g },
  { path: "deploy/profiles/shared-cell.env (compiled in)", text: sharedCell },
]);
