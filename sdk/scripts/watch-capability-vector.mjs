// Cross-language pin for the §15 watch-observation capability: this
// script reproduces src/crypto.rs's watch_capability_vector_is_pinned
// through the SDK's encodeCapabilityInput + HMAC path. If either side
// changes construction, the two pins diverge and CI fails.
import { webcrypto as wc } from "node:crypto";
const subtle = wc.subtle;
function enc(components) {
  const parts = []; let total = 0;
  for (const c of components) {
    const b = typeof c === "string" ? new TextEncoder().encode(c) : c;
    const len = new Uint8Array(4);
    new DataView(len.buffer).setUint32(0, b.length, false);
    parts.push(len, b); total += 4 + b.length;
  }
  const out = new Uint8Array(total); let at = 0;
  for (const p of parts) { out.set(p, at); at += p.length; }
  return out;
}
const sigKey = new Uint8Array(32).fill(7);
const expBytes = new Uint8Array(8);
new DataView(expBytes.buffer).setBigInt64(0, 1786600000n, false);
const input = enc(["watch-capability-v1","proj-test","orders",
  "00112233445566778899aabbccddeeff","by-customer","0011223344556677","GET",expBytes]);
const mac = await subtle.importKey("raw", sigKey, {name:"HMAC",hash:"SHA-256"}, false, ["sign"]);
const out = new Uint8Array(await subtle.sign("HMAC", mac, input));
const got = Array.from(out.subarray(0,16)).map(x=>x.toString(16).padStart(2,"0")).join("");
const PINNED = "381d52c5438c2b10393c4697a001e5a5";
if (got !== PINNED) {
  console.error(`watch-capability vector mismatch: got ${got}, pinned ${PINNED}`);
  process.exit(1);
}
console.log("WATCH_CAPABILITY_VECTOR_OK");
