/**
 * WebCrypto across the runtimes the SDK supports, shared by the watch-URL
 * signing in index.ts and the wake-up signature check in queue.ts.
 */

let cryptoCache: Crypto | undefined;
export async function webCrypto(): Promise<Crypto> {
  if (cryptoCache) return cryptoCache;
  const c = (globalThis as { crypto?: Crypto }).crypto;
  if (c?.subtle) return (cryptoCache = c);
  // Node 18 — the engines floor — ships WebCrypto only at
  // node:crypto.webcrypto; the GLOBAL landed in Node 19. This branch
  // is dead code wherever globalThis.crypto exists (browsers, Bun,
  // Deno, Node 19+), so bundlers never execute the import.
  try {
    // Variable specifier: the package ships without Node types, and a
    // literal import would make TS (and bundlers) try to resolve a
    // module that only exists at runtime on Node.
    const spec = "node:crypto";
    const nc = (await import(/* @vite-ignore */ spec)) as {
      webcrypto?: Crypto;
    };
    if (nc.webcrypto?.subtle) return (cryptoCache = nc.webcrypto);
  } catch {
    // no node:crypto either — fall through to the error
  }
  throw new Error(
    "WebCrypto is required to derive watch URLs, generate keys and verify queue wake-ups " +
      "(Node 18+, or a browser over HTTPS)",
  );
}

export async function subtle(): Promise<SubtleCrypto> {
  return (await webCrypto()).subtle;
}
