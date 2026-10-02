// Payload generators and the corpus-stats mode.
//
// corpus  : seeded synthetic product-analytics events with realistic
//           entropy (Zipf event names over 200, Zipf user ids over 1e6,
//           UUIDs, ISO timestamps on a synthetic clock, URL paths, 50
//           user agents, numeric metrics, line items, Zipf text). Each
//           record is sized to the target +-3% (jitter), exactly.
// b64rand : JSON with one base64 field of PRNG bytes (incompressible
//           beyond base64's 4/3).
// bytes   : a raw PRNG body, for {"format":{"kind":"bytes"}} streams.
//
// Everything is a pure function of (seed, tag, call order): the same
// flags produce byte-identical records. The vocabulary, user agents
// and event names are fixed tables independent of the seed.

import { Opts, Rng, Zipf, round3 } from "./common.ts";

export type PayloadKind = "corpus" | "b64rand" | "bytes";

export function payloadKind(s: string): PayloadKind {
  if (s === "corpus" || s === "b64rand" || s === "bytes") return s;
  throw new Error(`--payload must be corpus|b64rand|bytes, got ${s}`);
}

// ---------------------------------------------------------------- tables

const OBJECTS = [
  "product", "cart", "checkout", "page", "search", "account", "order", "payment", "subscription", "video",
  "article", "notification", "coupon", "wishlist", "review", "session", "signup", "invoice", "shipment", "feature",
];
const ACTIONS = ["viewed", "clicked", "added", "removed", "started", "completed", "failed", "updated", "shared", "opened"];
const EVENTS = OBJECTS.flatMap((o) => ACTIONS.map((a) => `${o}_${a}`)); // 200

const SYLLABLES = [
  "ka", "lo", "mi", "ra", "ten", "sor", "vel", "an", "bri", "cho", "du", "el", "fa", "gor", "hin", "is",
  "jo", "ku", "lan", "mor", "ne", "op", "pra", "qui", "ros", "sta", "tri", "um", "vor", "wen", "xa", "yo",
  "zel", "ber", "con", "dra", "est", "fin", "gal", "har", "in", "ler", "mon", "nor", "or", "pel", "ser", "tun",
];
const WORDS: string[] = (() => {
  const r = new Rng(0x5eed, 99);
  const seen = new Set<string>();
  const out: string[] = [];
  while (out.length < 4096) {
    const n = 1 + Math.min(3, Math.floor(r.exp(1.1)));
    let w = "";
    for (let i = 0; i < n; i++) w += r.pick(SYLLABLES);
    if (!seen.has(w)) {
      seen.add(w);
      out.push(w);
    }
  }
  return out;
})();

const USER_AGENTS: string[] = (() => {
  const t: Array<(v: number) => string> = [
    (v) => `Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/${v}.0.0.0 Safari/537.36`,
    (v) => `Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/${v}.0.0.0 Safari/537.36`,
    (v) => `Mozilla/5.0 (iPhone; CPU iPhone OS 17_${v % 7} like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.${v % 7} Mobile/15E148 Safari/604.1`,
    (v) => `Mozilla/5.0 (Linux; Android 14; Pixel 8) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/${v}.0.0.0 Mobile Safari/537.36`,
    (v) => `Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:${v}.0) Gecko/20100101 Firefox/${v}.0`,
    (v) => `Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/${17 + (v % 2)}.${v % 6} Safari/605.1.15`,
    (v) => `Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/${v}.0.0.0 Safari/537.36 Edg/${v}.0.0.0`,
    (v) => `Mozilla/5.0 (Linux; Android 13; SM-S918B) AppleWebKit/537.36 (KHTML, like Gecko) SamsungBrowser/${v - 100}.0 Chrome/${v}.0.0.0 Mobile Safari/537.36`,
    (v) => `Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/${v}.0.0.0 Safari/537.36`,
    (v) => `okhttp/4.${v % 13}.${v % 3}`,
    (v) => `python-requests/2.${20 + (v % 13)}.0`,
    (v) => `MyApp/${v % 9}.${v % 17}.${v % 5} (iPhone; iOS 17.${v % 6}; Scale/3.00)`,
  ];
  const out: string[] = [];
  for (let v = 118; out.length < 50; v++) for (const f of t) if (out.length < 50) out.push(f(v));
  return out;
})();

const COUNTRIES = [
  ["US", "en", "USD"], ["DE", "de", "EUR"], ["GB", "en", "GBP"], ["FR", "fr", "EUR"], ["IN", "hi", "INR"],
  ["BR", "pt", "BRL"], ["JP", "ja", "JPY"], ["CA", "en", "CAD"], ["AU", "en", "AUD"], ["NL", "nl", "EUR"],
  ["ES", "es", "EUR"], ["IT", "it", "EUR"], ["SE", "sv", "SEK"], ["PL", "pl", "PLN"], ["MX", "es", "MXN"],
  ["KR", "ko", "KRW"], ["SG", "en", "SGD"], ["DK", "da", "DKK"], ["NO", "nb", "NOK"], ["CH", "de", "CHF"],
  ["AT", "de", "EUR"], ["BE", "nl", "EUR"], ["IE", "en", "EUR"], ["FI", "fi", "EUR"], ["PT", "pt", "EUR"],
  ["NZ", "en", "NZD"], ["ZA", "en", "ZAR"], ["AR", "es", "ARS"], ["TR", "tr", "TRY"], ["ID", "id", "IDR"],
] as const;
const TIMEZONES = ["America/New_York", "Europe/Berlin", "Europe/London", "Europe/Paris", "Asia/Kolkata", "America/Sao_Paulo", "Asia/Tokyo", "America/Toronto"];
const REFERRERS = [
  "https://www.google.com/", "", "https://www.bing.com/", "https://duckduckgo.com/", "https://t.co/",
  "https://www.facebook.com/", "https://news.ycombinator.com/", "https://www.linkedin.com/feed/",
  "android-app://com.google.android.gm/", "https://www.reddit.com/r/", "https://github.com/",
];
const STATUSES = [200, 200, 200, 200, 200, 200, 200, 201, 204, 301, 302, 304, 400, 401, 403, 404, 404, 429, 500, 503];
const PLANS = ["free", "starter", "pro", "business", "enterprise"];
const CATEGORIES = ["shoes", "apparel", "electronics", "home", "garden", "toys", "books", "beauty", "sports", "grocery", "office", "pets"];

// ---------------------------------------------------------------- corpus

const HEX = Array.from({ length: 256 }, (_, i) => i.toString(16).padStart(2, "0"));

export class CorpusGen {
  private r: Rng;
  private clock = Date.UTC(2026, 8, 1);
  private seq = 0;
  private zEvent = new Zipf(EVENTS.length, 1.0);
  private zUser = new Zipf(1_000_000, 0.9);
  private zWord = new Zipf(WORDS.length, 1.07);
  private zUa = new Zipf(USER_AGENTS.length, 1.2);
  private zCountry = new Zipf(COUNTRIES.length, 1.1);
  private zProduct = new Zipf(50_000, 1.0);
  private zRef = new Zipf(REFERRERS.length, 1.0);
  constructor(seed: number, tag: number) {
    this.r = new Rng(seed, tag);
  }
  private uuid(): string {
    const r = this.r;
    let s = "";
    for (let i = 0; i < 16; i++) {
      let b = r.u32() & 0xff;
      if (i === 6) b = (b & 0x0f) | 0x40;
      if (i === 8) b = (b & 0x3f) | 0x80;
      s += HEX[b];
      if (i === 3 || i === 5 || i === 7 || i === 9) s += "-";
    }
    return s;
  }
  private word(): string {
    return WORDS[this.zWord.sample(this.r)]!;
  }
  private words(n: number, sep = " "): string {
    const w: string[] = [];
    for (let i = 0; i < n; i++) w.push(this.word());
    return w.join(sep);
  }
  private path(user: number): string {
    const r = this.r;
    const pid = this.zProduct.sample(r) + 1000;
    switch (r.int(14)) {
      case 0: return "/";
      case 1: case 2: return `/products/${pid}`;
      case 3: return `/products/${pid}/reviews`;
      case 4: return `/category/${r.pick(CATEGORIES)}/${this.word()}`;
      case 5: return `/category/${r.pick(CATEGORIES)}?page=${1 + r.int(20)}`;
      case 6: return `/search?q=${this.words(1 + r.int(3), "+")}`;
      case 7: return "/cart";
      case 8: return `/checkout/${r.pick(["shipping", "payment", "review", "confirm"])}`;
      case 9: return `/account/orders/${100000 + r.int(900000)}`;
      case 10: return `/api/v2/orders/${this.uuid()}`;
      case 11: return `/api/v2/users/u_${user}/events`;
      case 12: return `/blog/${this.words(3, "-")}`;
      default: return `/docs/${this.word()}/${this.word()}`;
    }
  }
  private num(x: number, places: number): string {
    const f = 10 ** places;
    return JSON.stringify(Math.round(x * f) / f);
  }
  private item(): string {
    const r = this.r;
    return `{"sku":"sku-${this.zProduct.sample(r) + 1000}","name":${JSON.stringify(this.words(2 + r.int(3)))},` +
      `"category":"${r.pick(CATEGORIES)}","qty":${1 + Math.floor(r.exp(0.6))},"price":${this.num(r.lognormal(24, 0.9), 2)},` +
      `"discount":${this.num(r.next() < 0.7 ? 0 : r.next() * 0.4, 2)}}`;
  }
  /** One event of exactly `target` (+-3% jitter) bytes of JSON text. */
  next(target: number): string {
    const r = this.r;
    const T = Math.max(96, Math.round(target * (0.97 + 0.06 * r.next())));
    this.clock += Math.max(1, Math.round(r.exp(50)));
    this.seq++;
    const user = (Math.imul(this.zUser.sample(r) + 1, 0x9e3779b1) >>> 0) % 1_000_000;
    const country = COUNTRIES[this.zCountry.sample(r)]!;
    const RESERVE = 10; // ,"note":""
    const parts: string[] = [];
    let len = 2;
    const add = (k: string, v: string, force = false): boolean => {
      const piece = `"${k}":${v}`;
      const extra = piece.length + (parts.length ? 1 : 0);
      if (!force && len + extra + RESERVE > T) return false;
      parts.push(piece);
      len += extra;
      return true;
    };
    add("event", `"${EVENTS[this.zEvent.sample(r)]}"`, true);
    add("ts", `"${new Date(this.clock).toISOString()}"`, true);
    add("userId", `"u_${user}"`, true);
    add("id", `"${this.uuid()}"`);
    add("path", JSON.stringify(this.path(user)));
    add("status", String(r.pick(STATUSES)));
    add("durationMs", String(Math.round(r.lognormal(180, 1.1))));
    add("sessionId", `"${this.uuid()}"`);
    add("country", `"${country[0]}"`);
    add("ua", JSON.stringify(USER_AGENTS[this.zUa.sample(r)]));
    add("ip", `"${(user * 7919) % 223 + 1}.${(user >>> 3) & 255}.${(user >>> 11) & 255}.${1 + r.int(254)}"`);
    add("locale", `"${country[1]}-${country[0]}"`);
    add("referrer", JSON.stringify(this.zRef.sample(r) === 9 ? `https://www.reddit.com/r/${this.word()}/` : REFERRERS[this.zRef.sample(r)]));
    add("properties",
      `{"productId":"sku-${this.zProduct.sample(r) + 1000}","price":${this.num(r.lognormal(39, 0.8), 2)},` +
      `"currency":"${country[2]}","quantity":${1 + Math.floor(r.exp(0.7))},"score":${this.num(r.next(), 4)},` +
      `"bytes":${Math.round(r.lognormal(24000, 1.3))},"plan":"${r.pick(PLANS)}","variant":"${r.pick(["control", "a", "b"])}"}`);
    add("context",
      `{"app":{"name":"${this.word()}","version":"${r.int(6)}.${r.int(30)}.${r.int(10)}","build":${1000 + r.int(9000)}},` +
      `"screen":{"width":${r.pick([390, 414, 768, 1280, 1440, 1920, 2560])},"height":${r.pick([844, 896, 1024, 800, 900, 1080, 1440])}},` +
      `"timezone":"${r.pick(TIMEZONES)}","library":{"name":"analytics-js","version":"4.${r.int(20)}.${r.int(10)}"}}`);
    // Line items while they fit (order-like events grow by items).
    const itemsHead = parts.length ? 10 : 9; // ,"items":[ + ]
    if (len + itemsHead + 1 + 120 + RESERVE <= T) {
      const items: string[] = [];
      let il = itemsHead + 1;
      for (;;) {
        const it = this.item();
        const extra = it.length + (items.length ? 1 : 0);
        if (len + il + extra + RESERVE > T) break;
        items.push(it);
        il += extra;
        if (len + il + 140 + RESERVE > T) break;
      }
      if (items.length) add("items", `[${items.join(",")}]`, true);
    }
    // Text filler to the exact size (every add above left room for it).
    const room = Math.max(0, T - len - (parts.length ? 1 : 0) - 9); // "note":""
    let note = "";
    while (note.length < room) note += (note ? " " : "") + this.word();
    parts.push(`"note":"${note.slice(0, room).trimEnd().padEnd(room, ".")}"`);
    return `{${parts.join(",")}}`;
  }
  get count(): number {
    return this.seq;
  }
}

// ---------------------------------------------------------------- payload gen

export class PayloadGen {
  private corpus: CorpusGen | null = null;
  private r: Rng;
  private seq = 0;
  private clock = Date.UTC(2026, 8, 1);
  constructor(readonly kind: PayloadKind, readonly recordBytes: number, seed: number, tag: number) {
    this.r = new Rng(seed, tag + 1000);
    if (kind === "corpus") this.corpus = new CorpusGen(seed, tag);
  }
  get isJson(): boolean {
    return this.kind !== "bytes";
  }
  /** One JSON record's text (corpus or b64rand). */
  json(): string {
    if (this.corpus) return this.corpus.next(this.recordBytes);
    this.seq++;
    this.clock += 1 + Math.round(this.r.exp(50));
    const head = `{"id":${this.seq},"ts":"${new Date(this.clock).toISOString()}","data":"`;
    // 4 base64 chars per 3 random bytes, no padding: at most 3 B short.
    const chars = Math.max(4, this.recordBytes - head.length - 2);
    const raw = this.r.fill(new Uint8Array(Math.floor(chars / 4) * 3));
    return `${head}${Buffer.from(raw).toString("base64")}"}`;
  }
  /** One raw record (bytes). */
  raw(): Uint8Array {
    return this.r.fill(new Uint8Array(this.recordBytes));
  }
}

// ---------------------------------------------------------------- corpus-stats

const FRAME_HEADER = 55;
const COMPRESS_MIN = 256; // src/crypto.rs COMPRESS_MIN_BYTES

/** Per-record zstd level 1 ratio at each size, with a 55 B frame header
 * in the compressed size: c_zstd = raw / (zstd + 55). c_frame applies
 * the server's rule as well (compress only from 256 B, keep zstd only
 * when it shrinks): raw / (min(raw, zstd) + 55). */
export function corpusStats(o: Opts): Record<string, unknown>[] {
  const seed = o.int("seed", 1);
  const samples = o.int("samples", 2000);
  const sizes = (o.str("sizes", "128,256,1024,2048,16384")).split(",").map(Number);
  const kinds = (o.str("payloads", "corpus,b64rand,bytes")).split(",").map(payloadKind);
  const out: Record<string, unknown>[] = [];
  for (const kind of kinds) {
    for (const size of sizes) {
      const g = new PayloadGen(kind, size, seed, 7);
      const n = size >= 16384 ? Math.max(100, Math.floor(samples / 4)) : samples;
      let raw = 0, z = 0, stored = 0, minR = Infinity, maxR = 0;
      const t0 = performance.now();
      for (let i = 0; i < n; i++) {
        const rec = kind === "bytes" ? g.raw() : Buffer.from(g.json());
        const zl = Bun.zstdCompressSync(rec, { level: 1 }).byteLength;
        raw += rec.byteLength;
        z += zl + FRAME_HEADER;
        stored += (rec.byteLength >= COMPRESS_MIN && zl < rec.byteLength ? zl : rec.byteLength) + FRAME_HEADER;
        const ratio = rec.byteLength / size;
        if (ratio < minR) minR = ratio;
        if (ratio > maxR) maxR = ratio;
      }
      out.push({
        payload: kind,
        target_bytes: size,
        records: n,
        mean_bytes: round3(raw / n),
        min_size_ratio: round3(minR),
        max_size_ratio: round3(maxR),
        c_zstd: round3(raw / z),
        c_frame: round3(raw / stored),
        mean_zstd_frame_bytes: round3(z / n),
        mean_stored_frame_bytes: round3(stored / n),
        gen_us_per_record: round3(((performance.now() - t0) * 1000) / n),
      });
    }
  }
  return out;
}

/** A few sample records, for eyeballing realism. */
export function corpusSample(seed: number, size: number, n: number): string[] {
  const g = new CorpusGen(seed, 7);
  return Array.from({ length: n }, () => g.next(size));
}
