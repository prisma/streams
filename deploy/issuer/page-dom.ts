// A DOM just large enough to run the issuer's page script (page.html) in
// a test, for page.test.ts: elements with attributes, text, children,
// listeners and `isConnected`; the page's four mount points; timers on a
// clock the test moves, which moves the issuer's clock with it; and
// `fetch` answered by the in-process issuer as a signed-in browser, its
// state copy revalidated whenever stale (the issuer's own loop does that
// every 5 s). A test can put an answer in front of the issuer for the
// next request to a path, a network failure included.
import { ISSUER_ORIGIN, type managed } from "./rig";

type Managed = Awaited<ReturnType<typeof managed>>;

class TextNode {
  constructor(readonly text: string) {}
}

export class FakeElement {
  parent: FakeElement | null = null;
  children: (FakeElement | TextNode)[] = [];
  className = "";
  disabled = false;
  value = "";
  readonly attributes = new Map<string, string>();
  private readonly listeners = new Map<string, ((e: unknown) => unknown)[]>();

  constructor(
    readonly tagName: string,
    private readonly mounted = false,
  ) {}

  get isConnected(): boolean {
    return this.mounted || (this.parent?.isConnected ?? false);
  }

  get textContent(): string {
    return this.children.map((c) => (c instanceof FakeElement ? c.textContent : c.text)).join("");
  }

  set textContent(text: string) {
    this.replaceChildren(text);
  }

  setAttribute(name: string, value: string): void {
    this.attributes.set(name, value);
  }

  addEventListener(type: string, listener: (e: unknown) => unknown): void {
    this.listeners.set(type, [...(this.listeners.get(type) ?? []), listener]);
  }

  append(...nodes: (FakeElement | string)[]): void {
    for (const n of nodes) {
      if (typeof n === "string") {
        this.children.push(new TextNode(n));
        continue;
      }
      if (n.parent) n.parent.children = n.parent.children.filter((c) => c !== n);
      n.parent = this;
      this.children.push(n);
    }
  }

  replaceChildren(...nodes: (FakeElement | string)[]): void {
    for (const c of this.children) if (c instanceof FakeElement) c.parent = null;
    this.children = [];
    this.append(...nodes);
  }

  scrollIntoView(): void {}

  /// Run the listeners of `type`, as a browser does, without waiting for
  /// what they start.
  dispatch(type: string): void {
    for (const listener of this.listeners.get(type) ?? []) void listener({ preventDefault() {} });
  }

  /// This element and every element under it, depth first.
  *walk(): Generator<FakeElement> {
    yield this;
    for (const c of this.children) if (c instanceof FakeElement) yield* c.walk();
  }
}

/// The page's script running against `m`'s issuer as the browser holding
/// `cookie`.
export class FakePage {
  /// Milliseconds since the page opened, on the clock its timers read.
  now = 0;
  /// Every request the page made, in order, with the status it got (0
  /// for a network failure) and when it was sent.
  readonly requests: { method: string; path: string; status: number; at: number }[] = [];
  /// Answers put in front of the issuer: the next request whose path one
  /// matches gets it (a throw is a network failure).
  readonly faults: { path: RegExp; answer: () => Response }[] = [];
  readonly mounts = {
    who: new FakeElement("div", true),
    banner: new FakeElement("div", true),
    secret: new FakeElement("div", true),
    main: new FakeElement("main", true),
  };
  private timers: { at: number; order: number; run: () => void }[] = [];
  private order = 0;
  private inflight = 0;

  constructor(
    private readonly m: Managed,
    private readonly cookie: string,
  ) {}

  private readonly document = {
    getElementById: (id: keyof FakePage["mounts"]) => this.mounts[id],
    createElement: (tag: string) => new FakeElement(tag),
  };

  private readonly fetch = async (path: string, init: { method?: string; headers?: Record<string, string>; body?: string }) => {
    const method = init.method ?? "GET";
    const at = this.now;
    this.inflight++;
    try {
      const fault = this.faults.findIndex((f) => f.path.test(path));
      if (fault >= 0) {
        const [f] = this.faults.splice(fault, 1);
        let res: Response;
        try {
          res = f.answer();
        } catch (e) {
          this.requests.push({ method, path, status: 0, at });
          throw e;
        }
        this.requests.push({ method, path, status: res.status, at });
        return res;
      }
      if (!this.m.cache.fresh()) await this.m.cache.refresh();
      const res = await this.m.issuer.fetch(new Request(`${ISSUER_ORIGIN}${path}`, {
        method,
        headers: { ...init.headers, cookie: this.cookie },
        body: init.body,
      }));
      this.requests.push({ method, path, status: res.status, at });
      return res;
    } finally {
      this.inflight--;
    }
  };

  private readonly setTimeout = (run: () => void, ms = 0) => {
    this.timers.push({ at: this.now + ms, order: this.order++, run });
    return 0;
  };

  /// Load the issuer's page and run its script, up to its first wait.
  async open(): Promise<this> {
    const html = await (await this.m.issuer.fetch(new Request(`${ISSUER_ORIGIN}/`))).text();
    const script = /<script>([\s\S]*?)<\/script>/.exec(html)![1];
    const globals = {
      document: this.document,
      location: { search: "", origin: ISSUER_ORIGIN },
      history: { replaceState() {} },
      fetch: this.fetch,
      setTimeout: this.setTimeout,
      navigator: { clipboard: { writeText: async () => {} } },
      confirm: () => true,
    };
    new Function(...Object.keys(globals), script)(...Object.values(globals));
    await this.advance(0);
    return this;
  }

  /// Wait until no request is in flight and the page has run what it can
  /// without a timer.
  async settle(): Promise<void> {
    for (let quiet = 0; quiet < 5; ) {
      await new Promise((done) => setImmediate(done));
      quiet = this.inflight === 0 ? quiet + 1 : 0;
    }
  }

  /// Move the clock `ms` on, firing each timer due on the way, in order.
  async advance(ms: number): Promise<void> {
    const until = this.now + ms;
    for (;;) {
      await this.settle();
      const due = this.timers.filter((t) => t.at <= until).sort((a, b) => a.at - b.at || a.order - b.order)[0];
      if (due === undefined) break;
      this.timers = this.timers.filter((t) => t !== due);
      this.moveTo(due.at);
      due.run();
    }
    this.moveTo(until);
    await this.settle();
  }

  private moveTo(at: number): void {
    this.m.clock.t += at - this.now;
    this.now = at;
  }

  /// The first element under the page's main mount that `pick` accepts.
  find(pick: (e: FakeElement) => boolean): FakeElement | undefined {
    for (const e of this.mounts.main.walk()) if (pick(e)) return e;
    return undefined;
  }

  /// The button labelled `label` under `within` (default: the main mount).
  button(label: string, within: FakeElement = this.mounts.main): FakeElement | undefined {
    for (const e of within.walk()) if (e.tagName === "button" && e.textContent === label) return e;
    return undefined;
  }

  /// The statuses of the requests made to `path` exactly, in order.
  statuses(path: string): number[] {
    return this.requests.filter((r) => r.path === path).map((r) => r.status);
  }

  /// When the page asked for `path` exactly, in milliseconds since it
  /// opened.
  askedAt(path: string): number[] {
    return this.requests.filter((r) => r.path === path).map((r) => r.at);
  }
}
