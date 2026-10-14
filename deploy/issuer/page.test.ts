// The issuer's one page and what it reads of the session (public-beta
// issuer triage B4.3, B4.4, B4.6). Run with `bun test ./deploy/issuer`.
import { afterEach, describe, expect, test } from "bun:test";
import { createHash } from "node:crypto";
import { FakeCell } from "../cell-admin/fake-cell";
import { cleanup, readBundle, run } from "../cell-admin/test-rig";
import { type FakeElement, FakePage } from "./page-dom";
import { admin, envWith, fakeS3, ISSUER_ORIGIN, issuerCell, managed } from "./rig";
import { csrfToken, readSession } from "./session";

afterEach(cleanup);

const hash = (text: string) => `'sha256-${createHash("sha256").update(text).digest("base64")}'`;

/// The page's inline elements `tag`: [attributes, text] each.
const inline = (html: string, tag: string) =>
  [...html.matchAll(new RegExp(`<${tag}\\b([^>]*)>([\\s\\S]*?)</${tag}>`, "g"))].map((m) => [m[1], m[2]]);

async function page() {
  const m = await managed(await issuerCell());
  const res = await m.get("/");
  return { m, res, html: await res.text() };
}

describe("the issuer's page", () => {
  test("is one static page whose only script and style the CSP names by hash, framed by no one, loading nothing from elsewhere", async () => {
    const { res, html } = await page();
    expect(res.status).toBe(200);
    expect(Object.fromEntries(["content-type", "cache-control", "x-content-type-options", "x-frame-options", "referrer-policy",
      "cross-origin-opener-policy"].map((h) => [h, res.headers.get(h)]))).toEqual({
      "content-type": "text/html; charset=utf-8",
      "cache-control": "no-store",
      "x-content-type-options": "nosniff",
      "x-frame-options": "DENY",
      "referrer-policy": "no-referrer",
      "cross-origin-opener-policy": "same-origin",
    });
    const scripts = inline(html, "script");
    const styles = inline(html, "style");
    expect([scripts.length, styles.length, scripts[0][0], styles[0][0]]).toEqual([1, 1, "", ""]);
    expect(res.headers.get("content-security-policy")).toBe(
      `default-src 'none'; script-src ${hash(scripts[0][1])}; style-src ${hash(styles[0][1])}; connect-src 'self'; ` +
        "base-uri 'none'; form-action 'none'; frame-ancestors 'none'; require-trusted-types-for 'script'",
    );
    // Nothing from another origin, no element that loads or posts, no
    // inline handler, and no HTML written from strings.
    expect(html).not.toMatch(/https?:\/\//);
    expect(html).not.toMatch(/<(link|iframe|object|embed|img|form|base|meta http-equiv)\b|\s(src|srcset|action|on[a-z]+)=/i);
    expect(scripts[0][1]).not.toMatch(/innerHTML|outerHTML|insertAdjacentHTML|document\.write|\beval\(|new Function|setTimeout\(\s*["'`]/);
    // Its calls are the issuer's own routes.
    for (const route of ["/auth/session", "/auth/login", "/v1/workspaces/", "/streams/projects", "/v1/projects/", "/streams/credentials"]) {
      expect(scripts[0][1]).toContain(`"${route}`);
    }
  });

  test("words every sign-in refusal, the beta at capacity and a workspace off the allow-list, and says who may manage", async () => {
    const { html } = await page();
    const [[, script]] = inline(html, "script");
    for (const code of ["denied", "expired", "failed", "not_oauth", "no_user", "no_workspace", "invalid_workspace",
      "workspace_not_allowed", "unavailable"]) {
      expect(script).toMatch(new RegExp(`\\n\\s+${code}: "[^"]+",\\n`));
    }
    expect(script).toContain("is not on the Prisma Streams beta's allow-list");
    // Off the list (the owner's call 16), a workspace that still has
    // projects keeps a reduced mode: it sees them, revokes and deletes,
    // and is offered no create and no rotation.
    expect(script).toContain("Its projects keep running and their credentials keep working.");
    expect(script).toContain("Here you can see them, revoke a credential and delete a project.");
    expect(script).toContain("main.replaceChildren(reducedCard(), ...projects.map(projectCard))");
    expect(script).toContain('...(p.status === "active" && session.allowListed ? [newCredentialForm(p, list)] : []));');
    expect(script).toContain("if (session.allowListed) actions.append(rotateButton(p, c, list), \" \");");
    expect(script).toContain("session.allowListed = false;");
    expect(script).not.toContain("to revoke a credential or delete a project, ask");
    expect(html).toContain("A workspace taken off the list keeps its projects running");
    expect(script).toContain("The Prisma Streams beta is at capacity");
    expect(html).toContain("There is no role check during the beta");
    // The SDK snippet: the credential in the environment, this issuer's origin.
    expect(script).toContain("StreamsClient.connect({");
    expect(script).toContain("issuerUrl: '\" + location.origin + \"',");
    // A project is deleted only once its name is typed.
    expect(script).toContain("button.disabled = input.value !== p.displayName;");
  });

  test("answers another method 405", async () => {
    const { m } = await page();
    const res = await m.call("POST", "/");
    expect([res.status, res.headers.get("allow")]).toEqual([405, "GET"]);
  });
});

describe("GET /auth/session", () => {
  test("tells the page who is signed in, whether its workspace is on the allow-list, and the CSRF value its changes send", async () => {
    const cell = await issuerCell();
    const m = await managed(cell);
    const cookie = m.signIn("ws_a");
    const res = await m.get("/auth/session", cookie);
    const sid = readSession(m.cfg.sessionKey, new Request("https://issuer.example.com/", { headers: { cookie } }), m.clock.t)!.sid;
    expect([res.status, res.headers.get("cache-control"), res.headers.get("x-content-type-options"), await res.json()]).toEqual([
      200, "no-store", "nosniff",
      {
        userId: "usr_ws_a", email: "owner@ws_a.example.com", workspaceId: "ws_a", allowListed: false,
        csrfToken: csrfToken(m.cfg.sessionKey, sid),
      },
    ]);
    expect(res.headers.get("access-control-allow-origin")).toBeNull();
    // The list is read at most every 5 s; the value it gives is the one a
    // change needs.
    expect((await run("allow", "--state", cell.state, "--workspace", "ws_a")).code).toBe(0);
    m.clock.t += 5_001;
    const { allowListed, csrfToken: token } = await (await m.get("/auth/session", cookie)).json();
    expect(allowListed).toBe(true);
    const created = await m.call("POST", "/v1/workspaces/ws_a/streams/projects", cookie, {
      body: JSON.stringify({ displayName: "app" }),
      headers: { "content-type": "application/json", "x-csrf-token": token },
    });
    expect(created.status).toBe(201);
    // It cannot say while the list cannot be read.
    m.clock.t += 5_001;
    fakeS3().failReads = true;
    const down = await m.get("/auth/session", cookie);
    expect([down.status, down.headers.get("retry-after"), await down.json()]).toEqual([
      503, "5", { error: { code: "issuer_unavailable", message: "the issuer cannot read the beta's allow-list; retry shortly" } },
    ]);
  });

  test("answers 401 without a session, and takes from the session's budget", async () => {
    const m = await managed(await issuerCell());
    const res = await m.get("/auth/session");
    expect([res.status, await res.json()]).toEqual([
      401, { error: { code: "unauthorized", message: "sign in with Prisma to manage Streams projects" } },
    ]);
    const cookie = m.signIn("ws_a");
    for (let i = 0; i < 60; i++) expect((await m.get("/auth/session", cookie)).status).toBe(200);
    expect((await m.get("/auth/session", cookie)).status).toBe(429);
    expect((await m.call("POST", "/auth/session", cookie)).status).toBe(405);
  });
});

describe("the page's watches", () => {
  const PROJECTS = "/v1/workspaces/ws_a/streams/projects";
  const fakes: FakeCell[] = [];
  afterEach(() => {
    for (const f of fakes.splice(0)) f.stop();
  });

  /// Workspace ws_a on the allow-list with one project the issuer just
  /// made, and a fake cell, on the test's clock, serving the bundle as it
  /// was before the project.
  async function placed() {
    const cell = await issuerCell();
    await admin(cell, "allow", "--workspace", "ws_a");
    const fake = new FakeCell();
    fakes.push(fake);
    fake.publish(readBundle(cell.state));
    const m = await managed(cell, envWith(cell, { STREAMS_CELL_ORIGIN: fake.url }));
    fake.now = () => m.clock.t;
    const cookie = m.signIn("ws_a");
    const made = await (await m.send("POST", PROJECTS, cookie, { displayName: "Orders" })).json();
    const id: string = made.project.id;
    return { cell, fake, m, cookie, id, cid: made.credential.id as string, project: `${PROJECTS}/${id}` };
  }

  /// The project card's line saying whether its cell serves it.
  const readyLine = (page: FakePage) =>
    page.find((e) => e.tagName === "p" && e.className === "muted" && e.parent?.className === "card")!;

  /// The status cell of the credential named `name`.
  const statusCell = (page: FakePage, name: string) =>
    page.find((e) => e.tagName === "tr" && (e.children[0] as FakeElement | undefined)?.textContent === name)!
      .children[3] as FakeElement;

  /// An answer of `status` with `retryAfter`, in the management API's form.
  const waitAnswer = (status: number, retryAfter: string) => () =>
    new Response(JSON.stringify({ error: { code: "issuer_unavailable", message: "retry shortly" } }), {
      status, headers: { "content-type": "application/json", "retry-after": retryAfter },
    });

  test("asks whether a new project's cell serves it until it does, through a 429 it waits out, then asks no more", async () => {
    const { cell, fake, m, cookie, project } = await placed();
    const page = await new FakePage(m, cookie).open();
    const line = readyLine(page);
    expect([line.textContent, page.statuses(project)]).toEqual(["Being placed on its cell; usually under a minute…", [200]]);
    // The session's budget spent just before the next ask: 429, Retry-After 1.
    await page.advance(4_999);
    const sid = readSession(m.cfg.sessionKey, new Request(`${ISSUER_ORIGIN}/`, { headers: { cookie } }), m.clock.t)!.sid;
    for (let i = 0; i < 60; i++) m.issuer.limits.charge("management_session", sid);
    await page.advance(1);
    expect(page.statuses(project)).toEqual([200, 429]);
    fake.publish(readBundle(cell.state));
    await page.advance(5_000);
    expect([line.textContent, page.statuses(project)]).toEqual(["Serving at its cell.", [200, 429, 200]]);
    await page.advance(120_000);
    expect(page.statuses(project)).toEqual([200, 429, 200]);
  });

  test("waits out a Retry-After longer than its own wait before it asks again", async () => {
    const { m, cookie, project } = await placed();
    const page = await new FakePage(m, cookie).open();
    page.faults.push({ path: new RegExp(`^${project}$`), answer: waitAnswer(503, "40") });
    await page.advance(5_000);
    await page.advance(39_999);
    expect(page.askedAt(project)).toEqual([0, 5_000]);
    await page.advance(1);
    expect(page.askedAt(project)).toEqual([0, 5_000, 45_000]);
    expect(page.statuses(project)).toEqual([200, 503, 200]);
  });

  test("asks every 5 s for a minute, then every 30 s, and gives up after ten minutes beside a Retry", async () => {
    const { m, cookie, project } = await placed();
    const page = await new FakePage(m, cookie).open();
    await page.advance(900_000);
    const fast = Array.from({ length: 12 }, (_, i) => i * 5_000);
    const slow = Array.from({ length: 19 }, (_, i) => 60_000 + i * 30_000);
    expect(page.askedAt(project)).toEqual([...fast, ...slow]);
    expect(readyLine(page).textContent).toBe("Not confirmed after 10 minutes. Retry");
  });

  test("stops on any other failure, says so beside a Retry, and asks again on Retry", async () => {
    const { cell, fake, m, cookie, project } = await placed();
    const page = await new FakePage(m, cookie).open();
    page.faults.push({ path: new RegExp(`^${project}$`), answer: () => { throw new TypeError("fetch failed"); } });
    await page.advance(5_000);
    const line = readyLine(page);
    expect(line.textContent).toBe("Not checked: Prisma Streams could not be reached. Check your connection and retry. Retry");
    await page.advance(120_000);
    expect(page.statuses(project)).toEqual([200, 0]);
    fake.publish(readBundle(cell.state));
    page.button("Retry", line)!.dispatch("click");
    await page.advance(0);
    expect([line.textContent, page.statuses(project)]).toEqual(["Serving at its cell.", [200, 0, 200]]);
  });

  test("stops once the issuer says why a project cannot be ready, and says it", async () => {
    const { m, cookie, id, cid, project } = await placed();
    expect((await m.send("POST", `/v1/projects/${id}/streams/credentials/${cid}/revoke`, cookie)).status).toBe(200);
    const page = await new FakePage(m, cookie).open();
    expect(readyLine(page).textContent).toBe("No live credential, so its cell cannot be asked whether it serves this project.");
    await page.advance(120_000);
    expect(page.statuses(project)).toEqual([200]);
  });

  test("watches a credential the page made until the cell serves it, and stops once its project is suspended, saying so", async () => {
    const { cell, fake, m, cookie, id } = await placed();
    fake.publish(readBundle(cell.state));
    const page = await new FakePage(m, cookie).open();
    const create = async (name: string) => {
      const form = page.find((e) => e.tagName === "form" && page.button("Create credential", e) !== undefined)!;
      (form.children[0] as FakeElement).value = name;
      form.dispatch("submit");
      await page.advance(0);
      return (await (await m.get(`/v1/projects/${id}/streams/credentials`, cookie)).json()).credentials
        .find((c: { displayName: string }) => c.displayName === name).id as string;
    };
    const svc = await create("svc");
    expect(statusCell(page, "svc").textContent).toBe("active (reaching the cell…)");
    fake.publish(readBundle(cell.state));
    await page.advance(5_000);
    expect(statusCell(page, "svc").textContent).toBe("active");
    const other = await create("other");
    await admin(cell, "suspend", "--project", id);
    await m.cache.refresh();
    await page.advance(5_000);
    expect(statusCell(page, "other").textContent).toBe("active (project suspended)");
    await page.advance(120_000);
    const path = (c: string) => `/v1/projects/${id}/streams/credentials/${c}`;
    expect([page.statuses(path(svc)), page.statuses(path(other))]).toEqual([[200, 200], [200, 200]]);
  });
});
