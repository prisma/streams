// RUNBOOK.md against the watch (public-beta alerting triage B5): the shared
// cell's "What to alert on" table holds the alerting table's rows 1 to 30,
// each with the rule RULES runs for it, its class and where it is paged;
// §7.5 says who answers, beside the Compute escalation contact; §8 states
// the operator surface's bearer gate; the canary's paragraph makes the
// watch's credential with the scopes it needs; the drills' paragraph runs
// drills.e2e.test.ts (B8). Run with `bun test ./deploy/cell-watch`.
import { describe, expect, test } from "bun:test";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { CANARY_SCOPES } from "./canary";
import { HEARTBEAT_WINDOW_MS, UPTIME_EVERY_MS, UPTIME_FAILURES, UPTIME_TIMEOUT_MS } from "./pager";
import { REASONS, RULES } from "./rules";

const LINES = readFileSync(join(import.meta.dir, "..", "..", "RUNBOOK.md"), "utf8").split("\n");

/// The cells of the first table after the line that starts with `marker`,
/// its header first and its `|---|` line left out; empty without one.
function tableAfter(marker: string): string[][] {
  const at = LINES.findIndex((l) => l.startsWith(marker));
  const first = at < 0 ? -1 : LINES.findIndex((l, i) => i > at && l.startsWith("|"));
  const rows: string[][] = [];
  for (let i = first; i >= 0 && i < LINES.length && (LINES[i] ?? "").startsWith("|"); i++) {
    rows.push((LINES[i] ?? "").split(/(?<!\\)\|/).slice(1, -1).map((c) => c.trim()));
  }
  return rows.filter((_, i) => i !== 1);
}

/// The text from the heading that starts with `heading` to the next heading
/// of its level or above, its whitespace runs made single spaces.
function section(heading: string): string {
  const from = LINES.findIndex((l) => l.startsWith(heading));
  if (from < 0) return "";
  const level = heading.indexOf(" ");
  const next = LINES.findIndex((l, i) => i > from && /^#+ /.test(l) && l.indexOf(" ") <= level);
  return LINES.slice(from, next < 0 ? undefined : next).join("\n").replace(/\s+/g, " ");
}

/// The class of each row of the alerting table (triage B2), in the order the row states them.
const CLASS: Readonly<Record<number, string>> = {
  1: "PAGE", 2: "PAGE", 3: "PAGE", 4: "NOTIFY / PAGE", 5: "NOTIFY", 6: "PAGE", 7: "PAGE",
  8: "PAGE / NOTIFY", 9: "PAGE / NOTIFY", 10: "PAGE / NOTIFY", 11: "PAGE", 12: "PAGE / NOTIFY",
  14: "PAGE", 15: "NOTIFY / PAGE", 16: "PAGE / NOTIFY", 17: "NOTIFY", 18: "NOTIFY",
  19: "PAGE", 20: "NOTIFY", 21: "PAGE", 22: "PAGE", 23: "NOTIFY", 24: "NOTIFY", 25: "NOTIFY",
  26: "NOTIFY", 27: "NOTIFY", 28: "NOTIFY", 29: "NOTIFY", 30: "PAGE", 31: "PAGE", 32: "NOTIFY",
  33: "PAGE / NOTIFY",
};
/// Where each class goes: §7.5's "Who answers" names them.
const TO: Readonly<Record<string, string>> = { PAGE: "`<on-call>`", NOTIFY: "`<notify channel>`" };
const ROWS = Object.keys(CLASS).map(Number);

describe("RUNBOOK.md's What to alert on", () => {
  const [header, ...rows] = tableAfter("**What to alert on");
  const cells = (row: number) => rows.find((c) => c[0] === String(row)) ?? [];

  test("has the columns #, rule, class, where, alert when and paged by", () => {
    expect(header).toEqual(["#", "rule", "class", "where", "alert when", "paged by"]);
  });

  test("holds rows 1 to 33 once each but row 13 (retired with the history tier), in order, each saying where and when", () => {
    expect(rows.map((c) => c[0])).toEqual(ROWS.map(String));
    for (const c of rows) expect([c[0], c.length, (c[3] ?? "") !== "", (c[4] ?? "") !== ""]).toEqual([c[0], 6, true, true]);
  });

  test("names the rule RULES runs for rows 3 to 33, the level rules marked, and the paging tool's checks for 1 and 2", () => {
    expect([cells(1)[1], cells(2)[1]]).toEqual(["uptime check", "heartbeat"]);
    for (const r of RULES) expect([r.row, cells(r.row)[1]]).toEqual([r.row, `\`${r.id}\`${r.level ? " (level)" : ""}`]);
  });

  test("row 4 leaves out a boot id its successor reports as replaced, by the operator data's boot section", () => {
    const when = cells(4)[4] ?? "";
    for (const want of [
      "3 boot ids or more in 30 min",
      "`boot.predecessor`",
      "`boot.predecessorSuperseded`",
      "`_cell/superseded`",
      "a deploy and a restart within 30 min notify twice and do not page",
      "the predecessor went before its successor registered (a crash, an OOM kill, a self-exit), and it counts",
    ]) {
      expect([want, when.includes(want)]).toEqual([want, true]);
    }
    expect((cells(4)[3] ?? "").includes("`/operator/data.json` `boot`")).toBe(true);
  });

  test("says NOTIFY_ONLY=1 holds back the level rules' PAGE only, as rules.ts does (B6)", () => {
    const text = paragraph("**What to alert on.**");
    expect(text).toContain(
      "`NOTIFY_ONLY=1`, which sends their PAGE as a NOTIFY, while every other row pages from the first deploy",
    );
    expect(text.includes("sends every PAGE as a NOTIFY")).toBe(false);
  });

  test("row 20 covers the operator document unread with its status, and row 2 the watch that refuses a bearer at its start", () => {
    for (const want of [
      "`/health` 200 while `/operator/data.json` has not been a 200 JSON document for 60 s",
      "`data_status` 401 or 403",
      "`STREAMS_OPERATOR_TOKEN`",
    ]) {
      expect([want, (cells(20)[4] ?? "").includes(want)]).toEqual([want, true]);
    }
    expect(cells(2)[4]).toContain("a watch whose `STREAMS_OPERATOR_TOKEN` the cell refuses (401 or 403) does not start");
  });

  test("row 17 says an idle cell's own metering no longer grows its counter, and NEXT-WORK 14.7 records the server's defect as fixed", () => {
    expect(cells(17)[4]).not.toContain("open on every enforce cell at this tip");
    expect(cells(17)[4]).toContain("the rollup's reads of its own ledgers meter nothing");
    expect(cells(17)[4]).toContain("NEXT-WORK 14.7");
    const next = readFileSync(join(import.meta.dir, "..", "..", "docs", "reviews", "2026-09-hardening", "NEXT-WORK.md"), "utf8");
    // Markdown wraps a sentence wherever its line fills, so the section is read as words.
    const defects = next.slice(next.indexOf("### 14.7 "), next.indexOf("### 14.8 ")).replace(/\s+/g, " ");
    expect(defects).not.toContain("RUNBOOK row 17 (`r17_unowned_usage`) is open on every enforce cell");
    expect(defects).toContain("Fixed (2026-10-14, round 4): the cell counted its own metering as unowned usage");
  });

  test("row 3 names every reason the watch gives a server's 503, superseded as a newer boot's fence (#179)", () => {
    for (const reason of REASONS) expect([reason, (cells(3)[4] ?? "").includes(`\`${reason}\``)]).toEqual([reason, true]);
    expect(cells(3)[4]).toContain("`superseded` is an older boot a newer boot of a fleet-off cell fenced");
  });

  test("gives each row its class, a PAGE paged to the on-call and a NOTIFY to the channel", () => {
    for (const row of ROWS) {
      const cls = CLASS[row] ?? "";
      const by = row <= 2 ? "the paging tool" : "cell-watch";
      const to = cls.split(" / ").map((c) => TO[c]).join(" / ");
      expect([row, cells(row)[2], cells(row)[5]]).toEqual([row, cls, `${by} to ${to}`]);
    }
  });
});

describe("RUNBOOK.md's canary paragraph", () => {
  test("makes the watch's hidden credential on the canary project with the scopes the canary needs, as its secret", () => {
    const from = LINES.findIndex((l) => l.startsWith("**The canary (rows 19 and 22).**"));
    const to = LINES.findIndex((l, i) => i > from && l.trim() === "");
    const text = LINES.slice(from, to).join(" ").replace(/\s+/g, " ");
    expect(from).toBeGreaterThan(LINES.findIndex((l) => l.startsWith("**What to alert on")));
    for (const want of [
      "`cell-admin credential --project sp_canary_<cell id> --hidden --name watch",
      `--scopes "${CANARY_SCOPES}"`,
      "secret `CANARY_CREDENTIAL`",
    ]) {
      expect([want, text.includes(want)]).toEqual([want, true]);
    }
  });
});

/// The paragraph that starts with `marker`, its whitespace runs made single spaces.
function paragraph(marker: string): string {
  const from = LINES.findIndex((l) => l.startsWith(marker));
  if (from < 0) return "";
  const to = LINES.findIndex((l, i) => i > from && l.trim() === "");
  return LINES.slice(from, to < 0 ? undefined : to).join(" ").replace(/\s+/g, " ");
}

describe("RUNBOOK.md's drills paragraph", () => {
  const text = paragraph("**Drills (rows 1, 2, 3, 6, 7, 12, 13, 19 and 21).**");

  test("follows the alert table and runs drills.e2e.test.ts on the built binaries", () => {
    const at = LINES.findIndex((l) => l.startsWith("**Drills ("));
    expect(at).toBeGreaterThan(LINES.findIndex((l) => l.startsWith("**The canary (rows 19 and 22).**")));
    for (const want of [
      "`cargo build --locked --bin streams-slate --bin s3lite`",
      "`CELL_WATCH_E2E_BIN_DIR=target/debug CELL_WATCH_DRILLS=1 bun test ./deploy/cell-watch/drills.e2e.test.ts`",
      "`NOTIFY_ONLY=0`",
      "`pager.ts`",
      "row 17 among the rules that never open on the drills' cell",
    ]) {
      expect([want, text.includes(want)]).toEqual([want, true]);
    }
  });

  test("names each drill's cause beside its row, and the PAGE rules without one", () => {
    for (const want of [
      "a policy naming `PROJECT_ID` in the bundle (row 7)",
      "the three feed files deleted, as the wrapper's `feeds deleted` does (row 6",
      "`POST /v1/debug/seal-pause?on=1` until the lag latch engages",
      "the cell stopped (row 3 after 2 min and the uptime check after 3 failures",
      "the watch killed (the heartbeat, 3 min after its last beat)",
      "the issuer stopped (row 21)",
      "`prisma-streams-internal` (row 19",
      "The other PAGE rules (row 4's crash loop, 9, 11, 14, 15, 16 and 22)",
    ]) {
      expect([want, text.includes(want)]).toEqual([want, true]);
    }
  });
});

describe("RUNBOOK.md's §7.5 and §8", () => {
  test("§7.5 says who answers, with the secondary and the Compute escalation contact", () => {
    const text = section("### 7.5 ");
    const who = text.slice(Math.max(0, text.indexOf("**Who answers.**")));
    expect(text.includes("**Who answers.**")).toBe(true);
    for (const name of ["`<on-call>`", "`<secondary>`", "`<notify channel>`", "`<compute contact>`"]) {
      expect([name, who.includes(name)]).toEqual([name, true]);
    }
  });

  test("§7.5 sets the paging service up with the stand-in pager's numbers, the severities and the escalation", () => {
    const text = section("### 7.5 ");
    const setup = text.slice(Math.max(0, text.indexOf("**Setting up the paging service**")));
    expect(text.includes("**Setting up the paging service**")).toBe(true);
    for (const want of [
      "`ALERT_SINK_KIND` is `events-v2`",
      "`critical` (a PAGE) is high urgency, the phone; `warning` (a NOTIFY) is low urgency and goes to `<notify channel>` alone",
      "`<on-call>`, then `<secondary>` after 15 minutes unacknowledged",
      `every ${UPTIME_EVERY_MS / 1_000} s with a ${UPTIME_TIMEOUT_MS / 1_000} s timeout, alerting after ${UPTIME_FAILURES} failures in a row (row 1)`,
      `no GET came for ${HEARTBEAT_WINDOW_MS / 60_000} minutes (row 2`,
      "`HEARTBEAT_URL`",
      "set-secrets",
    ]) {
      expect([want, setup.includes(want)]).toEqual([want, true]);
    }
  });

  test("Shared cells give a scratch or cert cell the stand-in pager as an app of its own, and production the real paging service", () => {
    const text = section("## Shared cells");
    const at = text.indexOf("**A scratch cell's paging stand-in.**");
    expect(at).toBeGreaterThan(text.indexOf("**Drills ("));
    const para = text.slice(at, text.indexOf("A single-server shared cell publishes", at));
    for (const want of [
      "`deploy-release.sh <scratch|cert> <tag>-watch --app pager-standin`",
      "`set-secrets <stage> --app pager-standin`",
      "`ALERT_SINK_KIND` `events-v2`",
      "`https://<domain>/v2/enqueue`",
      "`https://<domain>/heartbeat/<token>`",
      "`GET /changes`",
      "Production pages through the real paging service, PagerDuty Events v2, as §7.5 sets it up",
      "`docs/BETA-SCRATCH-DEPLOY.md`",
    ]) {
      expect([want, para.includes(want)]).toEqual([want, true]);
    }
  });

  test("§8 states the operator surface's bearer gate and nowhere calls it unsecured", () => {
    const text = section("## 8. ");
    expect(text).toContain("every server serves `/operator` behind the deployment bearer (SR-5");
    expect(/unsecured/i.test(text)).toBe(false);
  });
});
