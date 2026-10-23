// An in-memory stand-in for the server's /v1/queues routes (the subset of
// ~/.streams-k2/analysis/queues/API.md the replay uses), for the replay's
// own tests only: create, stats, send, batch send, claim with long poll,
// settle (ack, retry, fail), lease expiry and attempt exhaustion. It is not
// a model of the server's storage or cost and is never used in a run.
import http from 'node:http';
import { randomUUID } from 'node:crypto';
import { resolve } from 'node:path';
import { pathToFileURL } from 'node:url';

export function startFakeQueues(opts = {}) {
  const token = opts.token ?? 'test-token';
  // null: any non-empty key is accepted (the rig self-test's stream key).
  const key = opts.key === undefined ? 'test-key' : opts.key;
  const dropSeq = opts.dropSeq; // { queue, seq }: accept, answer 201, never store
  const queues = new Map();
  const leases = new Map(); // token -> message

  const now = () => Date.now();
  const json = (res, status, body) => {
    const text = JSON.stringify(body);
    res.writeHead(status, { 'content-type': 'application/json', 'content-length': Buffer.byteLength(text) });
    res.end(text);
  };
  const err = (res, status, code, retryable = false) => json(res, status, { error: { code, message: code, retryable } });

  function expire(q) {
    const t = now();
    for (const m of q.msgs.values()) {
      if (m.state === 'leased' && m.leaseDeadline <= t) {
        leases.delete(m.token);
        m.state = 'retrying';
        m.availableAt = t;
      }
    }
  }
  function stats(q) {
    expire(q);
    const t = now();
    const s = { due: 0, scheduled: 0, leased: 0, retrying: 0, failed: 0 };
    for (const m of q.msgs.values()) {
      if (m.state === 'leased') s.leased++;
      else if (m.state === 'failed') s.failed++;
      else if (m.state === 'pending' || m.state === 'retrying') {
        if (m.availableAt <= t) s.due++;
        else if (m.state === 'pending') s.scheduled++;
        else s.retrying++;
      }
    }
    return { ...s, paused: false, counters: { ...q.counters } };
  }
  function enqueue(q, name, send) {
    const t = now();
    const id = `msg_${randomUUID()}`;
    const availableAt = send.deliverAt ?? t + (send.delayMs ?? 0);
    const dropped = dropSeq && dropSeq.queue === name && send.body?.s === dropSeq.seq;
    if (!dropped) {
      q.msgs.set(id, { id, body: send.body, enqueuedAt: t, availableAt, state: 'pending', attempts: 0 });
      q.counters.sent++;
    }
    return { id, enqueuedAt: t, availableAt, duplicate: false };
  }
  function take(q, max) {
    expire(q);
    const t = now();
    const due = [...q.msgs.values()]
      .filter((m) => (m.state === 'pending' || m.state === 'retrying') && m.availableAt <= t)
      .sort((a, b) => a.availableAt - b.availableAt)
      .slice(0, max);
    return due.map((m) => {
      m.state = 'leased';
      m.attempts++;
      m.token = `lt_${randomUUID()}`;
      m.leaseDeadline = t + q.leaseMs;
      leases.set(m.token, m);
      q.counters.delivered++;
      return {
        id: m.id, token: m.token, attempt: m.attempts, cycle: 1, enqueuedAt: m.enqueuedAt,
        availableAt: m.availableAt, leaseDeadline: m.leaseDeadline, body: m.body,
        bodyBase64: null, contentType: 'application/json', headers: {}, key: null, dedupKey: null,
      };
    });
  }
  function settle(q, op) {
    const m = leases.get(op.token);
    if (!m || m.state !== 'leased') return 'stale';
    leases.delete(op.token);
    if (op.op === 'ack') {
      m.state = 'done';
      q.counters.acked++;
      q.msgs.delete(m.id);
    } else if (op.op === 'fail' || (op.op === 'retry' && m.attempts >= q.config.group.maxAttempts)) {
      m.state = 'failed';
      q.counters.failed++;
    } else if (op.op === 'retry') {
      m.state = 'retrying';
      m.availableAt = now() + (op.delayMs ?? 0);
      q.counters.retried++;
    } else {
      return 'stale';
    }
    return 'ok';
  }

  async function handle(req, res, body) {
    if (req.headers.authorization !== `Bearer ${token}`) return err(res, 401, 'unauthorized');
    if (req.url === '/v1/debug/store') return json(res, 200, { fake: true });
    const url = new URL(req.url, 'http://x');
    const m = url.pathname.match(/^\/v1\/queues\/([^/:]+)(.*)$/);
    if (!m) return err(res, 404, 'not_found');
    const name = decodeURIComponent(m[1]);
    const rest = m[2];
    const sentKey = req.headers['streams-encryption-key'];
    const hasKey = key === null ? Boolean(sentKey) : sentKey === key;
    if (req.method === 'PUT' && rest === '') {
      if (!hasKey) return err(res, 400, 'encryption_key_required');
      if (queues.has(name)) return json(res, 200, { name, config: queues.get(name).config });
      const config = body;
      queues.set(name, {
        config, msgs: new Map(), leaseMs: config.group?.leaseMs ?? 30_000,
        counters: { sent: 0, delivered: 0, acked: 0, retried: 0, failed: 0, expired: 0, discarded: 0, redriven: 0, stale: 0 },
      });
      return json(res, 201, { name, config });
    }
    const q = queues.get(name);
    if (!q) return err(res, 404, 'queue_not_found');
    if (req.method === 'GET' && rest === '') return json(res, 200, { name, config: q.config, stats: stats(q) });
    if (req.method === 'POST' && rest === '/messages') {
      if (!hasKey) return err(res, 400, 'encryption_key_required');
      return json(res, 201, enqueue(q, name, body));
    }
    if (req.method === 'POST' && rest === '/messages:batch') {
      if (!hasKey) return err(res, 400, 'encryption_key_required');
      return json(res, 201, { results: body.messages.map((s) => enqueue(q, name, s)) });
    }
    if (req.method === 'POST' && rest === '/groups/default:claim') {
      if (!hasKey) return err(res, 400, 'encryption_key_required');
      const deadline = now() + Math.min(body.waitMs ?? 0, 25_000);
      let closed = false;
      res.on('close', () => { closed = true; });
      for (;;) {
        const got = take(q, body.max ?? 1);
        if (got.length || now() >= deadline || closed) {
          if (closed) {
            // The client went away: its leases stand until they expire, as
            // a real server's would.
            return undefined;
          }
          return json(res, 200, { messages: got, paused: false });
        }
        await new Promise((r) => setTimeout(r, 10));
      }
    }
    if (req.method === 'POST' && rest === '/groups/default:settle') {
      const results = body.ops.map((op) => ({ token: op.token, outcome: settle(q, op), acceptedLate: false }));
      return json(res, 200, { results });
    }
    return err(res, 404, 'not_found');
  }

  const server = http.createServer((req, res) => {
    const chunks = [];
    req.on('data', (c) => chunks.push(c));
    req.on('end', () => {
      let body = {};
      try {
        body = chunks.length ? JSON.parse(Buffer.concat(chunks).toString('utf8')) : {};
      } catch {
        return err(res, 400, 'bad_json');
      }
      handle(req, res, body).catch((e) => err(res, 500, String(e)));
    });
  });
  return new Promise((resolve) => {
    server.listen(opts.port ?? 0, opts.host ?? '127.0.0.1', () => {
      const { port } = server.address();
      resolve({
        base: `http://127.0.0.1:${port}`, token, key, queues,
        close: () => new Promise((r) => { server.closeAllConnections?.(); server.close(() => r()); }),
      });
    });
  });
}

// The rig self-test (README): run-ab.sh with this file as the "server
// binary" checks the orchestration, the marks, the sampler and the pricing
// end to end without the real server. Takes `--listen HOST:PORT` and the
// bearer from AUTH_TOKEN, as streams-slate does; accepts any stream key.
if (process.argv[1] && import.meta.url === pathToFileURL(resolve(process.argv[1])).href) {
  const i = process.argv.indexOf('--listen');
  const [host, port] = (i > 0 ? process.argv[i + 1] : '127.0.0.1:8090').split(':');
  startFakeQueues({ token: process.env.AUTH_TOKEN, key: null, host, port: Number(port) }).then((f) => {
    process.stderr.write(`fake queues on ${f.base}\n`);
    process.on('SIGTERM', () => f.close().then(() => process.exit(0)));
  });
}
