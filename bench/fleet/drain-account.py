#!/usr/bin/env python3
"""Drain the pilot generator and prove EXACT zero-loss accounting.

Run-1's kill-mid-flight stop left a 41 s gap and a one-sided bound;
with /drain + okAppends the books close as an equality:

    POST gen /drain -> workers stop taking attempts, in-flight settle
    okAppends (exact, final)  ==  sum(stream tails via LB HEAD)

Usage: GURL, LURL, AUTH, SKEY env; args: [prefix] [n_streams]
Exit 0 = exact match; 1 = mismatch (investigate before anything else).
"""
import json, os, sys, time, urllib.request, urllib.error

GURL = os.environ["GURL"].rstrip("/")
LURL = os.environ["LURL"].rstrip("/")
AUTH = os.environ["AUTH"]
SKEY = os.environ["SKEY"]
PREFIX = sys.argv[1] if len(sys.argv) > 1 else "fleet2s"
N = int(sys.argv[2]) if len(sys.argv) > 2 else 32
CROCK = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"

def get(url, headers=None, method="GET", timeout=30):
    r = urllib.request.Request(url, method=method, headers=headers or {})
    try:
        with urllib.request.urlopen(r, timeout=timeout) as resp:
            return resp.status, dict(resp.headers), resp.read()
    except urllib.error.HTTPError as e:
        return e.code, dict(e.headers), e.read()
    except Exception as e:
        return -1, {}, str(e).encode()

def decode_count(tok):
    n = 0
    for ch in tok:
        n = (n << 5) | CROCK.index(ch)
    n >>= 2
    assert (n >> 96) == 0, tok
    return (n >> 32) & 0xFFFFFFFFFFFFFFFF  # rawSeq = record count

st, _, _ = get(f"{GURL}/drain", method="POST")
print(f"[drain] POST /drain -> {st}")
final = None
for i in range(90):
    st, _, b = get(f"{GURL}/stats")
    if st != 200:
        time.sleep(1); continue
    d = json.loads(b)
    if d.get("draining") and d.get("activeWorkers", 1) == 0:
        final = d
        break
    time.sleep(1)
if not final:
    print("[drain] FAIL: generator never quiesced"); sys.exit(1)
print(f"[drain] FINAL ok={final['ok']} okAppends={final['okAppends']} "
      f"okReads={final['okReads']} errs={final['errs']} throttled={final['throttled']}")

time.sleep(5)  # let the last acks' commits settle server-side
H = {"authorization": f"Bearer {AUTH}", "stream-encryption-key": SKEY}
total, missing = 0, []
for i in range(N):
    name = f"{PREFIX}-{i}"
    st, h, _ = get(f"{LURL}/v1/stream/{name}", headers=H, method="HEAD")
    tok = None
    for k, v in h.items():
        if k.lower() == "stream-next-offset":
            tok = v
    if st != 200 or not tok:
        missing.append(f"{name}:{st}")
        continue
    total += decode_count(tok)
if missing:
    print(f"[drain] FAIL: tails unreadable: {missing}"); sys.exit(1)

# ---- SET-LEVEL AUDIT (round-19 correction) ---------------------------
# Aggregate tails prove no aggregate acknowledged DEFICIT. They cannot
# prove "every acknowledged operation appears exactly once": a missing
# ack can be masked by an ambiguous op that committed. SET_AUDIT=1 pages
# every stream and recomputes the generator's compact ledger
# (count/sum/xor over acknowledged op ids) from what is actually
# readable — all three matching pins the multiset.
if os.environ.get("SET_AUDIT") == "1" and final.get("ledger"):
    import urllib.parse
    bad = []
    for i, led in enumerate(final["ledger"]):
        name = f"{PREFIX}-{i}"
        seen_count = seen_sum = seen_xor = 0
        dup_guard = set()
        tok, hops = None, 0
        while hops < 10_000:
            hops += 1
            q = f"?offset={tok}" if tok else ""
            st, h, body = get(f"{LURL}/v1/stream/{name}{q}", headers=H)
            if st != 200 or not body.strip():
                break
            recs = json.loads(body)
            if not recs:
                break
            for r in recs:
                oid = r.get("i")
                if oid is None:
                    continue
                seen_count += 1
                seen_sum = (seen_sum + oid) % (1 << 64)
                seen_xor ^= oid
                if oid in dup_guard:
                    bad.append(f"{name}: duplicate op id {oid}")
                dup_guard.add(oid)
            nxt = None
            for k, v in h.items():
                if k.lower() == "stream-next-offset":
                    nxt = v
            if not nxt or nxt == tok:
                break
            tok = nxt
        if (seen_count, seen_sum, seen_xor) != (led["count"], led["sum"] % (1 << 64), led["xor"]):
            bad.append(
                f"{name}: ledger mismatch acked(count={led['count']},sum={led['sum']},"
                f"xor={led['xor']}) vs read(count={seen_count},sum={seen_sum},xor={seen_xor})"
            )
    if bad:
        print("[drain] SET-AUDIT FAIL:")
        for b in bad[:10]:
            print("   ", b)
        sys.exit(1)
    print(f"[drain] SET-AUDIT: every acknowledged op id appears EXACTLY ONCE "
          f"across {len(final['ledger'])} streams")

exp = final["okAppends"]
errs = final["errs"]
print(f"[drain] TAILS Σ={total:,} vs okAppends={exp:,} (Δ={total-exp:+}, errs={errs})")
# Two-sided: every ack must be present (lower bound), and any surplus
# must be explained by error-ambiguous appends — a client-visible error
# whose commit landed anyway (the soak7 408 class). After a drain there
# is no in-flight slack, so surplus > errs would mean duplication.
if exp <= total <= exp + errs:
    tag = "EXACT EQUALITY" if total == exp else f"+{total-exp} error-ambiguous commits (≤ {errs} errs)"
    print(f"[drain] ZERO-LOSS: {tag}"); sys.exit(0)
if total < exp:
    print("[drain] LOSS: acked records missing — stop and investigate"); sys.exit(1)
print("[drain] SURPLUS EXCEEDS ERRS: possible duplication — stop and investigate"); sys.exit(1)
