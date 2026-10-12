# L6 seal spike tools (snapshot)

Throwaway, on the never-merged `l6-spike` branch. These are copies of
`~/.streams-k2/analysis/public-beta/l6/tools/` as they stood when milestone M1
was measured and analysed (2026-10-12). The live copies under `$L6/tools` are
the ones the wrappers call (they use absolute `$L6` paths); this snapshot pins
what produced `$L6/M1/` beside the code it measured.

| file | what |
|---|---|
| `spike-run.sh` | runs one `bench/k2cost/points/L6S.env` point, refuses or waits for other heavy legs (`WAIT`, `QUIET`, `OVERLAP`), runs `token-guard.sh` beside the rig (`GUARD`), appends one line to `$L6/runs.jsonl` |
| `token-guard.sh` | re-mints the rig's bearer token only when the rig's own 240 s refresh loop has died (the void identity run of 2026-10-11) |
| `m1-queue.sh`, `m1-identity-queue.sh` | the M1 grid and the identity pairs |
| `hostload.sh` | 30 s host-load sampler to `$L6/logs/hostload.jsonl` |
| `census.py` | the per-point census (SPIKE-PLAN §2.3 with review A1, A2, A5, A6, C1) |
| `predict.py` | pre-registered design-B predictions per census point (review C3) |
| `compare.py` | arm identity per billed cell, and M1's acceptance tables |
| `identity_spread.py` | arm identity against the run-to-run spread of each build |

Python 3.9, standard library only. sha256 at the M1 commit:

```
0c0f03840d361340a1b2ba457ac7fdf4049e5f1a3c77efd9202f02e3b72a1865  census.py
130e4d70a6ffd17b023189eb67a6367e8e77b9aedd43e80e5a518876f48a0ee4  compare.py
a0d909f31179b554c01f41101182a936fdc45c2111459cc5604579fe93d8142f  identity_spread.py
a74d39168416b228f8cc66b81d2d4e2beaa146c2632c87a8cadd9eac7aeff3f6  predict.py
3ba7cff71b210c7be4596733cc50f7bd1b6c199499d2b76a5ca2dd451ef8aa72  hostload.sh
2b774c11c745875c0d6ea05e26eaf56353d583ed66d31b65b0c1fdd914d8ea03  m1-identity-queue.sh
701cb40d14fffba5e07f5a12508b3ea34a5f4ac5bee05f6a24e26780b2ccc871  m1-queue.sh
7d5fa8d2525d7fd6bf511e7ca0c2bd674300f42e06be828e9d6b0dba70f5a3b2  spike-run.sh
8d8108bbf43e4c2f58353f7b4c490b129ae36db156e150999547b5222e10ba9e  token-guard.sh
```

Results, commands and caveats: `$L6/M1/NOTES.md` and `$L6/SPIKE-LOG.md`.
