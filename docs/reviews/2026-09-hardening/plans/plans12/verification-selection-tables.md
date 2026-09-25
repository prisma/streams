# Item 89: the verification plan's selection tables miss small codec/admission owners, and its `loom` key is dead

Tree: `slate` at `aaf2baa5`. `origin/slate` is at `6ef3bc64`, and the four commits `2ced74c7..aaf2baa5` are being verified in the tree right now. Every line number below was checked on `aaf2baa5`.

Summary: the reviewer is right on both counts, and the dead-key problem is one key bigger than reported. `plan()` computes five booleans. CI reads three of them. `loom` **and** `compiler` are read by nothing. Because of that, an edit to `src/offsets.rs`, `src/segmap.rs`, `src/telemetry_batch.rs` or `src/usage.rs` selects no conditional check at all.

The reviewer's change as written ("register the owners") cannot be built on its own:
- `src/offsets.rs` has two equivalent `|` to `^` mutants and one equivalent match-arm deletion.
- `src/segmap.rs` has one equivalent `<` to `<=` mutant.
- All three files have survivors that no test in their filters kills.

If the rows were simply added, the first edit to any of these functions would fail the mutation leg. So would pushing the rows in the same push as `33d8d3dd`, because that commit rewrote `offsets.rs` and in-diff would then select most of the file.

The buildable version:
- **C1** (tests only) adds the pins that kill the survivors.
- **C2** (behaviour-preserving) restructures the four equivalent mutants away.
- **C3** deletes both dead keys, red first, and fixes the RUST-QUALITY wording.
- **C4** registers `offsets`, `segmap` and `telemetry_batch`, red first.

`usage.rs` is deferred: an estimated ~180 mutants is more than the job can run, and the file would need a kill audit of its own (section 8).

---

## 1. Problem (verified on `aaf2baa5`)

### 1a. Five check keys are computed, three are consumed

`scripts/quality/verification_plan.py` (403 lines), `plan()`:
```python
80    codec = any(p.startswith(CODEC_PREFIXES) for p in implementation)
81    quota = any(p.startswith(QUOTA_PREFIXES) for p in implementation)
82    lifecycle = any(p.startswith(LIFECYCLE_PREFIXES) for p in implementation)
83    buffers = any(p.startswith(BUFFER_PREFIXES) for p in implementation)
...
98    return {'compiler': bool(source) or tooling, 'properties_fuzz': codec or quota or tooling,
99            'loom': lifecycle or tooling, 'miri': buffers or tooling,
100           'mutants': mutants, 'changed_rust_files': source,
```
`plan_schedule()` sets `'compiler': True,` (L114) and `'loom': True,` (L116).

**The only consumer of the check keys** is `.github/workflows/rust-quality.yml:79-94`:
```yaml
          plan = json.load(open('target/quality-plan/plan.json'))
          with open(os.environ['GITHUB_ENV'], 'a') as output:
              for name in ['miri', 'properties_fuzz', 'mutants']:
                  output.write(f'CHECK_{name.upper()}={str(plan[name]).lower()}\n')
      ...
        if: env.CHECK_PROPERTIES_FUZZ == 'true' || github.event_name == 'schedule'   # nightly.sh corpus
        if: env.CHECK_MIRI == 'true'                                                # nightly.sh miri
        if: env.CHECK_MUTANTS == 'true'                                             # mutations.sh
```
`scripts/quality/mutation_driver.py:105-121` reads `plan.json`, but only the mutation receipt fields (`mutation_source_files`, `mutation_discovery_source_files`, `deleted_*`, `renamed_*`, `possible_replacement_files`, `selection_kind`, `comparison_revision`). It never reads a check key.

`git grep -n "CHECK_\|'compiler'\|'loom'" -- . ':!target'` returns exactly the sites listed below and nothing else. `plan.json` is not uploaded; it is written to `target/quality-plan/` and printed to the log.

Every use site:

| Key | Computed | Asserted in tests | Consumed |
| --- | --- | --- | --- |
| `loom` | vp.py:82 (the `lifecycle` local, used only here), :99, :116 | test_verification_plan.py:124, 129, 139, 142, 151; test_production_changes.py:25 | **nowhere** |
| `compiler` | vp.py:98, :114 | test_verification_plan.py:37, 67, 138, 154; test_production_changes.py:23, 222 | **nowhere** |
| `properties_fuzz` | vp.py:98, :115 | several | rust-quality.yml:81/85 |
| `miri` | vp.py:99, :117 | several | rust-quality.yml:81/91 |
| `mutants` | vp.py:100, :118 | many | rust-quality.yml:81/94 |

**Loom already runs on every change.** All seven Loom models are ordinary `quality_loom_*` tests:
- `touch/loom_tests.rs:11`
- `shard/commit_handoff/loom_tests.rs:33` and `:69`
- `billing/sweep_custody/tests.rs:35`
- `billing/read_accumulator/tests.rs:156`
- `billing/read_spool/tests.rs:110`
- `bin/pilot/benchmark/tests.rs:477`

The six in the lib run in the unconditional `quality` job step `scripts/test-leg.sh target/legs/quality.log --min 15 -- --locked --release --lib quality_` (rust-quality.yml:48, no `if:`). The pilot one is a bin test, and it runs in ci.yml's full suite `cargo test --release` (ci.yml:104). Compiler and Clippy likewise run unconditionally in `scripts/quality.sh`.

**The docs claim otherwise.** `docs/RUST-QUALITY.md:132-133`: "All otherwise selected compiler, property, Miri and Loom checks remain selected." `:139-140`: "do not select runtime mutation/Miri/property/Loom checks by themselves."

`docs/quality/verification.json` pins the sha of `verification_plan.py` and `test_verification_plan.py` under `gate_tooling_sha256`. No script reads that key (`git grep gate_tooling_sha256 -- scripts tools .github` is empty), and its `scope_note` calls it a "Historical adoption receipt", so it is not a live pin.

### 1b. The four small owners select nothing

The prefix tables (vp.py:22-31) are:
- `CODEC_PREFIXES`: `src/crypto`, `src/postings`, `src/product_cursor`, `src/queue`, `src/application/read_`, `src/shard/record`, `src/rollup/allocation`, `src/rollup/storage`.
- `QUOTA_PREFIXES`: `src/quota`.
- `LIFECYCLE_PREFIXES`: `src/shard`, `src/tasks`, `src/runtime`, `src/bootstrap`, `src/sse`, `src/touch.rs`, `src/billing/read_accumulator`, `src/billing/read_spool`, `src/bin/pilot/…`.
- `BUFFER_PREFIXES`: `src/retained_bytes`, `src/application/read_`, `src/crypto`, `src/bootstrap`, `src/fleet`, `src/http`, `src/ops`.

None of these is a prefix of `src/offsets.rs`, `src/segmap.rs`, `src/telemetry_batch.rs` or `src/usage.rs`. None of the four paths appears in `OWNERS` (mutation_owners.py:53-178, 110 owner names).

Trace of `plan(['src/offsets.rs'])`:
- `source = implementation = ['src/offsets.rs']`, and `tooling`, `codec`, `quota`, `lifecycle` and `buffers` are all False.
- `mutation_source_files = []` (vp.py:87-90: not forced, not in `by_source`, not a critical prefix), so `mutants = False`.
- Result: `compiler=True`, everything else False.

The other three paths trace identically. Since `compiler` is read by nothing (1a), an edit to any of these four files currently runs **no conditional check**, only the unconditional legs.

What the four files are:
- `src/offsets.rs` (187 lines) is the one raw-wire offset codec (every raw surface since `33d8d3dd`).
- `src/segmap.rs` (628) holds segment-map transitions, and `validate` is the persisted-topology validator (`registry.rs:542`).
- `src/telemetry_batch.rs` (94) is the byte-bounded journal prefix selection used by `ops.rs:215` (a `src/ops` critical caller) and `audit.rs:197`.
- `src/usage.rs` (993) holds per-stream admission (`admit_on`, :197-240) and usage telemetry.

### 1c. Registration alone is unbuildable (new finding)

Mutation run history (`target/quality-mutations/*/mutants.out`) and the item-88 plan confirm cargo-mutants 27.1.0's genres in this repo: FnValue, BinaryOperator, UnaryOperator, MatchArm (the arm-deletion form "delete match arm 401 in peer_refusal" appears when a `_` arm exists), MatchArmGuard, and StructField. The relevant equivalent mutants on HEAD:

- **offsets.rs:37** `let n: u128 = ((epoch as u128) << 96) | ((next as u128) << 32);`. `|` becomes `^` and the result is identical, because the two operands never share a bit.
- **offsets.rs:62** `n = (n << 5) | v as u128;`. `|` becomes `^` and the result is identical, because `n << 5` has five zero low bits and `v < 32`. The item-88 plan (plans11/offset-codec-one.md §5) already deferred registration for exactly these two.
- **offsets.rs:81** `'0'..='9' => Some(ch as u8 - b'0'),`. Deleting this arm is equivalent: the `_` arm maps a digit through `ALPHABET.iter().position(..)`, and `ALPHABET[0..10] == b"0123456789"` while `to_ascii_uppercase` is the identity on digits.
- **segmap.rs:74** `None if !successor && *id < self.seg_id => continue,`. `<` becomes `<=` and nothing changes, because line 64 already refused `*id == self.seg_id` (`!seen.insert(..) || *id == self.seg_id`), so equality cannot reach the guard.

Survivors that no current test in the would-be filters kills (traced against every existing test in the file):
- **offsets:**
  - Deleting `'O' | 'o'` or `'I' | 'i' | 'L' | 'l'` is not caught. No test decodes an alias. The proptest's alphabet includes aliases, but it only checks that an accepted token is self-consistent, not that an alias is accepted.
  - Replacing `<impl Display for OffsetError>::fmt` with `Ok(Default::default())` is not caught. The words are pinned only by `http::tests::raw_position_tokens_and_fork_refusals_are_exact`, which is outside an `offsets::` filter.
- **segmap:**
  - `contains`: inner `&&` becoming `||`. Nothing routes `KEYSPACE_END` after a split.
  - `validate_lineage`: the guard's `&&` becoming `||`, and the overlap `||` becoming `&&`. No test expects "lineage ranges do not overlap".
  - `validate`: both coverage `||` becoming `&&`. No test expects "terminal segments do not exactly cover keyspace".
  - `check_partition`: FnValue `true`, and `||` becoming `&&`. No test expects `false`.
  - `split`: `split_at <= lo || split_at >= hi` with `||` becoming `&&`. No test splits on a bound.
  - `split` and `merge`: `self.version += 1` becoming `-=` or `*=`. No test reads `version`.
- **telemetry_batch:**
  - `max_bytes < 2` becoming `==` or `<=`. No test uses a 0-2 byte budget.
  - `1 + usize::from(count > 0)` becoming `-` or `*`, and `count > 0` becoming `<`, at both sites (the with-end sum and the comma push). No test has two rows.
  - `with_array_end > max_bytes` becoming `<`. The only test's single row never trips it.

`gh run list --workflow rust-quality.yml --event schedule` is empty, and `main` (the default branch) carries only `ci.yml` and `release.yml`. So the seven-night rotation has never run; for now, only the push/PR in-diff leg actually exercises an owner.

---

## 2. Contract decision

**Typed contract (tooling):** `plan.json` selects exactly three checks: `properties_fuzz`, `miri` and `mutants`. Each one gates exactly one `rust-quality` step. A planner test reads the workflow and refuses any boolean key that no step exports or gates (R1).

`loom` and `compiler` are deleted. This goes one key beyond the reviewer, because `compiler` has exactly the defect the reviewer describes for `loom`, and R1 would otherwise need a special case for it. Compiler, Clippy, the `quality_` leg (with its property and Loom models) and the full suite are unconditional, and the docs will say so. `LIFECYCLE_PREFIXES` stays, because it still feeds `CRITICAL_PREFIXES` for mutation selection. Only the `lifecycle` local goes.

**Small owners are selected by registration, not by prefix.** Three rows are added to `OWNERS`: `offsets`, `segmap` and `telemetry_batch`. The prefix tables are unchanged. Adding `src/offsets.rs` to `CODEC_PREFIXES` would only select `nightly.sh corpus`, which replays the *postings* fuzz corpus. That tells us nothing about offsets. The offsets properties (`quality_offsets_*`, 1,024 cases each) already run unconditionally in the `quality_` leg.

Rotation slots are derived, not configured: `sha256(name)[:4] % 7` gives `offsets` 2, `segmap` 2, `telemetry_batch` 5. The existing `test_every_scheduled_receipt_reaches_the_driver_unchanged` already proves that every owner lands in exactly one bucket.

**No wire change.**
- `plan.json` is an internal CI receipt. Its two readers (the rust-quality inline step and `mutation_driver.py`) read none of the removed keys, and `selected-owners.json` is unchanged.
- The C2 Rust edits are behaviour-preserving: tokens, parse results and every refusal or validation string stay byte-identical (proofs in section 3b).
- No `/v1/debug`, `/metrics`, status or body changes.

---

## 3. Tests

### 3a. Red tests (C3 and C4; the only behaviour changes are the planner's outputs)

Run from the repo root with Python ≥ 3.11. Stock macOS `python3` is 3.9, so use the usual `python3` → `/opt/homebrew/bin/python3.12` shim first on PATH. The simulated failure text below was produced with python3.12 against the same list values.

**R1** is `scripts/quality/test_verification_plan.py::Triggers::test_every_selected_check_gates_a_workflow_step` (new, C3). It needs `import re` and `from common import ROOT, syntax`; the file currently has `from common import syntax`.
```python
    def test_every_selected_check_gates_a_workflow_step(self):
        workflow = (ROOT / '.github/workflows/rust-quality.yml').read_text()
        exported = re.search(r"for name in \[([^\]]*)\]:", workflow)
        self.assertIsNotNone(exported, 'rust-quality no longer exports plan checks')
        exported = sorted(re.findall(r"'(\w+)'", exported.group(1)))
        for checks in (plan(['src/shard.rs']), plan_schedule(0)):
            self.assertEqual(sorted(k for k, v in checks.items() if type(v) is bool), exported)
        for name in exported:
            self.assertIn(f"if: env.CHECK_{name.upper()} == 'true'", workflow)
```
Trace on HEAD: `exported = ['miri', 'mutants', 'properties_fuzz']`. For `plan(['src/shard.rs'])` the bool keys are `compiler, properties_fuzz, loom, miri, mutants`. The `schedule_slot` int is excluded by `type(v) is bool`. The first `assertEqual` fails:
```
FAIL: test_every_selected_check_gates_a_workflow_step (test_verification_plan.Triggers.test_every_selected_check_gates_a_workflow_step)
AssertionError: Lists differ: ['compiler', 'loom', 'miri', 'mutants', 'properties_fuzz'] != ['miri', 'mutants', 'properties_fuzz']

First differing element 0:
'compiler'
'miri'

First list contains 2 additional elements.
First extra element 3:
'mutants'

- ['compiler', 'loom', 'miri', 'mutants', 'properties_fuzz']
?  --------------------

+ ['miri', 'mutants', 'properties_fuzz']
```
`FAILED (failures=1)`. It turns green after C3: both plans give exactly the three keys, and all three `if: env.CHECK_… == 'true'` substrings exist (rust-quality.yml:85, 91, 94).

**R2** is `scripts/quality/test_verification_plan.py::Triggers::test_small_codec_and_admission_owners_select_their_mutations` (new, C4; this is the reviewer's first step, extended).
```python
    def test_small_codec_and_admission_owners_select_their_mutations(self):
        for path, owner in (('src/offsets.rs', 'offsets'), ('src/segmap.rs', 'segmap'),
                            ('src/telemetry_batch.rs', 'telemetry_batch')):
            with self.subTest(path=path):
                checks = plan([path])
                self.assertEqual(checks['selected_mutation_owners'], [owner])
                self.assertTrue(checks['mutants'])
                self.assertEqual([entry.name for entry in validate_plan(checks)], [owner])
                self.assertFalse(plan([path], production_unchanged=[path])['mutants'])
```
Trace on HEAD (or on C3): `selected_mutation_owners == []` (section 1b), so each subtest fails at the first assert:
```
FAIL: test_small_codec_and_admission_owners_select_their_mutations (test_verification_plan.Triggers.test_small_codec_and_admission_owners_select_their_mutations) (path='src/offsets.rs')
AssertionError: Lists differ: [] != ['offsets']

Second list contains 1 additional elements.
First extra element 0:
'offsets'

- []
+ ['offsets']
```
The same block follows for `(path='src/segmap.rs')` / `'segmap'` and `(path='src/telemetry_batch.rs')` / `'telemetry_batch'`, then `FAILED (failures=3)`.

After C4 the rows resolve, `validate_plan` returns that one owner (no unregistered source), and the production-unchanged control omits the file.

### 3b. Pinning tests and proofs for the refactor (C1 pins, C2 restructure)

**C1 pins (test only, green on HEAD).** Each one is traced below.

`src/offsets.rs`, inside the existing `#[cfg(test)] mod tests`, after `non_canonical_tokens_keep_their_lax_reading`:
```rust
    /// Crockford's O and I/L spellings are a wire-visible reading a strict
    /// decoder must decide on explicitly (review item 88 step 2); until then
    /// no refactor may move them.
    #[test]
    fn crockford_aliases_read_as_their_digits() {
        for alias in [
            "000000000000000000I0000000",
            "000000000000000000i0000000",
            "000000000000000000L0000000",
            "000000000000000000l0000000",
        ] {
            assert_eq!(parse(alias), Ok((0, 2)), "{alias}");
        }
        assert_eq!(parse("OOOOOOOOOOOOOOOOOOOOOOOOOo"), Ok((0, 0)));
    }

    /// The fork-offset refusal hands these words to clients verbatim.
    #[test]
    fn refusal_words_are_wire_text() {
        assert_eq!(OffsetError::Length(1).to_string(), "invalid offset length: 1");
        assert_eq!(OffsetError::Char('U').to_string(), "invalid base32 char: U");
        assert_eq!(OffsetError::Epoch(3).to_string(), "unsupported offset epoch: 3");
    }
```
Traces:
- `encode(0, 2)` is `"00000000000000000010000000"`, with the `1` at index 18. That char covers padded bits 35..39, which is n bit 33, which is `next` bit 1, so `next = 2`. Each alias there decodes to `Some(1)` (offsets.rs:83), so the result is `Ok((0, 2))`.
- 25 × `O` plus `o` decode through :82 to 0, so the result is `Ok((0, 0))`.
- The three Display strings are the literals at :27-29. The same words are already pinned through the HTTP fork refusal (`src/http/tests.rs:80-91`).

`src/segmap.rs` tests module:
- `split_then_route_then_successors` (L542) gains two lines. After `route(mid)`: `assert_eq!(m.route(KEYSPACE_END).unwrap().seg_id, b);`. At the end: `assert_eq!(m.version, 2, "one transition bumps the CAS version once; a refused one bumps nothing");`.
  - Trace: `b = [mid, END)`, and `contains(END)` is true through `hi == END && k == END`. `a = [0, mid)` is false. The initial version is 1, the one split makes it 2, and `AlreadySealed` returns before the bump.
- `merge_adjacent_only` (L577) gains, at the end, `assert_eq!(m.version, 4, "two splits and one merge; the refused merge bumps nothing");`. Trace: 1, then 2, then 3; `NotAdjacent` returns before the bump; the merge makes it 4.
- `lineage_preserves_direction_and_historical_predecessor_rules` (L488) gains a case before `let mut reverse = map;`:
  ```rust
          let mut disjoint = map.clone();
          disjoint.segments[2].predecessors.push(a);
          assert_eq!(
              disjoint.validate().unwrap_err(),
              "lineage ranges do not overlap"
          );
  ```
  Trace: `segments[2]` is `b = [mid, END)`. Its new predecessor `a = [0, mid)` is found and is not reversed (1 < 2). The overlap check `a.lo(0) >= b.hi(END)` is false, but `b.lo(mid) >= a.hi(mid)` is true, so the call returns that error. Segments 0 and `a` validate clean first.
- New:
  ```rust
      /// A split must leave both children a non-empty range.
      #[test]
      fn a_split_point_on_the_parents_bounds_is_refused() {
          let mut m = SegmentMap::initial("root", 1);
          for bound in [0, KEYSPACE_END] {
              assert_eq!(
                  m.split(0, bound, 0, [1u8; 16], [2u8; 16], 2),
                  Err(MapError::InvalidSplitPoint)
              );
          }
          assert_eq!(m, SegmentMap::initial("root", 1), "a refused split changes nothing");
      }

      /// Both invariants anchor at key 0: a map whose only segment starts
      /// above it routes nothing below and must be refused.
      #[test]
      fn a_keyspace_that_starts_late_is_neither_covered_nor_partitioned() {
          let mut late = SegmentMap::initial("root", 1);
          late.segments[0].lo = 1;
          assert!(!late.check_partition());
          assert_eq!(
              late.validate().unwrap_err(),
              "terminal segments do not exactly cover keyspace"
          );
      }
  ```
  Traces:
  - Bounds: `0 <= lo(0)` and `END >= hi(END)` both hit :315 before any mutation (:302-314 only read), so `m` is unchanged.
  - Late start: `check_partition` has `first.0 = 1 != 0`, so it returns false. `validate` passes the per-segment checks (0 < next 1, 1 < END, seal fields both None, live with no successors) and has no lineage, then the leaves `[(1, END)]` fail the first coverage clause.

`src/telemetry_batch.rs` tests module. The pinned `array_delimiters_and_escaped_strings_count_toward_exact_limit` stays byte-identical.
```rust
    /// The floor is the empty array: below two bytes nothing can be framed.
    #[test]
    fn the_smallest_budget_frames_an_empty_array() {
        assert!(encode_prefix(std::iter::empty::<&u8>(), 1).is_err());
        let Ok(Selection::Encoded { body, count }) = encode_prefix(std::iter::empty::<&u8>(), 2)
        else {
            panic!("two bytes frame an empty array");
        };
        assert_eq!((body.as_slice(), count), (&b"[]"[..], 0));
    }

    /// Every row after the first pays one comma byte against the budget.
    #[test]
    fn a_later_row_pays_for_its_comma() {
        let events = ["a", "b"];
        for (max_bytes, expected, rows) in [(9, &br#"["a","b"]"#[..], 2), (8, &br#"["a"]"#[..], 1)] {
            let Ok(Selection::Encoded { body, count }) = encode_prefix(events.iter(), max_bytes) else {
                panic!("{max_bytes} bytes hold at least one row");
            };
            assert_eq!((body.as_slice(), count), (expected, rows), "{max_bytes}");
        }
    }
```
Traces:
- Budget 1 fails `< 2` and returns `Err`. Budget 2 with no events gives `[` then `]`, which is `Encoded{"[]", 0}`.
- Rows are `"a"` and `"b"`, 3 bytes each, with limit `max-2`.
  - Row 1: `with_array_end = 1+3+1+0 = 5`.
  - Row 2: `with_array_end = 4+3+1+1 = 9`. That is ≤ 9, so both rows are taken.
  - At budget 8, 9 > 8 breaks, leaving `["a"]` with count 1.

**C2 proofs (behaviour-preserving).**
- `offsets::encode`: `|` becomes `+`. Epoch occupies n bits 96..127 and next occupies bits 32..95. When `a & b == 0`, `a | b == a + b`, and the sum is below 2^128, so there is no overflow panic.
- `offsets::parse`: `|` becomes `+`. `(n << 5)` has zero low five bits and `v` is always < 32: `decode_char` returns an ALPHABET index or 0/1. The lax behaviour is kept: bits shifted past u128 are still dropped by `<<`, which checks only the shift amount.
- `decode_char`: the digit arm is deleted. For `'0'..='9'`, the `_` arm computes `to_ascii_uppercase(ch) == ch` and `position` gives the same index. No other char matched that arm.
- `segmap::validate_lineage` gets a `reversed` binding. Its truth table is identical:
  - `reversed ≡ (successor && id <= self) || (!successor && id >= self)`, the old :82 condition.
  - The new guard `!successor && !reversed` is `!successor && id < self`, the old :74 guard.
  - `reversed` is pure and computed before the lookup, so the error precedence is unchanged. A missing reversed edge still reports "references missing segment".

Pinning tests for C2 (all green before and after):
- offsets: `offsets::tests` (7 tests, including both `quality_offsets_*` properties at 1,024 cases, `non_canonical_tokens_keep_their_lax_reading`, the aliases and the refusal words), `http::tests::raw_position_tokens_and_fork_refusals_are_exact`, `golden_tests::golden_layout4_raw_offset_tokens` and `golden_layout4_raw_epoch_offset_token`.
- segmap: `segmap::tests` (10 tests; the pending test's 13 cases and the lineage test's 6 message cases pin every string and its precedence), plus the DST topology rigs through `registry.rs:542` in the full suite.

There is no compile-level proof: every change is expression-level.

---

## 4. Edits file by file, in commit order

**Ceilings:** no ceilinged file is touched, so none of the budgets is used.

| File | wc -l now | wc -l at `origin/slate` |
| --- | ---: | ---: |
| http.rs | 3,155 | 3,159 |
| product.rs | 4,205 | 4,205 |
| shard.rs | 3,186 | 3,196 |
| billing.rs | 2,157 | 2,201 |
| history.rs | 1,713 | 1,713 |
| auth.rs | 1,676 | 1,676 |
| registry.rs | 1,492 | 1,492 |
| sse/feed.rs | 1,165 | 1,165 |
| fleet.rs | 1,142 | 1,142 |

No touched file approaches 1,000 lines: `segmap.rs` ends at about 669 and `offsets.rs` at about 210. No DST file is touched.

**Ratcheted scopes:** none are touched.
- `offsets.rs` and `telemetry_batch.rs` have no `#[expect]`.
- In `segmap.rs`, `validate_lineage` has none. The `split` `too_many_arguments` expect (:290) and the `merge` `too_many_arguments` and `unwrap_used` expects (:353-360) keep their bodies, reasons and every fingerprinted call byte-identical. Inserting lines above them shifts position only.
- The Python files have no ratchets.

### C1: "Offset aliases, refusal words, segment-map bounds and the journal comma are pinned before their files become mutation owners" (test only)
1. `src/offsets.rs` (187 → about 209): the two tests from 3b go after L134. No new glob: the existing `use super::*` row is owners.json "unresolved-glob crate::tests src/offsets.rs". No new macro-dsl row, because both tests sit outside the `proptest!` invocation.
2. `src/segmap.rs` (628 → about 663): the 5 test edits from 3b.
3. `src/telemetry_batch.rs` (94 → about 121): the two tests from 3b, after L93.

All three changes sit inside a trailing `#[cfg(test)] mod tests`, so the planner classifies each file as `production_unchanged` and selects no mutants for this commit.

### C2: "Disjoint offset fields are added, not ORed; decode_char and validate_lineage lose the comparison no test could tell apart" (behaviour-preserving)
1. `src/offsets.rs`:
   - L37 becomes
     ```rust
         // Epoch and next occupy disjoint bits, so their sum is their concatenation.
         let n: u128 = ((epoch as u128) << 96) + ((next as u128) << 32);
     ```
   - L62 becomes `        n = (n << 5) + v as u128; // v < 32 fills the five bits the shift cleared`. `as` binds tighter than `+`, and clippy `precedence` is satisfied by the existing parentheses.
   - L81 (`'0'..='9' => Some(ch as u8 - b'0'),`) is deleted. Optionally add a doc line above `fn decode_char`: `/// Digits and letters read as their ALPHABET index; O and I/L are Crockford's only other spellings.`
   - No `match_same_arms` issue arises: the remaining arms are `Some(0)`, `Some(1)` and the lookup.
2. `src/segmap.rs` `validate_lineage` (L58-99): insert after the duplicate/self check (after L69):
   ```rust
               // Allocation order: a successor is newer and a predecessor older.
               let reversed = if successor {
                   *id <= self.seg_id
               } else {
                   *id >= self.seg_id
               };
   ```
   - L74 becomes `                None if !successor && !reversed => continue,`.
   - L82 becomes `            if reversed {`.
   - The non-strict `<=`/`>=` are deliberate. cargo-mutants rewrites `<=` only to `>` and `>=` only to `<`, both of which are killed. A strict `<` would regenerate the equivalent `<=` mutant, because equality is refused at L64.
   - Net +6 lines. Nesting is fn → for → let/if, depth 3. The function stays under 100 lines.

### C3: "The verification plan's checks are exactly the ones rust-quality gates on; loom and compiler go" (red R1 first)
1. `scripts/quality/test_verification_plan.py` (340 → about 352). Write R1 and run it red, then:
   - Imports: `import re`, and `from common import ROOT, syntax`.
   - L29: rename `…_uses_real_syntax_and_keeps_compiler_checks` to `test_visibility_selection_uses_real_syntax`, and delete L37 `assertTrue(checks['compiler'])`.
   - L67: delete `assertTrue(checks['compiler'])`.
   - L121: rename `test_pilot_benchmark_changes_select_lifecycle_and_mutations` to `test_pilot_benchmark_changes_select_mutations`, and delete L124.
   - L127: rename `test_generator_terminal_owner_selects_loom_and_mutations` to `test_generator_terminal_owner_selects_mutations`, and delete L129.
   - L133: rename `…_require_synchronization_and_mutations` to `test_touch_and_billing_state_owners_require_mutations`. Delete L138-139. At L142 change `['loom']` to `['mutants']`; `production_unchanged` omits mutation, so this stays a real negative control.
   - L151: change `['loom']` to `['mutants']` (commit_handoff is a registered owner).
   - L154: `assertTrue(plan(['src/new_owner.rs'])['compiler'])` becomes `assertFalse(plan(['src/new_owner.rs'])['mutants'])`, since an unregistered non-prefix file selects nothing.
2. `scripts/quality/test_production_changes.py` (292 → 289):
   - L16: rename `test_lint_annotations_preserve_all_compiler_checks` to `test_lint_annotations_leave_production_unchanged`, and delete L23 and L25.
   - L222: delete `assertTrue(checks['compiler'])`. The `miri` assert at L223 stays.
3. `scripts/quality/verification_plan.py` (403 → about 401):
   - Delete L82 (`lifecycle`).
   - L98-100 become
     ```python
         # Each check gates one rust-quality step. Compiler, Clippy and the
         # quality_ leg's property and Loom models run on every change.
         return {'properties_fuzz': codec or quota or tooling, 'miri': buffers or tooling,
                 'mutants': mutants, 'changed_rust_files': source,
     ```
   - Delete L114 `'compiler': True,` and L116 `'loom': True,`.
   - `LIFECYCLE_PREFIXES` stays, because it is still used in `CRITICAL_PREFIXES`.
4. `docs/RUST-QUALITY.md` (165 → about 168):
   - L132-133: "All otherwise selected compiler, property, Miri and Loom checks remain selected." becomes "All otherwise selected property-corpus and Miri checks remain selected."
   - L139-140: "do not select runtime mutation/Miri/property/Loom checks by themselves." becomes "do not select mutation, Miri or property-corpus checks by themselves."
   - New paragraph before "A scheduled bucket…" (L144): "`plan.json` selects exactly three checks, `properties_fuzz`, `miri` and `mutants`, and each gates one `rust-quality` step; a planner test refuses a check no step reads. Compiler, Clippy, the `quality_` leg (the property and Loom models) and the full suite are not selected: they run on every change."
5. `.github/workflows/rust-quality.yml`: no change.

### C4: "offsets, segmap and telemetry_batch are mutation owners" (red R2 first)
1. `scripts/quality/test_verification_plan.py`: add R2 next to `test_registered_non_prefix_source_is_selected_without_policy_duplication` (L79) and run it red.
2. `scripts/quality/mutation_owners.py` (318 → 324): after L71 (`admission_limits`):
   ```python
       # Codec and admission owners outside the critical prefixes: the row is
       # their only mutation selection; each whole file was killed at registration.
       owner('offsets', 'src/offsets.rs', 'offsets::'),
       owner('segmap', 'src/segmap.rs', 'segmap::'),
       owner('telemetry_batch', 'src/telemetry_batch.rs', 'telemetry_batch::'),
   ```
   - Every target is `service-lib`: none of these files is `#[path]`-included by `tools/quality-invariants` or `fuzz`.
   - Every filter substring matches only its own `…::tests::…` module. No other module path contains `offsets::`, `segmap::` or `telemetry_batch::`.
   - `validate_sources`, `declared_source_map` and `test_prior_table_parser_recovers_every_current_owner_without_execution` accept the literal `owner(...)` rows as they are.

**Push:** C1 through C4 go in one push on top of `aaf2baa5`, after the in-tree verification of the four current commits finishes and they are pushed. Pushing together with `2ced74c7..aaf2baa5` also works: in-diff then selects about the whole of `offsets.rs`, and all 37 of its mutants are killed (section 5).

---

## 5. Mutation analysis (cargo-mutants 27.1.0)

**Changed function bodies:**
- C1: none. Test modules only, and all three files are production-unchanged.
- C2: `offsets::encode`, `offsets::parse`, `offsets::decode_char`, `segmap::SegmentDesc::validate_lineage`.
- C3 and C4: Python only.

**CI in-diff for the item-89 push** (base `aaf2baa5`):
- `mutation_source_files = ['src/offsets.rs', 'src/segmap.rs']`, owners `['offsets', 'segmap']`.
- `telemetry_batch.rs` is `production_unchanged`.
- `tooling=True`, so `properties_fuzz` and `miri` also run.
- The selected mutants are a subset of the whole-file tables below:
  - offsets: the FnValue of encode, parse and decode_char; every operator on the new L37/L62 lines; and the `'O' | 'o'` arm next to the deleted line. That is about 17.
  - segmap: `validate_lineage`'s FnValue plus the operators, guard and arm on inserted lines. At most 18.
- Every whole-file mutant is killed, so any alignment of the hunks is safe.

**Whole-file tables** (the registration standard; section 7 confirms each locally):

`src/offsets.rs`: 37 mutants, all viable, all caught. Equivalents removed by C2: two `|`→`^` and the deleted digit arm.

| Function | Mutants | Killer |
| --- | --- | --- |
| `<impl Display for OffsetError>::fmt` | `Ok(Default::default())` | `refusal_words_are_wire_text` (C1) |
| `encode` | `String::new()`, `"xyzzy".into()` | `round_trip` (literal `encode(0,0)`, round trips) |
| `encode` | epoch `<<`→`>>` | `epoch_round_trip` (3,1) |
| `encode` | next `<<`→`>>` | `round_trip` (`(1<<33)+1`) |
| `encode` | `+`→`-` (0 - (1<<32) panics under the quality profile's overflow checks), `+`→`*` (0 for epoch 0) | `round_trip` |
| `encode` | `n << 2`→`>>`; `5*(25-i)`: `*`→`+` and `/`, `-`→`+` (shift ≥128 panics) and `/` (i=0 division panic); `padded >> shift`→`<<`; `& 31`→`\|` and `^` (all-'Z' literal) | `round_trip` |
| `parse` | `Ok((0,0))`, `Ok((0,1))`, `Ok((1,0))`, `Ok((1,1))` | `round_trip` (next 42), `epoch_round_trip` |
| `parse` | `==`→`!=` ("-1"), `!=`→`==` (length) | `round_trip` |
| `parse` | `n << 5`→`>>`; `+`→`-` (first nonzero char underflows → panic); `+`→`*` | `round_trip` |
| `parse` | `n >> 2`→`<<`; `(n >> 96)`→`<<` (epoch 0); `(n >> 32)`→`<<`; `& MASK`→`\|` (u64::MAX) and `^` | `round_trip`, `epoch_round_trip` |
| `parse_scalar` | `Ok(0)`, `Ok(1)` | `round_trip` |
| `decode_char` | `None`, `Some(0)`, `Some(1)`; closure `a == up`→`!=` | `round_trip` |
| `decode_char` | delete arm `'O' \| 'o'`; delete arm `'I' \| 'i' \| 'L' \| 'l'` | `crockford_aliases_read_as_their_digits` (C1) |

`src/telemetry_batch.rs`: 26 mutants, 25 caught and 1 unviable (`encode_prefix` → `Ok(Default::default())`, because `Selection` has no `Default`). `flush`'s body already equals its replacement, so the tool skips it. "Pinned" below is `array_delimiters_and_escaped_strings_count_toward_exact_limit`.

| Function | Mutants | Killer |
| --- | --- | --- |
| `<impl Write for BoundedWriter>::write` | `Ok(0)` (WriteZero becomes Err, and the test unwraps it), `Ok(1)` (duplicated bytes) | pinned |
| `write` | `>`→`>=` or `==` (the exact-fit final write equals the remainder), `>`→`<` (the first write overflows) | pinned |
| `encode_prefix` | `< 2`→`==` (budget 1 gives Ok) and `<=` (budget 2 gives Err) | `the_smallest_budget_frames_an_empty_array` |
| `encode_prefix` | `< 2`→`>` | pinned (large budget returns Err) |
| `encode_prefix` | `max_bytes - 2`→`+` (Oversized becomes Encoded[]) and `/` (exact fit becomes Oversized); delete `!` in `!row.overflow` (Err, unwrap); `count == 0`→`!=` | pinned |
| `encode_prefix` | `1 + from(..)`→`-` and `*`; `count > 0` (in the sum)→`<` | `a_later_row_pays_for_its_comma` (budget 8 takes two rows) |
| `encode_prefix` | `count > 0` (in the sum)→`==` and `>=` | pinned (exact fit breaks) |
| `encode_prefix` | `with_array_end > max`→`>=` and `==` | pinned |
| `encode_prefix` | `with_array_end > max`→`<` | `a_later_row_pays_for_its_comma` (budget 8, first row breaks) |
| `encode_prefix` | comma `count > 0`→`==` and `>=` | pinned (`[,` body) |
| `encode_prefix` | comma `count > 0`→`<` | `a_later_row_pays_for_its_comma` (missing comma) |
| `encode_prefix` | `count += 1`→`-=` (underflow panic) and `*=` (count stays 0) | pinned |

`src/segmap.rs`: 93 mutants, 90 caught and 3 unviable (`initial` → `Default`, `live` → `once(Box::leak(Default))`, `get` → `Some(Box::leak(Default))`). The `#[cfg(test)]` `route` is skipped. If a listing shows it anyway, its `None` is caught by `initial_routes_everything` and its `Some(..)` is unviable.

| Function | Mutants | Killer |
| --- | --- | --- |
| `contains` | `true` / `false`; `>=`→`<`; outer `&&`→`\|\|`; `<`→`==`, `>`, `<=`; `\|\|`→`&&`; `hi ==`→`!=`; `k ==`→`!=` | `initial_routes_everything`, `split_then_route_then_successors` (route(mid)) |
| `contains` | inner `&&`→`\|\|` | `split_then_route_…` route(KEYSPACE_END) is b (C1) |
| `is_live` | `true` / `false` | `split_then_route_…` (the sealed 0 would win route(mid-1)) |
| `validate_lineage` | `Ok(())`; the guard's `&&`→`\|\|`; guard→`true` | lineage test `missing_successor` |
| `validate_lineage` | delete `!` on `seen.insert`; `== self`→`!=`; reversed `<=`→`>` and `>=`→`<`; overlap `>=`→`<` ×2; backlink `&&`→`\|\|` and delete `!`; seal-authority delete `!` and `&&`→`\|\|` | `assert!(map.validate().is_ok())` |
| `validate_lineage` | dup/self `\|\|`→`&&` | lineage test `repeated` |
| `validate_lineage` | delete either `!` in the guard; guard→`false` | lineage test `historical` |
| `validate_lineage` | overlap `\|\|`→`&&` (becomes "predecessor remains live") | lineage test `disjoint` (C1) |
| `PendingTransition::validate` | `Ok(())`; delete arm "split" or "merge"; `!=`→`==`; delete either slice arm; both guards→`true` and `false`; `<=`→`>`; `\|\|`→`&&`; `>=`→`<`; `!=`→`==` ×2; `&&`→`\|\|` | `pending_transition_validation_preserves_shape_and_boundary_errors` (13 cases) |
| `live` | `iter::empty()` | `recursive_splits_keep_partition` (count 3) |
| `get` | `None`; closure `==`→`!=` | `split_then_route_…` (`get(0)` sealed_next 4242) |
| `validate` | `Ok(())` | lineage test error cases |
| `validate` | delete `!` on `ids.insert`; `>=`→`<` ×2; seal `!=`→`==`; live/successors `&&`→`\|\|` and delete `!`; first/last `!=`→`==`; delete `!` on `windows`; closure `==`→`!=` | `assert!(map.validate().is_ok())` |
| `validate` | coverage `\|\|`→`&&` ×2 | `a_keyspace_that_starts_late_…` (C1) |
| `check_partition` | `false`; `!=`→`==` ×2; closure `==`→`!=` | the `debug_assert!` in `split` (quality inherits dev), `initial_routes_everything` |
| `check_partition` | `true`; `\|\|`→`&&` | `a_keyspace_that_starts_late_…` (C1) |
| `split` | `Ok((0,0))`, `Ok((0,1))`, `Ok((1,0))`, `Ok((1,1))` | `split_then_route_…` |
| `split` | find `==`→`!=`; delete `!` on `is_live` | every split test |
| `split` | `<=`→`>`; `>=`→`<` | valid splits return Err |
| `split` | `\|\|`→`&&` | `a_split_point_on_the_parents_bounds_is_refused` (C1) |
| `split` | `+=`→`-=` and `*=` | `split_then_route_…` version 2 (C1) |
| `merge` | `Ok(0)`, `Ok(1)`; delete `!` ×2 (becomes AlreadySealed); `a_hi == b_lo`→`!=`; `b_hi == a_lo`→`!=`; find `==`→`!=` | `merge_adjacent_only` |
| `merge` | `+=`→`-=` and `*=` | `merge_adjacent_only` version 4 (C1) |

**No TIMEOUT risk.** Every loop is bounded: `0..26`, `chars()`, `take(512)`, and finite `Vec`s. `write` returning `Ok(1)` still overflows the limit, and `Ok(0)` is a WriteZero error. Each filter runs only its file's unit tests; the proptests' shrinking is bounded.

**Owner rows:** three are added, and no existing filter changes.

**Timing:** local per-mutant cost with a small filter is about 19-40 s (see `admission_limits`, `sweep_custody` in `target/quality-mutations`), and CI is roughly 4× that. The push's roughly 35 mutants come to about 45-75 min, well inside the job's 240 min.

---

## 6. Ledgers (checked; almost nothing moves)

- `scripts/quality/mutation_owners.py`: the three rows (C4).
- `docs/RUST-QUALITY.md`: the wording and contract paragraph (C3).
- `docs/refactor/test-inventory.json`: no change. The inventory is `src/dst` only (`scripts/test-inventory.py:138`).
- `docs/refactor/review-mechanisms.json`: no change. `src/telemetry_batch.rs::array_delimiters_and_escaped_strings_count_toward_exact_limit` (sha `7f0ab0f7…`) is untouched. `review-evidence.py` hashes per function and needs a unique name, and the new test names are unique.
- `docs/quality/owners.json` and `source-allowances.json`: no change. The new tests live in the existing `mod tests` modules, whose `use super::*` unresolved-glob rows already exist (offsets owners.json; segmap and telemetry_batch source-allowances :3647 and :3857). There is no new `proptest!` invocation, no new module and no new static, and no row is vacated.
- `docs/refactor/architecture-policy.json`, `docs/refactor/WIRE-MATRIX.md`, `src/dst/tests/README.md`: no change (no new modules, no wire change).
- Scenario map: no change is required. TOP-001's `split_then_route_then_successors` and `merge_adjacent_only` keep their names, and `scenario-map-report.py` checks symbols, not line numbers. Optionally, list the two new segmap tests under TOP-001.
- `docs/quality/verification.json`: no change. It is a historical receipt, and nothing reads `gate_tooling_sha256`.
- `docs/quality/adoption.md`: no change. It is a historical receipt, and it correctly says CI runs the Loom checks.

---

## 7. Controls (only after the in-tree mutation and gate runs have finished; never run a gate and a mutation leg concurrently)

Use the python3 → python3.12 shim throughout.

1. **R1 red** (tip of C2, before the C3 source edits): `python3 -m unittest discover -s scripts/quality -p test_verification_plan.py -k test_every_selected_check_gates_a_workflow_step` prints the R1 block from 3a and `FAILED (failures=1)`.
2. **R2 red** (tip of C3, before the C4 rows): `python3 -m unittest discover -s scripts/quality -p test_verification_plan.py -k test_small_codec_and_admission_owners_select_their_mutations` prints three subtest FAIL blocks and `FAILED (failures=3)`.
3. **Python suite green** (needs `cargo build --locked -p streams-quality-syntax`): `python3 -m unittest discover -s scripts/quality -v` reports `OK`, with the test count 2 higher than the same command on C2.
4. **Rust pins**, run on C1 and again on C2:
   - `scripts/test-leg.sh target/legs/item89.log --min 20 -- --locked --lib offsets:: -- segmap:: telemetry_batch::` gives `test result: ok. 20 passed; 0 failed` (offsets 7, segmap 10, telemetry_batch 3) and `TESTS_RAN_OK: target/legs/item89.log: floor 20, exact 0`.
   - On C2 also: `cargo test --locked --lib golden_layout4_raw -- http::tests::raw_position_tokens_and_fork_refusals_are_exact` gives `3 passed`.
5. **Equivalents gone** (C2): `cargo mutants --list --file src/offsets.rs --package streams-slate` lists 37 mutants, none of them `replace | with ^` or `delete match arm '0'..='9'`. `cargo mutants --list --file src/segmap.rs --package streams-slate` lists 93, with no `replace < with <=` in `validate_lineage`. `cargo mutants --list --file src/telemetry_batch.rs --package streams-slate` lists 26. Any extra listed mutant needs a killer before C4.
6. **Whole-file registration runs** (C2 tip, sequential, nothing else running). For each owner:
   ```
   cargo mutants --cargo-arg=--locked --cargo-arg=--lib --cargo-arg=--target-dir=target/owner-check/build --baseline run \
     --file src/<file>.rs --package streams-slate --cargo-test-arg=<file>:: \
     --profile quality --jobs 1 --timeout 90 --build-timeout 600 --gitignore true --output target/owner-check/<file>
   ```
   This mirrors `mutation_command` without `--in-diff`. Expected summaries (the tool prints only nonzero categories): offsets `37 mutants tested …: 37 caught`; telemetry_batch `26 mutants tested …: 25 caught, 1 unviable`; segmap `93 mutants tested …: 90 caught, 3 unviable`. A missed mutant blocks C4. Record the three counts in C4's message.
7. **Gate**: `scripts/quality.sh` ends `QUALITY_OK` (fmt, clippy `-D warnings`, the ratchet with no "accepted exception grew", rustdoc, and `review-evidence source inventory: OK; …`). Then run the usual full `scripts/gate.sh`.
8. **CI's plan before the push**: `QUALITY_EVENT_NAME=push QUALITY_HEAD_SHA=$(git rev-parse HEAD) QUALITY_BEFORE_SHA=$(git rev-parse origin/slate) QUALITY_BASE_REF=origin/slate python3 scripts/quality/verification_plan.py --out target/quality-plan` should give:
   - `mutation_source_files: ["src/offsets.rs","src/segmap.rs"]` and `selected_mutation_owners: ["offsets","segmap"]`
   - `unregistered_mutation_source_files: []` and `production_unchanged_files: ["src/telemetry_batch.rs"]`
   - `properties_fuzz`, `miri` and `mutants` all true, with no `compiler` or `loom` key.

   Then `scripts/quality/mutations.sh` with the same env should print `Mutation verification executed N selected mutant(s) across 2 registered owner(s).`, with 0 missed and 0 timeouts in each `outcomes.json`.
9. **After the push**, CI green must be confirmed per run with `gh run list --workflow rust-quality.yml --branch slate --limit 1` and `gh run view`, never assumed.

---

## 8. Out of scope

- **`src/usage.rs` registration** (the reviewer said "if runtime allows"; it does not).
  - The file is 993 lines with 33 production fns and about 61 operator sites, which puts it at about 180 mutants (float bucket arithmetic in `admit_on`, the lag and summary maps, and global statics).
  - That is about 1-2 h locally and 4-8 h on CI for a whole-file run, beyond the invariant-tools job's 240-min timeout.
  - `admit_on`'s float arithmetic needs its own equivalence audit, and the file is 7 lines from the 1,000-line ceiling.
  - Follow-up: move `admit_on`, `Limits` and `LimitHit` verbatim into `src/usage/admission.rs` and register that file (the `admission_limits` owner's `usage::runtime_tests::` filter shows the killers exist).
- **Other unregistered small owners** a sweep should consider: `src/admission.rs` (659), `src/backpressure.rs` (538), `src/touch_keys.rs` (133), `src/sketch.rs` (509).
- **Item 88 step 2** (strict decoder, Crockford-alias and wire decision) is Søren's. After item 89 it lands under the `offsets` owner, so every in-diff mutant of the new decoder must be killed. `crockford_aliases_read_as_their_digits` changes with that decision.
- **`properties_fuzz` is really "replay the postings corpus"**, so the name overstates what it selects. The scheduled rotation is dormant: `main` has no rust-quality.yml, and the existing buckets hold whole-file `shard.rs`/`http.rs` owners that no 240-min job could finish. Both are separate items.
- **The pilot-benchmark Loom model** runs only in ci.yml's full suite, not in the `--lib quality_` leg. It is covered, so no change is needed here.

## 9. Decisions for Søren

- **No edge decision.** No status, body, metric name or `/v1/debug` shape changes, and `plan.json` is an internal CI receipt with no reader of the removed keys.
- **One scope confirmation:** `src/usage.rs` stays unregistered in this item (section 8). The alternative is to register it now and accept a multi-hour whole-file audit plus about 180 mutants in any future rotation bucket.

---

## Skeptic corrections (C1..Cn)

The labels below are corrections. They are not the plan's commits C1-C4; where a commit is meant, it is written as "commit C1". Everything was re-checked read-only on `aaf2baa5`.

What holds up:
- Every `plan()`/`plan_schedule()` key site: vp.py:82, 98-100, 114, 116; test_verification_plan.py:37, 67, 124, 129, 138, 139, 142, 151, 154; test_production_changes.py:23, 25, 222.
- The only consumer is rust-quality.yml:79-94. `mutation_driver.py:105-121` reads no check key, and no doc, bench or action reads `compiler` or `loom`.
- `verification.json`'s `gate_tooling_sha256` has no reader.
- There are 110 owners. The rotation slots are offsets 2, segmap 2, telemetry_batch 5 (recomputed). The filter substrings are unique: `mod offsets`, `mod segmap` and `mod telemetry_batch` are declared only in src/lib.rs:42/59/98, with no `#[path]` includer in tools/, fuzz/ or src/bin.
- The alias tokens are 26 chars with the `I` at index 18, which gives `Ok((0, 2))`.
- The four equivalents hold:
  - offsets.rs:37 and :62 `|`→`^`
  - offsets.rs:81 deleting the digit arm, because ALPHABET[0..10] is the digits
  - segmap.rs:74 `<`→`<=`, because :64 refuses equality
- The C2 rewrites are truth-table identical, and `+` cannot overflow: epoch<<96 + next<<32 < 2^128, and (n<<5) has five clear low bits with v < 32.
- The whole-file tables add up. offsets gives 1+14+14+2+6 = 37. telemetry_batch gives 26, with 1 unviable because `Selection` has no `Default`.
- Every C1 pin traces to the stated kill. I checked:
  - the version bumps 1→2 and 1→2→3→(refused)→4
  - `disjoint`: b.lo(mid) >= a.hi(mid), and the `&&` mutant yields "predecessor remains live"
  - late start: leaves `[(1,END)]`
  - bounds: the check at :315 runs before any write
  - comma budgets 9 and 8: with_array_end 5 then 9
- `profile.quality` inherits dev (Cargo.toml:77-81), so the claimed overflow-panic and `debug_assert!` kills are real.
- No `#[expect]` scope is touched. The ratchet identity is `(path, qualified, kind, reason)` (source_rules.py:193), not the line, so shifting split/merge (:290, :353, :357) grows no metric.
- The R1 and R2 red texts match unittest's list-diff format, and each goes green as traced.

**C1: there are eight Loom models, not seven (§1a).** The plan misses `tests/pilot_membership.rs:12` `quality_loom_final_membership_acquires_all_accounting_and_prevents_late_work`. That integration test compiles `src/bin/pilot/generator/membership.rs` by `#[path]` (owners.json:1108-1114), and `mutation_driver.py:24-25` runs it for `pilot-generator`. Like the pilot benchmark model, it is not in `--lib quality_` (rust-quality.yml:48). Only ci.yml:104's `cargo test --release` runs it. The conclusion ("Loom already runs on every change") still holds. Two edits follow:
- Fix the §1a list.
- Make the new RUST-QUALITY paragraph (commit C3 step 4) exact: "Compiler, Clippy, the `--lib quality_` leg (lib property and Loom models) and ci.yml's full `cargo test --release` (which also carries the two pilot Loom models, `src/bin/pilot/benchmark/tests.rs` and `tests/pilot_membership.rs`) run on every change." As drafted, "the `quality_` leg (the property and Loom models)" wrongly implies it holds all of them.

**C2: `origin/slate` is now `aaf2baa5` (`git rev-parse origin/slate` = HEAD), so the four item-88/sweep-custody commits are already pushed.** Update three places:
- §0 line 3.
- The §4 table: its "wc -l at origin/slate" column (http.rs 3,159, shard.rs 3,196, billing.rs 2,201) is stale. Budgets now equal the current counts: http.rs 3,155; product.rs 4,205; shard.rs 3,186; billing.rs 2,157; history.rs 1,713; auth.rs 1,676; registry.rs 1,492; sse/feed.rs 1,165; fleet.rs 1,142. This item touches none of them.
- The Push paragraph. Its "pushing together with 2ced74c7..aaf2baa5" alternative is moot. The in-diff base, and control 8's `QUALITY_BEFORE_SHA`, is `aaf2baa5`.

**C3: wrong ledger citation (§4 commit C1 step 1, and §6).** The offsets `use super::*` unresolved-glob row is in `docs/quality/source-allowances.json:3563`, not owners.json. The offsets row in owners.json (:355-362) is the `macro-dsl` `crate::tests::macro(proptest::proptest)` row. It stays valid because both new tests go after L134, outside the `proptest!` at :144. The segmap and telemetry_batch glob rows (:3647 and :3857) are cited correctly.

**C4: line citation.** `test_touch_and_billing_state_owners_require_synchronization_and_mutations` is at test_verification_plan.py:**134**, not 133. The other cited lines are right: 37, 67, 121, 124, 127, 129, 138, 139, 142, 151 and 154.

**C5: which commit kills which survivor (§0, §1c, §3b).** The plan says commit C1 "adds the pins that kill the survivors". No commit-C1 test kills the HEAD survivor segmap.rs:74 guard `&&`→`||` (`!successor || *id < self.seg_id`). Every existing missing-segment case has a missing successor with id > self (`missing_successor`) or a missing predecessor with id < self (`historical`), and on those inputs the mutant and the original agree. It dies only after commit C2's restructure. Then the mutated guard `!successor || !reversed` continues past the missing successor `a` (reversed = 1 <= 0 = false), and `validate` returns "terminal segments do not exactly cover keyspace", not "segment 0 references missing segment 1".
- The §5 table is right. The narrative should say that commit C1 plus commit C2 kill every survivor.
- It should also state that this mutant is why commit C4 must not precede commit C2.

**C6: rustfmt.** `scripts/quality.sh` runs `cargo fmt --all -- --check`, and four snippet lines exceed max_width 100:
- telemetry_batch `for (max_bytes, …)` (101 cols)
- telemetry_batch `let Ok(Selection::Encoded …) = encode_prefix(events.iter(), max_bytes) else {` (103)
- segmap `assert_eq!(m.version, 2, "one transition bumps…")` (107)
- the plan's own segmap test bodies at 6-space indent

Run `cargo fmt` before committing C1 and re-take the line estimates (segmap about 663, telemetry_batch about 121). Formatting does not change any trace.

**C7: §5 in-diff list.** Rewriting offsets.rs:37 is a deletion plus an insertion, so the mutants on the adjacent L38 (`n << 2`→`>>`) are also selected. round_trip kills them. The count is about 18, not about 17. No verdict changes.

**C8: the §8 usage.rs rationale.** The 240-min argument applies only to the scheduled whole-file rotation. That rotation is dormant, because `main` lacks rust-quality.yml, and it already holds whole-file `shard.rs`/`http.rs` owners. What gates registration here is the in-diff leg. Deferring usage.rs should rest on the missing kill audit (admit_on's float arithmetic) and the 7-line ceiling headroom, not on runtime.

**C9: scenario map, optional.** If the two new segmap tests are listed under TOP-001 (docs/refactor/test-scenario-map.json:1209-1238, whose line fields are already stale and unchecked), also run `python3 scripts/scenario-map-report.py` to regenerate `docs/refactor/SCENARIO-MAP.md` in the same commit. Otherwise leave the map untouched.

**C10: overlap with item 88 step 2, noted.** plans11/offset-codec-one.md:398 assigned the `|`/`^` restructure to item 88 step 2. Commit C2 does it first. Step 2's strict decoder will rewrite `parse`/`decode_char` again under the new `offsets` owner and must replace `crockford_aliases_read_as_their_digits` per Søren's alias decision. That ordering is fine, but step 2's plan must expect every in-diff mutant of its decoder to be killed inside `offsets::`.

No control is unbuildable:
- `test-leg.sh … -- --locked --lib offsets:: -- segmap:: telemetry_batch::` gives 7+10+3 = 20.
- `golden_layout4_raw` matches exactly the two golden tests.
- `mutation_command` is mirrored, and the driver's summary string matches mutation_driver.py:157-158.
- `-k` discovery works from the repo root.

No required ledger is missed. test-inventory is src/dst only (test-inventory.py:138). No pinned review-mechanisms function changes. No global static, glob, macro, module or wire change. No doc reads the removed keys.

**Verdict: ready-with-corrections.**
