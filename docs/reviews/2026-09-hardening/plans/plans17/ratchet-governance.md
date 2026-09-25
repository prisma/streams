# Plan 17 — Exception-ratchet governance (external review §7, adopted)

Repo `/Users/sorenschmidt/code/streams`, branch `slate`, HEAD = origin/slate = `24c4c77a`.
Scope: Python gate tooling, its tests, the normative doc, one new ledger, and an audit document.
**No `.rs` file changes.** Two commits (C1 gate + docs, C2 audit).

---

## 1. Problem (verified on 24c4c77a)

### 1.1 The identity carries the prose, and a new identity is never compared

`scripts/quality/source_rules.py`

- `:116-201` `exception_contracts(sources, facts)`. For every `allow`/`expect` attribute fact with
  `'reason =' in value` (`:129-132`), it resolves the smallest enclosing parsed item as the scope
  (`:133-145`, `'crate'` if none), measures `scope_lines`/`nested_items`/`syntax_facts` (`:151-158`),
  and, per lint in the attribute, the `unwrap_*`/`expect_*` totals and fingerprints (`:159-188`) and
  `dead_code` field fingerprints (`:189-195`). Then:
  ```python
  196            identity = (path, attribute['qualified'], kind, value)
  197            if identity in contracts:
  198                contracts[identity].update(metrics)
  ```
  `value` is the whole attribute token text, for example
  `expect (clippy :: too_many_lines , reason = "…")`. Every ceiling is keyed under the reason text.
- `:204-216` `exception_growth(current, previous)`:
  ```python
  207        if identity not in previous:
  208            continue  # A new/changed reason is the explicit review decision.
  ```
  Changing any character of the reason creates a new identity, and every ceiling of that scope is
  skipped: scope lines, nested items, syntax facts, unwrap/expect totals, and the `unwrap_site:`,
  `:ordinary-call:` and `:path:` fingerprints.
- `:172-175`: this comment calls the reason edit the escape
  ("…cannot replace a benign call without an explicit new exception decision").

### 1.2 The bypass is tested, and it is written into the normative policy

- `scripts/quality/test_source_rules.py:131-134`:
  ```python
  def test_changed_reason_is_the_explicit_new_exception_decision(self):
      before = '#[expect(clippy::too_many_lines, reason = "transaction; one sequence; no split")]\nfn long() {\n' + ' let _x = 1;\n' * 101 + '}\n'
      after = before.replace('no split', 'reviewed 120-line ceiling').replace('\n}', '\n let _y = 2;\n}')
      self.assertFalse(self.growth(before, after))
  ```
- `docs/RUST-QUALITY.md:49-51` (normative) contradicts the external claim that no doc describes this
  policy. It says: "A legitimate expansion must narrow the exception or update its reason as the
  explicit reviewed decision." That sentence must change in the same commit as the gate.
- `AGENTS.md` (27 lines) points agents to RUST-QUALITY.md. It says nothing about reasons or approval.

### 1.3 A move resets the ceilings in two ways

1. The path is part of the identity (`:196`).
2. `scripts/quality/source_gate.py:37-44` fetches base versions only for paths that exist **now**:
   ```python
   39    for path in sources:
   40        result = subprocess.run(['git', 'show', f'{base}:{path}'], ...)
   42        if result.returncode == 0:
   44            prior_sources[path] = result.stdout
   ```
   - After a `git mv`, the file's base version is never loaded, so its contracts do not exist in
     `previous` at all.
   - After a partial move (`src/product.rs` → `src/product/scan.rs`, as in 0eb8a38d, df9ff212,
     f0c072b3, ca36a7f4 and d902a85b), the new file has no base version, so its moved exceptions are
     "new".

   Owners are file-relative. The extractor starts every file at `crate` (`tools/quality-syntax/src/scan.rs:31`,
   `owners: vec!["crate"]`), so a moved `fn product_scan` keeps the owner `crate::product_scan`. That
   makes matching by owner across files well defined.

### 1.4 Two more reset paths I found, the same class of bypass as the reason edit

- **Splitting or merging attributes.** Replacing `#[expect(a)]` + `#[expect(b)]` on one fn with
  `#[expect(a, b)]`, or the reverse, compiles with no code change. Under an identity keyed by lint
  *set*, which is the adopted design as literally worded, both old identities vanish and a new one
  appears, so every ceiling resets. The in-tree scan shows about 40 multi-lint attributes, and every
  fn carrying two single-lint attributes can be merged. (Red test R6.)
- **Summation slack.** `:197-198` *adds* the metrics of every attribute that shares an identity.
  Today that only happens when the reason text is identical. Once the reason is out of the identity,
  every same-lint attribute on one scope would sum. A fn-level
  `#[expect(clippy::let_underscore_must_use)]` plus a statement-level one on the same fn would double
  the ceiling. Deleting the redundant inner one (free: the outer covers it) would then leave slack
  for the fn to grow up to 2×. (Red test R7; it is already red today for identical reasons.)

### 1.5 Full use-site list

| Location | Role | Change |
| --- | --- | --- |
| `scripts/quality/source_rules.py:116-201` | contract extraction + identity | per-lint identity, dedupe, scope helper |
| `scripts/quality/source_rules.py:204-216` | growth comparison | predecessors + move + ledger |
| `scripts/quality/source_rules.py:172-175` | comment naming the escape | reword |
| `scripts/quality/source_gate.py:37-58` | base sources + call | base listing (incl. deleted files) + ledger |
| `scripts/quality/common.py:112-122` | `tracked_sources` directory rule | extract `source_directory` (shared) |
| `scripts/quality/gate.py:48-54,68-69,79` | report only (`len(contracts)` labelled "scopes") | label "contracts" |
| `scripts/quality/test_source_rules.py:4,49-53,131-134` | helper + bypass test | replace + new tests |
| `scripts/quality/test_source_gate.py:14-46,81-135` | fixtures | helper, ledger doc, 3 tests |
| `docs/RUST-QUALITY.md:41-53` | normative text | rewrite paragraph |
| `AGENTS.md:8-13` | agent pointer | add 4 lines |
| `scripts/quality.sh:16`, `scripts/architecture-gate.py:216-217`, `scripts/quality/gate.py:63`, `.github/workflows/ci.yml:55-56`, `rust-quality.yml:44` | callers / CI entry | none |
| `scripts/quality/source_rules.py:248-259` | allowance/format checks keyed on `value` | none (see §8) |
| `docs/quality/verification.json:70-86` | historical tooling hashes (2026-09-08) | none; not enforced and already stale since 99d5c098/dafce85f |

---

## 2. Contract decision

Approved by the owner (§7 adopted): the reason leaves the identity, growth needs a checked-in row
with a rationale and an approver, moves are compared, and the doc states the agent rule. Implemented
as follows.

1. **Identity = `(path, owner, scope kind, lint)`, one contract per lint.** I adopt per-lint rather
   than per-lint-set on the user's standing instruction ("use your own instinct if you learn
   something to the contrary"). A lint-set identity keeps a zero-code reset through attribute
   split/merge (§1.4). Per-lint is strictly stronger and adds no code. Adding a lint to an attribute
   still creates a *new* contract for that lint only; the other lints keep their ceilings.
   Allow/expect level is not in the identity, so `expect`→`allow` is compared, not reset. Listed as
   D1 for confirmation.
2. **One scope is measured once per identity.** Contributions are deduplicated by the scope span
   `(line, column, end_line, end_column)`. Distinct spans under one identity are still summed. Two
   inherent `impl A` blocks are the realistic case (pin R8).
3. **Moves.** A current contract absent from `previous` is compared with the **vanished** previous
   contracts that have the same `(owner, scope kind, lint)`. Vanished means in `previous` and not in
   `current`. With several origins, the comparison takes the per-metric **minimum**: deterministic
   and conservative, and the escape is a row. A copy that leaves its original in place is a new
   exception, as any new `#[expect]` is today (the existing policy at `source_rules.py:261-262`).
4. **Ledger `docs/quality/exception-growth.json`** = `{"rows": [...], "schema": 1}`.
   - Each row has exactly the keys `path`, `owner`, `scope`, `lint` (the contract), `metrics` (a
     non-empty `{metric: positive int}` holding the **new** values of the metrics that grew),
     `rationale` and `approver` (non-empty strings).
   - A row admits growth only when its `metrics` **equal** the set of grown metrics and their values.
     A row that admits nothing in the current comparison fails as unused. Rows are therefore
     consumed: the change after the growth lands removes the row, and removal needs no approval.
     This prevents a stale row from re-admitting a later identical-looking growth. Listed as D2 for
     confirmation.
   - A malformed or duplicate row raises `ValueError`, loudly, matching `from_entries`.
5. **Failure texts** (new):
   - `accepted exception grew without an approved growth row: {identity}{moved}: {metric} {before} -> {after}`,
     where `{moved}` = `" (moved from a.rs, c.rs)"` or empty.
   - `unused exception growth row (no matching growth in this comparison): {identity}`
6. **Doc rule** (RUST-QUALITY.md + AGENTS.md):
   - Reason edits are explanation updates only.
   - A coding agent may propose a row, with the gate's reported values, as a decision for the owner,
     but never adds its own approval rows.
   - A plan never re-decides a reason to absorb growth.
   - The gate checks each row's shape; it cannot check who approved it.
7. **No seeding rows.** C1/C2 change no `.rs` file, so the Rust sources at base (origin/slate) and
   HEAD are byte-identical and the contracts are identical: no growth, no rows needed. Every
   historical growth is baked into the base. The audit (C2) lists it for the owner (D6).
   Precondition: **push C1+C2 alone** (no foreign Rust commits in the same push), because the push
   comparison uses `QUALITY_BEFORE_SHA`.

Everything else stays the same: the metric definitions (scope lines include attributes, comments and
doc comments; ordinary-call/path fingerprints), the handling of new exceptions, and per-push
comparison. Those are D3–D5.

---

## 3. Red tests, pins, non-vacuity

Environment: `python3` must be ≥ 3.11. The gate imports `tomllib`; stock macOS 3.9 fails. Put a shim
first on PATH: `$SCRATCH/pyshim/python3 -> /opt/homebrew/bin/python3.12`. Build the extractor with
`cargo build --locked -p streams-quality-syntax`. Tests call `target/debug/streams-quality-syntax`.

Red procedure:
1. Commit nothing yet. Apply **only** the test-file edits below. That includes the ExceptionSiteCeilings
   helper refactor, but NOT the OwnerCeilings `base_sources` patch, which goes in with the code
   because `patch.object` on a missing attribute errors.
2. Run the red command in §7.2 against the unchanged `source_rules.py`/`source_gate.py`.

### 3.1 `scripts/quality/test_source_rules.py` (class `Rules`)

Module constants and helper, added after `fact()`:
```python
LONG = ('#[expect(clippy::too_many_lines, reason = "transaction; one sequence; no split")]\n'
        'fn long() {\n' + ' let _x = 1;\n' * 101 + '}\n')
GROWN = LONG.replace('\n}', '\n let _y = 2;\n}')
CONTRACT = "('src/a.rs', 'crate::long', 'function', 'clippy::too_many_lines')"
GREW = 'accepted exception grew without an approved growth row: '
UNUSED = 'unused exception growth row (no matching growth in this comparison): '
STRUCT = '#[expect(dead_code, reason = "wire DTO; compatibility field; no wire split")]\nstruct A { old: u8 }\n'
IMPL = ('#[expect(dead_code, reason = "wire DTO; compatibility field; no wire split")]\n'
        'impl A { fn old(&self) -> u8 { self.old } fn added(&self) -> u8 { self.added } }\n')


def row(**metrics):
    return dict(path='src/a.rs', owner='crate::long', scope='function', lint='clippy::too_many_lines',
                metrics=metrics, rationale='one transaction; a split re-reads its state',
                approver='Søren Bramer Schmidt')
```
Trace of the numbers:
- `LONG` = attribute (line 1), `fn long() {` (2), 101 statements (3-103), `}` (104).
- syn's `Spanned` joins the first token, the outer attribute's `#`, to the closing brace, so
  `scope_lines` = 104. `GROWN` = 105.
- `let _y = 2;` emits no fact: `Pat::Ident` is not a path and a literal is not a fact. So only
  `scope_lines` grows.
- The attribute's owner is `crate::long`. `visit_item_fn` enters the fn before visiting its attrs.

Helper (replaces `:49-53`). Old call sites are unchanged:
```python
def growth(self, before, after, *rows):
    old = before if isinstance(before, dict) else {'src/a.rs': before}
    new = after if isinstance(after, dict) else {'src/a.rs': after}
    return exception_growth(exception_contracts(new, syntax(new)),
                            exception_contracts(old, syntax(old)), *rows)
```

The expected red output below is on 24c4c77a's `source_rules.py`, traced through the code.

| # | Test (exact name) | Body (essence) | Red on 24c4c77a — why |
| --- | --- | --- | --- |
| R1 | `test_a_changed_reason_does_not_admit_growth` (**replaces** `test_changed_reason_is_the_explicit_new_exception_decision`) | `assertEqual(self.growth(LONG, GROWN.replace('no split', 'reviewed 120-line ceiling')), [f'{GREW}{CONTRACT}: scope_lines 104 -> 105'])` | FAIL `AssertionError: Lists differ: [] != ["accepted exception grew without an app[…]` / `Second list contains 1 additional elements.` The value differs, so `:207` `continue` returns `[]`. |
| R2 | `test_a_pure_reason_change_is_an_explanation_update` (control) | three `assertEqual(..., [])`: `LONG`→reason edited; the same fixture with the rustfmt multi-line attribute `#[expect(\n    clippy::too_many_lines,\n    reason = "…"\n)]`; the impl fixture from `:56` with `'no recovery'`→`'a poisoned feed must stop'` | passes today and after. Non-vacuity: R1 uses the same fixture plus one line and fails. |
| R3 | `test_a_verbatim_move_is_compared_with_the_exception_it_left` | `assertEqual(self.growth({'src/a.rs': LONG}, {'src/b.rs': LONG}), [])` (control), then `assertEqual(self.growth({'src/a.rs': LONG}, {'src/b.rs': GROWN}), [f"{GREW}{CONTRACT.replace('src/a.rs', 'src/b.rs')} (moved from src/a.rs): scope_lines 104 -> 105"])` | FAIL on the 2nd assert: `Lists differ: [] != [...]`. The b.rs identity is new, so it is skipped. |
| R4 | `test_a_growth_row_admits_exactly_its_recorded_growth` | (a) `growth(LONG, GROWN, [row(scope_lines=105)]) == []`; (b) `GROWN` + ` let _z = 3;` with `row(scope_lines=105)` → `[f'{GREW}{CONTRACT}: scope_lines 104 -> 106', f'{UNUSED}{CONTRACT}']`; (c) `growth(LONG, GROWN, [row(scope_lines=106)])` → `[f'{GREW}{CONTRACT}: scope_lines 104 -> 105', f'{UNUSED}{CONTRACT}']` (a row is not a ceiling); (d) `growth(LONG, LONG, [row(scope_lines=105)])` → `[f'{UNUSED}{CONTRACT}']` | ERROR `TypeError: exception_growth() takes 2 positional arguments but 3 were given` |
| R5 | `test_a_growth_row_needs_its_contract_rationale_and_approver` | For each of 9 row lists: `[missing approver]`, `[approver='']`, `[rationale='  ']`, `[lint=None]`, `[extra key note]`, `[metrics={}]`, `[metrics={'scope_lines': 0}]`, `[metrics={'scope_lines': True}]`, `[valid, valid]`, run `with self.subTest(rows=rows), self.assertRaises(ValueError): exception_growth({}, {}, rows)` | 9 subtest ERRORs, the same `TypeError` |
| R6 | `test_merging_exception_attributes_keeps_each_lints_ceiling` | body = `fn long(lock: &std::sync::Mutex<()>, value: Option<u8>) {` + `    let _guard = lock.lock().unwrap();` + 101×` let _x = 1;` + `}`; before = `#[expect(clippy::too_many_lines, reason = "owner; one sequence; no split")]` + `#[expect(clippy::unwrap_used, reason = "owner; poison invariant; no recovery")]` + body; after = `#[expect(clippy::too_many_lines, clippy::unwrap_used, reason = "owner; one sequence; no split")]` + body with the first ` let _x = 1;` → `    let _value = value.unwrap();`; `assertIn(f"{GREW}('src/a.rs', 'crate::long', 'function', 'clippy::unwrap_used'): unwrap_sites 1 -> 2", errors)` | FAIL `AssertionError: "accepted exception grew … 'clippy::unwrap_used'): unwrap_sites 1 -> 2" not found in []`. A lint-set identity would also return `[]`, so this is the per-lint discriminator. |
| R7 | `test_a_redundant_same_lint_attribute_is_not_slack` | `a = '#[expect(clippy::let_underscore_must_use, reason = "owner; result is advisory; no reader")]'`; before = `f'{a}\nfn drop_results() {{\n    {a}\n    let _ = first();\n    let _ = second();\n}}\n'`; after = `f'{a}\nfn drop_results() {{\n    let _ = first();\n    let _ = second();\n    let _ = third();\n    let _ = fourth();\n}}\n'`; `c = "('src/a.rs', 'crate::drop_results', 'function', 'clippy::let_underscore_must_use')"`; `assertEqual(errors, [f'{GREW}{c}: scope_lines 6 -> 7', f'{GREW}{c}: syntax_facts 8 -> 10'])` | FAIL `Lists differ: [] != [...]`. Today both attrs share one value, so `.update` sums to 12/2/16 before vs 7/1/10 after: no growth. Facts: 2 per attribute (meta + `expect` path via `visit_meta_list`) and 2 per call (`call-site` + `path`), so 8 before and 10 after. |
| R8 | `test_two_scopes_under_one_identity_are_both_measured` (pin) | two `#[expect(clippy::unwrap_used, reason = "owner; poison invariant; no recovery")]` `impl A { … }` blocks, each with one `.unwrap()`; after adds `self.value.unwrap();` to the 2nd; `assertTrue(any(e.endswith('unwrap_sites 2 -> 3') for e in errors), errors)` | passes today (same value → summed) and after (two spans summed). Kills the identity-only-dedupe mutant. |
| R9 | `test_a_copy_or_another_contract_elsewhere_is_a_new_exception` (pin) | subTests, each `== []`: copy `{'src/a.rs': LONG}`→`{'src/a.rs': LONG, 'src/b.rs': GROWN}`; another lint `{'src/b.rs': GROWN.replace('too_many_lines', 'excessive_nesting')}`; another scope kind `{'src/a.rs': STRUCT}`→`{'src/b.rs': IMPL}` | passes today and after. Kills the no-vanished-filter / owner-only / ignore-kind mutants. |
| R10 | `test_an_ambiguous_move_is_held_to_its_smallest_origin` | `shorter = LONG.replace(' let _x = 1;\n', '', 1)` (103 lines); `growth({'src/a.rs': LONG, 'src/c.rs': shorter}, {'src/b.rs': LONG}) == [f"{GREW}{CONTRACT.replace('src/a.rs', 'src/b.rs')} (moved from src/a.rs, src/c.rs): scope_lines 103 -> 104"]` | FAIL `Lists differ: [] != [...]` |

The existing growth tests (`:55-129`) stay unchanged. They are the refactor pins for the per-lint
extraction and `_lint_metrics`:
- `test_impl_expectation_cannot_hide_an_unrelated_optional_unwrap`
- `test_associated_panic_call_cannot_replace_an_unrelated_call`
- `test_associated_panic_aliases_and_error_variants_are_fingerprinted`
- `test_struct_dead_code_expectation_cannot_hide_a_new_field` (`'fields 1 -> 2'`, `'field_site:'`)
- `test_length_exception_has_an_independent_non_growing_size`

### 3.2 `scripts/quality/test_source_gate.py`

Add a module helper `commit_tree(root, files)`: write the files, `git init -q`, set user email/name,
`git add -f .`, `git commit -qm baseline`, and return the HEAD sha. `ExceptionSiteCeilings` gets:
- `baseline(self, root, files, rows=())`. It holds today's documents plus
  `'docs/quality/exception-growth.json': {'schema': 1, 'rows': list(rows)}`, with
  `'lines': {p: 100 for p in files}`, then calls `commit_tree`.
- `check(self, root, base, sources)`. It patches `ROOT` and `merge_base` and returns
  `source_gate.check(sources, syntax(sources))`.
- A `LONG` constant, identical to §3.1.

`test_real_source_gate_rejects_an_associated_call_replacement` is rewritten over these helpers with
the same assertions. It is a pin.

| # | Test | Body | Red on 24c4c77a |
| --- | --- | --- | --- |
| G1 | `ExceptionSiteCeilings.test_real_source_gate_compares_a_renamed_file_with_its_base` | baseline `{'src/owner.rs': BEFORE}`; unlink it, write `src/moved.rs`; `check({'src/moved.rs': BEFORE}) == []` (control); `errors = check({'src/moved.rs': AFTER})`; `assertTrue(any("'clippy::unwrap_used') (moved from src/owner.rs): unwrap_sites 1 -> 2" in e for e in errors), errors)` | FAIL `AssertionError: False is not true : []`. `git show base:src/moved.rs` fails, so `prior_sources` is empty and the growth check is skipped. |
| G2 | `ExceptionSiteCeilings.test_real_source_gate_admits_only_a_recorded_growth_row` | baseline `{'src/owner.rs': LONG}` with the row `(src/owner.rs, crate::long, function, clippy::too_many_lines, {'scope_lines': 105}, rationale, approver)`; `admitted = check({'src/owner.rs': grown})`; overwrite the ledger with `rows: []`; `refused = check(...)`; `assertEqual(admitted, [])`; `assertEqual(refused, ["accepted exception grew without an approved growth row: ('src/owner.rs', 'crate::long', 'function', 'clippy::too_many_lines'): scope_lines 104 -> 105"])` | FAIL `AssertionError: Lists differ: ['accepted exception grew without a new d[…]' != []` / `First list contains 1 additional elements.` The ledger is ignored and the old message names the value identity. |
| G3 | `BaseSources.test_base_sources_skip_directories_the_checkout_skips` | `commit_tree` with `src/a.rs`, `.agents/b.rs` (`not rust {`), `node_modules/c.rs`, `target/d.rs`, `docs/e.md`; under `patch.object(source_gate, 'ROOT', root)`: `assertEqual(source_gate.base_sources(base), ['src/a.rs'])` and `assertRaises(subprocess.CalledProcessError): source_gate.base_sources('0' * 40)` | ERROR `AttributeError: module 'source_gate' has no attribute 'base_sources'` |

`git add -f` makes sure a global gitignore cannot silently drop `target/`/`node_modules/`, which
would make G3 vacuous.

**Red totals** (`test_source_rules` 21 + `test_source_gate` 13 = 34 tests): `FAILED (failures=7, errors=11)`.
- Failures: R1, R3, R6, R7, R10, G1, G2.
- Errors: R4, R5×9, G3.

Every other test prints `ok`.

---

## 4. Edits, file by file, in commit order

No `.rs` file, DST file or ratcheted scope is touched. The ratchets (1,000-line file budget,
exception contracts, source occurrences) are computed over `.rs` sources only
(`common.tracked_sources`), so Python/Markdown sizes are outside them. No reason text is edited
anywhere. Line estimates: `source_rules.py` 267→~305, `source_gate.py` 65→~75, `common.py`
155→~160, `test_source_rules.py` 138→~255, `test_source_gate.py` 139→~215, `RUST-QUALITY.md`
172→~187, `AGENTS.md` 27→31. All stay under 1,000.

### C1 — "An exception's reason is explanation: growth under it needs an owner-approved row"

**`scripts/quality/source_rules.py`**
- Add `GROWTH_ROW_FIELDS = frozenset(('path', 'owner', 'scope', 'lint', 'metrics', 'rationale', 'approver'))`.
- Extract `exception_scopes(sources, facts)`. It moves `:125-145` verbatim and yields
  `(path, attribute, kind, location)`. Docstring: "the smallest parsed item enclosing the attribute,
  or the file; a statement-level expectation measures its item".
- Extract `_lint_metrics(lint, scoped_facts, scoped_items)`. It moves `:159-195` verbatim, with
  `if lint != name: continue` / `if lint == 'dead_code':` in place of the `in lints` tests, and
  returns a `Counter`. Reword the `:172-175` comment to "…without an approved growth row".
- `exception_contracts` becomes (docstring kept + one sentence on identity):
  ```python
  contracts, measured = {}, set()
  for path, attribute, kind, location in exception_scopes(sources, facts):
      scoped_facts = [f for f in facts[path]['facts'] if _inside(f['location'], location)]
      scoped_items = [i for i in facts[path]['items'] if _inside(i['location'], location)]
      scope = Counter({'scope_lines': location['end_line'] - location['line'] + 1,
                       'nested_items': len(scoped_items), 'syntax_facts': len(scoped_facts)})
      span = (location['line'], location['column'], location['end_line'], location['end_column'])
      for lint in sorted(_exception_lints(attribute['value'])):
          # The reason is explanation, never identity; one scope is measured
          # once per lint however many attributes on it name that lint.
          identity = (path, attribute['qualified'], kind, lint)
          if (identity, span) in measured:
              continue
          measured.add((identity, span))
          metrics = contracts.setdefault(identity, Counter())
          metrics.update(scope)
          metrics.update(_lint_metrics(lint, scoped_facts, scoped_items))
  return contracts
  ```
- `growth_rows(rows)` → `{identity: metrics}`:
  - A row is invalid unless `set(row) == GROWTH_ROW_FIELDS`, the six string fields are `str` and
    `.strip()` non-empty, `metrics` is a non-empty `dict`, and every value is `type(n) is int and n > 0`.
    An invalid row raises `ValueError(f'invalid exception growth row: {row}')`.
  - The identity is then `(path, owner, scope, lint)`. A duplicate raises the same error.
- `exception_predecessors(current, previous)` → `{identity: (before Counter, origin paths tuple)}`:
  ```python
  vanished = {}
  for identity in previous.keys() - current.keys():
      vanished.setdefault(identity[1:], []).append(identity)
  result = {}
  for identity in current:
      if identity in previous:
          result[identity] = (previous[identity], ())
      elif identity[1:] in vanished:
          origins = sorted(vanished[identity[1:]])
          floor = Counter({m: min(previous[o][m] for o in origins) for o in origins for m in previous[o]})
          result[identity] = (floor, tuple(o[0] for o in origins))
  return result  # absent: a new exception, reviewed in source
  ```
- `exception_growth(current, previous, rows=())`:
  ```python
  approved, failures, used = growth_rows(rows), [], set()
  for identity, (before, origins) in exception_predecessors(current, previous).items():
      grown = {m: n for m, n in current[identity].items() if n > before[m]}
      if approved.get(identity) == grown:
          used.add(identity)
          continue
      moved = f' (moved from {", ".join(origins)})' if origins else ''
      failures.extend(f'accepted exception grew without an approved growth row: {identity}{moved}: '
                      f'{m} {before[m]} -> {n}' for m, n in grown.items())
  failures.extend(f'unused exception growth row (no matching growth in this comparison): {identity}'
                  for identity in approved if identity not in used)
  return failures
  ```
  - No `if not grown` branch is needed. Rows have non-empty metrics, so `== {}` is never true, which
    avoids a redundant condition and its equivalent mutant.
  - py3.11: the f-strings use opposite quotes inside the replacement fields and contain no
    backslashes.

**`scripts/quality/common.py`**
- Add `EXCLUDED_DIRECTORIES = {'.git', 'target', 'node_modules', '.agents', '.cursor'}` and
  `def source_directory(name): return name not in EXCLUDED_DIRECTORIES and not name.startswith('.')`,
  with a comment: "one rule for the checkout walk and base listings".
- `tracked_sources` uses it: `subdirs[:] = sorted(d for d in subdirs if source_directory(d))`.
  This is a pure extraction; pin in §7.4.

**`scripts/quality/source_gate.py`**
- Import `source_directory`.
- Add
  ```python
  def base_sources(base):
      """Every Rust source the base tracked, including files this checkout deleted
      or renamed, so a moved exception is compared with the contract it left."""
      listing = subprocess.check_output(['git', 'ls-tree', '-r', '-z', '--name-only', base], cwd=ROOT).decode()
      return [p for p in listing.split('\0')
              if p.endswith('.rs') and all(source_directory(d) for d in p.split('/')[:-1])]
  ```
- Replace `:37-44`:
  ```python
  prior_lines, prior_sources = {}, {}
  for path in base_sources(base):
      text = subprocess.check_output(['git', 'show', f'{base}:{path}'], cwd=ROOT, text=True)
      prior_lines[path] = len(text.splitlines())
      prior_sources[path] = text
  ```
  File-growth behaviour for current paths is identical. Extra keys for deleted paths are never read
  (`violations` iterates `sources`). Both git calls fail closed.
- Inside `if prior_sources:` (`:53-58`), pass
  `json.loads((ROOT / 'docs/quality/exception-growth.json').read_text())['rows']` as the third
  argument of `rules.exception_growth`.

**`scripts/quality/gate.py`**: report label only. `:68` `'scopes'` → `'contracts'`; `:79`
"accepted exception scopes" → "accepted exception contracts".

**`scripts/quality/test_source_rules.py`**: §3.1. The import line gains nothing new; `exception_growth`
is already imported.

**`scripts/quality/test_source_gate.py`**: §3.2, plus in `OwnerCeilings.check_counts` add
`patch.object(source_gate, 'base_sources', return_value=[])` next to the `previous` patch. Those
fixtures have no git repo and a bogus base; `[]` keeps today's "no prior sources" path.

**`docs/quality/exception-growth.json`** (new, `write_json` form):
```json
{
  "rows": [],
  "schema": 1
}
```

**`docs/RUST-QUALITY.md`**: replace `:41-53` with:

> Every reasoned source exception also has a merge-base structural contract: its parsed scope size,
> nested-item count and syntax-fact multiplicity may not grow. `unwrap_used` and `expect_used`
> retain normalized direct-method and associated-call fingerprints. Lexical import aliases are
> resolved; ordinary callee and path sites under the exceptional scope are also retained
> conservatively so a local alias cannot hide a same-size replacement. `dead_code` retains exact
> field fingerprints. These are source ceilings, not reimplementations of Clippy; the pinned
> compiler fixtures remain the typed authority for method and associated-function lint truth. This
> preserves deliberate poisoned-lock failure while preventing an impl-wide expectation from silently
> covering an unrelated panic site or compatibility field.
>
> A contract is identified by its file, owning item, scope kind and one lint; the reason text is not
> part of it. Editing a reason is an explanation update only: it keeps every ceiling and never
> admits growth. Each lint of a multi-lint attribute keeps its own contract, and several attributes
> on one scope measure that scope once, so splitting, merging or deleting a redundant attribute
> neither resets nor loosens a ceiling. When a contract disappears from one file and the same owner,
> scope kind and lint appears in another in the same comparison, it has moved and is compared with
> the contract it left (with several such origins, with their smaller value for each metric).
>
> Growth is first answered by narrowing the exception, moving code out of its scope or
> restructuring. Growth that remains is admitted only by a row in
> `docs/quality/exception-growth.json` naming the contract (`path`, `owner`, `scope`, `lint`),
> exactly the grown `metrics` the gate reports (each `metric before -> after` becomes
> `"metric": after`), a `rationale` and the `approver`. A row admits exactly that growth: any other
> value fails, and a row that admits nothing in the current comparison fails as unused, so the
> change after its growth lands removes it. Rows carry the repository owner's approval. A coding
> agent may propose a row, with the gate's reported values, as a decision for the owner, but never
> adds its own approval rows, and a plan never re-decides an exception's reason to absorb growth.
> The gate checks each row's shape; it cannot check who approved it.

**`AGENTS.md`**: after `:13` add:
> An exception's `reason` is explanation only: never edit one to absorb growth. Growth under an
> existing exception needs an owner-approved row in `docs/quality/exception-growth.json`; propose
> it as a decision, never add it yourself (docs/RUST-QUALITY.md).

C1 commit message body: the §2 decisions in two sentences, the list of red tests, "no Rust change;
push alone", and the `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>` trailer.

### C2 — "The program's exception reason edits, classified by the ratchet's own contracts"

C2 is run after C1 is committed, because it uses C1's `exception_scopes`, `exception_contracts`,
`exception_predecessors` and `exception_growth`.

**`scripts/exception-audit.py`** (new, about 110 lines; outside `scripts/quality/`, so the plan
selects no tooling leg). Usage: `python3 scripts/exception-audit.py adc2cdc5 24c4c77a`. Method,
exactly:

1. **Range.** `git rev-list --first-parent --reverse adc2cdc5..24c4c77a` gives 124 commits. This
   equals the census' `--since=2026-09-21` set. The merge 7a6ecd23 is included, compared with its
   first parent. Each commit C is compared with `C^1`.
2. **Files.** Take `git diff --no-renames --name-only -z C^1 C -- '*.rs'`, keep paths whose every
   directory passes `common.source_directory`, and drop `docs/quality/syntax-fragments.json` keys.
   Load both sides with `git show` (a missing side is absent). Deletions and additions both appear,
   so moves are visible.
3. **Facts and contracts.** Facts come from `common.syntax()` (HEAD extractor build). Contracts come
   from `rules.exception_contracts` on each side. The metric definitions have been the same since
   dafce85f (2026-09-14, before the range); only the keying is C1's.
4. **Reasons and code-only size.** Use `rules.exception_scopes` on each side. For each per-lint
   identity, collect the reason texts (`value.split('reason', 1)[1].strip()`). Also collect, per
   distinct span:
   - `code_lines`: scope lines that are non-blank, not `//`-prefixed, and not covered by any
     `attribute`/`macro-attribute` fact's line range;
   - `code_facts`: scoped facts not inside any attribute fact's location.

   Sum these over the spans. This is a diagnostic for D4, labelled a heuristic.
5. **Pairing.** `rules.exception_predecessors(after, before)`. A **reason edit** is a pair whose
   reason-text set differs; for a move, compare with the origin identities' union. A **move** is a
   pair with non-empty origins.
6. **Class** per pair:
   - **scope expansion**: some metric `n > before[m]`. It is marked **(fingerprint-only)** when every
     grown key contains `:`, i.e. no size or total grew.
   - **scope reduction**: nothing grew and some metric fell, including a vanished key.
   - **explanation update**: all metrics equal.
7. **New-gate verdict.** Take `rules.exception_growth(after, before)` with no rows. The verdict is
   `row needed` if any failure starts with the growth prefix + `str(identity)`, else `pass`. The old
   gate passed all of these rows by construction.
8. **Unmatched pairs** (the residual reset paths, D3): vanished identities that are not origins,
   paired with appeared identities that have no predecessor, in the same file and with the same lint.
9. The script prints Tables 1–3 and Appendix A as Markdown.

**`docs/review/exception-audit-2026-09.md`** (new; `docs/review/` is new, and `docs/reviews/`
holds one patch file). Sections:
- **Scope and method.** The range, extractor build (`git rev-parse HEAD:tools/quality-syntax`),
  C1 sha, the exact command, class definitions, and limits: per-commit, not per-push; code-only Δ is
  a heuristic.
- **Summary.** Counts per class for reason edits and for moves; commits per worst class; how many
  would need rows now.
- **Table 1 — reason edits**, one row per commit × per-lint contract. Columns:
  `# | Commit | Subject (≤60) | Contract (path · owner · scope · lint) | Reason (→ Appendix A-n) | Lines a→b | Items a→b | Facts a→b | Sites (unwrap/expect/fields a→b or —) | Fingerprints (+new −gone or =) | Code-only Δ (lines, facts) | Class | New gate`
- **Table 2 — moves without a reason edit.** The same columns, with `Moved from` in place of `Reason`.
- **Table 3 — unmatched vanish/appear pairs.** Columns:
  `Commit | Subject | Vanished contract | Appeared contract | Kind (rename / narrowing / consolidation / unrelated — written by hand from the diff)`
  Expected to contain d2e87129, 063a674d, 33fbd10e and 58569eb7.
- **Table 4 — commit roll-up vs the external census** (13 "growth": 6faf4757 82095942 eb52b970
  527d3d3a f1c9e77e b9f3cd7c a1cf29f3 729c52ac 796211d7 a0185c3b 714abcc2 6ef3bc64 46d4b7df; 12
  "shrink": 1994fc25 5292e3e3 668bc80c 71345c03 ac9ab99e d93b421b 4bb51c0d 5c0e62d6 6515a15d
  69d8bc95 07db91a7 ed9238da; not counted: d2e87129 063a674d 33fbd10e 58569eb7 d902a85b ca36a7f4
  0eb8a38d df9ff212 f0c072b3). Columns:
  `Commit | Census | Audit (worst class) | Agrees | Note`
  Any reason-edit commit the sweep finds that the census missed is added with Census = "—".
- **Findings.** For each expansion, the commit message's stated justification. Evidence for D4 (how
  many expansions have code-only Δ ≤ 0) and for D5 (how many are fingerprint-only; how many moves
  would need rows).
- **Owner decisions (D6).** One line per expansion: accept as baked-in, or remediate.
- **Appendix A.** Full old → new reason texts, numbered to Table 1.

---

## 5. Mutation analysis

**CI leg.** No `.rs` file changes, so `cargo mutants --in-diff` has nothing to select.
`verification_plan.plan` gives `mutants: false`, `changed_rust_files: []`. The C1 paths under
`scripts/quality/` set `tooling = True` (`verification_plan.py:77`), so `properties_fuzz: true` and
`miri: true` run: a longer CI run is expected. Python has no mutation leg. The table below is the
hand disposition of every plausible mutant on the changed lines, with its killer.

| File · construct | Mutant | Killer |
| --- | --- | --- |
| `source_rules` identity | put `value` back in the identity | R1 |
| identity | key by lint set `tuple(sorted(lints))` | R6 |
| dedupe | drop `if (identity, span) in measured: continue` | R7 |
| dedupe key | `identity` without `span` | R8 |
| `metrics.update(scope)` | delete | R1, `test_length_exception…` |
| `_lint_metrics` | `lint != name` → `==` / drop the dead_code branch | `test_impl_expectation…`, `test_associated_panic…`, `test_struct_dead_code…` |
| `exception_predecessors` | `previous.keys() - current.keys()` → `previous.keys()` | R9 copy |
| predecessor key | `identity[1:]` → owner only / owner+lint | R9 another lint / another scope kind |
| predecessor floor | `min` → `max` | R10 |
| moved branch | delete (no move detection) | R3, G1 |
| `exception_growth` | `n > before[m]` → `>=` | R2, `assertFalse(self.growth(before, before))` (`:59`) |
| row match | `== grown` → `identity in approved` | R4(b) |
| row match | subset / "up to recorded" | R4(c) |
| `used.add` | delete | R4(a) |
| unused-row failures | delete | R4(d) |
| `growth_rows` | drop each clause: field-set, `.strip()`, str type, non-empty metrics, `int` (not bool), `> 0`, duplicate | R5, each subtest |
| failure text | any wording change | R1, R3, R4, R7, R10, G2 (exact equality) |
| `source_gate.base_sources` | drop `.endswith('.rs')` | G3 (`docs/e.md`); G1/G2 would parse JSON |
| `base_sources` | drop the directory filter | G3 (`.agents/b.rs` is unparseable) |
| `base_sources` | non-raising call | G3 `assertRaises(CalledProcessError)` |
| prior loop | iterate `sources` (old behaviour) | G1 |
| ledger | not passed | G2 |
| `common.source_directory` | invert / drop the `startswith('.')` | G3 |

Not killable by a test:
- `sorted(...)` in the lint loop only fixes message order, since Python set order depends on
  `PYTHONHASHSEED`. It is kept for deterministic gate output.
- The two `gate.py` label strings are report text.

`git show` of a listed base path uses `check_output`, so there is no boolean to mutate. A listed
path always exists at base, so no test is needed.

**Boundedness.** Every test uses in-memory fixtures of at most 110 lines. Each `syntax()` call is one
extractor subprocess. Git fixtures are temp repos with at most 7 files. There are no sleeps and no
network. Each test runs in under 1 s. The whole `scripts/quality` discovery run adds about 12 tests.

---

## 6. Ledgers

- **New:** `docs/quality/exception-growth.json` (empty rows).
- **Normative doc:** `docs/RUST-QUALITY.md` (C1), together with the code that enforces it.
  `AGENTS.md` pointer (C1).
- **Checked and unchanged:**
  - `docs/refactor/test-inventory.json`: Rust `src/dst` tests only.
  - `docs/refactor/review-mechanisms.json`: no `scripts/quality` pins (grep).
  - `docs/quality/owners.json` / `source-allowances.json`: no source occurrence changes.
  - `docs/refactor/architecture-policy.json`: no Rust.
  - `docs/refactor/WIRE-MATRIX.md`: no wire change.
  - `scripts/quality/mutation_owners.py`: no new critical `.rs`.
  - `docs/quality/policy.json` `immutable_sha256`: the ledger is deliberately mutable, so it is not
    added.
  - `docs/quality/verification.json` `gate_tooling_sha256`: a 2026-09-08 receipt, unenforced
    (grep), already stale.

---

## 7. Controls (exact commands, expected output)

All commands run from `/Users/sorenschmidt/code/streams` with the py3.12 shim first on PATH.
`$S` = the scratchpad.

1. **Baseline.**
   ```sh
   cargo build --locked -p streams-quality-syntax
   python3 -m unittest discover -s scripts/quality 2>&1 | tail -3
   ```
   Expect `Ran B tests` and `OK`. Record B.
2. **Red** (test edits only):
   ```sh
   (cd scripts/quality && python3 -m unittest test_source_rules test_source_gate -v) 2>&1 | tail -60
   ```
   Expect the §3 red list with the exact reasons shown, and `Ran 34 tests` /
   `FAILED (failures=7, errors=11)`.
3. **Green** (all of C1): the same command gives `Ran 34 tests` / `OK`, and
   `python3 -m unittest discover -s scripts/quality` gives `Ran B+12 tests` / `OK`.
4. **Refactor pin for `tracked_sources`.**
   ```sh
   python3 -c 'import sys; sys.path.insert(0,"scripts/quality"); from common import tracked_sources; print(len(tracked_sources()))'
   ```
   The number is identical before and after C1.
5. **The current tree stays green; no seeding rows.**
   - `git diff --stat 24c4c77a HEAD -- '*.rs'` prints nothing.
   - ```sh
     QUALITY_BASE_REF=24c4c77a python3 -c 'import sys; sys.path.insert(0,"scripts/quality"); import source_gate; print("\n".join(source_gate.check()) or "SOURCE_GATE_OK")'
     ```
     prints `SOURCE_GATE_OK`.
   - ```sh
     QUALITY_EVENT_NAME=push QUALITY_BEFORE_SHA=24c4c77a python3 scripts/architecture-gate.py --check | tail -1
     ```
     prints `architecture-gate: OK (…)`.
6. **End-to-end non-vacuity on the real tree** (scratch worktree; nothing is left in the repo):
   ```sh
   W=$S/wt-ratchet17; git worktree add --detach "$W" 24c4c77a && cd "$W"
   export QUALITY_SYNTAX=/Users/sorenschmidt/code/streams/target/debug/streams-quality-syntax QUALITY_BASE_REF=24c4c77a
   G='import sys; sys.path.insert(0,"scripts/quality"); import source_gate; print("\n".join(source_gate.check()) or "SOURCE_GATE_OK")'
   ```
   - **Probe.** In `src/application/append.rs`, `execute_once` (lines 242-423, a single
     `too_many_lines` exception, scope_lines 182), insert `    let _probe = 0;` after its unique line
     `    let mut desc = prepared.descriptor;`.
     - Old gate: `python3 -c "$G"` prints
       `accepted exception grew without a new decision: ('src/application/append.rs', 'crate::execute_once', 'function', 'expect (clippy :: too_many_lines , reason = "execute_once; one append validates, admits, commits and settles in the order the retry contract fixes; splitting it would separate the steps from the retry that orders them")'): scope_lines 182 -> 183`.
   - **Reason edit too.** Also append `; reviewed ceiling` inside that reason (the unique phrase
     `…from the retry that orders them`).
     - Old gate prints `SOURCE_GATE_OK`: **the bypass, demonstrated**.
   - **New gate.** Run `git checkout <C1> -- scripts/quality docs/quality/exception-growth.json`, then
     `python3 -c "$G"`. It prints exactly
     `accepted exception grew without an approved growth row: ('src/application/append.rs', 'crate::execute_once', 'function', 'clippy::too_many_lines'): scope_lines 182 -> 183`.
   - **With a row.** Write the row `{path, owner: crate::execute_once, scope: function, lint:
     clippy::too_many_lines, metrics: {scope_lines: 183}, rationale, approver}` into the worktree
     ledger. The gate prints `SOURCE_GATE_OK`.
   - **Probe removed, row kept.** The gate prints
     `unused exception growth row (no matching growth in this comparison): ('src/application/append.rs', 'crate::execute_once', 'function', 'clippy::too_many_lines')`.
   - **Move.** Reset the worktree ledger to `rows: []`, otherwise the append.rs row reads as unused.
     Then `git mv src/application/append.rs src/application/append_moved.rs` and re-add the probe.
     Expect exactly one line: the same growth line with the path `src/application/append_moved.rs`
     and ` (moved from src/application/append.rs)`.
     - This exercises the base listing on a real rename.
     - append.rs has no owners/allowance/architecture rows (grep: 0 each) and only this one
       exception, so no other line appears.
     - The `mod` declaration mismatch is irrelevant because the gate parses files independently.
   - **Cleanup.** `cd /Users/sorenschmidt/code/streams && git worktree remove --force "$W"`. Then
     `git status --porcelain` shows only the intended C1/C2 state.
7. **CI's own selection before the push.**
   ```sh
   QUALITY_EVENT_NAME=push QUALITY_HEAD_SHA=$(git rev-parse HEAD) QUALITY_BEFORE_SHA=$(git rev-parse origin/slate) QUALITY_BASE_REF=origin/slate python3 scripts/quality/verification_plan.py --out target/quality-plan17
   ```
   `plan.json` should have `"mutants": false`, `"properties_fuzz": true`, `"miri": true`,
   `"changed_rust_files": []`.
8. **Full gate.** `scripts/quality.sh` ends `QUALITY_OK`. `gate.py`'s line reads
   `… N accepted exception contracts; base 24c4c77a…`. N differs from the old "scopes" count:
   multi-lint attributes split, and same-lint attributes on one scope merge. That is expected.
9. **Audit (C2).**
   ```sh
   python3 scripts/exception-audit.py adc2cdc5 24c4c77a > $S/plans17/audit-tables.md
   ```
   It exits 0 and traverses all 124 commits (the script prints a final
   `<!-- commits: 124, pairs: R, reason edits: E, moves: M -->` line). Paste the tables into the
   doc, then write the Summary, Findings and Table 4 from them.
10. **Push C1+C2 alone** once `origin/slate` is still `24c4c77a` (or rebase and recheck step 5).
    Then run `gh run list --branch slate --json headSha,createdAt,name,status,conclusion`, match the
    sha, and confirm all three workflows green before claiming CI.

---

## 8. Out of scope

- Implementing D3, D4 or D5; changing the metric definitions; changing what counts as a new exception.
- The 15 `allow(clippy::result_large_err, reason = …)` rows in `source-allowances.json`. They embed
  the reason in a legacy inventory identity. Editing such a reason makes the row stale
  ("obsolete source allowances"), which is not a growth bypass.
- `violations()` `:248-259` (format/registration checks keyed on `value`).
- Per-push rather than per-commit gating. With reasons out of the identity, a later reason edit in
  the same push can no longer offset earlier growth, so the reviewer's note on that becomes moot.
- Conservative false positives, whose escape is a row:
  - an unrelated new exception with the same owner, kind and lint as a deleted one in another file
    is compared as a move;
  - two `impl A` blocks under one identity sum, so removing one block's exception gives the other
    that much slack.
- Refreshing `verification.json`'s historical tooling hashes.

## 9. Decisions for the owner

- **D1. Confirm the per-lint identity** (implemented in C1). The adopted wording keys by lint *set*.
  That leaves a zero-code reset: merge `#[expect(a)]`+`#[expect(b)]` into `#[expect(a, b)]`, or
  split them (§1.4, R6). Per-lint is strictly stronger. Say so if you prefer the set.
- **D2. Confirm row lifecycle = consumed** (implemented). A row that admits nothing fails as unused,
  so the push after an approval must delete it. Anyone may delete it, and deletion needs no approval.
  The alternative is rows bound to a base sha: no forced deletion, but a row breaks on every rebase
  and would need re-approval.
- **D3. Residual reset paths not closed** (Table 3 of the audit will show them):
  - an owner rename (fn/type renamed while it grows);
  - narrowing into a nested owner (impl-wide → one fn; a fn-level exception moved onto an extracted
    helper);
  - a move that re-wraps the owner (`fn f` → `impl X { fn f }`).

  Option: compare a new contract with a vanished same-file, same-lint contract whose owner
  *encloses* it (narrowing). A legitimate narrowing passes automatically (child ≤ parent). Renames
  stay new exceptions: a heuristic match would misfire.
- **D4. Scope size counts non-code.** `scope_lines` is the item's physical span, including the
  exception attribute itself, doc comments, comments and blank lines. `syntax_facts` counts
  attribute facts. Clippy's `too_many_lines` counts only code lines. Under the new rule, adding a
  comment or doc line inside or on an excepted item, or a reason edit that rustfmt re-wraps onto more
  lines, reads as growth and needs your row. Option: measure code lines/facts only; the extractor
  would have to report the item's first non-attribute token or comment-free line coverage. The
  audit's code-only Δ column measures how often this happened.
- **D5. Fingerprint breadth under `unwrap_used`/`expect_used`.** Every call and path under the scope
  is fingerprinted with its resolved callee (RUST-QUALITY `:44-47`). So:
  - any edit to any call's arguments counts as growth;
  - a verbatim move of a **callee** counts as growth in every ratcheted caller (the import alias
    resolution changes the fingerprint, as with product_entry after product_scan moved);
  - a verbatim move of the excepted fn itself counts as growth (`perr` → `super::perr`).

  "Move code out of it", the sanctioned remedy, therefore needs your rows for code under about 320
  such attributes (264 `unwrap_used`, 55 `expect_used`, plus multi-lint ones). Option: fingerprint only sites whose resolved callee or path ends in a panicking method
  (`unwrap`/`unwrap_err`/`expect`/`expect_err`), keeping import-alias resolution. That still covers
  every Clippy-lintable site and `let f = Option::unwrap` aliases, and retires the `ordinary-call`
  and full `path` families. It changes normative text. The audit's "fingerprint-only" count sizes
  it. Recommend deciding D4/D5 soon after C1, since they set how many rows you will be asked for.
- **D6. Historical expansions** (the audit's Table 1/2 "scope expansion" rows) are baked into the
  base and pass. For each one: accept, or schedule remediation.
- **D7. Approval authenticity.** The gate can only check a row's shape. The rule relies on review of
  commits that touch `docs/quality/exception-growth.json`. A mechanical check is possible, for
  example refusing rows added in a commit that carries an AI `Co-Authored-By` trailer. I do not
  recommend it: it is brittle and easy to bypass.

---

## Appendix — `scripts/exception-audit.py` core (C2)

The set equality `git rev-list --since=2026-09-21 origin/slate` == `git rev-list --first-parent
adc2cdc5..24c4c77a` was checked: it is the same 124 commits.

```python
#!/usr/bin/env python3
"""Classify exception reason edits and moves on a first-parent range with the source ratchet's own contracts."""
import json, subprocess, sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent / 'quality'))
from common import ROOT, source_directory, syntax  # noqa: E402
import source_rules as rules  # noqa: E402

GREW = 'accepted exception grew without an approved growth row: '

def git(*args):
    return subprocess.check_output(['git', *args], cwd=ROOT).decode()

def load(revision, paths):
    shown = {p: subprocess.run(['git', 'show', f'{revision}:{p}'], cwd=ROOT, capture_output=True) for p in paths}
    return {p: r.stdout.decode() for p, r in shown.items() if r.returncode == 0}

def changed(parent, commit):
    fragments = json.loads((ROOT / 'docs/quality/syntax-fragments.json').read_text())
    names = git('diff', '--no-renames', '--name-only', '-z', parent, commit, '--', '*.rs').split('\0')
    return [p for p in names if p and p not in fragments and all(source_directory(d) for d in p.split('/')[:-1])]

def scopes(sources, facts):
    """identity -> (reason texts, (code lines, code facts)) summed over distinct spans."""
    found = {}
    for path, attribute, kind, location in rules.exception_scopes(sources, facts):
        inside = [f for f in facts[path]['facts'] if rules._inside(f['location'], location)]
        attrs = [f['location'] for f in inside if f['kind'] in ('attribute', 'macro-attribute')]
        covered = {n for a in attrs for n in range(a['line'], a['end_line'] + 1)}
        text = sources[path].splitlines()[location['line'] - 1:location['end_line']]
        code_lines = sum(1 for n, t in enumerate(text, location['line'])
                         if n not in covered and t.strip() and not t.strip().startswith('//'))
        code_facts = sum(1 for f in inside if not any(rules._inside(f['location'], a) for a in attrs))
        span = (location['line'], location['column'], location['end_line'], location['end_column'])
        for lint in rules._exception_lints(attribute['value']):
            reasons, spans = found.setdefault((path, attribute['qualified'], kind, lint), ({}, {}))
            reasons[attribute['value'].split('reason', 1)[1].strip()] = None
            spans[span] = (code_lines, code_facts)
    return {i: (set(r), tuple(map(sum, zip(*s.values())))) for i, (r, s) in found.items()}

def classify(prior, metrics):
    grown = [m for m, n in metrics.items() if n > prior[m]]
    if grown:
        return 'scope expansion (fingerprint-only)' if all(':' in m for m in grown) else 'scope expansion'
    return 'scope reduction' if any(metrics[m] < n for m, n in prior.items()) else 'explanation update'

def audit(start, end):
    edits, moves, unmatched, commits = [], [], [], 0
    for commit in git('rev-list', '--first-parent', '--reverse', f'{start}..{end}').split():
        commits += 1
        paths = changed(f'{commit}^1', commit)
        if not paths:
            continue
        before, after = load(f'{commit}^1', paths), load(commit, paths)
        fb, fa = syntax(before), syntax(after)
        old, new = rules.exception_contracts(before, fb), rules.exception_contracts(after, fa)
        sb, sa = scopes(before, fb), scopes(after, fa)
        failing = rules.exception_growth(new, old)
        pairs = rules.exception_predecessors(new, old)
        for identity, (prior, origins) in pairs.items():
            sources = [(o,) + identity[1:] for o in origins] or [identity]
            reasons_before = set().union(*(sb[s][0] for s in sources))
            code_before = tuple(map(sum, zip(*(sb[s][1] for s in sources))))
            reasons_after, code_after = sa[identity]
            if reasons_before == reasons_after and not origins:
                continue
            row = dict(commit=commit[:8], identity=identity, origins=origins, prior=prior,
                       metrics=new[identity], reasons=(sorted(reasons_before), sorted(reasons_after)),
                       code=(code_before, code_after), cls=classify(prior, new[identity]),
                       gate='row needed' if any(f.startswith(f'{GREW}{identity}') for f in failing) else 'pass')
            (edits if reasons_before != reasons_after else moves).append(row)
        matched = {(o,) + i[1:] for i, (_, origins) in pairs.items() for o in origins}
        vanished = [i for i in old if i not in new and i not in matched]
        appeared = [i for i in new if i not in pairs]
        unmatched += [(commit[:8], v, a) for v in vanished for a in appeared if v[0] == a[0] and v[3] == a[3]]
    return commits, edits, moves, unmatched
```

`main()` renders Table 1 (`edits`), Table 2 (`moves`), Table 3 (`unmatched`) and Appendix A
(`reasons`) in the §4 C2 column order:
- metric cells use `a→b`;
- Sites cover `unwrap_sites`/`expect_sites`/`fields`;
- Fingerprints count keys containing `:` as `+grown −fell`;
- Code-only Δ is `after − before`.

The final line is `<!-- commits: N, pairs: …, reason edits: E, moves: M -->`. Subjects come from
`git log -1 --format=%s <commit>`, truncated to 60 characters.

---

## Skeptic corrections (C1..C9)

Checked against 24c4c77a with Read, grep, git show and wc only. Nothing was run. What holds up:
- Every quote and line citation in §1: `source_rules.py:116-201,196-200,204-216,172-175,261-262`, `source_gate.py:37-44,53-58`, `test_source_rules.py:49-53,131-134`, `RUST-QUALITY.md:41-53`, `scan.rs:31`, `common.py:112-122`, `gate.py:48-54,68,79`.
- The use-site list is complete. `git grep` finds `exception_growth`/`exception_contracts`/`source_gate` only in `gate.py`, `architecture-gate.py:216-217` and the two test files.
- The fact traces for R1/R6/R7/R8/R10 are right:
  - `attributes.rs::record` gives `attribute` + `path` per attribute, so 2 facts.
  - `visit_expr_call` gives `call-site` + `path` per call, so 2 facts.
  - Item spans include outer attributes (`span-locations`), so `LONG` = 104.
- Red totals: `test_source_rules` has 12 tests today (21 after), `test_source_gate` has 10 (13 after). `failures=7, errors=11` holds, counting the 9 R5 subtest errors.
- The range adc2cdc5..24c4c77a is 124 first-parent commits.
- No tooling change since dafce85f.
- append.rs is 423 lines, has one exception (lines 242-423 → scope_lines 182), and has no allowance, owner or architecture rows.
- `verification.json` hashes are unenforced (no reader in scripts/ or .github/).
- No reason text is edited, and no `.rs`/DST file or ratcheted scope is touched.

**C1 (blocking design defect; replaces the §2.4 / D2 "consumed rows" lifecycle).** A row that admits nothing fails as `unused`. That breaks every comparison that is not "exactly the push that landed the growth":
- **Local runs right after the push.** With no event, `common.py:20-35` sets `merge_base()` = merge-base(HEAD, origin/slate) = HEAD. There is no growth, but the row is unused, so `scripts/quality.sh`/`gate.sh` fail on every unrelated change until a separate commit deletes the row.
- **One push carrying both the growth commit and the deletion commit.** The push compares `QUALITY_BEFORE_SHA`..HEAD, sees growth with no row, and fails. The deletion has to go in a later push. That is fragile, and the plan does not document it.
- **Any comparison spanning several pushes.** `ci.yml:3-6` / `rust-quality.yml:2-5` run `push: [main, slate]` and `pull_request`. The slate→main promotion (main is 1,371 commits behind; merge base 8e3aa50b) compares the whole range, and by then every consumed row is gone. So every approved growth fails again.

Fix: make rows **exact-state** instead of consumed.
- A row persists while the contract's current value of each row metric **equals** the row's value.
- Growth is admitted iff `all(row.get(m) == n for m, n in grown.items())`.
- A row fails as `stale exception growth row (contract no longer has these values): {identity}` when the identity is absent from `current`, or when any row metric differs from `current[identity][m]`.
- There is no "unused" failure. A row that equals the current state while nothing grew in this comparison (the post-push local case, the promotion case) passes.
- Re-admission stays closed: after a shrink the row is stale and must be removed or lowered, and any later growth mismatches the row again.

Test changes:
- R4(c) expects `[GREW…104 -> 105, STALE…]`; R4(d) (LONG→LONG with row 105) expects `[STALE…]` (104 ≠ 105).
- Add R4(e): `growth(GROWN, GROWN, [row(scope_lines=105)]) == []`. This is the post-push case, red today with the same `TypeError`.
- The §7.6 "probe removed, row kept" step then expects the stale line.
- The mutant table needs kills for the stale check: drop the absent-identity clause (R4 with a row naming `crate::other` → stale), and `!=` → `<` (R4(d) inverted: row 103 vs current 104).
- Rewrite D2 as "confirm exact-state rows", and keep the consumed and base-sha variants as the rejected alternatives, with the reasons above.

**C2 (missing owner decision D8; promotion).** Taking the reason out of the identity makes any old-base comparison re-litigate all historical growth. The next slate→main PR or push compares with 8e3aa50b and reports every growth since July with no rows. The old gate passed most of it only because the reason texts differed. §2.7 "no seeding rows" is true only for the `QUALITY_BEFORE_SHA`=24c4c77a push. Add D8: before any promotion, either seed exact-state rows from the audit (a C2-style run against the promotion base) or define a promotion baseline. Outside that decision, do not touch main.

**C3 (governance gap; the doc rule).** D3's owner rename is a zero-cost reset of exactly the same class as the reason edit. Examples: rename `execute_once` → `execute_once_steps`, or re-attach the attribute to an extracted helper. Both go through `source_rules.py:261-264` ("a new narrow reasoned expectation is reviewed in-source"). The proposed RUST-QUALITY/AGENTS text forbids only reason re-decisions.
- Extend both texts: "never rename, re-wrap, split or re-attach an exception's owner, or move code into a new exception, to absorb growth. A new exception on code that left an existing exception's scope is growth: propose a row."
- In D3, recommend the cheap mechanical close alongside the doc rule. When exactly one vanished and one appeared contract share path, scope kind and lint, treat them as a predecessor pair; the false-positive escape is a row, the same conservative stance §8 already accepts for cross-file moves. Table 3 of the audit sizes it.

**C4 (pre-existing dirt confounds the controls).**
- `git status` on 24c4c77a shows uncommitted in-flight work: `M docs/quality/owners.json`, `M src/tasks/refusal.rs` (+63/−?), `M src/tasks/tests.rs` (+186).
- Controls 1, 3, 4, 5 and 8 run `tracked_sources()`/`source_gate.check()`/`quality.sh` on the working tree, so they would measure that foreign work. With the dirty `owners.json`, `SOURCE_GATE_OK` in step 5 is not a statement about C1.
- Run every control in a clean worktree at 24c4c77a (+C1/C2), as step 6 already does. Stage C1/C2 by explicit path, never `-a`.
- Step 6's cleanup expectation ("`git status --porcelain` shows only the intended C1/C2 state") is false while that dirt exists. Say "unchanged from before, apart from C1/C2".

**C5 (G3 does not kill the `startswith('.')` mutant).**
- `EXCLUDED_DIRECTORIES` already contains `.agents` (`common.py:114`). The fixture `.agents/b.rs` stays excluded when `and not name.startswith('.')` is deleted, so the §5 row "`source_directory` invert / drop the `startswith('.')` → G3" is wrong for the drop.
- Add `.hidden/g.rs` (a hidden directory outside the set) to the G3 tree. Keep `target/d.rs`/`node_modules/c.rs`: they kill the set-membership drop.

**C6 (base listing crashes on a deleted unparsed template).**
- `base_sources` lists every base `.rs`, including `docs/quality/syntax-fragments.json` templates (three `scripts/read-experiments/followup/*.rs` placeholder files).
- If a change deletes such a file **and** its fragments entry, `syntax(prior_sources)` feeds placeholder source to the extractor. `main.rs` returns the first `syn` error, so `check=True` raises `CalledProcessError`: the gate crashes instead of reporting.
- Today this cannot happen, because only current paths are loaded.
- Fix: drop from `base_sources`' result any path listed in the **base** `syntax-fragments.json` (`git show base:docs/quality/syntax-fragments.json`), and pin it in G3 with a fragment fixture. Alternatively, list the gap in §8.

**C7 (R2 is ambiguous and can go red after C1).**
- The "rustfmt multi-line attribute" case must put the multi-line form on **both** sides, with only the reason text differing.
- Single-line before → multi-line after adds 3 physical lines, so scope_lines goes 104 → 107 (D4's own point), and R2 would fail on green.
- Spell out both sides.

**C8 (cross-plan sequencing and report semantics).**
- The sibling `plans17/tracker-race.md:535` control quotes the old text `accepted exception grew without a new decision:`. If this plan lands first, that control silently never matches. List it in §6 as a dependent update: the sibling must use the new prefix.
- `gate.py:49-54` `source_metrics` now counts `scope_lines`/`syntax_facts` once per lint (a multi-lint attribute counts twice). The label change alone understates this. Note it next to step 8.

**C9 (placement and minor points).**
- Put the audit in `docs/quality/exception-audit-2026-09.md`, next to the other quality governance records (`docs/quality/pr19-merge-review.md`), not in a new `docs/review/` beside the existing `docs/reviews/`.
- `growth_rows` should also require `str` metric names.
- `growth_rows` should check `schema == 1` when it loads the ledger.
- The ledger should be validated even when `prior_sources` is empty, so that a malformed ledger is not hidden on a no-base run. Any read must stay fail-closed.

Verdict: **ready-with-corrections.** The identity change, the per-lint choice (D1), the span dedupe, move detection, the red tests and the controls are sound and traced correctly. C1 (row lifecycle) must be applied before implementation because the consumed lifecycle breaks local, multi-commit-push and promotion comparisons. C3's doc extension is needed for the governance rule to mean what the reviewer asked. C2 goes to the owner.
