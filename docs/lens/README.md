# durable-lens (spec + legacy Go notes)

Status: The Bun+TS server implements the lens spec in TypeScript (`src/lens/`).
This file contains **legacy Go‑specific notes** that are kept for reference.

A small Go package for applying **declarative schema-evolution lenses** to JSON
events in an immutable stream.

This repository implements the **Go runtime** pieces of “Option A”:

- a **canonical JSON** lens spec (designed to be authored by tooling, e.g. a
  TypeScript builder), and
- a **safe, deterministic** Go evaluator that applies a lens to JSON documents
  (`map[string]any`, `[]any`, scalars).

The goal is to let a product store **immutable events** forever, yet allow the
logical schema of those events to evolve — while guaranteeing that readers see
only events that match the *latest* schema.

## Why lenses?

If you store events in an immutable log (WAL segments, audit logs, chat message
streams, etc.), you quickly run into schema evolution:

- You want to add fields.
- Rename fields.
- Move fields in/out of nested objects.
- Normalize types / enums.
- Change arrays↔scalars.

In a mutable database you’d run a migration. In an immutable log, you can’t
rewrite historical data cheaply (or at all). A **lens** is a “migration for reads”:

- Writers keep appending events in whatever schema version is current at write
  time.
- Readers request the latest version.
- The system applies a chain of lenses to older events on read.

As long as each lens is safe and correct, **every event returned to a reader can
be promoted to the latest schema**.

## What this package does

### It does

- Parse the canonical JSON lens spec (see `schemas/durable-lens-v1.schema.json`).
- Apply a lens to an event:
  - **rename** fields
  - **copy** fields
  - **add** fields (with default)
  - **remove** fields
  - **hoist** (move a nested field outward)
  - **plunge** (move a field into a nested object)
  - **wrap** scalar → singleton array
  - **head** array → first element
  - **convert** using either:
    - a total mapping table (`map`), or
    - a version-stable builtin (`builtin`)
  - Scoped transforms:
    - **in**: apply ops inside a nested object
    - **map**: apply ops to each element of an array

### It does not

- Prove that a lens is semantically correct against two JSON Schemas.
  - It validates that the lens spec is well-formed and that operations are safe
    and executable.
  - “Schema proof” typically belongs in a higher-level system that owns the full
    schema registry and can run richer validation.

## Install

```bash
go get github.com/prisma/durable-streams-go/pkg/durable-lens
```

(This package currently lives inside the Durable Streams Go repo. If you
extract it into its own module, update the import path accordingly.)

## Quick start

### 1) Parse a lens

```go
import (
    lens "github.com/prisma/durable-streams-go/pkg/durable-lens"
)

raw := []byte(`{
  "apiVersion": "durable.lens/v1",
  "schema": "Task",
  "from": 1,
  "to": 2,
  "ops": [
    {"op":"rename","from":"/assignee","to":"/assignees"},
    {"op":"wrap","path":"/assignees","mode":"singleton"}
  ]
}`)

l, err := lens.Unmarshal(raw)
if err != nil {
    // invalid spec
}
```

### 2) Apply it to a JSON document

```go
var doc any
_ = json.Unmarshal([]byte(`{"assignee":"alice"}`), &doc)

out, err := l.Apply(doc)
if err != nil {
    // input didn't match expected shape for the lens
}

b, _ := json.Marshal(out)
fmt.Println(string(b))
// => {"assignees":["alice"]}
```

### 3) Or apply to raw JSON bytes

```go
outJSON, err := l.ApplyJSON([]byte(`{"assignee":"alice"}`))
```

### 4) High-performance in-place apply

If you already have a parsed JSON document and want to minimize allocations,
you can apply a lens by mutating the document in place:

```go
out, err := l.ApplyInPlace(doc)
```

This is the fast path used by Durable Streams when promoting older events on
the read path.

### 5) Compile a program for hot paths

If you apply the same lens many times, compile it into an executable program
once and reuse it:

```go
p, err := lens.CompileProgram(l)
if err != nil { /* handle */ }

out, err := p.ApplyInPlace(doc) // fastest: mutates input
```

Use `CompileProgramWithRegistry` if you need a custom builtin registry.

Other execution modes:

```go
out, err := p.Apply(doc)        // deep-copy once, then mutate the copy
outJSON, err := p.ApplyJSON(in) // unmarshal once, apply in place, marshal once
```

If you already have a chain of lenses, compile them into one program:

```go
p, err := lens.CompileProgramChain([]*lens.Lens{l1, l2, l3}, nil)
```

## Lens spec overview

A lens is a JSON object:

- `apiVersion`: currently `"durable.lens/v1"`
- `schema`: a logical name (used by your schema registry)
- `from` / `to`: source and destination schema versions (integers)
- `ops`: a list of operations, applied in order

Operations use **JSON Pointers** (RFC 6901) to address fields.

Example pointer:

- `"/user/login"` means `{ "user": { "login": ... } }`

### Operations

Below are the operations supported by the Go runtime.

#### rename
Move a value.

```json
{"op":"rename","from":"/a","to":"/b"}
```

#### copy
Copy a value.

```json
{"op":"copy","from":"/a","to":"/b"}
```

#### add
Set a field to a default value.

```json
{"op":"add","path":"/tags","schema":{"type":"array"},"default":[]}
```

If `default` is omitted, this runtime can derive a default only for simple
schemas (`type`, `default`, `const`). If it cannot derive a default it will
return an error.

#### remove
Remove a field.

```json
{"op":"remove","path":"/debug","schema":{"type":"object"}}
```

The `schema` and optional `default` are carried to support higher-level
validation / reversibility strategies, but this runtime only uses them for
structural validation.

#### hoist
Move a nested field outward.

```json
{"op":"hoist","host":"/user","name":"login","to":"/userLogin"}
```

#### plunge
Move a field into a nested object.

```json
{"op":"plunge","from":"/height","host":"/properties","name":"height","createHost":true}
```

#### wrap
Scalar → singleton array.

```json
{"op":"wrap","path":"/assignee","mode":"singleton"}
```

#### head
Array → first element.

```json
{"op":"head","path":"/labels"}
```

#### convert
Value mapping (either table or builtin).

Mapping table:

```json
{
  "op":"convert",
  "path":"/status",
  "fromType":"string",
  "toType":"string",
  "forward": {"map": {"open":"todo","closed":"done"}, "default":"todo"},
  "backward": {"map": {"todo":"open","done":"closed"}, "default":"open"}
}
```

Builtin:

```json
{
  "op":"convert",
  "path":"/name",
  "fromType":"string",
  "toType":"string",
  "forward": {"builtin":"lowercase"},
  "backward": {"builtin":"uppercase"}
}
```

Builtins provided by default:

- `lowercase`
- `uppercase`
- `string_to_int` (base-10)
- `int_to_string`
- `rfc3339_to_unix_millis`
- `unix_millis_to_rfc3339`

You can provide a custom registry (see below).

#### in
Apply operations inside a nested object.

```json
{
  "op":"in",
  "path":"/invoice",
  "ops": [
    {"op":"remove","path":"/object","schema":{"type":"string"}},
    {"op":"rename","from":"/id","to":"/invoiceId"}
  ]
}
```

Inside `in`, pointers are **relative to that nested object**.

#### map
Apply operations to each element of an array.

```json
{
  "op":"map",
  "path":"/labels",
  "ops": [
    {"op":"rename","from":"/name","to":"/category"}
  ]
}
```

Inside `map`, pointers are **relative to each element**.

## Using this in a stream product

A typical durable stream setup looks like this:

1. **Schema registry per stream**
   - Store JSON Schema for each version: `v1`, `v2`, `v3`, ...
   - Store lens specs: `v1→v2`, `v2→v3`, ...

2. **Write path**
   - Writer includes `schemaVersion` with every event (or stream metadata fixes
     it for a time window).
   - Validate the event against that version’s JSON Schema.
   - Append to the immutable log.

3. **Read path**
   - Reader asks for the “latest schema” (or a specific version).
   - Fetch events from storage.
   - If an event is older than latest:
     - apply the chain of lenses from `eventVersion` to `latestVersion`
   - Optionally validate the promoted event against the latest schema.

4. **Operational notes**
   - Cache the lens chain and/or compiled representation.
   - Keep a hard limit on chain length (you suggested 100; that’s reasonable).
   - When chains grow, consider periodically generating a “checkpoint lens”
     (e.g., squash v1→vN into a single lens) or rewriting older snapshots.

This package covers step (3): *apply a lens to a JSON event*.

## Custom builtin registry

```go
reg := lens.DefaultBuiltins().Clone()
reg.Forward["strip_prefix"] = func(v any) (any, error) {
    s, ok := v.(string)
    if !ok { return nil, fmt.Errorf("expected string") }
    return strings.TrimPrefix(s, "pre_"), nil
}

out, err := l.ApplyWithOptions(doc, lens.ApplyOptions{Registry: reg})
```

(If you expose custom builtins to end users, treat them as part of your public
contract and keep them stable across versions.)

## Semantics and safety notes

- This runtime is **strict**:
  - if a pointer doesn’t exist when an operation expects it, it errors
  - type mismatches error
  - destination pointers are overwritten when they already exist

In a stream product, that strictness is desirable because it catches:

- buggy lens specs
- corrupted events
- schema validation gaps

If you want “best-effort promotion,” you can wrap `Apply` and choose to drop
errors, but you lose the strong correctness contract.

## Testing

See [TESTING.md](./TESTING.md) for a detailed strategy and how to run tests.
