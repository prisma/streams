# State-Protocol Profile

`state-protocol` is the built-in profile for JSON streams that carry State
Protocol change records.

It owns the live `/touch/*` surface and the touch-processing semantics used for
invalidations.

## Contract

`state-protocol` means:

- stream content type must be `application/json`
- records are interpreted as State Protocol change records
- touch configuration lives in the profile document
- `/touch/*` exists only when `touch.enabled=true`

## Profile Document

Example:

```json
{
  "apiVersion": "durable.streams/profile/v1",
  "profile": {
    "kind": "state-protocol",
    "touch": {
      "enabled": true,
      "onMissingBefore": "coarse"
    }
  }
}
```

The `touch` object configures coalescing, lag degradation, template activation,
and related live invalidation behavior.

## Owned Behavior

`state-protocol` owns:

- profile validation for the touch config
- canonical change derivation for touch processing
- `/touch/meta`
- `/touch/wait`
- `/touch/templates/activate`
- seeding and cleanup of `stream_touch_state`

The core engine only provides the shared plumbing:

- durable stream storage
- worker execution
- journals
- template registry
- notifier integration

## Schema Relationship

Schemas are still optional on `state-protocol` streams.

If present, they validate the JSON payload shape and may still define
routing-key extraction. They do not own:

- touch configuration
- live invalidation behavior
- State Protocol semantics

## Implementation Rule

All State Protocol-specific behavior lives behind the profile definition under
`src/profiles/stateProtocol.ts` and `src/profiles/stateProtocol/`.

The supported extension model is:

- profile-specific behavior is implemented in the profile module
- the registry wires the profile in one place
- core runtime code dispatches through profile hooks

The core engine must not add direct `if (profile.kind === "state-protocol")`
branches for supported behavior.
