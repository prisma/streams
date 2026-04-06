# Memory Observability: State Protocol Journal Creation

This counter tracks cumulative journal creation events, useful for spotting churn from state-protocol routes that call `getOrCreateJournal`.

Observability name:
- `tieredstore.mem.leak_candidate.touch.journals.created_total`

Where implemented:
- `src/app_core.ts:585` maps this counter name.
- `src/touch/manager.ts:144` stores cumulative `journalsCreatedTotal`.
- `src/touch/manager.ts:881` increments counter on each new journal allocation.

Route call sites that trigger journal creation:
- `src/profiles/stateProtocol/routes.ts:88` activate path.
- `src/profiles/stateProtocol/routes.ts:103` meta path.
- `src/profiles/stateProtocol/routes.ts:229` wait declaration path.
- `src/profiles/stateProtocol/routes.ts:240` wait path.
