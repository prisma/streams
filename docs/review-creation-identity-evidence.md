# R05 creation identity follow-up

Creation commands, the persisted-claim plan, and product create now carry one
`TenantStreamRef`. Raw/product adapters qualify names once from their admitted
project; fork source names are qualified at the raw ingress. Claim, anchor,
initialization, readiness and cache refresh all retain those typed references.
Fresh descriptors derive both project and name from the same reference.

The application rejects a fork whose typed source and child refer to different
projects before looking up either descriptor or taking a source reference.
`r05_typed_fork_keeps_source_and_child_in_one_project` exercises that refusal,
checks that neither a foreign child nor a source pin was created, and then
proves a same-project command creates the child with the inherited record and
one durable source reference. Existing fork/creation scenarios are unchanged.

The recursive multitenancy identity scan passes for these owners; exact final
build and runtime execution receipts are external artifacts from the final
verification run.
