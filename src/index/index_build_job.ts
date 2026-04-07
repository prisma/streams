import type { RoutingL0BuildInput, RoutingL0BuildOutput } from "./routing_l0_build";
import type { RoutingCompactionBuildInput, RoutingCompactionBuildOutput } from "./routing_compaction_build";
import type { LexiconL0BuildInput, LexiconL0BuildOutput } from "./lexicon_l0_build";
import type { LexiconCompactionBuildInput, LexiconCompactionBuildOutput } from "./lexicon_compaction_build";
import type { SecondaryL0BuildInput, SecondaryL0BuildOutput } from "./secondary_l0_build";
import type { SecondaryCompactionBuildInput, SecondaryCompactionBuildOutput } from "./secondary_compaction_build";
import type { CompanionBuildInput, CompanionBuildOutput } from "../search/companion_build";

export type IndexBuildJobInput =
  | { kind: "routing_l0_build"; input: RoutingL0BuildInput }
  | { kind: "routing_compaction_build"; input: RoutingCompactionBuildInput }
  | { kind: "lexicon_l0_build"; input: LexiconL0BuildInput }
  | { kind: "lexicon_compaction_build"; input: LexiconCompactionBuildInput }
  | { kind: "secondary_l0_build"; input: SecondaryL0BuildInput }
  | { kind: "secondary_compaction_build"; input: SecondaryCompactionBuildInput }
  | { kind: "companion_build"; input: CompanionBuildInput };

export type IndexBuildJobOutput =
  | { kind: "routing_l0_build"; output: RoutingL0BuildOutput }
  | { kind: "routing_compaction_build"; output: RoutingCompactionBuildOutput }
  | { kind: "lexicon_l0_build"; output: LexiconL0BuildOutput }
  | { kind: "lexicon_compaction_build"; output: LexiconCompactionBuildOutput }
  | { kind: "secondary_l0_build"; output: SecondaryL0BuildOutput }
  | { kind: "secondary_compaction_build"; output: SecondaryCompactionBuildOutput }
  | { kind: "companion_build"; output: CompanionBuildOutput };

export type IndexBuildWorkerError = {
  kind: "worker_pool_failure";
  message: string;
};
