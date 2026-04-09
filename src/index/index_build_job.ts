import type { RoutingL0BuildInput, RoutingL0BuildOutput } from "./routing_l0_build";
import type { RoutingCompactionBuildInput, RoutingCompactionBuildOutput } from "./routing_compaction_build";
import type { RoutingLexiconL0BuildInput, RoutingLexiconL0BuildOutput } from "./routing_lexicon_l0_build";
import type { LexiconCompactionBuildInput, LexiconCompactionBuildOutput } from "./lexicon_compaction_build";
import type { SecondaryL0BuildInput, SecondaryL0BuildOutput } from "./secondary_l0_build";
import type { SecondaryCompactionBuildInput, SecondaryCompactionBuildOutput } from "./secondary_compaction_build";
import type { CompanionBuildInput, CompanionBuildOutput } from "../search/companion_build";
import type { CompanionMergeBuildInput, CompanionMergeBuildOutput } from "../search/companion_merge";
import type { SearchSegmentBuildInput, SearchSegmentBuildOutput } from "../search/search_segment_build";
import type { IndexBuildJobTelemetry } from "./index_build_telemetry";

export type IndexBuildJobInput =
  | { kind: "routing_l0_build"; input: RoutingL0BuildInput }
  | { kind: "routing_compaction_build"; input: RoutingCompactionBuildInput }
  | { kind: "routing_lexicon_l0_build"; input: RoutingLexiconL0BuildInput }
  | { kind: "lexicon_compaction_build"; input: LexiconCompactionBuildInput }
  | { kind: "secondary_l0_build"; input: SecondaryL0BuildInput }
  | { kind: "secondary_compaction_build"; input: SecondaryCompactionBuildInput }
  | { kind: "companion_build"; input: CompanionBuildInput }
  | { kind: "companion_merge_build"; input: CompanionMergeBuildInput }
  | { kind: "search_segment_build"; input: SearchSegmentBuildInput };

export type IndexBuildJobOutput =
  | { kind: "routing_l0_build"; output: RoutingL0BuildOutput }
  | { kind: "routing_compaction_build"; output: RoutingCompactionBuildOutput }
  | { kind: "routing_lexicon_l0_build"; output: RoutingLexiconL0BuildOutput }
  | { kind: "lexicon_compaction_build"; output: LexiconCompactionBuildOutput }
  | { kind: "secondary_l0_build"; output: SecondaryL0BuildOutput }
  | { kind: "secondary_compaction_build"; output: SecondaryCompactionBuildOutput }
  | { kind: "companion_build"; output: CompanionBuildOutput }
  | { kind: "companion_merge_build"; output: CompanionMergeBuildOutput }
  | { kind: "search_segment_build"; output: SearchSegmentBuildOutput };

export type IndexBuildJobResult = {
  output: IndexBuildJobOutput;
  telemetry: IndexBuildJobTelemetry | null;
};

export type IndexBuildWorkerError = {
  kind: "worker_pool_failure";
  message: string;
};
