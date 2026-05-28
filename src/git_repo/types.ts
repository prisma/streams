export const GIT_REPO_PROFILE_KIND = "git-repo" as const;
export const GIT_REPO_PROFILE_VERSION = 1 as const;

export type GitObjectFormat = "sha1" | "sha256";
export type GitObjectType = "blob" | "tree" | "commit" | "tag";
export type GitOid = string;

export type GitRepoProfileConfig = {
  kind: typeof GIT_REPO_PROFILE_KIND;
  version: typeof GIT_REPO_PROFILE_VERSION;
  objectFormat: GitObjectFormat;
  defaultBranch: string;
  http?: {
    enabled: boolean;
    allowFetch: boolean;
    allowPush: boolean;
  };
  fetch?: {
    protocolVersion: 2;
    allowFilter: boolean;
    allowDepth: boolean;
    allowPackfileUris: boolean;
  };
  push?: {
    allowAtomic: boolean;
    allowDeleteRefs: boolean;
    maxPackBytes: number;
  };
  materialization?: {
    publishRefCheckpoint: boolean;
    targetPackSizeBytes: number;
    treeIndexPageSize: number;
  };
  importExport?: {
    enabled: boolean;
    allowLocalPathImport: boolean;
    maxBytes: number;
    gitBinary: string;
    gitCommandTimeoutMs: number;
    gitCommandConcurrency: number;
  };
};

export function defaultGitRepoProfileConfig(): GitRepoProfileConfig {
  return {
    kind: GIT_REPO_PROFILE_KIND,
    version: GIT_REPO_PROFILE_VERSION,
    objectFormat: "sha1",
    defaultBranch: "refs/heads/main",
    http: {
      enabled: false,
      allowFetch: false,
      allowPush: false,
    },
    fetch: {
      protocolVersion: 2,
      allowFilter: true,
      allowDepth: true,
      allowPackfileUris: false,
    },
    push: {
      allowAtomic: true,
      allowDeleteRefs: false,
      maxPackBytes: 512 * 1024 * 1024,
    },
    materialization: {
      publishRefCheckpoint: true,
      targetPackSizeBytes: 64 * 1024 * 1024,
      treeIndexPageSize: 512,
    },
    importExport: {
      enabled: true,
      allowLocalPathImport: false,
      maxBytes: 512 * 1024 * 1024,
      gitBinary: "git",
      gitCommandTimeoutMs: 30_000,
      gitCommandConcurrency: 2,
    },
  };
}

export type GitRepoCreatedRecord = {
  type: "repo-created";
  repoId: string;
  objectFormat: GitObjectFormat;
  defaultBranch: string;
  createdAt: string;
};

export type GitRefUpdate = {
  ref: string;
  oldOid: GitOid | null;
  newOid: GitOid | null;
};

export type GitRepoObjectSet = {
  packUri?: string;
  idxUri?: string;
  looseObjectUris?: string[];
  objectCount: number;
  bytes: number;
};

export type GitRefTransactionCommittedRecord = {
  type: "ref-transaction-committed";
  repoId: string;
  txnId: string;
  idempotencyKey?: string;
  requestHash: string;
  actor?: string;
  createdAt: string;
  refUpdates: GitRefUpdate[];
  objects?: GitRepoObjectSet;
};

export type GitRefCheckpoint = {
  repoId: string;
  generation: number;
  streamOffset: number;
  refs: Record<string, GitOid | null>;
  head: {
    symbolicRef: string;
  };
  createdAt: string;
};

export type GitTreeIndexEntry = {
  name: string;
  mode: string;
  type: "file" | "dir" | "symlink";
  oid: GitOid;
  size: number;
};

export type GitTreeIndexPage = {
  repoId: string;
  treeOid: GitOid;
  page: number;
  firstName: string;
  lastName: string;
  entries: GitTreeIndexEntry[];
  createdAt: string;
};

export type GitTreeIndexManifest = {
  repoId: string;
  treeOid: GitOid;
  entryCount: number;
  pageSize: number;
  pages: Array<{
    page: number;
    firstName: string;
    lastName: string;
    count: number;
    uri: string;
  }>;
  createdAt: string;
};

export type GitPreferredClonePack = {
  packUri: string;
  idxUri: string;
  packHash: string;
  objectCount: number;
  bytes: number;
  blobOids: GitOid[];
};

export type GitMaintenancePublishedRecord = {
  type: "maintenance-published";
  repoId: string;
  createdAt: string;
  refCheckpointUri?: string;
  packIndexManifestUri?: string;
  preferredClonePackUris?: string[];
  preferredClonePacks?: GitPreferredClonePack[];
  refCheckpoint?: GitRefCheckpoint;
};

export type GitRepoDeletedRecord = {
  type: "repo-deleted";
  repoId: string;
  createdAt: string;
};

export type GitRepoRecord =
  | GitRepoCreatedRecord
  | GitRefTransactionCommittedRecord
  | GitMaintenancePublishedRecord
  | GitRepoDeletedRecord;

export type GitRepoStatusResponse = {
  repo: string;
  profile: GitRepoProfileConfig;
  refs: Record<string, GitOid | null>;
};

export type GitRefsResponse = {
  refs: Record<string, GitOid | null>;
  checkpoint: GitRefCheckpoint | null;
};

export type GitRefResponse = {
  ref: string;
  oid: GitOid | null;
};

export type GitRefTransactionRequest = {
  txnId?: string;
  idempotencyKey?: string;
  actor?: string;
  refUpdates: GitRefUpdate[];
  objects?: GitRepoObjectSet;
};

export type GitRefTransactionResponse = {
  transaction: GitRefTransactionCommittedRecord;
  refs: Record<string, GitOid | null>;
  idempotent: boolean;
};

export type GitTransactionStatus = "accepted" | "published" | "verified";

export type GitTransactionVerification =
  | {
      status: "not_checked";
    }
  | {
      status: "verified";
    }
  | {
      status: "failed";
      message: string;
    };

export type GitTransactionStatusResponse = {
  txnId: string;
  status: GitTransactionStatus;
  verification: GitTransactionVerification;
  transaction: GitRefTransactionCommittedRecord;
  offset: string;
  publishedThrough: string;
};

export type GitPublishRefCheckpointResponse = {
  checkpoint: GitRefCheckpoint;
  record: GitMaintenancePublishedRecord;
};

export type GitPublishTreeIndexRequest = {
  treeOid?: GitOid;
  commit?: GitOid;
  ref?: string;
  path?: string;
  pageSize?: number;
};

export type GitPublishTreeIndexResponse = {
  index: GitTreeIndexManifest;
};

export type GitWriteObjectRequest = {
  type: GitObjectType;
  bodyBase64: string;
  expectedOid?: GitOid;
};

export type GitImportRequest =
  | {
      format: "bundle";
      bundleBase64: string;
      txnId?: string;
      idempotencyKey?: string;
      actor?: string;
    }
  | {
      format: "pack";
      packBase64: string;
      refs: Record<string, GitOid>;
      txnId?: string;
      idempotencyKey?: string;
      actor?: string;
    }
  | {
      format: "local-bare-repo";
      path: string;
      refs?: string[];
      txnId?: string;
      idempotencyKey?: string;
      actor?: string;
    };

export type GitImportResponse = {
  imported: {
    refs: number;
    objects: number;
    bytes: number;
  };
  transaction: GitRefTransactionCommittedRecord;
  refs: Record<string, GitOid | null>;
};

export type GitLooseObjectResponse = {
  oid: GitOid;
  type: GitObjectType;
  format: GitObjectFormat;
  size: number;
  framedSize: number;
  objectKey: string;
  etag: string;
  deduplicated: boolean;
};

export type GitNodeStat = {
  path: string;
  type: "file" | "dir" | "symlink";
  mode: string;
  oid: GitOid;
  size: number;
};

export type GitCheckoutResponse = {
  repo: string;
  ref: string;
  commitOid: GitOid | null;
  rootTreeOid: GitOid | null;
};

export type GitStatResponse = {
  commitOid: GitOid;
  node: GitNodeStat;
};

export type GitReaddirResponse = {
  commitOid: GitOid;
  path: string;
  entries: GitNodeStat[];
  nextCursor: string | null;
};
