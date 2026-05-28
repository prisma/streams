export const WORKSPACE_FS_PROFILE_KIND = "workspace-fs" as const;
export const WORKSPACE_FS_PROFILE_VERSION = 1 as const;

export type WorkspaceFsObjectId = string;
export type WorkspaceFsCommitId = string;
export type WorkspaceFsTreeId = string;
export type WorkspaceFsBlobId = string;
export type WorkspaceFsChunkId = string;
export type WorkspaceFsRefName = `refs/${string}` | string;

export type WorkspaceFsAuthor = {
  id: string;
  name?: string;
};

export type WorkspaceFsBlobChunkRef = {
  id: WorkspaceFsChunkId;
  offset: number;
  size: number;
  compression?: "none";
};

export type WorkspaceFsBlobManifest = {
  kind: "blob";
  id: WorkspaceFsBlobId;
  size: number;
  executable?: boolean;
  contentType?: string;
  inlineBase64?: string;
  chunks?: WorkspaceFsBlobChunkRef[];
};

export type WorkspaceFsChunkObject = {
  kind: "chunk";
  id: WorkspaceFsChunkId;
  size: number;
  dataBase64: string;
};

export type WorkspaceFsTreeEntryType = "file" | "dir" | "symlink";

export type WorkspaceFsChangeSummary = {
  added: number;
  modified: number;
  deleted: number;
  renamed: number;
};

export type WorkspaceFsCanonicalGitCommit = {
  repoStream: string;
  ref: WorkspaceFsRefName;
  oldOid: string | null;
  newOid: string;
  txnId: string;
  objectCount: number;
  bytes: number;
  durability?: WorkspaceFsCommitDurability;
};

export type WorkspaceFsCommit = {
  kind: "commit";
  id: WorkspaceFsCommitId;
  parents: WorkspaceFsCommitId[];
  rootTreeId: WorkspaceFsTreeId | null;
  author: WorkspaceFsAuthor;
  message: string;
  createdAt: string;
  workspaceId?: string;
  changeSummary?: WorkspaceFsChangeSummary;
  git?: WorkspaceFsCanonicalGitCommit;
};

export type WorkspaceFsStoredObject = WorkspaceFsBlobManifest | WorkspaceFsChunkObject;

export type WorkspaceFsNodeStat = {
  path: string;
  type: WorkspaceFsTreeEntryType;
  mode: number;
  size: number;
  mtime?: string;
  blobId?: WorkspaceFsBlobId;
  treeId?: WorkspaceFsTreeId | null;
  symlinkTarget?: string;
};

export type WorkspaceFsCheckoutRequest = {
  ref?: WorkspaceFsRefName;
  workspaceId?: string;
  ttlSeconds?: number;
};

export type WorkspaceFsCheckoutResponse = {
  repo: string;
  ref: WorkspaceFsRefName;
  workspaceId: string;
  baseCommitId: WorkspaceFsCommitId | null;
  rootTreeId: WorkspaceFsTreeId | null;
};

export type WorkspaceFsWorkspacePutFileInput = {
  kind: "put-file";
  path: string;
  contentBase64?: string;
  text?: string;
  blobId?: WorkspaceFsBlobId;
  executable?: boolean;
  contentType?: string;
};

export type WorkspaceFsWorkspaceDeleteInput = {
  kind: "delete";
  path: string;
  recursive?: boolean;
  force?: boolean;
};

export type WorkspaceFsWorkspaceMkdirInput = {
  kind: "mkdir";
  path: string;
};

export type WorkspaceFsWorkspaceRenameInput = {
  kind: "rename";
  from: string;
  to: string;
};

export type WorkspaceFsWorkspaceSymlinkInput = {
  kind: "symlink";
  target: string;
  path: string;
};

export type WorkspaceFsWorkspaceOpInput =
  | WorkspaceFsWorkspacePutFileInput
  | WorkspaceFsWorkspaceDeleteInput
  | WorkspaceFsWorkspaceMkdirInput
  | WorkspaceFsWorkspaceRenameInput
  | WorkspaceFsWorkspaceSymlinkInput;

export type WorkspaceFsWorkspacePutFileOp = {
  kind: "put-file";
  path: string;
  blobId: WorkspaceFsBlobId;
  size: number;
  executable?: boolean;
  contentType?: string;
  previousBaseBlobId?: WorkspaceFsBlobId;
  createdAt: string;
};

export type WorkspaceFsWorkspaceDeleteOp = {
  kind: "delete";
  path: string;
  recursive?: boolean;
  force?: boolean;
  previousBaseObjectId?: WorkspaceFsObjectId;
  createdAt: string;
};

export type WorkspaceFsWorkspaceMkdirOp = {
  kind: "mkdir";
  path: string;
  createdAt: string;
};

export type WorkspaceFsWorkspaceRenameOp = {
  kind: "rename";
  from: string;
  to: string;
  createdAt: string;
};

export type WorkspaceFsWorkspaceSymlinkOp = {
  kind: "symlink";
  target: string;
  path: string;
  createdAt: string;
};

export type WorkspaceFsWorkspaceMarker =
  | {
      kind: "workspace-checkout";
      ref: WorkspaceFsRefName;
      baseCommitId: WorkspaceFsCommitId | null;
      rootTreeId: WorkspaceFsTreeId | null;
      createdAt: string;
    }
  | {
      kind: "workspace-rebased";
      ref: WorkspaceFsRefName;
      oldBaseCommitId: WorkspaceFsCommitId | null;
      baseCommitId: WorkspaceFsCommitId | null;
      rootTreeId: WorkspaceFsTreeId | null;
      createdAt: string;
    }
  | {
      kind: "workspace-committed";
      commitId: WorkspaceFsCommitId;
      git?: WorkspaceFsCanonicalGitCommit;
      createdAt: string;
    }
  | {
      kind: "workspace-discarded";
      createdAt: string;
    }
  | {
      kind: "workspace-overlay-index";
      workspaceId: string;
      baseCommitId: WorkspaceFsCommitId | null;
      generation: number;
      opCount: number;
      latestPaths: string[];
      childDirs: Array<{ dir: string; names: string[] }>;
      deletedPaths: string[];
      createdAt: string;
    };

export type WorkspaceFsWorkspaceOp =
  | WorkspaceFsWorkspacePutFileOp
  | WorkspaceFsWorkspaceDeleteOp
  | WorkspaceFsWorkspaceMkdirOp
  | WorkspaceFsWorkspaceRenameOp
  | WorkspaceFsWorkspaceSymlinkOp;

export type WorkspaceFsWorkspaceRecord = WorkspaceFsWorkspaceOp | WorkspaceFsWorkspaceMarker;

export type WorkspaceFsWorkspaceOpsRequest = {
  ops: WorkspaceFsWorkspaceOpInput[];
};

export type WorkspaceFsWorkspaceOpsResponse = {
  workspaceId: string;
  appended: number;
  ops: WorkspaceFsWorkspaceOp[];
};

export type WorkspaceFsWorkspaceStatusResponse = {
  workspaceId: string;
  state: "open" | "committed" | "discarded";
  baseCommitId: WorkspaceFsCommitId | null;
  opCount: number;
  changedPaths: string[];
  lastCommitId?: WorkspaceFsCommitId;
};

export type WorkspaceFsWorkspaceIndexResponse = {
  workspaceId: string;
  baseCommitId: WorkspaceFsCommitId | null;
  generation: number;
  opCount: number;
  path: string | null;
  latest: WorkspaceFsWorkspaceOp | null;
  deleted: boolean;
  children?: string[];
  latestPaths: string[];
  childDirs: Array<{ dir: string; names: string[] }>;
  deletedPaths: string[];
};

export type WorkspaceFsWorkspaceChangesResponse = {
  workspaceId: string;
  baseCommitId: WorkspaceFsCommitId | null;
  generation: number;
  prefix: string;
  paths: string[];
};

export type WorkspaceFsWorkspaceConflictsResponse = {
  workspaceId: string;
  ref: WorkspaceFsRefName;
  baseCommitId: WorkspaceFsCommitId | null;
  currentHead: WorkspaceFsCommitId | null;
  changedPaths: string[];
  upstreamChangedPaths: string[];
  conflictPaths: string[];
  canRebase: boolean;
};

export type WorkspaceFsWorkspaceRebaseResponse = WorkspaceFsWorkspaceConflictsResponse & {
  rebased: boolean;
  oldBaseCommitId: WorkspaceFsCommitId | null;
  newBaseCommitId: WorkspaceFsCommitId | null;
  rootTreeId: WorkspaceFsTreeId | null;
};

export type WorkspaceFsCommitDurability = "accepted" | "published" | "verified";

export type WorkspaceFsCommitRequest = {
  ref?: WorkspaceFsRefName;
  expectedHead?: WorkspaceFsCommitId | null;
  message: string;
  author: WorkspaceFsAuthor;
  durability?: WorkspaceFsCommitDurability;
  durabilityTimeoutMs?: number;
};

export type WorkspaceFsCommitResponse = {
  ref: WorkspaceFsRefName;
  oldCommitId: WorkspaceFsCommitId | null;
  newCommitId: WorkspaceFsCommitId;
  commit: WorkspaceFsCommit;
  git?: WorkspaceFsCanonicalGitCommit;
};

export type WorkspaceFsStatResponse = {
  node: WorkspaceFsNodeStat;
};

export type WorkspaceFsReaddirResponse = {
  path: string;
  entries: WorkspaceFsNodeStat[];
  nextCursor: string | null;
};

export type WorkspaceFsRefResponse = {
  ref: WorkspaceFsRefName;
  commitId: WorkspaceFsCommitId | null;
};

export type WorkspaceFsLogResponse = {
  commits: WorkspaceFsCommit[];
};

export type WorkspaceFsShowResponse = {
  commit: WorkspaceFsCommit;
};

export type WorkspaceFsBatchStatRequest = {
  commit?: WorkspaceFsCommitId | null;
  workspaceId?: string;
  paths: string[];
};

export type WorkspaceFsBatchStatResponse = {
  stats: Array<{ path: string; node: WorkspaceFsNodeStat | null; error?: string }>;
};

export type WorkspaceFsBatchReadMetadataRequest = {
  ids: WorkspaceFsObjectId[];
};

export type WorkspaceFsBatchReadMetadataResponse = {
  objects: Array<{ id: WorkspaceFsObjectId; object: WorkspaceFsStoredObject | null }>;
};

export type WorkspaceFsBatchReadBlobsRequest = {
  blobIds: WorkspaceFsBlobId[];
};

export type WorkspaceFsBatchReadBlobsResponse = {
  blobs: Array<{ blobId: WorkspaceFsBlobId; contentBase64: string | null; error?: string }>;
};
