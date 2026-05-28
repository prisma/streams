export const VFS_PROFILE_KIND = "vfs-repo" as const;
export const VFS_PROFILE_VERSION = 1 as const;

export type VfsObjectId = string;
export type VfsCommitId = string;
export type VfsTreeId = string;
export type VfsBlobId = string;
export type VfsChunkId = string;
export type VfsRefName = `refs/${string}` | string;

export type VfsAuthor = {
  id: string;
  name?: string;
};

export type VfsBlobChunkRef = {
  id: VfsChunkId;
  offset: number;
  size: number;
  compression?: "none";
};

export type VfsBlobManifest = {
  kind: "blob";
  id: VfsBlobId;
  size: number;
  executable?: boolean;
  contentType?: string;
  inlineBase64?: string;
  chunks?: VfsBlobChunkRef[];
};

export type VfsChunkObject = {
  kind: "chunk";
  id: VfsChunkId;
  size: number;
  dataBase64: string;
};

export type VfsTreeEntryType = "file" | "dir" | "symlink";

export type VfsTreeEntry = {
  name: string;
  type: VfsTreeEntryType;
  mode: number;
  size?: number;
  blobId?: VfsBlobId;
  treeId?: VfsTreeId;
  symlinkTarget?: string;
  mtime?: string;
};

export type VfsTreePage = {
  kind: "tree-page";
  id: VfsTreeId;
  pathHint?: string;
  entries: VfsTreeEntry[];
  nextPageId?: VfsTreeId;
};

export type VfsTreeIndexPage = {
  kind: "tree-index-page";
  id: VfsTreeId;
  ranges: Array<{
    fromName: string;
    toName: string;
    childPageId: VfsTreeId;
  }>;
};

export type VfsChangeSummary = {
  added: number;
  modified: number;
  deleted: number;
  renamed: number;
};

export type VfsCommit = {
  kind: "commit";
  id: VfsCommitId;
  parents: VfsCommitId[];
  rootTreeId: VfsTreeId | null;
  author: VfsAuthor;
  message: string;
  createdAt: string;
  workspaceId?: string;
  changeSummary?: VfsChangeSummary;
};

export type VfsRefUpdate = {
  kind: "ref-update";
  ref: VfsRefName;
  oldCommitId: VfsCommitId | null;
  newCommitId: VfsCommitId;
  actorId: string;
  createdAt: string;
};

export type VfsStoredObject = VfsBlobManifest | VfsChunkObject | VfsTreePage | VfsTreeIndexPage | VfsCommit;

export type VfsNodeStat = {
  path: string;
  type: VfsTreeEntryType;
  mode: number;
  size: number;
  mtime?: string;
  blobId?: VfsBlobId;
  treeId?: VfsTreeId | null;
  symlinkTarget?: string;
};

export type VfsCheckoutRequest = {
  ref?: VfsRefName;
  workspaceId?: string;
  ttlSeconds?: number;
};

export type VfsCheckoutResponse = {
  repo: string;
  ref: VfsRefName;
  workspaceId: string;
  baseCommitId: VfsCommitId | null;
  rootTreeId: VfsTreeId | null;
};

export type VfsWorkspacePutFileInput = {
  kind: "put-file";
  path: string;
  contentBase64?: string;
  text?: string;
  blobId?: VfsBlobId;
  executable?: boolean;
  contentType?: string;
};

export type VfsWorkspaceDeleteInput = {
  kind: "delete";
  path: string;
  recursive?: boolean;
  force?: boolean;
};

export type VfsWorkspaceMkdirInput = {
  kind: "mkdir";
  path: string;
};

export type VfsWorkspaceRenameInput = {
  kind: "rename";
  from: string;
  to: string;
};

export type VfsWorkspaceSymlinkInput = {
  kind: "symlink";
  target: string;
  path: string;
};

export type VfsWorkspaceOpInput =
  | VfsWorkspacePutFileInput
  | VfsWorkspaceDeleteInput
  | VfsWorkspaceMkdirInput
  | VfsWorkspaceRenameInput
  | VfsWorkspaceSymlinkInput;

export type VfsWorkspacePutFileOp = {
  kind: "put-file";
  path: string;
  blobId: VfsBlobId;
  size: number;
  executable?: boolean;
  contentType?: string;
  previousBaseBlobId?: VfsBlobId;
  createdAt: string;
};

export type VfsWorkspaceDeleteOp = {
  kind: "delete";
  path: string;
  recursive?: boolean;
  force?: boolean;
  previousBaseObjectId?: VfsObjectId;
  createdAt: string;
};

export type VfsWorkspaceMkdirOp = {
  kind: "mkdir";
  path: string;
  createdAt: string;
};

export type VfsWorkspaceRenameOp = {
  kind: "rename";
  from: string;
  to: string;
  createdAt: string;
};

export type VfsWorkspaceSymlinkOp = {
  kind: "symlink";
  target: string;
  path: string;
  createdAt: string;
};

export type VfsWorkspaceMarker =
  | {
      kind: "workspace-checkout";
      ref: VfsRefName;
      baseCommitId: VfsCommitId | null;
      rootTreeId: VfsTreeId | null;
      createdAt: string;
    }
  | {
      kind: "workspace-committed";
      commitId: VfsCommitId;
      createdAt: string;
    }
  | {
      kind: "workspace-discarded";
      createdAt: string;
    };

export type VfsWorkspaceOp =
  | VfsWorkspacePutFileOp
  | VfsWorkspaceDeleteOp
  | VfsWorkspaceMkdirOp
  | VfsWorkspaceRenameOp
  | VfsWorkspaceSymlinkOp;

export type VfsWorkspaceRecord = VfsWorkspaceOp | VfsWorkspaceMarker;

export type VfsWorkspaceOpsRequest = {
  ops: VfsWorkspaceOpInput[];
};

export type VfsWorkspaceOpsResponse = {
  workspaceId: string;
  appended: number;
  ops: VfsWorkspaceOp[];
};

export type VfsWorkspaceStatusResponse = {
  workspaceId: string;
  state: "open" | "committed" | "discarded";
  baseCommitId: VfsCommitId | null;
  opCount: number;
  changedPaths: string[];
  lastCommitId?: VfsCommitId;
};

export type VfsCommitRequest = {
  ref?: VfsRefName;
  expectedHead?: VfsCommitId | null;
  message: string;
  author: VfsAuthor;
};

export type VfsCommitResponse = {
  ref: VfsRefName;
  oldCommitId: VfsCommitId | null;
  newCommitId: VfsCommitId;
  commit: VfsCommit;
};

export type VfsStatResponse = {
  node: VfsNodeStat;
};

export type VfsReaddirResponse = {
  path: string;
  entries: VfsNodeStat[];
  nextCursor: string | null;
};

export type VfsRefResponse = {
  ref: VfsRefName;
  commitId: VfsCommitId | null;
  update: VfsRefUpdate | null;
};

export type VfsLogResponse = {
  commits: VfsCommit[];
};

export type VfsShowResponse = {
  commit: VfsCommit;
};

export type VfsBatchStatRequest = {
  commit?: VfsCommitId | null;
  workspaceId?: string;
  paths: string[];
};

export type VfsBatchStatResponse = {
  stats: Array<{ path: string; node: VfsNodeStat | null; error?: string }>;
};

export type VfsBatchReadMetadataRequest = {
  ids: VfsObjectId[];
};

export type VfsBatchReadMetadataResponse = {
  objects: Array<{ id: VfsObjectId; object: VfsStoredObject | null }>;
};

export type VfsBatchReadBlobsRequest = {
  blobIds: VfsBlobId[];
};

export type VfsBatchReadBlobsResponse = {
  blobs: Array<{ blobId: VfsBlobId; contentBase64: string | null; error?: string }>;
};
