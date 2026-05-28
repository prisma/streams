export {
  VfsClientError,
  VfsRepoClient as WorkspaceFsClient,
  VfsWorkspace as WorkspaceFsWorkspace,
  openVfsRepo as openWorkspaceFsRepo,
  vfsRepoStreamName as workspaceFsStreamName,
  type CheckoutOptions as WorkspaceCheckoutOptions,
  type OpenVfsRepoOptions as OpenWorkspaceFsRepoOptions,
  type VfsFetch as WorkspaceFsFetch,
  type VfsWorkspaceCommitOptions as WorkspaceCommitOptions,
} from "../vfs/client";
export { PrismaStreamsVfsFs as PrismaStreamsWorkspaceFs } from "../vfs/just_bash_adapter";
export { createVfsGitCommands as createWorkspaceGitCommands } from "../vfs/just_bash_git";
export * from "../vfs/types";
