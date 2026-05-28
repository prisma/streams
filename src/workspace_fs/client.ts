import {
  VfsClientError,
  VfsRepoClient,
  VfsWorkspace,
  type CheckoutOptions,
  type OpenVfsRepoOptions,
  type VfsFetch,
  type VfsWorkspaceCommitOptions,
} from "../vfs/client";

export type WorkspaceFsFetch = VfsFetch;

export type OpenWorkspaceFsRepoOptions = Omit<OpenVfsRepoOptions, "profileKind">;

export type WorkspaceCheckoutOptions = CheckoutOptions;

export type WorkspaceCommitOptions = VfsWorkspaceCommitOptions;

export { VfsClientError as WorkspaceFsClientError };

export function workspaceFsStreamName(tenantId: string, repoId: string): string {
  return `workspace/${tenantId}/${repoId}/control`;
}

export class WorkspaceFsWorkspace extends VfsWorkspace {}

export class WorkspaceFsClient extends VfsRepoClient {
  constructor(options: OpenWorkspaceFsRepoOptions) {
    const stream = options.stream ?? (options.tenantId && options.repoId ? workspaceFsStreamName(options.tenantId, options.repoId) : undefined);
    super({ ...options, stream, profileKind: "workspace-fs" });
  }

  override async checkout(options: WorkspaceCheckoutOptions = {}): Promise<WorkspaceFsWorkspace> {
    const workspace = await super.checkout(options);
    return new WorkspaceFsWorkspace(this, {
      repo: this.stream,
      ref: workspace.ref,
      workspaceId: workspace.workspaceId,
      baseCommitId: workspace.baseCommitId,
      rootTreeId: workspace.rootTreeId,
    });
  }
}

export function openWorkspaceFsRepo(options: OpenWorkspaceFsRepoOptions): WorkspaceFsClient {
  return new WorkspaceFsClient(options);
}
