import { createVfsGitCommands, type VfsGitCommandOptions } from "../vfs/just_bash_git";

export type WorkspaceGitCommandOptions = VfsGitCommandOptions;

export function createWorkspaceGitCommands(options: WorkspaceGitCommandOptions) {
  return createVfsGitCommands(options);
}
