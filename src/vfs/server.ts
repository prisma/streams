import type { StreamProfileVfsRouteArgs } from "../profiles/profile";
import { handleWorkspaceFsRoute } from "../workspace_fs/server";

export function handleVfsRepoRoute(args: StreamProfileVfsRouteArgs): Promise<Response> {
  return handleWorkspaceFsRoute(args);
}
