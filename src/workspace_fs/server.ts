import type { StreamProfileVfsRouteArgs } from "../profiles/profile";
import { handleVfsRepoRoute } from "../vfs/server";

export function handleWorkspaceFsRoute(args: StreamProfileVfsRouteArgs): Promise<Response> {
  return handleVfsRepoRoute(args);
}
