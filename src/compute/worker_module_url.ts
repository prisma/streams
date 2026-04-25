export function resolveWorkerModuleUrl(
  currentModuleUrl: string,
  sourceRelativePath: string,
  builtRelativePath = sourceRelativePath.endsWith(".ts")
    ? `${sourceRelativePath.slice(0, -3)}.js`
    : sourceRelativePath
): URL {
  return new URL(currentModuleUrl.endsWith(".js") ? builtRelativePath : sourceRelativePath, currentModuleUrl);
}
