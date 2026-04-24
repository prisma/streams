function hasFlag(argv: string[], flag: string): boolean {
  return argv.includes(flag) || argv.some((arg) => arg.startsWith(`${flag}=`));
}

export function ensureComputeArgv(argv: string[], env: NodeJS.ProcessEnv = process.env): string[] {
  const next = [...argv];
  if (!hasFlag(next, "--object-store")) {
    next.push("--object-store", "r2");
  }
  if (env.DS_MEMORY_LIMIT_MB != null && !hasFlag(next, "--auto-tune")) {
    next.push("--auto-tune");
  }
  return next;
}

if (import.meta.main) {
  process.argv = ensureComputeArgv(process.argv);
  await import("../server");
}
