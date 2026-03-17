type ConsoleFn = (...args: any[]) => void;

let patched = false;

function wrapConsole(orig: ConsoleFn, level: string): ConsoleFn {
  return (...args: any[]) => {
    const prefix = `[${new Date().toISOString()}] [${level}]`;
    if (args.length === 0) return orig(prefix);
    return orig(prefix, ...args);
  };
}

export function initConsoleLogging(): void {
  if (patched) return;
  patched = true;
  const globalAny = globalThis as any;
  if (globalAny.__ds_console_patched) return;
  globalAny.__ds_console_patched = true;

  console.log = wrapConsole(console.log.bind(console), "INFO");
  console.info = wrapConsole(console.info.bind(console), "INFO");
  console.warn = wrapConsole(console.warn.bind(console), "WARN");
  console.error = wrapConsole(console.error.bind(console), "ERROR");
  if (console.debug) console.debug = wrapConsole(console.debug.bind(console), "DEBUG");
}
