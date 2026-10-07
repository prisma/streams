// A strict flag parser: every command names the flags it takes, an
// unknown or repeated single flag is a usage error (exit 2), and a value
// never comes from anywhere but the command line.
import { AdminError } from "./errors";

export interface FlagSpec {
  /// Flags taking one value.
  single?: readonly string[];
  /// Flags taking one value each time, repeatable.
  multi?: readonly string[];
  /// Flags taking no value.
  bool?: readonly string[];
  /// Flags that must be present.
  required?: readonly string[];
}

export class Flags {
  constructor(private readonly values: Map<string, string[]>) {}

  get(name: string): string | undefined {
    return this.values.get(name)?.[0];
  }

  must(name: string): string {
    const v = this.get(name);
    if (v === undefined) throw new AdminError(`--${name} is required`, 2);
    return v;
  }

  all(name: string): string[] {
    return this.values.get(name) ?? [];
  }

  has(name: string): boolean {
    return this.values.has(name);
  }

  /// A positive whole number, or `dflt` when absent.
  count(name: string, dflt: number): number {
    const raw = this.get(name);
    if (raw === undefined) return dflt;
    if (!/^[0-9]+$/.test(raw) || !Number.isSafeInteger(Number(raw)) || Number(raw) < 1) {
      throw new AdminError(`--${name} ${raw} is not a positive whole number`, 2);
    }
    return Number(raw);
  }
}

export function parseFlags(argv: readonly string[], spec: FlagSpec): Flags {
  const values = new Map<string, string[]>();
  const single = new Set(spec.single ?? []);
  const multi = new Set(spec.multi ?? []);
  const bool = new Set(spec.bool ?? []);
  for (let i = 0; i < argv.length; i++) {
    const arg = argv[i];
    if (!arg.startsWith("--")) throw new AdminError(`unexpected argument ${arg}`, 2);
    const name = arg.slice(2);
    if (bool.has(name)) {
      values.set(name, ["true"]);
      continue;
    }
    if (!single.has(name) && !multi.has(name)) throw new AdminError(`unknown flag --${name}`, 2);
    const value = argv[i + 1];
    if (value === undefined || value.startsWith("--")) {
      throw new AdminError(`--${name} needs a value`, 2);
    }
    i += 1;
    if (single.has(name) && values.has(name)) throw new AdminError(`--${name} given twice`, 2);
    values.set(name, [...(values.get(name) ?? []), value]);
  }
  for (const name of spec.required ?? []) {
    if (!values.has(name)) throw new AdminError(`--${name} is required`, 2);
  }
  return new Flags(values);
}
