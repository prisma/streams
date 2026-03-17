import { TaggedError } from "better-result";

export class DurableStreamsError extends TaggedError("DurableStreamsError")<{
  message: string;
  cause?: unknown;
  code?: string;
}>() {}

export function dsError(message: string, opts?: { cause?: unknown; code?: string }): DurableStreamsError {
  return new DurableStreamsError({
    message,
    ...(opts?.cause !== undefined ? { cause: opts.cause } : {}),
    ...(opts?.code !== undefined ? { code: opts.code } : {}),
  });
}
