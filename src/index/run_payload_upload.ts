import { Result } from "better-result";
import type { ObjectStore } from "../objectstore/interface";
import { retryAbortable } from "../util/retry";
import { ConcurrencyGate } from "../concurrency_gate";

export type RunPayloadUploadTask =
  | {
      objectKey: string;
      payload: Uint8Array;
    }
  | {
      objectKey: string;
      localPath: string;
      sizeBytes: number;
    };

type RunPayloadUploadError = {
  kind: "run_payload_upload_failed";
  message: string;
};

function uploadFailed(message: string): Result<never, RunPayloadUploadError> {
  return Result.err({ kind: "run_payload_upload_failed", message });
}

export async function uploadRunPayloadsBoundedResult(input: {
  tasks: RunPayloadUploadTask[];
  os: ObjectStore;
  concurrency: number;
  retries: number;
  baseDelayMs: number;
  maxDelayMs: number;
  timeoutMs: number;
}): Promise<Result<number[], RunPayloadUploadError>> {
  const gate = new ConcurrencyGate(Math.max(1, input.concurrency));
  const sizes = new Array<number>(input.tasks.length).fill(0);
  let firstError: string | null = null;

  await Promise.all(
    input.tasks.map(async (task, index) =>
      gate.run(async () => {
        if (firstError) return;
        try {
          if ("payload" in task) {
            await retryAbortable(
              (signal) => input.os.put(task.objectKey, task.payload, { contentLength: task.payload.byteLength, signal }),
              {
                retries: input.retries,
                baseDelayMs: input.baseDelayMs,
                maxDelayMs: input.maxDelayMs,
                timeoutMs: input.timeoutMs,
              }
            );
            sizes[index] = task.payload.byteLength;
          } else if (input.os.putFile) {
            await retryAbortable(
              (signal) => input.os.putFile!(task.objectKey, task.localPath, task.sizeBytes, { signal }),
              {
                retries: input.retries,
                baseDelayMs: input.baseDelayMs,
                maxDelayMs: input.maxDelayMs,
                timeoutMs: input.timeoutMs,
              }
            );
            sizes[index] = task.sizeBytes;
          } else {
            const payload = await Bun.file(task.localPath).bytes();
            await retryAbortable(
              (signal) => input.os.put(task.objectKey, payload, { contentLength: payload.byteLength, signal }),
              {
                retries: input.retries,
                baseDelayMs: input.baseDelayMs,
                maxDelayMs: input.maxDelayMs,
                timeoutMs: input.timeoutMs,
              }
            );
            sizes[index] = payload.byteLength;
          }
        } catch (error: unknown) {
          firstError = String((error as any)?.message ?? error);
        }
      })
    )
  );

  if (firstError) return uploadFailed(firstError);
  return Result.ok(sizes);
}
