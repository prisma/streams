import type { TouchConfig } from "./spec.ts";

const DEFAULT_TOUCH_SUFFIX = ".__touch";

export function defaultTouchStreamName(sourceStream: string): string {
  return `${sourceStream}${DEFAULT_TOUCH_SUFFIX}`;
}

export function resolveTouchStreamName(sourceStream: string, touch: TouchConfig): string {
  const override = touch.derivedStream;
  if (override && override.trim() !== "") return override;
  return defaultTouchStreamName(sourceStream);
}
