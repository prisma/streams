import type { StreamProfileSpec } from "../profiles";

export type RequestObservabilityPairingDescriptor = {
  request: {
    events_stream: string;
    traces_stream: string;
  };
};

function readRequestPairing(
  profile: StreamProfileSpec
): Record<string, unknown> | null {
  const observability = profile.observability;
  if (
    !observability ||
    typeof observability !== "object" ||
    Array.isArray(observability)
  ) {
    return null;
  }
  const request = (observability as Record<string, unknown>).request;
  return request && typeof request === "object" && !Array.isArray(request)
    ? (request as Record<string, unknown>)
    : null;
}

function nonEmptyString(value: unknown): string | null {
  return typeof value === "string" && value.trim() !== "" ? value.trim() : null;
}

export function buildRequestObservabilityPairingDescriptor(
  stream: string,
  profile: StreamProfileSpec
): RequestObservabilityPairingDescriptor | null {
  const request = readRequestPairing(profile);
  if (!request) return null;

  if (profile.kind === "evlog") {
    const tracesStream = nonEmptyString(request.tracesStream);
    if (!tracesStream) return null;
    return {
      request: {
        events_stream: stream,
        traces_stream: tracesStream,
      },
    };
  }

  if (profile.kind === "otel-traces") {
    const eventsStream = nonEmptyString(request.eventsStream);
    if (!eventsStream) return null;
    return {
      request: {
        events_stream: eventsStream,
        traces_stream: stream,
      },
    };
  }

  return null;
}
