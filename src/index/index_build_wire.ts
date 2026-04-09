import { Buffer } from "node:buffer";

type TaggedBigInt = {
  __durable_streams_wire_type: "bigint";
  value: string;
};

type TaggedBytes = {
  __durable_streams_wire_type: "bytes";
  base64: string;
};

function isTaggedBigInt(value: unknown): value is TaggedBigInt {
  return (
    value != null &&
    typeof value === "object" &&
    (value as TaggedBigInt).__durable_streams_wire_type === "bigint" &&
    typeof (value as TaggedBigInt).value === "string"
  );
}

function isTaggedBytes(value: unknown): value is TaggedBytes {
  return (
    value != null &&
    typeof value === "object" &&
    (value as TaggedBytes).__durable_streams_wire_type === "bytes" &&
    typeof (value as TaggedBytes).base64 === "string"
  );
}

export function encodeIndexBuildWire<T>(value: T): string {
  return JSON.stringify(value, (_key, currentValue) => {
    if (typeof currentValue === "bigint") {
      return {
        __durable_streams_wire_type: "bigint",
        value: currentValue.toString(),
      } satisfies TaggedBigInt;
    }
    if (currentValue instanceof Uint8Array) {
      return {
        __durable_streams_wire_type: "bytes",
        base64: Buffer.from(currentValue).toString("base64"),
      } satisfies TaggedBytes;
    }
    return currentValue;
  });
}

export function decodeIndexBuildWire<T>(wire: string): T {
  return JSON.parse(wire, (_key, currentValue) => {
    if (isTaggedBigInt(currentValue)) return BigInt(currentValue.value);
    if (isTaggedBytes(currentValue)) return Uint8Array.from(Buffer.from(currentValue.base64, "base64"));
    return currentValue;
  }) as T;
}
