import { describe, expect, test } from "bun:test";
import { deriveStateProtocolChanges } from "../src/profiles/stateProtocol/changes";

describe("state-protocol change derivation", () => {
  test("reads the before image from old_value", () => {
    const changes = deriveStateProtocolChanges({
      type: "public.posts",
      key: "42",
      value: { id: 42, title: "After" },
      old_value: { id: 42, title: "Before" },
      headers: {
        operation: "update",
        txid: "12345",
        timestamp: "2026-03-16T12:00:00.000Z",
      },
    });

    expect(changes).toEqual([
      {
        entity: "public.posts",
        key: "42",
        op: "update",
        before: { id: 42, title: "Before" },
        after: { id: 42, title: "After" },
      },
    ]);
  });

  test("ignores the legacy oldValue field", () => {
    const changes = deriveStateProtocolChanges({
      type: "public.posts",
      key: "42",
      value: { id: 42, title: "After" },
      oldValue: { id: 42, title: "Before" },
      headers: {
        operation: "update",
      },
    });

    expect(changes).toHaveLength(1);
    expect(changes[0]).toEqual({
      entity: "public.posts",
      key: "42",
      op: "update",
      before: undefined,
      after: { id: 42, title: "After" },
    });
  });

  test("ignores control messages for touch derivation", () => {
    const changes = deriveStateProtocolChanges({
      headers: {
        control: "reset",
        offset: "-1",
      },
    });

    expect(changes).toEqual([]);
  });
});
