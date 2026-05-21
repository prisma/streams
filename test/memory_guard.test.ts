import { describe, expect, test } from "bun:test";
import { MemoryPressureMonitor, darwinTopMemArgs, parseDarwinTopMemBytes, parseLinuxMemAvailableBytes } from "../src/memory";

describe("memory pressure monitor", () => {
  test("parses darwin top mem output", () => {
    const output = `
Processes: 123 total

PID    MEM  COMMAND
10462  366M bun
`;
    expect(parseDarwinTopMemBytes(output, 10462)).toBe(366 * 1024 * 1024);
  });

  test("parses darwin top mem output with decimal units", () => {
    const output = `
PID    MEM  COMMAND
10462  1.1G bun
`;
    expect(parseDarwinTopMemBytes(output, 10462)).toBe(Math.round(1.1 * 1024 * 1024 * 1024));
  });

  test("returns null when the pid line is missing", () => {
    expect(parseDarwinTopMemBytes("PID MEM COMMAND\n999 10M bun\n", 10462)).toBeNull();
  });

  test("uses darwin top args that include pid and mem columns", () => {
    expect(darwinTopMemArgs(10462)).toEqual(["-l", "1", "-pid", "10462", "-stats", "pid,mem"]);
  });

  test("parses linux MemAvailable from /proc/meminfo", () => {
    const meminfo = `
MemTotal:        3905536 kB
MemFree:          241172 kB
MemAvailable:    3004396 kB
Buffers:          102848 kB
`;
    expect(parseLinuxMemAvailableBytes(meminfo)).toBe(3_004_396 * 1024);
  });

  test("does not start sampling when the pressure limit is disabled", async () => {
    let samples = 0;
    const monitor = new MemoryPressureMonitor(0, {
      intervalMs: 50,
      onSample: () => {
        samples += 1;
      },
    });

    try {
      monitor.start();
      await Bun.sleep(100);
    } finally {
      monitor.stop();
    }

    expect(samples).toBe(0);
    expect(monitor.isOverLimit()).toBe(false);
    expect(monitor.getLimitBytes()).toBe(0);
  });
});
