// Compute entrypoint: the platform runs `main` directly, so give it a
// file that IS the server role (edge-repro.ts stays the dual-role CLI
// for laptops).
process.argv[2] = "server";
await import("./edge-repro.ts");
