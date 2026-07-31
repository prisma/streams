const port = Number(process.env.PORT ?? 8080);
console.log("hello app booting; PORT env =", JSON.stringify(process.env.PORT));
console.log("env keys:", Object.keys(process.env).sort().join(","));
Bun.serve({
  port,
  hostname: "0.0.0.0",
  fetch(req) {
    const u = new URL(req.url);
    if (u.pathname === "/health") return new Response("ok");
    return new Response(JSON.stringify({ hello: "world", path: u.pathname, port }), {
      headers: { "content-type": "application/json" },
    });
  },
});
console.log(`hello app listening on 0.0.0.0:${port}`);
