/**
 * SDK ergonomics walkthrough: streams, tailing, and the queue profile.
 * Run: STREAM_KEY=$(./target/release/streams-keys generate) bun sdk/example.ts
 */
import { StreamsClient } from "./durable-streams";

const client = new StreamsClient({
  url: process.env.STREAMS_URL ?? "http://127.0.0.1:8090",
  key: process.env.STREAM_KEY!,
});

const run = `${Date.now()}`;

// ---- 1. plain stream: append + tail ----
const chat = await client.create(`sdk-chat-${run}`, {
  contentType: "application/json",
});
await chat.append({ user: "ada", text: "hello" }, { key: "room-1" });
await chat.append({ user: "bob", text: "hi!" }, { key: "room-1" });

const page = await chat.read({ offset: "-1", key: "room-1" });
console.log("chat replay:", page.messages, "upToDate:", page.upToDate);

// ---- 2. queue profile: producer + CF-style consumer ----
const jobs = await client.create(`sdk-jobs-${run}`, {
  contentType: "application/json",
  profile: "queue",
  queueMaxDeliveries: 3,
});

for (let i = 0; i < 5; i++) {
  await jobs.append({ jobId: i, task: "resize-image" });
}

const worker = jobs.queue("worker-a");
let handled = 0;
let failedOnce = false;

const batch = await worker.receive({ batchSize: 10, waitMs: 2000 });
console.log(`received ${batch.messages.length} jobs, backlog=${batch.backlog}`);
for (const msg of batch.messages) {
  const job = msg.body as { jobId: number };
  if (job.jobId === 2 && !failedOnce) {
    failedOnce = true;
    console.log(`job ${job.jobId}: simulated failure -> retry`);
    msg.retry({ delayMs: 100 });
  } else {
    handled++;
    console.log(`job ${job.jobId}: done (attempt ${msg.attempts})`);
  }
}
const s1 = await batch.settle();
console.log("settled:", s1);

// The retried job comes back after its delay.
await new Promise((r) => setTimeout(r, 300));
const batch2 = await worker.receive({ batchSize: 10, waitMs: 2000 });
for (const msg of batch2.messages) {
  console.log(`redelivered job ${(msg.body as { jobId: number }).jobId} (attempt ${msg.attempts})`);
  handled++;
}
console.log("settled:", await batch2.settle());

// ---- 3. poison message -> DLQ (maxDeliveries = 3) ----
await jobs.append({ jobId: 99, task: "always-fails" });
for (let round = 0; round < 4; round++) {
  const b = await worker.receive({ batchSize: 10, waitMs: 1000 });
  for (const m of b.messages) m.retry({ delayMs: 0 });
  const out = await b.settle();
  if (out && out.dlq > 0) {
    console.log(`poison job hit the DLQ after round ${round + 1}:`, out);
    break;
  }
}
const dlq = await jobs.read({ offset: "-1", key: "$dlq" });
console.log("DLQ view:", dlq.messages);

console.log(`\nOK: ${handled}/6 jobs handled, poison routed to $dlq`);
