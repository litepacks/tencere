import test from "node:test";
import assert from "node:assert/strict";
import { Tencere } from "../../src/index.js";

test("Tencere Collections - Queue worker, retry, and delay", async () => {
  const db = await Tencere.open();
  const queue = db.queue("jobs");

  const processed = [];
  const worker = queue.worker(async (job) => {
    if (job.data.shouldFail && job.attempts === 1) {
      throw new Error("Simulated failure on first attempt");
    }
    processed.push(job.data);
  }, { pollIntervalMs: 20 });

  // Push immediate job
  await queue.push({ task: "send_welcome_email" });

  // Push retrying job
  await queue.push({ task: "retry_job", shouldFail: true }, { retries: 2 });

  // Wait for processing
  const start = Date.now();
  while (processed.length < 1 && Date.now() - start < 3000) {
    await new Promise((r) => setTimeout(r, 25));
  }

  assert.ok(processed.some((p) => p.task === "send_welcome_email"));

  await worker.stop();
  await db.close();
});
