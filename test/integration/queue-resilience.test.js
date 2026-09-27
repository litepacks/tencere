import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs/promises";
import path from "node:path";
import os from "node:os";
import { setTimeout } from "node:timers/promises";
import { Tencere } from "../../src/index.js";
import { verifyInvariants } from "../../src/diagnostics/invariants.js";

test("Queue Resilience - Worker failure, lease expiration redelivery, and idempotency", async () => {
  const tmpDir = await fs.mkdtemp(path.join(os.tmpdir(), "tencere-queue-resilience-"));

  try {
    const db = await Tencere.open(tmpDir);
    const queue = db.queue("tasks");

    // 1. Worker crash simulation during processing (lease expiration & safe redelivery)
    let executionAttempts = 0;
    const worker1 = queue.worker(
      async (job) => {
        executionAttempts++;
        if (executionAttempts === 1) {
          // Simulate worker crash / stall before completing or acking
          throw new Error("Worker simulated unhandled crash");
        }
      },
      { pollIntervalMs: 20 }
    );

    await queue.push({ task: "important_work" }, { timeout: "100ms", retries: 3 });

    // Wait for initial failure and backoff / redelivery
    await setTimeout(2500);
    await worker1.stop();

    assert.ok(executionAttempts >= 2, `Job must be redelivered after worker failure (attempts: ${executionAttempts})`);

    // 2. Idempotency integration with db.idempotent()
    let sideEffectCount = 0;
    let processCalls = 0;

    const worker2 = queue.worker(
      async (job) => {
        processCalls++;
        await db.idempotent(`task:${job.id}`, async () => {
          sideEffectCount++;
        });

        // Simulate crash on first delivery AFTER side effect but BEFORE queue deletes job
        if (processCalls === 1) {
          throw new Error("Crashed after side effect but before ACK");
        }
      },
      { pollIntervalMs: 20 }
    );

    const job2 = await queue.push({ action: "charge_payment" }, { timeout: "100ms", retries: 2 });
    await setTimeout(2500);
    await worker2.stop();

    // The job was executed multiple times by the worker loop due to the crash
    assert.ok(processCalls >= 2, `Worker should have retried the unacknowledged job (calls: ${processCalls})`);
    // But the critical side-effect was protected by db.idempotent() and executed exactly once!
    assert.equal(sideEffectCount, 1, "db.idempotent() must guarantee exactly-once side effect execution across retries");

    // 3. Process crash with active in-flight job followed by DB restart & recovery
    // Push a job and manually simulate an active in-flight lease in storage
    const job3 = await queue.push({ action: "restart_recovery" });
    const jobRecord = await db._engine.get(`__queue_job:tasks:${job3.id}`);
    assert.ok(jobRecord);
    // Mark as active in storage as if a worker claimed it right before a sudden SIGKILL
    jobRecord.state = "active";
    jobRecord.attempts = 1;
    await db._engine.set(`__queue_job:tasks:${job3.id}`, jobRecord);

    // Close DB simulating crash
    await db.close();

    // Restart DB and verify hydration recovers the abandoned active job
    const recoveredDb = await Tencere.open(tmpDir);
    const recoveredQueue = recoveredDb.queue("tasks");

    let recoveredExecuted = false;
    const worker3 = recoveredQueue.worker(
      async (job) => {
        if (job.id === job3.id) {
          recoveredExecuted = true;
        }
      },
      { pollIntervalMs: 20 }
    );

    await setTimeout(300);
    await worker3.stop();

    assert.equal(recoveredExecuted, true, "Active jobs from crashed processes must be recovered and re-executed upon restart");

    // 4. Invariant assertion
    const report = await verifyInvariants(recoveredDb);
    assert.equal(report.valid, true, "Database invariants must hold after queue failure recoveries");

    await recoveredDb.close();
  } finally {
    await fs.rm(tmpDir, { recursive: true, force: true }).catch(() => {});
  }
});
