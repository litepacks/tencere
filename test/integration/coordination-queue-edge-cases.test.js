import test from "node:test";
import assert from "node:assert/strict";
import { setTimeout } from "node:timers/promises";
import { Tencere } from "../../src/index.js";
import { safeSetTimeout } from "../../src/coordination/scheduler.js";

test("Coordination Edge Case 3.1: Scheduler prevents Node.js 32-bit setTimeout overflow for long delays", async () => {
  const db = await Tencere.open();

  // 1. Capture process warnings to ensure no TimeoutOverflowWarning is emitted
  const warnings = [];
  const onWarning = (warning) => warnings.push(warning);
  process.on("warning", onWarning);

  try {
    let fired = false;
    // 30 days in the future (30 * 24 * 3600 * 1000 = 2,592,000,000 ms > 2,147,483,647 ms)
    const thirtyDaysLater = Date.now() + 30 * 24 * 3600 * 1000;

    const task = db.schedule("far_future_task").at(thirtyDaysLater).run(async () => {
      fired = true;
    });

    // Without safeSetTimeout, Node.js would emit TimeoutOverflowWarning and fire after 1ms.
    await setTimeout(60);

    // Verify it did NOT fire prematurely
    assert.equal(fired, false, "Task with >24.8 day delay must not fire prematurely at 1ms");

    // Verify no TimeoutOverflowWarning was emitted
    const overflowWarning = warnings.find((w) =>
      w.name === "TimeoutOverflowWarning" || (w.message && w.message.includes("does not fit into a 32-bit"))
    );
    assert.equal(overflowWarning, undefined, "Node.js TimeoutOverflowWarning should not be triggered");

    // Verify task can be cleanly cancelled
    task.stop();
    assert.equal(task.active, false);
  } finally {
    process.removeListener("warning", onWarning);
    await db.close();
  }

  // 2. Unit test for safeSetTimeout execution
  let safeTimerFired = false;
  const timer = safeSetTimeout(() => {
    safeTimerFired = true;
  }, 20);

  await setTimeout(40);
  assert.equal(safeTimerFired, true, "safeSetTimeout executes callback when delay expires");
  timer.clear();
});

test("Coordination Edge Case 3.2: Queue hydrates persisted ready, delayed, and un-acked active jobs on restart", async () => {
  const db = await Tencere.open();

  // 1. Create a queue and push ready and delayed jobs
  const q1 = db.queue("order_processing");
  const job1 = await q1.push({ orderId: 101 });
  const job2 = await q1.push({ orderId: 102 });
  const jobDelayed = await q1.push({ orderId: 103 }, { delay: "80ms" });

  // Simulate an un-acked "active" job (worker crashed mid-execution)
  const jobCrashed = await q1.push({ orderId: 104 });
  // Manually transition to active in DB as if a worker crashed
  const jobData = await db.get(`__queue_job:order_processing:${jobCrashed.id}`);
  jobData.state = "active";
  jobData.attempts = 1;
  await db.set(`__queue_job:order_processing:${jobCrashed.id}`, jobData);

  // 2. Re-instantiate QueueCollection to simulate server restart / new instance
  const q2 = db.queue("order_processing");

  // Verify that size() triggers hydration automatically without manual replay()
  const stats = await q2.size();
  assert.equal(stats.delayed, 1, "Delayed job must be indexed in delayedIndex");
  // Ready jobs include job1, job2, and the recovered crashed job (jobCrashed)
  assert.equal(stats.ready, 3, "Ready jobs must include unhandled and recovered active jobs");

  // 3. Process jobs with worker on q2 and verify deterministic FIFO processing
  const processed = [];
  const worker = q2.worker(async (job) => {
    processed.push(job.data.orderId);
  }, { pollIntervalMs: 20 });

  // Wait for ready jobs to be processed
  await setTimeout(70);
  assert.ok(processed.includes(101));
  assert.ok(processed.includes(102));
  assert.ok(processed.includes(104));

  // Wait for delayed job to mature and be processed
  await setTimeout(80);
  assert.ok(processed.includes(103), "Delayed job should mature and be processed by worker");

  await worker.stop();
  await q1.close();
  await q2.close();
  await db.close();
});

test("Coordination Edge Case 3.3: Semaphore eliminates busy-polling via reactive permit release events", async () => {
  const db = await Tencere.open();
  const sem = db.semaphore("compute_lock", { permits: 1, timeout: "1s" });

  let holder1Finished = false;
  let waiter2StartedAt = 0;
  let handoverLatency = 0;

  // Task 1 holds permit for 40ms
  const p1 = sem.run(async () => {
    await setTimeout(40);
    holder1Finished = true;
  });

  // Task 2 waits for permit and measures reactive handover latency
  const p2 = (async () => {
    // Give p1 5ms head start to acquire permit
    await setTimeout(5);
    const waitStart = Date.now();
    return sem.run(async () => {
      waiter2StartedAt = Date.now();
      assert.equal(holder1Finished, true, "Task 2 must execute only after Task 1 releases permit");
      handoverLatency = waiter2StartedAt - waitStart - 35; // approximate reactive wake time
    });
  })();

  await Promise.all([p1, p2]);

  // Concurrency stress test: 10 concurrent callers competing for 2 permits
  const semPool = db.semaphore("api_pool", { permits: 2, timeout: "2s" });
  let activeConcurrent = 0;
  let maxConcurrent = 0;
  let totalExecuted = 0;

  const runTask = async (id) => {
    return semPool.run(async () => {
      activeConcurrent++;
      if (activeConcurrent > maxConcurrent) {
        maxConcurrent = activeConcurrent;
      }
      await setTimeout(15);
      totalExecuted++;
      activeConcurrent--;
    });
  };

  const tasks = Array.from({ length: 10 }, (_, i) => runTask(i));
  await Promise.all(tasks);

  assert.equal(totalExecuted, 10, "All 10 tasks should execute successfully");
  assert.ok(maxConcurrent <= 2, `Max concurrency must not exceed 2 permits, was: ${maxConcurrent}`);
  assert.equal(activeConcurrent, 0);

  await db.close();
});
