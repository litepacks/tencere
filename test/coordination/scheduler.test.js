import test from "node:test";
import assert from "node:assert/strict";
import { setTimeout } from "node:timers/promises";
import { Tencere } from "../../src/index.js";

test("Tencere Coordination - Scheduler (.every, .at, task stop, error handling)", async () => {
  const db = await Tencere.open();

  // 1. .every() periodic execution
  let count = 0;
  const taskEvery = db.schedule("periodic_pulse").every("30ms").run(async () => {
    count++;
  });
  assert.equal(taskEvery.active, true);

  await setTimeout(200);
  assert.ok(count >= 2, `Expected at least 2 ticks, got ${count}`);

  taskEvery.stop();
  assert.equal(taskEvery.active, false);
  const frozenCount = count;
  await setTimeout(70);
  assert.equal(count, frozenCount, "Task should not run after stop()");

  // 2. .at() with number timestamp
  let atExecuted = false;
  const targetTime = Date.now() + 30;
  const taskAtNum = db.schedule("one_shot_num").at(targetTime).run(async () => {
    atExecuted = true;
  });
  await setTimeout(60);
  assert.equal(atExecuted, true);
  taskAtNum.stop();

  // 3. .at() with Date object
  let atDateExecuted = false;
  const taskAtDate = db.schedule("one_shot_date").at(new Date(Date.now() + 20)).run(async () => {
    atDateExecuted = true;
  });
  await setTimeout(50);
  assert.equal(atDateExecuted, true);
  taskAtDate.stop();

  // 4. .at() with string date
  let atStrExecuted = false;
  const taskAtStr = db.schedule("one_shot_str").at(new Date(Date.now() + 20).toISOString()).run(async () => {
    atStrExecuted = true;
  });
  await setTimeout(50);
  assert.equal(atStrExecuted, true);
  taskAtStr.stop();

  // 5. Error case: run() without every or at
  assert.throws(() => {
    db.schedule("invalid_task").run(async () => {});
  }, /requires either \.every\(\.\.\.\) or \.at\(\.\.\.\)/);

  // 6. Error inside task handler should be caught safely
  let errorCount = 0;
  const errTask = db.schedule("failing_task").every("20ms").run(async () => {
    errorCount++;
    throw new Error("Simulated scheduled task failure");
  });
  await setTimeout(50);
  assert.ok(errorCount >= 1);
  errTask.stop();

  await db.close();
});
