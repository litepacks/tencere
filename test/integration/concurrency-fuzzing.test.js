import test from "node:test";
import assert from "node:assert/strict";
import { Tencere } from "../../src/index.js";

test("Concurrency Fuzzing: 50 concurrent workers contending on identical keys and atomic operations", async () => {
  const seed = 998877;
  const db = await Tencere.open({ history: { enabled: true } });

  const numWorkers = 50;
  const incrementsPerWorker = 20;
  const expectedTotalIncrements = numWorkers * incrementsPerWorker;

  try {
    // 1. Concurrent atomic increments on a single shared key
    await db.set("shared:counter", 0);

    const incrementWorkers = Array.from({ length: numWorkers }, async (_, wId) => {
      for (let i = 0; i < incrementsPerWorker; i++) {
        await db.increment("shared:counter", 1);
      }
    });

    await Promise.all(incrementWorkers);

    const finalCounter = await db.get("shared:counter");
    assert.equal(
      finalCounter,
      expectedTotalIncrements,
      `Lost update detected! Expected ${expectedTotalIncrements}, got ${finalCounter} (seed: ${seed})`
    );

    // 2. Concurrent contention across Map, Set, and Sorted
    const m = db.map("hot_map");
    const s = db.sorted("hot_scores");

    const mixedWorkers = Array.from({ length: numWorkers }, async (_, wId) => {
      const key = `key:${wId % 5}`;
      await m.set(key, { lastWorker: wId, ts: Date.now() });
      await s.set(`player:${wId % 5}`, (wId + 1) * 10);
      await db.set(`kv:${wId % 5}`, `value-${wId}`);
    });

    await Promise.all(mixedWorkers);

    // Invariants must hold under high concurrency
    const inv = await db.debug.verify();
    assert.equal(
      inv.valid,
      true,
      `Concurrency invariants violation (seed ${seed}): ${inv.errors.join(", ")}`
    );

    const hash = await db.debug.stateHash();
    assert.ok(hash && hash.length === 64);
  } finally {
    await db.close();
  }
});
