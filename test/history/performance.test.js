import test from "node:test";
import assert from "node:assert/strict";
import { Tencere } from "../../src/index.js";

test("Tencere History - Performance Overhead and Throughput Benchmarks", async () => {
  const N = 5000;

  // 1. SET without history
  const dbNoHistory = await Tencere.open({ history: false });
  const t0 = performance.now();
  for (let i = 0; i < N; i++) {
    await dbNoHistory.set(`key:${i % 100}`, i);
  }
  const tNoHistory = performance.now() - t0;
  const opsNoHistory = Math.round((N / tNoHistory) * 1000);
  await dbNoHistory.close();

  // 2. SET with history
  const dbWithHistory = await Tencere.open({ history: { enabled: true, maxVersions: 20 } });
  const t1 = performance.now();
  for (let i = 0; i < N; i++) {
    await dbWithHistory.set(`key:${i % 100}`, i);
  }
  const tWithHistory = performance.now() - t1;
  const opsWithHistory = Math.round((N / tWithHistory) * 1000);

  // 3. Current GET vs Historical GET
  const t2 = performance.now();
  for (let i = 0; i < 1000; i++) {
    await dbWithHistory.get("key:42");
  }
  const tGetCurrent = performance.now() - t2;

  const t3 = performance.now();
  for (let i = 0; i < 1000; i++) {
    await dbWithHistory.get("key:42", { atSequence: 50n });
  }
  const tGetHistorical = performance.now() - t3;

  // 4. db.at() construction time
  const t4 = performance.now();
  for (let i = 0; i < 1000; i++) {
    dbWithHistory.at("10m ago");
  }
  const tAtConstruction = performance.now() - t4;

  // 5. Single key rollback
  const t5 = performance.now();
  const rbResult = await dbWithHistory.rollback("key:42", { sequence: 100n });
  const tSingleRollback = performance.now() - t5;

  assert.ok(rbResult.sequence > 0n);

  // Assertions for correctness and sanity
  assert.ok(opsNoHistory > 5000, `Expected > 5000 ops/s without history, got ${opsNoHistory}`);
  assert.ok(opsWithHistory > 3000, `Expected > 3000 ops/s with history, got ${opsWithHistory}`);
  assert.ok(tAtConstruction < 50, `db.at() construction should be instantaneous (< 50ms for 1000 views), took ${tAtConstruction}ms`);

  // History index memory footprint check
  const historyEntries = dbWithHistory._engine.historyManager.totalHistoryEntries;
  assert.ok(historyEntries > 0);

  await dbWithHistory.close();
});
