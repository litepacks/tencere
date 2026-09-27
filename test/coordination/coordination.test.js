import test from "node:test";
import assert from "node:assert/strict";
import { Tencere } from "../../src/index.js";

test("Tencere Coordination - once() guarantees single execution", async () => {
  const db = await Tencere.open();
  let count = 0;

  const fn = async () => {
    count++;
    return "done";
  };

  const res1 = await db.once("daily:report", fn);
  const res2 = await db.once("daily:report", fn);
  const res3 = await db.once("daily:report", fn);

  assert.equal(count, 1);
  assert.equal(res1, "done");
  assert.equal(res2, "done");
  assert.equal(res3, "done");

  await db.close();
});

test("Tencere Coordination - idempotent() deduplicates concurrent callers", async () => {
  const db = await Tencere.open();
  let executions = 0;

  const slowCreateOrder = async () => {
    executions++;
    await new Promise((r) => setTimeout(r, 60));
    return { orderId: 999 };
  };

  // 5 concurrent requests with identical idempotency key
  const results = await Promise.all([
    db.idempotent("req-42", slowCreateOrder),
    db.idempotent("req-42", slowCreateOrder),
    db.idempotent("req-42", slowCreateOrder),
    db.idempotent("req-42", slowCreateOrder),
    db.idempotent("req-42", slowCreateOrder)
  ]);

  assert.equal(executions, 1); // Only executed once!
  for (const r of results) {
    assert.deepEqual(r, { orderId: 999 });
  }

  // Replay from cache
  const replayed = await db.idempotent("req-42", slowCreateOrder);
  assert.equal(executions, 1);
  assert.deepEqual(replayed, { orderId: 999 });

  await db.close();
});

test("Tencere Coordination - semaphore limits concurrency", async () => {
  const db = await Tencere.open();
  const sem = db.semaphore("ext-api", { permits: 2, timeout: "2s" });

  let activeCount = 0;
  let maxObserved = 0;

  const task = async () => {
    return sem.run(async () => {
      activeCount++;
      if (activeCount > maxObserved) maxObserved = activeCount;
      await new Promise((r) => setTimeout(r, 30));
      activeCount--;
    });
  };

  await Promise.all([task(), task(), task(), task(), task()]);
  assert.ok(maxObserved <= 2);

  await db.close();
});

test("Tencere Coordination - rateLimit atomic sliding window", async () => {
  const db = await Tencere.open();

  const r1 = await db.rateLimit("user:1", { limit: 2, window: "500ms" });
  assert.equal(r1.allowed, true);
  assert.equal(r1.remaining, 1);

  const r2 = await db.rateLimit("user:1", { limit: 2, window: "500ms" });
  assert.equal(r2.allowed, true);
  assert.equal(r2.remaining, 0);

  // Exceeded
  const r3 = await db.rateLimit("user:1", { limit: 2, window: "500ms" });
  assert.equal(r3.allowed, false);
  assert.equal(r3.remaining, 0);

  await db.close();
});

test("Tencere Coordination - cache() with stampede prevention and SWR", async () => {
  const db = await Tencere.open();
  let loads = 0;

  const loader = async () => {
    loads++;
    await new Promise((r) => setTimeout(r, 40));
    return { time: Date.now(), data: "live" };
  };

  // Stampede: 10 concurrent requests
  const results = await Promise.all([
    db.cache("feed", { ttl: "100ms", stale: "200ms" }, loader),
    db.cache("feed", { ttl: "100ms", stale: "200ms" }, loader),
    db.cache("feed", { ttl: "100ms", stale: "200ms" }, loader),
    db.cache("feed", { ttl: "100ms", stale: "200ms" }, loader)
  ]);

  assert.equal(loads, 1); // Stampede prevented!
  assert.equal(results[0].data, "live");

  // Wait past TTL but within stale window
  await new Promise((r) => setTimeout(r, 120));

  // Should return stale immediately and trigger background refresh
  const staleHit = await db.cache("feed", { ttl: "100ms", stale: "200ms" }, loader);
  assert.equal(staleHit.data, "live");

  // Wait for background refresh to finish
  await new Promise((r) => setTimeout(r, 80));
  assert.equal(loads, 2);

  await db.close();
});
