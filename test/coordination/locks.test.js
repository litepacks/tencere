import test from "node:test";
import assert from "node:assert/strict";
import { Tencere, LockStaleOwnerError } from "../../src/index.js";

test("Tencere Coordination - Locks, fencing tokens, and stale owner protection", async () => {
  const db = await Tencere.open();

  // Basic withLock and fencing token
  let token1 = 0;
  await db.lock("resource:order:1", async ({ token }) => {
    token1 = token;
    assert.ok(token > 0);
  });

  let token2 = 0;
  await db.lock("resource:order:1", async ({ token }) => {
    token2 = token;
    assert.ok(token2 > token1); // Monotonically increasing!
  });

  // tryLock
  const lock = await db.tryLock("resource:invoice:1", { ttl: "2s" });
  assert.ok(lock);
  assert.ok(lock.token > token2);

  // Concurrent acquisition should return null
  const lockBusy = await db.tryLock("resource:invoice:1");
  assert.equal(lockBusy, null);

  // Release lock
  await lock.release();

  // Can acquire again after release
  const lock2 = await db.tryLock("resource:invoice:1");
  assert.ok(lock2);
  await lock2.release();

  // Stale owner protection test:
  // Simulate Worker A acquiring with short TTL (50ms)
  const lockA = await db.tryLock("resource:contended", { ttl: "40ms" });
  assert.ok(lockA);

  // Wait for lockA to expire
  await new Promise((r) => setTimeout(r, 60));

  // Worker B acquires the newly available lock
  const lockB = await db.tryLock("resource:contended", { ttl: "1s" });
  assert.ok(lockB);
  assert.ok(lockB.token > lockA.token);

  // Stale Worker A returns and tries to release()
  await assert.rejects(
    async () => {
      await lockA.release();
    },
    (err) => err instanceof LockStaleOwnerError
  );

  // Worker B's lock is safely preserved!
  assert.equal(await db.tryLock("resource:contended"), null);

  // Worker B releases cleanly
  await lockB.release();
  assert.ok(await db.tryLock("resource:contended"));

  await db.close();
});
