import test from "node:test";
import assert from "node:assert/strict";
import { Tencere, VersionMismatchError, LockStaleOwnerError } from "../../src/index.js";

test("P0 Edge Case 1: RateLimiter auto-expires keys and reclaims memory", async () => {
  const db = await Tencere.open();

  // Consume with short window of 60ms
  const res1 = await db.rateLimit("user:192.168.1.1", { limit: 10, window: "60ms" });
  assert.equal(res1.allowed, true);
  assert.equal(res1.remaining, 9);

  // Storage should have the rate limit key initially
  const rawKey = "__ratelimit:user:192.168.1.1";
  assert.equal(await db.has(rawKey), true);

  // Wait for the window to expire
  await new Promise((resolve) => setTimeout(resolve, 90));

  // The key must now be expired / deleted from storage, preventing unbounded memory leak
  assert.equal(await db.has(rawKey), false);

  await db.close();
});

test("P0 Edge Case 2: engine.update preserves BigInt, Buffer, Uint8Array and nested types without JSON loss", async () => {
  const db = await Tencere.open();

  // 1. BigInt value in update
  await db.set("counter:big", 1000000000000000000n);
  const updatedBig = await db.update("counter:big", (curr) => {
    assert.equal(typeof curr, "bigint");
    return curr + 500n;
  });
  assert.equal(updatedBig, 1000000000000000500n);
  const fetchedBig = await db.get("counter:big");
  assert.equal(fetchedBig, 1000000000000000500n);

  // 2. Binary bytes in update
  const originalBuf = Buffer.from("super-secret-binary-stream-1234");
  await db.set("blob:raw", originalBuf);
  const updatedBuf = await db.update("blob:raw", (curr) => {
    assert.ok(curr instanceof Uint8Array, "curr should be a genuine Uint8Array/Buffer, not a plain Object");
    return Buffer.concat([Buffer.from(curr), Buffer.from("-appended")]);
  });
  assert.ok(updatedBuf instanceof Uint8Array);
  assert.equal(Buffer.from(updatedBuf).toString("utf8"), "super-secret-binary-stream-1234-appended");
  const fetchedBuf = await db.get("blob:raw");
  assert.ok(fetchedBuf instanceof Uint8Array);
  assert.equal(Buffer.from(fetchedBuf).toString("utf8"), "super-secret-binary-stream-1234-appended");

  // 3. Composite nested object containing BigInt, Buffer, and Uint8Array
  await db.set("composite:1", {
    id: 1,
    balance: 9999999999999999999n,
    cert: Buffer.from("cert-bytes"),
    vector: new Uint8Array([1, 2, 3, 4])
  });

  const nextComposite = await db.update("composite:1", (curr) => {
    assert.equal(typeof curr.balance, "bigint");
    assert.ok(Buffer.isBuffer(curr.cert) || curr.cert instanceof Uint8Array);
    assert.ok(curr.vector instanceof Uint8Array);
    curr.balance += 1n;
    return curr;
  });

  assert.equal(nextComposite.balance, 10000000000000000000n);
  assert.ok(Buffer.isBuffer(nextComposite.cert) || nextComposite.cert instanceof Uint8Array);

  await db.close();
});

test("P0 Edge Case 3: Conditional delete with ifVersion protects locks against TOCTOU race conditions", async () => {
  const db = await Tencere.open();

  // Test atomic conditional delete
  const { version: v1 } = await db.set("doc:1", "initial");
  assert.ok(v1 > 0);

  // Wrong ifVersion must reject with VersionMismatchError
  await assert.rejects(
    async () => {
      await db._engine.delete("doc:1", { ifVersion: 99999 });
    },
    (err) => err instanceof VersionMismatchError
  );
  // Key should still exist
  assert.equal(await db.get("doc:1"), "initial");

  // Correct ifVersion must delete successfully
  const deleted = await db._engine.delete("doc:1", { ifVersion: v1 });
  assert.equal(deleted, true);
  assert.equal(await db.get("doc:1"), undefined);

  // Lock TOCTOU race test: Worker A gets lock, lease expires, Worker B takes it, Worker A release fails safely
  const lockA = await db.tryLock("order:exclusive:42", { ttl: "40ms" });
  assert.ok(lockA);

  await new Promise((r) => setTimeout(r, 60)); // Let A expire

  const lockB = await db.tryLock("order:exclusive:42", { ttl: "1s" });
  assert.ok(lockB);

  // Stale Worker A attempting release must reject and NOT delete Worker B's active lock
  await assert.rejects(
    async () => {
      await lockA.release();
    },
    (err) => err instanceof LockStaleOwnerError
  );

  // Worker B's lock remains active and valid
  const lockC = await db.tryLock("order:exclusive:42");
  assert.equal(lockC, null); // Cannot acquire because Worker B still holds it

  await lockB.release();
  await db.close();
});

test("P0 Edge Case 4: _withKeyLock Re-entrancy deadlocks are eliminated", async () => {
  const db = await Tencere.open();

  // Re-entrant write within update callback on the same key
  const res = await db.update("account:100", async (curr) => {
    // Nested operation on the same key inside the updater
    await db.set("account:100:audit", "updating");
    // Nested write directly on the same key
    const currentVal = (await db.get("account:100")) || 0;
    return currentVal + 10;
  });

  assert.equal(res, 10);
  assert.equal(await db.get("account:100"), 10);
  assert.equal(await db.get("account:100:audit"), "updating");

  await db.close();
});

test("P0 Edge Case 5: Idempotency lease recovery on worker crash avoids undefined return and false timeouts", async () => {
  const db = await Tencere.open();

  let executions = 0;

  // Simulate a worker starting task 'order:pay:88' with 50ms lease, then crashing
  const claimKey = "__idemp:order:pay:88";
  await db.set(claimKey, {
    state: "pending",
    ownerId: "crashed-worker-1",
    expiresAt: Date.now() + 50
  }, { ttl: "50ms" });

  // Caller 2 arrives while worker 1 has crashed and is pending
  // Caller 2 should wait out the expired lease, acquire the lease, and execute fn successfully
  const result = await db.idempotent("order:pay:88", async () => {
    executions++;
    return { charged: true, amount: 250 };
  }, { leaseTtl: "1s", waitTimeout: "2s" });

  assert.notEqual(result, undefined, "Idempotent operation must not return undefined");
  assert.deepEqual(result, { charged: true, amount: 250 });
  assert.equal(executions, 1);

  // Subsequent caller retrieves cached result
  const cached = await db.idempotent("order:pay:88", async () => {
    executions++;
    return { charged: false };
  });
  assert.deepEqual(cached, { charged: true, amount: 250 });
  assert.equal(executions, 1); // Not re-executed

  await db.close();
});
