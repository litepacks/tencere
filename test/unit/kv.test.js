import test from "node:test";
import assert from "node:assert/strict";
import { Tencere, VersionMismatchError } from "../../src/index.js";

test("Tencere KV - basic operations and value types", async () => {
  const db = await Tencere.open();

  // Basic set and get
  await db.set("foo", "bar");
  assert.equal(await db.get("foo"), "bar");
  assert.equal(await db.has("foo"), true);

  // Raw Uint8Array bytes
  const bytes = new Uint8Array([10, 20, 30, 40]);
  await db.set("binary_data", bytes);
  const fetchedBytes = await db.get("binary_data");
  assert.deepEqual(fetchedBytes, bytes);

  // Delete
  assert.equal(await db.delete("foo"), true);
  assert.equal(await db.has("foo"), false);
  assert.equal(await db.get("foo"), undefined);

  // getMany and setMany
  await db.setMany({ a: 1, b: 2, c: 3 });
  const batch = await db.getMany(["a", "b", "c"]);
  assert.deepEqual(batch, { a: 1, b: 2, c: 3 });

  await db.close();
});

test("Tencere KV - Atomic operations: increment, patch, CAS & update", async () => {
  const db = await Tencere.open();

  // Increment
  assert.equal(await db.increment("views", 1), 1);
  assert.equal(await db.increment("views", 5), 6);
  assert.equal(await db.get("views"), 6);

  // Patch
  await db.set("user:1", { name: "Ahmet", balance: 50, tags: ["admin"] });
  const patched = await db.patch("user:1", {
    $inc: { balance: 100 },
    $set: { active: true },
    $push: { tags: "pro" }
  });

  assert.equal(patched.balance, 150);
  assert.equal(patched.active, true);
  assert.deepEqual(patched.tags, ["admin", "pro"]);

  // CAS Versioning
  const v1Res = await db.get("user:1", { withVersion: true });
  assert.ok(v1Res.version > 0);

  // Successful CAS
  await db.set("user:1", { ...v1Res.value, balance: 200 }, { ifVersion: v1Res.version });
  const v2Res = await db.get("user:1", { withVersion: true });
  assert.equal(v2Res.value.balance, 200);
  assert.equal(v2Res.version, v1Res.version + 1);

  // Failing CAS with stale version
  await assert.rejects(
    async () => {
      await db.set("user:1", { ...v2Res.value, balance: 300 }, { ifVersion: v1Res.version });
    },
    (err) => err instanceof VersionMismatchError
  );

  // Optimistic update
  const updated = await db.update("user:1", (user) => {
    user.balance += 50;
    return user;
  });
  assert.equal(updated.balance, 250);

  await db.close();
});

test("Tencere KV - Lifecycle: Sliding TTL and Consume-on-Read", async () => {
  const db = await Tencere.open();

  // Consume on read
  await db.set("code:123", "SECRET_PIN", { ttl: "10s", consume: true });
  const pin = await db.get("code:123");
  assert.equal(pin, "SECRET_PIN");

  // Key is immediately consumed and deleted
  assert.equal(await db.get("code:123"), undefined);
  assert.equal(await db.has("code:123"), false);

  // Sliding TTL
  await db.set("session:1", "session_data", { ttl: "180ms", sliding: true });
  await new Promise((r) => setTimeout(r, 80));

  // Touch on read extends the expiration
  assert.equal(await db.get("session:1"), "session_data");

  await new Promise((r) => setTimeout(r, 80));
  // Key should still be alive because read refreshed it!
  assert.equal(await db.get("session:1"), "session_data");

  // Now wait for full expiration without touch
  await new Promise((r) => setTimeout(r, 220));
  assert.equal(await db.get("session:1"), undefined);

  await db.close();
});
