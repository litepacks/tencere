import test from "node:test";
import assert from "node:assert/strict";
import { ExpiryManager, parseDuration } from "../../src/core/expiry-wheel.js";

test("ExpiryManager - parseDuration", () => {
  assert.equal(parseDuration("500ms"), 500);
  assert.equal(parseDuration("10s"), 10000);
  assert.equal(parseDuration("2m"), 120000);
  assert.equal(parseDuration("1h"), 3600000);
  assert.equal(parseDuration("1d"), 86400000);
  assert.equal(parseDuration(5000), 5000);
});

test("ExpiryManager - schedule, sliding TTL, and purge", async () => {
  const expired = [];
  const manager = new ExpiryManager({
    onExpire: (key) => expired.push(key)
  });

  manager.schedule("k1", "50ms");
  manager.schedule("k2", "300ms", { sliding: true });
  manager.schedule("k3", "500ms");

  assert.equal(manager.isExpired("k1"), false);

  // Wait for k1 to expire and auto-purge via timer
  await new Promise((r) => setTimeout(r, 100));
  assert.ok(expired.includes("k1"));

  // Touch k2 (sliding TTL)
  const touched = manager.touch("k2");
  assert.equal(touched, true);

  manager.cancel("k3");
  assert.equal(manager.keyMap.has("k3"), false);

  manager.close();
});
