import test from "node:test";
import assert from "node:assert/strict";
import { Tencere, LIMITS, KeyTooLargeError } from "../../src/index.js";
import { Operation, OP_SET } from "../../src/core/operations.js";

test("Tencere Limits - Single source of truth export & frozen structure", async () => {
  // 1. LIMITS is defined and structured
  assert.ok(LIMITS);
  assert.equal(LIMITS.BINARY.MAX_KEY_BYTES, 65535);
  assert.equal(LIMITS.BINARY.MAX_EXTRA_BYTES, 65535);
  assert.equal(LIMITS.BINARY.MAX_VALUE_BYTES, 4294967295);
  assert.equal(LIMITS.BINARY.MAX_PARTITIONS, 65535);
  assert.equal(LIMITS.BINARY.MAX_TTL_MS, 4294967295);
  assert.equal(LIMITS.BINARY.MAX_VERSION, 18446744073709551615n);

  assert.equal(LIMITS.CLUSTER.RECOMMENDED_MAX_NODES, 9);
  assert.equal(LIMITS.CLUSTER.DEFAULT_HEARTBEAT_MS, 15);

  assert.equal(LIMITS.TIMESERIES.DEFAULT_MAX_SERIES, 100000);
  assert.equal(LIMITS.TIMESERIES.DEFAULT_MAX_TAGS, 8);

  assert.equal(LIMITS.COORDINATION.MAX_TIMEOUT_MS, 2147483647);

  // 2. Immutability: Object is frozen
  assert.throws(() => {
    // @ts-ignore
    LIMITS.BINARY.MAX_KEY_BYTES = 100;
  });

  // 3. Static and instance access
  assert.equal(Tencere.limits, LIMITS);

  const db = await Tencere.open();
  assert.equal(db.limits, LIMITS);
  await db.close();
});

test("Tencere Limits - KeyTooLargeError thrown when exceeding MAX_KEY_BYTES", async () => {
  // Key exceeding 65535 bytes
  const oversizedKey = "x".repeat(65536);

  const op = new Operation({
    op: OP_SET,
    key: oversizedKey,
    value: "test"
  });

  assert.throws(
    () => {
      op.encode();
    },
    (err) => err instanceof KeyTooLargeError && err.size === 65536 && err.limit === 65535
  );
});
