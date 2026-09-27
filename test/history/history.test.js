import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs/promises";
import { Tencere, HistoryDisabledError, UnsupportedHistoricalOperationError } from "../../src/index.js";

test("Tencere History - Configuration, Opt-in, Filtering, and Overrides", async () => {
  // 1. History is disabled by default
  {
    const db = await Tencere.open();
    await db.set("key1", "val1");
    assert.equal(await db.get("key1"), "val1");
    assert.throws(() => {
      db.at("10m ago");
    }, HistoryDisabledError);
    await assert.rejects(async () => {
      await db.rollback("key1");
    }, HistoryDisabledError);
    await db.close();
  }

  // 2. Global enable and filtering (include/exclude)
  {
    const db = await Tencere.open({
      history: {
        enabled: true,
        retention: "7d",
        maxVersions: 20,
        include: ["config:*", "user:*", "scores:*"],
        exclude: ["cache:*", "session:*"]
      }
    });

    await db.set("config:site", { title: "Tencere" });
    await db.set("user:42", { name: "Alice" });
    await db.set("cache:temp", { data: 123 });

    // config:site should have history
    const configHist = [];
    for await (const rev of db.history("config:site")) {
      configHist.push(rev);
    }
    assert.equal(configHist.length, 1);
    assert.equal(configHist[0].operation, "SET");
    assert.deepEqual(configHist[0].value, { title: "Tencere" });

    // cache:temp should NOT have history (excluded)
    const cacheHist = [];
    for await (const rev of db.history("cache:temp")) {
      cacheHist.push(rev);
    }
    assert.equal(cacheHist.length, 0);

    // Collection-level overrides
    const userMap = db.map("users", { history: { retention: "30d" } });
    const cacheMap = db.map("cache", { history: false });

    await userMap.set("u1", "active");
    await cacheMap.set("c1", "cached_val");

    const u1Hist = [];
    for await (const rev of userMap.history("u1")) {
      u1Hist.push(rev);
    }
    assert.equal(u1Hist.length, 1);
    assert.equal(u1Hist[0].value, "active");

    const c1Hist = [];
    for await (const rev of cacheMap.history("c1")) {
      c1Hist.push(rev);
    }
    assert.equal(c1Hist.length, 0);

    await db.close();
  }
});

test("Tencere History - Version Model, Sequences, and Key History Iteration", async () => {
  const db = await Tencere.open({
    history: { enabled: true, maxVersions: 10 }
  });

  // Multiple mutations on config
  const r1 = await db.set("config", { port: 8080 });
  const r2 = await db.set("config", { port: 9000 });
  const r3 = await db.set("config", { port: 9001 });

  assert.ok(typeof r1.version === "bigint");
  assert.ok(r2.version > r1.version);
  assert.ok(r3.version > r2.version);

  // withVersion read returns BigInt version
  const cur = await db.get("config", { withVersion: true });
  assert.equal(cur.version, r3.version);
  assert.deepEqual(cur.value, { port: 9001 });

  // Query history descending (newest first)
  const descRevs = [];
  for await (const rev of db.history("config")) {
    descRevs.push(rev);
  }
  assert.equal(descRevs.length, 3);
  assert.equal(descRevs[0].value.port, 9001);
  assert.equal(descRevs[1].value.port, 9000);
  assert.equal(descRevs[2].value.port, 8080);
  assert.equal(descRevs[0].version, r3.version);
  assert.equal(descRevs[1].version, r2.version);
  assert.equal(descRevs[2].version, r1.version);

  // Query history ascending with limit
  const ascRevs = [];
  for await (const rev of db.history("config", { direction: "asc", limit: 2 })) {
    ascRevs.push(rev);
  }
  assert.equal(ascRevs.length, 2);
  assert.equal(ascRevs[0].value.port, 8080);
  assert.equal(ascRevs[1].value.port, 9000);

  await db.close();
});

test("Tencere History - Coordination History Observation and Rollback Protection", async () => {
  const db = await Tencere.open({ history: { enabled: true } });

  // Acquire, renew, and release a lock
  const lock = await db.tryLock("payment:42", { ttl: "10s", renew: true });
  assert.ok(lock);

  // Wait a small interval so renew fires or manual release
  await new Promise((r) => setTimeout(r, 50));
  await lock.release();

  // Inspect coordination history
  const lockEvents = db.lockHistory("payment:42");
  assert.ok(lockEvents.length >= 2);
  assert.equal(lockEvents[lockEvents.length - 1].opName, "LOCK_ACQUIRE");
  assert.equal(lockEvents[0].opName, "LOCK_RELEASE");

  // Generic history query by type
  const queryEvents = db.history({ type: "lock", key: "payment:42" });
  assert.ok(queryEvents.length >= 2);

  // Coordination locks cannot be rolled back or acquired in historical view
  const past = db.at("10m ago");
  await assert.rejects(async () => {
    await past.lock("payment:42", async () => {});
  }, UnsupportedHistoricalOperationError);

  await db.close();
});

test("Tencere History - Queue and Stream Rollback Protections", async () => {
  const db = await Tencere.open({ history: { enabled: true } });

  // 1. Queue collection
  const queue = db.queue("emails");
  await queue.push({ to: "user@example.com", subject: "Welcome" });
  await queue.push({ to: "admin@example.com", subject: "Alert" });

  // Queue rollback is prohibited to protect external side effects
  await assert.rejects(async () => {
    await queue.rollback();
  }, UnsupportedHistoricalOperationError);

  // Replay dry-run works
  const dryReplay = await queue.replay({ dryRun: true });
  assert.equal(dryReplay.length, 2);

  // Explicit requeue
  const requeued = await queue.requeue(dryReplay[0].id);
  assert.equal(requeued, true);

  // 2. Stream collection
  const stream = db.stream("audit-log");
  await stream.append({ action: "login", user: "ahmet" });
  await stream.append({ action: "transfer", amount: 100 });

  // Stream history iteration
  const streamEvents = [];
  for await (const evt of stream.history()) {
    streamEvents.push(evt);
  }
  assert.equal(streamEvents.length, 2);
  assert.equal(streamEvents[0].data.action, "login");

  // Stream rollback is prohibited (append-only)
  await assert.rejects(async () => {
    await stream.rollback();
  }, UnsupportedHistoricalOperationError);

  await db.close();
});
