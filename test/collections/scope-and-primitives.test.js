import test from "node:test";
import assert from "node:assert/strict";
import { Tencere } from "../../src/index.js";

test("Tencere Collections - Scope, Counter, KVCollection, and Map edge cases", async () => {
  const db = await Tencere.open();

  // 1. Counter test
  const counter = db.counter("metrics:page_views");
  assert.equal(await counter.value(), 0);

  assert.equal(await counter.inc(), 1);
  assert.equal(await counter.inc(4), 5);
  assert.equal(await counter.add(10), 15);
  assert.equal(await counter.dec(), 14);
  assert.equal(await counter.dec(4), 10);
  assert.equal(await counter.value(), 10);

  await counter.reset();
  assert.equal(await counter.value(), 0);
  await counter.reset(42);
  assert.equal(await counter.value(), 42);

  // 2. KVCollection test
  const kv = db.kv("tenant1");
  await kv.set("config", { theme: "dark" });
  assert.equal(await kv.has("config"), true);
  assert.deepEqual(await kv.get("config"), { theme: "dark" });

  await kv.setMany({ a: 1, b: 2 });
  const batchResult = await kv.getMany(["a", "b", "missing"]);
  assert.deepEqual(batchResult, { a: 1, b: 2, missing: undefined });

  await kv.setMany([["c", 3], ["d", 4]]);
  assert.equal(await kv.get("c"), 3);

  assert.equal(await kv.increment("c", 2), 5);

  const patched = await kv.patch("config", { $set: { font: "monospace" } });
  assert.equal(patched.font, "monospace");

  const updated = await kv.update("c", (val) => val * 10);
  assert.equal(updated, 50);
  assert.equal(await kv.get("c"), 50);

  assert.equal(await kv.delete("config"), true);
  assert.equal(await kv.has("config"), false);

  // 3. Scope test (prefix isolation & delegation)
  const orgScope = db.scope("org_acme");
  await orgScope.set("plan", "enterprise");
  assert.equal(await orgScope.get("plan"), "enterprise");
  assert.equal(await orgScope.has("plan"), true);
  assert.equal(await db.get("org_acme:plan"), "enterprise");

  await orgScope.setMany({ region: "eu-central", maxUsers: 100 });
  const orgMany = await orgScope.getMany(["region", "maxUsers"]);
  assert.deepEqual(orgMany, { region: "eu-central", maxUsers: 100 });

  await orgScope.setMany([["env", "prod"]]);
  assert.equal(await orgScope.get("env"), "prod");

  assert.equal(await orgScope.increment("maxUsers", 50), 150);

  const orgPatched = await orgScope.patch("plan", { $set: { tier: "custom" } });
  assert.equal(orgPatched.tier, "custom");

  await orgScope.update("maxUsers", (v) => v + 10);
  assert.equal(await orgScope.get("maxUsers"), 160);

  // Sub-scoping
  const appScope = orgScope.scope("backend");
  await appScope.set("port", 8080);
  assert.equal(await appScope.get("port"), 8080);
  assert.equal(await db.get("org_acme:backend:port"), 8080);

  // Scope collection factories
  const scCounter = orgScope.counter("requests");
  await scCounter.inc(5);
  assert.equal(await scCounter.value(), 5);

  const scKv = orgScope.kv("store");
  await scKv.set("k1", "v1");
  assert.equal(await scKv.get("k1"), "v1");

  const scMap = orgScope.map("sessions");
  await scMap.set("s1", { userId: 1 });
  assert.equal(await scMap.has("s1"), true);
  assert.equal(await scMap.size(), 1);
  assert.deepEqual(await scMap.keys(), ["s1"]);
  const mapEntries = await scMap.entries();
  assert.equal(mapEntries.length, 1);
  assert.equal(mapEntries[0][0], "s1");
  const mapValues = await scMap.values();
  assert.equal(mapValues.length, 1);
  await scMap.clear();
  assert.equal(await scMap.size(), 0);

  const scSet = orgScope.setCollection("tags");
  await scSet.add("vip");
  assert.equal(await scSet.has("vip"), true);

  const scSorted = orgScope.sorted("leaderboard");
  await scSorted.set("p1", 100);
  assert.equal(await scSorted.score("p1"), 100);

  const scStream = orgScope.stream("logs");
  const eventId = await scStream.append({ msg: "start" });
  assert.ok(eventId);

  const scQueue = orgScope.queue("jobs");
  await scQueue.push({ type: "email" });
  const qSize = await scQueue.size();
  assert.equal(qSize.ready, 1);

  const scVec = orgScope.vector("vecs");
  await scVec.set("v1", { vector: [1, 0] });
  assert.equal(await scVec.count(), 1);

  // Scope coordination wrappers
  const lock = await orgScope.tryLock("job_lock");
  assert.ok(lock);
  await lock.release();

  let executedOnce = 0;
  await orgScope.once("init_task", async () => {
    executedOnce++;
  });
  await orgScope.once("init_task", async () => {
    executedOnce++;
  });
  assert.equal(executedOnce, 1);

  const idempResult = await orgScope.idempotent("charge:123", async () => "charged");
  assert.equal(idempResult, "charged");

  const sem = orgScope.semaphore("db_pool", { limit: 2 });
  const semResult = await sem.run(async () => "done");
  assert.equal(semResult, "done");

  const rl = await orgScope.rateLimit("api", { limit: 10, window: "1m" });
  assert.equal(rl.allowed, true);

  const cached = await orgScope.cache("expensive_data", { ttl: "1m" }, async () => "cached_val");
  assert.equal(cached, "cached_val");

  // Scope watch & waitFor
  const watchIter = orgScope.watch("watch_key");
  const nextChangePromise = (async () => {
    for await (const change of watchIter) {
      return change;
    }
  })();
  await orgScope.set("watch_key", "hello");
  const change = await nextChangePromise;
  assert.equal(change.type, "set");

  const waitPromise = orgScope.waitFor("status", (v) => v === "ready", { timeout: 1000 });
  await orgScope.set("status", "ready");
  const waitResult = await waitPromise;
  assert.equal(waitResult, "ready");

  assert.equal(await orgScope.delete("plan"), true);
  assert.equal(await orgScope.has("plan"), false);

  await db.close();
});
