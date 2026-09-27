import test from "node:test";
import assert from "node:assert/strict";
import { Tencere, TimeoutError } from "../../src/index.js";

test("Tencere Coordination - watch() reactive stream", async () => {
  const db = await Tencere.open();
  const changes = [];

  const watcherPromise = (async () => {
    for await (const change of db.watch("config:app")) {
      changes.push(change);
      if (changes.length === 2) break;
    }
  })();

  await new Promise((r) => setTimeout(r, 20));
  await db.set("config:app", { version: "1.0.0" });
  await db.set("config:app", { version: "1.0.1" });

  await watcherPromise;
  assert.equal(changes.length, 2);
  assert.equal(changes[0].value.version, "1.0.0");
  assert.equal(changes[1].value.version, "1.0.1");

  await db.close();
});

test("Tencere Coordination - waitFor() predicate and structured operators", async () => {
  const db = await Tencere.open();

  // 1. Predicate function
  const waiter1 = db.waitFor("deploy:1", (v) => v && v.ready === true);
  await db.set("deploy:1", { ready: false });
  await db.set("deploy:1", { ready: true });
  const res1 = await waiter1;
  assert.equal(res1.ready, true);

  // 2. Structured condition: { status: 'completed' }
  const waiter2 = db.waitFor("payment:1", { status: "completed" });
  await db.set("payment:1", { status: "pending" });
  await db.set("payment:1", { status: "completed" });
  const res2 = await waiter2;
  assert.equal(res2.status, "completed");

  // 3. Where operator condition: { where: { instances: { gte: 5 } } }
  const waiter3 = db.waitFor("cluster:1", {
    where: { instances: { gte: 5 } }
  });
  await db.set("cluster:1", { instances: 2 });
  await db.set("cluster:1", { instances: 5 });
  const res3 = await waiter3;
  assert.equal(res3.instances, 5);

  // 4. Timeout
  await assert.rejects(
    async () => {
      await db.waitFor("missing:key", { status: "ready" }, { timeout: "50ms" });
    },
    (err) => err instanceof TimeoutError
  );

  await db.close();
});
