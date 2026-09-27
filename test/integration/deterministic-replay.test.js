import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { Tencere } from "../../src/index.js";

test("Deterministic Mutation Replay: WAL replay reconstructs identical stateHash", async () => {
  const tmpDir = await fs.mkdtemp(path.join(os.tmpdir(), "tencere-replay-"));

  try {
    // 1. Initialize original database instance S1
    const db1 = await Tencere.open(tmpDir, { history: { enabled: true } });

    // KV mutations
    await db1.set("site:name", "Tencere DB");
    await db1.set("site:version", "1.0.0");
    await db1.set("site:counter", 10);
    await db1.incr("site:counter", 5); // 15
    await db1.patch("site:meta", { $set: { author: "DeepMind", env: "test" } });
    await db1.set("site:temp", "to-be-deleted");
    await db1.delete("site:temp");

    // Scope + Map
    const tenant = db1.scope("org:acme");
    const userMap = tenant.map("users");
    await userMap.set("u100", { name: "Alice", active: true });
    await userMap.set("u200", { name: "Bob", active: false });
    await userMap.set("u300", { name: "Charlie", active: true });
    await userMap.delete("u200");

    // Set
    const roleSet = tenant.setCollection("roles");
    await roleSet.add("admin");
    await roleSet.add("developer");
    await roleSet.add("viewer");
    await roleSet.delete("viewer");

    // Sorted collection
    const scores = db1.sorted("leaderboard");
    await scores.set("player1", 100);
    await scores.set("player2", 500);
    await scores.set("player3", 250);
    await scores.incr("player1", 450); // player1 is now 550

    // TimeSeries collection
    const metrics = db1.timeseries("telemetry");
    await metrics.add(10.5, { tags: { node: "a" } });
    await metrics.add(20.5, { tags: { node: "b" } });
    await metrics.add(30.5, { tags: { node: "a" } });

    // Queue collection
    const queue = db1.queue("tasks");
    await queue.push({ task: "send-email", to: "test@example.com" });
    await queue.push({ task: "resize-image", width: 800 });

    // Vector collection
    const vectors = db1.vector("embeddings", { dimensions: 4 });
    await vectors.set("doc1", [0.1, 0.2, 0.3, 0.4], { title: "First" });
    await vectors.set("doc2", [0.9, 0.8, 0.7, 0.6], { title: "Second" });

    // Rollback test: mutate then forward-restore
    await db1.set("rollbackKey", "v1");
    await db1.set("rollbackKey", "v2");
    await db1.set("rollbackKey", "v3");
    const rHist = [];
    for await (const r of db1.history("rollbackKey")) {
      rHist.push(r);
    }
    const v1Rev = rHist.find((r) => r.value === "v1");
    assert.ok(v1Rev);
    await db1.rollback("rollbackKey", { version: v1Rev.version });

    // Compute canonical state hash for original state S1
    const s1Hash = await db1.debug.stateHash();
    assert.ok(s1Hash && s1Hash.length === 64);

    const inv1 = await db1.debug.verify();
    assert.equal(inv1.valid, true, `S1 invariants failed: ${inv1.errors.join(", ")}`);

    // Gracefully close S1
    await db1.close();

    // 2. Open new database instance S2 pointing to the same persisted log
    const db2 = await Tencere.open(tmpDir, { history: { enabled: true } });

    // Compute canonical state hash for replayed state S2
    const s2Hash = await db2.debug.stateHash();

    // Verify S1 and S2 hashes are strictly identical
    assert.equal(
      s2Hash,
      s1Hash,
      `State hash mismatch after replay!\nOriginal: ${s1Hash}\nReplayed: ${s2Hash}`
    );

    // Verify invariants on replayed instance
    const inv2 = await db2.debug.verify();
    assert.equal(inv2.valid, true, `S2 invariants failed: ${inv2.errors.join(", ")}`);

    // Verify logical values directly
    assert.equal(await db2.get("site:name"), "Tencere DB");
    assert.equal(await db2.get("site:counter"), 15);
    assert.equal(await db2.get("site:temp"), undefined);
    assert.equal(await db2.get("rollbackKey"), "v1");

    const replayedTenant = db2.scope("org:acme");
    assert.deepEqual(await replayedTenant.map("users").get("u100"), { name: "Alice", active: true });
    assert.equal(await replayedTenant.map("users").get("u200"), undefined);

    const replayedTop = await db2.sorted("leaderboard").top(1);
    assert.equal(replayedTop[0].member, "player1");
    assert.equal(replayedTop[0].score, 550);

    const replayedLatest = await db2.timeseries("telemetry").latest();
    assert.equal(replayedLatest.value, 30.5);

    await db2.close();
  } finally {
    await fs.rm(tmpDir, { recursive: true, force: true }).catch(() => {});
  }
});
