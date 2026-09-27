import test from "node:test";
import assert from "node:assert/strict";
import { Tencere } from "../../src/index.js";

test("Composition Test Matrix: Scope + All Collections + Nested Scopes", async () => {
  const db = await Tencere.open({ history: { enabled: true } });

  try {
    const tenant1 = db.scope("tenant:42");
    const tenant2 = db.scope("tenant:99");

    // 1. Nested Scope
    const project = tenant1.scope("project:9");

    // KV
    await project.set("status", "active");
    await tenant2.set("status", "suspended");

    assert.equal(await project.get("status"), "active");
    assert.equal(await tenant2.get("status"), "suspended");
    assert.equal(await db.get("tenant:42:project:9:status"), "active");

    // Map
    const usersProj = project.map("users");
    await usersProj.set("u1", { name: "Alice", role: "admin" });

    const usersT2 = tenant2.map("users");
    await usersT2.set("u1", { name: "Bob", role: "guest" });

    assert.deepEqual(await usersProj.get("u1"), { name: "Alice", role: "admin" });
    assert.deepEqual(await usersT2.get("u1"), { name: "Bob", role: "guest" });

    // Set
    const tags = project.setCollection("tags");
    await tags.add("production");
    await tags.add("web");
    assert.equal(await tags.has("production"), true);
    assert.equal(await tags.size(), 2);

    // Sorted
    const scores = project.sorted("scores");
    await scores.set("player1", 100);
    await scores.set("player2", 250);
    const top = await scores.top(2);
    assert.equal(top[0].member, "player2");
    assert.equal(top[0].score, 250);

    // TimeSeries
    const metrics = project.timeseries("metrics");
    await metrics.add(42.5, { tags: { host: "srv1" } });
    await metrics.add(85.0, { tags: { host: "srv1" } });
    const latest = await metrics.latest();
    assert.equal(latest.value, 85.0);

    // Queue
    const queue = project.queue("jobs");
    const jId = await queue.push({ task: "sync" });
    assert.ok(jId);

    // Vector
    const vectors = project.vector("embeddings", { dimensions: 3 });
    await vectors.set("doc1", [1.0, 0.0, 0.0], { title: "Doc 1" });
    const searchRes = await vectors.search([1.0, 0.0, 0.0], { topK: 1 });
    assert.equal(searchRes.length, 1);
    assert.equal(searchRes[0].id, "doc1");

    // Coordination under Scope: Lock, Once, Idempotent, RateLimit
    let onceRuns = 0;
    await project.once("setup", async () => {
      onceRuns++;
      return "done";
    });
    await project.once("setup", async () => {
      onceRuns++;
      return "done";
    });
    assert.equal(onceRuns, 1);

    const rl = await project.rateLimit("api", { limit: 5, window: "10s" });
    assert.equal(rl.allowed, true);

    // Verify invariants
    const inv = await db.debug.verify();
    assert.equal(inv.valid, true, `Invariants failed: ${inv.errors.join(", ")}`);

    // Verify deterministic state hash works on scope and root
    const rootHash = await db.debug.stateHash();
    assert.ok(rootHash && rootHash.length === 64);
    const scopeHash = await tenant1.debug.stateHash();
    assert.ok(scopeHash && scopeHash.length === 64);
  } finally {
    await db.close();
  }
});

test("Composition Test Matrix: Scope + Watch & Canonical Change Events", async () => {
  const db = await Tencere.open();

  try {
    const tenant = db.scope("tenant:blue");
    const other = db.scope("tenant:red");

    const events = [];
    const watcher = (async () => {
      for await (const change of tenant.watch()) {
        events.push(change);
        if (events.length >= 3) break;
      }
    })();

    // Allow event stream listener attachment
    await new Promise((r) => setTimeout(r, 20));

    // Mutation inside tenant scope
    await tenant.set("greeting", "hello");
    await tenant.map("config").set("theme", "dark");

    // Mutation in another tenant (should NOT be received by tenant watcher)
    await other.set("greeting", "stranger");

    // Another mutation inside tenant scope
    await tenant.set("counter", 1);

    await watcher;

    assert.equal(events.length, 3);
    assert.equal(events[0].key, "tenant:blue:greeting");
    assert.equal(events[0].value, "hello");
    assert.equal(events[1].key, "tenant:blue:__map:config:theme");
    assert.equal(events[2].key, "tenant:blue:counter");
  } finally {
    await db.close();
  }
});

test("Composition Test Matrix: Watch across Sorted, TimeSeries, and Rollback RESTORE", async () => {
  const db = await Tencere.open({ history: { enabled: true } });

  try {
    // 1. Sorted Watch
    const scores = db.sorted("leaderboard");
    const sortedChanges = [];
    const sortedWatcher = (async () => {
      for await (const change of scores.watch()) {
        sortedChanges.push(change);
        if (sortedChanges.length >= 2) break;
      }
    })();

    await new Promise((r) => setTimeout(r, 15));
    await scores.set("player1", 50);
    await scores.set("player1", 100);
    await sortedWatcher;

    assert.equal(sortedChanges.length, 2);
    assert.equal(sortedChanges[0].member, "player1");
    assert.equal(sortedChanges[0].score, 50);
    assert.equal(sortedChanges[1].score, 100);

    // 2. TimeSeries Watch with where() filter
    const ts = db.timeseries("cpu");
    const tsPoints = [];
    const tsWatcher = (async () => {
      for await (const point of ts.where({ region: "eu" }).watch()) {
        tsPoints.push(point);
        if (tsPoints.length >= 1) break;
      }
    })();

    await new Promise((r) => setTimeout(r, 15));
    // Non-matching tag
    await ts.add(10, { tags: { region: "us" } });
    // Matching tag
    await ts.add(95, { tags: { region: "eu" } });
    await tsWatcher;

    assert.equal(tsPoints.length, 1);
    assert.equal(tsPoints[0].value, 95);
    assert.equal(tsPoints[0].tags.region, "eu");

    // 3. Rollback + Watch: RESTORE must appear in canonical watch stream
    const dbChanges = [];
    const dbWatcher = (async () => {
      for await (const change of db.watch("status")) {
        dbChanges.push(change);
        if (dbChanges.length >= 4) break;
      }
    })();

    await new Promise((r) => setTimeout(r, 15));
    await db.set("status", "A");
    await db.set("status", "B");
    await db.set("status", "C");

    // Perform rollback to when status was A
    const hist = [];
    for await (const h of db.history("status")) {
      hist.push(h);
    }
    const initialEntry = hist.find((h) => h.value === "A");
    assert.ok(initialEntry);

    await db.rollback({ key: "status", targetSequence: initialEntry.sequence });
    await dbWatcher;

    assert.equal(dbChanges.length, 4);
    assert.equal(dbChanges[0].value, "A");
    assert.equal(dbChanges[1].value, "B");
    assert.equal(dbChanges[2].value, "C");
    assert.equal(dbChanges[3].type, "restore");
    assert.equal(dbChanges[3].value, "A");
  } finally {
    await db.close();
  }
});

test("Composition Test Matrix: Historical Composition db.at() across Collections", async () => {
  const db = await Tencere.open({ history: { enabled: true } });

  try {
    // Initial state (Seq 1..n)
    await db.set("config:mode", "staging");
    await db.map("settings").set("darkTheme", false);
    await db.sorted("scores").set("alice", 10);
    await db.timeseries("ping").add(12.0);

    const midSeq = db._engine._sequenceCounter;

    // Mutate state after midSeq
    await db.set("config:mode", "production");
    await db.map("settings").set("darkTheme", true);
    await db.sorted("scores").set("alice", 999);
    await db.timeseries("ping").add(99.0);

    // Historical view at midSeq
    const past = db.at(midSeq);

    assert.equal(await past.get("config:mode"), "staging");
    assert.equal(await past.map("settings").get("darkTheme"), false);
    assert.equal(await past.sorted("scores").score("alice"), 10);

    // TimeSeries historical view: points added after midSeq must not be visible
    const pastPing = await past.timeseries("ping").latest();
    assert.equal(pastPing.value, 12.0);

    // Current state confirms latest mutations
    assert.equal(await db.get("config:mode"), "production");
    assert.equal(await db.map("settings").get("darkTheme"), true);
    assert.equal(await db.sorted("scores").score("alice"), 999);
    const currPing = await db.timeseries("ping").latest();
    assert.equal(currPing.value, 99.0);
  } finally {
    await db.close();
  }
});

test("Composition Test Matrix: TTL + Collections & History Preserved", async () => {
  const db = await Tencere.open({ history: { enabled: true } });

  try {
    // Set KV with TTL
    await db.set("tempKey", "active", { ttl: "60ms" });
    assert.equal(await db.get("tempKey"), "active");

    // Map entry with TTL
    const map = db.map("sessions");
    await map.set("sess1", { user: "john" }, { ttl: "60ms" });
    assert.deepEqual(await map.get("sess1"), { user: "john" });

    // Wait for TTL expiry
    await new Promise((r) => setTimeout(r, 120));

    // After expiry: values are gone from live view
    assert.equal(await db.get("tempKey"), undefined);
    assert.equal(await map.get("sess1"), undefined);

    // History: historical record of tempKey still exists
    const historyEntries = [];
    for await (const entry of db.history("tempKey")) {
      historyEntries.push(entry);
    }
    assert.ok(historyEntries.length >= 1);
    assert.equal(historyEntries[0].value, "active");

    const inv = await db.debug.verify();
    assert.equal(inv.valid, true);
  } finally {
    await db.close();
  }
});
