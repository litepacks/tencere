import test from "node:test";
import assert from "node:assert/strict";
import { Tencere } from "../../src/index.js";
import { TencereSync } from "../../src/sync/index.js";
import {
  TencereError,
  VersionMismatchError,
  LockAcquireError,
  LockStaleOwnerError,
  TimeoutError,
  KeyNotFoundError,
  DatabaseClosedError,
  ClusterNotAvailableError
} from "../../src/errors.js";

test("Tencere - Errors taxonomy completeness", () => {
  const base = new TencereError("base error");
  assert.equal(base.code, "ERR_TENCERE");
  assert.equal(base.name, "TencereError");

  const vErr = new VersionMismatchError(1, 2);
  assert.equal(vErr.code, "ERR_VERSION_MISMATCH");
  assert.equal(vErr.expected, 1);
  assert.equal(vErr.actual, 2);

  const lErr = new LockAcquireError("my_key", "custom reason");
  assert.equal(lErr.code, "ERR_LOCK_ACQUIRE");
  assert.equal(lErr.key, "my_key");

  const sErr = new LockStaleOwnerError("stale_key", 42);
  assert.equal(sErr.code, "ERR_LOCK_STALE_OWNER");
  assert.equal(sErr.key, "stale_key");
  assert.equal(sErr.token, 42);

  const tErr = new TimeoutError("timed out");
  assert.equal(tErr.code, "ERR_TIMEOUT");

  const kErr = new KeyNotFoundError("missing_key");
  assert.equal(kErr.code, "ERR_KEY_NOT_FOUND");
  assert.equal(kErr.key, "missing_key");

  const dbErr = new DatabaseClosedError();
  assert.equal(dbErr.code, "ERR_DATABASE_CLOSED");

  const cErr = new ClusterNotAvailableError("no cluster");
  assert.equal(cErr.code, "ERR_CLUSTER_NOT_AVAILABLE");
});

test("Tencere Collections - Map watch() stream", async () => {
  const db = await Tencere.open();
  const map = db.map("live_map");

  const changes = [];
  const watcher = (async () => {
    for await (const change of map.watch()) {
      changes.push(change);
      if (changes.length >= 2) break;
    }
  })();

  await map.set("user:1", { name: "Alice" });
  await map.set("user:1", { name: "Alice Updated" });

  await watcher;
  assert.equal(changes.length, 2);
  assert.equal(changes[0].key, "user:1");
  assert.equal(changes[0].value.name, "Alice");
  assert.equal(changes[1].key, "user:1");
  assert.equal(changes[1].value.name, "Alice Updated");

  await db.close();
});

test("TencereSync - Collections (SyncMap, SyncSet, SyncSorted) and methods", () => {
  const sync = new TencereSync();

  // 1. Sync KV methods
  sync.set("k1", "v1");
  sync.set("k2", "v2");
  assert.equal(sync.has("k1"), true);
  assert.equal(sync.has("missing"), false);

  const many = sync.getMany(["k1", "k2", "missing"]);
  assert.deepEqual(many, { k1: "v1", k2: "v2", missing: undefined });

  sync.setMany({ a: 10, b: 20 });
  assert.equal(sync.get("a"), 10);
  sync.setMany([["c", 30], ["d", 40]]);
  assert.equal(sync.get("c"), 30);

  assert.equal(sync.increment("a", 5), 15);
  assert.equal(sync.patch("k1", { $set: { extra: true } }).extra, true);

  assert.equal(sync.delete("k2"), true);
  assert.equal(sync.delete("nonexistent"), false);

  const stats = sync.stats();
  assert.ok(stats.keys >= 3);

  // 2. SyncMap
  const map = sync.map("users");
  map.set("u1", { name: "Bob" });
  map.set("u2", { name: "Charlie" });
  assert.equal(map.has("u1"), true);
  assert.deepEqual(map.get("u1"), { name: "Bob" });
  assert.equal(map.size(), 2);
  assert.deepEqual(map.keys().sort(), ["u1", "u2"]);
  assert.equal(map.values().length, 2);
  assert.equal(map.entries().length, 2);
  assert.equal(map.delete("u1"), true);
  assert.equal(map.size(), 1);

  // 3. SyncSet
  const set = sync.setCollection("tags");
  set.add("admin");
  set.add("moderator");
  assert.equal(set.has("admin"), true);
  assert.equal(set.has("guest"), false);
  assert.equal(set.size(), 2);
  assert.deepEqual(set.members().sort(), ["admin", "moderator"]);
  assert.equal(set.delete("admin"), true);
  assert.equal(set.size(), 1);

  // 4. SyncSorted
  const sorted = sync.sorted("scores");
  sorted.set("p1", 100);
  sorted.set("p2", 250);
  sorted.set("p3", 50);

  assert.equal(sorted.score("p2"), 250);
  assert.equal(sorted.rank("p2"), 2); // Default ascending: p3=0, p1=1, p2=2
  assert.equal(sorted.rank("p3"), 0);
  assert.equal(sorted.rank("p2", { reverse: true }), 0);
  assert.equal(sorted.rank("missing"), undefined);
  assert.equal(sorted.size(), 3);

  const top2 = sorted.top(2);
  assert.equal(top2.length, 2);
  assert.equal(top2[0].member, "p2");

  const bottom1 = sorted.bottom(1);
  assert.equal(bottom1.length, 1);
  assert.equal(bottom1[0].member, "p3");

  const between = sorted.between(60, 260);
  assert.equal(between.length, 2);

  assert.equal(sorted.delete("p1"), true);
  assert.equal(sorted.size(), 2);

  sync.clear();
  assert.equal(sync.stats().keys, 0);

  sync.close();
});
