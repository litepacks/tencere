import test from "node:test";
import assert from "node:assert/strict";
import { TencereSync } from "../../src/sync/index.js";

test("TencereSync - synchronous local operations", () => {
  const db = new TencereSync();

  // Basic KV
  db.set("foo", "bar");
  assert.equal(db.get("foo"), "bar");
  assert.equal(db.has("foo"), true);
  db.delete("foo");
  assert.equal(db.has("foo"), false);

  // Counter
  const count = db.counter("page:views");
  count.inc();
  count.add(10);
  assert.equal(count.value(), 11);
  count.dec();
  assert.equal(count.value(), 10);

  // Map
  const users = db.map("users");
  users.set("u1", { name: "Alice" });
  assert.deepEqual(users.get("u1"), { name: "Alice" });
  assert.equal(users.size(), 1);

  // Set
  const tags = db.setCollection("tags");
  tags.add("node");
  tags.add("db");
  assert.equal(tags.has("node"), true);
  assert.equal(tags.size(), 2);

  // Sorted
  const scores = db.sorted("scores");
  scores.set("alice", 100);
  scores.set("bob", 200);
  assert.equal(scores.score("alice"), 100);
  assert.equal(scores.rank("bob"), 1);
  assert.deepEqual(scores.top(2), [
    { member: "bob", score: 200 },
    { member: "alice", score: 100 }
  ]);

  db.close();
});
