import test from "node:test";
import assert from "node:assert/strict";
import { OrderedIndex } from "../../src/core/ordered-index.js";

test("OrderedIndex - insert, rank, score, delete, range", () => {
  const index = new OrderedIndex();

  index.insert(100, "alice");
  index.insert(200, "bob");
  index.insert(150, "charlie");
  index.insert(50, "david");

  assert.equal(index.length, 4);
  assert.equal(index.score("bob"), 200);
  assert.equal(index.score("david"), 50);

  // Ranks (ascending: david(0), alice(1), charlie(2), bob(3))
  assert.equal(index.rank("david"), 0);
  assert.equal(index.rank("alice"), 1);
  assert.equal(index.rank("charlie"), 2);
  assert.equal(index.rank("bob"), 3);

  // Reverse ranks (descending: bob(0), charlie(1), alice(2), david(3))
  assert.equal(index.rank("bob", { reverse: true }), 0);
  assert.equal(index.rank("charlie", { reverse: true }), 1);

  // Top and Bottom
  const top2 = index.top(2);
  assert.deepEqual(top2, [
    { member: "bob", score: 200 },
    { member: "charlie", score: 150 }
  ]);

  const bot2 = index.bottom(2);
  assert.deepEqual(bot2, [
    { member: "david", score: 50 },
    { member: "alice", score: 100 }
  ]);

  // Range by score
  const range = index.rangeByScore(100, 180);
  assert.deepEqual(range, [
    { member: "alice", score: 100 },
    { member: "charlie", score: 150 }
  ]);

  // Update score
  index.insert(300, "david");
  assert.equal(index.length, 4);
  assert.equal(index.score("david"), 300);
  assert.equal(index.rank("david", { reverse: true }), 0);

  // Delete
  assert.equal(index.delete("alice"), true);
  assert.equal(index.length, 3);
  assert.equal(index.has("alice"), false);
});
