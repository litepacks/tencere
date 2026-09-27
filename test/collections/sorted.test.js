import test from "node:test";
import assert from "node:assert/strict";
import { Tencere } from "../../src/index.js";

test("Tencere Collections - Sorted collections and values", async () => {
  const db = await Tencere.open();
  const scores = db.sorted("game:scores");

  // Numbers only
  await scores.set("ahmet", 980);
  await scores.set("john", 720);
  await scores.set("elena", 1250);
  await scores.set("zeynep", 850);

  assert.equal(await scores.score("ahmet"), 980);
  assert.equal(await scores.count(), 4);

  // Increment
  await scores.incr("ahmet", 20);
  assert.equal(await scores.score("ahmet"), 1000);

  // Ranks
  // Ascending order: john(720), zeynep(850), ahmet(1000), elena(1250)
  assert.equal(await scores.rank("john"), 0);
  assert.equal(await scores.rank("elena"), 3);

  // Descending rank (leaderboard place: elena is #0, ahmet is #1)
  assert.equal(await scores.rank("elena", { reverse: true }), 0);
  assert.equal(await scores.rank("ahmet", { reverse: true }), 1);

  // Top 2
  const top2 = await scores.top(2);
  assert.deepEqual(top2, [
    { member: "elena", score: 1250 },
    { member: "ahmet", score: 1000 }
  ]);

  // Bottom 2
  const bot2 = await scores.bottom(2);
  assert.deepEqual(bot2, [
    { member: "john", score: 720 },
    { member: "zeynep", score: 850 }
  ]);

  // Fluent range queries
  const betweenResults = await scores.between(800, 1100).limit(10).entries();
  assert.deepEqual(betweenResults, [
    { member: "zeynep", score: 850 },
    { member: "ahmet", score: 1000 }
  ]);

  const aboveResults = await scores.above(900).desc().take(2);
  assert.deepEqual(aboveResults, [
    { member: "elena", score: 1250 },
    { member: "ahmet", score: 1000 }
  ]);

  // Sorted values (decoupled value storage)
  await scores.set("user:vip", {
    score: 2500,
    value: { name: "VIP User", country: "TR" }
  });

  const top1WithVal = await scores.top(1);
  assert.deepEqual(top1WithVal[0], {
    member: "user:vip",
    score: 2500,
    value: { name: "VIP User", country: "TR" }
  });

  // Additional query methods: below, asc, offset, getValue, size, delete
  const belowResults = await scores.below(850).asc().offset(1).take(2);
  assert.equal(belowResults.length, 1);
  assert.equal(belowResults[0].member, "zeynep");

  assert.equal(await scores.size(), 5);
  assert.deepEqual(await scores.getValue("user:vip"), { name: "VIP User", country: "TR" });

  assert.equal(await scores.delete("ahmet"), true);
  assert.equal(await scores.size(), 4);

  await db.close();
});
