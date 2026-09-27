import test from "node:test";
import assert from "node:assert/strict";
import { Tencere, ReadOnlyDatabaseError } from "../../src/index.js";

test("Tencere Time Travel - Single Key Historical Reads and Precedence", async () => {
  const db = await Tencere.open({ history: { enabled: true } });

  const r1 = await db.set("setting", "alpha");
  await new Promise((r) => setTimeout(r, 20));
  const tMid = Date.now();
  await new Promise((r) => setTimeout(r, 20));

  const r2 = await db.set("setting", "beta");
  const r3 = await db.set("setting", "gamma");

  // Read at specific sequence
  assert.equal(await db.get("setting", { atSequence: r1.version }), "alpha");
  assert.equal(await db.get("setting", { atSequence: r2.version }), "beta");
  assert.equal(await db.get("setting", { atSequence: r3.version }), "gamma");

  // Read by version
  assert.equal(await db.get("setting", { version: r1.version }), "alpha");
  assert.equal(await db.get("setting", { version: r2.version }), "beta");

  // Read by timestamp
  assert.equal(await db.get("setting", { at: tMid }), "alpha");

  // Conflicting selectors reject
  await assert.rejects(async () => {
    await db.get("setting", { atSequence: r1.version, version: r2.version });
  });

  await db.close();
});

test("Tencere Time Travel - db.at() Generic Read-Only Historical View", async () => {
  const db = await Tencere.open({ history: { enabled: true } });

  // 1. Setup initial state
  await db.set("config:title", "First Title");
  await db.map("users").set("42", { name: "Alice", role: "admin" });
  await db.sorted("scores").set("player1", 100);
  await db.counter("page:views").inc(10);
  await db.scope("tenant:42").set("status", "pending");

  const rInit = await db.get("config:title", { withVersion: true });

  await new Promise((r) => setTimeout(r, 30));
  const tCheckpoint = Date.now();
  await new Promise((r) => setTimeout(r, 30));

  // 2. Modify state later
  await db.set("config:title", "Second Title");
  await db.map("users").set("42", { name: "Alice", role: "superadmin" });
  await db.sorted("scores").set("player1", 250);
  await db.sorted("scores").set("player2", 300);
  await db.counter("page:views").inc(15);
  await db.scope("tenant:42").set("status", "active");

  // 3. Current state reflects latest values
  assert.equal(await db.get("config:title"), "Second Title");
  assert.equal((await db.map("users").get("42")).role, "superadmin");
  assert.equal(await db.sorted("scores").score("player1"), 250);
  assert.equal(await db.counter("page:views").value(), 25);
  assert.equal(await db.scope("tenant:42").get("status"), "active");

  // 4. Historical View db.at(tCheckpoint)
  const past = db.at(tCheckpoint);

  assert.equal(await past.get("config:title"), "First Title");
  assert.equal(await past.has("config:title"), true);
  assert.equal((await past.map("users").get("42")).role, "admin");
  assert.equal(await past.sorted("scores").score("player1"), 100);
  assert.equal(await past.sorted("scores").score("player2"), undefined); // player2 did not exist yet

  // Top 10 on historical sorted collection
  const topPast = await past.sorted("scores").top(10);
  assert.equal(topPast.length, 1);
  assert.equal(topPast[0].member, "player1");
  assert.equal(topPast[0].score, 100);

  // Historical counter
  assert.equal(await past.counter("page:views").value(), 10);

  // Historical scope
  assert.equal(await past.scope("tenant:42").get("status"), "pending");

  // Historical view by sequence
  const pastBySeq = db.at({ sequence: rInit.version });
  assert.equal(await pastBySeq.get("config:title"), "First Title");

  // 5. Historical view MUST BE READ-ONLY
  await assert.rejects(async () => {
    await past.set("config:title", "Illegal");
  }, ReadOnlyDatabaseError);

  await assert.rejects(async () => {
    await past.delete("config:title");
  }, ReadOnlyDatabaseError);

  await assert.rejects(async () => {
    await past.increment("page:views", 1);
  }, ReadOnlyDatabaseError);

  await assert.rejects(async () => {
    await past.map("users").set("42", { name: "Hacker" });
  }, ReadOnlyDatabaseError);

  await assert.rejects(async () => {
    await past.sorted("scores").set("player1", 999);
  }, ReadOnlyDatabaseError);

  await assert.rejects(async () => {
    await past.counter("page:views").inc(5);
  }, ReadOnlyDatabaseError);

  await db.close();
});

test("Tencere Time Travel - Historical TTL Evaluation without applying current clock", async () => {
  const db = await Tencere.open({ history: { enabled: true } });

  // Key with 100ms TTL
  await db.set("temp:session", "token_abc", { ttl: "100ms" });
  const tCreated = Date.now();

  // Query at creation time: key is alive!
  const entryAlive = await db.get("temp:session", { at: tCreated });
  assert.equal(entryAlive, "token_abc");

  // Query at tCreated + 200ms (in the future relative to expiration): key should be considered expired
  const entryExpired = await db.get("temp:session", { at: tCreated + 200 });
  assert.equal(entryExpired, undefined);

  await db.close();
});

test("Tencere Time Travel - Deletion and Recreation Historical Reconstruction", async () => {
  const db = await Tencere.open({ history: { enabled: true } });

  const r1 = await db.set("lifecycle", "v1");
  await db.delete("lifecycle");
  const r3 = await db.set("lifecycle", "v3");

  const revs = [];
  for await (const rev of db.history("lifecycle", { direction: "asc" })) {
    revs.push(rev);
  }
  assert.equal(revs.length, 3);
  const delSeq = revs[1].sequence;

  // At sequence r1: exists with v1
  assert.equal(await db.get("lifecycle", { atSequence: r1.version }), "v1");

  // At sequence of deletion: absent (deleted)
  assert.equal(await db.get("lifecycle", { atSequence: delSeq }), undefined);

  // At sequence r3: exists with v3
  assert.equal(await db.get("lifecycle", { atSequence: r3.version }), "v3");

  await db.close();
});
