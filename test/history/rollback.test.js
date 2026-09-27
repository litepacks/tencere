import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs/promises";
import {
  Tencere,
  RollbackConflictError,
  StalePlanError
} from "../../src/index.js";

test("Tencere Rollback - Single Key Rollback & Forward RESTORE Mutation Semantics", async () => {
  const db = await Tencere.open({ history: { enabled: true } });

  // 100 SET foo=A, 120 SET foo=B, 150 SET foo=C
  const rA = await db.set("foo", "A");
  const rB = await db.set("foo", "B");
  const rC = await db.set("foo", "C");

  // Rollback to specific sequence rA
  const res1 = await db.rollback("foo", { sequence: rA.version });
  assert.equal(res1.restoredVersion, rA.version);
  assert.ok(res1.newVersion > rC.version); // New forward version
  assert.equal(await db.get("foo"), "A");

  // Verify history contains all revisions including RESTORE mutation (append-only)
  const historyList = [];
  for await (const rev of db.history("foo")) {
    historyList.push(rev);
  }
  assert.equal(historyList.length, 4);
  assert.equal(historyList[0].operation, "RESTORE");
  assert.equal(historyList[0].value, "A");
  assert.equal(historyList[1].value, "C");
  assert.equal(historyList[2].value, "B");
  assert.equal(historyList[3].value, "A");

  // Rollback of rollback: rollback latest restore to revert back to C
  const res2 = await db.rollback("foo", { version: rC.version });
  assert.equal(await db.get("foo"), "C");
  assert.equal(res2.restoredVersion, rC.version);

  await db.close();
});

test("Tencere Rollback - SET -> DELETE -> Rollback", async () => {
  const db = await Tencere.open({ history: { enabled: true } });

  const rSet = await db.set("active_flag", "true");
  await db.delete("active_flag");
  assert.equal(await db.get("active_flag"), undefined);

  // Rollback to version when it was active
  await db.rollback("active_flag", { version: rSet.version });
  assert.equal(await db.get("active_flag"), "true");

  await db.close();
});

test("Tencere Rollback - Counter Increment & Sorted Score Rollback", async () => {
  const db = await Tencere.open({ history: { enabled: true } });

  // 1. Counter rollback
  const c = db.counter("downloads");
  await c.inc(10);
  const r1 = await db.get("downloads", { withVersion: true });
  await c.inc(50);
  assert.equal(await c.value(), 60);

  await c.rollback({ version: r1.version });
  assert.equal(await c.value(), 10);

  // 2. Sorted score rollback
  const scores = db.sorted("leaderboard");
  await scores.set("player1", 100);
  const tBefore = Date.now();
  await new Promise((r) => setTimeout(r, 20));

  await scores.incr("player1", 50);
  assert.equal(await scores.score("player1"), 150);

  // Rollback player1 score to tBefore
  await scores.rollback({ to: tBefore });
  assert.equal(await scores.score("player1"), 100);

  await db.close();
});

test("Tencere Rollback - Scope & Collection Rollback Plans with Conflict Detection", async () => {
  const db = await Tencere.open({ history: { enabled: true } });

  const tenant = db.scope("tenant:corp");
  await tenant.set("name", "Corp Inc");
  await tenant.set("tier", "silver");
  await tenant.set("quota", 1000);

  await new Promise((r) => setTimeout(r, 30));
  const tSnapshot = Date.now();
  await new Promise((r) => setTimeout(r, 30));

  // Change tier and quota, and add new key
  await tenant.set("tier", "gold");
  await tenant.set("quota", 5000);
  await tenant.set("extra_key", "temporary");

  // 1. Generate preview rollback plan
  const plan = await tenant.rollbackPlan({ to: tSnapshot });
  const summary = await plan.summary();

  assert.equal(summary.affectedKeys, 3); // tier, quota, extra_key
  assert.equal(summary.restore, 2);      // tier, quota
  assert.equal(summary.delete, 1);       // extra_key (did not exist at tSnapshot)

  // Stream preview changes
  const changes = [];
  for await (const ch of plan.changes()) {
    changes.push(ch);
  }
  assert.equal(changes.length, 3);

  // 2. Conflict detection: if another writer modifies a planned key before apply
  await tenant.set("tier", "platinum"); // Concurrent modification!

  // Applying plan with default onConflict='abort' must reject!
  await assert.rejects(async () => {
    await plan.apply({ onConflict: "abort" });
  }, RollbackConflictError);

  // 3. Stale plan protection: cannot re-apply an applied plan
  const freshPlan = await tenant.rollbackPlan({ to: tSnapshot });
  await freshPlan.apply();

  // After rollback: tier is silver, quota is 1000, extra_key is gone
  assert.equal(await tenant.get("tier"), "silver");
  assert.equal(await tenant.get("quota"), 1000);
  assert.equal(await tenant.get("extra_key"), undefined);

  // Attempting to re-apply same plan throws StalePlanError
  await assert.rejects(async () => {
    await freshPlan.apply();
  }, StalePlanError);

  await db.close();
});

test("Tencere Rollback - Snapshots, Retention Pinning, and restorePlan", async () => {
  const db = await Tencere.open({ history: { enabled: true } });

  await db.set("state", "initial");

  // Pin snapshot at initial state
  const snapshot = await db.snapshot();
  assert.equal(await snapshot.get("state"), "initial");

  // Modify current state
  await db.set("state", "mutated");
  assert.equal(await db.get("state"), "mutated");

  // Restore plan from snapshot
  const plan = await db.restorePlan(snapshot);
  const sum = await plan.summary();
  assert.equal(sum.restore, 1);

  await plan.apply();
  assert.equal(await db.get("state"), "initial");

  // Release snapshot pin
  await snapshot.close();

  await db.close();
});

test("Tencere Rollback - Persistence and Recovery after Database Restart", async () => {
  const dataDir = "./scratch_history_recovery_test";
  await fs.rm(dataDir, { recursive: true, force: true });

  // 1. Initial run: write, rollback, and write again with WAL
  {
    const db = await Tencere.open(dataDir, {
      durability: "strict",
      history: { enabled: true }
    });

    const r1 = await db.set("db:version", "v1.0.0");
    await db.set("db:version", "v2.0.0");
    assert.equal(await db.get("db:version"), "v2.0.0");

    // Rollback to v1.0.0
    await db.rollback("db:version", { version: r1.version });
    assert.equal(await db.get("db:version"), "v1.0.0");

    await db.close();
  }

  // 2. Recovery run: reopen from same directory
  {
    const db = await Tencere.open(dataDir, {
      durability: "strict",
      history: { enabled: true }
    });

    // Verify rolled back state was persisted and recovered
    assert.equal(await db.get("db:version"), "v1.0.0");

    // History is also recovered
    const revs = [];
    for await (const rev of db.history("db:version")) {
      revs.push(rev);
    }
    assert.ok(revs.length >= 3);
    assert.equal(revs[0].operation, "RESTORE");
    assert.equal(revs[0].value, "v1.0.0");

    await db.close();
  }

  await fs.rm(dataDir, { recursive: true, force: true });
});

test("Tencere Rollback - Reversible Rollback (Rollback itself can be rolled back)", async () => {
  const db = await Tencere.open({ history: { enabled: true } });

  // A -> B -> C
  const rA = await db.set("doc", "A");
  await db.set("doc", "B");
  const rC = await db.set("doc", "C");

  assert.equal(await db.get("doc"), "C");

  // Rollback to A
  const roll1 = await db.rollback("doc", { version: rA.version });
  assert.equal(await db.get("doc"), "A");
  assert.equal(roll1.restoredVersion, rA.version);

  // Rollback to C (reversing the previous rollback)
  const roll2 = await db.rollback("doc", { version: rC.version });
  assert.equal(await db.get("doc"), "C");
  assert.equal(roll2.restoredVersion, rC.version);

  // History audit trail shows full sequence without truncation
  const historyList = [];
  for await (const rev of db.history("doc", { direction: "asc" })) {
    historyList.push(rev);
  }

  assert.equal(historyList.length, 5); // SET A, SET B, SET C, RESTORE A, RESTORE C
  assert.equal(historyList[3].operation, "RESTORE");
  assert.equal(historyList[3].value, "A");
  assert.equal(historyList[4].operation, "RESTORE");
  assert.equal(historyList[4].value, "C");

  await db.close();
});

test("Tencere History - Compaction Boundaries with Active Snapshot Pinning", async () => {
  const db = await Tencere.open({
    history: { enabled: true, retention: "100ms", maxVersions: 10 }
  });

  await db.set("pinned_state", "v1");
  const snap = await db.snapshot();

  await db.set("pinned_state", "v2");
  await db.set("pinned_state", "v3");

  // Snapshot boundary should protect v1 from compaction
  const minSeq = db._engine.historyManager.getMinimumRetainedSequence();
  assert.ok(minSeq !== null && minSeq <= snap._snapshotPin.sequence);

  // Historical read via snapshot succeeds
  assert.equal(await snap.get("pinned_state"), "v1");

  // Releasing snapshot removes the snapshot boundary constraint
  await snap.close();
  const minSeqAfter = db._engine.historyManager.getMinimumRetainedSequence();
  assert.equal(db._engine.historyManager._activeSnapshots.size, 0);

  await db.close();
});
