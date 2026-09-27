import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { Tencere, FaultInjectionError } from "../../src/index.js";

test("Fault Injection: deterministic fault triggers and recovery invariants", async () => {
  const tmpDir = await fs.mkdtemp(path.join(os.tmpdir(), "tencere-fault-"));

  try {
    const db = await Tencere.open(tmpDir, { history: { enabled: true } });

    // 1. Inject fault before log flush on 2nd occurrence
    db.debug.fault.inject("before-log-flush", { occurrence: 2 });

    await db.set("key:1", "val:1"); // 1st occurrence: succeeds

    // 2nd occurrence: must throw simulated fault
    await assert.rejects(
      async () => {
        await db.set("key:2", "val:2");
      },
      (err) => err instanceof FaultInjectionError && err.point === "before-log-flush"
    );

    // Verify database remains valid and invariants hold
    const inv = await db.debug.verify();
    assert.equal(inv.valid, true);

    assert.equal(await db.get("key:1"), "val:1");
    // key:2 failed before log flush
    assert.equal(await db.get("key:2"), undefined);

    // 2. Inject fault during checkpoint
    db.debug.fault.clear();
    db.debug.fault.inject("during-checkpoint", { occurrence: 1 });

    await assert.rejects(
      async () => {
        await db.checkpoint();
      },
      (err) => err instanceof FaultInjectionError && err.point === "during-checkpoint"
    );

    // 3. Inject fault during rollback
    db.debug.fault.clear();
    db.debug.fault.inject("during-rollback", { occurrence: 1 });

    await assert.rejects(
      async () => {
        await db.rollback("key:1");
      },
      (err) => err instanceof FaultInjectionError && err.point === "during-rollback"
    );

    db.debug.fault.clear();
    await db.close();
  } finally {
    await fs.rm(tmpDir, { recursive: true, force: true }).catch(() => {});
  }
});

test("Torn-write simulation: truncated active log tail recovers to last valid boundary", async () => {
  const tmpDir = await fs.mkdtemp(path.join(os.tmpdir(), "tencere-torn-"));

  try {
    const db1 = await Tencere.open(tmpDir, { history: { enabled: true } });

    // Write 5 valid records
    for (let i = 1; i <= 5; i++) {
      await db1.set(`metric:${i}`, i * 10);
    }

    const validHashBeforeCrash = await db1.debug.stateHash();
    await db1.close();

    // Locate active log file and append partial corrupted tail bytes
    const files = await fs.readdir(tmpDir);
    const logFile = files.find((f) => f.endsWith(".log") || f.includes(".wal") || f.includes(".seg"));

    if (logFile) {
      const fullLogPath = path.join(tmpDir, logFile);
      // Append a torn write (half a header, corrupted bytes at active tail)
      const garbageBytes = Buffer.from([0xde, 0xad, 0xbe, 0xef, 0x01, 0x02]);
      await fs.appendFile(fullLogPath, garbageBytes);
    }

    // Restart database: recovery must tolerate partial active tail and restore valid state
    const db2 = await Tencere.open(tmpDir, { history: { enabled: true } });

    // Verify all 5 valid records survived
    for (let i = 1; i <= 5; i++) {
      assert.equal(await db2.get(`metric:${i}`), i * 10);
    }

    const replayedHash = await db2.debug.stateHash();
    assert.equal(replayedHash, validHashBeforeCrash);

    const inv = await db2.debug.verify();
    assert.equal(inv.valid, true);

    await db2.close();
  } finally {
    await fs.rm(tmpDir, { recursive: true, force: true }).catch(() => {});
  }
});

test("Checkpoint Torture Test: repeated write -> checkpoint -> restart cycles", async () => {
  const tmpDir = await fs.mkdtemp(path.join(os.tmpdir(), "tencere-chk-"));

  try {
    let db = await Tencere.open(tmpDir, { history: { enabled: true } });

    for (let cycle = 1; cycle <= 4; cycle++) {
      // Write a batch of mutations across KV, Sorted, and Map
      for (let i = 0; i < 10; i++) {
        const id = (cycle - 1) * 10 + i;
        await db.set(`item:${id}`, { cycle, id });
        await db.sorted("scores").set(`player:${id}`, id * 100);
        await db.map("settings").set(`conf:${id}`, `val:${id}`);
      }

      // Checkpoint
      await db.checkpoint();

      // Write mutations after checkpoint
      await db.set(`after_chk:${cycle}`, `post-${cycle}`);

      // Restart instance
      const expectedHash = await db.debug.stateHash();
      await db.close();

      db = await Tencere.open(tmpDir, { history: { enabled: true } });

      // State after recovery must match exactly
      const recoveredHash = await db.debug.stateHash();
      assert.equal(
        recoveredHash,
        expectedHash,
        `Hash mismatch after checkpoint cycle ${cycle} recovery`
      );

      const inv = await db.debug.verify();
      assert.equal(inv.valid, true);
    }

    await db.close();
  } finally {
    await fs.rm(tmpDir, { recursive: true, force: true }).catch(() => {});
  }
});
