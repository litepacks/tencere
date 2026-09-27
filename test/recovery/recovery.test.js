import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs/promises";
import { Tencere } from "../../src/index.js";

test("Tencere Recovery - WAL persistence and deterministic replay", async () => {
  const dataDir = "./scratch_recovery_test";
  await fs.rm(dataDir, { recursive: true, force: true });

  // 1. Initial run: write keys, counters, and patches
  {
    const db = await Tencere.open(dataDir, { durability: "strict" });

    await db.set("user:100", { name: "Ahmet", active: true });
    await db.set("user:200", { name: "Mehmet", active: false });
    await db.increment("page:views", 10);
    await db.patch("user:100", { $set: { email: "ahmet@example.com" } });
    await db.delete("user:200");

    await db.close();
  }

  // 2. Recovery run: reopen from same directory
  {
    const db = await Tencere.open(dataDir, { durability: "strict" });

    // Verify user:100 was recovered with patched email
    const user100 = await db.get("user:100");
    assert.ok(user100);
    assert.equal(user100.name, "Ahmet");
    assert.equal(user100.email, "ahmet@example.com");
    assert.equal(user100.active, true);

    // Verify user:200 was deleted
    assert.equal(await db.get("user:200"), undefined);
    assert.equal(await db.has("user:200"), false);

    // Verify counter was recovered
    assert.equal(await db.get("page:views"), 10);

    // Verify further writes work after recovery
    await db.increment("page:views", 5);
    assert.equal(await db.get("page:views"), 15);

    await db.close();
  }

  await fs.rm(dataDir, { recursive: true, force: true });
});
