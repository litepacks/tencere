import test from "node:test";
import assert from "node:assert/strict";
import { Tencere } from "../../src/index.js";

test("Snapshot Retention Torture: Multiple overlapping snapshots pin retention boundary correctly", async () => {
  const db = await Tencere.open({
    history: {
      enabled: true,
      retention: "10ms", // aggressive retention to test pinning
      maxVersions: 5
    }
  });

  try {
    const hm = db._engine.historyManager;

    // Phase 1: Initial state & Snapshot 1
    await db.set("account:balance", 100);
    await db.set("account:status", "bronze");
    const snap1 = await db.snapshot();
    const seq1 = snap1._snapshotPin.sequence;

    // Phase 2: Mutate & Snapshot 2
    await db.set("account:balance", 250);
    await db.set("account:status", "silver");
    const snap2 = await db.snapshot();
    const seq2 = snap2._snapshotPin.sequence;

    // Phase 3: Mutate & Snapshot 3
    await db.set("account:balance", 1000);
    await db.set("account:status", "gold");
    const snap3 = await db.snapshot();
    const seq3 = snap3._snapshotPin.sequence;

    // Mutate state after snap3
    await db.set("account:balance", 5000);
    await db.set("account:status", "platinum");

    // Wait past retention duration
    await new Promise((r) => setTimeout(r, 40));

    // Force purge
    hm.purgeExpiredRevisions();

    // Verify retention boundary is pinned by oldest active snapshot (snap1)
    let minSnap = hm._getMinSnapshotSequence();
    assert.equal(minSnap, seq1, `Active snapshot pin should be seq1: ${seq1}`);

    // All 3 snapshots must read their consistent historical views
    assert.equal(await snap1.get("account:balance"), 100);
    assert.equal(await snap1.get("account:status"), "bronze");

    assert.equal(await snap2.get("account:balance"), 250);
    assert.equal(await snap2.get("account:status"), "silver");

    assert.equal(await snap3.get("account:balance"), 1000);
    assert.equal(await snap3.get("account:status"), "gold");

    // Current state is platinum
    assert.equal(await db.get("account:balance"), 5000);
    assert.equal(await db.get("account:status"), "platinum");

    // Close snap1: boundary must advance to at least snap2
    await snap1.close();
    minSnap = hm._getMinSnapshotSequence();
    assert.equal(minSnap, seq2, `Active snapshot pin must now be seq2: ${seq2}`);

    // snap2 and snap3 still readable
    assert.equal(await snap2.get("account:balance"), 250);
    assert.equal(await snap3.get("account:balance"), 1000);

    // Close snap2
    await snap2.close();
    minSnap = hm._getMinSnapshotSequence();
    assert.equal(minSnap, seq3, `Active snapshot pin must now be seq3: ${seq3}`);

    // Close snap3: all snapshot pins released
    await snap3.close();
    assert.equal(hm._activeSnapshots.size, 0);
    assert.equal(hm._getMinSnapshotSequence(), null);

    const inv = await db.debug.verify();
    assert.equal(inv.valid, true);
  } finally {
    await db.close();
  }
});
