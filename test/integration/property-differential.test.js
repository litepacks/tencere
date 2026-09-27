import test from "node:test";
import assert from "node:assert/strict";
import { Tencere } from "../../src/index.js";

/**
 * Deterministic pseudo-random number generator (Mulberry32) for reproducible property runs.
 */
function createRng(seed) {
  let s = seed >>> 0;
  return function next() {
    s = (s + 0x6d2b79f5) >>> 0;
    let t = Math.imul(s ^ (s >>> 15), 1 | s);
    t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

test("Property-Based & Differential Testing: 500-step randomized operations vs JS reference model", async () => {
  const seed = 133742;
  const rng = createRng(seed);

  const db = await Tencere.open();

  // Reference models
  const refKV = new Map();
  const refMaps = new Map(); // mapName -> Map
  const refSets = new Map(); // setName -> Set
  const refSorted = new Map(); // sortedName -> Map<member, score>
  const refCounters = new Map(); // name -> number

  const keysPool = ["alpha", "beta", "gamma", "delta", "epsilon"];
  const mapNames = ["users", "settings"];
  const setNames = ["tags", "roles"];
  const sortedNames = ["leaderboard"];

  function pick(arr) {
    return arr[Math.floor(rng() * arr.length)];
  }

  function randInt(min, max) {
    return Math.floor(rng() * (max - min + 1)) + min;
  }

  try {
    const totalOps = 500;

    for (let step = 1; step <= totalOps; step++) {
      const opChoice = randInt(1, 8);

      switch (opChoice) {
        // 1. KV SET / GET
        case 1: {
          const k = pick(keysPool);
          const val = randInt(1, 1000);
          refKV.set(k, val);
          await db.set(k, val);

          const actual = await db.get(k);
          assert.equal(actual, val, `[Step ${step}] KV SET/GET mismatch on key '${k}' (seed: ${seed})`);
          break;
        }

        // 2. KV DELETE
        case 2: {
          const k = pick(keysPool);
          refKV.delete(k);
          await db.delete(k);

          const actual = await db.get(k);
          assert.equal(actual, undefined, `[Step ${step}] KV DELETE mismatch on key '${k}'`);
          break;
        }

        // 3. KV INCREMENT
        case 3: {
          const k = pick(keysPool);
          const delta = randInt(1, 5);
          const cur = refKV.has(k) && typeof refKV.get(k) === "number" ? refKV.get(k) : 0;
          const next = cur + delta;
          refKV.set(k, next);

          const actual = await db.increment(k, delta);
          assert.equal(actual, next, `[Step ${step}] KV INCREMENT mismatch on key '${k}'`);
          break;
        }

        // 4. MAP SET & GET
        case 4: {
          const mName = pick(mapNames);
          const k = pick(keysPool);
          const val = { score: randInt(10, 500) };

          if (!refMaps.has(mName)) refMaps.set(mName, new Map());
          refMaps.get(mName).set(k, val);

          const map = db.map(mName);
          await map.set(k, val);
          const actual = await map.get(k);
          assert.deepEqual(actual, val, `[Step ${step}] MAP SET/GET mismatch on '${mName}':'${k}'`);
          break;
        }

        // 5. MAP DELETE
        case 5: {
          const mName = pick(mapNames);
          const k = pick(keysPool);
          if (refMaps.has(mName)) refMaps.get(mName).delete(k);

          const map = db.map(mName);
          await map.delete(k);
          const actual = await map.get(k);
          assert.equal(actual, undefined, `[Step ${step}] MAP DELETE mismatch on '${mName}':'${k}'`);
          break;
        }

        // 6. SET ADD & HAS
        case 6: {
          const sName = pick(setNames);
          const member = pick(keysPool);
          if (!refSets.has(sName)) refSets.set(sName, new Set());
          refSets.get(sName).add(member);

          const set = db.setCollection(sName);
          await set.add(member);
          const actual = await set.has(member);
          assert.equal(actual, true, `[Step ${step}] SET ADD/HAS mismatch on '${sName}':'${member}'`);
          break;
        }

        // 7. SORTED SET & SCORE
        case 7: {
          const sortName = pick(sortedNames);
          const member = pick(keysPool);
          const score = randInt(1, 100);

          if (!refSorted.has(sortName)) refSorted.set(sortName, new Map());
          refSorted.get(sortName).set(member, score);

          const sorted = db.sorted(sortName);
          await sorted.set(member, score);
          const actualScore = await sorted.score(member);
          assert.equal(actualScore, score, `[Step ${step}] SORTED score mismatch on '${sortName}':'${member}'`);
          break;
        }

        // 8. SORTED TOP COMPARISON
        case 8: {
          const sortName = pick(sortedNames);
          const sorted = db.sorted(sortName);
          const actualTop = await sorted.top(3);

          const memberMap = refSorted.get(sortName) || new Map();
          const expectedSorted = Array.from(memberMap.entries())
            .map(([member, score]) => ({ member, score }))
            .sort((a, b) => b.score - a.score || a.member.localeCompare(b.member))
            .slice(0, 3);

          assert.equal(
            actualTop.length,
            expectedSorted.length,
            `[Step ${step}] SORTED top count mismatch`
          );

          for (let i = 0; i < actualTop.length; i++) {
            assert.equal(actualTop[i].score, expectedSorted[i].score);
          }
          break;
        }
      }
    }

    // Comprehensive invariant validation at end of randomized run
    const inv = await db.debug.verify();
    assert.equal(inv.valid, true, `Invariants violation: ${inv.errors.join(", ")}`);
  } finally {
    await db.close();
  }
});
