import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs/promises";
import path from "node:path";
import {
  Tencere,
  InvalidTimeSeriesValueError,
  TagLimitExceededError,
  SeriesCardinalityExceededError,
  ReadOnlyDatabaseError
} from "../../src/index.js";

const TEST_DIR = path.resolve("./.test_timeseries_data");

async function cleanup() {
  await fs.rm(TEST_DIR, { recursive: true, force: true }).catch(() => {});
}

test("TimeSeries - Basic DX: add, latest, and number validations", async () => {
  const db = await Tencere.open();
  const temp = db.timeseries("temperature");

  // Default at: Date.now()
  const p1 = await temp.add(21.4);
  assert.equal(p1.value, 21.4);
  assert.ok(typeof p1.timestamp === "number");
  assert.ok(typeof p1.sequence === "bigint");
  assert.ok(p1.id.includes(":"));

  // Explicit at
  const t2 = Date.now() + 100;
  const p2 = await temp.add(21.8, { at: t2 });
  assert.equal(p2.value, 21.8);
  assert.equal(p2.timestamp, t2);

  // Latest single
  const latestSingle = await temp.latest();
  assert.equal(latestSingle.value, 21.8);

  // Latest N
  const latestTwo = await temp.latest(2);
  assert.equal(latestTwo.length, 2);
  assert.equal(latestTwo[0].value, 21.8);
  assert.equal(latestTwo[1].value, 21.4);

  // Reject non-numbers and invalid numbers
  await assert.rejects(async () => {
    await temp.add(NaN);
  }, InvalidTimeSeriesValueError);

  await assert.rejects(async () => {
    await temp.add(Infinity);
  }, InvalidTimeSeriesValueError);

  await assert.rejects(async () => {
    await temp.add(-Infinity);
  }, InvalidTimeSeriesValueError);

  await assert.rejects(async () => {
    await temp.add("21.5");
  }, InvalidTimeSeriesValueError);

  await db.close();
});

test("TimeSeries - Range and relative time parsing (between, values, iterate)", async () => {
  const db = await Tencere.open();
  const cpu = db.timeseries("cpu");

  const base = 1700000000000;
  await cpu.add(10, { at: base + 1000 });
  await cpu.add(20, { at: base + 2000 });
  await cpu.add(30, { at: base + 3000 });
  await cpu.add(40, { at: base + 4000 });

  // Range with numeric boundaries
  const pts = await cpu.between(base + 1500, base + 3500).values();
  assert.equal(pts.length, 2);
  assert.equal(pts[0].value, 20);
  assert.equal(pts[1].value, 30);

  // Range with Date objects
  const datePts = await cpu.between(new Date(base + 1500), new Date(base + 3500)).values();
  assert.equal(datePts.length, 2);
  assert.equal(datePts[0].value, 20);

  // Empty range
  const empty = await cpu.between(base + 5000, base + 6000).values();
  assert.deepEqual(empty, []);

  // Streaming iterator
  const streamed = [];
  for await (const pt of cpu.between(base, base + 2500).iterate()) {
    streamed.push(pt.value);
  }
  assert.deepEqual(streamed, [10, 20]);

  // Relative time support: '1h ago' to 'now'
  const now = Date.now();
  await cpu.add(99, { at: now - 60000 }); // 1m ago
  const recent = await cpu.between("5m ago", "now").values();
  assert.ok(recent.length >= 1);
  assert.ok(recent.some((p) => p.value === 99));

  await db.close();
});

test("TimeSeries - Out-of-order writes and same-timestamp duplicate ordering", async () => {
  const db = await Tencere.open();
  const ts = db.timeseries("late_arrivals");

  const t1 = 1000;
  const t2 = 2000;
  const t3 = 3000;

  // Insert out of order
  await ts.add(20, { at: t2 });
  await ts.add(30, { at: t3 });
  await ts.add(10, { at: t1 }); // late sample

  const ordered = await ts.between(0, 5000).values();
  assert.deepEqual(ordered.map((p) => p.value), [10, 20, 30]);

  // Multiple points at exact same timestamp
  const tSame = 4000;
  const pA = await ts.add(100, { at: tSame });
  const pB = await ts.add(200, { at: tSame });
  const pC = await ts.add(300, { at: tSame });

  const sameTimePoints = await ts.between(3999, 4001).values();
  assert.equal(sameTimePoints.length, 3);
  assert.equal(sameTimePoints[0].value, 100);
  assert.equal(sameTimePoints[1].value, 200);
  assert.equal(sameTimePoints[2].value, 300);
  assert.ok(sameTimePoints[0].sequence < sameTimePoints[1].sequence);
  assert.ok(sameTimePoints[1].sequence < sameTimePoints[2].sequence);

  await db.close();
});

test("TimeSeries - Tags canonicalization, limits, and where() filtering", async () => {
  const db = await Tencere.open();
  const reqs = db.timeseries("http_requests", {
    tags: {
      indexed: ["method", "status", "region"]
    },
    limits: {
      maxTags: 4,
      maxTagKeyLength: 16,
      maxTagValueLength: 32,
      maxSeries: 10
    }
  });

  // Tag canonicalization: different key order must map to exact same seriesId
  const p1 = await reqs.add(1, { tags: { method: "GET", region: "eu", status: "200" } });
  const p2 = await reqs.add(1, { tags: { region: "eu", status: "200", method: "GET" } });
  assert.equal(p1.id.split(":")[0], p2.id.split(":")[0]);

  await reqs.add(1, { tags: { method: "POST", status: "201", region: "us" } });
  await reqs.add(1, { tags: { method: "GET", status: "500", region: "eu" } });

  // Filter by tags using where()
  const euGets = await reqs.where({ method: "GET", region: "eu" }).values();
  assert.equal(euGets.length, 3); // 200 (x2) and 500

  const usPosts = await reqs.where({ method: "POST" }).values();
  assert.equal(usPosts.length, 1);
  assert.equal(usPosts[0].tags.region, "us");

  // Tag limits: maxTags
  await assert.rejects(async () => {
    await reqs.add(1, {
      tags: { a: "1", b: "2", c: "3", d: "4", e: "5" }
    });
  }, TagLimitExceededError);

  // Tag limits: maxTagKeyLength
  await assert.rejects(async () => {
    await reqs.add(1, {
      tags: { very_long_tag_key_exceeding_limit: "val" }
    });
  }, TagLimitExceededError);

  await db.close();
});

test("TimeSeries - Aggregations (count, sum, min, max, avg, first, last)", async () => {
  const db = await Tencere.open();
  const ts = db.timeseries("metrics");

  const base = 10000;
  await ts.add(10, { at: base + 100 });
  await ts.add(20, { at: base + 200 });
  await ts.add(-5, { at: base + 300 });
  await ts.add(35, { at: base + 400 });

  assert.equal(await ts.count(), 4);
  assert.equal(await ts.sum(), 60);
  assert.equal(await ts.min(), -5);
  assert.equal(await ts.max(), 35);
  assert.equal(await ts.avg(), 15);
  assert.equal(await ts.first(), 10);
  assert.equal(await ts.last(), 35);

  // Sub-range aggregations
  const sub = ts.between(base + 150, base + 350);
  assert.equal(await sub.count(), 2);
  assert.equal(await sub.sum(), 15);
  assert.equal(await sub.min(), -5);
  assert.equal(await sub.max(), 20);
  assert.equal(await sub.avg(), 7.5);

  // Query immutability: q1 is not mutated by q2
  const q1 = ts.between(base, base + 500);
  const q2 = q1.limit(2);
  assert.equal(await q1.count(), 4);
  assert.equal(await q2.count(), 2);

  await db.close();
});

test("TimeSeries - Bucketing with deterministic UTC/epoch boundaries", async () => {
  const db = await Tencere.open();
  const latency = db.timeseries("latency");

  // Buckets of 60,000ms (1 minute)
  // Minute 0: [0, 60000)
  await latency.add(100, { at: 10000 });
  await latency.add(200, { at: 20000 });
  await latency.add(300, { at: 30000 });

  // Minute 1: [60000, 120000)
  await latency.add(400, { at: 70000 });
  await latency.add(600, { at: 80000 });

  // Minute 2: [120000, 180000)
  await latency.add(50, { at: 130000 });

  // Bucket avg
  const avgBuckets = await latency.between(0, 200000).bucket("1m").avg();
  assert.equal(avgBuckets.length, 3);

  // First bucket
  assert.equal(avgBuckets[0].start, 0);
  assert.equal(avgBuckets[0].end, 60000);
  assert.equal(avgBuckets[0].value, 200); // (100+200+300)/3
  assert.equal(avgBuckets[0].count, 3);

  // Second bucket
  assert.equal(avgBuckets[1].start, 60000);
  assert.equal(avgBuckets[1].end, 120000);
  assert.equal(avgBuckets[1].value, 500); // (400+600)/2
  assert.equal(avgBuckets[1].count, 2);

  // Third bucket
  assert.equal(avgBuckets[2].start, 120000);
  assert.equal(avgBuckets[2].end, 180000);
  assert.equal(avgBuckets[2].value, 50);
  assert.equal(avgBuckets[2].count, 1);

  // Bucket sum & max
  const sumBuckets = await latency.between(0, 100000).bucket("1m").sum();
  assert.equal(sumBuckets[0].value, 600);
  assert.equal(sumBuckets[1].value, 1000);

  const maxBuckets = await latency.between(0, 100000).bucket("1m").max();
  assert.equal(maxBuckets[0].value, 300);
  assert.equal(maxBuckets[1].value, 600);

  await db.close();
});

test("TimeSeries - Batch ingestion (addMany)", async () => {
  const db = await Tencere.open();
  const ts = db.timeseries("batch_metrics");

  const base = 2000000;
  const points = await ts.addMany([
    [base + 10, 1.1],
    [base + 20, 2.2],
    { at: base + 30, value: 3.3, tags: { host: "api-1" } },
    { at: base + 40, value: 4.4, tags: { host: "api-2" } }
  ]);

  assert.equal(points.length, 4);
  assert.equal(points[0].value, 1.1);
  assert.equal(points[2].tags.host, "api-1");

  assert.equal(await ts.count(), 4);
  const host1 = await ts.where({ host: "api-1" }).values();
  assert.equal(host1.length, 1);
  assert.equal(host1[0].value, 3.3);

  await db.close();
});

test("TimeSeries - Realtime watch() stream and filtered watch", async () => {
  const db = await Tencere.open();
  const ts = db.timeseries("live_feed");

  const seen = [];
  const consumerPromise = (async () => {
    for await (const pt of ts.watch()) {
      seen.push(pt.value);
      if (seen.length === 3) break;
    }
  })();

  await new Promise((r) => setTimeout(r, 20));
  await ts.add(10);
  await ts.add(20);
  await ts.add(30);

  await consumerPromise;
  assert.deepEqual(seen, [10, 20, 30]);

  // Filtered watch
  const filteredSeen = [];
  const filteredPromise = (async () => {
    for await (const pt of ts.watch({ where: { env: "prod" } })) {
      filteredSeen.push(pt.value);
      if (filteredSeen.length === 2) break;
    }
  })();

  await new Promise((r) => setTimeout(r, 20));
  await ts.add(100, { tags: { env: "dev" } });
  await ts.add(200, { tags: { env: "prod" } });
  await ts.add(300, { tags: { env: "stage" } });
  await ts.add(400, { tags: { env: "prod" } });

  await filteredPromise;
  assert.deepEqual(filteredSeen, [200, 400]);

  await db.close();
});

test("TimeSeries - Retention purging", async () => {
  const db = await Tencere.open();
  const ts = db.timeseries("retention_test", {
    retention: "1h"
  });

  const now = Date.now();
  // Old point: 2 hours ago
  await ts.add(1, { at: now - 2 * 3600 * 1000 });
  // Recent point: 10 minutes ago
  await ts.add(2, { at: now - 10 * 60 * 1000 });
  // Current point
  await ts.add(3, { at: now });

  assert.equal(await ts.count(), 3);

  const purgeRes = await ts.purgeRetention();
  assert.equal(purgeRes.purged, 1);
  assert.equal(await ts.count(), 2);

  const remaining = await ts.values();
  assert.deepEqual(remaining.map((p) => p.value), [2, 3]);

  await db.close();
});

test("TimeSeries - Deletion and Correction mutations", async () => {
  const db = await Tencere.open();
  const ts = db.timeseries("sensor");

  const base = 50000;
  const p1 = await ts.add(10, { at: base + 10 });
  const p2 = await ts.add(20, { at: base + 20 });
  const p3 = await ts.add(30, { at: base + 30 });

  // Correct p2
  const corrected = await ts.correct(p2.id, { value: 25 });
  assert.equal(corrected.value, 25);
  const updatedP2 = await ts.latest(2);
  assert.equal(updatedP2[0].value, 30);
  assert.equal(updatedP2[1].value, 25);

  // Delete range [base+5, base+25] (deletes p1 and p2)
  const delRes = await ts.delete({ from: base + 5, to: base + 25 });
  assert.equal(delRes.deleted, 2);
  assert.equal(await ts.count(), 1);
  assert.equal((await ts.latest()).value, 30);

  await db.close();
});

test("TimeSeries - Historical database view db.at() visibility", async () => {
  const db = await Tencere.open({ history: { enabled: true } });
  const ts = db.timeseries("historical_ts");

  const base = 1000;
  // Step 1: Add first two points
  const p1 = await ts.add(10, { at: base + 100 });
  const p2 = await ts.add(20, { at: base + 200 });

  const seqCheckpoint = p2.sequence;

  // Step 2: Add third point later (even if its timestamp is in the past!)
  const p3 = await ts.add(15, { at: base + 150 });

  // Current view sees all 3
  assert.equal(await ts.count(), 3);

  // Historical view db.at({ sequence: seqCheckpoint }) only sees points up to sequence seqCheckpoint
  const past = db.at({ sequence: seqCheckpoint });
  const pastTs = past.timeseries("historical_ts");

  assert.equal(await pastTs.count(), 2);
  const pastVals = await pastTs.values();
  assert.deepEqual(pastVals.map((p) => p.value), [10, 20]);

  // Historical view is read-only
  await assert.rejects(async () => {
    await pastTs.add(99);
  }, ReadOnlyDatabaseError);

  await assert.rejects(async () => {
    await pastTs.delete({ from: 0, to: 10000 });
  }, ReadOnlyDatabaseError);

  await db.close();
});

test("TimeSeries - WAL Persistence and Replay Recovery", async () => {
  await cleanup();
  const db1 = await Tencere.open(TEST_DIR, {
    daktilo: { durability: "strict" }
  });

  const ts1 = db1.timeseries("persistent_metrics", {
    tags: { indexed: ["service"] }
  });

  await ts1.add(42.5, { at: 1000, tags: { service: "auth" } });
  await ts1.add(55.2, { at: 2000, tags: { service: "api" } });
  await ts1.add(67.8, { at: 3000, tags: { service: "auth" } });

  assert.equal(await ts1.count(), 3);
  await db1.close();

  // Reopen database from disk and verify WAL replay recovers points and tags
  const db2 = await Tencere.open(TEST_DIR);
  const ts2 = db2.timeseries("persistent_metrics");

  assert.equal(await ts2.count(), 3);
  const authPts = await ts2.where({ service: "auth" }).values();
  assert.equal(authPts.length, 2);
  assert.equal(authPts[0].value, 42.5);
  assert.equal(authPts[1].value, 67.8);

  const stats = await ts2.stats();
  assert.equal(stats.points, 3);
  assert.equal(stats.series, 2);

  await db2.close();
  await cleanup();
});

test("TimeSeries - Scope and System Telemetry", async () => {
  const db = await Tencere.open();

  // Scope timeseries
  const tenantScope = db.scope("tenant:corp");
  const tenantCpu = tenantScope.timeseries("cpu");
  await tenantCpu.add(85.5);
  assert.equal(await tenantCpu.count(), 1);

  // System Telemetry
  await db._engine.recordTelemetry("$system.latency", 12.4, { route: "/login" });
  await db._engine.recordTelemetry("$system.latency", 8.2, { route: "/login" });
  await db._engine.recordTelemetry("$system.latency", 25.0, { route: "/checkout" });

  const sysTs = db.timeseries("$system.latency");
  assert.equal(await sysTs.count(), 3);
  const loginLat = await sysTs.where({ route: "/login" }).avg();
  assert.equal(loginLat, 10.3);

  await db.close();
});
