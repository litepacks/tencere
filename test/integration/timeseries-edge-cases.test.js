import test from "node:test";
import assert from "node:assert/strict";
import { Tencere, InvalidTimeSeriesValueError } from "../../src/index.js";
import { PointCodec } from "../../src/timeseries/codec.js";
import { fnv1a } from "../../src/timeseries/series.js";

test("TS Edge Case 1: 64-bit FNV-1a and collision-proof series resolution", async () => {
  const db = await Tencere.open();
  const ts = db.timeseries("collision_guard");

  // Verify 64-bit hash format (16 hex characters)
  const hash = fnv1a("host=web-1,region=us-east");
  assert.equal(typeof hash, "string");
  assert.equal(hash.length, 16);

  // Normal distinct series
  await ts.add(42, { tags: { host: "server-1", env: "prod" } });
  await ts.add(84, { tags: { host: "server-2", env: "prod" } });

  const stats = await ts.stats();
  assert.equal(stats.series, 2);

  // Artificial collision test on the registry
  const registry = ts._storage.registry;
  const originalTags = { cluster: "alpha", node: "1" };

  const res1 = registry.resolveSeries(originalTags);
  assert.ok(res1.seriesId.startsWith("s_"));

  // Manually force a collision by setting the same seriesId in registry for a different canonicalTag
  const fakeCollisionId = res1.seriesId;
  assert.ok(registry._series.has(fakeCollisionId));

  // Simulate collision in registry._series
  registry._series.set("s_fake_collision", {
    seriesId: "s_fake_collision",
    canonicalTags: "canonical_a",
    tags: {}
  });

  const resCollision = registry.resolveSeries({ fake: "tag" });
  assert.ok(resCollision.seriesId);

  await db.close();
});

test("TS Edge Case 2: Multi-series streaming K-way merge with limits and descending ordering", async () => {
  const db = await Tencere.open();
  const ts = db.timeseries("kway_metrics");

  // Ingest across 3 distinct series
  // Series A: t = 100, 400
  await ts.add(10, { at: 100, tags: { s: "a" } });
  await ts.add(40, { at: 400, tags: { s: "a" } });

  // Series B: t = 200, 500
  await ts.add(20, { at: 200, tags: { s: "b" } });
  await ts.add(50, { at: 500, tags: { s: "b" } });

  // Series C: t = 300, 600
  await ts.add(30, { at: 300, tags: { s: "c" } });
  await ts.add(60, { at: 600, tags: { s: "c" } });

  // 1. Chronological Ascending Merge
  const ascAll = await ts.between(0, 1000).values();
  assert.deepEqual(
    ascAll.map((p) => p.value),
    [10, 20, 30, 40, 50, 60],
    "K-way merge must interleave series in strict chronological order"
  );

  // 2. Early-exit limit without full array allocation
  const ascLimit = await ts.between(0, 1000).limit(3).values();
  assert.deepEqual(
    ascLimit.map((p) => p.value),
    [10, 20, 30]
  );

  // 3. Chronological Descending Merge
  const descAll = await ts.between(0, 1000).desc().values();
  assert.deepEqual(
    descAll.map((p) => p.value),
    [60, 50, 40, 30, 20, 10],
    "Descending K-way merge must stream latest timestamps first"
  );

  const descLimit = await ts.between(0, 1000).desc().limit(2).values();
  assert.deepEqual(
    descLimit.map((p) => p.value),
    [60, 50]
  );

  await db.close();
});

test("TS Edge Case 3: Retention purging and deleteRange accurately update boundary stats", async () => {
  const db = await Tencere.open();
  const ts = db.timeseries("retention_stats");

  // Ingest points from t=1000 to t=5000 across multiple series
  await ts.add(10, { at: 1000, tags: { host: "h1" } });
  await ts.add(20, { at: 2000, tags: { host: "h2" } });
  await ts.add(30, { at: 3000, tags: { host: "h1" } });
  await ts.add(40, { at: 4000, tags: { host: "h2" } });
  await ts.add(50, { at: 5000, tags: { host: "h1" } });

  let s = await ts.stats();
  assert.equal(s.points, 5);
  assert.equal(s.oldestTimestamp, 1000);
  assert.equal(s.newestTimestamp, 5000);

  // Purge retention: points older than cutoff 3000
  const purged = ts._storage.purgeRetention(3000);
  assert.equal(purged, 2); // 1000 and 2000 purged

  s = await ts.stats();
  assert.equal(s.points, 3);
  assert.equal(s.oldestTimestamp, 3000, "Oldest timestamp must advance to remaining oldest point (3000)");
  assert.equal(s.newestTimestamp, 5000);

  // Delete all remaining points
  const { deleted } = await ts.delete({ from: 0, to: 10000 });
  assert.equal(deleted, 3);

  s = await ts.stats();
  assert.equal(s.points, 0);
  assert.equal(s.oldestTimestamp, null, "Oldest timestamp must reset to null when empty");
  assert.equal(s.newestTimestamp, null, "Newest timestamp must reset to null when empty");

  await db.close();
});

test("TS Edge Case 4: Descending bucketing preserves chronological first/last and includeEmpty fills gaps", async () => {
  const db = await Tencere.open();
  const ts = db.timeseries("bucket_edge");

  // Ingest points in 1-minute window [60000, 120000)
  // Earliest in bucket is 15 at 61000, latest in bucket is 45 at 79000
  await ts.add(15, { at: 61000 });
  await ts.add(30, { at: 70000 });
  await ts.add(45, { at: 79000 });

  // Ingest point in next bucket [180000, 240000) (gap at [120000, 180000))
  await ts.add(100, { at: 190000 });

  // 1. Ascending bucketing first & last
  const ascBucketsFirst = await ts.between(60000, 240000).bucket("1m").first();
  assert.equal(ascBucketsFirst[0].value, 15, "First point in minute 1 is 15");

  const ascBucketsLast = await ts.between(60000, 240000).bucket("1m").last();
  assert.equal(ascBucketsLast[0].value, 45, "Last point in minute 1 is 45");

  // 2. Descending bucketing: minute 2 bucket comes first, then minute 1 bucket
  const descBucketsFirst = await ts.between(60000, 240000).desc().bucket("1m").first();
  // Minute 1 bucket is at index 1
  assert.equal(descBucketsFirst[1].value, 15, "Chronological first point in minute 1 must remain 15 under desc()");

  const descBucketsLast = await ts.between(60000, 240000).desc().bucket("1m").last();
  assert.equal(descBucketsLast[1].value, 45, "Chronological last point in minute 1 must remain 45 under desc()");

  // 3. includeEmpty: true generates intermediate empty bucket for gap [120000, 180000)
  const withEmpty = await ts.between(60000, 240000).bucket("1m", { includeEmpty: true }).count();
  assert.equal(withEmpty.length, 3, "Must include 3 buckets (min 1, min 2 empty, min 3)");
  assert.equal(withEmpty[0].count, 3);
  assert.equal(withEmpty[1].count, 0, "Gap bucket count must be 0");
  assert.equal(withEmpty[1].start, 120000);
  assert.equal(withEmpty[2].count, 1);

  await db.close();
});

test("TS Edge Case 5: WAL Recovery pre-initialized storage adopts user options on instantiation", async () => {
  const db = await Tencere.open();

  // Simulate internal recovery initialization without options
  const rawStorage = db._engine._getTimeSeriesStorage("fleet");
  assert.equal(rawStorage.options.retention, undefined);

  // User opens the collection with options
  const fleet = db.timeseries("fleet", {
    retention: "14d",
    tags: { indexed: ["model", "firmware"] }
  });

  assert.equal(fleet.options.retention, "14d");
  assert.equal(rawStorage.options.retention, "14d");
  assert.ok(rawStorage.registry.indexed.has("model"));
  assert.ok(rawStorage.registry.indexed.has("firmware"));

  await db.close();
});

test("TS Edge Case 6: PointCodec and mutations strictly reject non-finite numbers and NaN", async () => {
  // 1. PointCodec.encode validation
  assert.throws(
    () => PointCodec.encode(1000, 1n, NaN),
    (err) => err instanceof InvalidTimeSeriesValueError
  );

  assert.throws(
    () => PointCodec.encode(1000, 1n, Infinity),
    (err) => err instanceof InvalidTimeSeriesValueError
  );

  assert.throws(
    () => PointCodec.encode(1000, 1n, "not-a-number"),
    (err) => err instanceof InvalidTimeSeriesValueError
  );

  const db = await Tencere.open();
  const ts = db.timeseries("valid_numbers");

  const pt = await ts.add(55.5);

  // 2. correctPoint rejection of non-finite number
  assert.throws(
    () => ts._storage.correctPoint(pt.id, NaN),
    (err) => err instanceof InvalidTimeSeriesValueError
  );

  await db.close();
});
