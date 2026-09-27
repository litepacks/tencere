/**
 * Tencere TimeSeries Performance Benchmarking Suite.
 *
 * Measures:
 *  - Single add (in-memory vs WAL async vs WAL batch vs WAL strict)
 *  - Batch add (addMany)
 *  - Out-of-order add
 *  - Range scans (1K, 10K, 50K points)
 *  - Stream aggregations (count, sum, min, max, avg, first, last)
 *  - Deterministic bucketing (1m, 5m, 1h)
 *  - Tag-filtered range queries
 *  - Watch / real-time event throughput
 *  - Retention pruning
 */

import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { performance } from "node:perf_hooks";
import { Tencere } from "../src/index.js";

function formatOps(opsPerSec) {
  if (opsPerSec >= 1_000_000) {
    return `${(opsPerSec / 1_000_000).toFixed(2)}M ops/sec`;
  }
  if (opsPerSec >= 1_000) {
    return `${(opsPerSec / 1_000).toFixed(2)}K ops/sec`;
  }
  return `${opsPerSec.toFixed(0)} ops/sec`;
}

function calculatePercentiles(latencies) {
  latencies.sort((a, b) => a - b);
  const p50 = latencies[Math.floor(latencies.length * 0.5)] || 0;
  const p95 = latencies[Math.floor(latencies.length * 0.95)] || 0;
  const p99 = latencies[Math.floor(latencies.length * 0.99)] || 0;
  return { p50, p95, p99 };
}

async function runBench(name, iterations, fn) {
  const warmup = Math.min(50, Math.floor(iterations * 0.05));
  for (let i = 0; i < warmup; i++) {
    await fn(i);
  }

  const latencies = [];
  const start = performance.now();

  for (let i = 0; i < iterations; i++) {
    const t0 = performance.now();
    await fn(i);
    latencies.push(performance.now() - t0);
  }

  const elapsedMs = performance.now() - start;
  const opsSec = (iterations / elapsedMs) * 1000;
  const { p50, p95, p99 } = calculatePercentiles(latencies);

  console.log(
    `  ${name.padEnd(46)} | ${formatOps(opsSec).padStart(16)} | p50: ${p50.toFixed(3)}ms | p99: ${p99.toFixed(3)}ms`
  );

  return { name, iterations, elapsedMs, opsSec, p50, p95, p99 };
}

async function main() {
  console.log("================================================================================");
  console.log("                     TENCERE TIMESERIES BENCHMARKS                              ");
  console.log("================================================================================");

  // [1] Ingestion: Single Add across durability modes
  console.log("\n[1] Ingestion: Single Add (durability modes)");
  {
    const memDb = await Tencere.open();
    const tsMem = memDb.timeseries("bench_mem");
    await runBench("Single add (in-memory, no WAL)", 20_000, async (i) => {
      await tsMem.add(20.0 + (i % 10), { at: 1700000000000 + i * 1000 });
    });
    await memDb.close();
  }

  const tmpBase = await fs.mkdtemp(path.join(os.tmpdir(), "tencere-ts-bench-"));

  try {
    const asyncDir = path.join(tmpBase, "async");
    const asyncDb = await Tencere.open(asyncDir, { durability: "async" });
    const tsAsync = asyncDb.timeseries("bench_async");
    await runBench("Single add (WAL async)", 10_000, async (i) => {
      await tsAsync.add(20.0 + (i % 10), { at: 1700000000000 + i * 1000 });
    });
    await asyncDb.close();

    const batchDir = path.join(tmpBase, "batch");
    const batchDb = await Tencere.open(batchDir, { durability: "batch", batchWindowMs: 5 });
    const tsBatch = batchDb.timeseries("bench_batch");
    await runBench("Single add (WAL batch 5ms)", 5_000, async (i) => {
      await tsBatch.add(20.0 + (i % 10), { at: 1700000000000 + i * 1000 });
    });
    await batchDb.close();
  } catch (err) {
    console.error("WAL bench error:", err);
  }

  // [2] Batch Ingestion & Out-of-Order
  console.log("\n[2] High-Throughput Batch Ingestion & Out-of-Order");
  {
    const db = await Tencere.open();
    const ts = db.timeseries("bench_batch");

    // 100 batches of 500 items = 50,000 points
    const batchSize = 500;
    const batchCount = 100;
    const batches = [];
    for (let b = 0; b < batchCount; b++) {
      const chunk = [];
      for (let j = 0; j < batchSize; j++) {
        chunk.push([1700000000000 + (b * batchSize + j) * 1000, 20.0 + (j % 50)]);
      }
      batches.push(chunk);
    }

    const t0 = performance.now();
    for (let b = 0; b < batchCount; b++) {
      await ts.addMany(batches[b]);
    }
    const elapsed = performance.now() - t0;
    const totalPoints = batchSize * batchCount;
    console.log(
      `  ${"Batch addMany (500 pts/batch, 50k total)".padEnd(46)} | ${formatOps((totalPoints / elapsed) * 1000).padStart(16)} | ${(elapsed / batchCount).toFixed(3)}ms/batch`
    );

    // Out of order adds
    const oooPoints = [];
    for (let i = 0; i < 5_000; i++) {
      // Random timestamps within the populated range
      const randomTs = 1700000000000 + Math.floor(Math.random() * totalPoints * 1000);
      oooPoints.push({ at: randomTs, value: Math.random() * 100 });
    }
    await runBench("Out-of-order single adds", 2_000, async (i) => {
      await ts.add(oooPoints[i].value, { at: oooPoints[i].at });
    });

    await db.close();
  }

  // [3] Query, Aggregations, and Bucketing
  console.log("\n[3] Range Queries, Aggregations & Bucketing (50,000 pre-populated points)");
  {
    const db = await Tencere.open();
    const ts = db.timeseries("bench_queries");

    const baseTs = 1700000000000;
    const count = 50_000;
    const points = [];
    for (let i = 0; i < count; i++) {
      points.push({
        at: baseTs + i * 1000,
        value: 10 + (i % 100) * 0.5,
        tags: { host: `server-${i % 5}`, region: i % 2 === 0 ? "us-east" : "eu-west" }
      });
    }
    await ts.addMany(points);

    await runBench("Range scan: 1,000 points (.values())", 1_000, async () => {
      await ts.between(baseTs, baseTs + 1_000_000).values();
    });

    await runBench("Range scan: 10,000 points (.values())", 200, async () => {
      await ts.between(baseTs, baseTs + 10_000_000).values();
    });

    await runBench("Range scan: 50,000 points (.values())", 50, async () => {
      await ts.between(baseTs, baseTs + 50_000_000).values();
    });

    await runBench("Stream aggregation: avg() (50,000 points)", 200, async () => {
      await ts.between(baseTs, baseTs + 50_000_000).avg();
    });

    await runBench("Stream aggregation: min/max/sum/count (50k pts)", 200, async () => {
      await ts.between(baseTs, baseTs + 50_000_000).sum();
      await ts.between(baseTs, baseTs + 50_000_000).min();
      await ts.between(baseTs, baseTs + 50_000_000).max();
    });

    await runBench("Bucketing 1m interval (50k points)", 200, async () => {
      await ts.between(baseTs, baseTs + 50_000_000).bucket("1m").avg();
    });

    await runBench("Bucketing 5m interval (50k points)", 200, async () => {
      await ts.between(baseTs, baseTs + 50_000_000).bucket("5m").avg();
    });

    await runBench("Bucketing 1h interval (50k points)", 200, async () => {
      await ts.between(baseTs, baseTs + 50_000_000).bucket("1h").avg();
    });

    await runBench("Tag filtered scan: where({ region: 'eu-west' })", 200, async () => {
      await ts.where({ region: "eu-west" }).between(baseTs, baseTs + 50_000_000).avg();
    });

    await runBench("Latest 100 points: .latest(100)", 1_000, async () => {
      await ts.latest(100);
    });

    await db.close();
  }

  // [4] Watch & Realtime Throughput
  console.log("\n[4] Watch / Real-time Event Throughput");
  {
    const db = await Tencere.open();
    const ts = db.timeseries("bench_watch");

    let receivedCount = 0;
    const watchPromise = (async () => {
      for await (const point of ts.watch()) {
        receivedCount++;
        if (receivedCount >= 10_000) break;
      }
    })();

    const start = performance.now();
    for (let i = 0; i < 10_000; i++) {
      await ts.add(i);
    }
    await watchPromise;
    const elapsed = performance.now() - start;
    console.log(
      `  ${"Watch pipeline throughput (10,000 points)".padEnd(46)} | ${formatOps((10_000 / elapsed) * 1000).padStart(16)} | ${(elapsed / 10000).toFixed(4)}ms/pt`
    );

    await db.close();
  }

  // [5] Retention Purging
  console.log("\n[5] Retention Purging");
  {
    const db = await Tencere.open();
    const ts = db.timeseries("bench_retention", { retention: "1h" });

    // Add 10,000 points: 5,000 older than 1h, 5,000 recent
    const now = Date.now();
    const points = [];
    for (let i = 0; i < 5_000; i++) {
      points.push({ at: now - 7200000 + i * 1000, value: i }); // 2h ago
    }
    for (let i = 0; i < 5_000; i++) {
      points.push({ at: now - 1800000 + i * 1000, value: i }); // 30m ago
    }
    await ts.addMany(points);

    const start = performance.now();
    const { purged } = await ts.purgeRetention();
    const elapsed = performance.now() - start;
    console.log(
      `  ${`Purged ${purged} expired points`.padEnd(46)} | ${(purged / (elapsed / 1000)).toFixed(0)} pts/sec | elapsed: ${elapsed.toFixed(2)}ms`
    );

    await db.close();
  }

  // Cleanup tmp dirs
  try {
    await fs.rm(tmpBase, { recursive: true, force: true });
  } catch {}

  console.log("\n================================================================================");
  console.log("                           BENCHMARK COMPLETE                                   ");
  console.log("================================================================================\n");
}

main().catch((err) => {
  console.error("Benchmark failed:", err);
  process.exit(1);
});
