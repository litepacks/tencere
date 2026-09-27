/**
 * Tencere High-Performance Benchmarking Suite.
 *
 * Measures:
 *  - Core KV across durability modes: in-memory, daktilo async, daktilo batch, daktilo strict
 *  - Payload sizes: 16 B, 128 B, 1 KiB, 16 KiB
 *  - Synchronous embedded engine (TencereSync)
 *  - Sorted collection (SkipList): insert, rank, top 10, range
 *  - Coordination primitives: lock, rate-limiter, idempotency, semaphore
 */

import fs from "node:fs/promises";
import path from "node:path";
import { performance } from "node:perf_hooks";
import { Tencere } from "../src/index.js";
import { TencereSync } from "../src/sync/index.js";

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
  // Warmup
  const warmupCount = Math.min(100, Math.floor(iterations * 0.1));
  for (let i = 0; i < warmupCount; i++) {
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
    `  ${name.padEnd(45)} | ${formatOps(opsSec).padStart(16)} | p50: ${p50.toFixed(3)}ms | p99: ${p99.toFixed(3)}ms`
  );

  return { name, iterations, elapsedMs, opsSec, p50, p95, p99 };
}

async function runAllBenchmarks() {
  console.log("================================================================================");
  console.log("                       TENCERE DATABASE BENCHMARKS                              ");
  console.log("================================================================================");

  // 1. TencereSync (Synchronous Local Engine)
  console.log("\n[1] Synchronous Local Engine (TencereSync)");
  const syncDb = new TencereSync();
  const syncIterations = 50_000;

  {
    const start = performance.now();
    for (let i = 0; i < syncIterations; i++) {
      syncDb.set(`sync:key:${i}`, i);
    }
    const elapsed = performance.now() - start;
    console.log(`  ${"TencereSync SET (small int)".padEnd(45)} | ${formatOps((syncIterations / elapsed) * 1000).padStart(16)}`);
  }

  {
    const start = performance.now();
    for (let i = 0; i < syncIterations; i++) {
      syncDb.get(`sync:key:${i}`);
    }
    const elapsed = performance.now() - start;
    console.log(`  ${"TencereSync GET (small int)".padEnd(45)} | ${formatOps((syncIterations / elapsed) * 1000).padStart(16)}`);
  }
  syncDb.close();

  // 2. In-Memory Core Engine (Zero WAL I/O)
  console.log("\n[2] In-Memory Core Engine (Tencere async, without WAL)");
  const memDb = await Tencere.open();

  await runBench("Async SET (16 B string)", 20_000, async (i) => {
    await memDb.set(`k:${i}`, "hello-16-bytes--");
  });

  await runBench("Async GET (16 B string)", 20_000, async (i) => {
    await memDb.get(`k:${i}`);
  });

  const payload128 = "A".repeat(128);
  await runBench("Async SET (128 B string)", 20_000, async (i) => {
    await memDb.set(`k128:${i}`, payload128);
  });

  const payload1k = new Uint8Array(1024).fill(0x5a);
  await runBench("Async SET (1 KiB Uint8Array raw)", 10_000, async (i) => {
    await memDb.set(`k1k:${i}`, payload1k);
  });

  const payload16k = new Uint8Array(16384).fill(0x5a);
  await runBench("Async SET (16 KiB Uint8Array raw)", 5_000, async (i) => {
    await memDb.set(`k16k:${i}`, payload16k);
  });

  await runBench("Atomic Increment (counter)", 20_000, async (i) => {
    await memDb.increment("bench:counter", 1);
  });

  await runBench("Atomic Patch ($set + $inc)", 10_000, async (i) => {
    await memDb.patch(`k:${i % 1000}`, { $set: { active: true }, $inc: { hits: 1 } });
  });

  await memDb.close();

  // 3. Durability Modes with Daktilo WAL
  console.log("\n[3] Durability Modes with Daktilo WAL");
  const testDir = "./scratch_bench_wal";

  // Durability: async
  {
    await fs.rm(testDir, { recursive: true, force: true });
    const asyncDb = await Tencere.open(testDir, { durability: "async" });
    await runBench("Tencere + Daktilo (async)", 10_000, async (i) => {
      await asyncDb.set(`async:${i}`, "daktilo-async-val");
    });
    await asyncDb.close();
  }

  // Durability: batch
  {
    await fs.rm(testDir, { recursive: true, force: true });
    const batchDb = await Tencere.open(testDir, { durability: "batch" });
    await runBench("Tencere + Daktilo (batch durable)", 5_000, async (i) => {
      await batchDb.set(`batch:${i}`, "daktilo-batch-val");
    });
    await batchDb.close();
  }

  // Durability: strict
  {
    await fs.rm(testDir, { recursive: true, force: true });
    const strictDb = await Tencere.open(testDir, { durability: "strict" });
    await runBench("Tencere + Daktilo (strict fsync)", 200, async (i) => {
      await strictDb.set(`strict:${i}`, "strict-fsync-val");
    });
    await strictDb.close();
    await fs.rm(testDir, { recursive: true, force: true });
  }

  // 4. Sorted Collection & OrderedIndex
  console.log("\n[4] Sorted Collection (OrderedIndex / SkipList)");
  const sortedDb = await Tencere.open();
  const scores = sortedDb.sorted("leaderboard");

  await runBench("Sorted: insert / update score", 15_000, async (i) => {
    await scores.set(`player:${i}`, (i * 37) % 10000);
  });

  await runBench("Sorted: rank lookup", 15_000, async (i) => {
    await scores.rank(`player:${i}`);
  });

  await runBench("Sorted: top(10)", 10_000, async () => {
    await scores.top(10);
  });

  await runBench("Sorted: range between(2000, 4000).limit(20)", 10_000, async () => {
    await scores.between(2000, 4000).limit(20).entries();
  });

  await sortedDb.close();

  // 5. Distributed Coordination Primitives
  console.log("\n[5] Distributed Coordination Primitives");
  const coordDb = await Tencere.open();

  await runBench("Lock: tryLock & release", 10_000, async (i) => {
    const lock = await coordDb.tryLock(`lock:${i % 100}`);
    if (lock) await lock.release();
  });

  await runBench("RateLimiter: sliding window permit", 15_000, async (i) => {
    await coordDb.rateLimit(`user:${i % 50}`, { limit: 10000, window: "1m" });
  });

  await runBench("Idempotent: deduplicated execution", 10_000, async (i) => {
    await coordDb.idempotent(`req:${i % 100}`, async () => "result");
  });

  await runBench("Semaphore: acquire & release", 5_000, async (i) => {
    const sem = coordDb.semaphore(`sem:${i % 20}`, { permits: 5 });
    await sem.run(async () => {});
  });

  // 6. Vector Similarity Search
  console.log("\n[6] Vector Similarity Search");
  const vecDb = await Tencere.open();
  const docs = vecDb.vector("bench_vectors", { dimensions: 128 });

  for (let i = 0; i < 500; i++) {
    const vec = new Float32Array(128);
    for (let d = 0; d < 128; d++) vec[d] = Math.random();
    await docs.set(`doc:${i}`, { vector: vec, value: { id: i } });
  }

  const queryVec = new Float32Array(128).fill(0.5);
  await runBench("Vector search: topK 10 (over 500 vectors x 128 dims)", 2_000, async () => {
    await docs.search(queryVec, { topK: 10 });
  });

  await vecDb.close();
  await coordDb.close();

  console.log("\n================================================================================");
  console.log("                           BENCHMARK COMPLETE                                   ");
  console.log("================================================================================");
}

runAllBenchmarks().catch(console.error);
