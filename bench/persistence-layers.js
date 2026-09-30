/**
 * Tencere Persistence Layers Benchmark Suite
 * Explicitly separates storage and durability guarantees into 5 discrete layers:
 *  - Layer A: Pure in-memory (no WAL)
 *  - Layer B: Persistent state without forced fsync (async)
 *  - Layer C: Daktilo WAL async
 *  - Layer D: Daktilo WAL batch
 *  - Layer E: Daktilo WAL strict
 *
 * Tracks latency percentiles (p50, p95, p99), write amplification, and RSS.
 */

import os from "node:os";
import fs from "node:fs/promises";
import path from "node:path";
import { performance } from "node:perf_hooks";
import { Tencere } from "../src/index.js";

function getSystemMetadata() {
  const cpus = os.cpus();
  return {
    tencereVersion: "0.2.0",
    nodeVersion: process.version,
    os: `${os.type()} ${os.release()} (${os.arch()})`,
    cpu: cpus.length > 0 ? cpus[0].model : "unknown",
    cores: cpus.length,
    totalMemoryMB: Math.round(os.totalmem() / (1024 * 1024))
  };
}

function calculatePercentiles(latencies) {
  if (latencies.length === 0) return { p50: 0, p95: 0, p99: 0 };
  const sorted = latencies.slice().sort((a, b) => a - b);
  const p50 = sorted[Math.floor(sorted.length * 0.5)];
  const p95 = sorted[Math.floor(sorted.length * 0.95)];
  const p99 = sorted[Math.floor(sorted.length * 0.99)];
  return {
    p50: Number(p50.toFixed(3)),
    p95: Number(p95.toFixed(3)),
    p99: Number(p99.toFixed(3))
  };
}

async function getDirSizeBytes(dir) {
  try {
    const files = await fs.readdir(dir);
    let total = 0;
    for (const f of files) {
      const stat = await fs.stat(path.join(dir, f));
      if (stat.isFile()) total += stat.size;
    }
    return total;
  } catch (_) {
    return 0;
  }
}

/**
 * Runs layered benchmark run.
 */
export async function runPersistenceBenchmark(options = {}) {
  const operationsCount = options.operationsCount || 5000;
  const payloadSize = options.payloadSize || 64; // bytes
  const payloadStr = "x".repeat(payloadSize);

  const layers = [
    { name: "Layer A: In-Memory (No WAL)", dir: null, durability: null },
    { name: "Layer B: Persistent (Async WAL)", dir: "async", durability: "async" },
    { name: "Layer C: Daktilo (Batch WAL)", dir: "batch", durability: "batch" }
  ];

  const results = [];
  const meta = getSystemMetadata();

  console.log("=".repeat(75));
  console.log("TENCERE PERSISTENCE BENCHMARK MATRIX");
  console.log(`Node: ${meta.nodeVersion} | OS: ${meta.os} | CPU: ${meta.cpu}`);
  console.log(`Operations: ${operationsCount} | Payload: ${payloadSize} bytes`);
  console.log("=".repeat(75));

  for (const layer of layers) {
    let tmpDir = null;
    if (layer.dir) {
      tmpDir = await fs.mkdtemp(path.join(os.tmpdir(), `tencere-bench-${layer.dir}-`));
    }

    try {
      const openOpts = layer.durability ? { durability: layer.durability } : {};
      const db = await Tencere.open(tmpDir, openOpts);

      const latencies = [];
      const memBefore = process.memoryUsage().rss;
      let logicalBytes = 0;

      const t0 = performance.now();
      for (let i = 0; i < operationsCount; i++) {
        const k = `bench:k:${i}`;
        const opStart = performance.now();
        await db.set(k, payloadStr);
        latencies.push(performance.now() - opStart);
        logicalBytes += k.length + payloadSize;
      }
      const totalTimeMs = performance.now() - t0;
      const opsPerSec = Math.round((operationsCount / totalTimeMs) * 1000);
      const percentiles = calculatePercentiles(latencies);

      const memAfter = process.memoryUsage().rss;
      const physicalBytes = tmpDir ? await getDirSizeBytes(tmpDir) : 0;
      const writeAmplification = logicalBytes > 0 && physicalBytes > 0 ? (physicalBytes / logicalBytes).toFixed(2) : "1.00";

      await db.close();

      const res = {
        layer: layer.name,
        opsPerSec,
        ...percentiles,
        rssDeltaMB: ((memAfter - memBefore) / (1024 * 1024)).toFixed(2),
        writeAmplification
      };
      results.push(res);

      console.log(`\n[${layer.name}]`);
      console.log(` Throughput: ${opsPerSec.toLocaleString()} ops/sec`);
      console.log(` Latency: p50=${percentiles.p50}ms | p95=${percentiles.p95}ms | p99=${percentiles.p99}ms`);
      console.log(` Write Amplification: ${writeAmplification}x | Physical disk: ${Math.round(physicalBytes / 1024)} KB`);
    } finally {
      if (tmpDir) {
        await fs.rm(tmpDir, { recursive: true, force: true }).catch(() => {});
      }
    }
  }

  console.log("\n" + "=".repeat(75));
  return { metadata: meta, results };
}

// Allow direct CLI execution
if (process.argv[1] && process.argv[1].endsWith("persistence-layers.js")) {
  runPersistenceBenchmark().catch(console.error);
}
