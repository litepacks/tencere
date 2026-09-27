/**
 * Crash worker process for crash torture harness.
 * Performs rapid continuous mutations across all collections until killed by SIGKILL.
 */

import { Tencere } from "../../src/index.js";

const dataDir = process.argv[2];
if (!dataDir) {
  process.exit(1);
}

async function run() {
  const db = await Tencere.open(dataDir, { history: { enabled: true } });

  // Let parent know child is ready
  if (process.send) {
    process.send("READY");
  }

  let i = 0;
  while (true) {
    i++;
    try {
      await db.set(`crash:kv:${i}`, { iteration: i, payload: "x".repeat(64) });
      await db.map("crash_map").set(`m:${i % 20}`, i);
      await db.sorted("crash_scores").set(`p:${i % 15}`, i * 5);
      await db.timeseries("crash_telemetry").add(i * 1.5, { tags: { batch: "crash" } });

      if (i % 10 === 0) {
        await db.checkpoint();
      }
    } catch (_) {
      // Ignore during crash
    }
  }
}

run().catch((err) => {
  console.error("Crash worker error:", err);
  process.exit(1);
});
