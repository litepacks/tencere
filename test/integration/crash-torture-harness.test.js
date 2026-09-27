import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { fork } from "node:child_process";
import { fileURLToPath } from "node:url";
import { Tencere } from "../../src/index.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const workerScript = path.join(__dirname, "crash-worker.js");

test("Crash Torture Harness: repeated SIGKILL during active mutations followed by invariant recovery", async () => {
  const tmpDir = await fs.mkdtemp(path.join(os.tmpdir(), "tencere-crash-harness-"));

  try {
    const cycles = 5;

    for (let c = 1; c <= cycles; c++) {
      // 1. Spawn child worker process
      const child = fork(workerScript, [tmpDir], {
        stdio: ["ignore", "ignore", "inherit", "ipc"]
      });

      // Wait until child is running mutations
      await new Promise((resolve) => {
        child.once("message", (msg) => {
          if (msg === "READY") resolve();
        });
        // Fallback timeout in case IPC message is missed
        setTimeout(resolve, 50);
      });

      // Let worker perform rapid mutations for random time between 10ms and 40ms
      const runDuration = 10 + Math.floor(Math.random() * 30);
      await new Promise((r) => setTimeout(r, runDuration));

      // 2. Brutally kill child process with SIGKILL (kill -9)
      child.kill("SIGKILL");

      await new Promise((resolve) => {
        child.on("exit", resolve);
      });

      // 3. Open database in main process to trigger recovery
      const db = await Tencere.open(tmpDir, { history: { enabled: true } });

      // 4. Verify all invariants hold after sudden crash
      const inv = await db.debug.verify();
      assert.equal(
        inv.valid,
        true,
        `Crash recovery invariant violation in cycle ${c}: ${inv.errors.join(", ")}`
      );

      const hash = await db.debug.stateHash();
      assert.ok(hash && hash.length === 64);

      // Verify basic operability after recovery
      await db.set(`recovered_cycle_${c}`, "healthy");
      assert.equal(await db.get(`recovered_cycle_${c}`), "healthy");

      await db.close();
    }
  } finally {
    await fs.rm(tmpDir, { recursive: true, force: true }).catch(() => {});
  }
});
