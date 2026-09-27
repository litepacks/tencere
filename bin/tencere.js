#!/usr/bin/env node

/**
 * Tencere Command Line Interface.
 */

import "../src/core/polyfill.js";
import fs from "node:fs/promises";
import path from "node:path";
import { Tencere, verifyInvariants, calculateStateHash } from "../src/index.js";
import { TencereServer } from "../src/core/server.js";
import { TencereClient } from "../src/client/index.js";
import { startRepl, formatValue, parseInputVal } from "../src/cli/repl.js";

const args = process.argv.slice(2);
const command = args[0];

function printHelp() {
  console.log(`
Tencere CLI - Modern KV, Cache & Coordination Database

Interactive REPL:
  tencere repl [host:port]          Connect to remote Tencere server (default: 127.0.0.1:7337)
  tencere repl --local <dataDir>    Open interactive REPL against local embedded database
  tencere                           (no arguments) Starts interactive REPL

Server & Administration:
  tencere serve <dataDir> [--port <port>] [--host <host>]
  tencere info <dataDir>
  tencere stats <dataDir>
  tencere inspect <dataDir> [prefix]
  tencere backup <dataDir> <backupDir>
  tencere restore <backupDir> <targetDir>

Diagnostics & Verification:
  tencere doctor <dataDir>          Comprehensive health & invariant check
  tencere verify <dataDir>          Verify data integrity and print deterministic stateHash
  tencere cluster status            Show cluster consensus role, term and peers
  tencere cluster verify            Verify cluster node state invariants

One-shot Operations (remote via TCP or local via --data):
  tencere get <key> [--host <host>] [--port <port>] [--data <dir>]
  tencere set <key> <val> [--ttl <ttl>] [--host <host>] [--port <port>] [--data <dir>]
  tencere del <key> [--host <host>] [--port <port>] [--data <dir>]
  tencere keys [prefix] [--host <host>] [--port <port>] [--data <dir>]
  tencere watch [pattern] [--host <host>] [--port <port>] [--data <dir>]
  tencere ping [--host <host>] [--port <port>]

TimeSeries Commands:
  tencere timeseries list [--data <dir>]
  tencere timeseries info <name> [--data <dir>]
  tencere timeseries tail <name> [limit] [--data <dir>]
  tencere timeseries query <name> [--from <from>] [--to <to>] [--bucket <bucket>] [--avg|--sum|--min|--max|--count] [--data <dir>]

Options:
  --port <port>   TCP port to listen on or connect to (default: 7337)
  --host <host>   Host address (default: 127.0.0.1 / 0.0.0.0 for serve)
  --data <dir>    Run operation against local embedded data directory
  --local <dir>   Run REPL against local embedded data directory
`);
}

function parseFlags(argv) {
  const flags = {};
  for (let i = 0; i < argv.length; i++) {
    if (argv[i].startsWith("--")) {
      const key = argv[i].slice(2);
      const val = argv[i + 1] && !argv[i + 1].startsWith("--") ? argv[++i] : true;
      flags[key] = val;
    }
  }
  return flags;
}

async function getClientOrLocal(flags, extraArgs = []) {
  if (!flags.data && !flags.local) {
    for (const a of extraArgs) {
      if (typeof a === "string" && (a.startsWith("./") || a.startsWith("/") || a.startsWith("../"))) {
        flags.data = a;
        break;
      }
    }
  }

  if (flags.data || flags.local) {
    const dataDir = flags.data || flags.local;
    const db = await Tencere.open(dataDir);
    return {
      isLocal: true,
      db,
      close: async () => db.close()
    };
  }
  const host = flags.host || "127.0.0.1";
  const port = Number(flags.port) || 7337;
  const address = `${host}:${port}`;
  const client = await TencereClient.connect(address);
  return {
    isLocal: false,
    client,
    close: async () => client.close()
  };
}

async function main() {
  if (!command) {
    // Default to interactive REPL
    await startRepl();
    return;
  }

  if (command === "--help" || command === "-h" || command === "help") {
    printHelp();
    process.exit(0);
  }

  const flags = parseFlags(args.slice(1));
  const dataDir = args[1] && !args[1].startsWith("--") ? args[1] : null;

  switch (command) {
    case "repl":
    case "cli": {
      if (flags.local || (dataDir && (dataDir.startsWith("./") || dataDir.startsWith("/") || dataDir.includes(path.sep)))) {
        await startRepl({ local: flags.local || dataDir });
      } else {
        const address = dataDir || (flags.host && flags.port ? `${flags.host}:${flags.port}` : "127.0.0.1:7337");
        await startRepl({ address });
      }
      break;
    }

    case "serve": {
      if (!dataDir) {
        console.error("Error: <dataDir> required for 'serve'");
        process.exit(1);
      }
      const port = Number(flags.port) || 7337;
      const host = flags.host || "0.0.0.0";

      console.log(`Starting Tencere database at ${dataDir}...`);
      const db = await Tencere.open(dataDir);
      const server = new TencereServer(db, { port, host });
      await server.start();
      console.log(`Tencere listening on ${host}:${port} [WAL: batch durability]`);

      const shutdown = async () => {
        console.log("\nShutting down Tencere server...");
        await server.stop();
        await db.close();
        process.exit(0);
      };

      process.on("SIGINT", shutdown);
      process.on("SIGTERM", shutdown);
      break;
    }

    case "get": {
      const key = dataDir;
      if (!key) {
        console.error("Error: <key> required for 'get'");
        process.exit(1);
      }
      const conn = await getClientOrLocal(flags);
      try {
        const val = conn.isLocal ? await conn.db.get(key) : await conn.client.get(key);
        console.log(formatValue(val));
      } finally {
        await conn.close();
      }
      break;
    }

    case "set": {
      const key = dataDir;
      const val = args[2] && !args[2].startsWith("--") ? parseInputVal(args[2]) : null;
      if (!key || val === null) {
        console.error("Error: Usage 'tencere set <key> <val> [--ttl <ttl>]'");
        process.exit(1);
      }
      const conn = await getClientOrLocal(flags);
      try {
        const setOpts = flags.ttl ? { ttl: flags.ttl } : {};
        if (conn.isLocal) {
          await conn.db.set(key, val, setOpts);
        } else {
          await conn.client.set(key, val, setOpts);
        }
        console.log("OK");
      } finally {
        await conn.close();
      }
      break;
    }

    case "del":
    case "delete": {
      const key = dataDir;
      if (!key) {
        console.error("Error: <key> required for 'del'");
        process.exit(1);
      }
      const conn = await getClientOrLocal(flags);
      try {
        const deleted = conn.isLocal ? await conn.db.delete(key) : await conn.client.delete(key);
        console.log(deleted ? 1 : 0);
      } finally {
        await conn.close();
      }
      break;
    }

    case "keys": {
      const prefix = dataDir || "";
      const conn = await getClientOrLocal(flags);
      try {
        const list = conn.isLocal ? conn.db.keys(prefix) : await conn.client.keys(prefix);
        console.log(formatValue(list));
      } finally {
        await conn.close();
      }
      break;
    }

    case "ping": {
      const conn = await getClientOrLocal(flags);
      try {
        if (!conn.isLocal) {
          await conn.client.ping();
        }
        console.log("PONG");
      } finally {
        await conn.close();
      }
      break;
    }

    case "watch": {
      const pattern = dataDir || "";
      const conn = await getClientOrLocal(flags);
      console.log(`Watching changes matching '${pattern || "*"}' (Ctrl+C to quit)...`);

      const shutdown = async () => {
        await conn.close();
        process.exit(0);
      };
      process.on("SIGINT", shutdown);
      process.on("SIGTERM", shutdown);

      if (conn.isLocal) {
        for await (const evt of conn.db.watch(pattern)) {
          const valStr = evt.value !== undefined ? ` -> ${formatValue(evt.value)}` : "";
          const prevStr = evt.previousValue !== undefined ? ` (prev: ${formatValue(evt.previousValue)})` : "";
          console.log(`[WATCH] ${evt.type.toUpperCase()} ${evt.key}${valStr}${prevStr} (v:${evt.version})`);
        }
      } else {
        await conn.client.watch(pattern, (evt) => {
          const valStr = evt.value !== undefined ? ` -> ${formatValue(evt.value)}` : "";
          const prevStr = evt.previousValue !== undefined ? ` (prev: ${formatValue(evt.previousValue)})` : "";
          console.log(`[WATCH] ${evt.type.toUpperCase()} ${evt.key}${valStr}${prevStr} (v:${evt.version})`);
        });
        await new Promise(() => {});
      }
      break;
    }

    case "timeseries":
    case "ts": {
      const sub = args[1];
      let name = args[2];
      if (name && (name.startsWith("./") || name.startsWith("/") || name.startsWith("../"))) {
        if (!flags.data) flags.data = name;
        name = undefined;
      }
      if (args[3] && (args[3].startsWith("./") || args[3].startsWith("/") || args[3].startsWith("../"))) {
        if (!flags.data) flags.data = args[3];
      }
      const conn = await getClientOrLocal(flags, args);
      try {
        if (!conn.isLocal) {
          console.error("TimeSeries CLI commands currently require local database (--data <dir> or path argument)");
          break;
        }
        if (sub === "list") {
          const stores = conn.db._engine._timeSeriesStores ? Array.from(conn.db._engine._timeSeriesStores.keys()) : [];
          console.log(formatValue(stores));
        } else if (sub === "info" || sub === "stats") {
          if (!name) {
            console.error("Usage: tencere timeseries info <name>");
            break;
          }
          const st = await conn.db.timeseries(name).stats();
          console.log(formatValue(st));
        } else if (sub === "tail" || sub === "latest") {
          if (!name) {
            console.error("Usage: tencere timeseries tail <name> [limit]");
            break;
          }
          const limit = args[3] ? Number(args[3]) : 10;
          const res = await conn.db.timeseries(name).latest(limit);
          console.log(formatValue(res));
        } else if (sub === "query") {
          if (!name) {
            console.error("Usage: tencere timeseries query <name> [--from <from>] [--to <to>] [--bucket <bucket>]");
            break;
          }
          let q = conn.db.timeseries(name);
          if (flags.from || flags.to) {
            q = q.between(flags.from || -Infinity, flags.to || Infinity);
          }
          if (flags.bucket) {
            q = q.bucket(flags.bucket);
          }
          if (flags.where) {
            const whereObj = {};
            const parts = String(flags.where).split(",");
            for (const p of parts) {
              const [k, v] = p.split("=");
              if (k && v) whereObj[k] = v;
            }
            q = q.where(whereObj);
          }
          let res;
          if (flags.avg) res = await q.avg();
          else if (flags.sum) res = await q.sum();
          else if (flags.min) res = await q.min();
          else if (flags.max) res = await q.max();
          else if (flags.count) res = await q.count();
          else res = await q.values();
          console.log(formatValue(res));
        } else {
          console.error("Usage: tencere timeseries <list|info|tail|query> [args]");
        }
      } finally {
        await conn.close();
      }
      break;
    }

    case "info": {
      if (!dataDir) {
        console.error("Error: <dataDir> required for 'info'");
        process.exit(1);
      }
      const db = await Tencere.open(dataDir);
      const stats = db.stats();
      console.log("\n=== Tencere Database Info ===");
      console.log(`Directory:            ${path.resolve(dataDir)}`);
      console.log(`Active Keys:          ${stats.keys}`);
      console.log(`Memory Usage:         ${(stats.memoryBytes / 1024).toFixed(2)} KB`);
      console.log(`Total Operations:     ${stats.operations}`);
      console.log(`WAL Head:             ${stats.daktiloHead}`);
      console.log(`WAL Durable Head:     ${stats.daktiloDurableHead}`);
      console.log(`Cache Hits/Misses:    ${stats.cacheHits} / ${stats.cacheMisses}`);
      console.log(`TTL Expirations:      ${stats.expirations}`);
      await db.close();
      break;
    }

    case "stats": {
      if (!dataDir) {
        console.error("Error: <dataDir> required for 'stats'");
        process.exit(1);
      }
      const db = await Tencere.open(dataDir);
      console.log(JSON.stringify(db.stats(), null, 2));
      await db.close();
      break;
    }

    case "inspect": {
      if (!dataDir) {
        console.error("Error: <dataDir> required for 'inspect'");
        process.exit(1);
      }
      const prefix = args[2] || "";
      const db = await Tencere.open(dataDir);
      const items = db._engine.storage.scan(prefix);
      console.log(`\nInspecting keys in ${dataDir} (filter: '${prefix || "*"}'):`);
      if (items.length === 0) {
        console.log("  (no keys found)");
      } else {
        for (const item of items) {
          const typeName = typeof item.value;
          console.log(`  - ${item.key} [v${item.version}, ${typeName}, size: ${JSON.stringify(item.value)?.length || 0}B]`);
        }
      }
      await db.close();
      break;
    }

    case "backup": {
      if (!dataDir || !args[2]) {
        console.error("Error: Usage 'tencere backup <dataDir> <backupDir>'");
        process.exit(1);
      }
      const backupDir = args[2];
      console.log(`Backing up ${dataDir} to ${backupDir}...`);
      const db = await Tencere.open(dataDir);
      await db.checkpoint();
      await db.close();

      await fs.mkdir(backupDir, { recursive: true });
      await fs.cp(dataDir, backupDir, { recursive: true });
      console.log(`Backup completed successfully at ${backupDir}`);
      break;
    }

    case "restore": {
      if (!dataDir || !args[2]) {
        console.error("Error: Usage 'tencere restore <backupDir> <targetDir>'");
        process.exit(1);
      }
      const backupDir = dataDir;
      const targetDir = args[2];
      console.log(`Restoring backup ${backupDir} to ${targetDir}...`);
      await fs.mkdir(targetDir, { recursive: true });
      await fs.cp(backupDir, targetDir, { recursive: true });

      const db = await Tencere.open(targetDir);
      console.log(`Database restored successfully. Key count: ${db.stats().keys}`);
      await db.close();
      break;
    }

    case "doctor": {
      if (!dataDir) {
        console.error("Error: <dataDir> required for 'doctor'");
        process.exit(1);
      }
      console.log(`\n🏥 Running Tencere Doctor diagnostics on '${dataDir}'...`);
      const db = await Tencere.open(dataDir);
      try {
        const stats = db.stats();
        const hash = await db.debug.stateHash();
        const report = await verifyInvariants(db);

        console.log("\n📊 Engine & Storage Metrics:");
        console.log(`  - State Hash:       ${hash}`);
        console.log(`  - Total Keys:       ${stats.keys}`);
        console.log(`  - Checkpoint Seq:   ${stats.checkpoint?.sequence ?? "none"}`);
        console.log(`  - Memory (approx):  ${stats.memory?.heapUsed ? Math.round(stats.memory.heapUsed / 1024 / 1024) + " MB" : "N/A"}`);
        console.log(`  - Active TTL Items: ${stats.ttl?.activeCount ?? 0}`);

        console.log("\n🔍 Invariant Checks:");
        console.log(`  - Sorted Collections:     ${report.stats.sortedCollections} (${report.stats.sortedMembers} members)`);
        console.log(`  - TimeSeries Collections: ${report.stats.timeSeriesCollections} (${report.stats.timeSeriesPoints} points)`);
        console.log(`  - Queue Jobs:             ${report.stats.queueJobs}`);
        console.log(`  - Active Locks:           ${report.stats.activeLocks}`);

        if (report.valid) {
          console.log("\n✅ Health Status: HEALTHY (All invariants hold, no corruptions detected)");
        } else {
          console.log(`\n❌ Health Status: UNHEALTHY (${report.errors.length} errors found):`);
          for (const err of report.errors) {
            console.log(`   [!] ${err}`);
          }
          process.exitCode = 1;
        }
      } finally {
        await db.close();
      }
      break;
    }

    case "verify": {
      if (!dataDir) {
        console.error("Error: <dataDir> required for 'verify'");
        process.exit(1);
      }
      console.log(`Verifying database integrity at '${dataDir}'...`);
      const db = await Tencere.open(dataDir);
      try {
        const hash = await db.debug.stateHash();
        const report = await verifyInvariants(db);
        if (!report.valid) {
          console.error(`Verification FAILED with ${report.errors.length} violations:`);
          for (const err of report.errors) {
            console.error(`  - ${err}`);
          }
          process.exit(1);
        }
        console.log(`Integrity verified successfully.`);
        console.log(`State Hash: ${hash}`);
        console.log(`Verified: ${report.stats.totalKeys} keys, ${report.stats.sortedCollections} sorted collections, ${report.stats.timeSeriesCollections} timeseries collections.`);
      } finally {
        await db.close();
      }
      break;
    }

    case "cluster": {
      const sub = args[1];
      if (sub === "status") {
        const conn = await getClientOrLocal(flags, args.slice(2));
        try {
          const stats = conn.isLocal ? conn.db.stats() : await conn.client.stats();
          console.log(JSON.stringify(stats.cluster || { enabled: false, message: "Cluster not configured" }, null, 2));
        } finally {
          await conn.close();
        }
      } else if (sub === "verify") {
        const conn = await getClientOrLocal(flags, args.slice(2));
        try {
          if (conn.isLocal) {
            const hash = await conn.db.debug.stateHash();
            const report = await verifyInvariants(conn.db);
            console.log(`Cluster Node State Hash: ${hash}`);
            console.log(`Node Invariants Valid: ${report.valid}`);
          } else {
            console.log("Cluster verification requires local data directory or direct node access.");
          }
        } finally {
          await conn.close();
        }
      } else {
        console.error("Usage: tencere cluster status | tencere cluster verify");
        process.exit(1);
      }
      break;
    }

    default:
      console.error(`Unknown command: ${command}`);
      printHelp();
      process.exit(1);
  }
}

main().catch((err) => {
  console.error("Fatal error:", err);
  process.exit(1);
});
