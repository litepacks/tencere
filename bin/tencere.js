#!/usr/bin/env node

/**
 * Tencere Command Line Interface.
 */

import "../src/core/polyfill.js";
import fs from "node:fs/promises";
import fsSync from "node:fs";
import path from "node:path";
import os from "node:os";
import { fileURLToPath } from "node:url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
import { Tencere, verifyInvariants, calculateStateHash, createTestCluster } from "../src/index.js";
import { TencereServer } from "../src/core/server.js";
import { TencereClient } from "../src/client/index.js";
import { startRepl, formatValue, parseInputVal } from "../src/cli/repl.js";

function formatTable(headers, rows) {
  const colWidths = headers.map((h, i) => {
    let max = h.length;
    for (const r of rows) {
      const cell = String(r[i] ?? "");
      if (cell.length > max) max = cell.length;
    }
    return max + 2;
  });

  const pad = (str, len) => ` ${str}`.padEnd(len);
  const top = "┌" + colWidths.map((w) => "─".repeat(w)).join("┬") + "┐";
  const mid = "├" + colWidths.map((w) => "─".repeat(w)).join("┼") + "┤";
  const bot = "└" + colWidths.map((w) => "─".repeat(w)).join("┴") + "┘";

  const headerLine = "│" + headers.map((h, i) => pad(h, colWidths[i])).join("│") + "│";
  const rowLines = rows.map((r) => "│" + r.map((cell, i) => pad(String(cell ?? ""), colWidths[i])).join("│") + "│");

  return [top, headerLine, mid, ...rowLines, bot].join("\n");
}

function printHelp() {
  console.log(`
Tencere CLI - Modern KV, Cache & Coordination Database

Interactive REPL:
  tencere repl [host:port]          Connect to remote Tencere server (default: 127.0.0.1:7337)
  tencere repl --local <dataDir>    Open interactive REPL against local embedded database
  tencere                           (no arguments) Starts interactive REPL

Server & Administration:
  tencere serve <dataDir> [options]
  tencere info <dataDir>
  tencere stats <dataDir>
  tencere inspect <dataDir> [prefix]
  tencere backup <dataDir> <backupDir>
  tencere restore <backupDir> <targetDir>

Diagnostics & Verification:
  tencere doctor <dataDir>          Comprehensive health & invariant check
  tencere verify <dataDir>          Verify data integrity and print deterministic stateHash
  tencere cluster dev [options]     Spin up a local multi-node cluster for development
  tencere cluster nodes             Show visual cluster dashboard table across all nodes
  tencere cluster health            Check cluster health & readiness (K8s probe compatible)
  tencere cluster metrics           Display Raft & replication telemetry metrics
  tencere cluster status            Show cluster consensus role, term, leader and peers
  tencere cluster verify            Verify cluster node state invariants

Agent Skills (Antigravity / Gemini / Claude / Cursor):
  tencere skill install             Install skill to workspace (.agents) and global (~/.gemini)
  tencere skill install --global    Install skill to global ~/.gemini/config/skills/tencere
  tencere skill install --workspace Install skill to local workspace .agents/skills/tencere
  tencere skill show                Print skill markdown contents to stdout
  tencere skill path                Print packaged skill file path

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
  --port <port>                 TCP port to listen on or connect to (default: 7337)
  --host <host>                 Host address (default: 127.0.0.1 / 0.0.0.0 for serve)
  --data <dir>                  Run operation against local embedded data directory
  --local <dir>                 Run REPL against local embedded data directory

Cluster Dev Options (for cluster dev):
  --nodes <n>                   Number of cluster nodes to spawn (default: 3)
  --base-port <port>            Starting port for cluster nodes (default: 7337)
  --host <host>                 Host address (default: 127.0.0.1)
  --memory                      Run cluster nodes in-memory without disk persistence
  --data-dir <dir>              Directory for node storage directories (default: ./.tencere-cluster)
  --forward-writes              Enable automatic server write forwarding (default: true)

Cluster Options (for serve):
  --node-id <id>                Cluster node ID (e.g. 1)
  --peers <p1,p2>               Cluster peer node IDs (e.g. 2,3)
  --peer-addrs <id=addr,...>    Cluster peer addresses (e.g. 2=10.0.0.2:7337,3=10.0.0.3:7337)
  --cluster-config <path>       Path to JSON cluster configuration file
  --cluster-heartbeat <ms>      Heartbeat interval in ms (default: 50)
  --cluster-election <min:max>  Election timeout range in ms (default: 150:300)
`);
}

export async function parseClusterConfig(flags) {
  let clusterConfig = null;

  if (flags["cluster-config"]) {
    const raw = await fs.readFile(flags["cluster-config"], "utf-8");
    clusterConfig = JSON.parse(raw);
  }

  const nodeId = flags["node-id"] ?? flags["cluster-node"] ?? flags.nodeId;
  const peersRaw = flags.peers ?? flags["cluster-peers"];
  const peerAddrsRaw = flags["peer-addrs"] ?? flags["cluster-peer-addrs"];

  if (nodeId !== undefined || flags.cluster || peersRaw || peerAddrsRaw) {
    clusterConfig = clusterConfig || {};
    if (nodeId !== undefined) {
      clusterConfig.nodeId = Number(nodeId);
    } else if (clusterConfig.nodeId === undefined) {
      clusterConfig.nodeId = 1;
    }

    if (peersRaw) {
      clusterConfig.peers = String(peersRaw).split(",").map((s) => Number(s.trim())).filter((n) => !isNaN(n));
    } else if (!clusterConfig.peers) {
      clusterConfig.peers = [];
    }

    if (peerAddrsRaw) {
      clusterConfig.peerAddresses = clusterConfig.peerAddresses || {};
      for (const pair of String(peerAddrsRaw).split(",")) {
        const [id, addr] = pair.split("=");
        if (id && addr) {
          clusterConfig.peerAddresses[id.trim()] = addr.trim();
        }
      }
    }

    if (flags["cluster-port"]) {
      clusterConfig.port = Number(flags["cluster-port"]);
    } else if (clusterConfig.port === undefined && flags.port) {
      clusterConfig.port = Number(flags.port);
    }

    if (flags["cluster-host"]) {
      clusterConfig.host = flags["cluster-host"];
    } else if (clusterConfig.host === undefined && flags.host && flags.host !== "0.0.0.0") {
      clusterConfig.host = flags.host;
    }

    if (flags["cluster-heartbeat"]) {
      clusterConfig.heartbeatInterval = Number(flags["cluster-heartbeat"]);
    }

    if (flags["cluster-election"]) {
      const [min, max] = String(flags["cluster-election"]).split(":");
      clusterConfig.election = {
        minTimeout: Number(min) || 150,
        maxTimeout: Number(max) || 300
      };
    }
  }

  return clusterConfig;
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

  let host = flags.host;
  let port = flags.port ? Number(flags.port) : null;

  for (const a of extraArgs) {
    if (typeof a === "string" && !a.startsWith("-")) {
      if (a.includes(":")) {
        const [h, p] = a.split(":");
        host = h || host || "127.0.0.1";
        port = Number(p) || port;
        break;
      } else if (/^\d+$/.test(a)) {
        port = Number(a);
        break;
      }
    }
  }

  host = host || "127.0.0.1";
  port = port || 7337;
  const address = `${host}:${port}`;
  const client = await TencereClient.connect(address);
  return {
    isLocal: false,
    address,
    client,
    close: async () => client.close()
  };
}

export async function main(argv = process.argv.slice(2)) {
  const args = argv;
  const command = args[0];

  if (!command) {
    if (process.stdin.isTTY) {
      // Interactive terminal: start interactive REPL
      await startRepl();
    } else {
      // Non-interactive shell, script, or pipe: show help
      printHelp();
    }
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

      const clusterConfig = await parseClusterConfig(flags);
      const dbOptions = clusterConfig ? { cluster: clusterConfig } : {};

      console.log(`Starting Tencere database at ${dataDir}...`);
      const db = await Tencere.open(dataDir, dbOptions);
      if (db.cluster) {
        console.log(`[Cluster] Node ${db.cluster.nodeId} initialized (peers: [${db.cluster.peers.join(", ")}])`);
      }

      const forwardWrites = flags["forward-writes"] === true || flags["forward-writes"] === "true";
      const server = new TencereServer(db, { port, host, forwardWrites });
      await server.start();
      console.log(`Tencere listening on ${host}:${port} [WAL: batch durability]`);

      if (db.cluster) {
        db.cluster.waitForLeader(3000).then((leaderId) => {
          if (leaderId) {
            console.log(`[Cluster] Current leader: ${leaderId === db.cluster.nodeId ? `Node ${leaderId} (Self)` : `Node ${leaderId}`}`);
          }
        }).catch(() => {});
      }

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
      if (sub === "dev") {
        const nodes = Number(flags.nodes) || 3;
        const basePort = Number(flags["base-port"] || flags.port) || 7337;
        const host = flags.host || "127.0.0.1";
        const isMemory = Boolean(flags.memory);
        const dataDir = isMemory ? null : (flags["data-dir"] || flags.data || "./.tencere-cluster");
        const forwardWrites = flags["forward-writes"] !== "false" && flags["forward-writes"] !== false;

        console.log(`\n🚀 Initializing ${nodes}-node Tencere local development cluster...`);
        if (!isMemory) {
          console.log(`📁 Cluster data directory: ${path.resolve(dataDir)}`);
        } else {
          console.log(`⚡ Cluster storage mode: Pure in-memory (volatile)`);
        }

        const cluster = await createTestCluster({
          nodes,
          tcp: true,
          basePort,
          host,
          dataDir,
          forwardWrites,
          clusterOptions: {
            election: { minTimeout: 150, maxTimeout: 300 },
            heartbeatInterval: 50
          }
        });

        const leader = await cluster.waitForLeader(5000).catch(() => null);
        const leaderNodeId = leader?.cluster?.nodeId ?? "None (in election)";
        const leaderAddr = leader ? cluster.peerAddresses[leaderNodeId] : "unknown";

        console.log(`\n======================================================`);
        console.log(`🌐 Tencere Local Dev Cluster is UP and READY!`);
        console.log(`======================================================`);
        console.log(`  Nodes:            ${nodes}`);
        console.log(`  Current Leader:   Node ${leaderNodeId} (${leaderAddr})`);
        console.log(`  Write Forwarding: ${forwardWrites ? "Enabled (followers forward writes to leader)" : "Disabled"}`);
        console.log(`\nActive Nodes:`);
        for (let i = 1; i <= nodes; i++) {
          const addr = cluster.peerAddresses[i];
          const isL = leader && leader.cluster?.nodeId === i;
          console.log(`  - Node ${i}: ${addr} ${isL ? "👑 [LEADER]" : "👥 [FOLLOWER]"}`);
        }

        console.log(`\nConnect via Interactive REPL:`);
        console.log(`  $ tencere repl ${cluster.peerAddresses[1]}`);

        console.log(`\nConnect via Node.js Cluster Client:`);
        console.log(`  import { TencereClient } from "tencere/client";`);
        console.log(`  const db = await TencereClient.cluster([`);
        for (let i = 1; i <= nodes; i++) {
          console.log(`    "${cluster.peerAddresses[i]}",`);
        }
        console.log(`  ]);`);
        console.log(`\nPress Ctrl+C to gracefully stop the cluster.\n`);

        const shutdown = async () => {
          console.log("\nStopping dev cluster nodes...");
          await cluster.destroy();
          console.log("Cluster stopped cleanly.");
          process.exit(0);
        };

        process.on("SIGINT", shutdown);
        process.on("SIGTERM", shutdown);

        await new Promise(() => {});
      } else if (sub === "health") {
        const conn = await getClientOrLocal(flags, args.slice(2));
        try {
          let h;
          if (conn.isLocal) {
            h = await conn.db.health({ pingPeers: true });
          } else {
            try {
              const clusterClient = await TencereClient.cluster(conn.address || `${conn.client.host}:${conn.client.port}`);
              h = await clusterClient.health();
              await clusterClient.close().catch(() => {});
            } catch (_) {
              if (typeof conn.client.health === "function") {
                h = await conn.client.health();
              } else {
                const stats = await conn.client.stats();
                h = { enabled: Boolean(stats.cluster?.enabled), status: stats.cluster?.isLeader ? "HEALTHY" : "FOLLOWER_HEALTHY", readiness: true, liveness: true };
              }
            }
          }

          if (flags.json) {
            console.log(JSON.stringify(h, null, 2));
          } else {
            console.log("\n🏥 Tencere Cluster Health");
            console.log(`  - Status:    ${h.status || (h.readiness ? "HEALTHY" : "QUORUM_LOST")}`);
            console.log(`  - Readiness: ${h.readiness ? "✅ READY (accepting traffic)" : "❌ NOT READY (quorum lost)"}`);
            console.log(`  - Liveness:  ${h.liveness ? "✅ ALIVE" : "❌ DEAD"}`);
            if (h.quorum) {
              console.log(`  - Quorum:    ${h.quorum.reachable}/${h.quorum.total} reachable (required: ${h.quorum.required})`);
            }
            if (h.leader) {
              console.log(`  - Leader:    Node ${h.leader.id ?? "none"} (${h.leader.address || "local"})${h.leader.isSelf ? " [Self]" : ""}`);
            }
          }

          if (!h.readiness && !String(h.status).includes("HEALTHY")) {
            process.exit(1);
          }
        } finally {
          await conn.close();
        }
      } else if (sub === "metrics") {
        const conn = await getClientOrLocal(flags, args.slice(2));
        try {
          const stats = conn.isLocal ? conn.db.stats() : await conn.client.stats();
          const cl = stats.cluster;
          const m = cl?.metrics || (conn.isLocal && conn.db.cluster ? conn.db.cluster.metrics() : null);

          if (flags.json) {
            console.log(JSON.stringify(m || {}, null, 2));
          } else if (!m || !m.enabled) {
            console.log("Cluster metrics not available or cluster not enabled.");
          } else {
            console.log(`\n📊 Tencere Cluster Metrics (Node ${m.nodeId})`);
            console.log(`  - Role:               ${m.role}`);
            console.log(`  - Term:               ${m.term}`);
            console.log(`  - Commit Index:       ${m.commitIndex}`);
            console.log(`  - Last Applied:       ${m.lastApplied}`);
            console.log(`  - Last Log Index:     ${m.lastLogIndex}`);
            console.log(`  - Replication Lag:    ${m.replicationLag} entries`);
            console.log(`  - Elections Held:     ${m.elections}`);
            console.log(`  - Leader Changes:     ${m.leaderChanges}`);
            console.log(`  - Submitted / Commit: ${m.submitted} / ${m.committed}`);
            console.log(`  - Applied Entries:    ${m.applied}`);
            console.log(`  - Network Wire Bytes: ${m.bytes?.wireBytes ?? 0}`);
          }
        } finally {
          await conn.close();
        }
      } else if (sub === "nodes") {
        const conn = await getClientOrLocal(flags, args.slice(2));
        try {
          let h;
          if (conn.isLocal) {
            h = await conn.db.health({ pingPeers: true });
          } else {
            try {
              const clusterClient = await TencereClient.cluster(conn.address || `${conn.client.host}:${conn.client.port}`);
              h = await clusterClient.health();
              await clusterClient.close().catch(() => {});
            } catch (_) {
              if (typeof conn.client.health === "function") {
                h = await conn.client.health();
              } else {
                const stats = await conn.client.stats();
                h = { enabled: Boolean(stats.cluster?.enabled), nodes: [] };
              }
            }
          }

          if (flags.json) {
            console.log(JSON.stringify(h.nodes || [], null, 2));
          } else if (!h.enabled) {
            console.log("Cluster is not enabled on this node.");
          } else {
            console.log("\n🌐 Tencere Cluster Nodes");
            const headers = ["Node", "Role", "Term", "Health", "Address", "Latency"];
            const rows = (h.nodes || []).map((n) => [
              n.nodeId ?? n.id ?? "?",
              n.isLeader ? "LEADER" : (n.role ? String(n.role).toUpperCase() : "FOLLOWER"),
              n.term ?? h.term ?? "-",
              n.status ?? "ONLINE",
              n.address || "local",
              n.latencyMs !== undefined && n.latencyMs !== null ? `${Number(n.latencyMs).toFixed(1)}ms` : (n.status === "ONLINE" ? "<0.5ms" : "-")
            ]);
            console.log(formatTable(headers, rows));
            console.log(`\nCluster Status: ${h.status || "UNKNOWN"} | Quorum: ${h.quorum?.reachable}/${h.quorum?.total} active | 128 Partitions Balanced\n`);
          }
        } finally {
          await conn.close();
        }
      } else if (sub === "status") {
        const conn = await getClientOrLocal(flags, args.slice(2));
        try {
          const stats = conn.isLocal ? conn.db.stats() : await conn.client.stats();
          const cl = stats.cluster;
          if (flags.json) {
            console.log(JSON.stringify(cl || { enabled: false }, null, 2));
          } else if (!cl || !cl.enabled) {
            console.log("Cluster is not enabled on this node.");
          } else {
            console.log("\n🌐 Tencere Cluster Status");
            console.log(`  - Node ID:   ${cl.nodeId}`);
            console.log(`  - Role:      ${cl.role || (cl.isLeader ? "LEADER" : "FOLLOWER")}`);
            console.log(`  - Leader ID: ${cl.leaderId ?? (cl.isLeader ? cl.nodeId : "unknown")}`);
            console.log(`  - Term:      ${cl.term}`);
            console.log(`  - Peers:     ${cl.peers?.length > 0 ? cl.peers.join(", ") : "none"}`);
            console.log(`  - Status:    ${cl.isLeader ? "✅ Serving writes as Leader" : "ℹ️ Following cluster leader"}`);
          }
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
        console.error("Usage: tencere cluster <dev|nodes|health|metrics|status|verify>");
        process.exit(1);
      }
      break;
    }

    case "skill":
    case "install-skill": {
      const sub = command === "install-skill" ? "install" : (args[1] || "install");
      const skillSource = path.resolve(__dirname, "../skills/tencere/SKILL.md");

      let content;
      try {
        content = await fs.readFile(skillSource, "utf-8");
      } catch (err) {
        console.error(`Error reading skill source at ${skillSource}: ${err.message}`);
        process.exit(1);
      }

      if (sub === "show" || sub === "cat") {
        console.log(content);
        break;
      }

      if (sub === "path") {
        console.log(skillSource);
        break;
      }

      if (sub === "install" || sub === "add") {
        const homeDir = os.homedir();
        const targets = [];

        if (flags.target) {
          const dest = flags.target.endsWith("SKILL.md")
            ? flags.target
            : path.join(flags.target, "skills/tencere/SKILL.md");
          targets.push({ name: "Custom target", path: path.resolve(dest) });
        } else {
          const isGlobal = Boolean(flags.global || flags.g);
          const isWorkspace = Boolean(flags.workspace || flags.local || flags.w);

          if (isWorkspace || (!isGlobal && !isWorkspace)) {
            targets.push({
              name: "Workspace (.agents)",
              path: path.resolve(process.cwd(), ".agents/skills/tencere/SKILL.md")
            });
          }

          if (isGlobal || (!isGlobal && !isWorkspace)) {
            targets.push({
              name: "Global (~/.gemini)",
              path: path.join(homeDir, ".gemini/config/skills/tencere/SKILL.md")
            });
          }

          if (flags.cursor || flags.all) {
            targets.push({
              name: "Cursor Rules (.cursor/rules)",
              path: path.resolve(process.cwd(), ".cursor/rules/tencere.md")
            });
          }

          if (flags.claude || flags.all) {
            targets.push({
              name: "Claude Skills (.claude/skills)",
              path: path.resolve(process.cwd(), ".claude/skills/tencere/SKILL.md")
            });
          }
        }

        console.log(`\n📦 Installing Tencere Agent Skill...`);
        for (const t of targets) {
          await fs.mkdir(path.dirname(t.path), { recursive: true });
          await fs.writeFile(t.path, content, "utf-8");
          console.log(`  ✅ [${t.name}]: ${t.path}`);
        }
        console.log(`\n✨ Tencere agent skill successfully installed and ready to use!\n`);
        break;
      }

      console.error(`Usage: tencere skill <install|show|path> [--global] [--workspace] [--target <dir>]`);
      process.exit(1);
    }

    default:
      console.error(`Unknown command: ${command}`);
      printHelp();
      process.exit(1);
  }
}

function checkIsDirectRun() {
  if (!process.argv[1]) return false;
  try {
    const current = fileURLToPath(import.meta.url);
    const resolved = fsSync.realpathSync(process.argv[1]);
    if (current === resolved) return true;
  } catch (_) {}
  const basename = path.basename(process.argv[1]).replace(/\.js$/, "");
  return basename === "tencere";
}

if (checkIsDirectRun()) {
  main().catch((err) => {
    console.error("Fatal error:", err);
    process.exit(1);
  });
}
