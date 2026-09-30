/**
 * TestCluster: In-memory & TCP cluster test harness for Tencere.
 * Enables zero-boilerplate cluster integration testing, network chaos testing
 * (partitions, split-brain, heal), node failure simulations, and cluster client testing.
 */

import path from "node:path";
import fs from "node:fs/promises";
import { MemoryNetwork } from "raptiye";
import { Tencere } from "../index.js";
import { TencereServer } from "../core/server.js";
import { TencereClient, TencereClusterClient } from "../client/index.js";

/**
 * Partitionable in-memory network for testing network splits, isolated nodes,
 * and recovery healing without physical network interfaces.
 */
export class PartitionableNetwork extends MemoryNetwork {
  constructor() {
    super();
    this.isolatedNodes = new Set();
    this.partitionPairs = new Set();
  }

  isolate(nodeId) {
    this.isolatedNodes.add(Number(nodeId));
  }

  heal(nodeId) {
    if (nodeId !== undefined) {
      this.isolatedNodes.delete(Number(nodeId));
    } else {
      this.isolatedNodes.clear();
      this.partitionPairs.clear();
    }
  }

  partition(groupA, groupB) {
    const setA = new Set((groupA || []).map(Number));
    const setB = new Set((groupB || []).map(Number));
    for (const a of setA) {
      for (const b of setB) {
        this.partitionPairs.add(`${a}->${b}`);
        this.partitionPairs.add(`${b}->${a}`);
      }
    }
  }

  deliver(from, to, buffers) {
    const fromId = Number(from);
    const toId = Number(to);
    if (this.isolatedNodes.has(fromId) || this.isolatedNodes.has(toId)) {
      return;
    }
    if (this.partitionPairs.has(`${fromId}->${toId}`)) {
      return;
    }
    super.deliver(from, to, buffers);
  }
}

let _nextDynamicPort = 12000 + Math.floor(Math.random() * 8000);

export class TestCluster {
  /**
   * @param {object} [options={}]
   * @param {number} [options.nodes=3] - Number of nodes in cluster (default: 3)
   * @param {boolean} [options.tcp=false] - Whether to use real TCP servers (default: false -> in-memory)
   * @param {number} [options.basePort] - Base port when TCP is enabled (defaults to dynamic range)
   * @param {string} [options.host="127.0.0.1"] - Host when TCP is enabled
   * @param {string} [options.dataDir] - Optional root directory for disk-backed logs
   * @param {boolean} [options.forwardWrites=true] - Server-side write forwarding if TCP
   * @param {object} [options.clusterOptions] - Extra cluster configuration options
   */
  constructor(options = {}) {
    this.nodeCount = options.nodes ?? 3;
    this.isTcp = Boolean(options.tcp);
    if (options.basePort) {
      this.basePort = Number(options.basePort);
    } else {
      this.basePort = _nextDynamicPort;
      _nextDynamicPort += (this.nodeCount + 5);
    }
    this.host = options.host || "127.0.0.1";
    this.baseDataDir = options.dataDir || null;
    this.forwardWrites = options.forwardWrites ?? true;
    this.clusterOptions = options.clusterOptions || {};

    this.network = new PartitionableNetwork();
    this.nodes = new Map(); // id -> Tencere
    this.servers = new Map(); // id -> TencereServer
    this.ports = new Map(); // id -> port
    this.peerAddresses = {};
    this._clients = [];
    this._started = false;
  }

  /**
   * Initializes and starts all cluster nodes and servers.
   *
   * @returns {Promise<TestCluster>}
   */
  async start() {
    for (let i = 1; i <= this.nodeCount; i++) {
      const port = this.basePort + i - 1;
      this.ports.set(i, port);
      this.peerAddresses[i] = `${this.host}:${port}`;
    }

    for (let id = 1; id <= this.nodeCount; id++) {
      await this._startNodeInstance(id);
    }

    this._started = true;
    await this.waitForLeader(this.clusterOptions.leaderTimeoutMs || 3000);
    return this;
  }

  async _startNodeInstance(id) {
    const peers = [];
    for (let p = 1; p <= this.nodeCount; p++) {
      if (p !== id) peers.push(p);
    }

    let nodeDir = undefined;
    if (this.baseDataDir) {
      nodeDir = path.join(this.baseDataDir, `node-${id}`);
      await fs.mkdir(nodeDir, { recursive: true });
    }

    const clusterConf = {
      nodeId: id,
      peers,
      network: this.clusterOptions.network || this.network,
      peerAddresses: { ...this.peerAddresses },
      election: this.clusterOptions.election || { minTimeout: 30, maxTimeout: 60 },
      heartbeatInterval: this.clusterOptions.heartbeatInterval || 15,
      ...this.clusterOptions
    };

    if (this.clusterOptions.clusterPortBase) {
      clusterConf.port = this.clusterOptions.clusterPortBase + id - 1;
      clusterConf.host = this.host;
    }

    const db = await Tencere.open(nodeDir, { cluster: clusterConf });
    db._testNodeId = id;
    db.address = this.peerAddresses[id];
    this.nodes.set(id, db);

    if (this.isTcp) {
      const server = new TencereServer(db, {
        port: this.ports.get(id),
        host: this.host,
        forwardWrites: this.forwardWrites
      });
      await server.start();
      this.servers.set(id, server);
    }

    return db;
  }

  /**
   * Returns current elected leader Tencere instance, or null if in election.
   *
   * @returns {import('../index.js').Tencere | null}
   */
  get leader() {
    let bestLeader = null;
    let highestTerm = -1;

    for (const node of this.nodes.values()) {
      const id = node.cluster?.nodeId || node._testNodeId;
      if (this.network && this.network.isolatedNodes.has(Number(id))) {
        continue;
      }
      if (node.cluster && node.cluster.isLeader()) {
        const term = Number(node.cluster.term) || 0;
        if (term > highestTerm) {
          highestTerm = term;
          bestLeader = node;
        }
      }
    }
    return bestLeader;
  }

  /**
   * Returns array of follower Tencere instances.
   *
   * @returns {import('../index.js').Tencere[]}
   */
  get followers() {
    const list = [];
    for (const node of this.nodes.values()) {
      const id = node.cluster?.nodeId || node._testNodeId;
      if (this.network && this.network.isolatedNodes.has(Number(id))) {
        continue;
      }
      if (node.cluster && !node.cluster.isLeader()) {
        list.push(node);
      }
    }
    return list;
  }

  /**
   * Returns specific node instance by ID (1..N).
   *
   * @param {number} id
   * @returns {import('../index.js').Tencere | null}
   */
  node(id) {
    return this.nodes.get(Number(id)) || null;
  }

  /**
   * Returns list of all cluster TCP addresses.
   *
   * @returns {string[]}
   */
  get addresses() {
    return Object.values(this.peerAddresses);
  }

  /**
   * Waits for a cluster leader to be elected.
   *
   * @param {number} [timeoutMs=3000]
   * @returns {Promise<import('../index.js').Tencere>}
   */
  async waitForLeader(timeoutMs = 3000) {
    const start = Date.now();
    while (Date.now() - start < timeoutMs) {
      const l = this.leader;
      if (l) return l;
      await new Promise((r) => setTimeout(r, 20));
    }
    throw new Error(`[TestCluster] Timed out waiting for leader election after ${timeoutMs}ms`);
  }

  /**
   * Isolates a node from all network communications (simulates network split / partitioned leader).
   *
   * @param {import('../index.js').Tencere | number} nodeOrId
   */
  isolate(nodeOrId) {
    const id = typeof nodeOrId === "object" && nodeOrId !== null
      ? (nodeOrId.cluster?.nodeId || nodeOrId._testNodeId)
      : Number(nodeOrId);

    if (this.network) {
      this.network.isolate(id);
    }
  }

  /**
   * Heals network isolation for a specific node, or all nodes.
   *
   * @param {import('../index.js').Tencere | number} [nodeOrId]
   */
  heal(nodeOrId) {
    if (nodeOrId !== undefined) {
      const id = typeof nodeOrId === "object" && nodeOrId !== null
        ? (nodeOrId.cluster?.nodeId || nodeOrId._testNodeId)
        : Number(nodeOrId);
      if (this.network) {
        this.network.heal(id);
      }
    } else if (this.network) {
      this.network.heal();
    }
  }

  /**
   * Partitions the cluster into two isolated subsets of nodes.
   *
   * @param {number[]} groupA
   * @param {number[]} groupB
   */
  partition(groupA, groupB) {
    if (this.network) {
      this.network.partition(groupA, groupB);
    }
  }

  /**
   * Stops a specific node and its TCP server.
   *
   * @param {import('../index.js').Tencere | number} nodeOrId
   */
  async stopNode(nodeOrId) {
    const id = typeof nodeOrId === "object" && nodeOrId !== null
      ? (nodeOrId.cluster?.nodeId || nodeOrId._testNodeId)
      : Number(nodeOrId);

    const server = this.servers.get(id);
    if (server) {
      await server.stop();
      this.servers.delete(id);
    }

    const node = this.nodes.get(id);
    if (node) {
      await node.close();
      this.nodes.delete(id);
    }
  }

  /**
   * Starts a previously stopped node.
   *
   * @param {number} id
   * @returns {Promise<import('../index.js').Tencere>}
   */
  async startNode(id) {
    return this._startNodeInstance(Number(id));
  }

  /**
   * Restarts a specific node.
   *
   * @param {import('../index.js').Tencere | number} nodeOrId
   * @returns {Promise<import('../index.js').Tencere>}
   */
  async restartNode(nodeOrId) {
    const id = typeof nodeOrId === "object" && nodeOrId !== null
      ? (nodeOrId.cluster?.nodeId || nodeOrId._testNodeId)
      : Number(nodeOrId);
    await this.stopNode(id);
    return this.startNode(id);
  }

  /**
   * Creates a cluster client connected to this test cluster.
   *
   * @param {object} [options={}]
   * @returns {Promise<TencereClusterClient | any>}
   */
  async client(options = {}) {
    if (this.isTcp) {
      const client = await TencereClient.cluster(this.addresses, options);
      this._clients.push(client);
      return client;
    }

    // In-memory cluster client proxy
    const self = this;
    const memoryClient = {
      async set(key, value, opt) {
        const l = await self.waitForLeader();
        return l.set(key, value, opt);
      },
      async get(key, opt) {
        const pref = options.readPreference || "leader";
        if (pref === "follower" && self.followers.length > 0) {
          const f = self.followers[Math.floor(Math.random() * self.followers.length)];
          return f.get(key, opt);
        }
        const l = await self.waitForLeader();
        return l.get(key, opt);
      },
      async delete(key) {
        const l = await self.waitForLeader();
        return l.delete(key);
      },
      async increment(key, delta) {
        const l = await self.waitForLeader();
        return l.increment(key, delta);
      },
      async patch(key, patch) {
        const l = await self.waitForLeader();
        return l.patch(key, patch);
      },
      async has(key) {
        const l = await self.waitForLeader();
        return l.has(key);
      },
      async stats() {
        const l = await self.waitForLeader();
        return l.stats();
      },
      async close() {}
    };

    return memoryClient;
  }

  /**
   * Evaluates comprehensive health status of the test cluster.
   *
   * @param {object} [options={}]
   * @returns {Promise<object>}
   */
  async health(options = {}) {
    const leader = this.leader || this.nodes.values().next().value;
    if (leader && leader.cluster) {
      return leader.cluster.health(options);
    }
    return {
      enabled: false,
      status: "STANDALONE",
      readiness: false,
      liveness: false
    };
  }

  /**
   * Retrieves Raft and replication telemetry metrics from the cluster leader.
   *
   * @returns {object}
   */
  metrics() {
    const leader = this.leader || this.nodes.values().next().value;
    if (leader && leader.cluster) {
      return leader.cluster.metrics();
    }
    return { enabled: false };
  }

  /**
   * Cleanly closes all nodes, servers, and clients.
   *
   * @returns {Promise<void>}
   */
  async destroy() {
    for (const c of this._clients) {
      await c.close().catch(() => {});
    }
    this._clients = [];

    for (const s of this.servers.values()) {
      await s.stop().catch(() => {});
    }
    this.servers.clear();

    for (const n of this.nodes.values()) {
      await n.close().catch(() => {});
    }
    this.nodes.clear();
  }

  async close() {
    return this.destroy();
  }
}

/**
 * Convenience helper to create, start, and return an initialized TestCluster.
 *
 * @param {object} [options={}]
 * @returns {Promise<TestCluster>}
 */
export async function createTestCluster(options = {}) {
  const cluster = new TestCluster(options);
  await cluster.start();
  return cluster;
}
