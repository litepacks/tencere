/**
 * Cluster-aware client for Tencere distributed clusters.
 *
 * Provides:
 * - Multi-seed initial connectivity & automatic topology discovery
 * - Leader write routing with automatic failover and retries on ERR_NOT_LEADER
 * - Configurable read preferences ('leader', 'follower', 'nearest')
 * - Automatic reconnection when cluster re-elects a leader
 * - Full parity with standalone TencereClient API (KV, pipeline, exec, etc.)
 */

import { EventEmitter } from "node:events";
import { TencereClient } from "./index.js";
import { ClusterNotAvailableError, NotLeaderError } from "../errors.js";

const WRITE_METHODS = new Set([
  "set",
  "delete",
  "clear",
  "add",
  "push",
  "pop",
  "shift",
  "unshift",
  "append",
  "trim",
  "insert",
  "upsert",
  "update",
  "increment",
  "decrement",
  "consume",
  "expire",
  "restore",
  "remove",
  "addMany"
]);

const WRITE_TARGETS = new Set([
  "lock",
  "unlock",
  "rateLimit",
  "setMany",
  "checkpoint"
]);

export class TencereClusterClient extends EventEmitter {
  /**
   * @param {string|string[]} seedAddresses
   * @param {object} [options={}]
   * @param {'leader'|'nearest'|'follower'} [options.readPreference='leader']
   * @param {number} [options.maxRetries=3]
   * @param {number} [options.retryDelayMs=100]
   * @param {number} [options.refreshIntervalMs=10000]
   */
  constructor(seedAddresses, options = {}) {
    super();
    const rawSeeds = Array.isArray(seedAddresses)
      ? seedAddresses
      : String(seedAddresses || "").split(",").map((s) => s.trim()).filter(Boolean);

    this.seeds = rawSeeds.length > 0 ? rawSeeds : ["127.0.0.1:7337"];
    this.options = {
      readPreference: options.readPreference || "leader",
      maxRetries: options.maxRetries ?? 3,
      retryDelayMs: options.retryDelayMs ?? 100,
      refreshIntervalMs: options.refreshIntervalMs ?? 10000,
      ...options
    };

    this._clients = new Map(); // address -> TencereClient
    this._leaderAddress = null;
    this._nodeAddresses = new Set(this.seeds);
    this._refreshTimer = null;
    this._roundRobinIdx = 0;
    this._closed = false;
  }

  /**
   * Connects to the cluster and discovers topology.
   *
   * @returns {Promise<TencereClusterClient>}
   */
  async connect() {
    await this.discoverTopology();

    if (this.options.refreshIntervalMs > 0) {
      this._refreshTimer = setInterval(() => {
        if (!this._closed) {
          this.discoverTopology().catch(() => {});
        }
      }, this.options.refreshIntervalMs);
      if (this._refreshTimer.unref) {
        this._refreshTimer.unref();
      }
    }

    return this;
  }

  /**
   * Probes active nodes and refreshes cluster topology (leader, peers).
   *
   * @returns {Promise<{ leader: string|null, nodes: string[] }>}
   */
  async discoverTopology() {
    const candidates = [
      ...(this._leaderAddress ? [this._leaderAddress] : []),
      ...this._nodeAddresses,
      ...this.seeds
    ];
    const uniqueCandidates = [...new Set(candidates)];

    let activeStats = null;
    let respondingAddr = null;

    for (const addr of uniqueCandidates) {
      try {
        const client = await this._getClient(addr);
        const stats = await client.stats();
        activeStats = stats;
        respondingAddr = addr;
        break;
      } catch (err) {
        // Node not reachable; drop stale client
        const c = this._clients.get(addr);
        if (c) {
          c.close().catch(() => {});
          this._clients.delete(addr);
        }
      }
    }

    if (!activeStats) {
      if (!this._leaderAddress && this._clients.size === 0) {
        throw new ClusterNotAvailableError(
          `Could not connect to any cluster seed node: [${uniqueCandidates.join(", ")}]`
        );
      }
      return { leader: this._leaderAddress, nodes: Array.from(this._nodeAddresses) };
    }

    const prevLeader = this._leaderAddress;

    if (activeStats.cluster && activeStats.cluster.enabled) {
      const clusterInfo = activeStats.cluster;
      if (clusterInfo.leaderAddress) {
        this._leaderAddress = clusterInfo.leaderAddress;
        this._nodeAddresses.add(clusterInfo.leaderAddress);
      }

      if (clusterInfo.peerAddresses && typeof clusterInfo.peerAddresses === "object") {
        for (const peerAddr of Object.values(clusterInfo.peerAddresses)) {
          if (peerAddr && typeof peerAddr === "string") {
            this._nodeAddresses.add(peerAddr);
          }
        }
      }

      this._nodeAddresses.add(respondingAddr);
    } else {
      // Standalone node mode
      this._leaderAddress = respondingAddr;
      this._nodeAddresses.add(respondingAddr);
    }

    if (prevLeader !== this._leaderAddress) {
      this.emit("leaderChange", {
        previousLeader: prevLeader,
        currentLeader: this._leaderAddress
      });
    }

    const topo = {
      leader: this._leaderAddress,
      nodes: Array.from(this._nodeAddresses)
    };
    this.emit("topologyChange", topo);
    return topo;
  }

  async _getClient(address) {
    if (this._closed) {
      throw new Error("TencereClusterClient is closed");
    }
    let client = this._clients.get(address);
    if (client && client.connected) {
      return client;
    }

    client = await TencereClient.connect(address, { autoRedirect: false });
    this._clients.set(address, client);

    client.socket?.on("close", () => {
      if (this._clients.get(address) === client) {
        this._clients.delete(address);
      }
    });
    client.socket?.on("error", () => {
      if (this._clients.get(address) === client) {
        this._clients.delete(address);
      }
    });

    return client;
  }

  async _getLeaderClient() {
    if (!this._leaderAddress) {
      await this.discoverTopology();
    }
    if (this._leaderAddress) {
      try {
        return await this._getClient(this._leaderAddress);
      } catch (_) {
        // leader unreachable, trigger rediscovery
        await this.discoverTopology();
      }
    }
    if (this._leaderAddress) {
      return this._getClient(this._leaderAddress);
    }
    throw new ClusterNotAvailableError("No leader available in cluster");
  }

  async _getReadClient() {
    const pref = this.options.readPreference;
    if (pref === "leader") {
      return this._getLeaderClient();
    }

    const allNodes = Array.from(this._nodeAddresses);
    if (allNodes.length === 0) {
      return this._getLeaderClient();
    }

    if (pref === "follower") {
      const followers = allNodes.filter((addr) => addr !== this._leaderAddress);
      if (followers.length > 0) {
        const addr = followers[Math.floor(Math.random() * followers.length)];
        try {
          return await this._getClient(addr);
        } catch (_) {
          return this._getLeaderClient();
        }
      }
      return this._getLeaderClient();
    }

    // 'nearest' or default: round-robin across all active nodes
    const addr = allNodes[(this._roundRobinIdx++) % allNodes.length];
    try {
      return await this._getClient(addr);
    } catch (_) {
      return this._getLeaderClient();
    }
  }

  async _executeWrite(action) {
    let lastError = null;
    const maxRetries = this.options.maxRetries;
    const retryDelay = this.options.retryDelayMs;

    for (let attempt = 0; attempt <= maxRetries; attempt++) {
      try {
        const leader = await this._getLeaderClient();
        return await action(leader);
      } catch (err) {
        lastError = err;
        if (err.code === "ERR_NOT_LEADER") {
          if (err.leaderAddress) {
            const prevLeader = this._leaderAddress;
            this._leaderAddress = err.leaderAddress;
            this._nodeAddresses.add(err.leaderAddress);
            if (prevLeader !== this._leaderAddress) {
              this.emit("leaderChange", {
                previousLeader: prevLeader,
                currentLeader: this._leaderAddress
              });
            }
          } else {
            await this.discoverTopology().catch(() => {});
          }
        } else {
          // Connection loss or leader crash
          await this.discoverTopology().catch(() => {});
        }

        if (attempt < maxRetries) {
          await new Promise((r) => setTimeout(r, retryDelay));
        }
      }
    }
    throw lastError;
  }

  async _executeRead(action) {
    let lastError = null;
    const maxRetries = this.options.maxRetries;
    const retryDelay = this.options.retryDelayMs;

    for (let attempt = 0; attempt <= maxRetries; attempt++) {
      try {
        const client = await this._getReadClient();
        return await action(client);
      } catch (err) {
        lastError = err;
        await this.discoverTopology().catch(() => {});
        if (attempt < maxRetries) {
          await new Promise((r) => setTimeout(r, retryDelay));
        }
      }
    }
    throw lastError;
  }

  // --- KV & Mutation Operations ---

  async get(key, options) {
    return this._executeRead((c) => c.get(key, options));
  }

  async set(key, value, options) {
    return this._executeWrite((c) => c.set(key, value, options));
  }

  async delete(key) {
    return this._executeWrite((c) => c.delete(key));
  }

  async has(key) {
    return this._executeRead((c) => c.has(key));
  }

  async increment(key, delta = 1) {
    return this._executeWrite((c) => c.increment(key, delta));
  }

  async patch(key, patch) {
    return this._executeWrite((c) => c.patch(key, patch));
  }

  async stats() {
    return this._executeRead((c) => c.stats());
  }

  async keys(prefix = "") {
    return this._executeRead((c) => c.keys(prefix));
  }

  async clear() {
    return this._executeWrite((c) => c.clear());
  }

  async ttl(key) {
    return this._executeRead((c) => c.ttl(key));
  }

  async getMany(keys) {
    return this._executeRead((c) => c.getMany(keys));
  }

  async setMany(entries) {
    return this._executeWrite((c) => c.setMany(entries));
  }

  async unlock(key) {
    return this._executeWrite((c) => c.unlock(key));
  }

  async checkpoint() {
    return this._executeWrite((c) => c.checkpoint());
  }

  async ping() {
    return this._executeRead((c) => c.ping());
  }

  async exec(target, name, method, args = []) {
    const isWrite = WRITE_TARGETS.has(target) || WRITE_METHODS.has(method);
    if (isWrite) {
      return this._executeWrite((c) => c.exec(target, name, method, args));
    }
    return this._executeRead((c) => c.exec(target, name, method, args));
  }

  async watch(pattern = "", handler) {
    const leader = await this._getLeaderClient();
    return leader.watch(pattern, handler);
  }

  async unwatch() {
    if (this._leaderAddress) {
      const client = this._clients.get(this._leaderAddress);
      if (client) {
        return client.unwatch();
      }
    }
  }

  pipeline() {
    return new ClusterClientPipeline(this);
  }

  getLeaderAddress() {
    return this._leaderAddress;
  }

  getNodes() {
    return Array.from(this._nodeAddresses);
  }

  status() {
    return {
      leaderAddress: this._leaderAddress,
      nodes: Array.from(this._nodeAddresses),
      readPreference: this.options.readPreference,
      connectedClients: this._clients.size
    };
  }

  /**
   * Evaluates and aggregates overall cluster health across all known cluster nodes.
   *
   * @param {object} [options={}]
   * @param {number} [options.timeoutMs=1000]
   * @returns {Promise<object>}
   */
  async health(options = {}) {
    await this.discoverTopology().catch(() => {});
    const nodes = Array.from(this._nodeAddresses);
    const nodeReports = [];
    let reachableCount = 0;
    let detectedLeader = null;
    let detectedTerm = 0;

    for (const addr of nodes) {
      const start = Date.now();
      try {
        const client = await this._getClient(addr);
        await client.ping();
        const latencyMs = Date.now() - start;
        const stats = await client.stats();
        const cl = stats.cluster || {};
        const isLeader = Boolean(cl.isLeader || (cl.leaderAddress && cl.leaderAddress === addr));

        if (isLeader || cl.role === "leader" || cl.role === "LEADER") {
          detectedLeader = addr;
          if (cl.term) detectedTerm = Number(cl.term);
        }

        reachableCount++;
        nodeReports.push({
          nodeId: cl.nodeId ?? null,
          address: addr,
          role: cl.role || (isLeader ? "leader" : "follower"),
          term: cl.term ?? null,
          status: "ONLINE",
          latencyMs,
          isLeader
        });
      } catch (err) {
        nodeReports.push({
          nodeId: null,
          address: addr,
          role: "unknown",
          term: null,
          status: "UNREACHABLE",
          latencyMs: null,
          isLeader: false,
          error: err.message
        });
      }
    }

    const total = nodes.length;
    const required = Math.floor(total / 2) + 1;
    const hasQuorum = reachableCount >= required && Boolean(detectedLeader || this._leaderAddress);

    let status = "HEALTHY";
    if (!hasQuorum) {
      status = "QUORUM_LOST";
    } else if (reachableCount < total) {
      status = "DEGRADED";
    }

    return {
      enabled: true,
      status, // "HEALTHY" | "DEGRADED" | "QUORUM_LOST"
      readiness: hasQuorum,
      liveness: true,
      leaderAddress: detectedLeader || this._leaderAddress,
      term: detectedTerm,
      quorum: {
        required,
        reachable: reachableCount,
        total,
        hasQuorum
      },
      nodes: nodeReports
    };
  }

  async close() {
    this._closed = true;
    if (this._refreshTimer) {
      clearInterval(this._refreshTimer);
      this._refreshTimer = null;
    }
    for (const client of this._clients.values()) {
      await client.close().catch(() => {});
    }
    this._clients.clear();
  }
}

export class ClusterClientPipeline {
  constructor(clusterClient) {
    this._clusterClient = clusterClient;
    this._operations = [];
  }

  ping() {
    this._operations.push((p) => p.ping());
    return this;
  }

  get(key, options) {
    this._operations.push((p) => p.get(key, options));
    return this;
  }

  set(key, value, options) {
    this._operations.push((p) => p.set(key, value, options));
    return this;
  }

  delete(key) {
    this._operations.push((p) => p.delete(key));
    return this;
  }

  has(key) {
    this._operations.push((p) => p.has(key));
    return this;
  }

  increment(key, delta = 1) {
    this._operations.push((p) => p.increment(key, delta));
    return this;
  }

  patch(key, patch) {
    this._operations.push((p) => p.patch(key, patch));
    return this;
  }

  ttl(key) {
    this._operations.push((p) => p.ttl(key));
    return this;
  }

  async exec() {
    return this._clusterClient._executeWrite(async (leaderClient) => {
      const p = leaderClient.pipeline();
      for (const op of this._operations) {
        op(p);
      }
      return p.exec();
    });
  }
}
