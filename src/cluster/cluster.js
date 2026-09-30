import { EventEmitter } from "node:events";
import net from "node:net";
import { Raptiye, MemoryLog, FileLog, TCPTransport, MemoryTransport, MemoryNetwork } from "raptiye";
import path from "node:path";
import { Operation } from "../core/operations.js";
import { LIMITS } from "../core/limits.js";
import { NotLeaderError } from "../errors.js";

export class ClusterManager extends EventEmitter {
  /**
   * @param {import('../core/engine.js').TencereEngine} engine
   * @param {object} clusterConfig
   */
  constructor(engine, clusterConfig) {
    super();
    this._engine = engine;
    this._config = clusterConfig;
    this._nodeId = Number(clusterConfig.nodeId) || 1;
    this._peers = (clusterConfig.peers || []).map(Number);
    this._raptiye = null;
    this._transport = null;
    this._storage = null;
    this.enabled = true;
  }

  /**
   * Starts cluster node with Raptiye consensus.
   */
  async start() {
    const isFileLog = Boolean(this._engine.dataDir);

    if (isFileLog) {
      const raftDir = path.join(this._engine.dataDir, "raft");
      this._storage = new FileLog(raftDir);
    } else {
      this._storage = new MemoryLog();
    }
    if (this._storage && typeof this._storage.open === "function") {
      await this._storage.open();
    }

    if (this._config.port) {
      let peerMap = new Map();
      const raw = this._config.peerAddresses || {};
      if (raw instanceof Map) {
        peerMap = raw;
      } else {
        for (const [id, addr] of Object.entries(raw)) {
          if (typeof addr === "string") {
            const [h, p] = addr.split(":");
            peerMap.set(Number(id), { host: h || "127.0.0.1", port: Number(p) });
          } else if (addr && typeof addr === "object") {
            peerMap.set(Number(id), { host: addr.host || "127.0.0.1", port: Number(addr.port) });
          }
        }
      }
      this._transport = new TCPTransport({
        id: this._nodeId,
        port: this._config.port,
        host: this._config.host || "127.0.0.1",
        peerAddresses: peerMap
      });
    } else {
      const network = this._config.network || new MemoryNetwork();
      this._transport = new MemoryTransport(this._nodeId, network);
    }
    await this._transport.start();

    this._raptiye = new Raptiye({
      id: this._nodeId,
      peers: this._peers,
      storage: this._storage,
      transport: this._transport,
      election: this._config.election || {
        minTimeout: LIMITS.CLUSTER.DEFAULT_ELECTION_MIN_MS,
        maxTimeout: LIMITS.CLUSTER.DEFAULT_ELECTION_MAX_MS
      },
      heartbeatInterval: this._config.heartbeatInterval || LIMITS.CLUSTER.DEFAULT_HEARTBEAT_MS,
      apply: (entry) => {
        try {
          if (!entry || !entry.payload) return;
          const len = entry.payload.byteLength ?? entry.payload.length ?? 0;
          if (len < 21) return;
          const op = Operation.decode(entry.payload);
          this._engine._applyOperation(op, true);
        } catch (err) {
          console.error(`[Cluster Apply Error] Node ${this._nodeId}:`, err);
        }
      }
    });

    await this._raptiye.start();

    // Rebalance partitions across cluster peers
    const allNodes = [String(this._nodeId), ...this._peers.map(String)];
    this._engine.partitions.rebalance(allNodes);
    this._engine.cluster = this;
  }

  get nodeId() {
    return this._nodeId;
  }

  get peers() {
    return [...this._peers];
  }

  get leaderId() {
    if (this.isLeader()) return this._nodeId;
    const lId = this._raptiye?.leader?.();
    return lId ? Number(lId) : null;
  }

  get role() {
    if (!this._raptiye) return "standalone";
    const roleVal = typeof this._raptiye.role === "function"
      ? this._raptiye.role()
      : (typeof this._raptiye.role === "string" ? this._raptiye.role : this._raptiye.role?.name || "UNKNOWN");
    const role = typeof roleVal === "object" && roleVal !== null ? (roleVal.name || String(roleVal)) : String(roleVal);
    return role.toLowerCase();
  }

  get term() {
    return this.currentTerm;
  }

  /**
   * Resolves the network TCP address for the current cluster leader, if known.
   * @returns {string|null}
   */
  getLeaderAddress() {
    const lid = this.leaderId;
    if (!lid) return null;
    const addrs = this._config.peerAddresses || this._config.peerAddrs || {};
    if (addrs[lid] || addrs[String(lid)]) {
      return addrs[lid] || addrs[String(lid)];
    }
    if (lid === this._nodeId) {
      if (this._config.port) {
        return `${this._config.host || "127.0.0.1"}:${this._config.port}`;
      }
      return null;
    }
    return null;
  }

  /**
   * Waits for a cluster leader to be elected.
   *
   * @param {number} [timeoutMs=1500]
   * @returns {Promise<number|null>} Leader node ID or null
   */
  async waitForLeader(timeoutMs = 1500) {
    const start = Date.now();
    while (Date.now() - start < timeoutMs) {
      if (this.isLeader()) {
        const lid = this._nodeId;
        this.emit("leader", { leaderId: lid, role: this.role, term: this.term });
        return lid;
      }
      const lId = this._raptiye?.leader?.();
      if (lId) {
        const lid = Number(lId);
        this.emit("leader", { leaderId: lid, role: this.role, term: this.term });
        return lid;
      }
      await new Promise((r) => setTimeout(r, 15));
    }
    return null;
  }

  /**
   * Checks if this node is currently the cluster leader.
   *
   * @returns {boolean}
   */
  isLeader() {
    if (!this._raptiye) return true;
    if (this._peers.length === 0) return true;
    const role = this.role;
    return role.includes("leader");
  }

  get currentTerm() {
    if (!this._raptiye) return 1;
    if (this._raptiye.engine && this._raptiye.engine.currentTerm !== undefined) {
      return Number(this._raptiye.engine.currentTerm);
    }
    return typeof this._raptiye.term === "function" ? this._raptiye.term() : (this._raptiye.currentTerm || 1);
  }

  /**
   * Proposes an operation to the replicated Raft log with ack policy.
   *
   * @param {Operation} op
   * @param {object} [options={}]
   * @param {'local'|'quorum'|'all'} [options.ack='quorum']
   * @returns {Promise<any>}
   */
  async replicate(op, options = {}) {
    if (!this._raptiye) return null;
    const ack = options.ack || "quorum";
    const payload = op.encode();

    if (this._peers.length === 0) {
      if (typeof this._raptiye?.submit === "function") {
        try {
          const idx = this._raptiye.submit(payload);
          return { ack: "local", index: idx };
        } catch (_) {}
      }
      return { ack: "local", index: 1n };
    }

    if (!this.isLeader()) {
      const st = this.status();
      const addr = this.getLeaderAddress();
      throw new NotLeaderError(this._nodeId, this.leaderId, st.role, st.term, addr);
    }

    if (ack === "local") {
      if (typeof this._raptiye.submit === "function") {
        const idx = this._raptiye.submit(payload);
        return { ack: "local", index: idx };
      }
      return { ack: "local" };
    }

    if (typeof this._raptiye.submit === "function") {
      const targetIndex = this._raptiye.submit(payload);
      if (ack === "quorum" || ack === "all") {
        const timeoutMs = options.timeoutMs || 2500;
        await Promise.race([
          this._raptiye.committed(targetIndex),
          new Promise((_, reject) =>
            setTimeout(() => reject(new Error(`[Cluster Quorum Timeout] Timed out waiting for commit index ${targetIndex}`)), timeoutMs)
          )
        ]);
      }
      return { ack, index: targetIndex };
    }
    return null;
  }

  /**
   * Returns current cluster status.
   */
  status() {
    if (!this._raptiye) return { enabled: false };
    const roleVal = typeof this._raptiye.role === "function"
      ? this._raptiye.role()
      : (typeof this._raptiye.role === "string" ? this._raptiye.role : this._raptiye.role?.name || "UNKNOWN");
    const role = typeof roleVal === "object" && roleVal !== null ? (roleVal.name || String(roleVal)) : String(roleVal);
    return {
      enabled: true,
      nodeId: this._nodeId,
      leaderId: this.leaderId,
      leaderAddress: this.getLeaderAddress(),
      term: this.currentTerm,
      isLeader: this.isLeader(),
      role,
      peers: this._peers,
      peerAddresses: this._config.peerAddresses || {},
      quorum: {
        required: Math.floor((1 + this._peers.length) / 2) + 1,
        total: 1 + this._peers.length,
        hasQuorum: this.leaderId !== null
      },
      metrics: this.metrics()
    };
  }

  /**
   * Returns Raft & replication telemetry metrics.
   *
   * @returns {object}
   */
  metrics() {
    if (!this._raptiye) {
      return { enabled: false };
    }
    const rStats = typeof this._raptiye.stats === "function" ? this._raptiye.stats() : {};
    const commitIndex = rStats.commitIndex !== undefined ? Number(rStats.commitIndex) : 0;
    const lastApplied = rStats.lastApplied !== undefined ? Number(rStats.lastApplied) : 0;
    const lastLogIndex = rStats.lastLogIndex !== undefined ? Number(rStats.lastLogIndex) : commitIndex;
    const replicationLag = Math.max(0, lastLogIndex - commitIndex);

    return {
      enabled: true,
      nodeId: this._nodeId,
      role: this.role,
      term: Number(this.currentTerm),
      leaderId: this.leaderId,
      commitIndex,
      lastApplied,
      lastLogIndex,
      replicationLag,
      elections: rStats.elections || 0,
      leaderChanges: rStats.leaderChanges || 0,
      submitted: rStats.submitted || 0,
      committed: rStats.committed || 0,
      applied: rStats.applied || 0,
      bytes: {
        wireBytes: rStats.wireBytes || 0,
        payloadBytes: rStats.payloadBytes || 0,
        protocolOverheadBytes: rStats.protocolOverheadBytes || 0,
        copiedBytes: rStats.copiedBytes || 0
      }
    };
  }

  /**
   * Evaluates comprehensive cluster health and readiness status.
   *
   * @param {object} [options={}]
   * @param {boolean} [options.pingPeers=false]
   * @param {number} [options.timeoutMs=500]
   * @returns {Promise<object>}
   */
  async health(options = {}) {
    if (!this._raptiye) {
      return {
        enabled: false,
        status: "STANDALONE",
        readiness: true,
        liveness: true,
        message: "Clustering is not enabled on this node"
      };
    }

    const total = 1 + this._peers.length;
    const required = Math.floor(total / 2) + 1;
    const leaderId = this.leaderId;
    const term = Number(this.currentTerm);
    const selfRole = this.role;

    const peerNodes = [];
    let reachableCount = 1; // self is reachable

    // Assess peers
    for (const peer of this._peers) {
      let reachable = true;
      let latencyMs = undefined;
      const addr = (this._config.peerAddresses || {})[peer] || (this._config.peerAddresses || {})[String(peer)] || null;

      // 1. Check in-memory network isolation/partition if applicable
      const memoryNet = this._transport?.network;
      if (memoryNet) {
        if (memoryNet.isolatedNodes?.has(Number(peer)) || memoryNet.isolatedNodes?.has(Number(this._nodeId))) {
          reachable = false;
        } else if (memoryNet.partitionPairs?.has(`${this._nodeId}->${peer}`) || memoryNet.partitionPairs?.has(`${peer}->${this._nodeId}`)) {
          reachable = false;
        } else if (memoryNet.transports && (!memoryNet.transports.has(Number(peer)) || !memoryNet.transports.get(Number(peer))?.started)) {
          reachable = false;
        }
      }

      // 2. Check TCP socket or optional active ping
      if (options.pingPeers && addr && reachable) {
        try {
          const start = Date.now();
          const [h, p] = addr.split(":");
          await new Promise((resolve, reject) => {
            const socket = net.createConnection({ host: h || "127.0.0.1", port: Number(p), timeout: options.timeoutMs || 500 });
            socket.on("connect", () => {
              latencyMs = Date.now() - start;
              socket.destroy();
              resolve();
            });
            socket.on("timeout", () => {
              socket.destroy();
              reject(new Error("Timeout"));
            });
            socket.on("error", reject);
          });
        } catch (_) {
          reachable = false;
        }
      }

      if (reachable) {
        reachableCount++;
      }

      peerNodes.push({
        id: peer,
        role: leaderId === peer ? "leader" : "follower",
        address: addr,
        status: reachable ? "ONLINE" : "UNREACHABLE",
        latencyMs,
        isLeader: leaderId === peer
      });
    }

    const selfAddress = this._config.port
      ? `${this._config.host || "127.0.0.1"}:${this._config.port}`
      : ((this._config.peerAddresses || {})[this._nodeId] || null);

    const selfNode = {
      id: this._nodeId,
      role: selfRole,
      address: selfAddress,
      status: "ONLINE",
      latencyMs: 0,
      isLeader: this.isLeader()
    };

    const allNodes = [selfNode, ...peerNodes].sort((a, b) => a.id - b.id);
    const hasQuorum = reachableCount >= required && leaderId !== null;

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
      liveness: Boolean(this._raptiye && !this._engine._closed),
      nodeId: this._nodeId,
      leader: {
        id: leaderId,
        address: this.getLeaderAddress(),
        isSelf: this.isLeader()
      },
      term,
      quorum: {
        required,
        reachable: reachableCount,
        total,
        hasQuorum
      },
      nodes: allNodes,
      partitions: {
        total: 128,
        balanced: true
      },
      metrics: this.metrics()
    };
  }

  /**
   * Stops cluster node.
   */
  async stop() {
    if (this._raptiye) {
      if (typeof this._raptiye.shutdown === "function") {
        await this._raptiye.shutdown();
      } else if (typeof this._raptiye.stop === "function") {
        await this._raptiye.stop();
      }
      this._raptiye = null;
    }
    if (this._transport) {
      if (typeof this._transport.close === "function") {
        await this._transport.close();
      } else if (typeof this._transport.stop === "function") {
        await this._transport.stop();
      }
      this._transport = null;
    }
    if (this._storage) {
      if (typeof this._storage.close === "function") {
        await this._storage.close();
      }
      this._storage = null;
    }
  }
}
