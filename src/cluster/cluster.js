/**
 * ClusterManager: Integrates Raptiye consensus and replication core.
 */

import { Raptiye, MemoryLog, FileLog, TCPTransport, MemoryTransport, MemoryNetwork } from "raptiye";
import path from "node:path";
import { Operation } from "../core/operations.js";

export class ClusterManager {
  /**
   * @param {import('../core/engine.js').TencereEngine} engine
   * @param {object} clusterConfig
   */
  constructor(engine, clusterConfig) {
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
      this._transport = new TCPTransport({
        id: this._nodeId,
        port: this._config.port,
        host: this._config.host || "127.0.0.1",
        peerAddresses: this._config.peerAddresses || {}
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
      election: this._config.election || { minTimeout: 30, maxTimeout: 80 },
      heartbeatInterval: this._config.heartbeatInterval || 15,
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
        return this._nodeId;
      }
      const lId = this._raptiye?.leader?.();
      if (lId) {
        return Number(lId);
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
    const roleVal = typeof this._raptiye.role === "function"
      ? this._raptiye.role()
      : (typeof this._raptiye.role === "string" ? this._raptiye.role : this._raptiye.role?.name || "UNKNOWN");
    const role = typeof roleVal === "object" && roleVal !== null ? (roleVal.name || String(roleVal)) : String(roleVal);
    return role.toLowerCase().includes("leader");
  }

  get currentTerm() {
    if (!this._raptiye) return 1;
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
      throw new Error(`[Cluster Fencing] Node ${this._nodeId} is not leader (role: ${st.role}, term: ${st.term})`);
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
      term: this.currentTerm,
      isLeader: this.isLeader(),
      role,
      peers: this._peers
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
