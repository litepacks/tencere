/**
 * Tencere remote client connecting to a standalone Tencere server over TCP.
 */

import net from "node:net";
import {
  ProtocolParser,
  ProtocolFrame,
  OP_PING,
  OP_GET,
  OP_SET,
  OP_DEL,
  OP_HAS,
  OP_INCR,
  OP_PATCH,
  OP_STATS,
  OP_KEYS,
  OP_CLEAR,
  OP_WATCH,
  OP_UNWATCH,
  RESP_OK,
  RESP_ERR,
  RESP_EVENT
} from "../core/protocol.js";
import { NotLeaderError } from "../errors.js";
import { TencereClusterClient, ClusterClientPipeline } from "./cluster-client.js";

export class TencereClient {
  /**
   * @param {string} address - e.g. '127.0.0.1:7337' or 'localhost:7337'
   * @param {object} [options={}]
   * @param {boolean} [options.autoRedirect=false] - Auto-redirect write operations if follower returns ERR_NOT_LEADER
   */
  constructor(address, options = {}) {
    const [host, port] = address.split(":");
    this.host = host || "127.0.0.1";
    this.port = Number(port) || 7337;
    this.options = options;
    this.socket = null;
    this.parser = null;
    this.nextReqId = 1;
    this.pending = new Map(); // reqId -> { resolve, reject }
    this.connected = false;
    this._writeQueue = [];
    this._scheduledFlush = false;
  }

  /**
   * Connects to a standalone Tencere server.
   *
   * @param {string} address
   * @param {object} [options={}]
   * @returns {Promise<TencereClient>}
   */
  static async connect(address, options = {}) {
    const client = new TencereClient(address, options);
    await client._connect();
    return client;
  }

  /**
   * Connects to a Tencere cluster via seed addresses with automatic discovery,
   * write routing to leader, and failover support.
   *
   * @param {string|string[]} seedAddresses
   * @param {object} [options={}]
   * @returns {Promise<TencereClusterClient>}
   */
  static async cluster(seedAddresses, options = {}) {
    const clusterClient = new TencereClusterClient(seedAddresses, options);
    await clusterClient.connect();
    return clusterClient;
  }

  async _connect() {
    return new Promise((resolve, reject) => {
      this.socket = net.createConnection({ host: this.host, port: this.port }, () => {
        this.connected = true;
        resolve();
      });

      this.parser = new ProtocolParser((frame) => {
        if (frame.op === RESP_EVENT) {
          if (this._watchHandler) {
            this._watchHandler(frame.payload);
          }
          return;
        }
        const handler = this.pending.get(frame.requestId);
        if (handler) {
          this.pending.delete(frame.requestId);
          if (frame.op === RESP_OK) {
            handler.resolve(frame.payload);
          } else {
            const p = frame.payload || {};
            const msg = p.error || "Remote server error";
            let err;
            if (p.code === "ERR_NOT_LEADER") {
              err = new NotLeaderError(
                p.nodeId,
                p.leaderId,
                p.role,
                p.term,
                p.leaderAddress
              );
            } else {
              err = new Error(msg);
              if (p.code) err.code = p.code;
            }
            handler.reject(err);
          }
        }
      });

      this.socket.on("data", (chunk) => {
        try {
          this.parser.push(new Uint8Array(chunk));
        } catch (err) {
          this._rejectAll(err);
        }
      });

      this.socket.on("error", (err) => {
        if (!this.connected) reject(err);
        this._rejectAll(err);
      });

      this.socket.on("close", () => {
        this.connected = false;
        this._rejectAll(new Error("Connection closed"));
      });
    });
  }

  _rejectAll(err) {
    for (const { reject } of this.pending.values()) {
      reject(err);
    }
    this.pending.clear();
  }

  _flushWrites() {
    if (this._writeQueue.length === 0 || !this.connected) return;
    const bufs = this._writeQueue;
    this._writeQueue = [];
    this._scheduledFlush = false;
    if (bufs.length === 1) {
      this.socket.write(bufs[0]);
    } else {
      this.socket.write(Buffer.concat(bufs));
    }
  }

  async _redirectTo(newAddress) {
    const [host, port] = newAddress.split(":");
    if (this.socket) {
      this.socket.removeAllListeners();
      this.socket.destroy();
      this.socket = null;
    }
    this.connected = false;
    this.host = host || "127.0.0.1";
    this.port = Number(port) || 7337;
    await this._connect();
  }

  async _send(op, payload, retried = false) {
    if (!this.connected) {
      throw new Error("TencereClient is not connected");
    }
    const requestId = this.nextReqId++;
    const frame = new ProtocolFrame({ requestId, op, payload });
    const encoded = frame.encode();

    try {
      return await new Promise((resolve, reject) => {
        this.pending.set(requestId, { resolve, reject });
        this._writeQueue.push(encoded);
        if (!this._scheduledFlush) {
          this._scheduledFlush = true;
          queueMicrotask(() => this._flushWrites());
        }
      });
    } catch (err) {
      if (!retried && this.options.autoRedirect && err.code === "ERR_NOT_LEADER" && err.leaderAddress) {
        await this._redirectTo(err.leaderAddress);
        return this._send(op, payload, true);
      }
      throw err;
    }
  }

  /**
   * Creates a pipelined batch execution context.
   *
   * @returns {ClientPipeline}
   */
  pipeline() {
    return new ClientPipeline(this);
  }

  async ping() {
    return this._send(OP_PING, null);
  }

  /**
   * Checks health and latency of the connected node.
   *
   * @returns {Promise<object>}
   */
  async health() {
    try {
      const start = Date.now();
      await this.ping();
      const latencyMs = Date.now() - start;
      const stats = await this.stats();
      const cl = stats?.cluster;
      return {
        status: "HEALTHY",
        readiness: true,
        liveness: true,
        latencyMs,
        address: `${this.host}:${this.port}`,
        quorum: cl?.quorum || null,
        cluster: cl || null,
        stats
      };
    } catch (err) {
      return {
        status: "OFFLINE",
        readiness: false,
        liveness: false,
        latencyMs: null,
        address: `${this.host}:${this.port}`,
        error: err.message
      };
    }
  }

  async get(key, options) {
    return this._send(OP_GET, { key, options });
  }

  async set(key, value, options) {
    return this._send(OP_SET, { key, value, options });
  }

  async delete(key) {
    return this._send(OP_DEL, { key });
  }

  async has(key) {
    return this._send(OP_HAS, { key });
  }

  async increment(key, delta = 1) {
    return this._send(OP_INCR, { key, delta });
  }

  async patch(key, patch) {
    return this._send(OP_PATCH, { key, patch });
  }

  async stats() {
    return this._send(OP_STATS, null);
  }

  async keys(prefix = "") {
    return this._send(OP_KEYS, { prefix });
  }

  async clear() {
    return this._send(OP_CLEAR, null);
  }

  async exec(target, name, method, args = []) {
    return this._send(OP_EXEC, { target, name, method, args });
  }

  async ttl(key) {
    return this._send(OP_EXEC, { target: "ttl", name: key });
  }

  async getMany(keys) {
    return this._send(OP_EXEC, { target: "getMany", args: [keys] });
  }

  async setMany(entries) {
    return this._send(OP_EXEC, { target: "setMany", args: [entries] });
  }

  async unlock(key) {
    return this._send(OP_EXEC, { target: "unlock", name: key });
  }

  async checkpoint() {
    return this._send(OP_EXEC, { target: "checkpoint" });
  }

  async watch(pattern = "", handler) {
    this._watchHandler = handler;
    return this._send(OP_WATCH, { pattern });
  }

  async unwatch() {
    this._watchHandler = null;
    return this._send(OP_UNWATCH, null);
  }

  async close() {
    if (this.socket) {
      this.socket.destroy();
      this.socket = null;
    }
    this.connected = false;
  }
}

export class ClientPipeline {
  /**
   * @param {TencereClient} client
   */
  constructor(client) {
    this._client = client;
    this._operations = [];
  }

  ping() {
    this._operations.push(() => this._client.ping());
    return this;
  }

  get(key, options) {
    this._operations.push(() => this._client.get(key, options));
    return this;
  }

  set(key, value, options) {
    this._operations.push(() => this._client.set(key, value, options));
    return this;
  }

  delete(key) {
    this._operations.push(() => this._client.delete(key));
    return this;
  }

  has(key) {
    this._operations.push(() => this._client.has(key));
    return this;
  }

  increment(key, delta = 1) {
    this._operations.push(() => this._client.increment(key, delta));
    return this;
  }

  patch(key, patch) {
    this._operations.push(() => this._client.patch(key, patch));
    return this;
  }

  ttl(key) {
    this._operations.push(() => this._client.ttl(key));
    return this;
  }

  /**
   * Dispatches all queued pipeline commands coalesced and returns their results.
   *
   * @returns {Promise<any[]>}
   */
  async exec() {
    const promises = this._operations.map((op) => op());
    return Promise.all(promises);
  }
}

export { TencereClusterClient, ClusterClientPipeline };

