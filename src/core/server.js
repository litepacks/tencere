/**
 * Standalone TCP server exposing Tencere core engine over native binary protocol.
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
  OP_EXEC,
  OP_WATCH,
  OP_UNWATCH,
  RESP_OK,
  RESP_ERR,
  RESP_EVENT
} from "./protocol.js";
import { TencereClient } from "../client/index.js";

export class TencereServer {
  /**
   * @param {import('../index.js').Tencere} db
   * @param {object} [options={}]
   * @param {number} [options.port=7337]
   * @param {string} [options.host='0.0.0.0']
   * @param {boolean} [options.forwardWrites=false] - Transparently forward write frames to cluster leader
   */
  constructor(db, options = {}) {
    this.db = db;
    this.port = options.port || 7337;
    this.host = options.host || "0.0.0.0";
    this.forwardWrites = Boolean(options.forwardWrites);
    this.server = null;
    this.connections = new Set();
    this._forwardClients = new Map();
  }

  /**
   * Starts listening for client connections.
   *
   * @returns {Promise<void>}
   */
  async start() {
    return new Promise((resolve, reject) => {
      this.server = net.createServer((socket) => {
        this.connections.add(socket);

        let watchListener = null;
        const cleanupWatch = () => {
          if (watchListener) {
            this.db._engine.events.removeListener("change", watchListener);
            watchListener = null;
          }
        };

        const socketContext = {
          socket,
          setWatch: (pattern) => {
            cleanupWatch();
            watchListener = (event) => {
              if (socket.destroyed) return;
              if (pattern) {
                const matches = pattern.endsWith("*")
                  ? event.key.startsWith(pattern.slice(0, -1))
                  : event.key === pattern || event.key.startsWith(pattern);
                if (!matches) return;
              }
              const evtFrame = new ProtocolFrame({
                requestId: 0n,
                op: RESP_EVENT,
                payload: event
              });
              socket.write(evtFrame.encode());
            };
            this.db._engine.events.on("change", watchListener);
          },
          clearWatch: cleanupWatch
        };

        const parser = new ProtocolParser(async (frame) => {
          try {
            const resp = await this._handleFrame(frame, socketContext);
            socket.write(resp.encode());
          } catch (err) {
            if (this.forwardWrites && err.code === "ERR_NOT_LEADER") {
              const leaderAddr = err.leaderAddress || this.db?.cluster?.getLeaderAddress?.();
              if (leaderAddr && (!frame.payload || !frame.payload._fwd)) {
                try {
                  const forwardedResp = await this._forwardFrame(frame, leaderAddr);
                  socket.write(forwardedResp.encode());
                  return;
                } catch (_) {
                  // Forwarding failed; fall through to returning ERR_NOT_LEADER
                }
              }
            }

            const payload = {
              error: err.message,
              code: err.code || "ERR_SERVER"
            };
            if (err.code === "ERR_NOT_LEADER") {
              payload.nodeId = err.nodeId ?? this.db?.cluster?.nodeId ?? null;
              payload.leaderId = err.leaderId ?? this.db?.cluster?.leaderId ?? null;
              payload.leaderAddress = err.leaderAddress ?? this.db?.cluster?.getLeaderAddress?.() ?? null;
              payload.term = err.term ?? this.db?.cluster?.term ?? 1;
              payload.role = err.role ?? this.db?.cluster?.role ?? "follower";
            }
            const errFrame = new ProtocolFrame({
              requestId: frame.requestId,
              op: RESP_ERR,
              payload
            });
            socket.write(errFrame.encode());
          }
        });

        socket.on("data", (chunk) => {
          try {
            parser.push(new Uint8Array(chunk));
          } catch (err) {
            socket.destroy();
          }
        });

        socket.on("close", () => {
          cleanupWatch();
          this.connections.delete(socket);
        });

        socket.on("error", () => {
          cleanupWatch();
          this.connections.delete(socket);
        });
      });

      this.server.on("error", reject);
      this.server.listen(this.port, this.host, () => {
        resolve();
      });
    });
  }

  async _handleFrame(frame, socketContext = null) {
    const { op, payload, requestId } = frame;

    switch (op) {
      case OP_WATCH: {
        const pattern = payload?.pattern || "";
        if (socketContext) {
          socketContext.setWatch(pattern);
        }
        return new ProtocolFrame({ requestId, op: RESP_OK, payload: true });
      }

      case OP_UNWATCH: {
        if (socketContext) {
          socketContext.clearWatch();
        }
        return new ProtocolFrame({ requestId, op: RESP_OK, payload: true });
      }

      case OP_PING:
        return new ProtocolFrame({ requestId, op: RESP_OK, payload: "PONG" });

      case OP_GET: {
        const res = await this.db.get(payload.key, payload.options);
        return new ProtocolFrame({ requestId, op: RESP_OK, payload: res });
      }

      case OP_SET: {
        const res = await this.db.set(payload.key, payload.value, payload.options);
        return new ProtocolFrame({ requestId, op: RESP_OK, payload: res });
      }

      case OP_DEL: {
        const res = await this.db.delete(payload.key);
        return new ProtocolFrame({ requestId, op: RESP_OK, payload: res });
      }

      case OP_HAS: {
        const res = await this.db.has(payload.key);
        return new ProtocolFrame({ requestId, op: RESP_OK, payload: res });
      }

      case OP_INCR: {
        const res = await this.db.increment(payload.key, payload.delta);
        return new ProtocolFrame({ requestId, op: RESP_OK, payload: res });
      }

      case OP_PATCH: {
        const res = await this.db.patch(payload.key, payload.patch);
        return new ProtocolFrame({ requestId, op: RESP_OK, payload: res });
      }

      case OP_STATS: {
        const res = this.db.stats();
        return new ProtocolFrame({ requestId, op: RESP_OK, payload: res });
      }

      case OP_KEYS: {
        const prefix = payload?.prefix || "";
        const list = this.db.keys(prefix);
        return new ProtocolFrame({ requestId, op: RESP_OK, payload: list });
      }

      case OP_CLEAR: {
        await this.db._engine.storage.clear();
        return new ProtocolFrame({ requestId, op: RESP_OK, payload: true });
      }

      case OP_EXEC: {
        const { target, name, method, args = [] } = payload;
        let result;
        if (target === "map") {
          const col = this.db.map(name);
          result = await col[method](...args);
        } else if (target === "set") {
          const col = this.db.setCollection(name);
          result = await col[method](...args);
        } else if (target === "sorted") {
          const col = this.db.sorted(name);
          if (method === "between" || method === "above" || method === "below") {
            result = await col[method](args[0], args[1]).entries();
          } else {
            result = await col[method](...args);
          }
        } else if (target === "counter") {
          const c = this.db.counter(name);
          result = await c[method](...args);
        } else if (target === "queue") {
          const q = this.db.queue(name);
          result = await q[method](...args);
        } else if (target === "stream") {
          const s = this.db.stream(name);
          result = await s[method](...args);
        } else if (target === "vector") {
          const v = this.db.vector(name);
          result = await v[method](...args);
        } else if (target === "rateLimit") {
          result = await this.db.rateLimit(name, args[0]);
        } else if (target === "lock") {
          const lock = await this.db.tryLock(name, args[0]);
          result = lock ? { acquired: true, token: lock.token } : { acquired: false };
        } else if (target === "unlock") {
          await this.db.delete(`__lock:${name}`);
          result = true;
        } else if (target === "ttl") {
          result = this.db.ttl(name);
        } else if (target === "getMany") {
          result = await this.db.getMany(args[0]);
        } else if (target === "setMany") {
          result = await this.db.setMany(args[0]);
        } else if (target === "checkpoint") {
          await this.db.checkpoint();
          result = true;
        }
        return new ProtocolFrame({ requestId, op: RESP_OK, payload: result });
      }

      default:
        return new ProtocolFrame({
          requestId,
          op: RESP_ERR,
          payload: { error: `Unknown opcode: 0x${op.toString(16)}` }
        });
    }
  }

  /**
   * Forwards a frame to the cluster leader node.
   *
   * @param {ProtocolFrame} frame
   * @param {string} leaderAddress
   * @returns {Promise<ProtocolFrame>}
   */
  async _forwardFrame(frame, leaderAddress) {
    let client = this._forwardClients.get(leaderAddress);
    if (!client || !client.connected) {
      client = await TencereClient.connect(leaderAddress, { autoRedirect: false });
      this._forwardClients.set(leaderAddress, client);

      client.socket?.on("close", () => {
        if (this._forwardClients.get(leaderAddress) === client) {
          this._forwardClients.delete(leaderAddress);
        }
      });
      client.socket?.on("error", () => {
        if (this._forwardClients.get(leaderAddress) === client) {
          this._forwardClients.delete(leaderAddress);
        }
      });
    }

    const fwdPayload = (frame.payload && typeof frame.payload === "object")
      ? { ...frame.payload, _fwd: true }
      : frame.payload;

    const result = await client._send(frame.op, fwdPayload);
    return new ProtocolFrame({
      requestId: frame.requestId,
      op: RESP_OK,
      payload: result
    });
  }

  /**
   * Stops the server and closes all active client sockets.
   *
   * @returns {Promise<void>}
   */
  async stop() {
    for (const socket of this.connections) {
      socket.destroy();
    }
    this.connections.clear();

    for (const client of this._forwardClients.values()) {
      await client.close().catch(() => {});
    }
    this._forwardClients.clear();

    if (this.server) {
      await new Promise((resolve) => this.server.close(resolve));
      this.server = null;
    }
  }

  async close() {
    return this.stop();
  }
}

