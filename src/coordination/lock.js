/**
 * Distributed lease-based locks with auto-renewal and monotonically increasing fencing tokens.
 */

import crypto from "node:crypto";
import { parseDuration } from "../core/expiry-wheel.js";
import { LockAcquireError, LockStaleOwnerError, TimeoutError, VersionMismatchError } from "../errors.js";

export class LockHandle {
  /**
   * @param {LockManager} manager
   * @param {string} key
   * @param {string} ownerId
   * @param {number} token - Monotonic fencing token
   * @param {number} ttlMs
   * @param {NodeJS.Timeout|null} [renewTimer=null]
   */
  constructor(manager, key, ownerId, token, ttlMs, renewTimer = null) {
    this._manager = manager;
    this.key = key;
    this.ownerId = ownerId;
    this.token = token;
    this.ttlMs = ttlMs;
    this._renewTimer = renewTimer;
    this._released = false;
  }

  /**
   * Releases the lock, safely validating ownership and fencing token.
   *
   * @returns {Promise<void>}
   */
  async release() {
    if (this._released) return;
    this._released = true;
    if (this._renewTimer) {
      clearInterval(this._renewTimer);
      this._renewTimer = null;
    }
    await this._manager._releaseLock(this.key, this.ownerId, this.token);
  }
}

export class LockManager {
  /**
   * @param {import('../core/engine.js').TencereEngine} engine
   */
  constructor(engine) {
    this._engine = engine;
    this._prefix = "__lock:";
    this._nodePrefix = `${process.pid.toString(36)}-${crypto.randomBytes(6).toString("hex")}`;
    this._ownerSeq = 0;
  }

  _lKey(key) {
    return `${this._prefix}${key}`;
  }

  /**
   * Attempts to acquire a lock without blocking.
   *
   * @param {string} key
   * @param {object} [options={}]
   * @param {number|string} [options.ttl='30s']
   * @param {boolean} [options.renew=false]
   * @returns {Promise<LockHandle|null>}
   */
  async tryAcquire(key, options = {}) {
    const lKey = this._lKey(key);
    const ttlMs = options.ttl ? parseDuration(options.ttl) : 30000;
    const now = Date.now();

    const existing = await this._engine.get(lKey, { withVersion: true });
    if (existing && existing.value && existing.value.expiresAt > now) {
      // Already held by another owner
      return null;
    }
    const currentVersion = existing ? existing.version : 0;

    const token = this._engine.nextFencingToken();
    const ownerId = `${this._nodePrefix}-${++this._ownerSeq}`;
    const expiresAt = now + ttlMs;

    const record = {
      ownerId,
      token,
      expiresAt,
      ttlMs
    };

    try {
      await this._engine.set(lKey, record, { ttl: ttlMs, ifVersion: currentVersion });
      if (this._engine.historyManager) {
        this._engine.historyManager.recordCoordination({
          type: "lock",
          key,
          op: 0x09, // OP_LOCK_ACQUIRE
          timestamp: now,
          details: { ownerId, token, ttlMs }
        });
      }
    } catch (err) {
      if (err.name === "VersionMismatchError") {
        return null;
      }
      throw err;
    }

    let renewTimer = null;
    if (options.renew) {
      const interval = Math.max(100, Math.floor(ttlMs / 2));
      renewTimer = setInterval(async () => {
        try {
          const cur = await this._engine.get(lKey);
          if (cur && cur.ownerId === ownerId && cur.token === token) {
            cur.expiresAt = Date.now() + ttlMs;
            await this._engine.set(lKey, cur, { ttl: ttlMs });
            if (this._engine.historyManager) {
              this._engine.historyManager.recordCoordination({
                type: "lock",
                key,
                op: 0x0b, // OP_LOCK_RENEW
                timestamp: Date.now(),
                details: { ownerId, token }
              });
            }
          } else {
            clearInterval(renewTimer);
          }
        } catch (_) {
          clearInterval(renewTimer);
        }
      }, interval);

      if (renewTimer && typeof renewTimer.unref === "function") {
        renewTimer.unref();
      }
    }

    return new LockHandle(this, key, ownerId, token, ttlMs, renewTimer);
  }

  /**
   * Releases lock verifying ownership.
   */
  async _releaseLock(key, ownerId, token) {
    const lKey = this._lKey(key);
    const existing = await this._engine.get(lKey, { withVersion: true });
    if (!existing || !existing.value) {
      return; // Already expired or cleaned up
    }
    const record = existing.value;
    if (record.ownerId !== ownerId || record.token !== token) {
      // Stale owner attempted to release a newer owner's lock!
      throw new LockStaleOwnerError(key, token);
    }
    try {
      await this._engine.delete(lKey, { ifVersion: existing.version });
    } catch (err) {
      if (err instanceof VersionMismatchError) {
        throw new LockStaleOwnerError(key, token);
      }
      throw err;
    }
    if (this._engine.historyManager) {
      this._engine.historyManager.recordCoordination({
        type: "lock",
        key,
        op: 0x0a, // OP_LOCK_RELEASE
        timestamp: Date.now(),
        details: { ownerId, token }
      });
    }
  }

  /**
   * Acquires a lock, executing the callback with lease guarantees and fencing token.
   *
   * @param {string} key
   * @param {object} [options={}]
   * @param {function({ token: number }): Promise<any>} fn
   * @returns {Promise<any>}
   */
  async withLock(key, options, fn) {
    let opts = options;
    let callback = fn;
    if (typeof options === "function") {
      callback = options;
      opts = {};
    }

    const timeoutMs = opts.timeout ? parseDuration(opts.timeout) : 10000;
    const start = Date.now();
    let handle = null;

    while (Date.now() - start < timeoutMs) {
      handle = await this.tryAcquire(key, opts);
      if (handle) break;
      // Exponential jitter sleep
      const wait = 20 + Math.floor(Math.random() * 50);
      await new Promise((r) => setTimeout(r, wait));
    }

    if (!handle) {
      throw new LockAcquireError(key, `Timed out after ${timeoutMs}ms waiting for lock`);
    }

    try {
      return await callback({ token: handle.token });
    } finally {
      try {
        await handle.release();
      } catch (err) {
        if (!(err instanceof LockStaleOwnerError)) {
          throw err;
        }
      }
    }
  }
}
