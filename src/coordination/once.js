/**
 * once(): distributed single-execution guarantee with defined retention/reset semantics.
 */

import { parseDuration } from "../core/expiry-wheel.js";

export class OnceCoordinator {
  /**
   * @param {import('../core/engine.js').TencereEngine} engine
   * @param {import('./lock.js').LockManager} lockManager
   */
  constructor(engine, lockManager) {
    this._engine = engine;
    this._locks = lockManager;
    this._prefix = "__once:";
  }

  _oKey(key) {
    return `${this._prefix}${key}`;
  }

  /**
   * Executes callback once for a given key within retention TTL.
   *
   * @param {string} key
   * @param {function(): Promise<any>} fn
   * @param {object} [options={}]
   * @param {string|number} [options.ttl='24h']
   * @returns {Promise<any>}
   */
  async execute(key, fn, options = {}) {
    const oKey = this._oKey(key);
    const ttl = options.ttl || "24h";

    // 1. Check if already executed
    const existing = await this._engine.get(oKey);
    if (existing) {
      return existing.result;
    }

    // 2. Try to claim execution lease
    const lockKey = `claim:once:${key}`;
    const lock = await this._locks.tryAcquire(lockKey, { ttl: "1m" });
    if (!lock) {
      // Another worker claimed it; wait briefly and return result if available
      for (let i = 0; i < 20; i++) {
        await new Promise((r) => setTimeout(r, 50));
        const check = await this._engine.get(oKey);
        if (check) return check.result;
      }
      return undefined;
    }

    try {
      // Double check after acquiring lock
      const check = await this._engine.get(oKey);
      if (check) return check.result;

      const result = await fn();
      await this._engine.set(
        oKey,
        {
          completedAt: Date.now(),
          result
        },
        { ttl }
      );
      return result;
    } finally {
      await lock.release();
    }
  }
}
