/**
 * First-class cache primitive with stampede protection and stale-while-revalidate.
 */

import { parseDuration } from "../core/expiry-wheel.js";

export class CacheManager {
  /**
   * @param {import('../core/engine.js').TencereEngine} engine
   * @param {import('./lock.js').LockManager} lockManager
   */
  constructor(engine, lockManager) {
    this._engine = engine;
    this._locks = lockManager;
    this._prefix = "__cache:";
    this._inFlight = new Map(); // key -> Promise
  }

  _cKey(key) {
    return `${this._prefix}${key}`;
  }

  /**
   * Retrieves from cache, or invokes loader with stampede protection and SWR.
   *
   * @param {string} key
   * @param {object} options
   * @param {string|number} options.ttl
   * @param {string|number} [options.stale] Stale-while-revalidate window
   * @param {function(): Promise<any>} loader
   * @returns {Promise<any>}
   */
  async getOrLoad(key, options, loader) {
    if (typeof options === "function") {
      const tmp = loader;
      loader = options;
      options = tmp || {};
    }
    const cKey = this._cKey(key);
    const ttlMs = parseDuration(options.ttl || "5m");
    const staleMs = options.stale ? parseDuration(options.stale) : 0;
    const now = Date.now();

    const cached = await this._engine.get(cKey);
    if (cached) {
      const age = now - cached.cachedAt;
      if (age < cached.ttlMs) {
        // Fresh hit
        return cached.value;
      }

      if (staleMs > 0 && age < cached.ttlMs + staleMs) {
        // Stale-while-revalidate: return stale value immediately and refresh in background
        this._revalidateInBackground(key, cKey, ttlMs, staleMs, loader);
        return cached.value;
      }
    }

    // Cache miss: stampede protection
    if (this._inFlight.has(key)) {
      return this._inFlight.get(key);
    }

    const loadPromise = (async () => {
      // Acquire stampede lease lock
      const lockKey = `claim:cache:${key}`;
      let lock = null;
      try {
        lock = await this._locks.tryAcquire(lockKey, { ttl: "30s" });
        if (!lock) {
          // Another loader is already loading; wait briefly and check cache
          for (let i = 0; i < 40; i++) {
            await new Promise((r) => setTimeout(r, 25));
            const recheck = await this._engine.get(cKey);
            if (recheck && Date.now() - recheck.cachedAt < recheck.ttlMs) {
              return recheck.value;
            }
          }
          // Fallback: run loader
          return await loader();
        }

        // Recheck cache in case previous holder just loaded
        const recheck = await this._engine.get(cKey);
        if (recheck && Date.now() - recheck.cachedAt < recheck.ttlMs) {
          return recheck.value;
        }

        const value = await loader();
        const record = {
          value,
          cachedAt: Date.now(),
          ttlMs,
          staleMs
        };
        // Set with TTL = ttlMs + staleMs so it remains available for SWR
        const totalRetention = ttlMs + staleMs;
        await this._engine.set(cKey, record, { ttl: totalRetention });
        return value;
      } finally {
        this._inFlight.delete(key);
        if (lock) {
          try {
            await lock.release();
          } catch (_) {}
        }
      }
    })();

    this._inFlight.set(key, loadPromise);
    return loadPromise;
  }

  _revalidateInBackground(key, cKey, ttlMs, staleMs, loader) {
    if (this._inFlight.has(key)) return;

    const bgPromise = (async () => {
      const lock = await this._locks.tryAcquire(`claim:cache:${key}`, { ttl: "30s" });
      if (!lock) return;

      try {
        const value = await loader();
        const record = {
          value,
          cachedAt: Date.now(),
          ttlMs,
          staleMs
        };
        await this._engine.set(cKey, record, { ttl: ttlMs + staleMs });
      } catch (_) {
        // Silent failure in background SWR
      } finally {
        this._inFlight.delete(key);
        try {
          await lock.release();
        } catch (_) {}
      }
    })();

    this._inFlight.set(key, bgPromise);
  }
}
