/**
 * High-performance atomic rate limiter with sliding window semantics.
 */

import { parseDuration } from "../core/expiry-wheel.js";

export class RateLimiter {
  /**
   * @param {import('../core/engine.js').TencereEngine} engine
   */
  constructor(engine) {
    this._engine = engine;
    this._prefix = "__ratelimit:";
  }

  _rKey(key) {
    return `${this._prefix}${key}`;
  }

  /**
   * Checks and consumes a rate limit permit.
   *
   * @param {string} key
   * @param {object} options
   * @param {number} options.limit - Max requests per window
   * @param {string|number} options.window - Duration window (e.g. '1m', '10s')
   * @param {number} [options.cost=1]
   * @returns {Promise<{ allowed: boolean, remaining: number, resetAt: number, total: number }>}
   */
  async consume(key, options) {
    const rKey = this._rKey(key);
    const limit = options.limit || 100;
    const windowMs = parseDuration(options.window || "1m");
    const cost = options.cost || 1;
    const now = Date.now();

    const state = await this._engine.update(
      rKey,
      (current) => {
        if (!current || now >= current.resetAt) {
          return {
            count: cost,
            resetAt: now + windowMs,
            total: limit
          };
        }

        if (current.count + cost <= limit) {
          current.count += cost;
        } else {
          current.blocked = true;
        }
        return current;
      },
      { maxRetries: 10, ttl: windowMs }
    );

    const allowed = state.count <= limit && !state.blocked;
    const remaining = Math.max(0, limit - state.count);

    return {
      allowed,
      remaining,
      resetAt: state.resetAt,
      total: limit
    };
  }
}
