/**
 * Lease-based distributed semaphore.
 * Prunes expired leases so crashed or dead clients never leak permits.
 */

import crypto from "node:crypto";
import { parseDuration } from "../core/expiry-wheel.js";
import { TimeoutError } from "../errors.js";

export class Semaphore {
  /**
   * @param {import('../core/engine.js').TencereEngine} engine
   * @param {string} key
   * @param {object} [options={}]
   * @param {number} [options.permits=1]
   * @param {number|string} [options.leaseTtl='30s']
   * @param {number|string} [options.timeout='30s']
   */
  constructor(engine, key, options = {}) {
    this._engine = engine;
    this._key = key;
    this._permits = options.permits || 1;
    this._leaseTtlMs = options.leaseTtl ? parseDuration(options.leaseTtl) : 30000;
    this._timeoutMs = options.timeout ? parseDuration(options.timeout) : 30000;
    this._storageKey = `__sem:${key}`;
    this._notifyEvent = `sem:${key}:released`;
  }

  /**
   * Attempts to acquire a permit.
   *
   * @returns {Promise<string|null>} permit ownerId or null
   */
  async tryAcquire() {
    const ownerId = crypto.randomUUID();
    const now = Date.now();

    return this._engine.update(
      this._storageKey,
      (current) => {
        let state = current;
        if (!state || typeof state !== "object") {
          state = { permits: this._permits, holders: [] };
        }

        // Clean expired leases
        state.holders = (state.holders || []).filter((h) => h.expiresAt > now);

        if (state.holders.length < this._permits) {
          state.holders.push({
            ownerId,
            expiresAt: now + this._leaseTtlMs
          });
          return state;
        }

        return state; // No permit available
      },
      { maxRetries: 5 }
    ).then((state) => {
      const hasPermit = state.holders.some((h) => h.ownerId === ownerId);
      return hasPermit ? ownerId : null;
    });
  }

  /**
   * Releases an acquired permit.
   *
   * @param {string} ownerId
   * @returns {Promise<void>}
   */
  async release(ownerId) {
    await this._engine.update(
      this._storageKey,
      (current) => {
        if (!current || !current.holders) return current;
        current.holders = current.holders.filter((h) => h.ownerId !== ownerId);
        return current;
      },
      { maxRetries: 5 }
    );
    this._engine.events.emit(this._notifyEvent);
  }

  /**
   * Runs callback inside the semaphore permit.
   *
   * @param {function(): Promise<any>} fn
   * @returns {Promise<any>}
   */
  async run(fn) {
    const start = Date.now();
    let ownerId = null;

    while (Date.now() - start < this._timeoutMs) {
      ownerId = await this.tryAcquire();
      if (ownerId) break;

      const elapsed = Date.now() - start;
      const remaining = this._timeoutMs - elapsed;
      if (remaining <= 0) break;

      // Event-driven reactive waiting instead of continuous busy-polling.
      // Wait for release notification or fallback timeout bounded by lease TTL
      const waitTime = Math.min(remaining, Math.max(25, Math.min(this._leaseTtlMs, 250)));

      await new Promise((resolve) => {
        let timer = null;
        const onWakeup = () => {
          if (timer) clearTimeout(timer);
          this._engine.events.removeListener(this._notifyEvent, onWakeup);
          resolve();
        };

        this._engine.events.once(this._notifyEvent, onWakeup);

        timer = setTimeout(() => {
          this._engine.events.removeListener(this._notifyEvent, onWakeup);
          resolve();
        }, waitTime);

        if (timer && typeof timer.unref === "function") {
          timer.unref();
        }
      });
    }

    if (!ownerId) {
      throw new TimeoutError(`Timed out after ${this._timeoutMs}ms waiting for semaphore permit '${this._key}'`);
    }

    try {
      return await fn();
    } finally {
      await this.release(ownerId);
    }
  }
}
