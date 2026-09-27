/**
 * Idempotency primitive: deduplicates concurrent requests, handles worker crashes,
 * and replays completed results.
 */

import crypto from "node:crypto";
import { parseDuration } from "../core/expiry-wheel.js";
import { TimeoutError } from "../errors.js";

export class IdempotencyCoordinator {
  /**
   * @param {import('../core/engine.js').TencereEngine} engine
   * @param {import('./lock.js').LockManager} lockManager
   */
  constructor(engine, lockManager) {
    this._engine = engine;
    this._locks = lockManager;
    this._prefix = "__idemp:";
    this._inFlight = new Map(); // key -> Promise
  }

  _iKey(key) {
    return `${this._prefix}${key}`;
  }

  /**
   * Executes operation idempotently with concurrent deduplication and result replay.
   *
   * @param {string} key
   * @param {function(): Promise<any>} fn
   * @param {object} [options={}]
   * @param {string|number} [options.ttl='24h']
   * @param {string|number} [options.leaseTtl='30s']
   * @param {string|number} [options.waitTimeout='60s']
   * @returns {Promise<any>}
   */
  async execute(key, fn, options = {}) {
    const iKey = this._iKey(key);
    const ttl = options.ttl || "24h";
    const leaseTtlMs = options.leaseTtl ? parseDuration(options.leaseTtl) : 30000;
    const waitTimeoutMs = options.waitTimeout ? parseDuration(options.waitTimeout) : 60000;

    // 1. Process-local in-flight deduplication
    if (this._inFlight.has(key)) {
      return this._inFlight.get(key);
    }

    const runPromise = (async () => {
      const startTime = Date.now();

      while (Date.now() - startTime < waitTimeoutMs) {
        // 2. Check if already completed in database
        const existing = await this._engine.get(iKey);
        if (existing) {
          if (existing.state === "completed") {
            return existing.result;
          }
          if (existing.state === "failed") {
            throw new Error(`Idempotent operation '${key}' previously failed: ${existing.error}`);
          }
          // If pending and lease has not expired, wait for completion
          if (existing.state === "pending" && existing.expiresAt > Date.now()) {
            const maxWait = Math.min(
              waitTimeoutMs - (Date.now() - startTime),
              existing.expiresAt - Date.now() + 50
            );
            const awaitRes = await this._awaitPending(key, maxWait);
            if (awaitRes.done) {
              return awaitRes.result;
            }
            // If lease expired or worker crashed, loop back to attempt taking over execution
            continue;
          }
        }

        // 3. Claim execution with lease
        const ownerId = crypto.randomUUID();
        const pendingRecord = {
          state: "pending",
          ownerId,
          expiresAt: Date.now() + leaseTtlMs
        };

        const lock = await this._locks.tryAcquire(`claim:idemp:${key}`, { ttl: leaseTtlMs });
        if (!lock) {
          const awaitRes = await this._awaitPending(key, Math.min(250, waitTimeoutMs - (Date.now() - startTime)));
          if (awaitRes.done) {
            return awaitRes.result;
          }
          continue;
        }

        try {
          await this._engine.set(iKey, pendingRecord, { ttl: leaseTtlMs });
          const result = await fn();

          const completedRecord = {
            state: "completed",
            result,
            completedAt: Date.now()
          };
          await this._engine.set(iKey, completedRecord, { ttl });
          return result;
        } catch (err) {
          const failedRecord = {
            state: "failed",
            error: err.message || String(err),
            failedAt: Date.now()
          };
          await this._engine.set(iKey, failedRecord, { ttl: "1h" });
          throw err;
        } finally {
          if (lock) {
            try {
              await lock.release();
            } catch (_) {}
          }
        }
      }

      throw new TimeoutError(`Timed out waiting for idempotent operation '${key}'`);
    })();

    this._inFlight.set(key, runPromise);
    try {
      return await runPromise;
    } finally {
      this._inFlight.delete(key);
    }
  }

  async _awaitPending(key, timeoutMs) {
    const iKey = this._iKey(key);
    const start = Date.now();

    while (Date.now() - start < timeoutMs) {
      await new Promise((r) => setTimeout(r, 25));
      const entry = await this._engine.get(iKey);
      if (!entry) {
        return { done: false };
      }

      if (entry.state === "completed") {
        return { done: true, result: entry.result };
      }
      if (entry.state === "failed") {
        throw new Error(`Idempotent operation '${key}' failed: ${entry.error}`);
      }
      // If lease expired, return done: false to allow caller loop to claim and take over
      if (entry.state === "pending" && entry.expiresAt <= Date.now()) {
        return { done: false };
      }
    }

    return { done: false };
  }
}
