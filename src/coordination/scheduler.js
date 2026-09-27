/**
 * Scheduler for periodic (.every) and one-time (.at) task execution with lease/fencing semantics.
 */

import { parseDuration } from "../core/expiry-wheel.js";

const MAX_TIMEOUT_MS = 2147483647; // 2^31 - 1 (~24.85 days) Node.js setTimeout limit

/**
 * Segmented setTimeout that safely handles delays > 2^31 - 1 ms
 * without triggering TimeoutOverflowWarning or premature 1ms execution.
 *
 * @param {Function} callback
 * @param {number} delayMs
 * @returns {{ clear: () => void }}
 */
export function safeSetTimeout(callback, delayMs) {
  let timer = null;
  let cancelled = false;

  function step(remaining) {
    if (cancelled) return;
    if (remaining <= MAX_TIMEOUT_MS) {
      timer = setTimeout(() => {
        if (!cancelled) callback();
      }, Math.max(0, remaining));
    } else {
      timer = setTimeout(() => {
        if (!cancelled) step(remaining - MAX_TIMEOUT_MS);
      }, MAX_TIMEOUT_MS);
    }
    if (timer && typeof timer.unref === "function") {
      timer.unref();
    }
  }

  step(delayMs);

  return {
    clear() {
      cancelled = true;
      if (timer) clearTimeout(timer);
    }
  };
}

export class ScheduledTask {
  constructor(stopFn) {
    this._stopFn = stopFn;
    this.active = true;
  }

  stop() {
    this.active = false;
    this._stopFn();
  }
}

export class ScheduleBuilder {
  /**
   * @param {import('../core/engine.js').TencereEngine} engine
   * @param {import('./lock.js').LockManager} lockManager
   * @param {string} name
   */
  constructor(engine, lockManager, name) {
    this._engine = engine;
    this._locks = lockManager;
    this._name = name;
    this._everyMs = 0;
    this._atTimestamp = 0;
  }

  every(interval) {
    this._everyMs = parseDuration(interval);
    return this;
  }

  at(timestampOrDate) {
    if (timestampOrDate instanceof Date) {
      this._atTimestamp = timestampOrDate.getTime();
    } else if (typeof timestampOrDate === "string") {
      this._atTimestamp = new Date(timestampOrDate).getTime();
    } else {
      this._atTimestamp = Number(timestampOrDate);
    }
    return this;
  }

  /**
   * Starts the scheduled task.
   *
   * @param {function(): Promise<void>} fn
   * @returns {ScheduledTask}
   */
  run(fn) {
    if (this._atTimestamp > 0) {
      return this._runAt(fn);
    }
    if (this._everyMs > 0) {
      return this._runEvery(fn);
    }
    throw new Error("Schedule requires either .every(...) or .at(...)");
  }

  _runAt(fn) {
    const delay = Math.max(0, this._atTimestamp - Date.now());
    const handle = safeSetTimeout(async () => {
      // Claim lease for this specific execution
      const lockKey = `sched:${this._name}:${this._atTimestamp}`;
      const lock = await this._locks.tryAcquire(lockKey, { ttl: "10m" });
      if (lock) {
        try {
          await fn();
        } finally {
          // Keep lock to prevent duplicate runs
        }
      }
    }, delay);

    return new ScheduledTask(() => handle.clear());
  }

  _runEvery(fn) {
    let active = true;
    let handle = null;

    const tick = async () => {
      if (!active || this._engine.isClosed) return;
      const bucket = Math.floor(Date.now() / this._everyMs) * this._everyMs;
      const lockKey = `sched:${this._name}:${bucket}`;
      const lock = await this._locks.tryAcquire(lockKey, { ttl: Math.min(this._everyMs * 2, 86400000) });

      if (lock) {
        try {
          await fn();
        } catch (err) {
          console.error(`[Scheduler] Task '${this._name}' error:`, err);
        }
      }

      if (active && !this._engine.isClosed) {
        handle = safeSetTimeout(tick, this._everyMs);
      }
    };

    // First tick after interval
    handle = safeSetTimeout(tick, this._everyMs);

    return new ScheduledTask(() => {
      active = false;
      if (handle) handle.clear();
    });
  }
}
