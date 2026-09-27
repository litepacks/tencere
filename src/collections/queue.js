/**
 * High-performance job queue built from Tencere primitives:
 *  - OrderedIndex for delayed/scheduled jobs and exponential backoff
 *  - Leases / visibility timeouts for crash-safe worker execution
 */

import { OrderedIndex } from "../core/ordered-index.js";
import { parseDuration } from "../core/expiry-wheel.js";

export class QueueCollection {
  /**
   * @param {import('../core/engine.js').TencereEngine} engine
   * @param {string} name
   */
  constructor(engine, name) {
    this._engine = engine;
    this._name = name;
    this._idSeqKey = `__queue_seq:${name}`;
    this._jobPrefix = `__queue_job:${name}:`;
    this._notifyEvent = `queue:${name}:pushed`;

    // Delayed jobs index: score = scheduledRunAt, member = jobId
    this._delayedIndex = new OrderedIndex();

    // In-memory ready list of jobIds
    this._readyQueue = [];

    // Active in-flight jobs: jobId -> { leaseExpiresAt, workerId }
    this._activeJobs = new Map();

    this._workers = [];
    this._hydrated = false;
    this._hydratePromise = null;
  }

  _jKey(id) {
    return `${this._jobPrefix}${id}`;
  }

  /**
   * Automatically hydrates persisted jobs from storage into RAM on restart or instantiation.
   */
  async _ensureHydrated() {
    if (this._hydrated) return;
    if (!this._hydratePromise) {
      this._hydratePromise = this._doHydrate();
    }
    await this._hydratePromise;
    this._hydrated = true;
  }

  async _doHydrate() {
    const keys = this._engine.keys(this._jobPrefix);
    if (!keys || keys.length === 0) {
      return;
    }

    const jobs = [];
    for (const k of keys) {
      const job = await this._engine.get(k);
      if (job && job.id !== undefined) {
        jobs.push(job);
      }
    }

    // Sort jobs by createdAt and ID for deterministic FIFO hydration
    jobs.sort((a, b) => {
      const diff = (a.createdAt || 0) - (b.createdAt || 0);
      if (diff !== 0) return diff;
      return Number(a.id) - Number(b.id);
    });

    const now = Date.now();
    for (const job of jobs) {
      const id = String(job.id);
      if (this._activeJobs.has(id)) continue;
      if (this._readyQueue.includes(id)) continue;
      if (this._delayedIndex.has(id)) continue;

      const state = job.state || job.status || "ready";
      if (state === "ready") {
        this._readyQueue.push(id);
      } else if (state === "delayed") {
        if (job.runAt && job.runAt <= now) {
          job.state = "ready";
          await this._engine.set(this._jKey(id), job);
          this._readyQueue.push(id);
        } else if (job.runAt) {
          this._delayedIndex.insert(job.runAt, id);
        } else {
          job.state = "ready";
          await this._engine.set(this._jKey(id), job);
          this._readyQueue.push(id);
        }
      } else if (state === "active") {
        // Crashed or dead process during execution: recover lease
        if (job.attempts <= (job.retries ?? 3)) {
          job.state = "ready";
          await this._engine.set(this._jKey(id), job);
          this._readyQueue.push(id);
        } else {
          job.state = "failed";
          await this._engine.set(this._jKey(id), job);
        }
      }
    }
  }

  /**
   * Pushes a job onto the queue.
   *
   * @param {any} data
   * @param {object} [options={}]
   * @param {number|string} [options.delay=0]
   * @param {number} [options.retries=3]
   * @param {number|string} [options.timeout='30s']
   * @returns {Promise<object>} created job descriptor
   */
  async push(data, options = {}) {
    await this._ensureHydrated();
    const idNum = await this._engine.increment(this._idSeqKey, 1);
    const id = `${idNum}`;
    const now = Date.now();
    const delayMs = options.delay ? parseDuration(options.delay) : 0;
    const timeoutMs = options.timeout ? parseDuration(options.timeout) : 30000;
    const retries = options.retries !== undefined ? Number(options.retries) : 3;

    const job = {
      id,
      data,
      delayMs,
      timeoutMs,
      retries,
      attempts: 0,
      createdAt: now,
      runAt: now + delayMs,
      state: delayMs > 0 ? "delayed" : "ready",
      error: null
    };

    await this._engine.set(this._jKey(id), job);

    if (delayMs > 0) {
      this._delayedIndex.insert(job.runAt, id);
    } else {
      this._readyQueue.push(id);
      this._engine.events.emit(this._notifyEvent);
    }

    return job;
  }

  /**
   * Checks delayed index and active job leases, promoting ready jobs.
   */
  _sweep() {
    const now = Date.now();

    // 1. Promote due delayed jobs
    const due = this._delayedIndex.rangeByScore(-Infinity, now, { limit: 100 });
    for (const item of due) {
      this._delayedIndex.delete(item.member);
      this._readyQueue.push(item.member);
    }

    // 2. Reclaim expired active job leases
    for (const [id, lease] of this._activeJobs.entries()) {
      if (lease.expiresAt <= now) {
        this._activeJobs.delete(id);
        this._readyQueue.push(id);
      }
    }
  }

  /**
   * Starts a worker loop processing jobs.
   *
   * @param {function(object): Promise<void>} handler
   * @param {object} [options={}]
   * @param {number} [options.pollIntervalMs=50]
   * @returns {{ stop: () => Promise<void> }}
   */
  worker(handler, options = {}) {
    let running = true;
    const pollInterval = options.pollIntervalMs || 50;

    const loop = async () => {
      try {
        await this._ensureHydrated();
        while (running && !this._engine.isClosed) {
          this._sweep();

        if (this._readyQueue.length === 0) {
          if (!running || this._engine.isClosed) break;

          // Compute exact next wake-up needed if delayed jobs or active leases exist
          let nextWaitMs = null;
          if (this._delayedIndex.length > 0) {
            const earliest = this._delayedIndex.bottom(1);
            if (earliest && earliest.length > 0) {
              const diff = earliest[0].score - Date.now();
              nextWaitMs = Math.max(1, diff);
            }
          }

          for (const lease of this._activeJobs.values()) {
            const diff = lease.expiresAt - Date.now();
            if (nextWaitMs === null || diff < nextWaitMs) {
              nextWaitMs = Math.max(1, diff);
            }
          }

          const waitTime = nextWaitMs !== null ? Math.min(nextWaitMs, pollInterval) : null;

          await new Promise((resolve) => {
            let timer = null;
            const onWakeup = () => {
              if (timer) clearTimeout(timer);
              resolve();
            };
            this._engine.events.once(this._notifyEvent, onWakeup);
            if (waitTime !== null) {
              timer = setTimeout(() => {
                this._engine.events.removeListener(this._notifyEvent, onWakeup);
                resolve();
              }, waitTime);
              if (typeof timer.unref === "function") timer.unref();
            }
          });
          continue;
        }

        const id = this._readyQueue.shift();
        const job = await this._engine.get(this._jKey(id));
        if (!job) continue;

        // Claim job lease
        job.state = "active";
        job.attempts += 1;
        const timeoutMs = job.timeoutMs || 30000;
        this._activeJobs.set(id, { expiresAt: Date.now() + timeoutMs });
        await this._engine.set(this._jKey(id), job);

        try {
          await handler(job);
          // Success: delete job and release active lease
          this._activeJobs.delete(id);
          await this._engine.delete(this._jKey(id));
        } catch (err) {
          this._activeJobs.delete(id);
          job.error = err.message || String(err);

          if (job.attempts <= job.retries) {
            // Exponential backoff
            const backoffMs = Math.min(60000, Math.pow(2, job.attempts) * 1000);
            job.state = "delayed";
            job.runAt = Date.now() + backoffMs;
            await this._engine.set(this._jKey(id), job);
            this._delayedIndex.insert(job.runAt, id);
          } else {
            // Dead-letter failed state
            job.state = "failed";
            await this._engine.set(this._jKey(id), job);
          }
        }
      }
    } catch (err) {
      console.error("[Queue Worker Loop Error]:", err);
    }
  };

    // Kick off loop
    const workerPromise = loop();

    const workerHandle = {
      stop: async () => {
        running = false;
        this._engine.events.emit(this._notifyEvent);
        await workerPromise;
      }
    };

    this._workers.push(workerHandle);
    return workerHandle;
  }

  /**
   * Returns queue depth statistics.
   */
  async size() {
    await this._ensureHydrated();
    this._sweep();
    return {
      ready: this._readyQueue.length,
      delayed: this._delayedIndex.length,
      active: this._activeJobs.size
    };
  }

  /**
   * Stops all active workers on this queue.
   */
  async close() {
    for (const w of this._workers) {
      await w.stop();
    }
    this._workers.length = 0;
  }

  /**
   * History inspection for queue jobs.
   *
   * @param {string} [jobId]
   * @param {object} [options={}]
   * @returns {AsyncGenerator<object>}
   */
  history(jobId, options = {}) {
    if (jobId && typeof jobId === "string") {
      return this._engine.history(this._jKey(jobId), options);
    }
    return this._engine.history(this._jobPrefix, typeof jobId === "object" ? jobId : options);
  }

  /**
   * Generic queue rollback is prohibited to avoid re-executing external side effects.
   */
  async rollback() {
    const { UnsupportedHistoricalOperationError } = await import("../errors.js");
    throw new UnsupportedHistoricalOperationError(
      "Generic queue rollback is not permitted to prevent re-executing external side effects. Use queue.requeue() or queue.replay() instead."
    );
  }

  /**
   * Requeues an existing job explicitly.
   *
   * @param {string} jobId
   * @returns {Promise<boolean>}
   */
  async requeue(jobId) {
    await this._ensureHydrated();
    const job = await this._engine.get(this._jKey(jobId));
    if (!job) return false;
    job.state = "ready";
    job.status = "ready";
    await this._engine.set(this._jKey(jobId), job);
    if (!this._readyQueue.includes(jobId)) {
      this._readyQueue.push(jobId);
      this._engine.events.emit(this._notifyEvent);
    }
    return true;
  }

  /**
   * Replays historical queue jobs with dry-run verification.
   *
   * @param {object} [options={}]
   * @param {boolean} [options.dryRun=true]
   * @returns {Promise<Array<object>>}
   */
  async replay(options = {}) {
    await this._ensureHydrated();
    const isDryRun = options.dryRun !== false;
    const replayed = [];
    const jobKeys = this._engine.keys(this._jobPrefix);
    for (const jk of jobKeys) {
      const job = await this._engine.get(jk);
      if (job) {
        replayed.push(job);
        if (!isDryRun && !this._readyQueue.includes(job.id)) {
          this._readyQueue.push(job.id);
          this._engine.events.emit(this._notifyEvent);
        }
      }
    }
    return replayed;
  }
}

