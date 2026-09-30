/**
 * Tencere: Modern, high-performance KV / cache / coordination database.
 */

import "./core/polyfill.js";
import { TencereEngine } from "./core/engine.js";
import { KVCollection } from "./collections/kv.js";
import { MapCollection } from "./collections/map.js";
import { SetCollection } from "./collections/set.js";
import { SortedCollection } from "./collections/sorted.js";
import { StreamCollection } from "./collections/stream.js";
import { QueueCollection } from "./collections/queue.js";
import { VectorCollection } from "./collections/vector.js";
import { TimeSeriesCollection } from "./collections/timeseries.js";
import { TimeSeriesQuery } from "./timeseries/query.js";
import { Counter } from "./collections/counter.js";
import { Scope } from "./collections/scope.js";
import { LockManager } from "./coordination/lock.js";
import { OnceCoordinator } from "./coordination/once.js";
import { IdempotencyCoordinator } from "./coordination/idempotency.js";
import { Semaphore } from "./coordination/semaphore.js";
import { RateLimiter } from "./coordination/rate-limiter.js";
import { CacheManager } from "./coordination/cache.js";
import { ScheduleBuilder } from "./coordination/scheduler.js";
import { WatchStream } from "./coordination/watch.js";
import { waitFor } from "./coordination/wait-for.js";
import { SemanticCache } from "./ai/semantic-cache.js";
import { AgentMemory } from "./ai/memory.js";
import { ClusterManager } from "./cluster/cluster.js";

import { calculateStateHash, calculatePartitionHash } from "./diagnostics/state-hash.js";
import { verifyInvariants } from "./diagnostics/invariants.js";
import { LIMITS } from "./core/limits.js";

export { LIMITS } from "./core/limits.js";
export * from "./errors.js";
export { TencereSync } from "./sync/index.js";
export { TencereClient, TencereClusterClient, ClusterClientPipeline } from "./client/index.js";
export { TencereServer } from "./core/server.js";
export { HistoryManager } from "./history/index.js";
export { HistoricalView } from "./history/view.js";
export { RollbackPlan } from "./history/plan.js";
export { HistoryConfig } from "./history/config.js";
export { TimeSeriesCollection, TimeSeriesQuery };
export { calculateStateHash, calculatePartitionHash, verifyInvariants };
export { FaultInjectionError, FaultInjector } from "./core/fault-injection.js";
export { createTestCluster, TestCluster, PartitionableNetwork } from "./testing/test-cluster.js";

export class Tencere {
  /**
   * @param {TencereEngine} engine
   * @param {object} [options={}]
   */
  constructor(engine, options = {}) {
    this._engine = engine;
    this.options = options;

    this._locks = new LockManager(engine);
    this._once = new OnceCoordinator(engine, this._locks);
    this._idemp = new IdempotencyCoordinator(engine, this._locks);
    this._rateLimiter = new RateLimiter(engine);
    this._cache = new CacheManager(engine, this._locks);

    this._queues = new Map();
    this._cluster = options.cluster ? new ClusterManager(engine, options.cluster) : null;

    // AI layers
    this.semantic = new SemanticCache(this);
    this.memory = new AgentMemory(this);

    // Diagnostics & Invariants framework
    this.debug = {
      stateHash: async (opts) => calculateStateHash(this._engine, opts),
      partitionHash: async (pId, total) => calculatePartitionHash(this._engine, pId, total),
      verify: async () => verifyInvariants(this._engine),
      inspect: async () => {
        const inv = await verifyInvariants(this._engine);
        const hash = calculateStateHash(this._engine);
        return {
          stateHash: hash,
          invariants: inv,
          stats: this.stats()
        };
      },
      fault: this._engine._faults
    };
  }

  /**
   * System limits, constraints, and protocol thresholds.
   */
  get limits() {
    return LIMITS;
  }

  /**
   * System limits, constraints, and protocol thresholds.
   */
  static get limits() {
    return LIMITS;
  }

  /**
   * Opens or creates an embedded or clustered Tencere database.
   *
   * @param {string} [dataDir]
   * @param {object} [options={}]
   * @returns {Promise<Tencere>}
   */
  static async open(dataDir = null, options = {}) {
    let dir = dataDir;
    let opts = options;
    if (typeof dataDir === "object" && dataDir !== null) {
      opts = dataDir;
      dir = opts.dataDir || null;
    }
    const engine = await TencereEngine.open(dir, opts);
    const db = new Tencere(engine, opts);

    if (db._cluster) {
      await db._cluster.start();
    }

    return db;
  }

  // ---------------- Basic KV ----------------

  async get(key, options) {
    return this._engine.get(key, options);
  }

  async set(key, value, options) {
    return this._engine.set(key, value, options);
  }

  async has(key) {
    return this._engine.has(key);
  }

  async delete(key) {
    return this._engine.delete(key);
  }

  async getMany(keys) {
    return this._engine.getMany(keys);
  }

  async setMany(entries, options) {
    return this._engine.setMany(entries, options);
  }

  async increment(key, delta, options) {
    return this._engine.increment(key, delta, options);
  }

  async incr(key, delta, options) {
    return this._engine.increment(key, delta, options);
  }

  async decr(key, delta, options) {
    return this._engine.increment(key, delta !== undefined ? -delta : -1, options);
  }

  async patch(key, patchSpec, options) {
    return this._engine.patch(key, patchSpec, options);
  }

  async update(key, updater, options) {
    return this._engine.update(key, updater, options);
  }

  keys(prefix = "") {
    return this._engine.keys(prefix);
  }

  ttl(key) {
    return this._engine.ttl(key);
  }

  // ---------------- History / Time Travel / Rollback ----------------

  at(selector) {
    return this._engine.at(selector);
  }

  async snapshot(options) {
    return this._engine.snapshot(options);
  }

  async rollback(key, options) {
    return this._engine.rollback(key, options);
  }

  async rollbackPlan(options) {
    return this._engine.rollbackPlan(options);
  }

  async restorePlan(snapshot) {
    return this._engine.rollbackPlan(snapshot._selector);
  }

  history(keyOrOptions, options) {
    return this._engine.history(keyOrOptions, options);
  }

  lockHistory(key) {
    return this._engine.lockHistory(key);
  }

  async compact() {
    return this._engine.compact();
  }

  // ---------------- Collections ----------------

  counter(key) {
    return new Counter(this._engine, key);
  }

  scope(prefix, options) {
    return new Scope(this, prefix, options);
  }

  kv(name, options) {
    return new KVCollection(this._engine, name, options);
  }

  map(name, options) {
    return new MapCollection(this._engine, name, options);
  }

  setCollection(name) {
    return new SetCollection(this._engine, name);
  }

  sorted(name, options) {
    return new SortedCollection(this._engine, name, options);
  }

  stream(name) {
    return new StreamCollection(this._engine, name);
  }

  queue(name) {
    if (!this._queues.has(name)) {
      this._queues.set(name, new QueueCollection(this._engine, name));
    }
    return this._queues.get(name);
  }

  vector(name, options) {
    return new VectorCollection(this._engine, name, options);
  }

  timeseries(name, options) {
    return new TimeSeriesCollection(this._engine, name, options);
  }

  // ---------------- Coordination ----------------

  async lock(key, optionsOrFn, maybeFn) {
    return this._locks.withLock(key, optionsOrFn, maybeFn);
  }

  async tryLock(key, options) {
    return this._locks.tryAcquire(key, options);
  }

  async once(key, fn, options) {
    return this._once.execute(key, fn, options);
  }

  async idempotent(key, fn, options) {
    return this._idemp.execute(key, fn, options);
  }

  semaphore(key, options) {
    return new Semaphore(this._engine, key, options);
  }

  async rateLimit(key, options) {
    return this._rateLimiter.consume(key, options);
  }

  async cache(key, options, loader) {
    return this._cache.getOrLoad(key, options, loader);
  }

  schedule(name) {
    return new ScheduleBuilder(this._engine, this._locks, name);
  }

  watch(pattern) {
    return new WatchStream(this._engine, pattern);
  }

  async waitFor(key, condition, options) {
    return waitFor(this, key, condition, options);
  }

  // ---------------- Observability & Lifecycle ----------------

  /**
   * Cluster manager if cluster mode is enabled, or null.
   * @type {ClusterManager|null}
   */
  get cluster() {
    return this._cluster;
  }

  stats() {
    const s = this._engine.stats();
    if (this._cluster) {
      s.cluster = this._cluster.status();
    }
    return s;
  }

  /**
   * Evaluates comprehensive health status for embedded or cluster engine.
   *
   * @param {object} [options={}]
   * @returns {Promise<object>}
   */
  async health(options = {}) {
    if (this._cluster) {
      return this._cluster.health(options);
    }
    const s = this.stats();
    return {
      enabled: false,
      status: "STANDALONE",
      readiness: !this._engine.isClosed,
      liveness: !this._engine.isClosed,
      keys: s.keys,
      operations: s.operations
    };
  }

  /**
   * Telemetry metrics for cluster or standalone engine.
   *
   * @returns {object}
   */
  metrics() {
    if (this._cluster) {
      return this._cluster.metrics();
    }
    return {
      enabled: false,
      status: "STANDALONE",
      stats: this.stats()
    };
  }

  async checkpoint() {
    return this._engine.checkpoint();
  }

  async close() {
    for (const q of this._queues.values()) {
      await q.close();
    }
    this._queues.clear();

    if (this._cluster) {
      await this._cluster.stop();
    }

    await this._engine.close();
  }
}
