/**
 * Scope: lightweight prefixed keyspace view for multi-tenancy and nested domains.
 */

import { ScopedEngine } from "../core/scoped-engine.js";
import { KVCollection } from "./kv.js";
import { MapCollection } from "./map.js";
import { SetCollection } from "./set.js";
import { SortedCollection } from "./sorted.js";
import { StreamCollection } from "./stream.js";
import { QueueCollection } from "./queue.js";
import { VectorCollection } from "./vector.js";
import { Counter } from "./counter.js";
import { TimeSeriesCollection } from "./timeseries.js";
import { LockManager } from "../coordination/lock.js";
import { Semaphore } from "../coordination/semaphore.js";
import { RateLimiter } from "../coordination/rate-limiter.js";
import { CacheManager } from "../coordination/cache.js";
import { OnceCoordinator } from "../coordination/once.js";
import { IdempotencyCoordinator } from "../coordination/idempotency.js";
import { waitFor } from "../coordination/wait-for.js";
import { calculateStateHash } from "../diagnostics/state-hash.js";
import { verifyInvariants } from "../diagnostics/invariants.js";

export class Scope {
  /**
   * @param {import('../index.js').Tencere} db
   * @param {string} prefix
   * @param {object} [options={}]
   */
  constructor(db, prefix, options = {}) {
    this._db = db;
    this._prefix = prefix.endsWith(":") ? prefix : prefix + ":";
    const rawEngine = db._engine || db;
    this._scopedEngine = new ScopedEngine(rawEngine, this._prefix);
    this._locks = new LockManager(this._scopedEngine);
    this._rateLimiter = new RateLimiter(this._scopedEngine);
    this._cache = new CacheManager(this._scopedEngine, this._locks);
    this._once = new OnceCoordinator(this._scopedEngine, this._locks);
    this._idemp = new IdempotencyCoordinator(this._scopedEngine, this._locks);

    if (options.history !== undefined && rawEngine.historyManager) {
      rawEngine.historyManager.setCollectionOverride(prefix, options.history);
    }
  }

  _k(key) {
    return `${this._prefix}${key}`;
  }

  /**
   * History inspection for scope or key within scope.
   *
   * @param {string} [key]
   * @param {object} [options={}]
   * @returns {AsyncGenerator<object>}
   */
  history(key, options = {}) {
    return this._scopedEngine.history(key, options);
  }

  /**
   * Returns a historical view of this scope.
   *
   * @param {string|number|Date|object} target
   * @returns {import('../history/view.js').HistoricalScope}
   */
  at(target) {
    return this._db.at(target).scope(this._prefix);
  }

  /**
   * Rollback scope to previous state.
   *
   * @param {object} [options={}]
   * @returns {Promise<{ applied: number, skipped: number, sequence: bigint }>}
   */
  async rollback(options = {}) {
    return this._scopedEngine.rollback(options);
  }

  /**
   * Generates a rollback plan for this scope.
   *
   * @param {object} [options={}]
   * @returns {Promise<import('../history/plan.js').RollbackPlan>}
   */
  async rollbackPlan(options = {}) {
    return this._scopedEngine.rollbackPlan(options);
  }

  /**
   * Creates a nested scope.
   *
   * @param {string} subScope
   * @returns {Scope}
   */
  scope(subScope) {
    return new Scope(this._db, `${this._prefix}${subScope}`);
  }

  subscope(subScope) {
    return this.scope(subScope);
  }

  async get(key, options) {
    return this._scopedEngine.get(key, options);
  }

  async set(key, value, options) {
    return this._scopedEngine.set(key, value, options);
  }

  async has(key) {
    return this._scopedEngine.has(key);
  }

  async delete(key) {
    return this._scopedEngine.delete(key);
  }

  async getMany(keys) {
    return this._scopedEngine.getMany(keys);
  }

  async setMany(entries, options) {
    return this._scopedEngine.setMany(entries, options);
  }

  async increment(key, delta) {
    return this._scopedEngine.increment(key, delta);
  }

  async patch(key, patchSpec) {
    return this._scopedEngine.patch(key, patchSpec);
  }

  async update(key, updater, options) {
    return this._scopedEngine.update(key, updater, options);
  }

  keys(subPrefix = "") {
    return this._scopedEngine.keys(subPrefix);
  }

  counter(key) {
    return new Counter(this._scopedEngine, key);
  }

  kv(name) {
    return new KVCollection(this._scopedEngine, name);
  }

  map(name) {
    return new MapCollection(this._scopedEngine, name);
  }

  setCollection(name) {
    return new SetCollection(this._scopedEngine, name);
  }

  sorted(name) {
    return new SortedCollection(this._scopedEngine, name);
  }

  stream(name) {
    return new StreamCollection(this._scopedEngine, name);
  }

  queue(name) {
    return new QueueCollection(this._scopedEngine, name);
  }

  vector(name, options) {
    return new VectorCollection(this._scopedEngine, name, options);
  }

  timeseries(name, options) {
    return new TimeSeriesCollection(this._scopedEngine, name, options);
  }

  async lock(name, optionsOrFn, maybeFn) {
    return this._locks.acquire(name, optionsOrFn, maybeFn);
  }

  async tryLock(name, options) {
    return this._locks.tryAcquire(name, options);
  }

  async once(name, fn, options) {
    return this._once.execute(name, fn, options);
  }

  async idempotent(name, fn, options) {
    return this._idemp.execute(name, fn, options);
  }

  semaphore(name, options) {
    return new Semaphore(this._scopedEngine, name, options);
  }

  async rateLimit(name, options) {
    return this._rateLimiter.consume(name, options);
  }

  async cache(name, options, loader) {
    return this._cache.getOrLoad(name, options, loader);
  }

  watch(pattern = "") {
    return this._scopedEngine.watch(pattern);
  }

  async waitFor(name, condition, options) {
    return waitFor(this._scopedEngine, name, condition, options);
  }

  get debug() {
    return {
      stateHash: async (opts) =>
        calculateStateHash(this._scopedEngine._engine, {
          ...opts,
          keyFilter: (k) => k.startsWith(this._prefix) && (!opts?.keyFilter || opts.keyFilter(k))
        }),
      verify: async () => verifyInvariants(this._scopedEngine._engine)
    };
  }
}
