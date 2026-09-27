/**
 * ScopedEngine: Transparent namespace prefix wrapper around TencereEngine.
 * Implements the unified composition pattern:
 * scope -> namespace prefix -> normal Tencere primitive
 */

import { WatchStream } from "../coordination/watch.js";
import { calculateStateHash } from "../diagnostics/state-hash.js";
import { verifyInvariants } from "../diagnostics/invariants.js";

class ScopedStorage {
  constructor(storage, prefix) {
    this._rawStorage = storage;
    this._prefix = prefix;
  }

  _k(key) {
    return `${this._prefix}${key}`;
  }

  _stripPrefix(fullKey) {
    if (fullKey.startsWith(this._prefix)) {
      return fullKey.slice(this._prefix.length);
    }
    return fullKey;
  }

  get(key) {
    return this._rawStorage.get(this._k(key));
  }

  set(key, value, version) {
    return this._rawStorage.set(this._k(key), value, version);
  }

  has(key) {
    return this._rawStorage.has(this._k(key));
  }

  delete(key) {
    return this._rawStorage.delete(this._k(key));
  }

  get entries() {
    return this._rawStorage.entries;
  }

  scan(prefix = "", options = {}) {
    const fullPrefix = this._k(prefix);
    const results = this._rawStorage.scan(fullPrefix, options);
    return results.map((entry) => ({
      ...entry,
      key: this._stripPrefix(entry.key)
    }));
  }

  keys(prefix = "") {
    const fullPrefix = this._k(prefix);
    const rawKeys = this._rawStorage.keys(fullPrefix);
    return rawKeys.map((k) => this._stripPrefix(k));
  }
}

export class ScopedEngine {
  /**
   * @param {import('./engine.js').TencereEngine|ScopedEngine} engine
   * @param {string} prefix
   */
  constructor(engine, prefix) {
    this._rawEngine = engine._rawEngine || engine;
    this._prefix = prefix.endsWith(":") ? prefix : prefix + ":";
    this.events = this._rawEngine.events;
    this.historyManager = this._rawEngine.historyManager;
    this.storage = new ScopedStorage(this._rawEngine.storage, this._prefix);
    this.daktilo = this._rawEngine.daktilo;
  }

  get _engine() {
    return this._rawEngine;
  }

  get isClosed() {
    return this._rawEngine.isClosed;
  }

  get _sequenceCounter() {
    return this._rawEngine._sequenceCounter;
  }

  set _sequenceCounter(val) {
    this._rawEngine._sequenceCounter = val;
  }

  get _historyEnabled() {
    return this._rawEngine._historyEnabled;
  }

  _k(key) {
    return `${this._prefix}${key}`;
  }

  _stripPrefix(fullKey) {
    if (fullKey.startsWith(this._prefix)) {
      return fullKey.slice(this._prefix.length);
    }
    return fullKey;
  }

  async get(key, options) {
    return this._rawEngine.get(this._k(key), options);
  }

  async set(key, value, options) {
    return this._rawEngine.set(this._k(key), value, options);
  }

  async has(key) {
    return this._rawEngine.has(this._k(key));
  }

  async delete(key) {
    return this._rawEngine.delete(this._k(key));
  }

  async getMany(keys) {
    const prefixed = keys.map((k) => this._k(k));
    const raw = await this._rawEngine.getMany(prefixed);
    const result = {};
    for (let i = 0; i < keys.length; i++) {
      result[keys[i]] = raw[prefixed[i]];
    }
    return result;
  }

  async setMany(entries, options) {
    const list = Array.isArray(entries) ? entries : Object.entries(entries);
    const prefixed = list.map(([k, v]) => [this._k(k), v]);
    return this._rawEngine.setMany(prefixed, options);
  }

  async increment(key, delta) {
    return this._rawEngine.increment(this._k(key), delta);
  }

  async incr(key, delta) {
    return this._rawEngine.increment(this._k(key), delta);
  }

  async decr(key, delta) {
    return this._rawEngine.increment(this._k(key), delta !== undefined ? -delta : -1);
  }

  async patch(key, patchSpec) {
    return this._rawEngine.patch(this._k(key), patchSpec);
  }

  async update(key, updater, options) {
    return this._rawEngine.update(this._k(key), updater, options);
  }

  keys(subPrefix = "") {
    const fullPrefix = this._k(subPrefix);
    const rawKeys = this._rawEngine.keys(fullPrefix);
    return rawKeys.map((k) => this._stripPrefix(k));
  }

  ttl(key) {
    return this._rawEngine.ttl(this._k(key));
  }

  async expire(key, duration, options) {
    return this._rawEngine.expire(this._k(key), duration, options);
  }

  async persist(key) {
    return this._rawEngine.persist(this._k(key));
  }

  _getTimeSeriesStorage(name) {
    return this._rawEngine._getTimeSeriesStorage(this._k(name));
  }

  history(keyOrOptions, options = {}) {
    if (typeof keyOrOptions === "string") {
      return this._rawEngine.history(this._k(keyOrOptions), options);
    }
    return this._rawEngine.history(this._prefix, keyOrOptions);
  }

  rollbackPlan(options = {}) {
    const subPrefix = options.prefix || "";
    return this._rawEngine.rollbackPlan({
      ...options,
      prefix: this._k(subPrefix)
    });
  }

  async rollback(options = {}) {
    const plan = await this.rollbackPlan(options);
    return plan.apply(options);
  }

  nextFencingToken() {
    return this._rawEngine.nextFencingToken();
  }

  watch(pattern = "") {
    return new WatchStream(this._rawEngine, this._k(pattern));
  }

  get debug() {
    return {
      stateHash: async (opts) =>
        calculateStateHash(this._rawEngine, {
          ...opts,
          keyFilter: (k) => k.startsWith(this._prefix) && (!opts?.keyFilter || opts.keyFilter(k))
        }),
      verify: async () => verifyInvariants(this._rawEngine)
    };
  }
}
