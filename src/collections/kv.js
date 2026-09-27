/**
 * Key-Value namespace collection.
 */

export class KVCollection {
  /**
   * @param {import('../core/engine.js').TencereEngine} engine
   * @param {string} prefix
   * @param {object} [options={}]
   */
  constructor(engine, prefix = "", options = {}) {
    this._engine = engine;
    this._prefix = prefix ? (prefix.endsWith(":") ? prefix : prefix + ":") : "";

    if (options.history !== undefined && engine.historyManager) {
      engine.historyManager.setCollectionOverride(prefix, options.history);
    }
  }

  _key(k) {
    return this._prefix ? `${this._prefix}${k}` : k;
  }

  /**
   * History inspection for key or collection.
   *
   * @param {string} [key]
   * @param {object} [options={}]
   * @returns {AsyncGenerator<object>}
   */
  history(key, options = {}) {
    if (key && typeof key === "string") {
      return this._engine.history(this._key(key), options);
    }
    return this._engine.history(this._prefix, typeof key === "object" ? key : options);
  }

  /**
   * Returns a historical view of this KV collection.
   *
   * @param {string|number|Date|object} target
   * @returns {import('../history/view.js').HistoricalKVCollection}
   */
  at(target) {
    return this._engine.at(target).kv(this._prefix);
  }

  /**
   * Rollback KV collection to previous state.
   *
   * @param {object} [options={}]
   * @returns {Promise<{ applied: number, skipped: number, sequence: bigint }>}
   */
  async rollback(options = {}) {
    const plan = await this.rollbackPlan(options);
    return plan.apply(options);
  }

  /**
   * Generates a rollback plan for this KV collection.
   *
   * @param {object} [options={}]
   * @returns {Promise<import('../history/plan.js').RollbackPlan>}
   */
  async rollbackPlan(options = {}) {
    return this._engine.rollbackPlan({ prefix: this._prefix, ...options });
  }

  async get(key, options) {
    return this._engine.get(this._key(key), options);
  }

  async set(key, value, options) {
    return this._engine.set(this._key(key), value, options);
  }

  async has(key) {
    return this._engine.has(this._key(key));
  }

  async delete(key) {
    return this._engine.delete(this._key(key));
  }

  async getMany(keys) {
    const prefixed = keys.map((k) => this._key(k));
    const raw = await this._engine.getMany(prefixed);
    const result = {};
    for (let i = 0; i < keys.length; i++) {
      result[keys[i]] = raw[prefixed[i]];
    }
    return result;
  }

  async setMany(entries, options) {
    const list = Array.isArray(entries) ? entries : Object.entries(entries);
    const prefixed = list.map(([k, v]) => [this._key(k), v]);
    return this._engine.setMany(prefixed, options);
  }

  async increment(key, delta) {
    return this._engine.increment(this._key(key), delta);
  }

  async patch(key, patchSpec) {
    return this._engine.patch(this._key(key), patchSpec);
  }

  async update(key, updater, options) {
    return this._engine.update(this._key(key), updater, options);
  }

  async keys() {
    const rawKeys = this._engine.keys(this._prefix);
    return this._prefix ? rawKeys.map((k) => k.slice(this._prefix.length)) : rawKeys;
  }

  async clear() {
    const all = await this.keys();
    for (const k of all) {
      await this.delete(k);
    }
  }
}
