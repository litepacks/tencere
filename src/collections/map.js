/**
 * Map collection primitive.
 * Maps keys to values within a defined collection namespace.
 */

export class MapCollection {
  /**
   * @param {import('../core/engine.js').TencereEngine} engine
   * @param {string} name
   * @param {object} [options={}]
   */
  constructor(engine, name, options = {}) {
    this._engine = engine;
    this._name = name;
    this._prefix = `__map:${name}:`;

    if (options.history !== undefined && engine.historyManager) {
      engine.historyManager.setCollectionOverride(name, options.history);
    }
  }

  _k(key) {
    return `${this._prefix}${key}`;
  }

  _strip(fullKey) {
    return fullKey.slice(this._prefix.length);
  }

  /**
   * History inspection for map collection or member.
   *
   * @param {string} [member]
   * @param {object} [options={}]
   * @returns {AsyncGenerator<object>}
   */
  history(member, options = {}) {
    if (member && typeof member === "string") {
      return this._engine.history(this._k(member), options);
    }
    return this._engine.history(this._prefix, typeof member === "object" ? member : options);
  }

  /**
   * Returns a historical view of this map.
   *
   * @param {string|number|Date|object} target
   * @returns {import('../history/view.js').HistoricalMapCollection}
   */
  at(target) {
    return this._engine.at(target).map(this._name);
  }

  /**
   * Rollback map collection to previous state.
   *
   * @param {object} [options={}]
   * @returns {Promise<{ applied: number, skipped: number, sequence: bigint }>}
   */
  async rollback(options = {}) {
    const plan = await this.rollbackPlan(options);
    return plan.apply(options);
  }

  /**
   * Generates a rollback plan for this map.
   *
   * @param {object} [options={}]
   * @returns {Promise<import('../history/plan.js').RollbackPlan>}
   */
  async rollbackPlan(options = {}) {
    return this._engine.rollbackPlan({ prefix: this._prefix, ...options });
  }

  async get(key, options) {
    return this._engine.get(this._k(key), options);
  }

  async set(key, value, options) {
    return this._engine.set(this._k(key), value, options);
  }

  async has(key) {
    return this._engine.has(this._k(key));
  }

  async delete(key) {
    return this._engine.delete(this._k(key));
  }

  async entries() {
    const raw = this._engine.storage.scan(this._prefix);
    return raw.map((entry) => [this._strip(entry.key), entry.value]);
  }

  async keys() {
    const rawKeys = this._engine.storage.keys(this._prefix);
    return rawKeys.map((k) => this._strip(k));
  }

  async values() {
    const raw = this._engine.storage.scan(this._prefix);
    return raw.map((entry) => entry.value);
  }

  async size() {
    return this._engine.storage.keys(this._prefix).length;
  }

  async clear() {
    const keys = this._engine.storage.keys(this._prefix);
    for (const k of keys) {
      await this._engine.delete(k);
    }
  }

  /**
   * Watches this map collection for additions, updates, and deletions.
   *
   * @returns {AsyncGenerator<{ key: string, type: string, value: any, previousValue: any }>}
   */
  async *watch() {
    const queue = [];
    let notify = null;
    let closed = false;

    const listener = (event) => {
      if (closed || !event.key.startsWith(this._prefix)) return;
      const strippedKey = this._strip(event.key);
      let type = "updated";
      if (event.type === "delete" || event.type === "expire") {
        type = "deleted";
      } else if (event.previousValue === undefined) {
        type = "added";
      }

      queue.push({
        key: strippedKey,
        type,
        value: event.value,
        previousValue: event.previousValue
      });

      if (notify) {
        const fn = notify;
        notify = null;
        fn();
      }
    };

    this._engine.events.on("change", listener);

    try {
      while (!closed && !this._engine.isClosed) {
        while (queue.length > 0) {
          yield queue.shift();
        }
        await new Promise((resolve) => {
          notify = resolve;
        });
      }
    } finally {
      closed = true;
      this._engine.events.removeListener("change", listener);
    }
  }
}
