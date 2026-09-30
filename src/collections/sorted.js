/**
 * Sorted collection primitive backed by an OrderedIndex and decoupled value storage.
 */

import { OrderedIndex } from "../core/ordered-index.js";
import { WatchStream } from "../coordination/watch.js";

export class SortedQuery {
  /**
   * @param {SortedCollection} collection
   * @param {number} min
   * @param {number} max
   */
  constructor(collection, min, max) {
    this._col = collection;
    this._min = min;
    this._max = max;
    this._offset = 0;
    this._limit = Infinity;
    this._reverse = false;
  }

  limit(n) {
    this._limit = n;
    return this;
  }

  offset(n) {
    this._offset = n;
    return this;
  }

  asc() {
    this._reverse = false;
    return this;
  }

  desc() {
    this._reverse = true;
    return this;
  }

  async take(n) {
    this._limit = n;
    return this.entries();
  }

  /**
   * Executes query and returns entries: Array<{ member: string, score: number, value?: any }>
   */
  async entries() {
    this._col._ensureHydrated();
    const raw = this._col._index.rangeByScore(this._min, this._max, {
      offset: this._offset,
      limit: this._limit,
      reverse: this._reverse
    });

    if (!this._col._hasValues) {
      return raw.map((item) => ({ member: item.member, score: item.score }));
    }

    const promises = raw.map(async (item) => {
      const val = await this._col.getValue(item.member);
      return {
        member: item.member,
        score: item.score,
        ...(val !== undefined ? { value: val } : {})
      };
    });
    return Promise.all(promises);
  }
}

export class SortedCollection {
  /**
   * @param {import('../core/engine.js').TencereEngine} engine
   * @param {string} name
   * @param {object} [options={}]
   */
  constructor(engine, name, options = {}) {
    this._engine = engine;
    this._name = name;
    this._sPrefix = `__sorted_score:${name}:`;
    this._valPrefix = `__sorted_val:${name}:`;
    this._hasValues = false;

    if (options.history !== undefined && engine.historyManager) {
      engine.historyManager.setCollectionOverride(name, options.history);
    }

    // Retrieve or initialize OrderedIndex on engine
    if (!engine._sortedIndexes) {
      engine._sortedIndexes = new Map();
    }
    if (!engine._sortedIndexes.has(name)) {
      engine._sortedIndexes.set(name, new OrderedIndex());
    }
    this._index = engine._sortedIndexes.get(name);
    this._hydrated = false;

    // Keep memory index synchronized with engine mutations (set, restore, delete)
    engine.events.on("change", (e) => {
      if (e && typeof e.key === "string" && e.key.startsWith(this._sPrefix)) {
        const member = e.key.slice(this._sPrefix.length);
        if (e.type === "set" || e.type === "restore") {
          if (typeof e.value === "number") {
            this._index.insert(e.value, member);
          }
        } else if (e.type === "delete") {
          this._index.delete(member);
        }
      }
    });
  }

  _ensureHydrated() {
    if (this._hydrated) return;
    this._hydrated = true;
    const rawKeys = this._engine.storage.keys(this._sPrefix);
    for (const k of rawKeys) {
      const member = k.slice(this._sPrefix.length);
      const entry = this._engine.storage.get(k);
      if (entry && typeof entry.value === "number") {
        this._index.insert(entry.value, member);
      }
    }
  }

  _sKey(member) {
    return `${this._sPrefix}${member}`;
  }

  _vKey(member) {
    return `${this._valPrefix}${member}`;
  }

  _stripMember(key) {
    if (key.startsWith(this._sPrefix)) {
      return key.slice(this._sPrefix.length);
    }
    return key;
  }

  /**
   * Adds or updates a member in the sorted collection.
   *
   * @param {string} member
   * @param {number|{ score: number, value?: any }} scoreOrObj
   * @returns {Promise<void>}
   */
  async set(member, scoreOrObj) {
    let score;
    let value = undefined;

    if (typeof scoreOrObj === "number") {
      score = scoreOrObj;
    } else if (typeof scoreOrObj === "object" && scoreOrObj !== null) {
      score = scoreOrObj.score;
      value = scoreOrObj.value;
    } else {
      score = Number(scoreOrObj);
    }

    this._index.insert(score, member);
    await this._engine.set(this._sKey(member), score);

    if (value !== undefined) {
      this._hasValues = true;
      await this._engine.set(this._vKey(member), value);
    }
  }

  /**
   * Increments score of a member.
   *
   * @param {string} member
   * @param {number} [delta=1]
   * @returns {Promise<number>} new score
   */
  async incr(member, delta = 1) {
    const cur = (await this.score(member)) || 0;
    const next = cur + delta;
    this._index.insert(next, member);
    await this._engine.set(this._sKey(member), next);
    return next;
  }

  /**
   * Returns score for member.
   *
   * @param {string} member
   * @returns {Promise<number|undefined>}
   */
  async score(member) {
    const val = await this._engine.get(this._sKey(member));
    if (val !== undefined) {
      const num = Number(val);
      if (this._index.score(member) !== num) {
        this._index.insert(num, member);
      }
      return num;
    }
    return this._index.score(member);
  }

  /**
   * Returns associated value for member if stored.
   *
   * @param {string} member
   * @returns {Promise<any>}
   */
  async getValue(member) {
    return this._engine.get(this._vKey(member));
  }

  /**
   * Returns 0-based rank of member.
   *
   * @param {string} member
   * @param {object} [options={}]
   * @param {boolean} [options.reverse=false]
   * @returns {Promise<number|undefined>}
   */
  async rank(member, options = {}) {
    this._ensureHydrated();
    return this._index.rank(member, options);
  }

  /**
   * Deletes member from sorted collection.
   *
   * @param {string} member
   * @returns {Promise<boolean>}
   */
  async delete(member) {
    const deleted = this._index.delete(member);
    await this._engine.delete(this._sKey(member));
    await this._engine.delete(this._vKey(member));
    return deleted;
  }

  /**
   * History inspection for sorted collection or member.
   *
   * @param {string} [member]
   * @param {object} [options={}]
   * @returns {AsyncGenerator<object>}
   */
  history(member, options = {}) {
    if (member && typeof member === "string") {
      return this._engine.history(this._sKey(member), options);
    }
    return this._engine.history(this._sPrefix, typeof member === "object" ? member : options);
  }

  /**
   * Returns a historical view of this sorted collection.
   *
   * @param {string|number|Date|object} target
   * @returns {import('../history/view.js').HistoricalSortedCollection}
   */
  at(target) {
    return this._engine.at(target).sorted(this._name);
  }

  /**
   * Rollback sorted collection to previous state.
   *
   * @param {object} [options={}]
   * @returns {Promise<{ applied: number, skipped: number, sequence: bigint }>}
   */
  async rollback(options = {}) {
    const plan = await this.rollbackPlan(options);
    return plan.apply(options);
  }

  /**
   * Generates a rollback plan for this sorted collection.
   *
   * @param {object} [options={}]
   * @returns {Promise<import('../history/plan.js').RollbackPlan>}
   */
  async rollbackPlan(options = {}) {
    return this._engine.rollbackPlan({ prefix: `__sorted_score:${this._name}:`, ...options });
  }

  /**
   * Returns top N members (highest score first).
   *
   * @param {number} [n=10]
   * @returns {Promise<Array<{ member: string, score: number, value?: any }>>}
   */
  async top(n = 10) {
    this._ensureHydrated();
    const items = this._index.top(n);
    if (!this._hasValues) {
      return items.map((item) => ({ member: item.member, score: item.score }));
    }
    const promises = items.map(async (item) => {
      const val = await this.getValue(item.member);
      return {
        member: item.member,
        score: item.score,
        ...(val !== undefined ? { value: val } : {})
      };
    });
    return Promise.all(promises);
  }

  /**
   * Returns bottom N members (lowest score first).
   *
   * @param {number} [n=10]
   * @returns {Promise<Array<{ member: string, score: number, value?: any }>>}
   */
  async bottom(n = 10) {
    this._ensureHydrated();
    const items = this._index.bottom(n);
    if (!this._hasValues) {
      return items.map((item) => ({ member: item.member, score: item.score }));
    }
    const promises = items.map(async (item) => {
      const val = await this.getValue(item.member);
      return {
        member: item.member,
        score: item.score,
        ...(val !== undefined ? { value: val } : {})
      };
    });
    return Promise.all(promises);
  }

  /**
   * Range query between min and max scores.
   *
   * @param {number} min
   * @param {number} max
   * @returns {SortedQuery}
   */
  between(min, max) {
    return new SortedQuery(this, min, max);
  }

  /**
   * Range query for scores >= min.
   *
   * @param {number} min
   * @returns {SortedQuery}
   */
  above(min) {
    return new SortedQuery(this, min, Infinity);
  }

  /**
   * Range query for scores <= max.
   *
   * @param {number} max
   * @returns {SortedQuery}
   */
  below(max) {
    return new SortedQuery(this, -Infinity, max);
  }

  /**
   * Total number of elements in the sorted collection.
   *
   * @returns {Promise<number>}
   */
  async count() {
    return this._index.length;
  }

  async size() {
    return this._index.length;
  }

  /**
   * Watches this sorted collection for additions, score updates, and deletions.
   *
   * @returns {AsyncGenerator<{ member: string, type: string, score: number, previousScore: number, version: bigint|number, timestamp: number }>}
   */
  async *watch() {
    const watchStream = new WatchStream(this._engine, this._sPrefix);
    for await (const event of watchStream) {
      const strippedKey = this._stripMember(event.key);
      let type = "updated";
      if (event.type === "delete" || event.type === "expire") {
        type = "deleted";
      } else if (event.type === "restore") {
        type = "restored";
      } else if (event.previousValue === undefined) {
        type = "added";
      }

      yield {
        member: strippedKey,
        type,
        score: event.value,
        previousScore: event.previousValue,
        version: event.version,
        timestamp: event.timestamp
      };
    }
  }
}
