/**
 * HistoricalView: Read-only database view at a historical sequence / timestamp.
 * Enables time-travel queries across KV and all Tencere collections.
 */

import { ReadOnlyDatabaseError, UnsupportedHistoricalOperationError } from "../errors.js";
import { OrderedIndex } from "../core/ordered-index.js";
import { SortedQuery } from "../collections/sorted.js";
import { TimeSeriesQuery } from "../timeseries/query.js";

export class HistoricalView {
  /**
   * @param {import('./index.js').HistoryManager} historyManager
   * @param {object} selector - { at?: string|number|Date, atSequence?: bigint|number, version?: bigint|number }
   * @param {object} [snapshotPin=null]
   */
  constructor(historyManager, selector, snapshotPin = null) {
    this._historyManager = historyManager;
    this._selector = selector;
    this._snapshotPin = snapshotPin;
    this.isClosed = false;
  }

  // ---------------- Read Operations ----------------

  async get(key, options = {}) {
    this._checkClosed();
    const entry = this._historyManager.getHistoricalEntry(key, this._selector);
    if (!entry.exists) {
      return options.withVersion ? { value: undefined, version: 0n } : undefined;
    }
    if (options.withVersion) {
      return { value: entry.value, version: entry.version };
    }
    return entry.value;
  }

  async has(key) {
    this._checkClosed();
    const entry = this._historyManager.getHistoricalEntry(key, this._selector);
    return Boolean(entry.exists);
  }

  async getMany(keys) {
    this._checkClosed();
    const result = {};
    for (const k of keys) {
      result[k] = await this.get(k);
    }
    return result;
  }

  keys(prefix = "") {
    this._checkClosed();
    return this._historyManager.keysAt(prefix, this._selector);
  }

  ttl(key) {
    this._checkClosed();
    const entry = this._historyManager.getHistoricalEntry(key, this._selector);
    if (!entry.exists) return -2;
    return -1;
  }

  // ---------------- Reject Mutations (Read-Only) ----------------

  async set() {
    throw new ReadOnlyDatabaseError();
  }

  async delete() {
    throw new ReadOnlyDatabaseError();
  }

  async increment() {
    throw new ReadOnlyDatabaseError();
  }

  async patch() {
    throw new ReadOnlyDatabaseError();
  }

  async update() {
    throw new ReadOnlyDatabaseError();
  }

  async setMany() {
    throw new ReadOnlyDatabaseError();
  }

  // ---------------- Collections (Historical) ----------------

  map(name) {
    return new HistoricalMapCollection(this, name);
  }

  sorted(name) {
    return new HistoricalSortedCollection(this, name);
  }

  counter(key) {
    return new HistoricalCounter(this, key);
  }

  kv(name) {
    return new HistoricalKVCollection(this, name);
  }

  setCollection(name) {
    return new HistoricalSetCollection(this, name);
  }

  scope(prefix) {
    return new HistoricalScope(this, prefix);
  }

  vector(name, options) {
    return new HistoricalVectorCollection(this, name, options);
  }

  queue(name) {
    return new HistoricalQueueCollection(this, name);
  }

  stream(name) {
    return new HistoricalStreamCollection(this, name);
  }

  timeseries(name, options) {
    return new HistoricalTimeSeriesCollection(this, name, options);
  }

  // ---------------- Coordination (Disallowed on Historical View) ----------------

  async lock() {
    throw new UnsupportedHistoricalOperationError("Coordination leases cannot be acquired in a historical view");
  }

  async tryLock() {
    throw new UnsupportedHistoricalOperationError("Coordination leases cannot be acquired in a historical view");
  }

  async once() {
    throw new UnsupportedHistoricalOperationError("once() execution is not supported in a historical view");
  }

  async idempotent() {
    throw new UnsupportedHistoricalOperationError("idempotency is not supported in a historical view");
  }

  semaphore() {
    throw new UnsupportedHistoricalOperationError("Semaphores cannot be acquired in a historical view");
  }

  async rateLimit() {
    throw new UnsupportedHistoricalOperationError("Rate limiting is not supported in a historical view");
  }

  // ---------------- Snapshot Lifecycle ----------------

  async close() {
    if (this.isClosed) return;
    this.isClosed = true;
    if (this._snapshotPin) {
      this._snapshotPin.close();
      this._snapshotPin = null;
    }
  }

  _checkClosed() {
    if (this.isClosed) {
      throw new ReadOnlyDatabaseError("Historical view or snapshot is closed");
    }
  }
}

/**
 * Read-only Historical Map
 */
class HistoricalMapCollection {
  constructor(view, name) {
    this._view = view;
    this._name = name;
    this._prefix = `__map:${name}:`;
  }

  _k(key) {
    return `${this._prefix}${key}`;
  }

  _strip(fullKey) {
    return fullKey.slice(this._prefix.length);
  }

  async get(key, options) {
    return this._view.get(this._k(key), options);
  }

  async has(key) {
    return this._view.has(this._k(key));
  }

  async entries() {
    const rawKeys = this._view.keys(this._prefix);
    const result = [];
    for (const rk of rawKeys) {
      const val = await this._view.get(rk);
      if (val !== undefined) {
        result.push([this._strip(rk), val]);
      }
    }
    return result;
  }

  async keys() {
    const rawKeys = this._view.keys(this._prefix);
    return rawKeys.map((k) => this._strip(k));
  }

  async values() {
    const rawKeys = this._view.keys(this._prefix);
    const result = [];
    for (const rk of rawKeys) {
      const val = await this._view.get(rk);
      if (val !== undefined) {
        result.push(val);
      }
    }
    return result;
  }

  async size() {
    return this._view.keys(this._prefix).length;
  }

  async set() {
    throw new ReadOnlyDatabaseError();
  }

  async delete() {
    throw new ReadOnlyDatabaseError();
  }

  async clear() {
    throw new ReadOnlyDatabaseError();
  }
}

/**
 * Read-only Historical Sorted Collection
 */
class HistoricalSortedCollection {
  constructor(view, name) {
    this._view = view;
    this._name = name;
    this._sPrefix = `__sorted_score:${name}:`;
    this._vPrefix = `__sorted_val:${name}:`;
    this._indexPromise = null;
  }

  _sKey(member) {
    return `${this._sPrefix}${member}`;
  }

  _vKey(member) {
    return `${this._vPrefix}${member}`;
  }

  _stripMember(fullKey) {
    return fullKey.slice(this._sPrefix.length);
  }

  async _getIndex() {
    if (!this._indexPromise) {
      this._indexPromise = (async () => {
        const idx = new OrderedIndex();
        const scoreKeys = this._view.keys(this._sPrefix);
        for (const sk of scoreKeys) {
          const score = await this._view.get(sk);
          if (typeof score === "number") {
            const member = this._stripMember(sk);
            idx.insert(score, member);
          }
        }
        return idx;
      })();
    }
    return this._indexPromise;
  }

  async score(member) {
    const idx = await this._getIndex();
    return idx.score(member);
  }

  async rank(member, options = {}) {
    const idx = await this._getIndex();
    return idx.rank(member, options);
  }

  async getValue(member) {
    return this._view.get(this._vKey(member));
  }

  async top(n = 10) {
    const idx = await this._getIndex();
    const items = idx.top(n);
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

  async bottom(n = 10) {
    const idx = await this._getIndex();
    const items = idx.bottom(n);
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

  between(min, max) {
    return new HistoricalSortedQuery(this, min, max);
  }

  above(min) {
    return new HistoricalSortedQuery(this, min, Infinity);
  }

  below(max) {
    return new HistoricalSortedQuery(this, -Infinity, max);
  }

  async set() {
    throw new ReadOnlyDatabaseError();
  }

  async incr() {
    throw new ReadOnlyDatabaseError();
  }

  async delete() {
    throw new ReadOnlyDatabaseError();
  }
}

class HistoricalSortedQuery {
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

  async entries() {
    const idx = await this._col._getIndex();
    const raw = idx.rangeByScore(this._min, this._max, {
      offset: this._offset,
      limit: this._limit,
      reverse: this._reverse
    });

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

/**
 * Read-only Historical Counter
 */
class HistoricalCounter {
  constructor(view, key) {
    this._view = view;
    this._key = key;
  }

  async value() {
    const val = await this._view.get(this._key);
    return val !== undefined ? Number(val) : 0;
  }

  async inc() {
    throw new ReadOnlyDatabaseError();
  }

  async dec() {
    throw new ReadOnlyDatabaseError();
  }

  async add() {
    throw new ReadOnlyDatabaseError();
  }

  async reset() {
    throw new ReadOnlyDatabaseError();
  }
}

/**
 * Read-only Historical Scope
 */
class HistoricalScope {
  constructor(view, prefix) {
    this._view = view;
    this._prefix = prefix.endsWith(":") ? prefix : prefix + ":";
  }

  _k(key) {
    return `${this._prefix}${key}`;
  }

  scope(sub) {
    return new HistoricalScope(this._view, `${this._prefix}${sub}`);
  }

  async get(key, options) {
    return this._view.get(this._k(key), options);
  }

  async has(key) {
    return this._view.has(this._k(key));
  }

  async getMany(keys) {
    const prefixed = keys.map((k) => this._k(k));
    const raw = await this._view.getMany(prefixed);
    const result = {};
    for (let i = 0; i < keys.length; i++) {
      result[keys[i]] = raw[prefixed[i]];
    }
    return result;
  }

  map(name) {
    return new HistoricalMapCollection(this._view, this._k(name));
  }

  sorted(name) {
    return new HistoricalSortedCollection(this._view, this._k(name));
  }

  counter(name) {
    return new HistoricalCounter(this._view, this._k(name));
  }

  async set() {
    throw new ReadOnlyDatabaseError();
  }

  async delete() {
    throw new ReadOnlyDatabaseError();
  }

  async increment() {
    throw new ReadOnlyDatabaseError();
  }

  async patch() {
    throw new ReadOnlyDatabaseError();
  }
}

/**
 * Read-only Historical KV
 */
class HistoricalKVCollection {
  constructor(view, name) {
    this._view = view;
    this._prefix = `__kv:${name}:`;
  }

  _k(key) {
    return `${this._prefix}${key}`;
  }

  async get(key, options) {
    return this._view.get(this._k(key), options);
  }

  async has(key) {
    return this._view.has(this._k(key));
  }

  async set() {
    throw new ReadOnlyDatabaseError();
  }

  async delete() {
    throw new ReadOnlyDatabaseError();
  }
}

/**
 * Read-only Historical Set
 */
class HistoricalSetCollection {
  constructor(view, name) {
    this._view = view;
    this._prefix = `__set:${name}:`;
  }

  _k(member) {
    return `${this._prefix}${member}`;
  }

  async has(member) {
    return this._view.has(this._k(member));
  }

  async members() {
    const rawKeys = this._view.keys(this._prefix);
    return rawKeys.map((k) => k.slice(this._prefix.length));
  }

  async size() {
    return this._view.keys(this._prefix).length;
  }

  async add() {
    throw new ReadOnlyDatabaseError();
  }

  async delete() {
    throw new ReadOnlyDatabaseError();
  }
}

/**
 * Read-only Historical Vector
 */
class HistoricalVectorCollection {
  constructor(view, name, options = {}) {
    this._view = view;
    this._name = name;
    this._prefix = `__vec:${name}:`;
  }

  async get(id) {
    return this._view.get(`${this._prefix}${id}`);
  }

  async search(queryVector, limit = 10) {
    // Vector search across historical documents
    const docKeys = this._view.keys(this._prefix);
    const docs = [];
    for (const dk of docKeys) {
      const doc = await this._view.get(dk);
      if (doc && Array.isArray(doc.vector)) {
        const sim = cosineSimilarity(queryVector, doc.vector);
        docs.push({ id: doc.id, score: sim, metadata: doc.metadata });
      }
    }
    docs.sort((a, b) => b.score - a.score);
    return docs.slice(0, limit);
  }

  async insert() {
    throw new ReadOnlyDatabaseError();
  }

  async delete() {
    throw new ReadOnlyDatabaseError();
  }
}

function cosineSimilarity(a, b) {
  let dot = 0;
  let normA = 0;
  let normB = 0;
  const len = Math.min(a.length, b.length);
  for (let i = 0; i < len; i++) {
    dot += a[i] * b[i];
    normA += a[i] * a[i];
    normB += b[i] * b[i];
  }
  if (normA === 0 || normB === 0) return 0;
  return dot / (Math.sqrt(normA) * Math.sqrt(normB));
}

/**
 * Read-only Historical Queue
 */
class HistoricalQueueCollection {
  constructor(view, name) {
    this._view = view;
    this._name = name;
    this._prefix = `__queue_job:${name}:`;
  }

  async length() {
    return this._view.keys(this._prefix).length;
  }

  async push() {
    throw new ReadOnlyDatabaseError();
  }

  async pop() {
    throw new ReadOnlyDatabaseError();
  }

  async ack() {
    throw new ReadOnlyDatabaseError();
  }

  async rollback() {
    throw new UnsupportedHistoricalOperationError(
      "Generic queue rollback is not permitted to prevent re-executing external side effects. Use queue.requeue() or queue.replay() instead."
    );
  }
}

/**
 * Read-only Historical Stream
 */
class HistoricalStreamCollection {
  constructor(view, name) {
    this._view = view;
    this._name = name;
  }

  async append() {
    throw new ReadOnlyDatabaseError();
  }

  async rollback() {
    throw new UnsupportedHistoricalOperationError(
      "Stream rollback cannot truncate log history; write compensating records instead."
    );
  }
}

/**
 * Read-only Historical TimeSeries
 */
class HistoricalTimeSeriesCollection {
  constructor(view, name, options = {}) {
    this._view = view;
    this._name = name;
    this.options = options;
    const engine = view._historyManager.engine;
    this._storage = engine._getTimeSeriesStorage(name);

    let maxSeq = null;
    if (view._historyManager && typeof view._historyManager.getSequenceAt === "function") {
      maxSeq = view._historyManager.getSequenceAt(view._selector);
    } else if (view._selector.atSequence !== undefined) {
      maxSeq = BigInt(view._selector.atSequence);
    } else if (view._selector.sequence !== undefined) {
      maxSeq = BigInt(view._selector.sequence);
    } else if (view._selector.version !== undefined) {
      maxSeq = BigInt(view._selector.version);
    }
    this._maxSequence = maxSeq;
  }

  between(from, to) {
    return new TimeSeriesQuery(this, { maxSequence: this._maxSequence }).between(from, to);
  }

  where(tags) {
    return new TimeSeriesQuery(this, { maxSequence: this._maxSequence }).where(tags);
  }

  bucket(interval, options) {
    return new TimeSeriesQuery(this, { maxSequence: this._maxSequence }).bucket(interval, options);
  }

  async values() {
    return new TimeSeriesQuery(this, { maxSequence: this._maxSequence }).values();
  }

  iterate() {
    return new TimeSeriesQuery(this, { maxSequence: this._maxSequence }).iterate();
  }

  async latest(count = 1) {
    const list = [];
    for await (const pt of this.iterate()) {
      list.push(pt);
    }
    if (list.length === 0) return count > 1 ? [] : null;
    if (count === 1) return list[list.length - 1];
    return list.slice(-count);
  }

  async count() {
    return new TimeSeriesQuery(this, { maxSequence: this._maxSequence }).count();
  }

  async sum() {
    return new TimeSeriesQuery(this, { maxSequence: this._maxSequence }).sum();
  }

  async avg() {
    return new TimeSeriesQuery(this, { maxSequence: this._maxSequence }).avg();
  }

  async min() {
    return new TimeSeriesQuery(this, { maxSequence: this._maxSequence }).min();
  }

  async max() {
    return new TimeSeriesQuery(this, { maxSequence: this._maxSequence }).max();
  }

  async first() {
    return new TimeSeriesQuery(this, { maxSequence: this._maxSequence }).first();
  }

  async last() {
    return new TimeSeriesQuery(this, { maxSequence: this._maxSequence }).last();
  }

  async add() {
    throw new ReadOnlyDatabaseError();
  }

  async addMany() {
    throw new ReadOnlyDatabaseError();
  }

  async delete() {
    throw new ReadOnlyDatabaseError();
  }

  async correct() {
    throw new ReadOnlyDatabaseError();
  }
}

