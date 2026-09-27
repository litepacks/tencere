/**
 * TencereSync: Pure synchronous local embedded database binding.
 *
 * Exposes synchronous local operations (KV, Map, Set, Sorted, TTL, Counters, CAS).
 * Does NOT fake synchronous distributed I/O.
 */

import { StorageEngine } from "../core/storage.js";
import { ExpiryManager, parseDuration } from "../core/expiry-wheel.js";
import { OrderedIndex } from "../core/ordered-index.js";
import { VersionMismatchError, DatabaseClosedError } from "../errors.js";

class SyncCounter {
  constructor(syncDb, key) {
    this._db = syncDb;
    this._key = key;
  }

  inc(delta = 1) {
    return this._db.increment(this._key, delta);
  }

  add(delta) {
    return this._db.increment(this._key, delta);
  }

  dec(delta = 1) {
    return this._db.increment(this._key, -delta);
  }

  value() {
    const val = this._db.get(this._key);
    return val !== undefined ? Number(val) : 0;
  }

  reset(val = 0) {
    this._db.set(this._key, val);
    return val;
  }
}

class SyncMap {
  constructor(syncDb, name) {
    this._db = syncDb;
    this._prefix = `__map:${name}:`;
  }

  _k(k) {
    return `${this._prefix}${k}`;
  }

  _strip(k) {
    return k.slice(this._prefix.length);
  }

  get(key) {
    return this._db.get(this._k(key));
  }

  set(key, value, options) {
    return this._db.set(this._k(key), value, options);
  }

  has(key) {
    return this._db.has(this._k(key));
  }

  delete(key) {
    return this._db.delete(this._k(key));
  }

  entries() {
    const scan = this._db.storage.scan(this._prefix);
    return scan.map((e) => [this._strip(e.key), e.value]);
  }

  keys() {
    return this._db.storage.keys(this._prefix).map((k) => this._strip(k));
  }

  values() {
    return this._db.storage.scan(this._prefix).map((e) => e.value);
  }

  size() {
    return this._db.storage.keys(this._prefix).length;
  }
}

class SyncSet {
  constructor(syncDb, name) {
    this._db = syncDb;
    this._prefix = `__set:${name}:`;
  }

  _k(m) {
    return `${this._prefix}${m}`;
  }

  _strip(k) {
    return k.slice(this._prefix.length);
  }

  add(member) {
    this._db.set(this._k(member), 1);
    return true;
  }

  delete(member) {
    return this._db.delete(this._k(member));
  }

  has(member) {
    return this._db.has(this._k(member));
  }

  members() {
    return this._db.storage.keys(this._prefix).map((k) => this._strip(k));
  }

  size() {
    return this._db.storage.keys(this._prefix).length;
  }
}

class SyncSorted {
  constructor(syncDb, name) {
    this._db = syncDb;
    this._name = name;
    this._vPrefix = `__sorted_val:${name}:`;
    this._index = new OrderedIndex();
    this._hasValues = false;
  }

  _vKey(member) {
    return `${this._vPrefix}${member}`;
  }

  set(member, scoreOrObj) {
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
    if (value !== undefined) {
      this._hasValues = true;
      this._db.set(this._vKey(member), value);
    }
  }

  score(member) {
    return this._index.score(member);
  }

  rank(member, options) {
    return this._index.rank(member, options);
  }

  incr(member, delta = 1) {
    const cur = this._index.score(member) || 0;
    const next = cur + delta;
    this._index.insert(next, member);
    return next;
  }

  delete(member) {
    const del = this._index.delete(member);
    this._db.delete(this._vKey(member));
    return del;
  }

  top(n = 10) {
    const items = this._index.top(n);
    if (!this._hasValues) {
      return items.map((item) => ({ member: item.member, score: item.score }));
    }
    return items.map((item) => {
      const val = this._db.get(this._vKey(item.member));
      return {
        member: item.member,
        score: item.score,
        ...(val !== undefined ? { value: val } : {})
      };
    });
  }

  bottom(n = 10) {
    const items = this._index.bottom(n);
    if (!this._hasValues) {
      return items.map((item) => ({ member: item.member, score: item.score }));
    }
    return items.map((item) => {
      const val = this._db.get(this._vKey(item.member));
      return {
        member: item.member,
        score: item.score,
        ...(val !== undefined ? { value: val } : {})
      };
    });
  }

  between(min, max, options = {}) {
    return this._index.rangeByScore(min, max, options);
  }

  above(min, options = {}) {
    return this._index.rangeByScore(min, Infinity, options);
  }

  below(max, options = {}) {
    return this._index.rangeByScore(-Infinity, max, options);
  }

  count() {
    return this._index.length;
  }

  size() {
    return this._index.length;
  }
}

export class TencereSync {
  /**
   * @param {string} [dataDir]
   * @param {object} [options={}]
   */
  constructor(dataDir = null, options = {}) {
    this.dataDir = dataDir;
    this.options = options;
    this.storage = new StorageEngine(options);
    this.expiry = new ExpiryManager({
      onExpire: (key) => this._handleExpired(key)
    });
    this.versions = new Map();
    this.isClosed = false;
  }

  _handleExpired(key) {
    this.storage.delete(key);
    this.versions.delete(key);
  }

  set(key, value, options = {}) {
    if (this.isClosed) throw new DatabaseClosedError();

    if (this.expiry.isExpired(key)) {
      this._handleExpired(key);
    }

    const currentVersion = this.versions.get(key) || 0;
    if (options.ifVersion !== undefined && options.ifVersion !== currentVersion) {
      throw new VersionMismatchError(options.ifVersion, currentVersion);
    }

    const nextVersion = currentVersion + 1;
    this.storage.set(key, value, nextVersion);
    this.versions.set(key, nextVersion);

    if (options.ttl) {
      const ttlMs = parseDuration(options.ttl);
      this.expiry.schedule(key, ttlMs, {
        sliding: Boolean(options.sliding),
        consume: Boolean(options.consume)
      });
    } else {
      this.expiry.cancel(key);
    }

    return { ok: true, version: nextVersion };
  }

  get(key, options = {}) {
    if (this.isClosed) throw new DatabaseClosedError();

    if (this.expiry.isExpired(key)) {
      this._handleExpired(key);
      return options.withVersion ? { value: undefined, version: 0 } : undefined;
    }

    const entry = this.storage.get(key);
    if (!entry) {
      return options.withVersion ? { value: undefined, version: 0 } : undefined;
    }

    if (this.expiry.isConsumeOnRead(key)) {
      this.delete(key);
      return options.withVersion ? { value: entry.value, version: entry.version } : entry.value;
    }

    if (options.touch !== false) {
      this.expiry.touch(key);
    }

    if (options.withVersion) {
      return { value: entry.value, version: entry.version };
    }
    return entry.value;
  }

  has(key) {
    if (this.isClosed) throw new DatabaseClosedError();
    if (this.expiry.isExpired(key)) {
      this._handleExpired(key);
      return false;
    }
    return this.storage.has(key);
  }

  delete(key) {
    if (this.isClosed) throw new DatabaseClosedError();
    this.expiry.cancel(key);
    this.versions.delete(key);
    return this.storage.delete(key);
  }

  increment(key, delta = 1) {
    if (this.isClosed) throw new DatabaseClosedError();
    const cur = this.get(key);
    const next = (typeof cur === "number" ? cur : 0) + delta;
    this.set(key, next);
    return next;
  }

  patch(key, patchSpec) {
    if (this.isClosed) throw new DatabaseClosedError();
    const current = this.get(key) || {};
    const obj = { ...current };

    if (patchSpec.$set) {
      for (const [k, v] of Object.entries(patchSpec.$set)) obj[k] = v;
    }
    if (patchSpec.$inc) {
      for (const [k, v] of Object.entries(patchSpec.$inc)) {
        obj[k] = (typeof obj[k] === "number" ? obj[k] : 0) + Number(v);
      }
    }
    if (patchSpec.$unset) {
      const keys = Array.isArray(patchSpec.$unset)
        ? patchSpec.$unset
        : Object.keys(patchSpec.$unset);
      for (const k of keys) delete obj[k];
    }
    this.set(key, obj);
    return obj;
  }

  getMany(keys) {
    if (this.isClosed) throw new DatabaseClosedError();
    const result = {};
    for (const k of keys) {
      result[k] = this.get(k);
    }
    return result;
  }

  setMany(entries, options = {}) {
    if (this.isClosed) throw new DatabaseClosedError();
    const list = Array.isArray(entries) ? entries : Object.entries(entries);
    for (const [k, v] of list) {
      this.set(k, v, options);
    }
  }

  clear() {
    if (this.isClosed) throw new DatabaseClosedError();
    this.storage.clear();
    this.versions.clear();
  }

  stats() {
    const s = this.storage.stats();
    return {
      keys: s.keyCount,
      memoryBytes: s.totalBytes,
      reads: s.reads,
      writes: s.writes,
      deletes: s.deletes
    };
  }

  counter(key) {
    return new SyncCounter(this, key);
  }

  map(name) {
    return new SyncMap(this, name);
  }

  setCollection(name) {
    return new SyncSet(this, name);
  }

  sorted(name) {
    return new SyncSorted(this, name);
  }

  close() {
    this.isClosed = true;
    this.expiry.close();
    this.storage.close();
  }
}
