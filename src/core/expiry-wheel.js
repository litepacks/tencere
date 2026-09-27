/**
 * High-performance ExpiryManager using a binary min-heap and single adaptive timer.
 * Eliminates per-key JavaScript timers and scales to millions of keys.
 */

import { parseDuration } from "./time.js";
export { parseDuration };


class ExpiryEntry {
  constructor(key, expireAt, ttlMs, sliding = false, consume = false) {
    this.key = key;
    this.expireAt = expireAt;
    this.ttlMs = ttlMs;
    this.sliding = sliding;
    this.consume = consume;
  }
}

export class ExpiryManager {
  /**
   * @param {object} [options={}]
   * @param {function(string): void} [options.onExpire]
   * @param {number} [options.tickIntervalMs=100]
   */
  constructor(options = {}) {
    this.onExpire = options.onExpire || (() => {});
    this.heap = []; // Array of ExpiryEntry
    this.keyMap = new Map(); // key -> ExpiryEntry
    this.keyPos = new Map(); // key -> index in heap
    this.timer = null;
    this.closed = false;
  }

  /**
   * Schedules or updates TTL for a key.
   *
   * @param {string} key
   * @param {number|string} ttl
   * @param {object} [options={}]
   * @param {boolean} [options.sliding=false]
   * @param {boolean} [options.consume=false]
   * @returns {number} expireAt timestamp
   */
  schedule(key, ttl, options = {}) {
    if (this.closed) return 0;
    const ttlMs = parseDuration(ttl);
    if (ttlMs <= 0) {
      this.cancel(key);
      return 0;
    }
    const expireAt = Date.now() + ttlMs;
    const sliding = Boolean(options.sliding);
    const consume = Boolean(options.consume);

    if (this.keyMap.has(key)) {
      const entry = this.keyMap.get(key);
      entry.expireAt = expireAt;
      entry.ttlMs = ttlMs;
      entry.sliding = sliding;
      entry.consume = consume;
      const pos = this.keyPos.get(key);
      this._sink(this._swim(pos));
    } else {
      const entry = new ExpiryEntry(key, expireAt, ttlMs, sliding, consume);
      this.keyMap.set(key, entry);
      const pos = this.heap.length;
      this.heap.push(entry);
      this.keyPos.set(key, pos);
      this._swim(pos);
    }

    this._rescheduleTimer();
    return expireAt;
  }

  get hasActiveExpiries() {
    return this.keyMap.size > 0;
  }

  getEntry(key) {
    return this.keyMap.get(key);
  }

  /**
   * Touches a key if it has sliding TTL, extending its lifetime.
   *
   * @param {string} key
   * @returns {boolean} true if refreshed
   */
  touch(key) {
    if (this.closed || this.keyMap.size === 0) return false;
    const entry = this.keyMap.get(key);
    return this.touchEntry(entry);
  }

  /**
   * Touches an already retrieved entry avoiding another keyMap lookup.
   *
   * @param {ExpiryEntry} entry
   * @returns {boolean}
   */
  touchEntry(entry) {
    if (!entry || !entry.sliding || this.closed) {
      return false;
    }
    entry.expireAt = Date.now() + entry.ttlMs;
    const pos = this.keyPos.get(entry.key);
    if (pos !== undefined) {
      this._sink(this._swim(pos));
      this._rescheduleTimer();
    }
    return true;
  }

  /**
   * Checks if an entry is marked consume-on-read.
   *
   * @param {string} key
   * @returns {boolean}
   */
  isConsumeOnRead(key) {
    if (this.keyMap.size === 0) return false;
    const entry = this.keyMap.get(key);
    return entry ? entry.consume : false;
  }

  /**
   * Checks if a key is expired right now.
   *
   * @param {string} key
   * @param {number} [now]
   * @returns {boolean}
   */
  isExpired(key, now) {
    if (this.keyMap.size === 0) return false;
    const entry = this.keyMap.get(key);
    if (!entry) return false;
    return entry.expireAt <= (now !== undefined ? now : Date.now());
  }

  /**
   * Cancels expiration for a key (e.g. on manual delete).
   *
   * @param {string} key
   */
  cancel(key) {
    if (this.keyMap.size === 0) return;
    const pos = this.keyPos.get(key);
    if (pos === undefined) return;
    this.keyMap.delete(key);
    this.keyPos.delete(key);

    const last = this.heap.pop();
    if (pos < this.heap.length && last) {
      this.heap[pos] = last;
      this.keyPos.set(last.key, pos);
      this._sink(this._swim(pos));
    }
    this._rescheduleTimer();
  }

  /**
   * Returns current TTL remaining in milliseconds (or -1 if no TTL, -2 if not found).
   *
   * @param {string} key
   * @returns {number}
   */
  ttl(key) {
    const entry = this.keyMap.get(key);
    if (!entry) return -1;
    const rem = entry.expireAt - Date.now();
    return rem > 0 ? rem : 0;
  }

  /**
   * Process all expired keys up to now.
   */
  purgeExpired(now = Date.now()) {
    const expired = [];
    while (this.heap.length > 0 && this.heap[0].expireAt <= now) {
      const top = this.heap[0];
      expired.push(top.key);
      this.cancel(top.key);
    }
    for (const key of expired) {
      this.onExpire(key);
    }
    return expired;
  }

  _rescheduleTimer() {
    if (this.closed) return;
    if (this.timer) {
      clearTimeout(this.timer);
      this.timer = null;
    }
    if (this.heap.length === 0) return;

    const nextExpiry = this.heap[0].expireAt;
    const delay = Math.max(1, Math.min(nextExpiry - Date.now(), 2147483647));

    this.timer = setTimeout(() => {
      this.timer = null;
      this.purgeExpired();
      this._rescheduleTimer();
    }, delay);

    // Unref timer so it doesn't prevent Node process exit
    if (this.timer && typeof this.timer.unref === "function") {
      this.timer.unref();
    }
  }

  _swap(i, j) {
    const a = this.heap[i];
    const b = this.heap[j];
    this.heap[i] = b;
    this.heap[j] = a;
    this.keyPos.set(b.key, i);
    this.keyPos.set(a.key, j);
  }

  _swim(pos) {
    while (pos > 0) {
      const parent = (pos - 1) >> 1;
      if (this.heap[pos].expireAt < this.heap[parent].expireAt) {
        this._swap(pos, parent);
        pos = parent;
      } else {
        break;
      }
    }
    return pos;
  }

  _sink(pos) {
    const len = this.heap.length;
    while ((pos << 1) + 1 < len) {
      let left = (pos << 1) + 1;
      let right = left + 1;
      let best = pos;

      if (this.heap[left].expireAt < this.heap[best].expireAt) {
        best = left;
      }
      if (right < len && this.heap[right].expireAt < this.heap[best].expireAt) {
        best = right;
      }
      if (best !== pos) {
        this._swap(pos, best);
        pos = best;
      } else {
        break;
      }
    }
    return pos;
  }

  close() {
    this.closed = true;
    if (this.timer) {
      clearTimeout(this.timer);
      this.timer = null;
    }
    this.heap.length = 0;
    this.keyMap.clear();
    this.keyPos.clear();
  }
}
