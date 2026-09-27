/**
 * HistoryManager: Central coordinator for Tencere's canonical mutation history,
 * version indexing, retention boundaries, coordination audit log, and time travel.
 */

import { HistoryConfig, parseHistoricalPoint } from "./config.js";
import { OP_NAMES, OP_DEL, OP_EXPIRE, CAPABILITIES } from "./types.js";
import {
  HistoryDisabledError,
  HistoryUnavailableError,
  TencereError
} from "../errors.js";

class RevisionEntry {
  constructor(seq, ver, prevVer, op, ts, ttlMs, expiresAt, size, value, extra) {
    this.sequence = seq;
    this.version = ver;
    this.prevVersion = prevVer;
    this.op = op;
    this.timestamp = ts;
    this.ttlMs = ttlMs;
    this.expiresAt = expiresAt;
    this.size = size;
    this.value = value;
    this.extra = extra;
  }
}

function findLatestAtOrBeforeSeq(revisions, targetSeq) {
  let low = 0;
  let high = revisions.length - 1;
  let result = null;
  while (low <= high) {
    const mid = (low + high) >> 1;
    if (revisions[mid].sequence <= targetSeq) {
      result = revisions[mid];
      low = mid + 1;
    } else {
      high = mid - 1;
    }
  }
  return result;
}

function findLatestAtOrBeforeVer(revisions, targetVer) {
  let low = 0;
  let high = revisions.length - 1;
  let result = null;
  while (low <= high) {
    const mid = (low + high) >> 1;
    if (revisions[mid].version <= targetVer) {
      result = revisions[mid];
      low = mid + 1;
    } else {
      high = mid - 1;
    }
  }
  return result;
}

function findLatestAtOrBeforeTs(revisions, targetTs) {
  let low = 0;
  let high = revisions.length - 1;
  let result = null;
  while (low <= high) {
    const mid = (low + high) >> 1;
    if (revisions[mid].timestamp <= targetTs) {
      result = revisions[mid];
      low = mid + 1;
    } else {
      high = mid - 1;
    }
  }
  return result;
}

export class HistoryManager {
  /**
   * @param {import('../core/engine.js').TencereEngine} engine
   * @param {boolean|object} [options=false]
   */
  constructor(engine, options = false) {
    this.engine = engine;
    this.config = new HistoryConfig(options);
    this.enabled = this.config.enabled;

    // Sparse binary / in-memory version index: key -> Array<RevisionEntry>
    this._keyHistory = new Map();

    // Active snapshot retention pins: snapshotId -> { id, sequence, timestamp, close }
    this._activeSnapshots = new Map();
    this._snapshotCounter = 0;

    // Coordination event audit log: ring buffer
    this._coordinationEvents = [];
    this._maxCoordinationEvents = 2000;
  }

  get totalHistoryEntries() {
    let sum = 0;
    for (const revs of this._keyHistory.values()) {
      sum += revs.length;
    }
    return sum;
  }

  /**
   * Sets collection override on the history config.
   */
  setCollectionOverride(collectionName, override) {
    this.config.setCollectionOverride(collectionName, override);
  }

  /**
   * Fast-path check: should mutation on this key be tracked?
   */
  shouldTrack(key, collectionName = null) {
    if (!this.enabled) return false;
    return this.config.shouldTrackKey(key, collectionName);
  }

  /**
   * Records a canonical mutation into the history index.
   *
   * @param {object} params
   * @param {bigint} params.sequence - Daktilo sequence
   * @param {string} params.key - Key or target
   * @param {number} params.op - OpCode
   * @param {bigint} [params.version] - Version (defaults to sequence)
   * @param {bigint} [params.prevVersion]
   * @param {number} [params.timestamp]
   * @param {number} [params.ttlMs=0]
   * @param {any} [params.value]
   * @param {object|null} [params.extra=null]
   * @param {number} [params.size=0]
   * @param {string|null} [params.collectionName=null]
   */
  record(params) {
    if (!this.enabled) return;
    if (!this.config.shouldTrackKey(params.key, params.collectionName)) return;

    const seq = BigInt(params.sequence);
    const ver = params.version !== undefined ? BigInt(params.version) : seq;
    const prevVer = params.prevVersion !== undefined && params.prevVersion !== null
      ? BigInt(params.prevVersion)
      : 0n;
    const ts = params.timestamp || Date.now();
    const ttlMs = params.ttlMs || 0;
    const expiresAt = ttlMs > 0 ? ts + ttlMs : 0;

    let revisions = this._keyHistory.get(params.key);
    if (!revisions) {
      revisions = [];
      this._keyHistory.set(params.key, revisions);
    }

    const entry = new RevisionEntry(
      seq,
      ver,
      prevVer,
      params.op,
      ts,
      ttlMs,
      expiresAt,
      params.size || 0,
      params.value,
      params.extra || null
    );

    revisions.push(entry);

    // Enforce maxVersions retention per key
    const max = this.config.maxVersions;
    if (max > 0 && revisions.length > max) {
      // Check if oldest is pinned by active snapshot
      const minPinnedSeq = this._getMinSnapshotSequence();
      while (revisions.length > max) {
        if (minPinnedSeq !== null && revisions[0].sequence >= minPinnedSeq) {
          // Cannot discard because an active snapshot requires it
          break;
        }
        revisions.shift();
      }
    }
  }

  /**
   * Records a coordination event (locks, semaphores, rate-limits, etc.) for auditing.
   */
  recordCoordination({ type, key, op, sequence = 0n, timestamp = Date.now(), details = null }) {
    if (!this.enabled) return;

    const event = {
      type,
      key,
      op,
      opName: OP_NAMES[op] || String(op),
      sequence: BigInt(sequence),
      timestamp,
      details
    };

    this._coordinationEvents.push(event);
    if (this._coordinationEvents.length > this._maxCoordinationEvents) {
      this._coordinationEvents.shift();
    }
  }

  /**
   * Retrieves coordination history for auditing.
   *
   * @param {object} [filter={}]
   * @param {string} [filter.type]
   * @param {string} [filter.key]
   * @param {number} [filter.limit=100]
   * @returns {Array<object>}
   */
  getCoordinationHistory(filter = {}) {
    let list = this._coordinationEvents;
    if (filter.type) {
      list = list.filter((e) => e.type === filter.type);
    }
    if (filter.key) {
      list = list.filter((e) => e.key === filter.key);
    }
    const limit = filter.limit || 100;
    return list.slice(-limit).reverse();
  }

  /**
   * Calculates the minimum sequence retained across history and active snapshots.
   * Any Daktilo log segment strictly older than this boundary may be safely compacted.
   *
   * @returns {bigint}
   */
  getMinimumRetainedSequence() {
    let minSeq = null;

    // Check active snapshots
    const minSnap = this._getMinSnapshotSequence();
    if (minSnap !== null) {
      minSeq = minSnap;
    }

    // Check key history boundaries
    for (const revisions of this._keyHistory.values()) {
      if (revisions.length > 0) {
        const first = revisions[0].sequence;
        if (minSeq === null || first < minSeq) {
          minSeq = first;
        }
      }
    }

    return minSeq !== null ? minSeq : 1n;
  }

  /**
   * Purges expired historical revisions, strictly preserving any revisions pinned by active snapshots.
   *
   * @returns {number} number of revisions purged
   */
  purgeExpiredRevisions() {
    if (!this.enabled) return 0;
    const now = Date.now();
    const retentionMs = this.config.retentionMs;
    const minPinnedSeq = this._getMinSnapshotSequence();
    let purged = 0;

    for (const revisions of this._keyHistory.values()) {
      if (!revisions || revisions.length <= 1) continue;

      let cutIdx = 0;
      for (let i = 0; i < revisions.length - 1; i++) {
        const rev = revisions[i];
        if (minPinnedSeq !== null && rev.sequence >= minPinnedSeq) {
          break;
        }
        const nextRev = revisions[i + 1];
        if (minPinnedSeq !== null && nextRev && nextRev.sequence > minPinnedSeq) {
          // rev is visible at minPinnedSeq, pin it
          break;
        }
        if (retentionMs > 0 && (now - rev.timestamp) > retentionMs) {
          cutIdx = i + 1;
        } else {
          break;
        }
      }

      if (cutIdx > 0) {
        revisions.splice(0, cutIdx);
        purged += cutIdx;
      }
    }
    return purged;
  }

  _getMinSnapshotSequence() {
    if (this._activeSnapshots.size === 0) return null;
    let min = null;
    for (const snap of this._activeSnapshots.values()) {
      if (min === null || snap.sequence < min) {
        min = snap.sequence;
      }
    }
    return min;
  }

  /**
   * Registers an active snapshot pin.
   *
   * @param {bigint} sequence
   * @param {number} timestamp
   * @returns {{ id: string, sequence: bigint, timestamp: number, close: () => void }}
   */
  registerSnapshot(sequence, timestamp) {
    const id = `snap_${Date.now()}_${++this._snapshotCounter}`;
    const handle = {
      id,
      sequence: BigInt(sequence),
      timestamp,
      close: () => {
        this._activeSnapshots.delete(id);
      }
    };
    this._activeSnapshots.set(id, handle);
    return handle;
  }

  /**
   * Unregisters an active snapshot pin.
   *
   * @param {string} id
   */
  unregisterSnapshot(id) {
    this._activeSnapshots.delete(id);
  }

  /**
   * Queries history revisions for a specific key.
   *
   * @param {string} key
   * @param {object} [options={}]
   * @param {string|number|Date} [options.from]
   * @param {string|number|Date} [options.to]
   * @param {number} [options.limit]
   * @param {'desc'|'asc'} [options.direction='desc']
   * @returns {AsyncGenerator<object>}
   */
  async *history(key, options = {}) {
    if (!this.enabled) {
      throw new HistoryDisabledError();
    }

    const revisions = this._keyHistory.get(key) || [];
    const fromTs = options.from ? parseHistoricalPoint(options.from) : null;
    const toTs = options.to ? parseHistoricalPoint(options.to) : null;
    const limit = options.limit && options.limit > 0 ? options.limit : Infinity;
    const direction = options.direction === "asc" ? "asc" : "desc";

    let filtered = revisions.slice();
    if (fromTs !== null) {
      filtered = filtered.filter((r) => r.timestamp >= fromTs);
    }
    if (toTs !== null) {
      filtered = filtered.filter((r) => r.timestamp <= toTs);
    }

    if (direction === "desc") {
      filtered.reverse();
    }

    let yielded = 0;
    for (const rev of filtered) {
      if (yielded >= limit) break;
      yielded++;

      yield {
        sequence: rev.sequence,
        operation: OP_NAMES[rev.op] || String(rev.op),
        version: rev.version,
        prevVersion: rev.prevVersion,
        timestamp: rev.timestamp,
        ttlMs: rev.ttlMs,
        size: rev.size,
        get value() {
          return rev.value;
        }
      };
    }
  }

  /**
   * Reconstructs the exact state of a key at a historical target selector.
   * Selectors: { at: '1h ago' | ISO | timestamp }, { atSequence: 100n }, or { version: 42n }.
   *
   * @param {string} key
   * @param {object} [selector={}]
   * @param {string|number|Date} [selector.at]
   * @param {bigint|number} [selector.atSequence]
   * @param {bigint|number} [selector.version]
   * @returns {{ exists: boolean, value: any, version?: bigint, sequence?: bigint, expired?: boolean }}
   */
  getHistoricalEntry(key, selector = {}) {
    if (!this.enabled) {
      throw new HistoryDisabledError();
    }

    const at = selector.at !== undefined ? selector.at : selector.to;
    const atSequence = selector.atSequence !== undefined ? selector.atSequence : selector.sequence;
    const version = selector.version;

    // Detect conflicting selectors
    let selectorCount = 0;
    if (at !== undefined) selectorCount++;
    if (atSequence !== undefined) selectorCount++;
    if (version !== undefined) selectorCount++;

    if (selectorCount > 1) {
      throw new TencereError(
        "Conflicting historical selectors: specify at most one of 'at', 'atSequence', or 'version'",
        "ERR_CONFLICTING_SELECTORS"
      );
    }

    const revisions = this._keyHistory.get(key);
    if (!revisions || revisions.length === 0) {
      return { exists: false, value: undefined };
    }

    let targetRev = null;
    let queryTimestamp = null;

    if (atSequence !== undefined) {
      const targetSeq = BigInt(atSequence);
      targetRev = findLatestAtOrBeforeSeq(revisions, targetSeq);
      if (targetRev) queryTimestamp = targetRev.timestamp;
    } else if (version !== undefined) {
      const targetVer = BigInt(version);
      targetRev = findLatestAtOrBeforeVer(revisions, targetVer);
      if (targetRev) queryTimestamp = targetRev.timestamp;
    } else if (at !== undefined) {
      const targetTs = parseHistoricalPoint(at);
      queryTimestamp = targetTs;
      targetRev = findLatestAtOrBeforeTs(revisions, targetTs);
    } else {
      // Default: latest revision
      targetRev = revisions[revisions.length - 1];
      queryTimestamp = targetRev.timestamp;
    }

    if (!targetRev) {
      // Key was not yet created at requested point
      return { exists: false, value: undefined };
    }

    // Check if revision represents a deletion
    if (targetRev.op === OP_DEL) {
      return { exists: false, value: undefined, version: targetRev.version, sequence: targetRev.sequence };
    }

    // Check historical TTL expiration
    // Distinguish mutation timestamp from historical query timestamp!
    if (targetRev.expiresAt > 0 && queryTimestamp !== null) {
      if (queryTimestamp >= targetRev.expiresAt) {
        return { exists: false, value: undefined, expired: true, version: targetRev.version, sequence: targetRev.sequence };
      }
    }

    return {
      exists: true,
      value: targetRev.value,
      version: targetRev.version,
      sequence: targetRev.sequence
    };
  }

  /**
   * Returns all keys that existed and were alive at the requested selector.
   *
   * @param {string} [prefix=""]
   * @param {object} [selector={}]
   * @returns {string[]}
   */
  keysAt(prefix = "", selector = {}) {
    const aliveKeys = [];
    for (const key of this._keyHistory.keys()) {
      if (prefix && !key.startsWith(prefix)) {
        continue;
      }
      const entry = this.getHistoricalEntry(key, selector);
      if (entry.exists) {
        aliveKeys.push(key);
      }
    }
    return aliveKeys;
  }

  /**
   * Resolves the maximum mutation sequence effective at the requested selector.
   *
   * @param {object} [selector={}]
   * @returns {bigint|null}
   */
  getSequenceAt(selector = {}) {
    if (selector.atSequence !== undefined) {
      return BigInt(selector.atSequence);
    }
    if (selector.sequence !== undefined) {
      return BigInt(selector.sequence);
    }
    if (selector.version !== undefined) {
      return BigInt(selector.version);
    }
    const at = selector.at !== undefined ? selector.at : selector.to;
    if (at !== undefined) {
      const targetTs = parseHistoricalPoint(at);
      let maxSeq = 0n;
      for (const revs of this._keyHistory.values()) {
        const rev = findLatestAtOrBeforeTs(revs, targetTs);
        if (rev && rev.sequence > maxSeq) {
          maxSeq = rev.sequence;
        }
      }
      return maxSeq;
    }
    return this.engine._sequenceCounter ? BigInt(this.engine._sequenceCounter) : null;
  }
}
