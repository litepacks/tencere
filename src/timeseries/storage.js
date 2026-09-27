/**
 * Physical storage and indexing for TimeSeries collection.
 * Reuses OrderedIndex (SkipList) for O(log N) temporal indexing and
 * 24-byte compact binary buffers for point storage.
 */

import { OrderedIndex } from "../core/ordered-index.js";
import { PointCodec, POINT_RECORD_SIZE } from "./codec.js";
import { SeriesRegistry } from "./series.js";
import { KeyNotFoundError } from "../errors.js";

function padSeq(seq) {
  const hex = BigInt(seq).toString(16);
  return hex.padStart(16, "0");
}

export class TimeSeriesStorage {
  /**
   * @param {import('../core/engine.js').TencereEngine} engine
   * @param {string} collection
   * @param {object} [options={}]
   */
  constructor(engine, collection, options = {}) {
    this.engine = engine;
    this.collection = collection;
    this.options = options;
    this.registry = new SeriesRegistry(collection, options);

    // seriesId -> OrderedIndex
    this._indexes = new Map();

    // pointId -> Buffer (24-byte point record)
    this._points = new Map();

    // Point metadata: pointId -> { seriesId, tags }
    this._meta = new Map();

    this.oldestTimestamp = null;
    this.newestTimestamp = null;
    this.totalPoints = 0;
  }

  updateOptions(options = {}) {
    if (!options || typeof options !== "object") return;
    this.options = { ...this.options, ...options };
    if (this.registry) {
      this.registry.updateOptions(options);
    }
  }

  _getIndex(seriesId) {
    let index = this._indexes.get(seriesId);
    if (!index) {
      index = new OrderedIndex();
      this._indexes.set(seriesId, index);
    }
    return index;
  }

  /**
   * Stores a single point in memory index and storage.
   *
   * @param {string} seriesId
   * @param {number} timestamp
   * @param {bigint|number} sequence
   * @param {number} value
   * @param {Record<string, string>} [tags]
   * @returns {{ id: string, timestamp: number, sequence: bigint, value: number, tags: Record<string, string> }}
   */
  appendPoint(seriesId, timestamp, sequence, value, tags = {}) {
    const seq = BigInt(sequence);
    const pointId = `${seriesId}:${seq}`;
    const member = `${padSeq(seq)}:${seriesId}`;

    const buf = PointCodec.encode(timestamp, seq, value);
    this._points.set(pointId, buf);
    this._meta.set(pointId, { seriesId, tags: tags || {} });

    // Insert into ordered index: score = timestamp
    const index = this._getIndex(seriesId);
    index.insert(timestamp, member);

    // Track boundary stats
    if (this.oldestTimestamp === null || timestamp < this.oldestTimestamp) {
      this.oldestTimestamp = timestamp;
    }
    if (this.newestTimestamp === null || timestamp > this.newestTimestamp) {
      this.newestTimestamp = timestamp;
    }
    this.totalPoints++;

    // Synchronize to underlying StorageEngine if available
    if (this.engine?.storage) {
      const storageKey = `__ts_pt:${this.collection}:${pointId}`;
      this.engine.storage.set(storageKey, buf);
    }

    return {
      id: pointId,
      timestamp,
      sequence: seq,
      value,
      tags: tags || {}
    };
  }

  /**
   * Appends multiple points in a batch.
   */
  appendMany(points) {
    const results = [];
    for (const pt of points) {
      const res = this.appendPoint(pt.seriesId, pt.timestamp, pt.sequence, pt.value, pt.tags);
      results.push(res);
    }
    return results;
  }

  /**
   * Restores a point during WAL recovery replay.
   */
  restorePoint(seriesId, timestamp, sequence, value, tags) {
    this.registry.resolveSeries(tags);
    return this.appendPoint(seriesId, timestamp, sequence, value, tags);
  }

  /**
   * Fetches a decoded point by its pointId.
   */
  getPoint(pointId) {
    const buf = this._points.get(pointId);
    if (!buf) return undefined;
    const decoded = PointCodec.decode(buf);
    const meta = this._meta.get(pointId) || {};
    return {
      id: pointId,
      timestamp: decoded.timestamp,
      sequence: decoded.sequence,
      value: decoded.value,
      tags: meta.tags || {}
    };
  }

  /**
   * Returns the N latest points across the specified series.
   *
   * @param {string[]} seriesIds
   * @param {number} [count=1]
   * @returns {Array<object>}
   */
  getLatest(seriesIds, count = 1) {
    if (count <= 0 || seriesIds.length === 0) return [];

    const candidates = [];
    for (const sid of seriesIds) {
      const index = this._indexes.get(sid);
      if (!index || index.length === 0) continue;

      // Scan backwards from +Infinity
      const raw = index.rangeByScore(-Infinity, Infinity, { reverse: true, limit: count });
      for (const item of raw) {
        // item.member is `${padSeq(seq)}:${seriesId}`
        const parts = item.member.split(":");
        const seq = BigInt(`0x${parts[0]}`);
        const pointId = `${sid}:${seq}`;
        const pt = this.getPoint(pointId);
        if (pt) candidates.push(pt);
      }
    }

    // Sort descending by timestamp, tie-break by sequence
    candidates.sort((a, b) => {
      if (b.timestamp !== a.timestamp) return b.timestamp - a.timestamp;
      return b.sequence > a.sequence ? 1 : -1;
    });

    return candidates.slice(0, count);
  }

  /**
   * Streaming generator of points within a time window [from, to] across matching series.
   *
   * @param {string[]} seriesIds
   * @param {number} from
   * @param {number} to
   * @param {object} [options={}]
   * @param {boolean} [options.reverse=false]
   * @param {number} [options.limit=Infinity]
   * @param {bigint} [options.maxSequence=null] Optional historical ceiling for db.at()
   * @returns {AsyncGenerator<object>}
   */
  async *iterateRange(seriesIds, from, to, options = {}) {
    const { reverse = false, limit = Infinity, maxSequence = null } = options;
    if (from > to || seriesIds.length === 0 || limit <= 0) return;

    if (seriesIds.length === 1) {
      // Single series fast path
      const index = this._indexes.get(seriesIds[0]);
      if (!index) return;

      const raw = index.rangeByScore(from, to, { reverse });
      let yielded = 0;
      for (const item of raw) {
        if (yielded >= limit) break;
        const parts = item.member.split(":");
        const seq = BigInt(`0x${parts[0]}`);

        // db.at({ sequence: ... }) historical filter
        if (maxSequence !== null && seq > maxSequence) {
          continue;
        }

        const pointId = `${seriesIds[0]}:${seq}`;
        const pt = this.getPoint(pointId);
        if (pt) {
          yield pt;
          yielded++;
        }
      }
      return;
    }

    // Multi-series streaming K-way merge iterator (O(K) memory instead of O(N) allocation)
    const cursors = [];
    for (const sid of seriesIds) {
      const index = this._indexes.get(sid);
      if (!index || index.length === 0) continue;
      const raw = index.rangeByScore(from, to, { reverse });
      if (raw.length === 0) continue;

      cursors.push({
        sid,
        raw,
        idx: 0,
        len: raw.length
      });
    }

    if (cursors.length === 0) return;

    let yielded = 0;
    while (cursors.length > 0 && yielded < limit) {
      // Find the cursor with the earliest (or latest if reverse) point
      let bestIdx = 0;
      let bestItem = cursors[0].raw[cursors[0].idx];
      let bestParts = bestItem.member.split(":");
      let bestSeq = BigInt(`0x${bestParts[0]}`);

      for (let i = 1; i < cursors.length; i++) {
        const curCursor = cursors[i];
        const curItem = curCursor.raw[curCursor.idx];
        const curScore = curItem.score;
        const curParts = curItem.member.split(":");
        const curSeq = BigInt(`0x${curParts[0]}`);

        let isBetter = false;
        if (!reverse) {
          if (curScore < bestItem.score) isBetter = true;
          else if (curScore === bestItem.score && curSeq < bestSeq) isBetter = true;
        } else {
          if (curScore > bestItem.score) isBetter = true;
          else if (curScore === bestItem.score && curSeq > bestSeq) isBetter = true;
        }

        if (isBetter) {
          bestIdx = i;
          bestItem = curItem;
          bestParts = curParts;
          bestSeq = curSeq;
        }
      }

      const chosenCursor = cursors[bestIdx];
      chosenCursor.idx++;
      if (chosenCursor.idx >= chosenCursor.len) {
        cursors.splice(bestIdx, 1);
      }

      if (maxSequence !== null && bestSeq > maxSequence) {
        continue;
      }

      const pointId = `${chosenCursor.sid}:${bestSeq}`;
      const pt = this.getPoint(pointId);
      if (pt) {
        yield pt;
        yielded++;
      }
    }
  }

  /**
   * Deletes points in [from, to] for specified series.
   */
  deleteRange(seriesIds, from, to) {
    let deletedCount = 0;
    for (const sid of seriesIds) {
      const index = this._indexes.get(sid);
      if (!index) continue;
      const raw = index.rangeByScore(from, to);
      for (const item of raw) {
        const parts = item.member.split(":");
        const seq = BigInt(`0x${parts[0]}`);
        const pointId = `${sid}:${seq}`;

        index.delete(item.member);
        this._points.delete(pointId);
        this._meta.delete(pointId);
        if (this.engine?.storage) {
          this.engine.storage.delete(`__ts_pt:${this.collection}:${pointId}`);
        }
        deletedCount++;
        this.totalPoints--;
      }
    }

    if (deletedCount > 0) {
      if (this.totalPoints <= 0) {
        this.totalPoints = 0;
        this.oldestTimestamp = null;
        this.newestTimestamp = null;
      } else {
        let minTs = Infinity;
        let maxTs = -Infinity;
        for (const idx of this._indexes.values()) {
          if (idx.length > 0) {
            const first = idx.header.forward[0];
            if (first && first.score < minTs) minTs = first.score;
            const last = idx.tail;
            if (last && last.score > maxTs) maxTs = last.score;
          }
        }
        this.oldestTimestamp = minTs !== Infinity ? minTs : null;
        this.newestTimestamp = maxTs !== -Infinity ? maxTs : null;
      }
    }

    return deletedCount;
  }

  /**
   * Corrects a point's value by pointId.
   */
  correctPoint(pointId, newValue) {
    const existing = this.getPoint(pointId);
    if (!existing) {
      throw new KeyNotFoundError(`TimeSeries point '${pointId}' not found`);
    }

    const buf = PointCodec.encode(existing.timestamp, existing.sequence, newValue);
    this._points.set(pointId, buf);

    if (this.engine?.storage) {
      this.engine.storage.set(`__ts_pt:${this.collection}:${pointId}`, buf);
    }

    return {
      ...existing,
      value: newValue
    };
  }

  /**
   * Purges points older than a retention cutoff timestamp.
   *
   * @param {number} cutoffTimestamp
   * @returns {number} Count of purged points
   */
  purgeRetention(cutoffTimestamp) {
    const seriesIds = Array.from(this._indexes.keys());
    return this.deleteRange(seriesIds, -Infinity, cutoffTimestamp - 1);
  }

  /**
   * Gathers statistics for this TimeSeries collection.
   */
  stats() {
    let rawBytes = this.totalPoints * POINT_RECORD_SIZE;
    let activeSeries = 0;
    for (const idx of this._indexes.values()) {
      if (idx.length > 0) activeSeries++;
    }
    return {
      points: this.totalPoints,
      series: activeSeries,
      oldestTimestamp: this.totalPoints > 0 ? this.oldestTimestamp : null,
      newestTimestamp: this.totalPoints > 0 ? this.newestTimestamp : null,
      storageBytes: rawBytes,
      retention: this.options.retention || null
    };
  }
}
