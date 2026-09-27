/**
 * TimeSeries collection: composable time-stamped metric measurements with
 * fluent query builder, compact binary storage, and deterministic ordering.
 */

import { TimeSeriesStorage } from "../timeseries/storage.js";
import { TimeSeriesQuery } from "../timeseries/query.js";
import { parseTimePoint, parseDuration } from "../core/time.js";
import { InvalidTimeSeriesValueError, DatabaseClosedError } from "../errors.js";
import {
  Operation,
  OP_TS_ADD,
  OP_TS_DELETE,
  OP_TS_CORRECT,
  OP_TS_BATCH
} from "../core/operations.js";

export class TimeSeriesCollection {
  /**
   * @param {import('../core/engine.js').TencereEngine} engine
   * @param {string} name
   * @param {object} [options={}]
   * @param {string|number} [options.retention] e.g. '30d', '7d'
   * @param {object} [options.limits] e.g. { maxSeries, maxTags, maxTagKeyLength, maxTagValueLength }
   * @param {object} [options.tags] e.g. { indexed: ['method', 'status'] }
   */
  constructor(engine, name, options = {}) {
    this._engine = engine;
    this._name = name;
    this.options = options;

    if (!engine._timeSeriesStores) {
      engine._timeSeriesStores = new Map();
    }
    if (!engine._timeSeriesStores.has(name)) {
      engine._timeSeriesStores.set(name, new TimeSeriesStorage(engine, name, options));
    } else if (options && Object.keys(options).length > 0) {
      engine._timeSeriesStores.get(name).updateOptions(options);
    }
    this._storage = engine._timeSeriesStores.get(name);
    this._eventPrefix = `timeseries:${name}`;
  }

  _checkClosed() {
    if (this._engine.isClosed) {
      throw new DatabaseClosedError();
    }
  }

  /**
   * Appends a new timestamped measurement.
   *
   * @param {number} value - Finite numeric value
   * @param {object} [options={}]
   * @param {string|number|Date} [options.at] - Explicit timestamp (defaults to Date.now())
   * @param {Record<string, string>} [options.tags] - Low-cardinality tags
   * @returns {Promise<{ id: string, timestamp: number, sequence: bigint, value: number, tags: Record<string, string> }>}
   */
  async add(value, options = {}) {
    this._checkClosed();

    if (typeof value !== "number" || !Number.isFinite(value)) {
      throw new InvalidTimeSeriesValueError(
        `TimeSeries value must be a finite number, received: ${value}`
      );
    }

    const timestamp = options.at !== undefined ? parseTimePoint(options.at) : Date.now();
    const { seriesId, normalizedTags } = this._storage.registry.resolveSeries(options.tags);

    const seq = BigInt(++this._engine._sequenceCounter);

    // Canonical mutation
    if (this._engine.daktilo || (this._engine.cluster && this._engine.cluster.enabled)) {
      const partition = this._engine.partitions.getPartition(`__ts:${this._name}:${seriesId}`);
      const op = new Operation({
        op: OP_TS_ADD,
        partition,
        key: `__ts:${this._name}:${seriesId}`,
        timestamp,
        version: seq,
        value,
        extra: {
          collection: this._name,
          seriesId,
          tags: normalizedTags
        }
      });
      if (this._engine.daktilo) {
        await this._engine._logOperation(op);
      }
      await this._engine._replicateCluster(op, options);
    }

    const point = this._storage.appendPoint(seriesId, timestamp, seq, value, normalizedTags);

    // Emit live change event for real-time watchers
    this._engine.events.emit(this._eventPrefix, point);
    this._engine.events.emit(`${this._eventPrefix}:${seriesId}`, point);

    return point;
  }

  /**
   * Efficient batch ingestion of multiple measurement samples.
   *
   * @param {Array<[number, number]|{ at?: number|string|Date, value: number, tags?: Record<string, string> }>} items
   * @returns {Promise<Array<object>>}
   */
  async addMany(items) {
    this._checkClosed();
    if (!Array.isArray(items) || items.length === 0) return [];

    const prepared = [];
    const now = Date.now();

    for (let i = 0; i < items.length; i++) {
      const item = items[i];
      let val;
      let at = now + i;
      let tags = null;

      if (Array.isArray(item)) {
        at = parseTimePoint(item[0]);
        val = item[1];
      } else if (typeof item === "object" && item !== null) {
        val = item.value;
        if (item.at !== undefined) at = parseTimePoint(item.at);
        tags = item.tags || null;
      } else {
        val = item;
      }

      if (typeof val !== "number" || !Number.isFinite(val)) {
        throw new InvalidTimeSeriesValueError(
          `TimeSeries value at index ${i} must be a finite number, received: ${val}`
        );
      }

      const { seriesId, normalizedTags } = this._storage.registry.resolveSeries(tags);
      const seq = BigInt(++this._engine._sequenceCounter);

      prepared.push({
        seriesId,
        timestamp: at,
        sequence: seq,
        value: val,
        tags: normalizedTags
      });
    }

    // Canonical batch mutation
    if (this._engine.daktilo || (this._engine.cluster && this._engine.cluster.enabled)) {
      const partition = this._engine.partitions.getPartition(`__ts:${this._name}`);
      const op = new Operation({
        op: OP_TS_BATCH,
        partition,
        key: `__ts:${this._name}`,
        timestamp: now,
        version: prepared[0].sequence,
        extra: {
          collection: this._name,
          points: prepared
        }
      });
      if (this._engine.daktilo) {
        await this._engine._logOperation(op);
      }
      await this._engine._replicateCluster(op);
    }

    const points = this._storage.appendMany(prepared);

    for (const point of points) {
      this._engine.events.emit(this._eventPrefix, point);
      this._engine.events.emit(`${this._eventPrefix}:${point.tags ? point.seriesId : 'default'}`, point);
    }

    return points;
  }

  /**
   * Retrieves the most recent measurement(s).
   *
   * @param {number} [count=1]
   * @returns {Promise<object|null|Array<object>>}
   */
  async latest(count = 1) {
    this._checkClosed();
    const seriesIds = Array.from(this._storage._indexes.keys());
    if (seriesIds.length === 0) return count > 1 ? [] : null;

    const list = this._storage.getLatest(seriesIds, count);
    if (count > 1) {
      return list;
    }
    return list.length > 0 ? list[0] : null;
  }

  /**
   * Starts a fluent query bounded between [from, to].
   */
  between(from, to) {
    return new TimeSeriesQuery(this).between(from, to);
  }

  /**
   * Starts a fluent query filtered by tags.
   */
  where(tags) {
    return new TimeSeriesQuery(this).where(tags);
  }

  /**
   * Starts a fluent query bucketed by an interval.
   */
  bucket(interval, options) {
    return new TimeSeriesQuery(this).bucket(interval, options);
  }

  /**
   * Materializes all points in collection.
   */
  async values() {
    return new TimeSeriesQuery(this).values();
  }

  /**
   * Streams all points in collection.
   */
  iterate() {
    return new TimeSeriesQuery(this).iterate();
  }

  // ---------------- Aggregations over all points ----------------

  async count() {
    return new TimeSeriesQuery(this).count();
  }

  async sum() {
    return new TimeSeriesQuery(this).sum();
  }

  async min() {
    return new TimeSeriesQuery(this).min();
  }

  async max() {
    return new TimeSeriesQuery(this).max();
  }

  async avg() {
    return new TimeSeriesQuery(this).avg();
  }

  async first() {
    return new TimeSeriesQuery(this).first();
  }

  async last() {
    return new TimeSeriesQuery(this).last();
  }

  /**
   * Real-time reactive stream of points.
   *
   * @param {object} [options={}]
   * @param {Record<string, string>} [options.where]
   * @returns {AsyncGenerator<object>}
   */
  async *watch(options = {}) {
    const queue = [];
    let notify = null;
    let closed = false;

    const filterTags = options.where || null;

    const listener = (point) => {
      if (closed) return;
      if (filterTags) {
        for (const [k, v] of Object.entries(filterTags)) {
          if (!point.tags || point.tags[k] !== String(v)) {
            return;
          }
        }
      }
      queue.push(point);
      if (notify) {
        const fn = notify;
        notify = null;
        fn();
      }
    };

    this._engine.events.on(this._eventPrefix, listener);

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
      this._engine.events.off(this._eventPrefix, listener);
    }
  }

  /**
   * Deletes points in a specified time window.
   *
   * @param {object} range
   * @param {string|number|Date} [range.from=-Infinity]
   * @param {string|number|Date} [range.to=Infinity]
   * @param {Record<string, string>} [range.tags]
   * @returns {Promise<{ deleted: number }>}
   */
  async delete(range = {}) {
    this._checkClosed();
    const from = range.from !== undefined ? parseTimePoint(range.from) : -Infinity;
    const to = range.to !== undefined ? parseTimePoint(range.to) : Infinity;

    const seriesIds = this._storage.registry.matchSeries(range.tags);

    if (this._engine.daktilo) {
      const partition = this._engine.partitions.getPartition(`__ts:${this._name}`);
      const op = new Operation({
        op: OP_TS_DELETE,
        partition,
        key: `__ts:${this._name}`,
        timestamp: Date.now(),
        version: BigInt(++this._engine._sequenceCounter),
        extra: {
          collection: this._name,
          seriesIds,
          from,
          to
        }
      });
      await this._engine._logOperation(op);
    }

    const deleted = this._storage.deleteRange(seriesIds, from, to);
    return { deleted };
  }

  /**
   * Corrects the value of an existing point by its ID.
   *
   * @param {string} pointId
   * @param {object} updates
   * @param {number} updates.value
   * @returns {Promise<object>}
   */
  async correct(pointId, updates = {}) {
    this._checkClosed();
    const newValue = updates.value;
    if (typeof newValue !== "number" || !Number.isFinite(newValue)) {
      throw new InvalidTimeSeriesValueError("Updated value must be a finite number");
    }

    if (this._engine.daktilo) {
      const partition = this._engine.partitions.getPartition(`__ts:${this._name}`);
      const op = new Operation({
        op: OP_TS_CORRECT,
        partition,
        key: `__ts:${this._name}`,
        timestamp: Date.now(),
        version: BigInt(++this._engine._sequenceCounter),
        value: newValue,
        extra: {
          collection: this._name,
          pointId
        }
      });
      await this._engine._logOperation(op);
    }

    return this._storage.correctPoint(pointId, newValue);
  }

  /**
   * Purges samples older than the retention boundary.
   *
   * @returns {Promise<{ purged: number }>}
   */
  async purgeRetention() {
    this._checkClosed();
    const retention = this.options.retention;
    if (!retention) return { purged: 0 };

    const retentionMs = parseDuration(retention);
    if (retentionMs <= 0) return { purged: 0 };

    const cutoff = Date.now() - retentionMs;
    const purged = this._storage.purgeRetention(cutoff);
    return { purged };
  }

  /**
   * Returns metadata and storage statistics for this TimeSeries collection.
   *
   * @returns {Promise<object>}
   */
  async stats() {
    this._checkClosed();
    return this._storage.stats();
  }
}
