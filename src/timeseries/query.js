/**
 * Fluent, immutable query builder and streaming aggregation engine for TimeSeries.
 */

import { parseTimePoint, parseDuration, alignToBucket } from "../core/time.js";

export class TimeSeriesQuery {
  /**
   * @param {import('../collections/timeseries.js').TimeSeriesCollection|import('./storage.js').TimeSeriesStorage} target
   * @param {object} [state={}]
   */
  constructor(target, state = {}) {
    this._target = target;
    this._tags = state.tags ? { ...state.tags } : null;
    this._from = state.from !== undefined ? state.from : -Infinity;
    this._to = state.to !== undefined ? state.to : Infinity;
    this._bucket = state.bucket || null;
    this._limit = state.limit || Infinity;
    this._reverse = state.reverse || false;
    this._maxSequence = state.maxSequence || null;
    this._includeEmpty = state.includeEmpty || false;
  }

  _clone(updates = {}) {
    return new TimeSeriesQuery(this._target, {
      tags: this._tags,
      from: this._from,
      to: this._to,
      bucket: this._bucket,
      limit: this._limit,
      reverse: this._reverse,
      maxSequence: this._maxSequence,
      includeEmpty: this._includeEmpty,
      ...updates
    });
  }

  /**
   * Filters points by low-cardinality tags.
   *
   * @param {Record<string, string>} tags
   * @returns {TimeSeriesQuery}
   */
  where(tags) {
    return this._clone({
      tags: { ...(this._tags || {}), ...tags }
    });
  }

  /**
   * Sets the query time window [from, to]. Supports relative strings like '1h ago', 'now', ISO, or ms.
   *
   * @param {string|number|Date} from
   * @param {string|number|Date} to
   * @returns {TimeSeriesQuery}
   */
  between(from, to) {
    return this._clone({
      from: parseTimePoint(from),
      to: parseTimePoint(to)
    });
  }

  /**
   * Partitions the query into fixed-duration buckets (e.g. '1m', '5m', '1h', 60000).
   *
   * @param {string|number} interval
   * @param {object} [options={}]
   * @param {boolean} [options.includeEmpty=false]
   * @returns {TimeSeriesQuery}
   */
  bucket(interval, options = {}) {
    const bucketMs = typeof interval === "number" ? interval : parseDuration(interval);
    return this._clone({
      bucket: bucketMs,
      includeEmpty: options.includeEmpty || false
    });
  }

  /**
   * Limits the number of returned points or buckets.
   *
   * @param {number} n
   * @returns {TimeSeriesQuery}
   */
  limit(n) {
    return this._clone({ limit: n });
  }

  asc() {
    return this._clone({ reverse: false });
  }

  desc() {
    return this._clone({ reverse: true });
  }

  _getStorage() {
    return this._target._storage || this._target;
  }

  /**
   * Resolves matching series IDs.
   *
   * @returns {string[]}
   */
  _resolveSeriesIds() {
    const storage = this._getStorage();
    return storage.registry.matchSeries(this._tags);
  }

  /**
   * Streaming async iterator of points.
   *
   * @returns {AsyncGenerator<object>}
   */
  async *iterate() {
    const storage = this._getStorage();
    const seriesIds = this._resolveSeriesIds();
    yield* storage.iterateRange(seriesIds, this._from, this._to, {
      reverse: this._reverse,
      limit: this._limit,
      maxSequence: this._maxSequence
    });
  }

  /**
   * Materializes all points into an array.
   *
   * @returns {Promise<Array<object>>}
   */
  async values() {
    const list = [];
    for await (const pt of this.iterate()) {
      list.push(pt);
    }
    return list;
  }

  /**
   * Alias for values().
   */
  async points() {
    return this.values();
  }

  /**
   * Computes aggregation over points, either scalar or bucketed.
   *
   * @param {'count'|'sum'|'min'|'max'|'avg'|'first'|'last'} type
   * @returns {Promise<number|null|Array<{ start: number, end: number, value: number, count: number }>>}
   */
  async _aggregate(type) {
    if (!this._bucket) {
      // Scalar aggregation over entire window
      let count = 0;
      let sum = 0;
      let min = Infinity;
      let max = -Infinity;
      let first = undefined;
      let last = undefined;

      for await (const pt of this.iterate()) {
        const val = pt.value;
        count++;
        sum += val;
        if (val < min) min = val;
        if (val > max) max = val;
        if (first === undefined) first = val;
        last = val;
      }

      if (count === 0) {
        if (type === "count") return 0;
        return null;
      }

      switch (type) {
        case "count": return count;
        case "sum": return sum;
        case "min": return min;
        case "max": return max;
        case "avg": return sum / count;
        case "first": return this._reverse ? last : first;
        case "last": return this._reverse ? first : last;
        default: return null;
      }
    }

    // Bucketed aggregation
    const bucketMs = this._bucket;
    const buckets = [];
    let currentBucket = null;

    const finalizeBucket = (b) => {
      if (!b) return;
      let aggVal = null;
      if (b.count > 0) {
        switch (type) {
          case "count": aggVal = b.count; break;
          case "sum": aggVal = b.sum; break;
          case "min": aggVal = b.min; break;
          case "max": aggVal = b.max; break;
          case "avg": aggVal = b.sum / b.count; break;
          case "first": aggVal = this._reverse ? b.last : b.first; break;
          case "last": aggVal = this._reverse ? b.first : b.last; break;
        }
      } else if (type === "count") {
        aggVal = 0;
      }

      buckets.push({
        start: b.start,
        end: b.end,
        value: aggVal,
        count: b.count
      });
    };

    for await (const pt of this.iterate()) {
      const bStart = alignToBucket(pt.timestamp, bucketMs);
      const bEnd = bStart + bucketMs;

      if (!currentBucket || currentBucket.start !== bStart) {
        if (currentBucket) {
          finalizeBucket(currentBucket);
          if (this._includeEmpty) {
            if (!this._reverse) {
              for (let gap = currentBucket.start + bucketMs; gap < bStart; gap += bucketMs) {
                if (buckets.length >= this._limit) break;
                buckets.push({
                  start: gap,
                  end: gap + bucketMs,
                  value: type === "count" ? 0 : null,
                  count: 0
                });
              }
            } else {
              for (let gap = currentBucket.start - bucketMs; gap > bStart; gap -= bucketMs) {
                if (buckets.length >= this._limit) break;
                buckets.push({
                  start: gap,
                  end: gap + bucketMs,
                  value: type === "count" ? 0 : null,
                  count: 0
                });
              }
            }
          }
        }

        if (buckets.length >= this._limit) break;

        currentBucket = {
          start: bStart,
          end: bEnd,
          count: 0,
          sum: 0,
          min: Infinity,
          max: -Infinity,
          first: undefined,
          last: undefined
        };
      }

      currentBucket.count++;
      currentBucket.sum += pt.value;
      if (pt.value < currentBucket.min) currentBucket.min = pt.value;
      if (pt.value > currentBucket.max) currentBucket.max = pt.value;
      if (currentBucket.first === undefined) currentBucket.first = pt.value;
      currentBucket.last = pt.value;
    }

    if (currentBucket && buckets.length < this._limit) {
      finalizeBucket(currentBucket);
    }
    return buckets;
  }

  async count() {
    return this._aggregate("count");
  }

  async sum() {
    return this._aggregate("sum");
  }

  async min() {
    return this._aggregate("min");
  }

  async max() {
    return this._aggregate("max");
  }

  async avg() {
    return this._aggregate("avg");
  }

  async first() {
    return this._aggregate("first");
  }

  async last() {
    return this._aggregate("last");
  }

  /**
   * Watches for real-time incoming points matching this query's tags.
   *
   * @returns {AsyncGenerator<object>}
   */
  watch() {
    if (typeof this._target.watch === "function") {
      return this._target.watch({ where: this._tags });
    }
    throw new Error("Target does not support watch()");
  }
}
