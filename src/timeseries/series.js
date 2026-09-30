/**
 * Series management, tag canonicalization, validation, and cardinality safeguards.
 */

import { TagLimitExceededError, SeriesCardinalityExceededError } from "../errors.js";
import { LIMITS } from "../core/limits.js";

/**
 * FNV-1a 64-bit hash implementation.
 *
 * @param {string} str
 * @returns {string} 16-hex digit string
 */
export function fnv1a(str) {
  let hash = 0xcbf29ce484222325n;
  const prime = 0x100000001b3n;
  const mask = 0xffffffffffffffffn;
  for (let i = 0; i < str.length; i++) {
    hash ^= BigInt(str.charCodeAt(i));
    hash = (hash * prime) & mask;
  }
  return hash.toString(16).padStart(16, "0");
}

export class SeriesRegistry {
  /**
   * @param {string} collection
   * @param {object} [options={}]
   * @param {number} [options.maxTags]
   * @param {number} [options.maxTagKeyLength]
   * @param {number} [options.maxTagValueLength]
   * @param {number} [options.maxSeries]
   * @param {string[]} [options.indexed]
   */
  constructor(collection, options = {}) {
    this.collection = collection;
    const limits = options.limits || {};
    const tagOpts = options.tags || {};

    const tsLimits = LIMITS.TIMESERIES;
    this.maxTags = limits.maxTags !== undefined ? limits.maxTags : (tagOpts.maxTags || tsLimits.DEFAULT_MAX_TAGS);
    this.maxTagKeyLength = limits.maxTagKeyLength !== undefined ? limits.maxTagKeyLength : tsLimits.MAX_TAG_KEY_LENGTH;
    this.maxTagValueLength = limits.maxTagValueLength !== undefined ? limits.maxTagValueLength : tsLimits.MAX_TAG_VALUE_LENGTH;
    this.maxSeries = limits.maxSeries !== undefined ? limits.maxSeries : tsLimits.DEFAULT_MAX_SERIES;
    this.indexed = Array.isArray(tagOpts.indexed) ? new Set(tagOpts.indexed) : null;

    // seriesId -> { seriesId, canonicalTags, tags }
    this._series = new Map();
    this._canonicalToSeries = new Map();

    // Default series without tags
    this._defaultSeries = {
      seriesId: "default",
      canonicalTags: "",
      tags: {}
    };
    this._series.set("default", this._defaultSeries);
    this._canonicalToSeries.set("", "default");
  }

  updateOptions(options = {}) {
    const limits = options.limits || {};
    const tagOpts = options.tags || {};
    if (limits.maxTags !== undefined) this.maxTags = limits.maxTags;
    if (tagOpts.maxTags !== undefined) this.maxTags = tagOpts.maxTags;
    if (limits.maxTagKeyLength !== undefined) this.maxTagKeyLength = limits.maxTagKeyLength;
    if (limits.maxTagValueLength !== undefined) this.maxTagValueLength = limits.maxTagValueLength;
    if (limits.maxSeries !== undefined) this.maxSeries = limits.maxSeries;
    if (tagOpts.indexed !== undefined) {
      this.indexed = Array.isArray(tagOpts.indexed) ? new Set(tagOpts.indexed) : null;
    }
  }

  get count() {
    return this._series.size;
  }

  /**
   * Canonicalizes and validates tags, returning the canonical tag string and seriesId.
   *
   * @param {Record<string, string>} [tags]
   * @returns {{ seriesId: string, canonicalTags: string, normalizedTags: Record<string, string> }}
   */
  resolveSeries(tags) {
    if (!tags || typeof tags !== "object" || Object.keys(tags).length === 0) {
      return {
        seriesId: "default",
        canonicalTags: "",
        normalizedTags: {}
      };
    }

    const rawKeys = Object.keys(tags);
    if (rawKeys.length > this.maxTags) {
      throw new TagLimitExceededError(
        `Exceeded maximum tags per point: ${rawKeys.length} > ${this.maxTags}`
      );
    }

    for (const key of rawKeys) {
      if (typeof key !== "string" || key.length === 0 || key.length > this.maxTagKeyLength) {
        throw new TagLimitExceededError(
          `Tag key '${key}' invalid or exceeds max length ${this.maxTagKeyLength}`
        );
      }
      const val = String(tags[key]);
      if (val.length > this.maxTagValueLength) {
        throw new TagLimitExceededError(
          `Tag value for '${key}' exceeds max length ${this.maxTagValueLength}`
        );
      }
    }

    // Filter to indexed keys if whitelist is configured
    const keys = (this.indexed ? rawKeys.filter((k) => this.indexed.has(k)) : rawKeys).sort();

    const parts = [];
    const normalizedTags = {};

    for (const key of keys) {
      const val = String(tags[key]);
      normalizedTags[key] = val;
      parts.push(`${key}=${val}`);
    }

    const canonicalTags = parts.join(",");
    if (!canonicalTags) {
      return {
        seriesId: "default",
        canonicalTags: "",
        normalizedTags: {}
      };
    }

    if (this._canonicalToSeries.has(canonicalTags)) {
      const existingId = this._canonicalToSeries.get(canonicalTags);
      return {
        seriesId: existingId,
        canonicalTags,
        normalizedTags
      };
    }

    const baseId = `s_${fnv1a(canonicalTags)}`;
    let seriesId = baseId;
    let collisionCounter = 1;

    // Guaranteed collision prevention: if different canonicalTags hit same hash
    while (this._series.has(seriesId) && this._series.get(seriesId).canonicalTags !== canonicalTags) {
      seriesId = `${baseId}_${collisionCounter++}`;
    }

    if (!this._series.has(seriesId)) {
      if (this._series.size >= this.maxSeries) {
        throw new SeriesCardinalityExceededError(
          `Exceeded maximum series cardinality of ${this.maxSeries} in collection '${this.collection}'`
        );
      }
      this._series.set(seriesId, {
        seriesId,
        canonicalTags,
        tags: normalizedTags
      });
      this._canonicalToSeries.set(canonicalTags, seriesId);
    }

    return {
      seriesId,
      canonicalTags,
      normalizedTags
    };
  }

  /**
   * Retrieves series info by ID.
   *
   * @param {string} seriesId
   * @returns {{ seriesId: string, canonicalTags: string, tags: Record<string, string> }|undefined}
   */
  getSeries(seriesId) {
    return this._series.get(seriesId);
  }

  /**
   * Finds all registered series IDs that match a given filter.
   *
   * @param {Record<string, string>} [filter]
   * @returns {string[]}
   */
  matchSeries(filter) {
    if (!filter || Object.keys(filter).length === 0) {
      return Array.from(this._series.keys());
    }

    const matching = [];
    for (const [id, s] of this._series.entries()) {
      let match = true;
      for (const [k, v] of Object.entries(filter)) {
        if (s.tags[k] !== String(v)) {
          match = false;
          break;
        }
      }
      if (match) {
        matching.push(id);
      }
    }
    return matching;
  }

  /**
   * Lists all registered series.
   *
   * @returns {Array<{ seriesId: string, canonicalTags: string, tags: Record<string, string> }>}
   */
  listSeries() {
    return Array.from(this._series.values());
  }
}
