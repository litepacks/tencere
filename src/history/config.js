/**
 * Configuration and pattern matching for Tencere's history subsystem.
 */

import { parseDuration as parseDurationString, parseTimePoint as parseHistoricalPoint } from "../core/time.js";
export { parseDurationString, parseHistoricalPoint };


/**
 * Checks if a key matches a glob/prefix pattern.
 * Supports exact match, wildcard suffix 'prefix:*', or wildcard '*'.
 *
 * @param {string} pattern
 * @param {string} key
 * @returns {boolean}
 */
export function matchesPattern(pattern, key) {
  if (pattern === "*") return true;
  if (pattern === key) return true;

  if (pattern.endsWith("*")) {
    const prefix = pattern.slice(0, -1);
    return key.startsWith(prefix);
  }

  if (pattern.startsWith("*")) {
    const suffix = pattern.slice(1);
    return key.endsWith(suffix);
  }

  return false;
}

export class HistoryConfig {
  /**
   * @param {boolean|object} [options=false]
   */
  constructor(options = false) {
    if (typeof options === "boolean") {
      this.enabled = options;
      this.retentionMs = 0;
      this.maxVersions = 50;
      this.include = [];
      this.exclude = [];
      this.mode = "normal"; // 'normal' | 'audit'
    } else if (typeof options === "object" && options !== null) {
      this.enabled = Boolean(options.enabled ?? true);
      this.retentionMs = options.retention ? parseDurationString(options.retention) : 0;
      this.maxVersions = options.maxVersions !== undefined ? Number(options.maxVersions) : 50;
      this.include = Array.isArray(options.include) ? options.include : [];
      this.exclude = Array.isArray(options.exclude) ? options.exclude : [];
      this.mode = options.mode || "normal";
    } else {
      this.enabled = false;
      this.retentionMs = 0;
      this.maxVersions = 50;
      this.include = [];
      this.exclude = [];
      this.mode = "normal";
    }

    this._collectionOverrides = new Map();
  }

  /**
   * Registers a collection-level override.
   *
   * @param {string} collectionName
   * @param {boolean|object} overrideOptions
   */
  setCollectionOverride(collectionName, overrideOptions) {
    this._collectionOverrides.set(collectionName, overrideOptions);
  }

  /**
   * Evaluates whether history should be tracked for a given key.
   *
   * @param {string} key
   * @param {string|null} [collectionName=null]
   * @returns {boolean}
   */
  shouldTrackKey(key, collectionName = null) {
    if (!this.enabled) return false;

    // Detect collection name from internal prefix if not explicitly provided
    if (!collectionName && typeof key === "string") {
      const m = key.match(/^__(?:map|sorted_score|sorted_val|kv|set|counter|vector|queue|stream):([^:]+):/);
      if (m) {
        collectionName = m[1];
      }
    }

    // Check collection override first if specified
    if (collectionName && this._collectionOverrides.has(collectionName)) {
      const override = this._collectionOverrides.get(collectionName);
      if (override === false) return false;
      if (typeof override === "object" && override !== null && override.enabled === false) {
        return false;
      }
      if (override === true || (typeof override === "object" && override !== null && override.enabled !== false)) {
        return true;
      }
    }

    // Determine normalized logical key for pattern matching
    let logicalKey = key;
    if (typeof key === "string" && key.startsWith("__")) {
      const idx = key.indexOf(":", key.indexOf(":") + 1);
      if (idx !== -1) {
        logicalKey = key.slice(idx + 1);
      }
    }

    // Check exclude patterns
    for (const pat of this.exclude) {
      if (matchesPattern(pat, key) || (logicalKey !== key && matchesPattern(pat, logicalKey))) {
        return false;
      }
      if (collectionName && matchesPattern(pat, `${collectionName}:*`)) {
        return false;
      }
    }

    // Check include patterns
    if (this.include.length > 0) {
      for (const pat of this.include) {
        if (matchesPattern(pat, key) || (logicalKey !== key && matchesPattern(pat, logicalKey))) {
          return true;
        }
        if (collectionName && matchesPattern(pat, `${collectionName}:*`)) {
          return true;
        }
      }
      return false; // Not in include list
    }

    return true;
  }
}
