/**
 * SemanticCache: Higher-level semantic caching layer built on vector similarity and TTL.
 */

import crypto from "node:crypto";
import { parseDuration } from "../core/expiry-wheel.js";

export class SemanticCache {
  /**
   * @param {import('../index.js').Tencere} db
   * @param {object} [options={}]
   * @param {number} [options.threshold=0.85] Cosine similarity threshold for cache hits
   * @param {string|number} [options.ttl='24h']
   */
  constructor(db, options = {}) {
    this._db = db;
    this._threshold = options.threshold || 0.85;
    this._defaultTtl = options.ttl || "24h";
    this._vectors = db.vector("__semantic_cache");
  }

  /**
   * Generates a deterministic, collision-free document ID from prompt.
   *
   * @param {string} prompt
   * @returns {string}
   * @private
   */
  _promptId(prompt) {
    const hash = crypto.createHash("sha256").update(String(prompt)).digest("base64url");
    return `prompt:${hash}`;
  }

  /**
   * Stores a prompt-response entry into semantic cache.
   *
   * @param {string} prompt
   * @param {any} response
   * @param {object} options
   * @param {number[]|Float32Array} options.embedding
   * @param {string|number} [options.ttl]
   * @param {object} [options.metadata]
   * @returns {Promise<void>}
   */
  async set(prompt, response, options = {}) {
    if (!options.embedding) {
      throw new Error("Semantic cache requires an embedding vector in options.embedding");
    }
    const ttlMs = parseDuration(options.ttl !== undefined ? options.ttl : this._defaultTtl);
    const expiresAt = ttlMs > 0 ? Date.now() + ttlMs : null;
    const id = this._promptId(prompt);

    await this._vectors.set(id, {
      vector: options.embedding,
      value: {
        prompt,
        response,
        metadata: options.metadata || {},
        createdAt: Date.now(),
        expiresAt
      }
    });
  }

  /**
   * Retrieves matching response from semantic cache if similarity >= threshold.
   *
   * @param {string|object} prompt Prompt string, or options object if prompt is omitted
   * @param {object} [options={}]
   * @param {number[]|Float32Array} [options.embedding]
   * @param {number} [options.threshold]
   * @param {boolean} [options.details=false] If true, returns match metadata and score
   * @returns {Promise<any|null>}
   */
  async get(prompt, options = {}) {
    const opts = typeof prompt === "object" && prompt !== null && prompt.embedding ? prompt : options;
    if (!opts || !opts.embedding) {
      return null;
    }
    const threshold = opts.threshold !== undefined ? opts.threshold : this._threshold;
    const now = Date.now();

    const matches = await this._vectors.search(opts.embedding, {
      topK: 1,
      filter: (doc) => !doc.value?.expiresAt || doc.value.expiresAt > now
    });

    if (matches.length > 0 && matches[0].score >= threshold) {
      if (opts.details) {
        return {
          response: matches[0].value.response,
          score: matches[0].score,
          prompt: matches[0].value.prompt,
          metadata: matches[0].value.metadata,
          createdAt: matches[0].value.createdAt,
          expiresAt: matches[0].value.expiresAt
        };
      }
      return matches[0].value.response;
    }
    return null;
  }

  /**
   * Checks if an exact prompt is cached and unexpired.
   *
   * @param {string} prompt
   * @returns {Promise<boolean>}
   */
  async has(prompt) {
    const id = this._promptId(prompt);
    const doc = await this._vectors.get(id);
    if (!doc) return false;
    if (doc.value?.expiresAt && doc.value.expiresAt <= Date.now()) {
      await this._vectors.delete(id);
      return false;
    }
    return true;
  }

  /**
   * Deletes a cached prompt entry.
   *
   * @param {string} prompt
   * @returns {Promise<boolean>}
   */
  async delete(prompt) {
    const id = this._promptId(prompt);
    return await this._vectors.delete(id);
  }

  /**
   * Clears all cached prompt entries.
   *
   * @returns {Promise<void>}
   */
  async clear() {
    await this._vectors.clear();
  }

  /**
   * Prunes all expired entries from cache.
   *
   * @returns {Promise<number>} Number of pruned entries
   */
  async prune() {
    this._vectors._ensureLoaded();
    const now = Date.now();
    let pruned = 0;
    const ids = Array.from(this._vectors._docs.keys());
    for (const id of ids) {
      const doc = this._vectors._docs.get(id);
      if (doc && doc.value?.expiresAt && doc.value.expiresAt <= now) {
        await this._vectors.delete(id);
        pruned++;
      }
    }
    return pruned;
  }

  /**
   * Returns current count of entries in cache.
   *
   * @returns {Promise<number>}
   */
  async count() {
    return await this._vectors.count();
  }
}
