/**
 * StorageEngine for Tencere.
 * Composes:
 *  - cemalloc: explicit slab/page memory allocation for binary values and memory limits
 *  - muttafa: mutation buffering engine for amortizing mutations and generation control
 *  - binary-codec: type-tagged binary serialization
 */

import { Allocator } from "cemalloc";
import { MutationEngine, MemorySink } from "muttafa";
import { BinaryCodec } from "./binary-codec.js";

export function parseByteSize(val) {
  if (typeof val === "number") return val;
  if (!val || typeof val !== "string") return 0;
  const match = val.trim().match(/^(\d+(?:\.\d+)?)\s*(b|kb|mb|gb|tb)?$/i);
  if (!match) {
    const n = Number(val);
    return isNaN(n) ? 0 : n;
  }
  const num = parseFloat(match[1]);
  const unit = (match[2] || "b").toLowerCase();
  switch (unit) {
    case "kb":
      return Math.round(num * 1024);
    case "mb":
      return Math.round(num * 1024 * 1024);
    case "gb":
      return Math.round(num * 1024 * 1024 * 1024);
    case "tb":
      return Math.round(num * 1024 * 1024 * 1024 * 1024);
    default:
      return Math.round(num);
  }
}

export class StorageEntry {
  /**
   * @param {string|object} key
   * @param {Uint8Array} [bytes]
   * @param {number} [version=1]
   * @param {number} [updatedAt=Date.now()]
   * @param {boolean} [allocated=false]
   */
  constructor(key, bytes, version = 1, updatedAt = Date.now(), allocated = false) {
    if (typeof key === "object" && key !== null) {
      this.key = key.key;
      this.bytes = key.bytes;
      this.version = key.version || 1;
      this.updatedAt = key.updatedAt || Date.now();
      this.allocated = key.allocated || false;
      this.rawAllocatedBuffer = key.rawAllocatedBuffer || null;
      return;
    }
    this.key = key;
    this.bytes = bytes;
    this.version = version;
    this.updatedAt = updatedAt;
    this.allocated = allocated;
    this.rawAllocatedBuffer = null;
  }

  get value() {
    return BinaryCodec.decode(this.bytes);
  }

  get size() {
    return this.bytes ? this.bytes.byteLength : 0;
  }
}

export class StorageEngine {
  /**
   * @param {object} [options={}]
   * @param {object} [options.memory]
   * @param {string|number} [options.memory.limit]
   */
  constructor(options = {}) {
    this.options = options;
    const memoryLimit = options.memory?.limit ? parseByteSize(options.memory.limit) : 0;

    // Initialize cemalloc Allocator
    this.allocator = new Allocator(memoryLimit > 0 ? { hardLimit: memoryLimit } : {});

    // Initialize muttafa MutationEngine with in-memory sink
    this.mutationSink = new MemorySink();
    this.mutationEngine = new MutationEngine({
      sink: this.mutationSink,
      maxEntries: 10000,
      autoFlush: false
    });

    // In-memory key map: key -> StorageEntry
    this.entries = new Map();
    this.totalBytes = 0;
    this.writesCount = 0;
    this.readsCount = 0;
    this.deletesCount = 0;
    this.closed = false;
  }

  /**
   * Stores a key-value pair.
   *
   * @param {string} key
   * @param {any} value
   * @param {number} [version=1]
   * @returns {StorageEntry}
   */
  set(key, value, version = 1) {
    if (this.closed) return null;
    const encoded = BinaryCodec.encode(value);
    const size = encoded.byteLength;

    // Clean up existing entry if present (single lookup)
    const existing = this.entries.get(key);
    if (existing !== undefined) {
      this._freeEntry(existing);
    }

    // Allocate buffer through cemalloc for values >= 64 bytes to save GC churn
    let bytes;
    let allocated = false;
    let rawAllocatedBuffer = null;
    if (size >= 64 && size <= 65536) {
      try {
        rawAllocatedBuffer = this.allocator.alloc(size);
        rawAllocatedBuffer.set(encoded);
        bytes = rawAllocatedBuffer.subarray(0, size);
        allocated = true;
      } catch (err) {
        // Fall back to standard Uint8Array on allocation pressure or error
        bytes = encoded;
        allocated = false;
        rawAllocatedBuffer = null;
      }
    } else {
      bytes = encoded;
    }

    const entry = new StorageEntry(key, bytes, version, Date.now(), allocated);
    entry.rawAllocatedBuffer = rawAllocatedBuffer;

    this.entries.set(key, entry);
    this.totalBytes += size;
    this.writesCount++;

    // Buffer mutation via Muttafa
    try {
      this.mutationEngine.put(key, encoded);
    } catch (_) {
      // Non-fatal if buffer is flushing
    }

    return entry;
  }

  /**
   * Retrieves an entry by key.
   *
   * @param {string} key
   * @returns {StorageEntry | null}
   */
  get(key) {
    if (this.closed) return null;
    this.readsCount++;
    return this.entries.get(key) || null;
  }

  /**
   * Checks if key exists in storage.
   *
   * @param {string} key
   * @returns {boolean}
   */
  has(key) {
    if (this.closed) return false;
    return this.entries.has(key);
  }

  /**
   * Deletes a key from storage.
   *
   * @param {string} key
   * @returns {boolean} true if deleted
   */
  delete(key) {
    if (this.closed) return false;
    const entry = this.entries.get(key);
    if (!entry) return false;

    this._freeEntry(entry);
    this.entries.delete(key);
    this.deletesCount++;

    try {
      this.mutationEngine.delete(key);
    } catch (_) {}

    return true;
  }

  /**
   * Scans keys with an optional prefix.
   *
   * @param {string} [prefix=""]
   * @param {object} [options={}]
   * @param {number} [options.limit=Infinity]
   * @returns {Array<{ key: string, value: any, version: number }>}
   */
  scan(prefix = "", options = {}) {
    const { limit = Infinity } = options;
    const results = [];
    for (const [k, entry] of this.entries.entries()) {
      if (results.length >= limit) break;
      if (!prefix || k.startsWith(prefix)) {
        results.push({
          key: k,
          value: BinaryCodec.decode(entry.bytes),
          version: entry.version
        });
      }
    }
    return results;
  }

  /**
   * Returns all keys matching prefix.
   */
  keys(prefix = "") {
    const list = [];
    for (const k of this.entries.keys()) {
      if (!prefix || k.startsWith(prefix)) {
        list.push(k);
      }
    }
    return list;
  }

  /**
   * Clears all storage.
   */
  clear() {
    for (const entry of this.entries.values()) {
      this._freeEntry(entry);
    }
    this.entries.clear();
    this.totalBytes = 0;
  }

  _freeEntry(entry) {
    if (entry && entry.allocated && (entry.rawAllocatedBuffer || entry.bytes)) {
      try {
        this.allocator.free(entry.rawAllocatedBuffer || entry.bytes);
      } catch (_) {}
    }
    if (entry && entry.bytes) {
      this.totalBytes = Math.max(0, this.totalBytes - entry.bytes.byteLength);
    }
  }

  /**
   * Observability and storage statistics.
   */
  stats() {
    return {
      keyCount: this.entries.size,
      totalBytes: this.totalBytes,
      writes: this.writesCount,
      reads: this.readsCount,
      deletes: this.deletesCount,
      allocator: this.allocator.stats ? this.allocator.stats() : null,
      mutations: this.mutationEngine.stats ? this.mutationEngine.stats() : null
    };
  }

  /**
   * Closes the storage engine and releases all buffers.
   */
  async close() {
    this.closed = true;
    this.clear();
    try {
      await this.mutationEngine.close();
    } catch (_) {}
  }
}
