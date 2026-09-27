/**
 * Deterministic diagnostic state hash calculation for Tencere.
 * Independent of memory addresses, allocation order, hash table iteration randomness,
 * process ID, and wall clock.
 */

import { createHash } from "node:crypto";
import { BinaryCodec } from "../core/binary-codec.js";

/**
 * Deterministically serialize any JavaScript value into canonical string representation.
 * Keys in objects are sorted alphabetically.
 * Primitives, BigInt, Buffers/TypedArrays, Dates are consistently formatted.
 *
 * @param {any} val
 * @returns {string}
 */
export function canonicalStringify(val) {
  if (val === null) return "null";
  if (val === undefined) return "undefined";
  if (typeof val === "bigint") return `bigint:${val.toString()}`;
  if (typeof val === "number") {
    if (Number.isNaN(val)) return "num:NaN";
    if (!Number.isFinite(val)) return val > 0 ? "num:+Infinity" : "num:-Infinity";
    return Object.is(val, -0) ? "num:-0" : `num:${val.toString()}`;
  }
  if (typeof val === "boolean") return val ? "bool:true" : "bool:false";
  if (typeof val === "string") return `str:${JSON.stringify(val)}`;
  if (Buffer.isBuffer(val) || val instanceof Uint8Array) {
    return `buf:${Buffer.from(val.buffer, val.byteOffset, val.byteLength).toString("hex")}`;
  }
  if (val instanceof Date) {
    return `date:${val.toISOString()}`;
  }
  if (Array.isArray(val)) {
    return "[" + val.map(canonicalStringify).join(",") + "]";
  }
  if (typeof val === "object") {
    const keys = Object.keys(val).sort();
    return "{" + keys.map((k) => `${JSON.stringify(k)}:${canonicalStringify(val[k])}`).join(",") + "}";
  }
  return `unknown:${String(val)}`;
}

/**
 * Computes deterministic SHA-256 state hash for a given TencereEngine or Tencere instance.
 *
 * @param {import('../core/engine.js').TencereEngine|import('../index.js').Tencere} dbOrEngine
 * @param {object} [options={}]
 * @param {function(string): boolean} [options.keyFilter] Optional key filter predicate
 * @returns {string} SHA-256 hexadecimal hash
 */
export function calculateStateHash(dbOrEngine, options = {}) {
  const engine = dbOrEngine._engine || dbOrEngine;
  const hash = createHash("sha256");

  // 1. Sort all storage keys deterministically
  const rawEntries = engine.storage?.entries;
  if (rawEntries) {
    const sortedKeys = Array.from(rawEntries.keys()).sort();

    for (const key of sortedKeys) {
      if (options.keyFilter && !options.keyFilter(key)) {
        continue;
      }

      const entry = rawEntries.get(key);
      if (!entry) continue;

      let decodedVal;
      try {
        decodedVal = entry.bytes ? BinaryCodec.decode(entry.bytes) : null;
      } catch (_) {
        decodedVal = null;
      }

      // Feed key, version, and canonical value representation
      hash.update(`k:${key}|v:${entry.version || 1}|val:${canonicalStringify(decodedVal)}\n`);
    }
  }

  // 2. Deterministically incorporate TimeSeries collections if present
  const tsStores = engine._timeSeriesStores || engine._timeseries;
  if (tsStores && tsStores.size > 0) {
    const tsNames = Array.from(tsStores.keys()).sort();
    for (const name of tsNames) {
      if (options.keyFilter && !options.keyFilter(`__ts:${name}`)) {
        continue;
      }
      const tsStorage = tsStores.get(name);
      if (!tsStorage) continue;

      hash.update(`ts_col:${name}\n`);
      const seriesIds = Array.from(tsStorage._indexes.keys()).sort();
      for (const sId of seriesIds) {
        hash.update(`  series:${sId}\n`);
        const index = tsStorage._indexes.get(sId);
        if (!index) continue;
        const members = index.rangeByScore(-Infinity, Infinity);
        for (const item of members) {
          const parts = item.member.split(":");
          const seq = BigInt(`0x${parts[0]}`);
          const pointId = `${sId}:${seq}`;
          const pointBuf = tsStorage._points.get(pointId);
          const hex = pointBuf ? Buffer.from(pointBuf).toString("hex") : "";
          hash.update(`    pt:${item.member}|s:${item.score}|raw:${hex}\n`);
        }
      }
    }
  }

  return hash.digest("hex");
}

/**
 * Computes deterministic SHA-256 hash for a specific cluster partition.
 *
 * @param {import('../core/engine.js').TencereEngine|import('../index.js').Tencere} dbOrEngine
 * @param {string|number} partitionId
 * @param {number} [totalPartitions=16]
 * @returns {string} SHA-256 hexadecimal hash
 */
export function calculatePartitionHash(dbOrEngine, partitionId, totalPartitions = 16) {
  const targetPartition = Number(partitionId);

  // Hash partition router (FNV-1a 32-bit string hash mod totalPartitions)
  const isKeyInPartition = (key) => {
    let h = 0x811c9dc5;
    for (let i = 0; i < key.length; i++) {
      h ^= key.charCodeAt(i);
      h = Math.imul(h, 0x01000193);
    }
    const partition = Math.abs(h % totalPartitions);
    return partition === targetPartition;
  };

  return calculateStateHash(dbOrEngine, {
    keyFilter: isKeyInPartition
  });
}
