/**
 * Invariant assertions and diagnostics framework for Tencere.
 * Used by test suites, crash recovery verifications, and `tencere verify`.
 */

import { BinaryCodec } from "../core/binary-codec.js";
import { PointCodec, POINT_RECORD_SIZE } from "../timeseries/codec.js";

/**
 * Result structure of an invariant verification run.
 * @typedef {object} InvariantReport
 * @property {boolean} valid
 * @property {string[]} errors
 * @property {string[]} warnings
 * @property {object} stats
 */

/**
 * Runs a comprehensive verification of database invariants.
 *
 * @param {import('../core/engine.js').TencereEngine|import('../index.js').Tencere} dbOrEngine
 * @returns {Promise<InvariantReport>}
 */
export async function verifyInvariants(dbOrEngine) {
  const engine = dbOrEngine._engine || dbOrEngine;
  const errors = [];
  const warnings = [];
  const stats = {
    totalKeys: 0,
    sortedCollections: 0,
    sortedMembers: 0,
    timeSeriesCollections: 0,
    timeSeriesPoints: 0,
    queueJobs: 0,
    activeLocks: 0
  };

  const rawEntries = engine.storage?.entries;
  if (!rawEntries) {
    errors.push("Storage engine entries map is missing or undefined");
    return { valid: false, errors, warnings, stats };
  }

  stats.totalKeys = rawEntries.size;

  // 1. Storage & KV Invariants
  for (const [key, entry] of rawEntries.entries()) {
    if (!key || typeof key !== "string") {
      errors.push(`Invalid key type: ${typeof key}`);
      continue;
    }

    if (!entry || typeof entry !== "object") {
      errors.push(`Invalid entry object for key '${key}'`);
      continue;
    }

    const ver = typeof entry.version === "bigint" ? Number(entry.version) : entry.version;
    if (typeof ver !== "number" || isNaN(ver) || ver < 1) {
      errors.push(`Version monotonicity violation at '${key}': version is ${entry.version} (expected >= 1)`);
    }

    if (!entry.bytes || !(entry.bytes instanceof Uint8Array || Buffer.isBuffer(entry.bytes))) {
      errors.push(`Corrupt or missing payload bytes at key '${key}'`);
      continue;
    }

    try {
      BinaryCodec.decode(entry.bytes);
    } catch (err) {
      errors.push(`Failed to decode value at key '${key}': ${err.message}`);
    }

    // Lock inspection
    if (key.includes("__lock:")) {
      try {
        const val = BinaryCodec.decode(entry.bytes);
        stats.activeLocks++;
        const tok = val?.token ?? val?.fencingToken;
        if (typeof tok !== "number" || tok < 1) {
          errors.push(`Lock '${key}' has invalid fencing token: ${tok}`);
        }
      } catch (_) {}
    }

    // Queue job inspection
    if (key.includes("__queue_job:")) {
      stats.queueJobs++;
      try {
        const job = BinaryCodec.decode(entry.bytes);
        if (!job || typeof job !== "object") {
          errors.push(`Corrupt queue job record at '${key}'`);
        } else {
          const status = job.state || job.status || "ready";
          const validStatuses = ["ready", "active", "completed", "failed", "delayed"];
          if (!validStatuses.includes(status)) {
            errors.push(`Queue job '${key}' has invalid status '${status}'`);
          }
          if (status === "active") {
            if (typeof job.attempts !== "number" || job.attempts < 1) {
              errors.push(`Queue job '${key}' is active but missing attempts count`);
            }
          }
        }
      } catch (_) {}
    }
  }

  // 2. Sorted Collection Invariants
  if (engine._sortedIndexes) {
    stats.sortedCollections = engine._sortedIndexes.size;
    for (const [name, index] of engine._sortedIndexes.entries()) {
      const sPrefix = `__sorted_score:${name}:`;
      const members = index.rangeByScore(-Infinity, Infinity);
      stats.sortedMembers += members.length;

      for (const item of members) {
        const storageKey = `${sPrefix}${item.member}`;
        const entry = rawEntries.get(storageKey);
        if (!entry) {
          errors.push(`Sorted index '${name}' references member '${item.member}' but storage key '${storageKey}' does not exist`);
          continue;
        }

        try {
          const storedScore = BinaryCodec.decode(entry.bytes);
          if (storedScore !== item.score) {
            errors.push(`Sorted parity mismatch for '${name}':'${item.member}': index score ${item.score} != storage score ${storedScore}`);
          }
        } catch (_) {}
      }
    }
  }

  // 3. TimeSeries Invariants
  const tsStores = engine._timeSeriesStores || engine._timeseries;
  if (tsStores) {
    stats.timeSeriesCollections = tsStores.size;
    for (const [colName, tsStorage] of tsStores.entries()) {
      for (const [seriesId, index] of tsStorage._indexes.entries()) {
        const points = index.rangeByScore(-Infinity, Infinity);
        stats.timeSeriesPoints += points.length;

        let prevTs = -Infinity;
        let prevSeq = -1;

        for (const item of points) {
          const parts = item.member.split(":");
          const seq = BigInt(`0x${parts[0]}`);
          const pointId = `${seriesId}:${seq}`;
          const ptBuf = tsStorage._points.get(pointId);
          if (!ptBuf || ptBuf.length !== POINT_RECORD_SIZE) {
            errors.push(`TimeSeries '${colName}' series '${seriesId}' has invalid point buffer size for '${item.member}'`);
            continue;
          }

          let point;
          try {
            point = PointCodec.decode(ptBuf);
          } catch (err) {
            errors.push(`Corrupt point decode in TimeSeries '${colName}': ${err.message}`);
            continue;
          }

          if (!Number.isFinite(point.value) || Number.isNaN(point.value)) {
            errors.push(`Invalid non-finite TimeSeries point value at '${colName}':'${seriesId}'`);
          }

          // Check (timestamp, sequence) monotonic ordering
          if (point.timestamp < prevTs) {
            errors.push(`TimeSeries '${colName}' monotonicity violation: timestamp ${point.timestamp} < previous ${prevTs}`);
          } else if (point.timestamp === prevTs && point.sequence <= prevSeq) {
            errors.push(`TimeSeries '${colName}' same-timestamp sequence violation: seq ${point.sequence} <= previous ${prevSeq}`);
          }

          prevTs = point.timestamp;
          prevSeq = point.sequence;
        }
      }
    }
  }

  return {
    valid: errors.length === 0,
    errors,
    warnings,
    stats
  };
}
