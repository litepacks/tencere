/**
 * TencereEngine: The core database engine.
 * Composes:
 *  - StorageEngine (cemalloc memory management + muttafa mutation buffering)
 *  - Daktilo (Write-Ahead Log for durable ordered history)
 *  - ExpiryManager (adaptive timing-wheel / min-heap expiration index)
 *  - PartitionManager (consistent hash routing)
 *  - BinaryCodec & OperationCodec
 */

import "./polyfill.js";
import path from "node:path";
import fs from "node:fs/promises";
import { EventEmitter } from "node:events";
import { AsyncLocalStorage } from "node:async_hooks";
import { Daktilo } from "daktilo";
import { FaultInjector } from "./fault-injection.js";
import { StorageEngine } from "./storage.js";
import { ExpiryManager, parseDuration } from "./expiry-wheel.js";
import { PartitionManager } from "./partitions.js";
import {
  Operation,
  OP_SET,
  OP_DEL,
  OP_EXPIRE,
  OP_INCR,
  OP_PATCH,
  OP_CHECKPOINT,
  OP_RESTORE,
  OP_RESTORE_BEGIN,
  OP_RESTORE_COMMIT,
  OP_TS_ADD,
  OP_TS_DELETE,
  OP_TS_CORRECT,
  OP_TS_BATCH,
  FLAG_SLIDING,
  FLAG_CONSUME,
  FLAG_HAS_VERSION,
  FLAG_HAS_TTL,
  FLAG_RESTORE
} from "./operations.js";
import {
  DatabaseClosedError,
  VersionMismatchError,
  KeyNotFoundError,
  HistoryDisabledError,
  HistoryUnavailableError,
  ReadOnlyDatabaseError,
  NotLeaderError
} from "../errors.js";
import { HistoryManager } from "../history/index.js";
import { HistoricalView } from "../history/view.js";
import { RollbackPlan } from "../history/plan.js";
import { TimeSeriesCollection } from "../collections/timeseries.js";
import { TimeSeriesStorage } from "../timeseries/storage.js";


export class TencereEngine {
  /**
   * @param {string|null} dataDir
   * @param {object} [options={}]
   */
  constructor(dataDir, options = {}) {
    this.dataDir = dataDir ? path.resolve(dataDir) : null;
    this.options = options;
    this.durability = options.durability || "batch"; // 'strict' | 'batch' | 'async'

    this.storage = new StorageEngine(options);
    this.expiry = new ExpiryManager({
      onExpire: (key) => this._handleExpiredKey(key)
    });
    this.partitions = new PartitionManager(options.partition || {});

    this.historyManager = new HistoryManager(this, options.history);
    this._historyEnabled = this.historyManager.enabled;
    this._sequenceCounter = 0n;

    this.daktilo = null;
    this.events = new EventEmitter();
    this.events.setMaxListeners(0); // Unlimited reactive subscribers

    this.versions = new Map(); // key -> monotonic version number
    this.fencingCounter = 0; // Monotonic fencing token counter
    this.isClosed = false;

    // Operation statistics
    this.opsCount = 0;
    this.cacheHits = 0;
    this.cacheMisses = 0;
    this.expirationsCount = 0;
    this._keyLocks = new Map();
    this._lockContext = new AsyncLocalStorage();
    this._faults = options._faults || new FaultInjector();
  }

  /**
   * Opens or creates a Tencere database instance.
   *
   * @param {string|null} dataDir
   * @param {object} [options={}]
   * @returns {Promise<TencereEngine>}
   */
  static async open(dataDir, options = {}) {
    let dir = dataDir;
    let opts = options;
    if (typeof dataDir === "object" && dataDir !== null) {
      opts = dataDir;
      dir = opts.dataDir || null;
    }
    const engine = new TencereEngine(dir, opts);
    await engine._init();
    return engine;
  }

  async _init() {
    if (this.dataDir) {
      await fs.mkdir(this.dataDir, { recursive: true });
      const walDir = path.join(this.dataDir, "wal");
      await fs.mkdir(walDir, { recursive: true });

      // Open Daktilo WAL
      this.daktilo = await Daktilo.open(walDir, {
        durability: this.durability,
        maxSegmentBytes: this.options.maxSegmentBytes || 64 * 1024 * 1024
      });
      this._sequenceCounter = this.daktilo.head();

      // Deterministic recovery: replay WAL operations
      await this._recover();
    }
  }

  /**
   * Replays deterministic WAL operations from Daktilo.
   */
  async _recover() {
    if (!this.daktilo) return;
    const head = this.daktilo.head();
    if (head === 0n) return; // Empty log
    this._sequenceCounter = head;

    const checkpointSeq = this.daktilo.checkpointSequence || 1n;
    const fromSeq = checkpointSeq > 0n ? checkpointSeq : 1n;

    for await (const record of this.daktilo.range(fromSeq, head)) {
      try {
        const op = Operation.decode(record.payload);
        this._applyOperation(op, false, record.sequence);

        if (this._historyEnabled) {
          this.historyManager.record({
            sequence: record.sequence,
            key: op.key,
            op: op.op,
            version: op.version,
            timestamp: op.timestamp,
            ttlMs: op.ttlMs,
            value: op.value,
            extra: op.extra,
            size: record.payload.byteLength
          });
        }
      } catch (err) {
        // Log corruption or decode error during recovery
        console.error(`[Tencere Recovery] Error decoding record at sequence ${record.sequence}:`, err);
      }
    }
  }

  /**
   * Applies an operation to local in-memory storage and indexes.
   *
   * @param {Operation} op
   * @param {boolean} [emit=true]
   * @param {bigint|null} [sequence=null]
   */
  _applyOperation(op, emit = true, sequence = null) {
    this._faults.trigger("before-state-apply");
    if (op.version && BigInt(op.version) > this._sequenceCounter) {
      this._sequenceCounter = BigInt(op.version);
    }
    if (sequence && BigInt(sequence) > this._sequenceCounter) {
      this._sequenceCounter = BigInt(sequence);
    }
    const hasListeners = emit && this.events.listenerCount("change") > 0;

    switch (op.op) {
      case OP_SET: {
        let prevValue;
        if (hasListeners) {
          const prevEntry = this.storage.get(op.key);
          prevValue = prevEntry ? prevEntry.value : undefined;
        }

        this.storage.set(op.key, op.value, op.version);
        this.versions.set(op.key, op.version);

        if (op.ttlMs > 0) {
          const isSliding = Boolean(op.flags & FLAG_SLIDING);
          const isConsume = Boolean(op.flags & FLAG_CONSUME);
          this.expiry.schedule(op.key, op.ttlMs, {
            sliding: isSliding,
            consume: isConsume
          });
        } else {
          this.expiry.cancel(op.key);
        }

        if (hasListeners) {
          this.events.emit("change", {
            key: op.key,
            type: "set",
            value: op.value,
            previousValue: prevValue,
            version: op.version,
            timestamp: op.timestamp
          });
        }
        break;
      }

      case OP_RESTORE: {
        let prevValue;
        if (hasListeners) {
          const prevEntry = this.storage.get(op.key);
          prevValue = prevEntry ? prevEntry.value : undefined;
        }

        if (op.value === undefined) {
          this.storage.delete(op.key);
          this.expiry.cancel(op.key);
          this.versions.delete(op.key);
        } else {
          this.storage.set(op.key, op.value, op.version);
          this.versions.set(op.key, op.version);

          if (op.ttlMs > 0) {
            const isSliding = Boolean(op.flags & FLAG_SLIDING);
            const isConsume = Boolean(op.flags & FLAG_CONSUME);
            this.expiry.schedule(op.key, op.ttlMs, {
              sliding: isSliding,
              consume: isConsume
            });
          } else {
            this.expiry.cancel(op.key);
          }
        }

        if (hasListeners) {
          this.events.emit("change", {
            key: op.key,
            type: "restore",
            value: op.value,
            previousValue: prevValue,
            version: op.version,
            timestamp: op.timestamp
          });
        }
        break;
      }

      case OP_DEL: {
        let prevValue;
        if (hasListeners) {
          const prevEntry = this.storage.get(op.key);
          prevValue = prevEntry ? prevEntry.value : undefined;
        }

        this.storage.delete(op.key);
        this.expiry.cancel(op.key);
        this.versions.delete(op.key);

        if (hasListeners) {
          this.events.emit("change", {
            key: op.key,
            type: "delete",
            value: undefined,
            previousValue: prevValue,
            version: op.version,
            timestamp: op.timestamp
          });
        }
        break;
      }

      case OP_EXPIRE: {
        if (this.storage.has(op.key)) {
          const isSliding = Boolean(op.flags & FLAG_SLIDING);
          const isConsume = Boolean(op.flags & FLAG_CONSUME);
          this.expiry.schedule(op.key, op.ttlMs, {
            sliding: isSliding,
            consume: isConsume
          });
        }
        break;
      }

      case OP_INCR: {
        const prevEntry = this.storage.get(op.key);
        const cur = prevEntry ? Number(prevEntry.value) : 0;
        const delta = Number(op.value) || 0;
        const next = cur + delta;
        this.storage.set(op.key, next, op.version);
        this.versions.set(op.key, op.version);

        if (hasListeners) {
          this.events.emit("change", {
            key: op.key,
            type: "incr",
            value: next,
            previousValue: cur,
            version: op.version,
            timestamp: op.timestamp
          });
        }
        break;
      }

      case OP_PATCH: {
        const prevEntry = this.storage.get(op.key);
        const prevValue = prevEntry ? prevEntry.value : undefined;
        const patched = this._applyPatch(prevValue, op.value);
        this.storage.set(op.key, patched, op.version);
        this.versions.set(op.key, op.version);

        if (hasListeners) {
          this.events.emit("change", {
            key: op.key,
            type: "patch",
            value: patched,
            previousValue: prevValue,
            version: op.version,
            timestamp: op.timestamp
          });
        }
        break;
      }

      case OP_TS_ADD: {
        if (op.extra && op.extra.collection) {
          const store = this._getTimeSeriesStorage(op.extra.collection);
          store.restorePoint(op.extra.seriesId, op.timestamp, op.version, op.value, op.extra.tags);
        }
        break;
      }

      case OP_TS_DELETE: {
        if (op.extra && op.extra.collection) {
          const store = this._getTimeSeriesStorage(op.extra.collection);
          store.deleteRange(op.extra.seriesIds || [], op.extra.from, op.extra.to);
        }
        break;
      }

      case OP_TS_CORRECT: {
        if (op.extra && op.extra.collection) {
          const store = this._getTimeSeriesStorage(op.extra.collection);
          store.correctPoint(op.extra.pointId, op.value);
        }
        break;
      }

      case OP_TS_BATCH: {
        if (op.extra && op.extra.collection && Array.isArray(op.extra.points)) {
          const store = this._getTimeSeriesStorage(op.extra.collection);
          for (const pt of op.extra.points) {
            store.restorePoint(pt.seriesId, pt.timestamp, pt.sequence, pt.value, pt.tags);
          }
        }
        break;
      }

      default:
        break;
    }
    this._faults.trigger("after-state-apply");
  }

  _getTimeSeriesStorage(name) {
    if (!this._timeSeriesStores) {
      this._timeSeriesStores = new Map();
    }
    if (!this._timeSeriesStores.has(name)) {
      this._timeSeriesStores.set(name, new TimeSeriesStorage(this, name));
    }
    return this._timeSeriesStores.get(name);
  }

  timeseries(name, options = {}) {
    return new TimeSeriesCollection(this, name, options);
  }

  recordTelemetry(metric, value, tags = {}) {
    if (!metric) return;
    const tsCol = this.timeseries(metric);
    return tsCol.add(value, { tags });
  }

  _applyPatch(currentValue, patchSpec) {
    let obj = currentValue;
    if (!obj || typeof obj !== "object") {
      obj = {};
    } else {
      obj = { ...obj };
    }

    if (patchSpec.$set) {
      for (const [k, v] of Object.entries(patchSpec.$set)) {
        obj[k] = v;
      }
    }
    if (patchSpec.$inc) {
      for (const [k, v] of Object.entries(patchSpec.$inc)) {
        const cur = typeof obj[k] === "number" ? obj[k] : 0;
        obj[k] = cur + (Number(v) || 0);
      }
    }
    if (patchSpec.$unset) {
      const keysToUnset = Array.isArray(patchSpec.$unset)
        ? patchSpec.$unset
        : Object.keys(patchSpec.$unset);
      for (const k of keysToUnset) {
        delete obj[k];
      }
    }
    if (patchSpec.$push) {
      for (const [k, v] of Object.entries(patchSpec.$push)) {
        const arr = Array.isArray(obj[k]) ? [...obj[k]] : [];
        arr.push(v);
        obj[k] = arr;
      }
    }
    return obj;
  }

  _handleExpiredKey(key) {
    this.expirationsCount++;
    const prevEntry = this.storage.get(key);
    const prevValue = prevEntry ? prevEntry.value : undefined;
    const curVer = this.versions.get(key);
    const version = typeof curVer === "bigint" ? curVer + 1n : (curVer || 0) + 1;

    this.storage.delete(key);
    this.versions.delete(key);

    this.events.emit("change", {
      key,
      type: "expire",
      value: undefined,
      previousValue: prevValue,
      version,
      timestamp: Date.now()
    });
  }

  /**
   * Appends operation to Daktilo WAL if enabled.
   */
  async _logOperation(op) {
    if (this.daktilo) {
      this._faults.trigger("before-log-flush");
      const payload = op.encode();
      this._faults.trigger("before-log-append");
      const seq = await this.daktilo.append(payload);
      this._faults.trigger("after-log-append");
      this._sequenceCounter = seq;
      this._faults.trigger("after-log-flush");
      return { seq, size: payload.byteLength };
    }
    const seq = ++this._sequenceCounter;
    return { seq, size: 0 };
  }

  async _logOperationIfActive(op) {
    if (this.daktilo) {
      const { seq } = await this._logOperation(op);
      return seq;
    }
    return ++this._sequenceCounter;
  }

  _checkClusterLeader() {
    if (this.cluster && this.cluster.enabled && this.cluster._peers?.length > 0) {
      if (!this.cluster.isLeader()) {
        const status = this.cluster.status();
        const leaderAddress = typeof this.cluster.getLeaderAddress === "function" ? this.cluster.getLeaderAddress() : null;
        throw new NotLeaderError(this.cluster._nodeId, this.cluster.leaderId, status.role, status.term, leaderAddress);
      }
    }
  }

  _replicateCluster(op, options = {}) {
    if (this.cluster && this.cluster.enabled && this.cluster._peers?.length > 0) {
      this._checkClusterLeader();
      return this.cluster.replicate(op, { ack: options.ack, timeoutMs: options.timeoutMs });
    }
    return null;
  }

  async _withKeyLock(key, fn) {
    const currentHeldKeys = this._lockContext.getStore();
    if (currentHeldKeys && currentHeldKeys.has(key)) {
      // Re-entrant access within the same async execution context
      return await fn();
    }

    const prev = this._keyLocks.get(key);
    const { promise, resolve } = Promise.withResolvers();
    this._keyLocks.set(key, promise);

    if (prev) {
      try {
        await prev;
      } catch (_) {}
    }

    if (currentHeldKeys) {
      currentHeldKeys.add(key);
      try {
        return await fn();
      } finally {
        currentHeldKeys.delete(key);
        if (this._keyLocks.get(key) === promise) {
          this._keyLocks.delete(key);
        }
        resolve();
      }
    }

    const heldKeys = new Set();
    heldKeys.add(key);

    return this._lockContext.run(heldKeys, async () => {
      try {
        return await fn();
      } finally {
        if (this._keyLocks.get(key) === promise) {
          this._keyLocks.delete(key);
        }
        resolve();
      }
    });
  }

  /**
   * Basic KV SET.
   *
   * @param {string} key
   * @param {any} value
   * @param {object} [options={}]
   * @param {number|string} [options.ttl]
   * @param {boolean} [options.sliding=false]
   * @param {boolean} [options.consume=false]
   * @param {number|bigint} [options.ifVersion]
   * @returns {Promise<{ ok: boolean, version: number|bigint }>}
   */
  async set(key, value, options = {}) {
    if (this.isClosed) throw new DatabaseClosedError();
    this._checkClusterLeader();

    const shouldTrack = this._historyEnabled && this.historyManager.shouldTrack(key);

    // Fast-path: uncontended in-memory operation (no WAL, no history tracking for this key, no pending key locks, no cluster)
    if (!this.daktilo && !shouldTrack && !this._keyLocks.has(key) && !this.cluster) {
      if (this.expiry.isExpired(key)) {
        this._handleExpiredKey(key);
      }

      const currentVersion = this.versions.get(key) || 0;
      if (options.ifVersion !== undefined) {
        if (BigInt(options.ifVersion) !== BigInt(currentVersion)) {
          throw new VersionMismatchError(options.ifVersion, currentVersion);
        }
      }

      const nextVersion = typeof currentVersion === "bigint" ? currentVersion + 1n : currentVersion + 1;
      const ttlMs = options.ttl ? parseDuration(options.ttl) : 0;

      const hasListeners = this.events.listenerCount("change") > 0;
      let prevValue;
      if (hasListeners) {
        const prevEntry = this.storage.get(key);
        prevValue = prevEntry ? prevEntry.value : undefined;
      }

      this.storage.set(key, value, nextVersion);
      this.versions.set(key, nextVersion);

      if (ttlMs > 0) {
        this.expiry.schedule(key, ttlMs, {
          sliding: Boolean(options.sliding),
          consume: Boolean(options.consume)
        });
      } else {
        this.expiry.cancel(key);
      }

      if (hasListeners) {
        this.events.emit("change", {
          key,
          type: "set",
          value,
          previousValue: prevValue,
          version: nextVersion,
          timestamp: Date.now()
        });
      }
      this.opsCount++;

      return { ok: true, version: nextVersion };
    }

    return this._withKeyLock(key, async () => {
      // Check lazy expiration first
      if (this.expiry.isExpired(key)) {
        this._handleExpiredKey(key);
      }

      const currentVersion = this.versions.get(key) || (shouldTrack ? 0n : 0);

      // CAS optimistic check
      if (options.ifVersion !== undefined) {
        if (BigInt(options.ifVersion) !== BigInt(currentVersion)) {
          throw new VersionMismatchError(options.ifVersion, currentVersion);
        }
      }

      let flags = 0;
      if (options.sliding) flags |= FLAG_SLIDING;
      if (options.consume) flags |= FLAG_CONSUME;
      if (options.ifVersion !== undefined) flags |= FLAG_HAS_VERSION;

      const ttlMs = options.ttl ? parseDuration(options.ttl) : 0;
      if (ttlMs > 0) flags |= FLAG_HAS_TTL;

      const timestamp = Date.now();
      let seq = ++this._sequenceCounter;
      const nextVersion = shouldTrack ? seq : (Number(currentVersion) + 1);
      let payloadSize = 0;

      const partition = this.partitions.getPartition(key);
      const op = new Operation({
        op: OP_SET,
        partition,
        key,
        flags,
        version: nextVersion,
        ttlMs,
        value,
        timestamp
      });

      if (this.daktilo) {
        const logRes = await this._logOperation(op);
        seq = logRes.seq;
        payloadSize = logRes.size;
      }

      const rep = this._replicateCluster(op, options);
      if (rep) await rep;

      if (shouldTrack) {
        this.historyManager.record({
          sequence: seq,
          key,
          op: OP_SET,
          version: nextVersion,
          prevVersion: currentVersion,
          timestamp,
          ttlMs,
          value,
          size: payloadSize
        });
      }

      const hasListeners = this.events.listenerCount("change") > 0;
      let prevValue;
      if (hasListeners) {
        const prevEntry = this.storage.get(key);
        prevValue = prevEntry ? prevEntry.value : undefined;
      }

      this.storage.set(key, value, nextVersion);
      this.versions.set(key, nextVersion);

      if (ttlMs > 0) {
        this.expiry.schedule(key, ttlMs, {
          sliding: Boolean(options.sliding),
          consume: Boolean(options.consume)
        });
      } else {
        this.expiry.cancel(key);
      }

      if (hasListeners) {
        this.events.emit("change", {
          key,
          type: "set",
          value,
          previousValue: prevValue,
          version: nextVersion,
          timestamp
        });
      }
      this.opsCount++;

      return { ok: true, version: nextVersion };
    });
  }

  /**
   * Basic KV GET.
   *
   * @param {string} key
   * @param {object} [options={}]
   * @param {boolean} [options.withVersion=false]
   * @param {boolean} [options.touch=true]
   * @param {string|number|Date} [options.at]
   * @param {bigint|number} [options.atSequence]
   * @param {bigint|number} [options.version]
   * @returns {Promise<any>}
   */
  async get(key, options = {}) {
    if (this.isClosed) throw new DatabaseClosedError();

    // Check historical selector
    if (options.at !== undefined || options.atSequence !== undefined || options.version !== undefined) {
      if (!this._historyEnabled) throw new HistoryDisabledError();
      const entry = this.historyManager.getHistoricalEntry(key, options);
      if (!entry.exists) {
        return options.withVersion ? { value: undefined, version: 0n } : undefined;
      }
      return options.withVersion ? { value: entry.value, version: entry.version } : entry.value;
    }

    // Check lazy expiration (fast-path: bypass when no keys have TTL)
    let expEntry = null;
    if (this.expiry.hasActiveExpiries) {
      expEntry = this.expiry.getEntry(key);
      if (expEntry && expEntry.expireAt <= Date.now()) {
        this._handleExpiredKey(key);
        this.cacheMisses++;
        return options.withVersion ? { value: undefined, version: this._historyEnabled ? 0n : 0 } : undefined;
      }
    }

    const entry = this.storage.get(key);
    if (!entry) {
      this.cacheMisses++;
      return options.withVersion ? { value: undefined, version: this._historyEnabled ? 0n : 0 } : undefined;
    }

    this.cacheHits++;

    // Consume-on-read or sliding TTL renewal if entry has expiry configured
    if (expEntry) {
      if (expEntry.consume) {
        await this.delete(key);
        const v = entry.version !== undefined ? (this._historyEnabled ? BigInt(entry.version) : entry.version) : 0;
        return options.withVersion ? { value: entry.value, version: v } : entry.value;
      }
      if (options.touch !== false && expEntry.sliding) {
        this.expiry.touchEntry(expEntry);
      }
    }

    if (options.withVersion) {
      const v = entry.version !== undefined ? (this._historyEnabled ? BigInt(entry.version) : entry.version) : 0;
      return { value: entry.value, version: v };
    }
    return entry.value;
  }

  /**
   * Checks if key exists.
   *
   * @param {string} key
   * @param {object} [options={}]
   * @returns {Promise<boolean>}
   */
  async has(key, options = {}) {
    if (this.isClosed) throw new DatabaseClosedError();

    if (options.at !== undefined || options.atSequence !== undefined || options.version !== undefined) {
      if (!this._historyEnabled) throw new HistoryDisabledError();
      const entry = this.historyManager.getHistoricalEntry(key, options);
      return Boolean(entry.exists);
    }

    if (this.expiry.isExpired(key)) {
      this._handleExpiredKey(key);
      return false;
    }
    return this.storage.has(key);
  }

  /**
   * Deletes a key.
   *
   * @param {string} key
   * @returns {Promise<boolean>}
   */
  async delete(key, options = {}) {
    if (this.isClosed) throw new DatabaseClosedError();
    this._checkClusterLeader();

    const shouldTrack = this._historyEnabled && this.historyManager.shouldTrack(key);

    // Fast-path: uncontended in-memory operation (no WAL, no history, no pending locks, no ifVersion check, no cluster)
    if (!this.daktilo && !shouldTrack && !this._keyLocks.has(key) && options.ifVersion === undefined && !this.cluster) {
      if (!this.storage.has(key)) {
        return false;
      }

      const currentVersion = this.versions.get(key) || 0;
      const nextVersion = typeof currentVersion === "bigint" ? currentVersion + 1n : currentVersion + 1;

      const hasListeners = this.events.listenerCount("change") > 0;
      let prevValue;
      if (hasListeners) {
        const prevEntry = this.storage.get(key);
        prevValue = prevEntry ? prevEntry.value : undefined;
      }

      this.storage.delete(key);
      this.expiry.cancel(key);
      this.versions.delete(key);

      if (hasListeners) {
        this.events.emit("change", {
          key,
          type: "delete",
          value: undefined,
          previousValue: prevValue,
          version: nextVersion,
          timestamp: Date.now()
        });
      }
      this.opsCount++;

      return true;
    }

    return this._withKeyLock(key, async () => {
      if (!this.storage.has(key)) {
        if (options.ifVersion !== undefined) {
          throw new VersionMismatchError(options.ifVersion, 0);
        }
        return false;
      }

      const currentVersion = this.versions.get(key) || (shouldTrack ? 0n : 0);

      // CAS optimistic check
      if (options.ifVersion !== undefined) {
        if (BigInt(options.ifVersion) !== BigInt(currentVersion)) {
          throw new VersionMismatchError(options.ifVersion, currentVersion);
        }
      }

      let flags = 0;
      if (options.ifVersion !== undefined) flags |= FLAG_HAS_VERSION;

      let seq = ++this._sequenceCounter;
      const nextVersion = shouldTrack ? seq : (Number(currentVersion) + 1);
      const timestamp = Date.now();
      let payloadSize = 0;

      const partition = this.partitions.getPartition(key);
      const op = new Operation({
        op: OP_DEL,
        partition,
        key,
        flags,
        version: nextVersion,
        timestamp
      });

      if (this.daktilo) {
        const logRes = await this._logOperation(op);
        seq = logRes.seq;
        payloadSize = logRes.size;
      }

      const rep = this._replicateCluster(op, options);
      if (rep) await rep;

      if (shouldTrack) {
        this.historyManager.record({
          sequence: seq,
          key,
          op: OP_DEL,
          version: nextVersion,
          prevVersion: currentVersion,
          timestamp,
          value: undefined,
          size: payloadSize
        });
      }

      const hasListeners = this.events.listenerCount("change") > 0;
      let prevValue;
      if (hasListeners) {
        const prevEntry = this.storage.get(key);
        prevValue = prevEntry ? prevEntry.value : undefined;
      }

      this.storage.delete(key);
      this.expiry.cancel(key);
      this.versions.delete(key);

      if (hasListeners) {
        this.events.emit("change", {
          key,
          type: "delete",
          value: undefined,
          previousValue: prevValue,
          version: nextVersion,
          timestamp
        });
      }
      this.opsCount++;

      return true;
    });
  }

  /**
   * Returns TTL remaining in milliseconds (-2 if key does not exist, -1 if no TTL configured).
   *
   * @param {string} key
   * @returns {number}
   */
  ttl(key) {
    if (this.isClosed) throw new DatabaseClosedError();
    if (this.expiry.isExpired(key)) {
      this._handleExpiredKey(key);
      return -2;
    }
    if (!this.storage.has(key)) {
      return -2;
    }
    return this.expiry.ttl(key);
  }

  /**
   * Batch get.
   *
   * @param {string[]} keys
   * @returns {Promise<Record<string, any>>}
   */
  async getMany(keys) {
    const result = {};
    for (const k of keys) {
      result[k] = await this.get(k);
    }
    return result;
  }

  /**
   * Batch set.
   *
   * @param {Record<string, any> | Array<[string, any]>} entries
   * @param {object} [options={}]
   * @returns {Promise<void>}
   */
  async setMany(entries, options = {}) {
    const list = Array.isArray(entries) ? entries : Object.entries(entries);
    for (const [k, v] of list) {
      await this.set(k, v, options);
    }
  }

  /**
   * Returns list of keys matching prefix.
   *
   * @param {string} [prefix=""]
   * @returns {string[]}
   */
  keys(prefix = "") {
    if (this.isClosed) throw new DatabaseClosedError();
    return this.storage.keys(prefix);
  }

  /**
   * Atomic increment.
   *
   * @param {string} key
   * @param {number} [delta=1]
   * @returns {Promise<number>} next value
   */
  async incr(key, delta = 1, options = {}) {
    return this.increment(key, delta, options);
  }

  async decr(key, delta = 1, options = {}) {
    return this.increment(key, -delta, options);
  }

  async increment(key, delta = 1, options = {}) {
    if (this.isClosed) throw new DatabaseClosedError();
    this._checkClusterLeader();

    const shouldTrack = this._historyEnabled && this.historyManager.shouldTrack(key);

    // Fast-path: uncontended in-memory operation (no cluster)
    if (!this.daktilo && !shouldTrack && !this._keyLocks.has(key) && !this.cluster) {
      if (this.expiry.isExpired(key)) {
        this._handleExpiredKey(key);
      }

      const prevEntry = this.storage.get(key);
      const cur = prevEntry ? Number(prevEntry.value) || 0 : 0;
      const next = cur + delta;
      const currentVersion = this.versions.get(key) || 0;
      const nextVersion = typeof currentVersion === "bigint" ? currentVersion + 1n : currentVersion + 1;

      this.storage.set(key, next, nextVersion);
      this.versions.set(key, nextVersion);

      if (this.events.listenerCount("change") > 0) {
        this.events.emit("change", {
          key,
          type: "incr",
          value: next,
          previousValue: cur,
          version: nextVersion,
          timestamp: Date.now()
        });
      }
      this.opsCount++;

      return next;
    }

    return this._withKeyLock(key, async () => {
      if (this.expiry.isExpired(key)) {
        this._handleExpiredKey(key);
      }

      const prevEntry = this.storage.get(key);
      const cur = prevEntry ? Number(prevEntry.value) || 0 : 0;
      const next = cur + delta;
      const currentVersion = this.versions.get(key) || (shouldTrack ? 0n : 0);
      let seq = ++this._sequenceCounter;
      const nextVersion = shouldTrack ? seq : (Number(currentVersion) + 1);
      const timestamp = Date.now();
      let payloadSize = 0;

      const partition = this.partitions.getPartition(key);
      const op = new Operation({
        op: OP_INCR,
        partition,
        key,
        value: delta,
        version: nextVersion,
        timestamp
      });

      if (this.daktilo) {
        const logRes = await this._logOperation(op);
        seq = logRes.seq;
        payloadSize = logRes.size;
      }

      const rep = this._replicateCluster(op, options);
      if (rep) await rep;

      if (shouldTrack) {
        this.historyManager.record({
          sequence: seq,
          key,
          op: OP_INCR,
          version: nextVersion,
          prevVersion: currentVersion,
          timestamp,
          value: next,
          size: payloadSize
        });
      }

      this.storage.set(key, next, nextVersion);
      this.versions.set(key, nextVersion);

      if (this.events.listenerCount("change") > 0) {
        this.events.emit("change", {
          key,
          type: "incr",
          value: next,
          previousValue: cur,
          version: nextVersion,
          timestamp
        });
      }
      this.opsCount++;

      return next;
    });
  }

  /**
   * Atomic document patch ($set, $inc, $unset, $push).
   *
   * @param {string} key
   * @param {object} patchSpec
   * @returns {Promise<any>} patched document
   */
  async patch(key, patchSpec, options = {}) {
    if (this.isClosed) throw new DatabaseClosedError();
    this._checkClusterLeader();

    const shouldTrack = this._historyEnabled && this.historyManager.shouldTrack(key);

    // Fast-path: uncontended in-memory operation
    if (!this.daktilo && !shouldTrack && !this._keyLocks.has(key) && !this.cluster) {
      if (this.expiry.isExpired(key)) {
        this._handleExpiredKey(key);
      }

      const prevEntry = this.storage.get(key);
      const prevValue = prevEntry ? prevEntry.value : undefined;
      const nextValue = this._applyPatch(prevValue, patchSpec);

      const currentVersion = this.versions.get(key) || 0;
      const nextVersion = typeof currentVersion === "bigint" ? currentVersion + 1n : currentVersion + 1;

      this.storage.set(key, nextValue, nextVersion);
      this.versions.set(key, nextVersion);

      if (this.events.listenerCount("change") > 0) {
        this.events.emit("change", {
          key,
          type: "patch",
          value: nextValue,
          previousValue: prevValue,
          version: nextVersion,
          timestamp: Date.now()
        });
      }
      this.opsCount++;

      return nextValue;
    }

    return this._withKeyLock(key, async () => {
      if (this.expiry.isExpired(key)) {
        this._handleExpiredKey(key);
      }

      const prevEntry = this.storage.get(key);
      const prevValue = prevEntry ? prevEntry.value : undefined;
      const nextValue = this._applyPatch(prevValue, patchSpec);

      const currentVersion = this.versions.get(key) || (shouldTrack ? 0n : 0);
      let seq = ++this._sequenceCounter;
      const nextVersion = shouldTrack ? seq : (Number(currentVersion) + 1);
      const timestamp = Date.now();
      let payloadSize = 0;

      const partition = this.partitions.getPartition(key);
      const op = new Operation({
        op: OP_PATCH,
        partition,
        key,
        value: patchSpec,
        version: nextVersion,
        timestamp
      });

      if (this.daktilo) {
        const logRes = await this._logOperation(op);
        seq = logRes.seq;
        payloadSize = logRes.size;
      }

      const rep = this._replicateCluster(op, options);
      if (rep) await rep;

      if (shouldTrack) {
        this.historyManager.record({
          sequence: seq,
          key,
          op: OP_PATCH,
          version: nextVersion,
          prevVersion: currentVersion,
          timestamp,
          value: nextValue,
          size: payloadSize
        });
      }

      this.storage.set(key, nextValue, nextVersion);
      this.versions.set(key, nextVersion);

      if (this.events.listenerCount("change") > 0) {
        this.events.emit("change", {
          key,
          type: "patch",
          value: nextValue,
          previousValue: prevValue,
          version: nextVersion,
          timestamp
        });
      }
      this.opsCount++;

      return nextValue;
    });
  }

  /**
   * Appends an atomic RESTORE mutation forward in time.
   *
   * @param {string} key
   * @param {any} value
   * @param {object} [options={}]
   * @returns {Promise<{ fromVersion: bigint, restoredVersion: bigint, newVersion: bigint, sequence: bigint }>}
   */
  async restoreKey(key, value, options = {}) {
    if (this.isClosed) throw new DatabaseClosedError();

    return this._withKeyLock(key, async () => {
      const currentVersion = this.versions.get(key) || (this._historyEnabled ? 0n : 0);
      let seq = ++this._sequenceCounter;
      const nextVersion = this._historyEnabled ? seq : (Number(currentVersion) + 1);
      const timestamp = Date.now();
      let payloadSize = 0;

      const extra = {
        fromVersion: String(options.fromVersion ?? currentVersion),
        targetVersion: String(options.targetVersion ?? 0)
      };

      const partition = this.partitions.getPartition(key);
      const op = new Operation({
        op: OP_RESTORE,
        partition,
        key,
        flags: FLAG_RESTORE,
        version: nextVersion,
        value,
        timestamp,
        extra
      });

      if (this.daktilo) {
        const logRes = await this._logOperation(op);
        seq = logRes.seq;
        payloadSize = logRes.size;
      }

      const rep = this._replicateCluster(op, options);
      if (rep) await rep;

      if (this._historyEnabled) {
        this.historyManager.record({
          sequence: seq,
          key,
          op: OP_RESTORE,
          version: nextVersion,
          prevVersion: currentVersion,
          timestamp,
          value,
          extra,
          size: payloadSize
        });
      }

      const hasListeners = this.events.listenerCount("change") > 0;
      let prevValue;
      if (hasListeners) {
        const prevEntry = this.storage.get(key);
        prevValue = prevEntry ? prevEntry.value : undefined;
      }

      if (value === undefined) {
        this.storage.delete(key);
        this.expiry.cancel(key);
        this.versions.delete(key);
      } else {
        this.storage.set(key, value, nextVersion);
        this.versions.set(key, nextVersion);
      }

      if (hasListeners) {
        this.events.emit("change", {
          key,
          type: "restore",
          value,
          previousValue: prevValue,
          version: nextVersion,
          timestamp
        });
      }
      this.opsCount++;

      return {
        fromVersion: BigInt(currentVersion),
        restoredVersion: options.targetVersion !== undefined ? BigInt(options.targetVersion) : 0n,
        newVersion: BigInt(nextVersion),
        sequence: BigInt(seq)
      };
    });
  }

  /**
   * Rollback a single key to a target historical point or previous version.
   * Rollback never truncates Daktilo log; it appends an atomic forward RESTORE mutation.
   *
   * @param {string} key
   * @param {object} [options={}]
   * @param {bigint|number} [options.version]
   * @param {bigint|number} [options.sequence]
   * @param {string|number|Date} [options.to]
   * @returns {Promise<{ fromVersion: bigint, restoredVersion: bigint, newVersion: bigint, sequence: bigint }>}
   */
  async rollback(keyOrOptions, maybeOptions = {}) {
    if (this.isClosed) throw new DatabaseClosedError();
    if (!this._historyEnabled) throw new HistoryDisabledError();
    this._faults.trigger("during-rollback");

    let key = keyOrOptions;
    let options = maybeOptions;
    if (typeof keyOrOptions === "object" && keyOrOptions !== null) {
      key = keyOrOptions.key;
      options = { ...keyOrOptions };
    }

    let targetSelector = {};
    if (options.version !== undefined) {
      targetSelector.version = options.version;
    } else if (options.sequence !== undefined) {
      targetSelector.atSequence = options.sequence;
    } else if (options.targetSequence !== undefined) {
      targetSelector.atSequence = options.targetSequence;
    } else if (options.to !== undefined) {
      targetSelector.at = options.to;
    } else {
      // Default: immediate previous version
      const revisions = this.historyManager._keyHistory.get(key) || [];
      if (revisions.length < 2) {
        targetSelector.version = 0n;
      } else {
        const prevRev = revisions[revisions.length - 2];
        targetSelector.version = prevRev.version;
      }
    }

    const histEntry = this.historyManager.getHistoricalEntry(key, targetSelector);
    const targetValue = histEntry.exists ? histEntry.value : undefined;
    const targetVer = histEntry.version !== undefined ? histEntry.version : 0n;

    return this.restoreKey(key, targetValue, {
      targetVersion: targetVer
    });
  }

  /**
   * Creates an inspectable, conflict-checked RollbackPlan for a collection, scope, or db.
   *
   * @param {object} [options={}]
   * @param {string} [options.prefix='']
   * @param {string} [options.scope='']
   * @param {string|number|Date} [options.to]
   * @param {bigint|number} [options.sequence]
   * @param {bigint|number} [options.version]
   * @returns {Promise<RollbackPlan>}
   */
  async rollbackPlan(options = {}) {
    if (this.isClosed) throw new DatabaseClosedError();
    if (!this._historyEnabled) throw new HistoryDisabledError();

    const prefix = options.scope || options.prefix || "";
    const selector = {};
    if (options.to !== undefined) selector.at = options.to;
    if (options.at !== undefined) selector.at = options.at;
    if (options.sequence !== undefined) selector.atSequence = options.sequence;
    if (options.atSequence !== undefined) selector.atSequence = options.atSequence;
    if (options.version !== undefined) selector.version = options.version;

    const currentSeq = this._sequenceCounter;
    const currentKeys = new Set(this.storage.keys(prefix));
    const historicalKeys = new Set(this.historyManager.keysAt(prefix, selector));
    const allKeys = new Set([...currentKeys, ...historicalKeys]);

    const planEntries = [];
    const capturedVersions = new Map();
    let targetSeq = 0n;

    for (const k of allKeys) {
      const curVersion = this.versions.get(k) || 0n;
      capturedVersions.set(k, BigInt(curVersion));

      const hist = this.historyManager.getHistoricalEntry(k, selector);
      if (hist.sequence && hist.sequence > targetSeq) {
        targetSeq = hist.sequence;
      }

      const curEntry = this.storage.get(k);
      const curVal = curEntry ? curEntry.value : undefined;

      if (!hist.exists) {
        if (curVal !== undefined) {
          planEntries.push({
            key: k,
            action: "delete",
            currentVersion: BigInt(curVersion),
            targetVersion: 0n,
            value: undefined,
            size: 0
          });
        } else {
          planEntries.push({
            key: k,
            action: "noop",
            currentVersion: BigInt(curVersion),
            targetVersion: 0n,
            value: undefined,
            size: 0
          });
        }
      } else {
        const isSame = JSON.stringify(curVal) === JSON.stringify(hist.value);
        if (isSame) {
          planEntries.push({
            key: k,
            action: "noop",
            currentVersion: BigInt(curVersion),
            targetVersion: hist.version || 0n,
            value: hist.value,
            size: 0
          });
        } else {
          planEntries.push({
            key: k,
            action: "restore",
            currentVersion: BigInt(curVersion),
            targetVersion: hist.version || 0n,
            value: hist.value,
            size: typeof hist.value === "string" ? hist.value.length : 64
          });
        }
      }
    }

    return new RollbackPlan({
      engine: this,
      historyManager: this.historyManager,
      prefix,
      selector,
      targetSequence: targetSeq,
      currentSequence: currentSeq,
      planEntries,
      capturedVersions
    });
  }

  /**
   * Returns a read-only historical database view at the specified point.
   *
   * @param {string|number|Date|object} selector
   * @returns {HistoricalView}
   */
  at(selector) {
    if (this.isClosed) throw new DatabaseClosedError();
    if (!this._historyEnabled) throw new HistoryDisabledError();

    let sel = selector;
    if (typeof selector === "bigint") {
      sel = { atSequence: selector };
    } else if (typeof selector === "number") {
      if (selector < 1_000_000_000) {
        sel = { atSequence: BigInt(selector) };
      } else {
        sel = { at: selector };
      }
    } else if (typeof selector === "string" || selector instanceof Date) {
      sel = { at: selector };
    }
    return new HistoricalView(this.historyManager, sel);
  }

  /**
   * Creates a pinned historical or point-in-time snapshot.
   *
   * @param {object} [options={}]
   * @returns {Promise<HistoricalView>}
   */
  async snapshot(options = {}) {
    if (this.isClosed) throw new DatabaseClosedError();
    if (!this._historyEnabled) throw new HistoryDisabledError();

    let sel = {};
    if (options.at !== undefined) sel.at = options.at;
    if (options.to !== undefined) sel.at = options.to;
    if (options.sequence !== undefined) sel.atSequence = options.sequence;
    if (options.atSequence !== undefined) sel.atSequence = options.atSequence;
    if (options.version !== undefined) sel.version = options.version;

    let targetSeq = this._sequenceCounter;
    if (sel.atSequence !== undefined) {
      targetSeq = BigInt(sel.atSequence);
    } else if (sel.at === undefined && sel.version === undefined) {
      sel.atSequence = targetSeq;
    }

    const pin = this.historyManager.registerSnapshot(targetSeq, Date.now());
    return new HistoricalView(this.historyManager, sel, pin);
  }

  /**
   * Iterates or inspects historical revisions for a key or coordination primitive.
   *
   * @param {string|object} keyOrOptions
   * @param {object} [options={}]
   * @returns {AsyncGenerator<object>|Array<object>}
   */
  history(keyOrOptions, options = {}) {
    if (this.isClosed) throw new DatabaseClosedError();
    if (!this._historyEnabled) throw new HistoryDisabledError();

    if (typeof keyOrOptions === "object" && keyOrOptions !== null) {
      if (keyOrOptions.type) {
        return this.historyManager.getCoordinationHistory(keyOrOptions);
      }
      return this.historyManager.history(keyOrOptions.key || "", keyOrOptions);
    }

    return this.historyManager.history(keyOrOptions, options);
  }

  /**
   * Returns coordination event history for locks.
   *
   * @param {string} key
   * @returns {Array<object>}
   */
  lockHistory(key) {
    return this.historyManager.getCoordinationHistory({ type: "lock", key });
  }

  /**
   * Compacts WAL log segments strictly before the minimum retained history/snapshot sequence.
   */
  async compact() {
    if (this.daktilo) {
      const minSeq = this._historyEnabled ? this.historyManager.getMinimumRetainedSequence() : 1n;
      await this.daktilo.compact(minSeq);
    }
  }

  /**
   * Optimistic update with automatic CAS retry.
   *
   * @param {string} key
   * @param {function(any): any} updater
   * @param {object} [options={}]
   * @param {number} [options.maxRetries=5]
   * @returns {Promise<any>}
   */
  async update(key, updater, options = {}) {
    const maxRetries = options.maxRetries || 5;
    for (let attempt = 0; attempt < maxRetries; attempt++) {
      const { value, version } = await this.get(key, { withVersion: true });
      const cloned = value !== undefined ? safeClone(value) : undefined;
      const next = await updater(cloned);

      try {
        const setOpts = { ifVersion: version };
        if (options.ttl !== undefined) setOpts.ttl = options.ttl;
        if (options.sliding !== undefined) setOpts.sliding = options.sliding;
        await this.set(key, next, setOpts);
        return next;
      } catch (err) {
        if (err instanceof VersionMismatchError && attempt < maxRetries - 1) {
          // Exponential backoff with jitter
          const delay = Math.floor(Math.random() * (1 << attempt) * 10);
          await new Promise((r) => setTimeout(r, delay));
          continue;
        }
        throw err;
      }
    }
  }

  /**
   * Next monotonic fencing token for distributed coordination.
   *
   * @returns {number}
   */
  nextFencingToken() {
    return ++this.fencingCounter;
  }

  /**
   * Returns database statistics.
   */
  stats() {
    const storageStats = this.storage.stats();
    return {
      keys: storageStats.keyCount,
      memoryBytes: storageStats.totalBytes,
      operations: this.opsCount,
      reads: storageStats.reads,
      writes: storageStats.writes,
      deletes: storageStats.deletes,
      cacheHits: this.cacheHits,
      cacheMisses: this.cacheMisses,
      expirations: this.expirationsCount,
      activeWatchers: this.events.listenerCount("change"),
      daktiloHead: this.daktilo ? Number(this.daktilo.head()) : 0,
      daktiloDurableHead: this.daktilo ? Number(this.daktilo.durableHead()) : 0
    };
  }

  /**
   * Flushes WAL and persists checkpoints.
   */
  async checkpoint() {
    this._faults.trigger("during-checkpoint");
    if (this.daktilo) {
      await this.daktilo.flush();
      await this.daktilo.checkpoint(this.daktilo.head());
    }
  }

  /**
   * Closes database engine and releases all resources.
   */
  async close() {
    if (this.isClosed) return;
    this.isClosed = true;

    this.expiry.close();
    await this.storage.close();

    if (this.daktilo) {
      await this.daktilo.close();
    }
    this.events.removeAllListeners();
  }
}

/**
 * Safely deep-clones values for update() callbacks, preserving BigInt, Buffers,
 * Uint8Arrays, circular references, and prototype-free objects without throwing.
 */
export function safeClone(value) {
  if (value === null || typeof value !== "object") {
    return value;
  }
  if (Buffer.isBuffer(value)) {
    return Buffer.from(value);
  }
  if (value instanceof Uint8Array) {
    return new Uint8Array(value);
  }
  try {
    return structuredClone(value);
  } catch (_) {
    if (Array.isArray(value)) {
      return value.map((item) => safeClone(item));
    }
    const copy = {};
    for (const [k, v] of Object.entries(value)) {
      copy[k] = safeClone(v);
    }
    return copy;
  }
}

