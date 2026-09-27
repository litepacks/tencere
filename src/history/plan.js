/**
 * RollbackPlan: Generates previewable, streamable, and conflict-checked rollback plans
 * for collections, scopes, or the entire database.
 */

import { RollbackConflictError, StalePlanError } from "../errors.js";
import { OP_RESTORE, OP_RESTORE_BEGIN, OP_RESTORE_COMMIT, OP_DEL } from "./types.js";
import { Operation } from "../core/operations.js";

export class RollbackPlan {
  /**
   * @param {object} params
   * @param {import('../core/engine.js').TencereEngine} params.engine
   * @param {import('./index.js').HistoryManager} params.historyManager
   * @param {string} [params.prefix='']
   * @param {object} params.selector
   * @param {bigint} params.targetSequence
   * @param {bigint} params.currentSequence
   * @param {Array<object>} params.planEntries
   * @param {Map<string, bigint>} params.capturedVersions
   */
  constructor(params) {
    this._engine = params.engine;
    this._historyManager = params.historyManager;
    this._prefix = params.prefix || "";
    this._selector = params.selector;
    this.targetSequence = params.targetSequence;
    this.currentSequence = params.currentSequence;
    this._entries = params.planEntries; // Array<{ key, action, currentVersion, targetVersion, value, size }>
    this._capturedVersions = params.capturedVersions;

    this.affectedKeys = this._entries.filter((e) => e.action !== "noop").length;
    this.restore = this._entries.filter((e) => e.action === "restore").length;
    this.delete = this._entries.filter((e) => e.action === "delete").length;
    this.unchanged = this._entries.filter((e) => e.action === "noop").length;
    this.estimatedBytes = this._entries.reduce((acc, e) => acc + (e.size || 0), 0);

    this.applied = false;
  }

  /**
   * Returns a concise summary of the rollback plan.
   *
   * @returns {Promise<object>}
   */
  async summary() {
    return {
      targetSequence: this.targetSequence,
      currentSequence: this.currentSequence,
      affectedKeys: this.affectedKeys,
      restore: this.restore,
      delete: this.delete,
      unchanged: this.unchanged,
      estimatedBytes: this.estimatedBytes
    };
  }

  /**
   * Streams plan changes without materializing all representations at once.
   *
   * @returns {AsyncGenerator<object>}
   */
  async *changes() {
    for (const entry of this._entries) {
      if (entry.action === "noop") continue;
      yield {
        key: entry.key,
        action: entry.action,
        currentVersion: entry.currentVersion,
        targetVersion: entry.targetVersion,
        value: entry.value
      };
    }
  }

  /**
   * Applies the rollback plan.
   *
   * @param {object} [options={}]
   * @param {'abort'|'skip'|'overwrite'} [options.onConflict='abort']
   * @returns {Promise<{ applied: number, skipped: number, sequence: bigint }>}
   */
  async apply(options = {}) {
    if (this.applied) {
      throw new StalePlanError("Rollback plan has already been applied");
    }

    const onConflict = options.onConflict || "abort";
    let appliedCount = 0;
    let skippedCount = 0;

    // 1. Conflict detection pass
    for (const entry of this._entries) {
      if (entry.action === "noop") continue;

      const currentLiveVersion = this._engine.versions.get(entry.key) || 0n;
      const expectedVersion = this._capturedVersions.get(entry.key) || 0n;

      if (BigInt(currentLiveVersion) !== BigInt(expectedVersion)) {
        if (onConflict === "abort") {
          throw new RollbackConflictError(
            `Key '${entry.key}' was modified after rollback plan was created. Expected version ${expectedVersion}, got ${currentLiveVersion}`
          );
        }
      }
    }

    // 2. Mark begin for crash safety / auditability
    const startSeq = await this._engine._logOperationIfActive(new Operation({
      op: OP_RESTORE_BEGIN,
      key: "__restore:begin",
      timestamp: Date.now(),
      extra: {
        targetSequence: this.targetSequence.toString(),
        affectedKeys: this.affectedKeys
      }
    }));

    // 3. Apply mutations forward
    let lastSeq = startSeq || 0n;

    for (const entry of this._entries) {
      if (entry.action === "noop") continue;

      const currentLiveVersion = this._engine.versions.get(entry.key) || 0n;
      const expectedVersion = this._capturedVersions.get(entry.key) || 0n;

      if (BigInt(currentLiveVersion) !== BigInt(expectedVersion)) {
        if (onConflict === "skip") {
          skippedCount++;
          continue;
        }
        // If overwrite, proceed
      }

      if (entry.action === "delete") {
        const deleted = await this._engine.delete(entry.key);
        if (deleted) appliedCount++;
      } else if (entry.action === "restore") {
        const res = await this._engine.restoreKey(entry.key, entry.value, {
          targetVersion: entry.targetVersion,
          fromVersion: currentLiveVersion
        });
        lastSeq = res.sequence;
        appliedCount++;
      }
    }

    // 4. Mark commit
    await this._engine._logOperationIfActive(new Operation({
      op: OP_RESTORE_COMMIT,
      key: "__restore:commit",
      timestamp: Date.now(),
      extra: {
        applied: appliedCount,
        skipped: skippedCount
      }
    }));

    this.applied = true;
    return {
      applied: appliedCount,
      skipped: skippedCount,
      sequence: lastSeq
    };
  }
}
