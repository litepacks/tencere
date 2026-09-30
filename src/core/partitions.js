/**
 * Partition management and consistent key-to-partition routing.
 * Ensures single-node and cluster modes share the exact same partition abstraction.
 */

export const DEFAULT_PARTITION_COUNT = 128;

/**
 * Fast 32-bit FNV-1a hash.
 *
 * @param {string} str
 * @returns {number}
 */
export function fnv1a32(str) {
  let hash = 0x811c9dc5;
  for (let i = 0; i < str.length; i++) {
    hash ^= str.charCodeAt(i);
    hash = Math.imul(hash, 0x01000193);
  }
  return hash >>> 0;
}

export class PartitionManager {
  /**
   * @param {object} [options={}]
   * @param {number} [options.partitionCount=128]
   * @param {string} [options.localNodeId='local']
   */
  constructor(options = {}) {
    this.partitionCount = options.partitionCount || DEFAULT_PARTITION_COUNT;
    this.localNodeId = options.localNodeId || "local";

    // partitionId -> nodeId
    this.partitionOwners = new Array(this.partitionCount).fill(this.localNodeId);
    this._mask = (this.partitionCount & (this.partitionCount - 1)) === 0 ? this.partitionCount - 1 : -1;
  }

  /**
   * Resolves partition index for a given key.
   *
   * @param {string} key
   * @returns {number}
   */
  getPartition(key) {
    const h = fnv1a32(key);
    return this._mask !== -1 ? (h & this._mask) : (h % this.partitionCount);
  }

  /**
   * Returns owner nodeId for a partition.
   *
   * @param {number} partitionId
   * @returns {string}
   */
  getOwner(partitionId) {
    return this.partitionOwners[partitionId] || this.localNodeId;
  }

  /**
   * Checks whether the current local node owns the given key.
   *
   * @param {string} key
   * @returns {boolean}
   */
  isLocal(key) {
    const p = this.getPartition(key);
    return this.getOwner(p) === this.localNodeId;
  }

  /**
   * Reassigns partitions evenly across an array of cluster node IDs.
   *
   * @param {string[]} nodeIds
   */
  rebalance(nodeIds) {
    if (!nodeIds || nodeIds.length === 0) return;
    for (let p = 0; p < this.partitionCount; p++) {
      this.partitionOwners[p] = nodeIds[p % nodeIds.length];
    }
  }

  /**
   * Manually sets ownership of a single partition.
   *
   * @param {number} partitionId
   * @param {string} nodeId
   */
  assign(partitionId, nodeId) {
    if (partitionId >= 0 && partitionId < this.partitionCount) {
      this.partitionOwners[partitionId] = nodeId;
    }
  }
}
