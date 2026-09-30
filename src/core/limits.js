/**
 * Tencere System Limits, Protocol Constants & Defaults.
 * Single source of truth for binary serialization widths, cluster recommendations,
 * collection constraints, and coordination timeouts.
 */

export const LIMITS = Object.freeze({
  /**
   * Binary WAL (Daktilo) and network wire-protocol field limits.
   */
  BINARY: Object.freeze({
    /** Maximum key length in bytes: 65,535 (2^16 - 1, Uint16, 64 KB) */
    MAX_KEY_BYTES: 65535,
    /** Maximum extra JSON metadata length in bytes: 65,535 (2^16 - 1, Uint16, 64 KB) */
    MAX_EXTRA_BYTES: 65535,
    /** Maximum serialized value length in bytes: 4,294,967,295 (2^32 - 1, Uint32, ~4 GB) */
    MAX_VALUE_BYTES: 4294967295,
    /** Maximum number of partition IDs: 65,535 (2^16 - 1, Uint16) */
    MAX_PARTITIONS: 65535,
    /** Maximum TTL duration in milliseconds: 4,294,967,295 ms (~49.7 days, Uint32) */
    MAX_TTL_MS: 4294967295,
    /** Maximum version / sequence integer: 18,446,744,073,709,551,615 (2^64 - 1, Uint64 BigInt) */
    MAX_VERSION: 18446744073709551615n
  }),

  /**
   * High-availability clustering (Raptiye / Raft) recommendations and defaults.
   */
  CLUSTER: Object.freeze({
    /** Minimum nodes in a cluster */
    MIN_NODES: 1,
    /** Recommended maximum nodes in a single Raft consensus group before fan-out latency increases */
    RECOMMENDED_MAX_NODES: 9,
    /** Default Raft leader heartbeat interval in ms */
    DEFAULT_HEARTBEAT_MS: 15,
    /** Default minimum election timeout in ms */
    DEFAULT_ELECTION_MIN_MS: 30,
    /** Default maximum election timeout in ms */
    DEFAULT_ELECTION_MAX_MS: 60,
    /** Maximum batch size in bytes for Raft replication: 1 MB */
    MAX_RAFT_BATCH_BYTES: 1048576,
    /** Maximum number of concurrent inflight replication batches */
    MAX_INFLIGHT_BATCHES: 64
  }),

  /**
   * TimeSeries collection constraints and defaults.
   */
  TIMESERIES: Object.freeze({
    /** Default maximum number of unique time series per collection */
    DEFAULT_MAX_SERIES: 100000,
    /** Default maximum tags allowed per metric sample */
    DEFAULT_MAX_TAGS: 8,
    /** Maximum character length for a tag key */
    MAX_TAG_KEY_LENGTH: 64,
    /** Maximum character length for a tag value */
    MAX_TAG_VALUE_LENGTH: 128
  }),

  /**
   * Distributed coordination and scheduling limits.
   */
  COORDINATION: Object.freeze({
    /** Node.js setTimeout 32-bit signed integer limit: 2,147,483,647 ms (~24.85 days) */
    MAX_TIMEOUT_MS: 2147483647,
    /** Default distributed lock lease duration in ms */
    DEFAULT_LOCK_TTL_MS: 5000,
    /** Default semaphore resource capacity */
    DEFAULT_SEMAPHORE_CAPACITY: 1
  }),

  /**
   * Vector collection defaults.
   */
  VECTOR: Object.freeze({
    /** Recommended upper bound for embedding vector dimensions */
    RECOMMENDED_MAX_DIMENSIONS: 4096,
    /** Default top-K neighbors returned in similarity search */
    DEFAULT_TOP_K: 10
  })
});

// Flat convenience aliases
export const MAX_KEY_BYTES = LIMITS.BINARY.MAX_KEY_BYTES;
export const MAX_EXTRA_BYTES = LIMITS.BINARY.MAX_EXTRA_BYTES;
export const MAX_VALUE_BYTES = LIMITS.BINARY.MAX_VALUE_BYTES;
export const MAX_PARTITIONS = LIMITS.BINARY.MAX_PARTITIONS;
export const MAX_TTL_MS = LIMITS.BINARY.MAX_TTL_MS;
export const MAX_TIMEOUT_MS = LIMITS.COORDINATION.MAX_TIMEOUT_MS;
