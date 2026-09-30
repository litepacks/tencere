/**
 * Tencere error taxonomy.
 */

export class TencereError extends Error {
  constructor(message, code = "ERR_TENCERE") {
    super(message);
    this.name = this.constructor.name;
    this.code = code;
  }
}

export class VersionMismatchError extends TencereError {
  constructor(expected, actual) {
    super(
      `CAS version mismatch: expected ${expected}, got ${actual}`,
      "ERR_VERSION_MISMATCH"
    );
    this.expected = expected;
    this.actual = actual;
  }
}

export class LockAcquireError extends TencereError {
  constructor(key, reason = "Lock currently held by another owner") {
    super(`Failed to acquire lock for '${key}': ${reason}`, "ERR_LOCK_ACQUIRE");
    this.key = key;
  }
}

export class LockStaleOwnerError extends TencereError {
  constructor(key, token) {
    super(
      `Stale lock owner cannot release lock '${key}' with token ${token}`,
      "ERR_LOCK_STALE_OWNER"
    );
    this.key = key;
    this.token = token;
  }
}

export class TimeoutError extends TencereError {
  constructor(message = "Operation timed out") {
    super(message, "ERR_TIMEOUT");
  }
}

export class KeyNotFoundError extends TencereError {
  constructor(key) {
    super(`Key '${key}' not found`, "ERR_KEY_NOT_FOUND");
    this.key = key;
  }
}

export class DatabaseClosedError extends TencereError {
  constructor() {
    super("Database engine is closed", "ERR_DATABASE_CLOSED");
  }
}

export class ClusterNotAvailableError extends TencereError {
  constructor(message = "Cluster is not enabled or not ready") {
    super(message, "ERR_CLUSTER_NOT_AVAILABLE");
  }
}

export class NotLeaderError extends TencereError {
  constructor(nodeId, leaderId = null, role = "follower", term = 1, leaderAddress = null) {
    super(
      `[Cluster Fencing] Node ${nodeId} is not leader (role: ${role}, term: ${term})${leaderAddress ? `. Leader is at ${leaderAddress}` : ""}`,
      "ERR_NOT_LEADER"
    );
    this.nodeId = nodeId;
    this.leaderId = leaderId;
    this.role = role;
    this.term = term;
    this.leaderAddress = leaderAddress;
  }
}

export class ReadOnlyDatabaseError extends TencereError {
  constructor(message = "Cannot perform write mutation on a read-only historical database view or snapshot") {
    super(message, "ERR_READ_ONLY");
  }
}

export class HistoryDisabledError extends TencereError {
  constructor(message = "History subsystem is not enabled") {
    super(message, "ERR_HISTORY_DISABLED");
  }
}

export class HistoryUnavailableError extends TencereError {
  constructor(message = "Historical state for the requested target is unavailable or has been pruned") {
    super(message, "ERR_HISTORY_UNAVAILABLE");
  }
}

export class RollbackConflictError extends TencereError {
  constructor(message = "Rollback plan conflict: target key has been concurrently modified") {
    super(message, "ERR_ROLLBACK_CONFLICT");
  }
}

export class StalePlanError extends TencereError {
  constructor(message = "Rollback plan is stale or has already been applied") {
    super(message, "ERR_STALE_PLAN");
  }
}

export class UnsupportedHistoricalOperationError extends TencereError {
  constructor(message = "Operation not supported on this historical primitive") {
    super(message, "ERR_UNSUPPORTED_HISTORICAL_OP");
  }
}

export class TimeSeriesError extends TencereError {
  constructor(message, code = "ERR_TIMESERIES") {
    super(message, code);
  }
}

export class InvalidTimeSeriesValueError extends TimeSeriesError {
  constructor(message = "TimeSeries value must be a finite number") {
    super(message, "ERR_INVALID_TS_VALUE");
  }
}

export class TagLimitExceededError extends TimeSeriesError {
  constructor(message = "Exceeded maximum tags per point or tag key/value length") {
    super(message, "ERR_TAG_LIMIT");
  }
}

export class SeriesCardinalityExceededError extends TimeSeriesError {
  constructor(message = "Exceeded maximum series cardinality") {
    super(message, "ERR_SERIES_CARDINALITY");
  }
}

export class LimitExceededError extends TencereError {
  constructor(message = "Operation exceeds defined limits", code = "ERR_LIMIT_EXCEEDED") {
    super(message, code);
  }
}

export class KeyTooLargeError extends LimitExceededError {
  constructor(size, limit = 65535) {
    super(`Key size (${size} bytes) exceeds maximum allowed limit (${limit} bytes)`, "ERR_KEY_TOO_LARGE");
    this.size = size;
    this.limit = limit;
  }
}

export class ValueTooLargeError extends LimitExceededError {
  constructor(size, limit = 4294967295) {
    super(`Value size (${size} bytes) exceeds maximum allowed limit (${limit} bytes)`, "ERR_VALUE_TOO_LARGE");
    this.size = size;
    this.limit = limit;
  }
}

