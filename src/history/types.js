/**
 * Canonical mutation operation codes, flags, names, and capability definitions
 * for Tencere's unified history and time-travel architecture.
 */

export const OP_SET = 0x01;
export const OP_DEL = 0x02;
export const OP_EXPIRE = 0x03;
export const OP_INCR = 0x04;
export const OP_PATCH = 0x05;
export const OP_SORTED_SET = 0x06;
export const OP_SORTED_DEL = 0x07;
export const OP_STREAM_APPEND = 0x08;
export const OP_LOCK_ACQUIRE = 0x09;
export const OP_LOCK_RELEASE = 0x0a;
export const OP_LOCK_RENEW = 0x0b;
export const OP_LOCK_EXPIRE = 0x0c;
export const OP_QUEUE_PUSH = 0x0d;
export const OP_QUEUE_ACK = 0x0e;
export const OP_CHECKPOINT = 0x0f;
export const OP_RESTORE = 0x10;
export const OP_RESTORE_BEGIN = 0x11;
export const OP_RESTORE_COMMIT = 0x12;

export const OP_NAMES = {
  [OP_SET]: "SET",
  [OP_DEL]: "DELETE",
  [OP_EXPIRE]: "EXPIRE",
  [OP_INCR]: "COUNTER_ADD",
  [OP_PATCH]: "PATCH",
  [OP_SORTED_SET]: "SORTED_SET",
  [OP_SORTED_DEL]: "SORTED_DELETE",
  [OP_STREAM_APPEND]: "STREAM_APPEND",
  [OP_LOCK_ACQUIRE]: "LOCK_ACQUIRE",
  [OP_LOCK_RELEASE]: "LOCK_RELEASE",
  [OP_LOCK_RENEW]: "LOCK_RENEW",
  [OP_LOCK_EXPIRE]: "LOCK_EXPIRE",
  [OP_QUEUE_PUSH]: "QUEUE_PUSH",
  [OP_QUEUE_ACK]: "QUEUE_ACK",
  [OP_CHECKPOINT]: "CHECKPOINT",
  [OP_RESTORE]: "RESTORE",
  [OP_RESTORE_BEGIN]: "RESTORE_BEGIN",
  [OP_RESTORE_COMMIT]: "RESTORE_COMMIT"
};

export const FLAG_SLIDING = 0x01;
export const FLAG_CONSUME = 0x02;
export const FLAG_HAS_VERSION = 0x04;
export const FLAG_HAS_TTL = 0x08;
export const FLAG_RESTORE = 0x10;

/**
 * Historical capabilities taxonomy across state and coordination primitives.
 */
export const CAPABILITIES = {
  kv: { history: true, at: true, rollback: true },
  map: { history: true, at: true, rollback: true },
  set: { history: true, at: true, rollback: true },
  sorted: { history: true, at: true, rollback: true },
  counter: { history: true, at: true, rollback: true },
  vector: { history: true, at: true, rollback: true },
  ttl: { history: true, at: true, rollback: true },
  stream: { history: true, at: true, rollback: "limited" },
  queue: { history: true, at: true, rollback: "limited" },
  lock: { history: true, at: false, rollback: false },
  semaphore: { history: true, at: false, rollback: false },
  rateLimit: { history: true, at: false, rollback: false },
  once: { history: true, at: false, rollback: false },
  idempotency: { history: true, at: false, rollback: false },
  scheduler: { history: true, at: false, rollback: false }
};
