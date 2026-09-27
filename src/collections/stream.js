/**
 * Stream collection: append-oriented log stream with async iterator consumption.
 */

export class StreamCollection {
  /**
   * @param {import('../core/engine.js').TencereEngine} engine
   * @param {string} name
   */
  constructor(engine, name) {
    this._engine = engine;
    this._name = name;
    this._seqKey = `__stream_seq:${name}`;
    this._itemPrefix = `__stream_item:${name}:`;
    this._eventPrefix = `stream:${name}`;
  }

  _itemKey(seq) {
    return `${this._itemPrefix}${seq}`;
  }

  /**
   * Appends an event to the stream.
   *
   * @param {any} data
   * @returns {Promise<{ id: string, sequence: number, timestamp: number, data: any }>}
   */
  async append(data) {
    const seq = await this._engine.increment(this._seqKey, 1);
    const timestamp = Date.now();
    const entry = {
      id: `${seq}`,
      sequence: seq,
      timestamp,
      data
    };

    await this._engine.set(this._itemKey(seq), entry);

    // Notify active stream consumers
    this._engine.events.emit(this._eventPrefix, entry);

    return entry;
  }

  /**
   * Consumes events from the stream starting from `fromId`.
   *
   * @param {object} [options={}]
   * @param {number|string} [options.fromId=0] Starting sequence
   * @param {boolean} [options.tail=true] If true, keeps waiting for new incoming events
   * @returns {AsyncGenerator<{ id: string, sequence: number, timestamp: number, data: any }>}
   */
  async *consume(options = {}) {
    let nextSeq = Number(options.fromId || 0) + 1;
    const tail = options.tail !== false;
    let closed = false;

    // First replay existing entries from nextSeq up to current head
    const currentHead = Number((await this._engine.get(this._seqKey)) || 0);
    while (nextSeq <= currentHead) {
      const item = await this._engine.get(this._itemKey(nextSeq));
      if (item) {
        yield item;
      }
      nextSeq++;
    }

    if (!tail) return;

    // Then consume live entries via event notification
    const queue = [];
    let notify = null;

    const listener = (entry) => {
      if (closed || entry.sequence < nextSeq) return;
      queue.push(entry);
      if (notify) {
        const fn = notify;
        notify = null;
        fn();
      }
    };

    this._engine.events.on(this._eventPrefix, listener);

    try {
      while (!closed && !this._engine.isClosed) {
        while (queue.length > 0) {
          const entry = queue.shift();
          nextSeq = entry.sequence + 1;
          yield entry;
        }

        await new Promise((resolve) => {
          notify = resolve;
        });
      }
    } finally {
      closed = true;
      this._engine.events.removeListener(this._eventPrefix, listener);
    }
  }

  /**
   * Returns current head sequence of stream.
   */
  async head() {
    const val = await this._engine.get(this._seqKey);
    return val !== undefined ? Number(val) : 0;
  }

  /**
   * History inspection for stream (streams are inherently append-only history).
   *
   * @param {object} [options={}]
   * @returns {AsyncGenerator<object>}
   */
  async *history(options = {}) {
    const head = await this.head();
    for (let seq = 1; seq <= head; seq++) {
      const item = await this._engine.get(this._itemKey(seq));
      if (item) {
        yield item;
      }
    }
  }

  /**
   * Rollback is prohibited on streams to avoid truncating immutable log records.
   * Prefer appending compensating records instead.
   */
  async rollback() {
    const { UnsupportedHistoricalOperationError } = await import("../errors.js");
    throw new UnsupportedHistoricalOperationError(
      "Stream rollback cannot truncate log history; write compensating records instead."
    );
  }
}

